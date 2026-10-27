package com.ashutosh.scheduler.worker.engine;

import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/**
 * Handles atomic job claiming and fenced status writes using native PostgreSQL queries.
 */
@Repository
public class JobClaimRepository {

    private static final Logger log = LoggerFactory.getLogger(JobClaimRepository.class);

    private final JdbcTemplate jdbcTemplate;

    public JobClaimRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static final RowMapper<JobEntity> JOB_ROW_MAPPER = new RowMapper<>() {
        @Override
        public JobEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
            JobEntity entity = new JobEntity();
            entity.setId(rs.getObject("id", UUID.class));
            entity.setUserId(rs.getObject("user_id", UUID.class));
            entity.setQueue(rs.getString("queue"));
            entity.setType(rs.getString("type"));
            entity.setPayload(rs.getString("payload"));
            entity.setBody(rs.getString("body"));
            entity.setStatus(JobStatus.valueOf(rs.getString("status")));
            entity.setPriority(rs.getInt("priority"));
            entity.setRunAt(rs.getTimestamp("run_at").toInstant());
            entity.setAttempts(rs.getInt("attempts"));
            entity.setMaxAttempts(rs.getInt("max_attempts"));
            entity.setLockedBy(rs.getString("locked_by"));
            if (rs.getTimestamp("locked_until") != null) {
                entity.setLockedUntil(rs.getTimestamp("locked_until").toInstant());
            }
            entity.setLastError(rs.getString("last_error"));
            entity.setIdempotencyKey(rs.getString("idempotency_key"));
            entity.setRequestHash(rs.getString("request_hash"));
            entity.setCreatedAt(rs.getTimestamp("created_at").toInstant());
            entity.setUpdatedAt(rs.getTimestamp("updated_at").toInstant());
            return entity;
        }
    };

    /**
     * Atomically claims up to :limit due jobs for the specified worker using SKIP LOCKED.
     * All time evaluation occurs in PostgreSQL via now().
     *
     * @param workerId unique identifier of the claiming worker
     * @param leaseDurationSeconds lease duration in seconds
     * @param limit maximum number of jobs to claim
     * @return list of claimed JobEntity records, sorted by priority DESC, run_at ASC
     */
    public List<JobEntity> claimJobs(String workerId, int leaseDurationSeconds, int limit) {
        String sql = """
            UPDATE jobs
            SET status = 'RUNNING',
                locked_by = ?,
                locked_until = now() + (? * INTERVAL '1 second'),
                attempts = attempts + 1,
                updated_at = now()
            WHERE id IN (
                SELECT id FROM jobs
                WHERE status = 'PENDING'
                  AND run_at <= now()
                ORDER BY priority DESC, run_at ASC
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            """;

        List<JobEntity> claimed = jdbcTemplate.query(
            sql,
            JOB_ROW_MAPPER,
            workerId,
            leaseDurationSeconds,
            limit
        );

        // PostgreSQL RETURNING does not guarantee ordering; sort in Java
        claimed.sort(
            Comparator.comparingInt(JobEntity::getPriority).reversed()
                      .thenComparing(JobEntity::getRunAt)
        );

        return claimed;
    }

    /**
     * Atomically marks a job as SUCCEEDED with fencing guard.
     *
     * @param jobId job ID
     * @param workerId worker ID holding the lease
     * @return true if write succeeded, false if fencing rejected the write (0 rows updated)
     */
    public boolean markSuccess(UUID jobId, String workerId) {
        String sql = """
            UPDATE jobs
            SET status = 'SUCCEEDED',
                last_error = NULL,
                locked_by = NULL,
                locked_until = NULL,
                updated_at = now()
            WHERE id = ?
              AND locked_by = ?
              AND status = 'RUNNING'
            """;

        int rows = jdbcTemplate.update(sql, jobId, workerId);
        if (rows == 0) {
            log.warn("FENCING FAILURE: Unable to mark job {} SUCCEEDED by worker {}. " +
                     "Status is no longer RUNNING or locked_by has been reassigned.", jobId, workerId);
            return false;
        }
        return true;
    }

    /**
     * Reschedules a job for retry with exponential backoff and fencing guard.
     *
     * @param jobId job ID
     * @param workerId worker ID holding the lease
     * @param delaySeconds delay in seconds before next execution (run_at = now() + delay)
     * @param error error message from the failed attempt
     * @return true if write succeeded, false if fencing rejected the write (0 rows updated)
     */
    public boolean rescheduleRetry(UUID jobId, String workerId, long delaySeconds, String error) {
        String sql = """
            UPDATE jobs
            SET status = 'PENDING',
                run_at = now() + (? * INTERVAL '1 second'),
                locked_by = NULL,
                locked_until = NULL,
                last_error = ?,
                updated_at = now()
            WHERE id = ?
              AND locked_by = ?
              AND status = 'RUNNING'
            """;

        int rows = jdbcTemplate.update(sql, delaySeconds, error, jobId, workerId);
        if (rows == 0) {
            log.warn("FENCING FAILURE: Unable to reschedule retry for job {} by worker {}. " +
                     "Status is no longer RUNNING or locked_by has been reassigned.", jobId, workerId);
            return false;
        }
        return true;
    }

    /**
     * Transitions a job to DEAD (DLQ) with fencing guard.
     *
     * @param jobId job ID
     * @param workerId worker ID holding the lease
     * @param error diagnostic failure message
     * @return true if write succeeded, false if fencing rejected the write (0 rows updated)
     */
    public boolean markDead(UUID jobId, String workerId, String error) {
        String sql = """
            UPDATE jobs
            SET status = 'DEAD',
                locked_by = NULL,
                locked_until = NULL,
                last_error = ?,
                updated_at = now()
            WHERE id = ?
              AND locked_by = ?
              AND status = 'RUNNING'
            """;

        int rows = jdbcTemplate.update(sql, error, jobId, workerId);
        if (rows == 0) {
            log.warn("FENCING FAILURE: Unable to mark job {} DEAD by worker {}. " +
                     "Status is no longer RUNNING or locked_by has been reassigned.", jobId, workerId);
            return false;
        }
        return true;
    }

    /**
     * Extends a worker's active lease (heartbeat) with fencing guard.
     *
     * @param jobId job ID
     * @param workerId worker ID holding the lease
     * @param leaseDurationSeconds seconds to extend from now()
     * @return true if lease extended, false if job is no longer RUNNING or locked by another worker
     */
    public boolean extendLease(UUID jobId, String workerId, int leaseDurationSeconds) {
        String sql = """
            UPDATE jobs
            SET locked_until = now() + (? * INTERVAL '1 second'),
                updated_at = now()
            WHERE id = ?
              AND locked_by = ?
              AND status = 'RUNNING'
            """;

        int rows = jdbcTemplate.update(sql, leaseDurationSeconds, jobId, workerId);
        return rows > 0;
    }

    /**
     * Atomically marks a job as FAILED with fencing guard (backward compatibility).
     *
     * @param jobId job ID
     * @param workerId worker ID holding the lease
     * @param error error message
     * @return true if write succeeded, false if fencing rejected the write (0 rows updated)
     */
    public boolean markFailed(UUID jobId, String workerId, String error) {
        return markDead(jobId, workerId, error);
    }

    /**
     * Sweeps orphaned RUNNING jobs whose locked_until has expired.
     * Uses the partial index idx_jobs_reaper ((locked_until) WHERE status = 'RUNNING')
     * and FOR UPDATE SKIP LOCKED to allow concurrent multi-instance sweeping.
     *
     * @param limit maximum number of orphaned jobs to sweep in this pass
     * @return count of successfully recovered/dead jobs
     */
    @org.springframework.transaction.annotation.Transactional
    public int reapOrphanedJobs(int limit) {
        String selectSql = """
            SELECT id, attempts, max_attempts
            FROM jobs
            WHERE status = 'RUNNING'
              AND locked_until < now()
            ORDER BY locked_until ASC
            LIMIT ?
            FOR UPDATE SKIP LOCKED
            """;

        List<OrphanedJob> orphans = jdbcTemplate.query(selectSql, (rs, rowNum) -> new OrphanedJob(
            rs.getObject("id", UUID.class),
            rs.getInt("attempts"),
            rs.getInt("max_attempts")
        ), limit);

        if (orphans.isEmpty()) {
            return 0;
        }

        int reaped = 0;
        for (OrphanedJob orphan : orphans) {
            if (orphan.attempts < orphan.maxAttempts) {
                // Requeue to PENDING for retry
                int rows = jdbcTemplate.update("""
                    UPDATE jobs
                    SET status = 'PENDING',
                        run_at = now(),
                        locked_by = NULL,
                        locked_until = NULL,
                        last_error = 'Lease expired: worker crashed or unresponsive',
                        updated_at = now()
                    WHERE id = ? AND status = 'RUNNING'
                    """, orphan.id);
                if (rows > 0) {
                    reaped++;
                    log.warn("LEASE REAPER: Orphaned job {} re-queued to PENDING (attempt {}/{})",
                        orphan.id, orphan.attempts, orphan.maxAttempts);
                }
            } else {
                // Max attempts reached: transition to DEAD (DLQ)
                int rows = jdbcTemplate.update("""
                    UPDATE jobs
                    SET status = 'DEAD',
                        locked_by = NULL,
                        locked_until = NULL,
                        last_error = 'Lease expired and max attempts reached (' || max_attempts || ')',
                        updated_at = now()
                    WHERE id = ? AND status = 'RUNNING'
                    """, orphan.id);
                if (rows > 0) {
                    reaped++;
                    log.error("LEASE REAPER: Orphaned job {} exceeded max attempts ({}), transitioned to DEAD",
                        orphan.id, orphan.maxAttempts);
                }
            }
        }
        return reaped;
    }

    private record OrphanedJob(UUID id, int attempts, int maxAttempts) {}
}
