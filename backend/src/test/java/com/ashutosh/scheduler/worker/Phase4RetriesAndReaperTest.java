package com.ashutosh.scheduler.worker;

import com.ashutosh.scheduler.TestcontainersConfiguration;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import com.ashutosh.scheduler.worker.engine.BackoffCalculator;
import com.ashutosh.scheduler.worker.engine.HeartbeatManager;
import com.ashutosh.scheduler.worker.engine.JobClaimRepository;
import com.ashutosh.scheduler.worker.engine.JobWorkerEngine;
import com.ashutosh.scheduler.worker.engine.LeaseReaper;
import com.ashutosh.scheduler.worker.engine.WorkerProperties;
import com.ashutosh.scheduler.worker.handler.JobExecutionResult;
import com.ashutosh.scheduler.worker.handler.JobHandler;
import com.ashutosh.scheduler.worker.handler.JobHandlerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@ActiveProfiles("test")
class Phase4RetriesAndReaperTest {

    @Autowired
    private JobClaimRepository claimRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private WorkerProperties workerProperties;

    @Autowired
    private LeaseReaper leaseReaper;

    @Autowired
    private BackoffCalculator backoffCalculator;

    @Autowired
    private HeartbeatManager heartbeatManager;

    private UserEntity testUser;

    @BeforeEach
    void setup() {
        jobRepository.deleteAll();
        userRepository.deleteAll();

        testUser = new UserEntity(UuidV7.generate(), "phase4-test-user");
        userRepository.save(testUser);
    }

    @Test
    @DisplayName("Retryable failure when attempts < max_attempts must reschedule to PENDING with future run_at")
    void testRetryableFailureReschedulesWithBackoff() {
        UUID jobId = UuidV7.generate();
        insertJob(jobId, "RETRYABLE_TEST", 0, 3, Instant.now().minusSeconds(5));

        JobHandler retryHandler = new JobHandler() {
            @Override
            public String type() { return "RETRYABLE_TEST"; }

            @Override
            public JobExecutionResult execute(JobEntity job) {
                return JobExecutionResult.retryableFailure("Simulated network blip");
            }
        };

        JobWorkerEngine engine = new JobWorkerEngine(
            claimRepository, new JobHandlerRegistry(List.of(retryHandler)),
            workerProperties, backoffCalculator, heartbeatManager
        );

        int dispatched = engine.pollAndDispatch();
        assertThat(dispatched).isEqualTo(1);

        awaitJobStatus(jobId, JobStatus.PENDING);

        JobEntity entity = jobRepository.findById(jobId).orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(entity.getAttempts()).isEqualTo(1);
        assertThat(entity.getMaxAttempts()).isEqualTo(3);
        assertThat(entity.getLockedBy()).isNull();
        assertThat(entity.getLockedUntil()).isNull();
        assertThat(entity.getLastError()).contains("Simulated network blip");
        // run_at must be strictly in the future
        assertThat(entity.getRunAt()).isAfter(Instant.now());
    }

    @Test
    @DisplayName("Retry exhaustion: failing when attempts >= max_attempts must transition to DEAD (DLQ)")
    void testRetryExhaustionTransitionsToDead() {
        UUID jobId = UuidV7.generate();
        // Insert with attempts=1, max_attempts=2
        insertJobWithAttempts(jobId, "EXHAUST_TEST", 0, 1, 2, Instant.now().minusSeconds(5));

        JobHandler exhaustHandler = new JobHandler() {
            @Override
            public String type() { return "EXHAUST_TEST"; }

            @Override
            public JobExecutionResult execute(JobEntity job) {
                return JobExecutionResult.retryableFailure("Downstream 503 Service Unavailable");
            }
        };

        JobWorkerEngine engine = new JobWorkerEngine(
            claimRepository, new JobHandlerRegistry(List.of(exhaustHandler)),
            workerProperties, backoffCalculator, heartbeatManager
        );

        int dispatched = engine.pollAndDispatch();
        assertThat(dispatched).isEqualTo(1);

        awaitJobStatus(jobId, JobStatus.DEAD);

        JobEntity entity = jobRepository.findById(jobId).orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(JobStatus.DEAD);
        assertThat(entity.getAttempts()).isEqualTo(2); // increments during claim
        assertThat(entity.getLockedBy()).isNull();
        assertThat(entity.getLockedUntil()).isNull();
        assertThat(entity.getLastError()).contains("Exhausted max attempts (2)");
    }

    @Test
    @DisplayName("Permanent failure must transition directly to DEAD without wasting retries")
    void testPermanentFailureTransitionsImmediatelyToDead() {
        UUID jobId = UuidV7.generate();
        insertJob(jobId, "PERMANENT_TEST", 0, 5, Instant.now().minusSeconds(5));

        JobHandler permHandler = new JobHandler() {
            @Override
            public String type() { return "PERMANENT_TEST"; }

            @Override
            public JobExecutionResult execute(JobEntity job) {
                return JobExecutionResult.permanentFailure("400 Bad Request: Invalid JSON Schema");
            }
        };

        JobWorkerEngine engine = new JobWorkerEngine(
            claimRepository, new JobHandlerRegistry(List.of(permHandler)),
            workerProperties, backoffCalculator, heartbeatManager
        );

        int dispatched = engine.pollAndDispatch();
        assertThat(dispatched).isEqualTo(1);

        awaitJobStatus(jobId, JobStatus.DEAD);

        JobEntity entity = jobRepository.findById(jobId).orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(JobStatus.DEAD);
        assertThat(entity.getAttempts()).isEqualTo(1); // Failed on attempt 1 of 5
        assertThat(entity.getLockedBy()).isNull();
        assertThat(entity.getLockedUntil()).isNull();
        assertThat(entity.getLastError()).contains("Permanent failure: 400 Bad Request");
    }

    @Test
    @DisplayName("Lease Reaper: Crashed worker job with attempts < max_attempts must be re-queued to PENDING")
    void testLeaseReaperRecoversCrashedWorkerJob() {
        UUID jobId = UuidV7.generate();
        // Insert orphaned RUNNING job locked in the past
        Instant tenMinutesAgo = Instant.now().minusSeconds(600);
        jdbcTemplate.update("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, attempts, max_attempts, locked_by, locked_until)
            VALUES (?, ?, 'default', 'HTTP', '{"url":"https://example.com"}'::jsonb, 'body', 'RUNNING', 0, ?, 1, 3, 'crashed-worker-ghost', ?)
            """, jobId, testUser.getId(), Timestamp.from(tenMinutesAgo), Timestamp.from(tenMinutesAgo));

        int reaped = leaseReaper.reapOnce();
        assertThat(reaped).isEqualTo(1);

        JobEntity entity = jobRepository.findById(jobId).orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(entity.getLockedBy()).isNull();
        assertThat(entity.getLockedUntil()).isNull();
        assertThat(entity.getLastError()).contains("Lease expired: worker crashed or unresponsive");
        assertThat(entity.getRunAt()).isBeforeOrEqualTo(Instant.now());
    }

    @Test
    @DisplayName("Lease Reaper: Crashed worker job with attempts >= max_attempts must transition to DEAD (DLQ)")
    void testLeaseReaperMarksExhaustedJobDead() {
        UUID jobId = UuidV7.generate();
        // Insert orphaned RUNNING job whose attempts == max_attempts
        Instant tenMinutesAgo = Instant.now().minusSeconds(600);
        jdbcTemplate.update("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, attempts, max_attempts, locked_by, locked_until)
            VALUES (?, ?, 'default', 'HTTP', '{"url":"https://example.com"}'::jsonb, 'body', 'RUNNING', 0, ?, 3, 3, 'crashed-worker-ghost', ?)
            """, jobId, testUser.getId(), Timestamp.from(tenMinutesAgo), Timestamp.from(tenMinutesAgo));

        int reaped = leaseReaper.reapOnce();
        assertThat(reaped).isEqualTo(1);

        JobEntity entity = jobRepository.findById(jobId).orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(JobStatus.DEAD);
        assertThat(entity.getLockedBy()).isNull();
        assertThat(entity.getLockedUntil()).isNull();
        assertThat(entity.getLastError()).contains("Lease expired and max attempts reached (3)");
    }

    @Test
    @DisplayName("Heartbeat: extendLease atomically renews locked_until into the future for active worker")
    void testHeartbeatExtendsLease() {
        UUID jobId = UuidV7.generate();
        insertJob(jobId, "HTTP", 0, 3, Instant.now().minusSeconds(5));

        // Claim job for 10 seconds
        List<JobEntity> claimed = claimRepository.claimJobs("worker-heartbeat", 10, 1);
        assertThat(claimed).hasSize(1);
        Instant initialLockedUntil = claimed.get(0).getLockedUntil();

        // Perform heartbeat extension by 60 seconds
        boolean extended = claimRepository.extendLease(jobId, "worker-heartbeat", 60);
        assertThat(extended).isTrue();

        JobEntity updated = jobRepository.findById(jobId).orElseThrow();
        assertThat(updated.getLockedUntil()).isAfter(initialLockedUntil);
    }

    @Test
    @DisplayName("Graceful shutdown: in-flight jobs complete and write status before executor terminates")
    void testGracefulShutdownCompletesInFlightJob() throws Exception {
        UUID jobId = UuidV7.generate();
        insertJob(jobId, "SLOW_JOB", 0, 3, Instant.now().minusSeconds(5));

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);

        JobHandler slowHandler = new JobHandler() {
            @Override
            public String type() { return "SLOW_JOB"; }

            @Override
            public JobExecutionResult execute(JobEntity job) {
                taskStarted.countDown();
                try {
                    Thread.sleep(150); // Simulates in-flight work
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                taskFinished.countDown();
                return JobExecutionResult.success("Slow job finished cleanly");
            }
        };

        JobWorkerEngine engine = new JobWorkerEngine(
            claimRepository, new JobHandlerRegistry(List.of(slowHandler)),
            workerProperties, backoffCalculator, heartbeatManager
        );

        engine.pollAndDispatch();

        // Wait until task starts executing
        assertThat(taskStarted.await(2, TimeUnit.SECONDS)).isTrue();

        // Trigger graceful shutdown while task is in flight
        engine.stop();

        // Task must finish cleanly
        assertThat(taskFinished.await(2, TimeUnit.SECONDS)).isTrue();

        awaitJobStatus(jobId, JobStatus.SUCCEEDED);
        JobEntity entity = jobRepository.findById(jobId).orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
    }

    private void insertJob(UUID jobId, String type, int priority, int maxAttempts, Instant runAt) {
        jdbcTemplate.update("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, attempts, max_attempts)
            VALUES (?, ?, 'default', ?, '{"url":"https://example.com"}'::jsonb, 'body', 'PENDING', ?, ?, 0, ?)
            """, jobId, testUser.getId(), type, priority, Timestamp.from(runAt), maxAttempts);
    }

    private void insertJobWithAttempts(UUID jobId, String type, int priority, int attempts, int maxAttempts, Instant runAt) {
        jdbcTemplate.update("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, attempts, max_attempts)
            VALUES (?, ?, 'default', ?, '{"url":"https://example.com"}'::jsonb, 'body', 'PENDING', ?, ?, ?, ?)
            """, jobId, testUser.getId(), type, priority, Timestamp.from(runAt), attempts, maxAttempts);
    }

    private void awaitJobStatus(UUID jobId, JobStatus expectedStatus) {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            JobEntity job = jobRepository.findById(jobId).orElse(null);
            if (job != null && job.getStatus() == expectedStatus) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {}
        }
    }
}
