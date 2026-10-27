package com.ashutosh.scheduler.worker.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Background sweeper that reclaims orphaned jobs from crashed or unresponsive workers.
 *
 * Jobs in status RUNNING whose locked_until < now() are reclaimed:
 * - If attempts < max_attempts: re-queued to PENDING for retry
 * - If attempts >= max_attempts: transitioned to DEAD (Dead-Letter Queue)
 *
 * Queries leverage the partial index idx_jobs_reaper ((locked_until) WHERE status = 'RUNNING')
 * and FOR UPDATE SKIP LOCKED so multiple application instances can run the reaper concurrently
 * without lock contention or double-reclaiming.
 */
@Component
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final JobClaimRepository claimRepository;
    private final WorkerProperties properties;

    public LeaseReaper(JobClaimRepository claimRepository, WorkerProperties properties) {
        this.claimRepository = claimRepository;
        this.properties = properties;
    }

    /**
     * Executes a single sweep of expired leases.
     * Invoked periodically by Spring Scheduling and callable directly in tests.
     *
     * @return number of orphaned jobs reaped
     */
    @Scheduled(fixedDelayString = "${scheduler.worker.reaper-interval-ms:10000}")
    public int reapOnce() {
        if (!properties.isReaperEnabled()) {
            return 0;
        }

        try {
            int reaped = claimRepository.reapOrphanedJobs(properties.getReaperBatchSize());
            if (reaped > 0) {
                log.info("LeaseReaper sweep complete: {} orphaned jobs recovered.", reaped);
            }
            return reaped;
        } catch (Exception e) {
            log.error("Error occurred during LeaseReaper execution: {}", e.getMessage(), e);
            return 0;
        }
    }
}
