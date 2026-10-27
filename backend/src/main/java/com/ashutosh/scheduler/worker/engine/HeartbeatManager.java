package com.ashutosh.scheduler.worker.engine;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Periodically extends locked_until for in-flight jobs to prevent the lease reaper
 * from prematurely reclaiming long-running HTTP executions.
 */
@Component
public class HeartbeatManager {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatManager.class);

    private final JobClaimRepository claimRepository;
    private final WorkerProperties properties;
    private final ScheduledExecutorService scheduler;
    private final ConcurrentHashMap<UUID, ScheduledFuture<?>> activeHeartbeats = new ConcurrentHashMap<>();

    public HeartbeatManager(JobClaimRepository claimRepository, WorkerProperties properties) {
        this.claimRepository = claimRepository;
        this.properties = properties;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "job-heartbeat-worker");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts periodic lease renewal for an executing job.
     * Returns an AutoCloseable so it can be used cleanly in try-with-resources.
     *
     * @param jobId job ID
     * @param workerId current worker holding the lease
     * @return AutoCloseable handle that stops the heartbeat on close
     */
    public AutoCloseable startHeartbeat(UUID jobId, String workerId) {
        int leaseSeconds = properties.getLeaseDurationSeconds();
        long intervalSeconds = Math.max(1, leaseSeconds / 3);

        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
            try {
                boolean extended = claimRepository.extendLease(jobId, workerId, leaseSeconds);
                if (!extended) {
                    log.debug("Heartbeat for job {} rejected (job finished or lease reassigned). Stopping heartbeat.", jobId);
                    stopHeartbeat(jobId);
                } else {
                    log.trace("Heartbeat: extended lease for job {} by {}s", jobId, leaseSeconds);
                }
            } catch (Exception e) {
                log.warn("Failed to extend lease for job {}: {}", jobId, e.getMessage());
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

        activeHeartbeats.put(jobId, future);

        return () -> stopHeartbeat(jobId);
    }

    public void stopHeartbeat(UUID jobId) {
        ScheduledFuture<?> future = activeHeartbeats.remove(jobId);
        if (future != null) {
            future.cancel(false);
        }
    }

    @PreDestroy
    public void shutdown() {
        activeHeartbeats.values().forEach(f -> f.cancel(true));
        activeHeartbeats.clear();
        scheduler.shutdownNow();
    }
}
