package com.ashutosh.scheduler.worker.engine;

import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.worker.handler.JobExecutionResult;
import com.ashutosh.scheduler.worker.handler.JobHandler;
import com.ashutosh.scheduler.worker.handler.JobHandlerRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker engine that coordinates the atomic claim loop and job execution.
 *
 * Concurrency is bounded by a Semaphore, dispatching claimed jobs to Java 21
 * Virtual Threads. Database connections are NEVER held while the handler executes.
 */
@Component
public class JobWorkerEngine {

    private static final Logger log = LoggerFactory.getLogger(JobWorkerEngine.class);

    private final JobClaimRepository claimRepository;
    private final JobHandlerRegistry handlerRegistry;
    private final WorkerProperties properties;
    private final BackoffCalculator backoffCalculator;
    private final HeartbeatManager heartbeatManager;

    private final String workerId;
    private final Semaphore semaphore;
    private final ExecutorService taskExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread pollerThread;

    @org.springframework.beans.factory.annotation.Autowired
    public JobWorkerEngine(JobClaimRepository claimRepository,
                           JobHandlerRegistry handlerRegistry,
                           WorkerProperties properties,
                           BackoffCalculator backoffCalculator,
                           HeartbeatManager heartbeatManager) {
        this.claimRepository = claimRepository;
        this.handlerRegistry = handlerRegistry;
        this.properties = properties;
        this.backoffCalculator = backoffCalculator != null ? backoffCalculator : new BackoffCalculator(properties);
        this.heartbeatManager = heartbeatManager != null ? heartbeatManager : new HeartbeatManager(claimRepository, properties);

        this.workerId = generateWorkerId();
        this.semaphore = new Semaphore(properties.getConcurrency());
        this.taskExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public JobWorkerEngine(JobClaimRepository claimRepository,
                           JobHandlerRegistry handlerRegistry,
                           WorkerProperties properties) {
        this(claimRepository, handlerRegistry, properties, new BackoffCalculator(properties), new HeartbeatManager(claimRepository, properties));
    }

    @PostConstruct
    public void start() {
        if (!properties.isEnabled()) {
            log.info("JobWorkerEngine [{}] is disabled via configuration.", workerId);
            return;
        }

        if (running.compareAndSet(false, true)) {
            log.info("Starting JobWorkerEngine [{}] with max concurrency={}", workerId, properties.getConcurrency());
            pollerThread = new Thread(this::pollLoop, "job-poller-" + workerId);
            pollerThread.setDaemon(true);
            pollerThread.start();
        }
    }

    @PreDestroy
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping JobWorkerEngine [{}] gracefully...", workerId);
            if (pollerThread != null) {
                pollerThread.interrupt();
            }
            taskExecutor.shutdown();
            try {
                int timeout = properties.getShutdownTimeoutSeconds();
                if (!taskExecutor.awaitTermination(timeout, TimeUnit.SECONDS)) {
                    log.warn("JobWorkerEngine [{}] tasks did not finish within {}s. Forcing shutdownNow...", workerId, timeout);
                    taskExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                taskExecutor.shutdownNow();
            }
            log.info("JobWorkerEngine [{}] stopped cleanly.", workerId);
        }
    }

    /**
     * Continuous background polling loop with exponential backoff on empty queue and errors.
     */
    private void pollLoop() {
        long currentSleepMs = properties.getPollIntervalMs();

        while (running.get()) {
            try {
                int claimedCount = pollAndDispatch();
                if (claimedCount > 0) {
                    currentSleepMs = properties.getPollIntervalMs();
                } else {
                    // Exponential backoff when queue is idle or worker is at capacity, avoiding hot-looping
                    currentSleepMs = Math.min(currentSleepMs * 2, properties.getMaxPollIntervalMs());
                    Thread.sleep(currentSleepMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                // Catches all exceptions, logs, and backs off exponentially on errors too
                log.error("Unexpected error in worker poll loop: {}", t.getMessage(), t);
                currentSleepMs = Math.min(Math.max(currentSleepMs, properties.getPollIntervalMs()) * 2, properties.getMaxPollIntervalMs());
                try {
                    Thread.sleep(currentSleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Executes a single claim-and-dispatch pass.
     *
     * Concurrency invariant:
     * 1. Acquire permits FIRST relative to batch size.
     * 2. Claim up to the acquired count from DB.
     * 3. Release unused permits immediately if DB returned fewer rows.
     * 4. Release permit in finally on every path including dispatch failure.
     *
     * @return number of jobs claimed and dispatched in this pass
     */
    public int pollAndDispatch() {
        int maxBatch = properties.getBatchSize();
        int acquired = 0;

        // 1. Acquire permits FIRST up to maxBatch
        while (acquired < maxBatch && semaphore.tryAcquire()) {
            acquired++;
        }

        if (acquired == 0) {
            return 0; // Worker is at capacity; do not poll DB
        }

        List<JobEntity> jobs;
        try {
            // 2. Claim up to acquired count from DB
            jobs = claimRepository.claimJobs(
                workerId,
                properties.getLeaseDurationSeconds(),
                acquired
            );
        } catch (Throwable t) {
            // DB claim failed: immediately release all acquired permits
            semaphore.release(acquired);
            throw t;
        }

        int claimedCount = jobs.size();

        // 3. If claimed fewer jobs than acquired permits, release unused permits immediately
        if (claimedCount < acquired) {
            semaphore.release(acquired - claimedCount);
        }

        if (claimedCount == 0) {
            return 0;
        }

        // 4. For each claimed job, dispatch to virtual thread executor
        for (JobEntity job : jobs) {
            boolean dispatched = false;
            try {
                taskExecutor.submit(() -> {
                    try {
                        processJob(job);
                    } finally {
                        semaphore.release();
                    }
                });
                dispatched = true;
            } finally {
                // If submit failed (e.g. RejectedExecutionException during shutdown),
                // release permit so it is never leaked!
                if (!dispatched) {
                    semaphore.release();
                }
            }
        }

        return claimedCount;
    }

    /**
     * Executes the job handler outside of any database transaction.
     * Protected by periodic heartbeat extension and fenced writes.
     */
    private void processJob(JobEntity job) {
        log.info("Processing job id={}, type={}, attempts={}", job.getId(), job.getType(), job.getAttempts());

        // Start periodic lease extension (heartbeat) while handler executes
        try (AutoCloseable heartbeat = heartbeatManager.startHeartbeat(job.getId(), workerId)) {
            Optional<JobHandler> handlerOpt = handlerRegistry.getHandler(job.getType());
            if (handlerOpt.isEmpty()) {
                // Unknown job type is a permanent failure; transitions directly to DEAD (DLQ)
                String errorMsg = "Unknown job type: '" + job.getType() + "'. No registered handler.";
                log.error(errorMsg);
                claimRepository.markDead(job.getId(), workerId, errorMsg);
                return;
            }

            JobHandler handler = handlerOpt.get();
            try {
                // Handler runs OUTSIDE any DB transaction (zero DB connections held during execution)
                JobExecutionResult result = handler.execute(job);

                switch (result) {
                    case JobExecutionResult.Success s -> {
                        log.info("Job {} executed successfully.", job.getId());
                        claimRepository.markSuccess(job.getId(), workerId);
                    }
                    case JobExecutionResult.RetryableFailure rf -> {
                        handleRetryableFailure(job, rf.error());
                    }
                    case JobExecutionResult.PermanentFailure pf -> {
                        log.error("Job {} failed permanently: {}", job.getId(), pf.error());
                        claimRepository.markDead(job.getId(), workerId, "Permanent failure: " + pf.error());
                    }
                }
            } catch (Exception e) {
                log.error("Uncaught exception during execution of job {}: {}", job.getId(), e.getMessage(), e);
                handleRetryableFailure(job, "Uncaught handler exception: " + e.getMessage());
            }
        } catch (Exception e) {
            log.warn("Error managing heartbeat for job {}: {}", job.getId(), e.getMessage());
        }
    }

    private void handleRetryableFailure(JobEntity job, String error) {
        if (job.getAttempts() < job.getMaxAttempts()) {
            long delay = backoffCalculator.calculateDelaySeconds(job.getAttempts());
            log.warn("Job {} failed (attempt {}/{}). Rescheduling in {}s. Error: {}",
                job.getId(), job.getAttempts(), job.getMaxAttempts(), delay, error);
            claimRepository.rescheduleRetry(job.getId(), workerId, delay, error);
        } else {
            log.error("Job {} exhausted all {} attempts. Transitioning to DEAD (DLQ). Error: {}",
                job.getId(), job.getMaxAttempts(), error);
            claimRepository.markDead(job.getId(), workerId,
                "Exhausted max attempts (" + job.getMaxAttempts() + "). Last error: " + error);
        }
    }

    private static String generateWorkerId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "worker";
        }
        return host + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public String getWorkerId() {
        return workerId;
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }
}
