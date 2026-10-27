package com.ashutosh.scheduler.worker;

import com.ashutosh.scheduler.TestcontainersConfiguration;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import com.ashutosh.scheduler.worker.engine.JobClaimRepository;
import com.ashutosh.scheduler.worker.engine.JobWorkerEngine;
import com.ashutosh.scheduler.worker.engine.WorkerProperties;
import com.ashutosh.scheduler.worker.handler.JobHandlerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import com.ashutosh.scheduler.worker.handler.JobExecutionResult;
import com.ashutosh.scheduler.worker.handler.JobHandler;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@ActiveProfiles("test")
class JobWorkerEngineTest {

    private static final Logger log = LoggerFactory.getLogger(JobWorkerEngineTest.class);

    @Autowired
    private JobClaimRepository claimRepository;

    @Autowired
    private JobHandlerRegistry handlerRegistry;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private WorkerProperties workerProperties;

    private UserEntity testUser;

    @BeforeEach
    void setup() {
        jobRepository.deleteAll();
        userRepository.deleteAll();

        testUser = new UserEntity(UuidV7.generate(), "worker-test-user");
        userRepository.save(testUser);
    }

    @Test
    @DisplayName("EXPLAIN: Claim query subselect must use the partial claim index idx_jobs_claim")
    void testExplainClaimQueryUsesPartialIndex() {
        // Seed enough rows to encourage PostgreSQL optimizer to use indexes
        List<Object[]> batch = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            batch.add(new Object[]{
                UuidV7.generate(), testUser.getId(), "default", "ECHO",
                "{\"url\":\"https://example.com\"}", "body " + i,
                "PENDING", i % 10, Timestamp.from(Instant.now().minusSeconds(10)), 3
            });
        }
        jdbcTemplate.batchUpdate("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, max_attempts)
            VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?)
            """, batch);

        String explainSql = """
            EXPLAIN
            SELECT id FROM jobs
            WHERE status = 'PENDING'
              AND run_at <= now()
            ORDER BY priority DESC, run_at ASC
            LIMIT 10
            FOR UPDATE SKIP LOCKED
            """;

        List<String> planLines = jdbcTemplate.queryForList(explainSql, String.class);
        String fullPlan = String.join("\n", planLines);

        log.info("EXPLAIN Query Plan:\n{}", fullPlan);

        // Verify that the query plan explicitly utilizes idx_jobs_claim
        assertThat(fullPlan).contains("idx_jobs_claim");
    }

    @Test
    @DisplayName("Priority and run_at ordering must be strictly respected, future run_at ignored")
    void testPriorityAndRunAtOrdering() {
        UUID jobLow = UuidV7.generate();
        UUID jobMid = UuidV7.generate();
        UUID jobHigh = UuidV7.generate();
        UUID jobFuture = UuidV7.generate();

        Instant past = Instant.now().minus(10, ChronoUnit.SECONDS);
        Instant future = Instant.now().plus(1, ChronoUnit.HOURS);

        // Low priority (0)
        insertJob(jobLow, "ECHO", 0, past);
        // High priority (10)
        insertJob(jobHigh, "ECHO", 10, past);
        // Mid priority (5)
        insertJob(jobMid, "ECHO", 5, past);
        // Future job (priority 10, but in future)
        insertJob(jobFuture, "ECHO", 10, future);

        List<JobEntity> claimed = claimRepository.claimJobs("test-worker", 30, 10);

        // Exactly 3 claimed; future job MUST NOT be claimed
        assertThat(claimed).hasSize(3);
        assertThat(claimed.get(0).getId()).isEqualTo(jobHigh);
        assertThat(claimed.get(1).getId()).isEqualTo(jobMid);
        assertThat(claimed.get(2).getId()).isEqualTo(jobLow);

        // Verify future job is still PENDING
        JobEntity futureJob = jobRepository.findById(jobFuture).orElseThrow();
        assertThat(futureJob.getStatus()).isEqualTo(JobStatus.PENDING);
    }

    @Test
    @DisplayName("Fencing: A stale worker's completion write must be rejected (0 rows updated)")
    void testFencingRejectsStaleWorkerWrite() {
        UUID jobId = UuidV7.generate();
        insertJob(jobId, "ECHO", 0, Instant.now().minusSeconds(10));

        // Worker 1 claims the job
        List<JobEntity> claimed = claimRepository.claimJobs("worker-1", 30, 1);
        assertThat(claimed).hasSize(1);

        // Simulate lease reassignment / takeover by Worker 2
        jdbcTemplate.update(
            "UPDATE jobs SET locked_by = 'worker-2' WHERE id = ?",
            jobId
        );

        // Worker 1 attempts to write success -> MUST BE REJECTED BY FENCING
        boolean success = claimRepository.markSuccess(jobId, "worker-1");
        assertThat(success).isFalse();

        // Worker 1 attempts to write failure -> MUST BE REJECTED BY FENCING
        boolean failure = claimRepository.markFailed(jobId, "worker-1", "stale error");
        assertThat(failure).isFalse();

        // Job in DB must still be locked by worker-2
        JobEntity entity = jobRepository.findById(jobId).orElseThrow();
        assertThat(entity.getLockedBy()).isEqualTo("worker-2");
        assertThat(entity.getStatus()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    @DisplayName("Unknown job type must be handled cleanly as permanent failure without crashing (transition to DEAD)")
    void testUnknownJobTypeHandledCleanly() {
        UUID jobId = UuidV7.generate();
        insertJob(jobId, "UNKNOWN_MYSTERY_TYPE", 0, Instant.now().minusSeconds(10));

        JobWorkerEngine engine = new JobWorkerEngine(claimRepository, handlerRegistry, workerProperties);
        int dispatched = engine.pollAndDispatch();
        assertThat(dispatched).isEqualTo(1);

        // Wait briefly for virtual thread execution
        awaitJobStatus(jobId, JobStatus.DEAD);

        JobEntity entity = jobRepository.findById(jobId).orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(JobStatus.DEAD);
        assertThat(entity.getLastError()).contains("Unknown job type: 'UNKNOWN_MYSTERY_TYPE'");
    }

    @Test
    @DisplayName("Concurrency: 1000 jobs, 5 concurrent workers: record executions per job id with random durations; assert exactly once")
    void testConcurrency1000Jobs5Workers() throws Exception {
        int totalJobs = 1000;
        int numWorkers = 5;

        // Map to record executions per job ID
        ConcurrentHashMap<UUID, AtomicInteger> executionCounts = new ConcurrentHashMap<>();

        // Custom test handler with random durations (1-15ms) and per-job execution recording
        JobHandler concurrencyHandler = new JobHandler() {
            @Override
            public String type() {
                return "CONCURRENCY_TEST";
            }

            @Override
            public JobExecutionResult execute(JobEntity job) {
                executionCounts.computeIfAbsent(job.getId(), k -> new AtomicInteger(0)).incrementAndGet();
                try {
                    // Random handler duration simulating real-world network fluctuations
                    Thread.sleep(ThreadLocalRandom.current().nextInt(1, 16));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return JobExecutionResult.success("Done");
            }
        };

        JobHandlerRegistry customRegistry = new JobHandlerRegistry(List.of(concurrencyHandler));

        // Batch insert 1,000 PENDING jobs
        List<Object[]> batch = new ArrayList<>(totalJobs);
        for (int i = 0; i < totalJobs; i++) {
            batch.add(new Object[]{
                UuidV7.generate(), testUser.getId(), "default", "CONCURRENCY_TEST",
                "{\"url\":\"https://example.com\"}", "Job payload " + i,
                "PENDING", i % 10, Timestamp.from(Instant.now().minusSeconds(5)), 3
            });
        }
        jdbcTemplate.batchUpdate("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, max_attempts)
            VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?)
            """, batch);

        // Initialize 5 distinct worker engines, each with its own unique workerId
        List<JobWorkerEngine> workers = new ArrayList<>(numWorkers);
        WorkerProperties props = new WorkerProperties();
        props.setConcurrency(20);
        props.setBatchSize(10);
        props.setLeaseDurationSeconds(60);

        for (int i = 0; i < numWorkers; i++) {
            workers.add(new JobWorkerEngine(claimRepository, customRegistry, props));
        }

        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger totalProcessed = new AtomicInteger(0);

        // Launch 5 worker loops polling simultaneously
        for (JobWorkerEngine worker : workers) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (totalProcessed.get() < totalJobs) {
                        int claimed = worker.pollAndDispatch();
                        if (claimed == 0) {
                            Thread.sleep(10);
                        } else {
                            totalProcessed.addAndGet(claimed);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Release all 5 workers simultaneously
        startLatch.countDown();

        // Wait until all 1,000 jobs reach SUCCEEDED status
        long deadline = System.currentTimeMillis() + 45_000; // 45 second timeout
        while (System.currentTimeMillis() < deadline) {
            Integer succeeded = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE status = 'SUCCEEDED'", Integer.class
            );
            if (succeeded != null && succeeded == totalJobs) {
                break;
            }
            Thread.sleep(100);
        }

        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // ASSERTIONS:
        // 1. Every single job succeeded in DB
        Integer succeededCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM jobs WHERE status = 'SUCCEEDED'", Integer.class
        );
        assertThat(succeededCount).isEqualTo(totalJobs);

        // 2. Exactly zero jobs failed or remain pending
        Integer nonSucceeded = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM jobs WHERE status != 'SUCCEEDED'", Integer.class
        );
        assertThat(nonSucceeded).isEqualTo(0);

        // 3. ZERO jobs claimed or attempted more than once in DB
        Integer multiAttemptCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM jobs WHERE attempts > 1", Integer.class
        );
        assertThat(multiAttemptCount)
            .as("No job should ever be claimed or processed more than once under concurrency")
            .isEqualTo(0);

        // 4. In-memory execution map: all 1,000 jobs executed EXACTLY ONCE
        assertThat(executionCounts)
            .as("Every one of the 1,000 jobs must have an execution recorded")
            .hasSize(totalJobs);

        executionCounts.forEach((jobId, count) -> {
            assertThat(count.get())
                .as("Job %s must be executed exactly once", jobId)
                .isEqualTo(1);
        });
    }

    @Test
    @DisplayName("EXPLAIN (ANALYZE, BUFFERS): 100k+ seeded rows with future-dated high-priority jobs to analyze skip-scan behavior")
    void testExplainAnalyzeBuffers100kRows() {
        // 1. Seed 100,000 future-dated priority 10 jobs using PostgreSQL generate_series
        jdbcTemplate.execute("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, attempts, max_attempts, created_at, updated_at)
            SELECT
                gen_random_uuid(),
                '""" + testUser.getId() + """
            ',
                'default',
                'HTTP',
                '{"url":"https://example.com"}'::jsonb,
                'future body',
                'PENDING',
                10,
                now() + INTERVAL '1 hour',
                0,
                3,
                now(),
                now()
            FROM generate_series(1, 100000);
            """);

        // 2. Seed 10 due jobs with lower priority 1
        jdbcTemplate.execute("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, attempts, max_attempts, created_at, updated_at)
            SELECT
                gen_random_uuid(),
                '""" + testUser.getId() + """
            ',
                'default',
                'HTTP',
                '{"url":"https://example.com"}'::jsonb,
                'due body',
                'PENDING',
                1,
                now() - INTERVAL '10 seconds',
                0,
                3,
                now(),
                now()
            FROM generate_series(1, 10);
            """);

        // Recompute statistics for query optimizer
        jdbcTemplate.execute("ANALYZE jobs");

        String explainSql = """
            EXPLAIN (ANALYZE, BUFFERS)
            SELECT id FROM jobs
            WHERE status = 'PENDING'
              AND run_at <= now()
            ORDER BY priority DESC, run_at ASC
            LIMIT 10
            FOR UPDATE SKIP LOCKED;
            """;

        List<String> planLines = jdbcTemplate.queryForList(explainSql, String.class);
        String fullPlan = String.join("\n", planLines);

        log.info("EXPLAIN (ANALYZE, BUFFERS) 100k rows:\n{}", fullPlan);
        System.out.println("=== EXPLAIN (ANALYZE, BUFFERS) ON 100K ROWS ===");
        System.out.println(fullPlan);
        System.out.println("================================================");

        assertThat(fullPlan).contains("idx_jobs_claim");
    }

    private void insertJob(UUID jobId, String type, int priority, Instant runAt) {
        jdbcTemplate.update("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, max_attempts)
            VALUES (?, ?, 'default', ?, '{"url":"https://example.com"}'::jsonb, 'test-body', 'PENDING', ?, ?, 3)
            """, jobId, testUser.getId(), type, priority, Timestamp.from(runAt));
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
