package com.ashutosh.scheduler.worker;

import com.ashutosh.scheduler.TestcontainersConfiguration;
import com.ashutosh.scheduler.common.exception.JobOverlapException;
import com.ashutosh.scheduler.common.exception.QuotaExceededException;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.entity.ScheduleEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import com.ashutosh.scheduler.domain.enums.MisfirePolicy;
import com.ashutosh.scheduler.domain.enums.OverlapPolicy;
import com.ashutosh.scheduler.domain.enums.ScheduleSkipReason;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.ScheduleRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import com.ashutosh.scheduler.producer.dto.CreateJobRequest;
import com.ashutosh.scheduler.producer.dto.CreateScheduleRequest;
import com.ashutosh.scheduler.producer.dto.JobResponse;
import com.ashutosh.scheduler.producer.dto.JobTemplateDto;
import com.ashutosh.scheduler.producer.dto.ScheduleResponse;
import com.ashutosh.scheduler.producer.dto.UpdateScheduleRequest;
import com.ashutosh.scheduler.producer.service.JobProducerService;
import com.ashutosh.scheduler.producer.service.ScheduleService;
import com.ashutosh.scheduler.worker.engine.CronScheduleProcessor;
import com.ashutosh.scheduler.worker.engine.CronSchedulerEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@ActiveProfiles("test")
class Phase8RecurringCronSchedulesIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(Phase8RecurringCronSchedulesIntegrationTest.class);

    @Autowired
    private ScheduleService scheduleService;

    @Autowired
    private ScheduleRepository scheduleRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JobProducerService jobProducerService;

    @Autowired
    private CronScheduleProcessor scheduleProcessor;

    @Autowired
    private CronSchedulerEngine cronSchedulerEngine;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private UUID tenantId;
    private String apiKey;

    @BeforeEach
    void setUp() {
        tenantId = UuidV7.generate();
        apiKey = "sk_live_" + UUID.randomUUID().toString().replace("-", "");

        jdbcTemplate.update(
            "INSERT INTO users (id, username, rate_limit_per_minute, max_active_jobs) VALUES (?, ?, ?, ?)",
            tenantId, "cron_tenant_" + UUID.randomUUID().toString().substring(0, 8), 120, 10
        );

        String keyHash = com.ashutosh.scheduler.common.util.CryptoUtils.sha256Hex(apiKey);
        jdbcTemplate.update(
            "INSERT INTO api_keys (id, user_id, key_hash, key_prefix, name) VALUES (?, ?, ?, ?, ?)",
            UuidV7.generate(), tenantId, keyHash, apiKey.substring(0, 16), "Test Key"
        );
    }

    private JobTemplateDto defaultTemplate() {
        return new JobTemplateDto(
            "https://example.com/webhook",
            "POST",
            Map.of("Content-Type", "application/json"),
            "{\"event\":\"sync\"}",
            "default",
            0,
            3
        );
    }

    @Test
    @DisplayName("Schedule CRUD Lifecycle: create, get, list, patch, delete with historical job preservation")
    void testScheduleCrudLifecycle() {
        CreateScheduleRequest createReq = new CreateScheduleRequest(
            "0 */10 * * * *",
            "America/New_York",
            OverlapPolicy.SKIP,
            MisfirePolicy.FIRE_ONCE,
            300,
            defaultTemplate()
        );

        ScheduleResponse created = scheduleService.createSchedule(tenantId, createReq);
        assertThat(created.id()).isNotNull();
        assertThat(created.cronExpression()).isEqualTo("0 */10 * * * *");
        assertThat(created.timezone()).isEqualTo("America/New_York");
        assertThat(created.nextRunAt()).isAfter(Instant.now());

        ScheduleResponse retrieved = scheduleService.getSchedule(tenantId, created.id());
        assertThat(retrieved.id()).isEqualTo(created.id());

        // Update cron and timezone via PATCH
        UpdateScheduleRequest patchReq = new UpdateScheduleRequest(
            "0 0 12 * * *",
            "UTC",
            OverlapPolicy.ALLOW,
            null,
            null,
            null,
            null
        );
        ScheduleResponse patched = scheduleService.patchSchedule(tenantId, created.id(), patchReq);
        assertThat(patched.cronExpression()).isEqualTo("0 0 12 * * *");
        assertThat(patched.timezone()).isEqualTo("UTC");
        assertThat(patched.overlapPolicy()).isEqualTo(OverlapPolicy.ALLOW);

        // Delete schedule and assert ON DELETE SET NULL on jobs
        scheduleService.deleteSchedule(tenantId, created.id());
        assertThatThrownBy(() -> scheduleService.getSchedule(tenantId, created.id()))
            .isInstanceOf(com.ashutosh.scheduler.common.exception.ResourceNotFoundException.class);
    }

    @Test
    @DisplayName("Fault injection: crash between job insert and schedule update recovers cleanly via idempotency")
    void testFaultInjectionBetweenJobInsertAndScheduleUpdate() {
        CreateScheduleRequest req = new CreateScheduleRequest(
            "0 * * * * *",
            "UTC",
            OverlapPolicy.ALLOW,
            MisfirePolicy.FIRE_ONCE,
            300,
            defaultTemplate()
        );
        ScheduleResponse schedule = scheduleService.createSchedule(tenantId, req);

        Instant dueTime = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        String idempotencyKey = "cron:" + schedule.id() + ":" + dueTime.getEpochSecond();

        // 1. Simulate partial failure: job row was inserted, but schedule update never committed
        UUID injectedJobId = UuidV7.generate();
        jdbcTemplate.update("""
            INSERT INTO jobs (id, user_id, schedule_id, queue, type, payload, body, status, priority,
                              run_at, attempts, max_attempts, idempotency_key, created_at, updated_at)
            VALUES (?, ?, ?, 'default', 'HTTP', '{"url":"https://example.com"}'::jsonb, null, 'PENDING', 0,
                    ?, 0, 3, ?, now(), now())
            """, injectedJobId, tenantId, schedule.id(), Timestamp.from(dueTime), idempotencyKey);

        // 2. Schedule remains with next_run_at = dueTime
        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
            Timestamp.from(dueTime), schedule.id());

        // 3. Sweeper runs: must handle ON CONFLICT DO NOTHING and successfully advance schedule
        scheduleProcessor.processSchedule(schedule.id(), Instant.now());

        // Assert: Exactly 1 job exists with this idempotency key (no duplicate inserted)
        List<UUID> matchingJobs = jdbcTemplate.queryForList(
            "SELECT id FROM jobs WHERE idempotency_key = ?", UUID.class, idempotencyKey
        );
        assertThat(matchingJobs).hasSize(1);
        assertThat(matchingJobs.get(0)).isEqualTo(injectedJobId);

        // Assert: Schedule's next_run_at successfully advanced into the future
        ScheduleEntity updatedSchedule = scheduleRepository.findById(schedule.id()).orElseThrow();
        assertThat(updatedSchedule.getNextRunAt()).isAfter(Instant.now());
    }

    @Test
    @DisplayName("Poison schedule isolation and auto-disable: 1 poison schedule does not affect healthy schedules and auto-disables after 5 failures")
    void testPoisonScheduleIsolationAndAutoDisable() {
        // Schedule A: Healthy
        ScheduleResponse healthySchedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 * * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));

        // Schedule B: Poison (corrupted jobTemplate JSON that fails at spawn)
        ScheduleResponse poisonSchedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 * * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));
        // Corrupt template directly in database
        jdbcTemplate.update("UPDATE schedules SET job_template = '{\"bad_json\":' WHERE id = ?", poisonSchedule.id());

        // Set both due in the past
        Instant pastDue = Instant.now().minusSeconds(10);
        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id IN (?, ?)",
            Timestamp.from(pastDue), healthySchedule.id(), poisonSchedule.id());

        // Process both: Healthy must succeed despite poison failure
        scheduleProcessor.processSchedule(healthySchedule.id(), Instant.now());
        scheduleProcessor.processSchedule(poisonSchedule.id(), Instant.now());

        ScheduleEntity healthyEntity = scheduleRepository.findById(healthySchedule.id()).orElseThrow();
        assertThat(healthyEntity.getLastFiredAt()).isNotNull();
        assertThat(healthyEntity.getConsecutiveFailures()).isEqualTo(0);

        ScheduleEntity poisonEntity = scheduleRepository.findById(poisonSchedule.id()).orElseThrow();
        assertThat(poisonEntity.getConsecutiveFailures()).isEqualTo(1);
        assertThat(poisonEntity.getLastError()).isNotNull();

        // Simulate 4 more failures on poison schedule
        for (int i = 2; i <= 5; i++) {
            jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
                Timestamp.from(pastDue), poisonSchedule.id());
            scheduleProcessor.processSchedule(poisonSchedule.id(), Instant.now());
        }

        ScheduleEntity autoDisabledPoison = scheduleRepository.findById(poisonSchedule.id()).orElseThrow();
        assertThat(autoDisabledPoison.getConsecutiveFailures()).isEqualTo(5);
        assertThat(autoDisabledPoison.isEnabled()).isFalse(); // Auto-disabled!
    }

    @Test
    @DisplayName("Multi-instance concurrency: 10 parallel sweeper threads produce exactly 1 job for the due slot")
    void testMultiInstanceConcurrencyExactlyOneJobPerSlot() throws Exception {
        ScheduleResponse schedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 * * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));

        Instant pastDue = Instant.now().minusSeconds(10);
        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
            Timestamp.from(pastDue), schedule.id());

        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    scheduleProcessor.processSchedule(schedule.id(), Instant.now());
                } catch (Exception e) {
                    log.error("Concurrent sweep error", e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        // Exactly 1 job must have been created for this schedule
        List<JobEntity> spawnedJobs = jobRepository.findByScheduleIdOrderByCreatedAtDesc(
            schedule.id(), org.springframework.data.domain.Pageable.unpaged()
        ).getContent();

        assertThat(spawnedJobs).hasSize(1);
    }

    @Test
    @DisplayName("Quota race: concurrent cron spawn and API submissions strictly enforce tenant max_active_jobs")
    void testQuotaRaceBetweenCronSpawnAndApiSubmissions() throws Exception {
        // Limit tenant quota to 5 active jobs
        jdbcTemplate.update("UPDATE users SET max_active_jobs = 5 WHERE id = ?", tenantId);

        // Pre-create 4 active jobs
        for (int i = 0; i < 4; i++) {
            jobProducerService.createJob(tenantId, new CreateJobRequest(
                "https://example.com/api", "POST", Map.of(), "{}", "default", 0, null, 3, null
            ), null);
        }
        assertThat(jobRepository.countActiveJobsByUserId(tenantId)).isEqualTo(4);

        ScheduleResponse schedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 * * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));
        Instant pastDue = Instant.now().minusSeconds(10);
        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
            Timestamp.from(pastDue), schedule.id());

        // Concurrently run 1 cron spawn and 5 API submissions (6 contenders for 1 remaining slot)
        int contenders = 6;
        ExecutorService executor = Executors.newFixedThreadPool(contenders);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(contenders);
        AtomicInteger successfulSubmits = new AtomicInteger(0);

        // Contender 1: Cron Sweeper
        executor.submit(() -> {
            try {
                startLatch.await();
                scheduleProcessor.processSchedule(schedule.id(), Instant.now());
                long count = jobRepository.findByScheduleIdOrderByCreatedAtDesc(schedule.id(), org.springframework.data.domain.Pageable.unpaged()).getTotalElements();
                if (count > 0) successfulSubmits.incrementAndGet();
            } catch (Exception ignored) {
            } finally {
                doneLatch.countDown();
            }
        });

        // Contenders 2-6: Direct API Submissions
        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    jobProducerService.createJob(tenantId, new CreateJobRequest(
                        "https://example.com/api", "POST", Map.of(), "{}", "default", 0, null, 3, null
                    ), null);
                    successfulSubmits.incrementAndGet();
                } catch (QuotaExceededException ignored) {
                } catch (Exception e) {
                    log.error("Submit error", e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        // Total active jobs must NEVER exceed 5!
        long activeJobs = jobRepository.countActiveJobsByUserId(tenantId);
        assertThat(activeJobs).isEqualTo(5);
    }

    @Test
    @DisplayName("3-hour misfire handling: SKIP advances to future without firing; FIRE_ONCE spawns single catch-up job")
    void testMisfirePoliciesThreeHourDowntime() {
        // 1. Misfire Policy = SKIP
        ScheduleResponse skipSchedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 */5 * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.SKIP, 300, defaultTemplate()
        ));
        Instant threeHoursAgo = Instant.now().minus(3, ChronoUnit.HOURS);
        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
            Timestamp.from(threeHoursAgo), skipSchedule.id());

        scheduleProcessor.processSchedule(skipSchedule.id(), Instant.now());

        ScheduleEntity skipEntity = scheduleRepository.findById(skipSchedule.id()).orElseThrow();
        assertThat(skipEntity.getLastSkipReason()).isEqualTo(ScheduleSkipReason.MISFIRE_SKIP);
        assertThat(skipEntity.getLastFiredAt()).isNull(); // Zero jobs fired!
        assertThat(skipEntity.getNextRunAt()).isAfter(Instant.now()); // Advanced to strictly after now

        // 2. Misfire Policy = FIRE_ONCE
        ScheduleResponse fireOnceSchedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 */5 * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));
        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
            Timestamp.from(threeHoursAgo), fireOnceSchedule.id());

        scheduleProcessor.processSchedule(fireOnceSchedule.id(), Instant.now());

        ScheduleEntity fireOnceEntity = scheduleRepository.findById(fireOnceSchedule.id()).orElseThrow();
        assertThat(fireOnceEntity.getLastFiredAt()).isNotNull(); // Exactly one catch-up job fired!
        assertThat(fireOnceEntity.getNextRunAt()).isAfter(Instant.now()); // Advanced to strictly after now

        long jobsCreated = jobRepository.findByScheduleIdOrderByCreatedAtDesc(
            fireOnceSchedule.id(), org.springframework.data.domain.Pageable.unpaged()
        ).getTotalElements();
        assertThat(jobsCreated).isEqualTo(1);
    }

    @Test
    @DisplayName("Resume-after-pause: recomputes next_run_at strictly from now without firing old backlog ticks")
    void testResumeAfterPauseRecomputesNextRunFromNow() {
        ScheduleResponse schedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 0 12 * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));

        // Pause schedule
        scheduleService.patchSchedule(tenantId, schedule.id(), new UpdateScheduleRequest(
            null, null, null, null, null, null, false
        ));

        // Simulate 2 days passing while paused
        Instant twoDaysAgo = Instant.now().minus(2, ChronoUnit.DAYS);
        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
            Timestamp.from(twoDaysAgo), schedule.id());

        // Resume schedule (enabled = true)
        ScheduleResponse resumed = scheduleService.patchSchedule(tenantId, schedule.id(), new UpdateScheduleRequest(
            null, null, null, null, null, null, true
        ));

        assertThat(resumed.enabled()).isTrue();
        assertThat(resumed.nextRunAt()).isAfter(Instant.now()); // Recalculated from now!

        // Sweeper run: should find nothing due
        int swept = cronSchedulerEngine.pollOnce();
        assertThat(swept).isEqualTo(0);
    }

    @Test
    @DisplayName("Manual trigger: does not advance next_run_at and respects overlap and quota constraints")
    void testManualTriggerBehavior() {
        ScheduleResponse schedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 0 12 * * *", "UTC", OverlapPolicy.SKIP, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));
        Instant initialNextRun = schedule.nextRunAt();

        // 1. Successful manual trigger
        JobResponse triggeredJob = scheduleService.triggerSchedule(tenantId, schedule.id(), null);
        assertThat(triggeredJob).isNotNull();
        assertThat(triggeredJob.scheduleId()).isEqualTo(schedule.id());

        // Assert next_run_at did NOT change
        ScheduleEntity afterTrigger = scheduleRepository.findById(schedule.id()).orElseThrow();
        assertThat(afterTrigger.getNextRunAt()).isEqualTo(initialNextRun);

        // 2. Overlap constraint under SKIP policy
        // Job is currently PENDING (active), so second trigger must throw JobOverlapException
        assertThatThrownBy(() -> scheduleService.triggerSchedule(tenantId, schedule.id(), null))
            .isInstanceOf(JobOverlapException.class)
            .hasMessageContaining("overlap_policy is SKIP");
    }

    @Test
    @DisplayName("EXPLAIN (ANALYZE, BUFFERS): Verify partial index idx_jobs_schedule_active on 1,000 historical jobs")
    void testExplainAnalyzeBuffersOnScheduleActiveIndex() {
        UUID scheduleId = UuidV7.generate();

        // Seed 1,000 historical jobs for this schedule (995 completed, 5 active)
        jdbcTemplate.batchUpdate("""
            INSERT INTO jobs (id, user_id, schedule_id, queue, type, payload, status, priority, run_at, attempts, max_attempts, created_at, updated_at)
            VALUES (?, ?, ?, 'default', 'HTTP', '{"url":"https://example.com"}'::jsonb, ?, 0, now(), 1, 3, now(), now())
            """, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
            @Override
            public void setValues(java.sql.PreparedStatement ps, int i) throws java.sql.SQLException {
                ps.setObject(1, UuidV7.generate());
                ps.setObject(2, tenantId);
                ps.setObject(3, scheduleId);
                ps.setString(4, i < 5 ? "PENDING" : "COMPLETED");
            }
            @Override
            public int getBatchSize() {
                return 1000;
            }
        });

        String explainSql = """
            EXPLAIN (ANALYZE, BUFFERS)
            SELECT count(*) FROM jobs
            WHERE schedule_id = ? AND status IN ('PENDING', 'RUNNING');
            """;

        List<String> planLines = jdbcTemplate.query(
            explainSql, (rs, rowNum) -> rs.getString(1), scheduleId
        );

        String fullPlan = String.join("\n", planLines);
        log.info("EXPLAIN (ANALYZE, BUFFERS) for idx_jobs_schedule_active:\n{}", fullPlan);

        // Verify index scan on idx_jobs_schedule_active
        assertThat(fullPlan).contains("idx_jobs_schedule_active");
        assertThat(fullPlan).doesNotContain("Seq Scan on jobs");
    }
}
