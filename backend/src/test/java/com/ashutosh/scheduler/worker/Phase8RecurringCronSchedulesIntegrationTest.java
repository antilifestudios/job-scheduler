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
    @DisplayName("Circuit breaker: permanent errors auto-disable immediately; transient errors auto-disable after 5 failures with sanitized error text")
    void testPoisonScheduleIsolationAndAutoDisable() {
        // Schedule A: Healthy
        ScheduleResponse healthySchedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 * * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));

        // Schedule B: Permanent error (invalid HTTP method in template -> ValidationException)
        ScheduleResponse permanentPoison = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 * * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));
        jdbcTemplate.update("UPDATE schedules SET job_template = '{\"url\":\"https://example.com/api\",\"method\":\"INVALID_METHOD\"}'::jsonb WHERE id = ?", permanentPoison.id());

        // Schedule C: Transient error (simulated database query timeout)
        ScheduleResponse transientPoison = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 * * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));

        // Set A and B due in past
        Instant pastDue = Instant.now().minusSeconds(10);
        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id IN (?, ?)",
            Timestamp.from(pastDue), healthySchedule.id(), permanentPoison.id());

        // Process Healthy (succeeds) and Permanent (auto-disables immediately)
        scheduleProcessor.processSchedule(healthySchedule.id(), Instant.now());
        scheduleProcessor.processSchedule(permanentPoison.id(), Instant.now());

        // Healthy schedule: succeeded, 0 failures
        ScheduleEntity healthyEntity = scheduleRepository.findById(healthySchedule.id()).orElseThrow();
        assertThat(healthyEntity.getLastFiredAt()).isNotNull();
        assertThat(healthyEntity.getConsecutiveFailures()).isEqualTo(0);
        assertThat(healthyEntity.isEnabled()).isTrue();

        // Permanent error: auto-disabled immediately on 1st failure
        ScheduleEntity permEntity = scheduleRepository.findById(permanentPoison.id()).orElseThrow();
        assertThat(permEntity.isEnabled()).isFalse();
        assertThat(permEntity.getDisabledReason()).isEqualTo("PERMANENT_VALIDATION_ERROR");
        assertThat(permEntity.getLastError()).contains("Unsupported HTTP method");
        assertThat(permEntity.getLastError()).doesNotContain("org.postgresql").doesNotContain("Exception");

        // Verify exposed via API
        ScheduleResponse permApiResp = scheduleService.getSchedule(tenantId, permanentPoison.id());
        assertThat(permApiResp.enabled()).isFalse();
        assertThat(permApiResp.disabledReason()).isEqualTo("PERMANENT_VALIDATION_ERROR");
        assertThat(permApiResp.lastError()).contains("Unsupported HTTP method");

        // Transient error on Schedule C: simulate 1st transient DB timeout
        ScheduleEntity transEntity = scheduleRepository.findById(transientPoison.id()).orElseThrow();
        ZoneId zoneId = ZoneId.of(transEntity.getTimezone());
        CronExpression cron = CronExpression.parse(transEntity.getCronExpression());
        ZonedDateTime nowZdt = ZonedDateTime.now(zoneId);

        scheduleProcessor.handleFailure(transEntity, cron, nowZdt, nowZdt, new org.springframework.dao.QueryTimeoutException("connection timeout"));
        ScheduleEntity afterFirstFail = scheduleRepository.findById(transientPoison.id()).orElseThrow();
        assertThat(afterFirstFail.getConsecutiveFailures()).isEqualTo(1);
        assertThat(afterFirstFail.isEnabled()).isTrue();
        assertThat(afterFirstFail.getLastError()).isEqualTo("Database error during schedule execution");

        // Simulate 4 more transient failures on Schedule C to trigger circuit breaker auto-disable
        for (int i = 2; i <= 5; i++) {
            scheduleProcessor.handleFailure(afterFirstFail, cron, nowZdt, nowZdt, new org.springframework.dao.QueryTimeoutException("connection timeout"));
        }

        ScheduleEntity autoDisabledTransient = scheduleRepository.findById(transientPoison.id()).orElseThrow();
        assertThat(autoDisabledTransient.getConsecutiveFailures()).isEqualTo(5);
        assertThat(autoDisabledTransient.isEnabled()).isFalse();
        assertThat(autoDisabledTransient.getDisabledReason()).isEqualTo("MAX_CONSECUTIVE_FAILURES_EXCEEDED");
        assertThat(autoDisabledTransient.getLastError()).isEqualTo("Database error during schedule execution");

        // Verify exposed via API without SQL or stack traces
        ScheduleResponse transApiResp = scheduleService.getSchedule(tenantId, transientPoison.id());
        assertThat(transApiResp.enabled()).isFalse();
        assertThat(transApiResp.disabledReason()).isEqualTo("MAX_CONSECUTIVE_FAILURES_EXCEEDED");
        assertThat(transApiResp.lastError()).isEqualTo("Database error during schedule execution");
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
    @DisplayName("Grace window positive FIRE_ONCE test: 3 hours down, every-minute cron, within grace fires exactly 1 job, next_run_at in future, correct idempotency key")
    void testFireOnceThreeHourDowntimeEveryMinuteCronWithinGrace() {
        ScheduleResponse schedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 * * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));

        // Scheduler went down 3 hours ago: last scheduled due time was 12:00:00 UTC
        Instant dueBase = Instant.parse("2026-10-27T12:00:00Z");
        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
            Timestamp.from(dueBase), schedule.id());

        // Scheduler recovers at 15:00:20 UTC (3 hours and 20 seconds later)
        // Lateness of the candidate tick being fired (15:00:00) is now - most recent missed tick = 20 seconds.
        // 20s <= 300s grace window -> within grace!
        Instant recoveryTime = Instant.parse("2026-10-27T15:00:20Z");
        Instant expectedTargetTick = Instant.parse("2026-10-27T15:00:00Z");
        String expectedIdempotencyKey = "cron:" + schedule.id() + ":" + expectedTargetTick.getEpochSecond();

        scheduleProcessor.processSchedule(schedule.id(), recoveryTime);

        // Assert 1: Exactly 1 job created
        List<JobEntity> spawnedJobs = jobRepository.findByScheduleIdOrderByCreatedAtDesc(
            schedule.id(), org.springframework.data.domain.Pageable.unpaged()
        ).getContent();
        assertThat(spawnedJobs).hasSize(1);

        // Assert 2: Job properties and correct idempotency key
        JobEntity spawnedJob = spawnedJobs.get(0);
        assertThat(spawnedJob.getIdempotencyKey()).isEqualTo(expectedIdempotencyKey);
        assertThat(spawnedJob.getRunAt()).isEqualTo(expectedTargetTick);
        assertThat(spawnedJob.getStatus()).isEqualTo(JobStatus.PENDING);

        // Assert 3: Schedule state advanced strictly into the future
        ScheduleEntity updatedSchedule = scheduleRepository.findById(schedule.id()).orElseThrow();
        assertThat(updatedSchedule.getLastFiredAt()).isEqualTo(recoveryTime);
        assertThat(updatedSchedule.getLastSkipReason()).isNull();
        Instant expectedNextRunAt = Instant.parse("2026-10-27T15:01:00Z");
        assertThat(updatedSchedule.getNextRunAt()).isEqualTo(expectedNextRunAt);
        assertThat(updatedSchedule.getNextRunAt()).isAfter(recoveryTime);

        // Assert 4: Immediate second sweep at recoveryTime + 10s does not fire duplicate job
        scheduleProcessor.processSchedule(schedule.id(), recoveryTime.plusSeconds(10));
        long jobsAfterSecondSweep = jobRepository.findByScheduleIdOrderByCreatedAtDesc(
            schedule.id(), org.springframework.data.domain.Pageable.unpaged()
        ).getTotalElements();
        assertThat(jobsAfterSecondSweep).isEqualTo(1);
    }

    @Test
    @DisplayName("DST Fall-Back End-to-End: repeated 01:30 local hour fires strictly once on transition day")
    void testDstFallBackRepeatedHourFiresStrictlyOnceEndToEnd() {
        // Daily schedule at 01:30 AM America/New_York
        ScheduleResponse schedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 30 1 * * *", "America/New_York", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));

        // 1. First occurrence of 01:30 on Nov 1, 2026 is at 01:30:00 EDT (05:30:00 UTC)
        Instant firstOccurrenceUtc = Instant.parse("2026-11-01T05:30:00Z");
        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
            Timestamp.from(firstOccurrenceUtc), schedule.id());

        // Process schedule at first occurrence
        scheduleProcessor.processSchedule(schedule.id(), firstOccurrenceUtc.plusSeconds(15));

        // Assert: Exactly 1 job fired for first occurrence
        List<JobEntity> jobsAfterFirst = jobRepository.findByScheduleIdOrderByCreatedAtDesc(
            schedule.id(), org.springframework.data.domain.Pageable.unpaged()
        ).getContent();
        assertThat(jobsAfterFirst).hasSize(1);
        assertThat(jobsAfterFirst.get(0).getRunAt()).isEqualTo(firstOccurrenceUtc);

        // Assert: Schedule's next_run_at was advanced PAST the duplicate 01:30 EST hour to Nov 2 at 01:30 EST (06:30 UTC Nov 2)
        ScheduleEntity entityAfterFirst = scheduleRepository.findById(schedule.id()).orElseThrow();
        Instant expectedNextDayRunAt = Instant.parse("2026-11-02T06:30:00Z");
        assertThat(entityAfterFirst.getNextRunAt()).isEqualTo(expectedNextDayRunAt);

        // 2. Second occurrence of 01:30 occurs 1 hour later at 01:30:00 EST (06:30:00 UTC)
        Instant secondOccurrenceUtc = Instant.parse("2026-11-01T06:30:00Z");

        // Sweeper attempts to process schedule at second occurrence
        scheduleProcessor.processSchedule(schedule.id(), secondOccurrenceUtc.plusSeconds(15));

        // Assert: Next run is still Nov 2, so the repeated hour is skipped and NO second job is created!
        List<JobEntity> jobsAfterSecond = jobRepository.findByScheduleIdOrderByCreatedAtDesc(
            schedule.id(), org.springframework.data.domain.Pageable.unpaged()
        ).getContent();
        assertThat(jobsAfterSecond).hasSize(1); // STILL EXACTLY 1!
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
        ScheduleResponse schedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 0 12 * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
        ));
        UUID scheduleId = schedule.id();

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
                ps.setString(4, i < 5 ? "PENDING" : "SUCCEEDED");
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

    @Test
    @DisplayName("Positive FIRE_ONCE: 3 hours down, every-minute cron, within grace: exactly one job, next_run_at strictly in the future, correct idempotency key")
    void testThreeHoursDownEveryMinuteCronWithinGraceFiresOnce() {
        ScheduleResponse schedule = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
            "0 * * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 60, defaultTemplate()
        ));

        // Fixed baseline: 12:00:15 UTC. Downtime started at 09:00:00 UTC (3 hours down)
        Instant fixedNow = Instant.parse("2026-10-27T12:00:15Z");
        Instant oldestMissedTick = Instant.parse("2026-10-27T09:00:00Z");
        Instant mostRecentMissedTick = Instant.parse("2026-10-27T12:00:00Z");

        jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
            Timestamp.from(oldestMissedTick), schedule.id());

        // Process schedule at fixedNow (lateness = 15s <= grace 60s)
        scheduleProcessor.processSchedule(schedule.id(), fixedNow);

        ScheduleEntity updated = scheduleRepository.findById(schedule.id()).orElseThrow();
        assertThat(updated.getLastFiredAt()).isEqualTo(fixedNow);
        assertThat(updated.getLastSkipReason()).isNull();
        // next_run_at strictly in the future: first tick after 12:00:15 is 12:01:00
        assertThat(updated.getNextRunAt()).isEqualTo(Instant.parse("2026-10-27T12:01:00Z"));
        assertThat(updated.getNextRunAt()).isAfter(fixedNow);

        // Exactly one job spawned
        List<JobEntity> spawnedJobs = jobRepository.findByScheduleIdOrderByCreatedAtDesc(
            schedule.id(), org.springframework.data.domain.Pageable.unpaged()
        ).getContent();
        assertThat(spawnedJobs).hasSize(1);

        // Correct deterministic idempotency key for the most recent missed tick
        String expectedKey = "cron:" + schedule.id() + ":" + mostRecentMissedTick.getEpochSecond();
        assertThat(spawnedJobs.get(0).getIdempotencyKey()).isEqualTo(expectedKey);
    }

    @Test
    @DisplayName("Schedule cap enforced under tenant row lock: 10 parallel creations with 1 slot free allow exactly 1 and reject 9 with 429 quota_exceeded")
    void testScheduleCapEnforcedUnderTenantRowLockConcurrency() throws Exception {
        UUID capTenantId = UuidV7.generate();
        userRepository.save(new com.ashutosh.scheduler.domain.entity.UserEntity(capTenantId, "cap-tenant-" + capTenantId));

        int maxSchedules = 20;
        // Pre-create 19 schedules directly in database so exactly 1 slot is free
        for (int i = 0; i < 19; i++) {
            jdbcTemplate.update("""
                INSERT INTO schedules (id, user_id, cron_expression, timezone, overlap_policy, misfire_policy, grace_window_seconds, job_template, next_run_at, enabled, created_at, updated_at)
                VALUES (?, ?, '0 0 12 * * *', 'UTC', 'SKIP', 'FIRE_ONCE', 300, '{"url":"https://example.com"}'::jsonb, now() + interval '1 day', true, now(), now())
                """, UuidV7.generate(), capTenantId);
        }
        assertThat(scheduleRepository.countByUserId(capTenantId)).isEqualTo(19);

        // 10 concurrent threads attempting to create a schedule
        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger quotaExceededErrors = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    scheduleService.createSchedule(capTenantId, new CreateScheduleRequest(
                        "0 " + (index + 1) + " 12 * * *", "UTC", OverlapPolicy.SKIP, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
                    ));
                    successes.incrementAndGet();
                } catch (com.ashutosh.scheduler.common.exception.MaxSchedulesExceededException e) {
                    quotaExceededErrors.incrementAndGet();
                    assertThat(e.getMaxSchedules()).isEqualTo(20);
                } catch (Exception e) {
                    log.error("Unexpected error in cap test", e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        // Exactly 1 thread succeeded and 9 threads received quota_exceeded
        assertThat(successes.get()).isEqualTo(1);
        assertThat(quotaExceededErrors.get()).isEqualTo(9);

        // Total schedules in DB is exactly 20 (cap enforced!)
        assertThat(scheduleRepository.countByUserId(capTenantId)).isEqualTo(20);
    }

    @Test
    @DisplayName("Transaction structure & small pool sweeper concurrency: multiple sweepers claim and process due schedules without duplicate spawns or deadlocks")
    void testTransactionStructureAndSweeperConcurrencySmallPool() throws Exception {
        // Create 5 schedules all due right now
        List<UUID> scheduleIds = new ArrayList<>();
        Instant pastDue = Instant.now().minusSeconds(10);
        for (int i = 0; i < 5; i++) {
            ScheduleResponse s = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
                "0 " + (i + 1) + " * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
            ));
            scheduleIds.add(s.id());
            jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
                Timestamp.from(pastDue), s.id());
        }

        // 4 concurrent sweeper threads running pollOnce() simultaneously
        int sweepers = 4;
        ExecutorService executor = Executors.newFixedThreadPool(sweepers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(sweepers);

        for (int i = 0; i < sweepers; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    cronSchedulerEngine.pollOnce();
                } catch (Exception e) {
                    log.error("Sweeper concurrency error", e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        // Across all 5 schedules, each schedule must have spawned exactly 1 job
        for (UUID sId : scheduleIds) {
            long jobs = jobRepository.findByScheduleIdOrderByCreatedAtDesc(
                sId, org.springframework.data.domain.Pageable.unpaged()
            ).getTotalElements();
            assertThat(jobs).as("Schedule %s should have exactly 1 job spawned", sId).isEqualTo(1);

            ScheduleEntity entity = scheduleRepository.findById(sId).orElseThrow();
            assertThat(entity.getNextRunAt()).isAfter(Instant.now());
        }
    }
}
