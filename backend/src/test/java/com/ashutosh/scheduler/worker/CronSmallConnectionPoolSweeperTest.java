package com.ashutosh.scheduler.worker;

import com.ashutosh.scheduler.TestcontainersConfiguration;
import com.ashutosh.scheduler.common.util.CryptoUtils;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.entity.ScheduleEntity;
import com.ashutosh.scheduler.domain.enums.MisfirePolicy;
import com.ashutosh.scheduler.domain.enums.OverlapPolicy;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.ScheduleRepository;
import com.ashutosh.scheduler.producer.dto.CreateScheduleRequest;
import com.ashutosh.scheduler.producer.dto.JobTemplateDto;
import com.ashutosh.scheduler.producer.dto.ScheduleResponse;
import com.ashutosh.scheduler.producer.service.ScheduleService;
import com.ashutosh.scheduler.worker.engine.CronSchedulerEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = {
    "spring.datasource.hikari.maximum-pool-size=3",
    "spring.datasource.hikari.connection-timeout=5000"
})
class CronSmallConnectionPoolSweeperTest {

    private static final Logger log = LoggerFactory.getLogger(CronSmallConnectionPoolSweeperTest.class);

    @Autowired
    private ScheduleService scheduleService;

    @Autowired
    private ScheduleRepository scheduleRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private CronSchedulerEngine cronSchedulerEngine;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private UUID tenantId;

    @BeforeEach
    void setUp() {
        tenantId = UuidV7.generate();
        String apiKey = "sk_live_" + UUID.randomUUID().toString().replace("-", "");

        jdbcTemplate.update(
            "INSERT INTO users (id, username, rate_limit_per_minute, max_active_jobs) VALUES (?, ?, ?, ?)",
            tenantId, "small_pool_tenant_" + UUID.randomUUID().toString().substring(0, 8), 120, 50
        );

        String keyHash = CryptoUtils.sha256Hex(apiKey);
        jdbcTemplate.update(
            "INSERT INTO api_keys (id, user_id, key_hash, key_prefix, name) VALUES (?, ?, ?, ?, ?)",
            UuidV7.generate(), tenantId, keyHash, apiKey.substring(0, 16), "Small Pool Key"
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
    @DisplayName("Small connection pool (size=3) with 6 concurrent sweepers: zero deadlocks, claim lock released immediately, and re-check prevents double execution")
    void testSmallConnectionPoolWithConcurrentSweepers() throws Exception {
        // Create 5 due schedules
        List<ScheduleResponse> schedules = new ArrayList<>();
        Instant pastDue = Instant.now().minusSeconds(15);

        for (int i = 0; i < 5; i++) {
            ScheduleResponse s = scheduleService.createSchedule(tenantId, new CreateScheduleRequest(
                "0 * * * * *", "UTC", OverlapPolicy.ALLOW, MisfirePolicy.FIRE_ONCE, 300, defaultTemplate()
            ));
            schedules.add(s);
            jdbcTemplate.update("UPDATE schedules SET next_run_at = ? WHERE id = ?",
                Timestamp.from(pastDue), s.id());
        }

        // Run 6 concurrent sweeper threads (2x the pool size of 3)
        int sweeperThreads = 6;
        ExecutorService executor = Executors.newFixedThreadPool(sweeperThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(sweeperThreads);
        AtomicInteger totalSweeperRuns = new AtomicInteger(0);
        AtomicInteger sweeperExceptions = new AtomicInteger(0);

        for (int i = 0; i < sweeperThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    int processed = cronSchedulerEngine.pollOnce();
                    totalSweeperRuns.addAndGet(processed);
                } catch (Exception e) {
                    log.error("Sweeper thread failed: {}", e.getMessage(), e);
                    sweeperExceptions.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(15, TimeUnit.SECONDS);
        executor.shutdown();

        // 1. All sweeper threads completed without hanging or timing out on connection checkout
        assertThat(completed).isTrue();
        assertThat(sweeperExceptions.get()).isEqualTo(0);

        // 2. Exactly 5 jobs spawned across the 5 schedules (exactly 1 job per schedule)
        for (ScheduleResponse s : schedules) {
            List<JobEntity> jobs = jobRepository.findByScheduleIdOrderByCreatedAtDesc(s.id(), Pageable.unpaged()).getContent();
            assertThat(jobs).as("Schedule %s must have exactly 1 spawned job", s.id()).hasSize(1);

            // 3. Each schedule's next_run_at was safely advanced strictly into the future
            ScheduleEntity entity = scheduleRepository.findById(s.id()).orElseThrow();
            assertThat(entity.getNextRunAt()).isAfter(Instant.now());
        }
    }
}
