package com.ashutosh.scheduler.worker.engine;

import com.ashutosh.scheduler.common.util.CryptoUtils;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.ScheduleEntity;
import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.enums.MisfirePolicy;
import com.ashutosh.scheduler.domain.enums.OverlapPolicy;
import com.ashutosh.scheduler.domain.enums.ScheduleSkipReason;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.ScheduleRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import com.ashutosh.scheduler.producer.dto.HttpPayloadDto;
import com.ashutosh.scheduler.producer.dto.JobTemplateDto;
import com.ashutosh.scheduler.producer.validator.ScheduleValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

@Service
public class CronScheduleProcessor {

    private static final Logger log = LoggerFactory.getLogger(CronScheduleProcessor.class);

    private final ScheduleRepository scheduleRepository;
    private final JobRepository jobRepository;
    private final UserRepository userRepository;
    private final ScheduleValidator scheduleValidator;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final int maxConsecutiveFailures;

    public CronScheduleProcessor(
            ScheduleRepository scheduleRepository,
            JobRepository jobRepository,
            UserRepository userRepository,
            ScheduleValidator scheduleValidator,
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            @Value("${scheduler.cron.max-consecutive-failures:5}") int maxConsecutiveFailures) {
        this.scheduleRepository = scheduleRepository;
        this.jobRepository = jobRepository;
        this.userRepository = userRepository;
        this.scheduleValidator = scheduleValidator;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.maxConsecutiveFailures = maxConsecutiveFailures;
    }

    /**
     * Executes schedule processing in an isolated transaction per schedule.
     * Guarantees poison schedule failures do not roll back adjacent schedules in a batch.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processSchedule(UUID scheduleId, Instant now) {
        // Enforce lock order: 1. Schedule row FOR UPDATE
        ScheduleEntity schedule = scheduleRepository.findById(scheduleId).orElse(null);
        if (schedule == null || !schedule.isEnabled()) {
            return;
        }

        // Enforce lock order: 2. Tenant row FOR UPDATE
        UserEntity user = userRepository.findByIdForUpdate(schedule.getUserId()).orElse(null);
        if (user == null) {
            log.warn("Schedule {} orphaned; user {} not found. Auto-disabling.", scheduleId, schedule.getUserId());
            schedule.setEnabled(false);
            schedule.setLastError("Owning user not found.");
            scheduleRepository.save(schedule);
            return;
        }

        ZoneId zoneId = ZoneId.of(schedule.getTimezone());
        CronExpression cron = CronExpression.parse(schedule.getCronExpression());
        ZonedDateTime nowZdt = now.atZone(zoneId);
        ZonedDateTime dueZdt = schedule.getNextRunAt().atZone(zoneId);

        try {
            long delaySeconds = Duration.between(schedule.getNextRunAt(), now).getSeconds();
            boolean isMisfire = delaySeconds > schedule.getGraceWindowSeconds();

            if (isMisfire) {
                if (schedule.getMisfirePolicy() == MisfirePolicy.SKIP) {
                    log.info("Schedule {} misfire skipped (delay={}s > grace={}s, policy=SKIP)",
                        scheduleId, delaySeconds, schedule.getGraceWindowSeconds());
                    schedule.setLastSkipReason(ScheduleSkipReason.MISFIRE_SKIP);
                    schedule.setLastTickAt(now);
                    advanceNextRunAt(schedule, cron, nowZdt);
                    scheduleRepository.save(schedule);
                    return;
                }
            }

            // Target tick: for on-time or FIRE_ONCE catch-up, target the most recent tick before now
            ZonedDateTime targetTickZdt = isMisfire ? findLatestMissedTick(cron, dueZdt, nowZdt) : dueZdt;
            String idempotencyKey = "cron:" + scheduleId + ":" + targetTickZdt.toEpochSecond();

            // Check overlap policy
            if (schedule.getOverlapPolicy() == OverlapPolicy.SKIP && jobRepository.hasActiveJobsForSchedule(scheduleId)) {
                log.info("Schedule {} overlap skipped (active job exists, policy=SKIP)", scheduleId);
                schedule.setLastSkipReason(ScheduleSkipReason.OVERLAP_SKIP);
                schedule.setLastTickAt(now);
                advanceNextRunAt(schedule, cron, nowZdt);
                scheduleRepository.save(schedule);
                return;
            }

            // Check tenant active job quota
            long activeJobs = jobRepository.countActiveJobsByUserId(schedule.getUserId());
            if (activeJobs >= user.getMaxActiveJobs()) {
                log.warn("Schedule {} quota skipped (active={}/{}, policy=QUOTA_SKIP)",
                    scheduleId, activeJobs, user.getMaxActiveJobs());
                schedule.setLastSkipReason(ScheduleSkipReason.QUOTA_SKIP);
                schedule.setLastTickAt(now);
                advanceNextRunAt(schedule, cron, nowZdt);
                scheduleRepository.save(schedule);
                return;
            }

            // Parse and validate template at spawn time
            JobTemplateDto template = objectMapper.readValue(schedule.getJobTemplate(), JobTemplateDto.class);
            scheduleValidator.validateJobTemplate(template);

            // Spawn job into work queue with ON CONFLICT DO NOTHING
            spawnJob(schedule, targetTickZdt.toInstant(), idempotencyKey, template, now);

            // Update schedule audit and advance to first tick strictly after now
            schedule.setLastFiredAt(now);
            schedule.setLastTickAt(now);
            schedule.setLastSkipReason(null);
            schedule.setConsecutiveFailures(0);
            schedule.setLastError(null);
            advanceNextRunAt(schedule, cron, nowZdt);
            scheduleRepository.save(schedule);

            log.info("Schedule {} spawned job with key={}, nextRunAt={}", scheduleId, idempotencyKey, schedule.getNextRunAt());

        } catch (Exception e) {
            handleFailure(schedule, cron, nowZdt, e);
        }
    }

    private void spawnJob(ScheduleEntity schedule, Instant targetRunAt, String idempotencyKey,
                          JobTemplateDto template, Instant now) throws Exception {
        String method = (template.method() == null || template.method().isBlank()) ? "POST" : template.method().trim().toUpperCase();
        String queue = (template.queue() == null || template.queue().isBlank()) ? "default" : template.queue().trim();
        int priority = template.priority() == null ? 0 : template.priority();
        int maxAttempts = template.maxAttempts() == null ? 3 : template.maxAttempts();
        Map<String, String> headers = template.headers() == null ? Map.of() : template.headers();

        String requestHash = CryptoUtils.computeRequestHash(method, template.url(), headers, template.body());
        HttpPayloadDto payloadDto = new HttpPayloadDto(template.url(), method, headers, null);
        String payloadJson = objectMapper.writeValueAsString(payloadDto);

        UUID jobId = UuidV7.generate();

        String insertSql = """
            INSERT INTO jobs (id, user_id, schedule_id, queue, type, payload, body, status, priority,
                              run_at, attempts, max_attempts, idempotency_key, request_hash, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'HTTP', ?::jsonb, ?, 'PENDING', ?, ?, 0, ?, ?, ?, ?, ?)
            ON CONFLICT (user_id, idempotency_key) DO NOTHING
            """;

        jdbcTemplate.update(insertSql,
            jobId, schedule.getUserId(), schedule.getId(), queue, payloadJson, template.body(),
            priority, Timestamp.from(targetRunAt), maxAttempts, idempotencyKey, requestHash,
            Timestamp.from(now), Timestamp.from(now)
        );
    }

    private void advanceNextRunAt(ScheduleEntity schedule, CronExpression cron, ZonedDateTime nowZdt) {
        ZonedDateTime nextZdt = cron.next(nowZdt);
        if (nextZdt != null) {
            schedule.setNextRunAt(nextZdt.toInstant());
        }
    }

    private ZonedDateTime findLatestMissedTick(CronExpression cron, ZonedDateTime startZdt, ZonedDateTime nowZdt) {
        ZonedDateTime cursor = startZdt;
        ZonedDateTime next = cron.next(cursor);
        while (next != null && !next.isAfter(nowZdt)) {
            cursor = next;
            next = cron.next(cursor);
        }
        return cursor;
    }

    private void handleFailure(ScheduleEntity schedule, CronExpression cron, ZonedDateTime nowZdt, Exception e) {
        int failures = schedule.getConsecutiveFailures() + 1;
        schedule.setConsecutiveFailures(failures);
        schedule.setLastError(e.getMessage());
        log.error("Execution failure for schedule {}: {} (consecutive={})", schedule.getId(), e.getMessage(), failures);

        if (failures >= maxConsecutiveFailures) {
            schedule.setEnabled(false);
            log.error("Schedule {} auto-disabled after reaching {} consecutive failures", schedule.getId(), failures);
        }

        advanceNextRunAt(schedule, cron, nowZdt);
        scheduleRepository.save(schedule);
    }
}
