package com.ashutosh.scheduler.worker.engine;

import com.ashutosh.scheduler.common.exception.ValidationException;
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
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import tools.jackson.databind.ObjectMapper;

import java.sql.SQLException;
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
        ScheduleEntity schedule = scheduleRepository.findByIdForUpdate(scheduleId).orElse(null);
        if (schedule == null || !schedule.isEnabled() || schedule.getNextRunAt().isAfter(now)) {
            return;
        }

        // Enforce lock order: 2. Tenant row FOR UPDATE
        UserEntity user = userRepository.findByIdForUpdate(schedule.getUserId()).orElse(null);
        if (user == null) {
            log.warn("Schedule {} orphaned; user {} not found. Auto-disabling.", scheduleId, schedule.getUserId());
            schedule.setEnabled(false);
            schedule.setDisabledReason("USER_NOT_FOUND");
            schedule.setLastError("Owning user not found.");
            scheduleRepository.save(schedule);
            return;
        }

        ZoneId zoneId = ZoneId.of(schedule.getTimezone());
        CronExpression cron = CronExpression.parse(schedule.getCronExpression());
        ZonedDateTime nowZdt = now.atZone(zoneId);
        ZonedDateTime dueZdt = schedule.getNextRunAt().atZone(zoneId);
        ZonedDateTime targetTickZdt = dueZdt;

        try {
            // Find the most recent missed tick before or at now
            targetTickZdt = findLatestMissedTick(cron, dueZdt, nowZdt);

            // Measure lateness of the candidate tick being fired (now - most recent missed tick)
            long latenessSeconds = Math.max(0, Duration.between(targetTickZdt.toInstant(), now).getSeconds());
            boolean missedMultipleTicks = targetTickZdt.isAfter(dueZdt);
            boolean latenessExceedsGrace = latenessSeconds > schedule.getGraceWindowSeconds();

            // Misfire handling
            if (schedule.getMisfirePolicy() == MisfirePolicy.SKIP && (missedMultipleTicks || latenessExceedsGrace)) {
                log.info("Schedule {} misfire skipped (lateness={}s, missedMultiple={}, grace={}s, policy=SKIP)",
                    scheduleId, latenessSeconds, missedMultipleTicks, schedule.getGraceWindowSeconds());
                schedule.setLastSkipReason(ScheduleSkipReason.MISFIRE_SKIP);
                schedule.setLastTickAt(now);
                advanceNextRunAt(schedule, cron, nowZdt, targetTickZdt);
                scheduleRepository.save(schedule);
                return;
            }

            if (schedule.getMisfirePolicy() == MisfirePolicy.FIRE_ONCE && latenessExceedsGrace) {
                log.info("Schedule {} misfire expired (lateness={}s > grace={}s, policy=FIRE_ONCE)",
                    scheduleId, latenessSeconds, schedule.getGraceWindowSeconds());
                schedule.setLastSkipReason(ScheduleSkipReason.MISFIRE_SKIP);
                schedule.setLastTickAt(now);
                advanceNextRunAt(schedule, cron, nowZdt, targetTickZdt);
                scheduleRepository.save(schedule);
                return;
            }

            // Target tick is valid and within grace window
            String idempotencyKey = "cron:" + scheduleId + ":" + targetTickZdt.toEpochSecond();

            // Check overlap policy
            if (schedule.getOverlapPolicy() == OverlapPolicy.SKIP && jobRepository.hasActiveJobsForSchedule(scheduleId)) {
                log.info("Schedule {} overlap skipped (active job exists, policy=SKIP)", scheduleId);
                schedule.setLastSkipReason(ScheduleSkipReason.OVERLAP_SKIP);
                schedule.setLastTickAt(now);
                advanceNextRunAt(schedule, cron, nowZdt, targetTickZdt);
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
                advanceNextRunAt(schedule, cron, nowZdt, targetTickZdt);
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
            schedule.setDisabledReason(null);
            advanceNextRunAt(schedule, cron, nowZdt, targetTickZdt);
            scheduleRepository.save(schedule);

            log.info("Schedule {} spawned job with key={}, nextRunAt={}", scheduleId, idempotencyKey, schedule.getNextRunAt());

        } catch (Exception e) {
            handleFailure(schedule, cron, nowZdt, targetTickZdt, e);
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
            ON CONFLICT (user_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
            """;

        jdbcTemplate.update(insertSql,
            jobId, schedule.getUserId(), schedule.getId(), queue, payloadJson, template.body(),
            priority, Timestamp.from(targetRunAt), maxAttempts, idempotencyKey, requestHash,
            Timestamp.from(now), Timestamp.from(now)
        );
    }

    private void advanceNextRunAt(ScheduleEntity schedule, CronExpression cron, ZonedDateTime nowZdt, ZonedDateTime targetTickZdt) {
        ZonedDateTime nextZdt = cron.next(nowZdt);
        if (nextZdt != null) {
            // DST Fall-Back Guard: If the next tick matches targetTickZdt local time (the repeated hour in EDT -> EST),
            // advance past the duplicate occurrence so the schedule fires strictly ONCE on that calendar day.
            if (targetTickZdt != null && nextZdt.toLocalDateTime().isEqual(targetTickZdt.toLocalDateTime())) {
                ZonedDateTime nextPass = cron.next(nextZdt);
                if (nextPass != null) {
                    nextZdt = nextPass;
                }
            }
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

    public void handleFailure(ScheduleEntity schedule, CronExpression cron, ZonedDateTime nowZdt,
                              ZonedDateTime targetTickZdt, Exception e) {
        boolean isPermanent = (e instanceof ValidationException
                || e instanceof tools.jackson.core.JacksonException
                || e instanceof IllegalArgumentException);

        String safeError = sanitizeErrorMessage(e);
        schedule.setLastError(safeError);

        if (isPermanent) {
            schedule.setEnabled(false);
            schedule.setDisabledReason("PERMANENT_VALIDATION_ERROR");
            log.error("Schedule {} auto-disabled due to permanent error: {}", schedule.getId(), safeError, e);
        } else {
            int failures = schedule.getConsecutiveFailures() + 1;
            schedule.setConsecutiveFailures(failures);
            log.error("Execution failure for schedule {}: {} (consecutive={})", schedule.getId(), safeError, failures, e);

            if (failures >= maxConsecutiveFailures) {
                schedule.setEnabled(false);
                schedule.setDisabledReason("MAX_CONSECUTIVE_FAILURES_EXCEEDED");
                log.error("Schedule {} auto-disabled after reaching {} consecutive failures", schedule.getId(), failures);
            }
        }

        advanceNextRunAt(schedule, cron, nowZdt, targetTickZdt);
        scheduleRepository.save(schedule);
    }

    private String sanitizeErrorMessage(Throwable t) {
        if (t == null) {
            return "Unknown error occurred";
        }
        if (t instanceof ValidationException) {
            return t.getMessage();
        }
        if (t instanceof tools.jackson.core.JacksonException) {
            return "Malformed job template payload";
        }
        if (t instanceof DataAccessException || t instanceof SQLException) {
            return "Database error during schedule execution";
        }
        return "Internal execution error during schedule processing";
    }
}
