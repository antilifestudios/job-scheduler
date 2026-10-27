package com.ashutosh.scheduler.producer.service;

import com.ashutosh.scheduler.common.exception.JobOverlapException;
import com.ashutosh.scheduler.common.exception.MaxSchedulesExceededException;
import com.ashutosh.scheduler.common.exception.QuotaExceededException;
import com.ashutosh.scheduler.common.exception.ResourceNotFoundException;
import com.ashutosh.scheduler.common.exception.ValidationException;
import com.ashutosh.scheduler.common.util.CryptoUtils;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.entity.ScheduleEntity;
import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import com.ashutosh.scheduler.domain.enums.MisfirePolicy;
import com.ashutosh.scheduler.domain.enums.OverlapPolicy;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.ScheduleRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import com.ashutosh.scheduler.producer.dto.CreateJobRequest;
import com.ashutosh.scheduler.producer.dto.CreateScheduleRequest;
import com.ashutosh.scheduler.producer.dto.HttpPayloadDto;
import com.ashutosh.scheduler.producer.dto.JobResponse;
import com.ashutosh.scheduler.producer.dto.JobTemplateDto;
import com.ashutosh.scheduler.producer.dto.PageResponse;
import com.ashutosh.scheduler.producer.dto.ScheduleResponse;
import com.ashutosh.scheduler.producer.dto.UpdateScheduleRequest;
import com.ashutosh.scheduler.producer.validator.ScheduleValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

@Service
public class ScheduleService {

    private static final Logger log = LoggerFactory.getLogger(ScheduleService.class);

    private final ScheduleRepository scheduleRepository;
    private final JobRepository jobRepository;
    private final UserRepository userRepository;
    private final ScheduleValidator scheduleValidator;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public ScheduleService(
            ScheduleRepository scheduleRepository,
            JobRepository jobRepository,
            UserRepository userRepository,
            ScheduleValidator scheduleValidator,
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper) {
        this.scheduleRepository = scheduleRepository;
        this.jobRepository = jobRepository;
        this.userRepository = userRepository;
        this.scheduleValidator = scheduleValidator;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ScheduleResponse createSchedule(UUID userId, CreateScheduleRequest request) {
        String normalizedCron = scheduleValidator.validateAndNormalizeCreate(userId, request);
        ZoneId zoneId = scheduleValidator.validateTimezone(request.timezone());

        CronExpression cron = CronExpression.parse(normalizedCron);
        ZonedDateTime nowZdt = ZonedDateTime.now(zoneId);
        ZonedDateTime firstRunZdt = cron.next(nowZdt);
        if (firstRunZdt == null) {
            throw new ValidationException("Cron expression has no future fire times.");
        }

        // 1. Lock the tenant row in the transaction to serialize concurrent schedule creations for this tenant
        UserEntity user = userRepository.findByIdForUpdate(userId)
            .orElseThrow(() -> new ResourceNotFoundException("User not found: " + userId));

        // 2. Enforce schedule quota under the tenant row lock
        long currentCount = scheduleRepository.countByUserId(userId);
        int maxSchedules = scheduleValidator.getMaxSchedulesPerTenant();
        if (currentCount >= maxSchedules) {
            throw new MaxSchedulesExceededException(
                "Active schedule quota of " + maxSchedules + " exceeded. Current schedules: " + currentCount + ".",
                maxSchedules,
                currentCount
            );
        }

        String templateJson;
        try {
            templateJson = objectMapper.writeValueAsString(request.jobTemplate());
        } catch (Exception e) {
            throw new ValidationException("Failed to serialize jobTemplate: " + e.getMessage());
        }

        ScheduleEntity entity = new ScheduleEntity();
        entity.setId(UuidV7.generate());
        entity.setUserId(userId);
        entity.setCronExpression(normalizedCron);
        entity.setTimezone(zoneId.getId());
        entity.setOverlapPolicy(request.overlapPolicy() != null ? request.overlapPolicy() : OverlapPolicy.SKIP);
        entity.setMisfirePolicy(request.misfirePolicy() != null ? request.misfirePolicy() : MisfirePolicy.FIRE_ONCE);
        entity.setGraceWindowSeconds(request.graceWindowSeconds() != null ? request.graceWindowSeconds() : 300);
        entity.setJobTemplate(templateJson);
        entity.setNextRunAt(firstRunZdt.toInstant());
        entity.setEnabled(true);
        entity.setCreatedAt(Instant.now());
        entity.setUpdatedAt(Instant.now());

        ScheduleEntity saved = scheduleRepository.save(entity);
        log.info("Schedule created: id={}, userId={}, cron={}, nextRunAt={}",
            saved.getId(), userId, saved.getCronExpression(), saved.getNextRunAt());

        return ScheduleResponse.fromEntity(saved);
    }

    @Transactional(readOnly = true)
    public ScheduleResponse getSchedule(UUID userId, UUID scheduleId) {
        ScheduleEntity entity = scheduleRepository.findByIdAndUserId(scheduleId, userId)
            .orElseThrow(() -> new ResourceNotFoundException("Schedule not found: " + scheduleId));
        return ScheduleResponse.fromEntity(entity);
    }

    @Transactional(readOnly = true)
    public PageResponse<ScheduleResponse> listSchedules(UUID userId, Pageable pageable) {
        Page<ScheduleEntity> page = scheduleRepository.findByUserIdOrderByCreatedAtDesc(userId, pageable);
        return PageResponse.from(page.map(ScheduleResponse::fromEntity));
    }

    @Transactional(readOnly = true)
    public PageResponse<JobResponse> listScheduleJobs(UUID userId, UUID scheduleId, Pageable pageable) {
        // Verify tenant owns schedule
        scheduleRepository.findByIdAndUserId(scheduleId, userId)
            .orElseThrow(() -> new ResourceNotFoundException("Schedule not found: " + scheduleId));

        Page<JobEntity> page = jobRepository.findByScheduleIdOrderByCreatedAtDesc(scheduleId, pageable);
        return PageResponse.from(page.map(job -> JobResponse.fromEntity(job, objectMapper)));
    }

    @Transactional
    public ScheduleResponse patchSchedule(UUID userId, UUID scheduleId, UpdateScheduleRequest request) {
        String normalizedCron = scheduleValidator.validateAndNormalizeUpdate(request);

        // Lock order: schedule row first
        ScheduleEntity schedule = scheduleRepository.findByIdAndUserIdForUpdate(scheduleId, userId)
            .orElseThrow(() -> new ResourceNotFoundException("Schedule not found: " + scheduleId));

        boolean cronOrTzChanged = false;

        if (request.timezone() != null) {
            ZoneId zoneId = scheduleValidator.validateTimezone(request.timezone());
            schedule.setTimezone(zoneId.getId());
            cronOrTzChanged = true;
        }

        if (normalizedCron != null) {
            schedule.setCronExpression(normalizedCron);
            cronOrTzChanged = true;
        }

        if (request.overlapPolicy() != null) {
            schedule.setOverlapPolicy(request.overlapPolicy());
        }

        if (request.misfirePolicy() != null) {
            schedule.setMisfirePolicy(request.misfirePolicy());
        }

        if (request.graceWindowSeconds() != null) {
            schedule.setGraceWindowSeconds(request.graceWindowSeconds());
        }

        if (request.jobTemplate() != null) {
            try {
                schedule.setJobTemplate(objectMapper.writeValueAsString(request.jobTemplate()));
            } catch (Exception e) {
                throw new ValidationException("Failed to serialize jobTemplate: " + e.getMessage());
            }
        }

        // Handle pause/resume
        if (request.enabled() != null) {
            boolean wasDisabled = !schedule.isEnabled();
            schedule.setEnabled(request.enabled());
            if (wasDisabled && request.enabled()) {
                // Resume recalculates next_run_at strictly from now and clears circuit-breaker state
                cronOrTzChanged = true;
                schedule.setDisabledReason(null);
                schedule.setConsecutiveFailures(0);
                schedule.setLastError(null);
            }
        }

        if (cronOrTzChanged && schedule.isEnabled()) {
            ZoneId zoneId = ZoneId.of(schedule.getTimezone());
            CronExpression cron = CronExpression.parse(schedule.getCronExpression());
            ZonedDateTime nextZdt = cron.next(ZonedDateTime.now(zoneId));
            if (nextZdt != null) {
                schedule.setNextRunAt(nextZdt.toInstant());
            }
        }

        schedule.setUpdatedAt(Instant.now());
        ScheduleEntity saved = scheduleRepository.save(schedule);
        log.info("Schedule updated: id={}, nextRunAt={}, enabled={}", saved.getId(), saved.getNextRunAt(), saved.isEnabled());

        return ScheduleResponse.fromEntity(saved);
    }

    @Transactional
    public void deleteSchedule(UUID userId, UUID scheduleId) {
        ScheduleEntity schedule = scheduleRepository.findByIdAndUserIdForUpdate(scheduleId, userId)
            .orElseThrow(() -> new ResourceNotFoundException("Schedule not found: " + scheduleId));

        scheduleRepository.delete(schedule);
        log.info("Schedule deleted: id={}, userId={}. Historical jobs preserved with schedule_id=NULL.", scheduleId, userId);
    }

    @Transactional
    public JobResponse triggerSchedule(UUID userId, UUID scheduleId, String headerIdempotencyKey) {
        // Enforce lock order: 1. Schedule row FOR UPDATE
        ScheduleEntity schedule = scheduleRepository.findByIdAndUserIdForUpdate(scheduleId, userId)
            .orElseThrow(() -> new ResourceNotFoundException("Schedule not found: " + scheduleId));

        // Enforce lock order: 2. Tenant row FOR UPDATE
        UserEntity user = userRepository.findByIdForUpdate(userId)
            .orElseThrow(() -> new ResourceNotFoundException("User not found: " + userId));

        // Check overlap policy
        if (schedule.getOverlapPolicy() == OverlapPolicy.SKIP && jobRepository.hasActiveJobsForSchedule(scheduleId)) {
            throw new JobOverlapException("Schedule has an active job in progress and overlap_policy is SKIP.");
        }

        // Check tenant active job quota
        long activeJobs = jobRepository.countActiveJobsByUserId(userId);
        if (activeJobs >= user.getMaxActiveJobs()) {
            throw new QuotaExceededException(
                "Active job quota exceeded: " + activeJobs + "/" + user.getMaxActiveJobs() + " active jobs.",
                user.getMaxActiveJobs(),
                activeJobs
            );
        }

        // Idempotency key handling (must not start with cron:)
        String effectiveKey;
        if (headerIdempotencyKey != null && !headerIdempotencyKey.isBlank()) {
            if (headerIdempotencyKey.trim().startsWith("cron:")) {
                throw new ValidationException("The 'cron:' idempotency key prefix is reserved for recurring schedule executions.");
            }
            effectiveKey = headerIdempotencyKey.trim();
        } else {
            effectiveKey = "trigger:" + scheduleId + ":" + UuidV7.generate();
        }

        // Parse template
        JobTemplateDto template;
        try {
            template = objectMapper.readValue(schedule.getJobTemplate(), JobTemplateDto.class);
        } catch (Exception e) {
            throw new IllegalStateException("Corrupted jobTemplate in schedule: " + e.getMessage(), e);
        }

        // Validate template at spawn
        scheduleValidator.validateJobTemplate(template);

        String method = (template.method() == null || template.method().isBlank()) ? "POST" : template.method().trim().toUpperCase();
        String queue = (template.queue() == null || template.queue().isBlank()) ? "default" : template.queue().trim();
        int priority = template.priority() == null ? 0 : template.priority();
        int maxAttempts = template.maxAttempts() == null ? 3 : template.maxAttempts();
        Map<String, String> headers = template.headers() == null ? Map.of() : template.headers();

        String requestHash = CryptoUtils.computeRequestHash(method, template.url(), headers, template.body());
        HttpPayloadDto payloadDto = new HttpPayloadDto(template.url(), method, headers, null);
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payloadDto);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize payload", e);
        }

        UUID jobId = UuidV7.generate();
        Instant now = Instant.now();

        String insertSql = """
            INSERT INTO jobs (id, user_id, schedule_id, queue, type, payload, body, status, priority,
                              run_at, attempts, max_attempts, idempotency_key, request_hash, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'HTTP', ?::jsonb, ?, 'PENDING', ?, ?, 0, ?, ?, ?, ?, ?)
            ON CONFLICT (user_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
            """;

        jdbcTemplate.update(insertSql,
            jobId, userId, scheduleId, queue, payloadJson, template.body(),
            priority, Timestamp.from(now), maxAttempts, effectiveKey, requestHash,
            Timestamp.from(now), Timestamp.from(now)
        );

        schedule.setLastFiredAt(now);
        schedule.setLastTickAt(now);
        schedule.setConsecutiveFailures(0);
        schedule.setLastError(null);
        scheduleRepository.save(schedule);

        JobEntity job = jobRepository.findById(jobId)
            .orElseGet(() -> jobRepository.findByUserIdAndIdempotencyKey(userId, effectiveKey)
                .orElseThrow(() -> new IllegalStateException("Failed to retrieve triggered job")));

        log.info("Schedule manually triggered: scheduleId={}, jobId={}, idempotencyKey={}", scheduleId, job.getId(), effectiveKey);
        return JobResponse.fromEntity(job, objectMapper);
    }
}
