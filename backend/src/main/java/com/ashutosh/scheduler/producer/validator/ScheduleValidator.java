package com.ashutosh.scheduler.producer.validator;

import com.ashutosh.scheduler.common.exception.MaxSchedulesExceededException;
import com.ashutosh.scheduler.common.exception.ValidationException;
import com.ashutosh.scheduler.domain.repo.ScheduleRepository;
import com.ashutosh.scheduler.producer.dto.CreateJobRequest;
import com.ashutosh.scheduler.producer.dto.CreateScheduleRequest;
import com.ashutosh.scheduler.producer.dto.JobTemplateDto;
import com.ashutosh.scheduler.producer.dto.UpdateScheduleRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

@Component
public class ScheduleValidator {

    private static final int DEFAULT_MAX_SCHEDULES_PER_TENANT = 20;
    private static final int DEFAULT_MIN_INTERVAL_SECONDS = 60;
    private static final int CHECK_AHEAD_TICKS = 5;
    private static final int MAX_TEMPLATE_BYTES = 65536; // 64 KB

    private final JobRequestValidator jobRequestValidator;
    private final ScheduleRepository scheduleRepository;
    private final int maxSchedulesPerTenant;
    private final int minIntervalSeconds;

    public ScheduleValidator(
            JobRequestValidator jobRequestValidator,
            ScheduleRepository scheduleRepository,
            @Value("${scheduler.cron.max-schedules-per-tenant:20}") int maxSchedulesPerTenant,
            @Value("${scheduler.cron.min-interval-seconds:60}") int minIntervalSeconds) {
        this.jobRequestValidator = jobRequestValidator;
        this.scheduleRepository = scheduleRepository;
        this.maxSchedulesPerTenant = maxSchedulesPerTenant;
        this.minIntervalSeconds = minIntervalSeconds;
    }

    public String validateAndNormalizeCreate(UUID userId, CreateScheduleRequest request) {
        if (request == null) {
            throw new ValidationException("Schedule request body cannot be null.");
        }

        // 1. Validate timezone
        ZoneId zoneId = validateTimezone(request.timezone());

        // 3. Validate and normalize cron expression
        String normalizedCron = validateAndNormalizeCron(request.cronExpression(), zoneId);

        // 4. Validate job template
        validateJobTemplate(request.jobTemplate());

        // 5. Grace window validation
        if (request.graceWindowSeconds() != null && request.graceWindowSeconds() < 1) {
            throw new ValidationException("graceWindowSeconds must be at least 1 second.");
        }

        return normalizedCron;
    }

    public String validateAndNormalizeUpdate(UpdateScheduleRequest request) {
        if (request == null) {
            throw new ValidationException("Update request body cannot be null.");
        }

        ZoneId zoneId = null;
        if (request.timezone() != null) {
            zoneId = validateTimezone(request.timezone());
        }

        String normalizedCron = null;
        if (request.cronExpression() != null) {
            if (zoneId == null) {
                zoneId = ZoneId.of("UTC");
            }
            normalizedCron = validateAndNormalizeCron(request.cronExpression(), zoneId);
        }

        if (request.jobTemplate() != null) {
            validateJobTemplate(request.jobTemplate());
        }

        if (request.graceWindowSeconds() != null && request.graceWindowSeconds() < 1) {
            throw new ValidationException("graceWindowSeconds must be at least 1 second.");
        }

        return normalizedCron;
    }

    public ZoneId validateTimezone(String timezoneStr) {
        if (timezoneStr == null || timezoneStr.isBlank()) {
            return ZoneId.of("UTC");
        }
        String trimmed = timezoneStr.trim();
        // IANA zone IDs only; reject fixed offsets like +05:30, -04:00, UTC+2, GMT-5
        if (!ZoneId.getAvailableZoneIds().contains(trimmed)) {
            throw new ValidationException(
                "Invalid timezone: '" + timezoneStr + "'. Must be a recognized IANA zone ID (e.g. 'UTC', 'America/New_York', 'Asia/Kolkata')."
            );
        }
        return ZoneId.of(trimmed);
    }

    public String validateAndNormalizeCron(String cronExpr, ZoneId zoneId) {
        if (cronExpr == null || cronExpr.isBlank()) {
            throw new ValidationException("cronExpression is required.");
        }

        String trimmed = cronExpr.trim();
        String normalized;

        if (trimmed.startsWith("@")) {
            // Built-in Spring macro (@hourly, @daily, @weekly, @monthly, @yearly)
            normalized = trimmed;
        } else {
            String[] parts = trimmed.split("\\s+");
            if (parts.length == 5) {
                // Standard 5-field crontab: normalize to 6 fields with 0 seconds
                normalized = "0 " + trimmed;
            } else if (parts.length == 6) {
                if (!parts[0].equals("0")) {
                    throw new ValidationException(
                        "Sub-minute cron schedules are forbidden; the seconds field must be '0'."
                    );
                }
                normalized = trimmed;
            } else {
                throw new ValidationException(
                    "Invalid cron expression format. Expected 6 fields with '0' seconds or standard 5 fields."
                );
            }
        }

        CronExpression cron;
        try {
            cron = CronExpression.parse(normalized);
        } catch (IllegalArgumentException e) {
            throw new ValidationException("Invalid cron expression: " + e.getMessage());
        }

        // Forward-looking minimum interval heuristic check across the next N fire times
        ZonedDateTime cursor = ZonedDateTime.now(zoneId);
        for (int i = 0; i < CHECK_AHEAD_TICKS; i++) {
            ZonedDateTime next = cron.next(cursor);
            if (next == null) {
                break;
            }
            long intervalSeconds = Duration.between(cursor, next).getSeconds();
            if (i > 0 && intervalSeconds < minIntervalSeconds) {
                throw new ValidationException(
                    "Cron schedule interval (" + intervalSeconds + "s) violates minimum interval of " + minIntervalSeconds + " seconds."
                );
            }
            cursor = next;
        }

        return normalized;
    }

    public void validateJobTemplate(JobTemplateDto template) {
        if (template == null) {
            throw new ValidationException("jobTemplate cannot be null.");
        }
        if (template.body() != null && template.body().getBytes(StandardCharsets.UTF_8).length > MAX_TEMPLATE_BYTES) {
            throw new ValidationException("jobTemplate.body size exceeds maximum limit of " + MAX_TEMPLATE_BYTES + " bytes.");
        }

        // Reuse JobRequestValidator for URL, SSRF, Method, Headers, Priority, MaxAttempts
        CreateJobRequest mockRequest = new CreateJobRequest(
            template.url(),
            template.method(),
            template.headers(),
            template.body(),
            template.queue(),
            template.priority(),
            null,
            template.maxAttempts(),
            null
        );
        jobRequestValidator.validate(mockRequest, null);
    }

    public int getMaxSchedulesPerTenant() {
        return maxSchedulesPerTenant;
    }
}
