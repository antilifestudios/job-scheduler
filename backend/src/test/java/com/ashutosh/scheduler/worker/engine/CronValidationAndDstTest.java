package com.ashutosh.scheduler.worker.engine;

import com.ashutosh.scheduler.common.exception.ValidationException;
import com.ashutosh.scheduler.domain.repo.ScheduleRepository;
import com.ashutosh.scheduler.producer.dto.CreateJobRequest;
import com.ashutosh.scheduler.producer.dto.CreateScheduleRequest;
import com.ashutosh.scheduler.producer.dto.JobTemplateDto;
import com.ashutosh.scheduler.producer.validator.JobRequestValidator;
import com.ashutosh.scheduler.producer.validator.ScheduleValidator;
import com.ashutosh.scheduler.worker.ssrf.SsrfValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.scheduling.support.CronExpression;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class CronValidationAndDstTest {

    private ScheduleValidator scheduleValidator;
    private JobRequestValidator jobRequestValidator;
    private ScheduleRepository scheduleRepository;

    @BeforeEach
    void setUp() {
        com.ashutosh.scheduler.worker.engine.WorkerProperties properties = new com.ashutosh.scheduler.worker.engine.WorkerProperties();
        properties.setSsrfEnabled(true);
        properties.setSsrfAllowPrivateIps(false);
        org.springframework.mock.env.MockEnvironment environment = new org.springframework.mock.env.MockEnvironment();
        environment.setActiveProfiles("prod");

        SsrfValidator ssrfValidator = new SsrfValidator(properties, environment);
        jobRequestValidator = new JobRequestValidator(ssrfValidator);
        scheduleRepository = Mockito.mock(ScheduleRepository.class);
        when(scheduleRepository.countByUserId(any())).thenReturn(0L);

        scheduleValidator = new ScheduleValidator(
            jobRequestValidator,
            scheduleRepository,
            20,
            60
        );
    }

    @Test
    @DisplayName("Fixed 0 seconds field required: sub-minute expressions rejected, 5-field normalized, macros accepted")
    void testCronSyntaxAndNormalization() {
        UUID userId = UUID.randomUUID();
        JobTemplateDto validTemplate = new JobTemplateDto(
            "https://example.com/webhook", "POST", Map.of(), "{\"data\":1}", "default", 0, 3
        );

        // 1. Sub-minute 6-field with non-zero seconds -> REJECTED
        CreateScheduleRequest subMinuteRequest = new CreateScheduleRequest(
            "*/10 * * * * *", "UTC", null, null, null, validTemplate
        );
        assertThatThrownBy(() -> scheduleValidator.validateAndNormalizeCreate(userId, subMinuteRequest))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("seconds field must be '0'");

        // 2. Standard 5-field crontab -> NORMALIZED to 6 fields starting with 0
        CreateScheduleRequest fiveFieldRequest = new CreateScheduleRequest(
            "*/5 * * * *", "UTC", null, null, null, validTemplate
        );
        String normalizedFive = scheduleValidator.validateAndNormalizeCreate(userId, fiveFieldRequest);
        assertThat(normalizedFive).isEqualTo("0 */5 * * * *");

        // 3. Explicit 6-field starting with 0 -> ACCEPTED
        CreateScheduleRequest sixFieldRequest = new CreateScheduleRequest(
            "0 30 8 * * MON-FRI", "UTC", null, null, null, validTemplate
        );
        String normalizedSix = scheduleValidator.validateAndNormalizeCreate(userId, sixFieldRequest);
        assertThat(normalizedSix).isEqualTo("0 30 8 * * MON-FRI");

        // 4. Built-in Spring macro (@hourly, @daily) -> ACCEPTED
        CreateScheduleRequest macroRequest = new CreateScheduleRequest(
            "@hourly", "UTC", null, null, null, validTemplate
        );
        String normalizedMacro = scheduleValidator.validateAndNormalizeCreate(userId, macroRequest);
        assertThat(normalizedMacro).isEqualTo("@hourly");
    }

    @Test
    @DisplayName("Minimum interval heuristic: rejects schedules firing more frequently than 60 seconds")
    void testMinimumIntervalHeuristic() {
        ZoneId zoneId = ZoneId.of("UTC");

        // A cron expression that fires every 10 seconds (if parsed raw) violates minimum interval
        assertThatThrownBy(() -> scheduleValidator.validateAndNormalizeCron("0,10,20,30,40,50 * * * * *", zoneId))
            .isInstanceOf(ValidationException.class);
    }

    @Test
    @DisplayName("IANA zone IDs only: accepts valid IANA IDs, rejects fixed offsets and invalid strings")
    void testIanaTimezoneValidation() {
        // Valid IANA IDs
        assertThat(scheduleValidator.validateTimezone("UTC").getId()).isEqualTo("UTC");
        assertThat(scheduleValidator.validateTimezone("America/New_York").getId()).isEqualTo("America/New_York");
        assertThat(scheduleValidator.validateTimezone("Asia/Kolkata").getId()).isEqualTo("Asia/Kolkata");
        assertThat(scheduleValidator.validateTimezone("Europe/London").getId()).isEqualTo("Europe/London");

        // Fixed offsets -> REJECTED
        assertThatThrownBy(() -> scheduleValidator.validateTimezone("+05:30"))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Must be a recognized IANA zone ID");

        assertThatThrownBy(() -> scheduleValidator.validateTimezone("-04:00"))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Must be a recognized IANA zone ID");

        assertThatThrownBy(() -> scheduleValidator.validateTimezone("UTC+2"))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Must be a recognized IANA zone ID");

        assertThatThrownBy(() -> scheduleValidator.validateTimezone("Invalid/Unknown"))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Must be a recognized IANA zone ID");
    }

    @Test
    @DisplayName("Reserved cron: prefix in job idempotency keys is rejected")
    void testReservedCronPrefixRejected() {
        CreateJobRequest request = new CreateJobRequest(
            "https://example.com/webhook", "POST", Map.of(), "{\"data\":1}", "default", 0, null, 3, "cron:fake-schedule:12345"
        );
        assertThatThrownBy(() -> jobRequestValidator.validate(request, null))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("The 'cron:' idempotency key prefix is reserved");

        CreateJobRequest headerRequest = new CreateJobRequest(
            "https://example.com/webhook", "POST", Map.of(), "{\"data\":1}", "default", 0, null, 3, null
        );
        assertThatThrownBy(() -> jobRequestValidator.validate(headerRequest, "cron:header-key:12345"))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("The 'cron:' idempotency key prefix is reserved");
    }

    @Test
    @DisplayName("Job template validation at create: rejects SSRF target, bad methods, and oversized body")
    void testJobTemplateValidation() {
        // SSRF Target
        JobTemplateDto ssrfTemplate = new JobTemplateDto(
            "http://127.0.0.1:8080/private", "POST", Map.of(), null, "default", 0, 3
        );
        assertThatThrownBy(() -> scheduleValidator.validateJobTemplate(ssrfTemplate))
            .isInstanceOf(ValidationException.class);

        // Unsupported method
        JobTemplateDto badMethodTemplate = new JobTemplateDto(
            "https://example.com/api", "TRACE", Map.of(), null, "default", 0, 3
        );
        assertThatThrownBy(() -> scheduleValidator.validateJobTemplate(badMethodTemplate))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("Unsupported HTTP method");

        // Oversized body (> 64 KB)
        String bigBody = "x".repeat(70000);
        JobTemplateDto bigBodyTemplate = new JobTemplateDto(
            "https://example.com/api", "POST", Map.of(), bigBody, "default", 0, 3
        );
        assertThatThrownBy(() -> scheduleValidator.validateJobTemplate(bigBodyTemplate))
            .isInstanceOf(ValidationException.class)
            .hasMessageContaining("jobTemplate.body size exceeds maximum limit");
    }

    @Test
    @DisplayName("DST Transition Behavior: spring-forward gap jump and fall-back single execution with fixed 2026 dates")
    void testDstTransitionsSpringForwardAndFallBack() {
        ZoneId nyZone = ZoneId.of("America/New_York");

        // 1. Spring-Forward in America/New_York on Sunday, March 8, 2026:
        // Clocks skip from 01:59:59 to 03:00:00 (02:30 does not exist on the local clock).
        CronExpression daily230am = CronExpression.parse("0 30 2 * * *");

        // Reference point: 2026-03-07 02:30:00 EST
        ZonedDateTime preTransition = ZonedDateTime.of(
            LocalDateTime.of(2026, 3, 7, 2, 30, 0), nyZone
        );

        ZonedDateTime nextSpring = daily230am.next(preTransition);
        assertThat(nextSpring).isNotNull();
        // Standard Java ZonedDateTime adjusts non-existent wall-clock time forward to 03:30 EDT
        assertThat(nextSpring.toLocalDate().toString()).isEqualTo("2026-03-08");
        assertThat(nextSpring.getHour()).isEqualTo(3);
        assertThat(nextSpring.getMinute()).isEqualTo(30);

        // 2. Fall-Back in America/New_York on Sunday, November 1, 2026:
        // Clocks fall back from 02:00:00 to 01:00:00 (the 01:xx hour is repeated).
        CronExpression daily130am = CronExpression.parse("0 30 1 * * *");

        // Reference point: 2026-10-31 01:30:00 EDT
        ZonedDateTime preFallBack = ZonedDateTime.of(
            LocalDateTime.of(2026, 10, 31, 1, 30, 0), nyZone
        );

        ZonedDateTime fallBackTick = daily130am.next(preFallBack);
        assertThat(fallBackTick).isNotNull();
        assertThat(fallBackTick.toLocalDate().toString()).isEqualTo("2026-11-01");
        assertThat(fallBackTick.getHour()).isEqualTo(1);
        assertThat(fallBackTick.getMinute()).isEqualTo(30);

        // Subsequent tick advances strictly to the next calendar day (2026-11-02), NOT repeating on the same day
        ZonedDateTime postFallBackTick = daily130am.next(fallBackTick);
        assertThat(postFallBackTick).isNotNull();
        assertThat(postFallBackTick.toLocalDate().toString()).isEqualTo("2026-11-02");
        assertThat(postFallBackTick.getHour()).isEqualTo(1);
        assertThat(postFallBackTick.getMinute()).isEqualTo(30);
    }
}
