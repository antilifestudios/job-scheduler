package com.ashutosh.scheduler.common.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(ValidationException ex) {
        return buildErrorResponse(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        return buildErrorResponse(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<Map<String, Object>> handleIdempotencyConflict(IdempotencyConflictException ex) {
        return buildErrorResponse(HttpStatus.CONFLICT, ex.getMessage());
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(ResourceNotFoundException ex) {
        return buildErrorResponse(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(QuotaExceededException.class)
    public ResponseEntity<Map<String, Object>> handleQuotaExceeded(QuotaExceededException ex) {
        // Notice: strictly NO Retry-After or X-RateLimit-* headers on quota limits
        Map<String, Object> body = Map.of(
            "timestamp", Instant.now().toString(),
            "status", HttpStatus.TOO_MANY_REQUESTS.value(),
            "code", "quota_exceeded",
            "message", ex.getMessage(),
            "maxActiveJobs", ex.getMaxActiveJobs(),
            "activeJobs", ex.getActiveJobs()
        );
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(body);
    }

    @ExceptionHandler(RateLimitException.class)
    public ResponseEntity<Map<String, Object>> handleRateLimit(RateLimitException ex) {
        // Retry-After and X-RateLimit-* headers are emitted exclusively for rate limits
        Map<String, Object> body = Map.of(
            "timestamp", Instant.now().toString(),
            "status", HttpStatus.TOO_MANY_REQUESTS.value(),
            "code", "rate_limited",
            "message", ex.getMessage(),
            "retryAfterSeconds", ex.getRetryAfterSeconds()
        );
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .header("Retry-After", String.valueOf(ex.getRetryAfterSeconds()))
            .header("X-RateLimit-Limit", String.valueOf(ex.getLimit()))
            .header("X-RateLimit-Remaining", String.valueOf(ex.getRemaining()))
            .header("X-RateLimit-Reset", String.valueOf(ex.getResetEpochSeconds()))
            .body(body);
    }

    @ExceptionHandler(MaxApiKeysExceededException.class)
    public ResponseEntity<Map<String, Object>> handleMaxKeys(MaxApiKeysExceededException ex) {
        Map<String, Object> body = Map.of(
            "timestamp", Instant.now().toString(),
            "status", HttpStatus.CONFLICT.value(),
            "code", "max_keys_exceeded",
            "message", ex.getMessage()
        );
        return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }

    @ExceptionHandler(CannotRevokeLastKeyException.class)
    public ResponseEntity<Map<String, Object>> handleCannotRevokeLastKey(CannotRevokeLastKeyException ex) {
        Map<String, Object> body = Map.of(
            "timestamp", Instant.now().toString(),
            "status", HttpStatus.CONFLICT.value(),
            "code", "cannot_revoke_last_key",
            "message", ex.getMessage()
        );
        return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }

    @ExceptionHandler(JobCancellationException.class)
    public ResponseEntity<Map<String, Object>> handleJobCancellation(JobCancellationException ex) {
        Map<String, Object> body = Map.of(
            "timestamp", Instant.now().toString(),
            "status", HttpStatus.CONFLICT.value(),
            "code", "job_cancellation_conflict",
            "message", ex.getMessage()
        );
        return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }

    @ExceptionHandler(JobOverlapException.class)
    public ResponseEntity<Map<String, Object>> handleJobOverlap(JobOverlapException ex) {
        Map<String, Object> body = Map.of(
            "timestamp", Instant.now().toString(),
            "status", HttpStatus.CONFLICT.value(),
            "code", "schedule_overlap",
            "message", ex.getMessage()
        );
        return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }

    @ExceptionHandler(MaxSchedulesExceededException.class)
    public ResponseEntity<Map<String, Object>> handleMaxSchedulesExceeded(MaxSchedulesExceededException ex) {
        // Aligned with job quota error contract (HTTP 429, code: quota_exceeded, strictly NO rate-limit headers)
        Map<String, Object> body = Map.of(
            "timestamp", Instant.now().toString(),
            "status", HttpStatus.TOO_MANY_REQUESTS.value(),
            "code", "quota_exceeded",
            "message", ex.getMessage(),
            "maxSchedules", ex.getMaxSchedules(),
            "currentSchedules", ex.getCurrentSchedules()
        );
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(body);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneric(Exception ex) {
        log.error("Unhandled internal server error: {}", ex.getMessage(), ex);
        return buildErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, "An internal server error occurred.");
    }

    private ResponseEntity<Map<String, Object>> buildErrorResponse(HttpStatus status, String message) {
        Map<String, Object> body = Map.of(
            "timestamp", Instant.now().toString(),
            "status", status.value(),
            "error", status.getReasonPhrase(),
            "message", message
        );
        return ResponseEntity.status(status).body(body);
    }
}
