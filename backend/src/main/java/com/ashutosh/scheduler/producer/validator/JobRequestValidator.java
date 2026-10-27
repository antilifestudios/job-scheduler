package com.ashutosh.scheduler.producer.validator;

import com.ashutosh.scheduler.common.exception.ValidationException;
import com.ashutosh.scheduler.producer.dto.CreateJobRequest;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * Validates inbound job creation requests.
 */
@Component
public class JobRequestValidator {

    private static final int MAX_URL_LENGTH = 2048;
    private static final int MAX_HEADER_COUNT = 50;
    private static final int MAX_BODY_BYTES = 65536; // 64 KB
    private static final Duration MAX_FUTURE_SCHEDULE = Duration.ofDays(365);

    private static final Set<String> ALLOWED_METHODS = Set.of(
        "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"
    );

    private static final Set<String> FORBIDDEN_HEADERS = Set.of(
        "host", "content-length", "transfer-encoding", "connection"
    );

    public void validate(CreateJobRequest request, String headerIdempotencyKey) {
        if (request == null) {
            throw new ValidationException("Request body cannot be null.");
        }

        // 1. Idempotency Key agreement check
        validateIdempotencyKeys(request.idempotencyKey(), headerIdempotencyKey);

        // 2. URL syntax validation
        validateUrl(request.url());

        // 3. HTTP Method validation
        validateMethod(request.method());

        // 4. Headers validation
        validateHeaders(request.headers());

        // 5. Body validation (UTF-8 text only, no NUL bytes)
        validateBody(request.body());

        // 6. Priority validation (0-10)
        if (request.priority() != null && (request.priority() < 0 || request.priority() > 10)) {
            throw new ValidationException("Priority must be between 0 and 10 inclusive.");
        }

        // 7. Max attempts validation (1-20)
        if (request.maxAttempts() != null && (request.maxAttempts() < 1 || request.maxAttempts() > 20)) {
            throw new ValidationException("Max attempts must be between 1 and 20.");
        }

        // 8. Bounded runAt (at most 1 year in the future)
        if (request.runAt() != null) {
            Instant maxAllowed = Instant.now().plus(MAX_FUTURE_SCHEDULE);
            if (request.runAt().isAfter(maxAllowed)) {
                throw new ValidationException("runAt cannot be scheduled more than 365 days into the future.");
            }
        }
    }

    private void validateIdempotencyKeys(String bodyKey, String headerKey) {
        if (bodyKey != null && !bodyKey.isBlank() && headerKey != null && !headerKey.isBlank()) {
            if (!bodyKey.trim().equals(headerKey.trim())) {
                throw new ValidationException("Header 'Idempotency-Key' and JSON field 'idempotencyKey' must match if both are provided.");
            }
        }
    }

    private void validateUrl(String url) {
        if (url == null || url.isBlank()) {
            throw new ValidationException("URL is required and cannot be blank.");
        }
        if (url.length() > MAX_URL_LENGTH) {
            throw new ValidationException("URL length exceeds maximum allowed limit of " + MAX_URL_LENGTH + " characters.");
        }

        // NOTE: URL validation verifies syntax and URI structure only.
        // It does NOT constitute SSRF protection. SSRF protection is strictly
        // enforced at execution time via DNS pre-resolution and IP filtering in Phase 6.
        try {
            URI uri = URI.create(url.trim());
            String scheme = uri.getScheme();
            if (scheme == null || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
                throw new ValidationException("URL must use http or https scheme.");
            }
            if (uri.getHost() == null || uri.getHost().isBlank()) {
                throw new ValidationException("URL must contain a valid host.");
            }
        } catch (IllegalArgumentException e) {
            throw new ValidationException("Invalid URL syntax: " + e.getMessage());
        }
    }

    private void validateMethod(String method) {
        if (method != null && !method.isBlank()) {
            if (!ALLOWED_METHODS.contains(method.trim().toUpperCase())) {
                throw new ValidationException("Unsupported HTTP method: " + method + ". Allowed methods: " + ALLOWED_METHODS);
            }
        }
    }

    private void validateHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return;
        }

        if (headers.size() > MAX_HEADER_COUNT) {
            throw new ValidationException("Header count exceeds maximum allowed limit of " + MAX_HEADER_COUNT);
        }

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();

            if (name == null || name.isBlank()) {
                throw new ValidationException("Header name cannot be blank.");
            }

            // Reject CR / LF in header names and values to prevent HTTP response splitting
            if (name.contains("\r") || name.contains("\n")) {
                throw new ValidationException("CR/LF characters are strictly prohibited in header names.");
            }
            if (value != null && (value.contains("\r") || value.contains("\n"))) {
                throw new ValidationException("CR/LF characters are strictly prohibited in header values.");
            }

            String lowerName = name.trim().toLowerCase();
            if (FORBIDDEN_HEADERS.contains(lowerName) || lowerName.startsWith("proxy-")) {
                throw new ValidationException("Restricted header is not allowed: " + name);
            }
        }
    }

    private void validateBody(String body) {
        if (body == null) {
            return;
        }

        // Support is UTF-8 text only (no binary, no NUL bytes)
        if (body.indexOf('\0') >= 0) {
            throw new ValidationException("NUL byte (\\0) is not supported in text body.");
        }

        if (body.length() > MAX_BODY_BYTES) {
            throw new ValidationException("Body size exceeds maximum allowed limit of " + MAX_BODY_BYTES + " characters.");
        }
    }
}
