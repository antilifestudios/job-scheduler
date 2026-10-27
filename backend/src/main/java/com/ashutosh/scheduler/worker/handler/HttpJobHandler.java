package com.ashutosh.scheduler.worker.handler;

import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.JobAttemptEntity;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.repo.JobAttemptRepository;
import com.ashutosh.scheduler.producer.dto.HttpPayloadDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

/**
 * Executes real outbound HTTP jobs using Java 21's native HttpClient.
 * Records detailed diagnostic history to the job_attempts audit log.
 */
@Component
public class HttpJobHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpJobHandler.class);
    private static final int MAX_SNIPPET_LENGTH = 4096; // Cap response snippet at 4 KB

    private static final Set<String> RESTRICTED_HEADERS = Set.of(
        "host", "content-length", "connection", "upgrade"
    );

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final JobAttemptRepository jobAttemptRepository;

    public HttpJobHandler(HttpClient httpClient,
                          ObjectMapper objectMapper,
                          JobAttemptRepository jobAttemptRepository) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.jobAttemptRepository = jobAttemptRepository;
    }

    @Override
    public String type() {
        return "HTTP";
    }

    @Override
    public JobExecutionResult execute(JobEntity job) {
        long startTime = System.currentTimeMillis();
        Instant startedAt = Instant.now();

        HttpPayloadDto payload;
        try {
            payload = objectMapper.readValue(job.getPayload(), HttpPayloadDto.class);
        } catch (Exception e) {
            String errorMsg = "Malformed JSON payload: " + e.getMessage();
            recordAttempt(job, startedAt, System.currentTimeMillis() - startTime, null, null, errorMsg);
            return JobExecutionResult.permanentFailure(errorMsg);
        }

        HttpRequest httpRequest;
        try {
            httpRequest = buildHttpRequest(payload, job.getBody());
        } catch (IllegalArgumentException e) {
            String errorMsg = "Invalid HTTP request configuration: " + e.getMessage();
            recordAttempt(job, startedAt, System.currentTimeMillis() - startTime, null, null, errorMsg);
            return JobExecutionResult.permanentFailure(errorMsg);
        }

        try {
            HttpResponse<String> response = httpClient.send(
                httpRequest,
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
            );

            long durationMs = System.currentTimeMillis() - startTime;
            int statusCode = response.statusCode();
            String snippet = truncateSnippet(response.body());

            recordAttempt(job, startedAt, durationMs, statusCode, snippet, null);

            log.info("HTTP call for job {} completed with status {} in {}ms", job.getId(), statusCode, durationMs);

            return classifyResponse(statusCode, snippet, durationMs);

        } catch (HttpTimeoutException e) {
            long durationMs = System.currentTimeMillis() - startTime;
            String errorMsg = "HTTP request timeout: " + e.getMessage();
            log.warn("Job {} timed out after {}ms: {}", job.getId(), durationMs, errorMsg);
            recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
            return JobExecutionResult.retryableFailure(errorMsg);

        } catch (SocketException e) {
            long durationMs = System.currentTimeMillis() - startTime;
            String errorMsg = "Network connection failure: " + e.getMessage();
            log.warn("Job {} connection error: {}", job.getId(), errorMsg);
            recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
            return JobExecutionResult.retryableFailure(errorMsg);

        } catch (InterruptedException e) {
            long durationMs = System.currentTimeMillis() - startTime;
            Thread.currentThread().interrupt();
            String errorMsg = "HTTP execution interrupted";
            recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
            return JobExecutionResult.retryableFailure(errorMsg);

        } catch (IOException e) {
            long durationMs = System.currentTimeMillis() - startTime;
            String errorMsg = "I/O error during HTTP execution: " + e.getMessage();
            log.error("Job {} I/O error: {}", job.getId(), errorMsg, e);
            recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
            return JobExecutionResult.retryableFailure(errorMsg);

        } catch (Exception e) {
            long durationMs = System.currentTimeMillis() - startTime;
            String errorMsg = "Unexpected error during HTTP call: " + e.getMessage();
            log.error("Job {} unexpected error: {}", job.getId(), errorMsg, e);
            recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
            return JobExecutionResult.retryableFailure(errorMsg);
        }
    }

    private HttpRequest buildHttpRequest(HttpPayloadDto payload, String body) {
        String method = (payload.method() != null && !payload.method().isBlank())
            ? payload.method().trim().toUpperCase()
            : "GET";

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(payload.url().trim()));

        int timeoutMs = (payload.timeoutMs() != null && payload.timeoutMs() > 0)
            ? Math.min(payload.timeoutMs(), 60000)
            : 10000;
        builder.timeout(Duration.ofMillis(timeoutMs));

        // Set request body based on HTTP method
        if (body != null && !body.isEmpty() && !method.equals("GET") && !method.equals("HEAD")) {
            builder.method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        } else {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        }

        // Add headers, skipping restricted ones
        if (payload.headers() != null) {
            payload.headers().forEach((name, value) -> {
                if (name != null && value != null && !RESTRICTED_HEADERS.contains(name.trim().toLowerCase())) {
                    builder.header(name.trim(), value.trim());
                }
            });
        }

        return builder.build();
    }

    private JobExecutionResult classifyResponse(int statusCode, String snippet, long durationMs) {
        if (statusCode >= 200 && statusCode < 300) {
            return JobExecutionResult.success("HTTP " + statusCode + " (" + durationMs + "ms)");
        } else if (statusCode == 429) {
            return JobExecutionResult.retryableFailure("HTTP 429 Too Many Requests (downstream rate limit): " + snippet);
        } else if (statusCode >= 500 && statusCode < 600) {
            return JobExecutionResult.retryableFailure("HTTP " + statusCode + " Server Error: " + snippet);
        } else if (statusCode >= 400 && statusCode < 500) {
            return JobExecutionResult.permanentFailure("HTTP " + statusCode + " Client Error: " + snippet);
        } else {
            return JobExecutionResult.permanentFailure("Unexpected HTTP status: " + statusCode);
        }
    }

    private void recordAttempt(JobEntity job, Instant startedAt, long durationMs,
                               Integer statusCode, String snippet, String error) {
        try {
            JobAttemptEntity attempt = new JobAttemptEntity(
                UuidV7.generate(),
                job.getId(),
                job.getAttempts(),
                startedAt,
                durationMs,
                statusCode,
                snippet,
                error
            );
            jobAttemptRepository.save(attempt);
        } catch (Exception e) {
            log.error("Failed to record job attempt for job {}: {}", job.getId(), e.getMessage());
        }
    }

    private String truncateSnippet(String body) {
        if (body == null) {
            return null;
        }
        if (body.length() <= MAX_SNIPPET_LENGTH) {
            return body;
        }
        return body.substring(0, MAX_SNIPPET_LENGTH) + " ... [TRUNCATED " + (body.length() - MAX_SNIPPET_LENGTH) + " bytes]";
    }
}
