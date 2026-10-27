package com.ashutosh.scheduler.worker.handler;

import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.JobAttemptEntity;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.repo.JobAttemptRepository;
import com.ashutosh.scheduler.producer.dto.HttpPayloadDto;
import com.ashutosh.scheduler.worker.ssrf.SsrfException;
import com.ashutosh.scheduler.worker.ssrf.SsrfValidator;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import okio.BufferedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Executes real outbound HTTP jobs using OkHttp with single-resolution DNS pinning,
 * connection-level network interception, bounded timeouts, and secure manual redirect following.
 */
@Component
public class HttpJobHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpJobHandler.class);

    private static final int MAX_SNIPPET_LENGTH = 4096;
    private static final long MAX_READ_BYTES = 65536; // Cap raw response read at 64 KB
    private static final int MAX_REDIRECT_HOPS = 5;

    public static final String GENERIC_SSRF_ERROR = "Destination host is not reachable or request rejected";

    private static final Set<String> FORBIDDEN_INBOUND_HEADERS = Set.of(
        "host", "content-length", "connection", "upgrade"
    );

    private static final Set<String> PRESERVED_CROSS_ORIGIN_HEADERS = Set.of(
        "content-type", "accept", "user-agent"
    );

    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final JobAttemptRepository jobAttemptRepository;
    private final SsrfValidator ssrfValidator;

    public HttpJobHandler(OkHttpClient httpClient,
                          ObjectMapper objectMapper,
                          JobAttemptRepository jobAttemptRepository,
                          SsrfValidator ssrfValidator) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.jobAttemptRepository = jobAttemptRepository;
        this.ssrfValidator = ssrfValidator;
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

        // Fast-fail URL syntax and disallowed schemes/ports before initiating any network action
        try {
            ssrfValidator.validateUrl(payload.url());
        } catch (SsrfException e) {
            log.warn("Job {} failed initial SSRF URL validation: {}", job.getId(), e.getMessage());
            long durationMs = System.currentTimeMillis() - startTime;
            recordAttempt(job, startedAt, durationMs, null, null, GENERIC_SSRF_ERROR);
            return JobExecutionResult.permanentFailure(GENERIC_SSRF_ERROR);
        }

        int totalTimeoutMs = (payload.timeoutMs() != null && payload.timeoutMs() > 0)
            ? Math.min(payload.timeoutMs(), 60000)
            : 10000;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeoutMs);

        String initialMethod = (payload.method() != null && !payload.method().isBlank())
            ? payload.method().trim().toUpperCase()
            : "GET";

        Map<String, String> currentHeaders = new HashMap<>();
        if (payload.headers() != null) {
            payload.headers().forEach((k, v) -> {
                if (k != null && v != null && !FORBIDDEN_INBOUND_HEADERS.contains(k.trim().toLowerCase())) {
                    currentHeaders.put(k.trim(), v.trim());
                }
            });
        }

        String currentUrl = payload.url().trim();
        String currentMethod = initialMethod;
        String currentBody = job.getBody();

        int hopCount = 0;

        while (true) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                long durationMs = System.currentTimeMillis() - startTime;
                String errorMsg = "HTTP execution total deadline exceeded (" + totalTimeoutMs + "ms)";
                log.warn("Job {} timed out after {}ms", job.getId(), durationMs);
                recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
                return JobExecutionResult.retryableFailure(errorMsg);
            }

            Request request;
            try {
                request = buildRequest(currentUrl, currentMethod, currentHeaders, currentBody);
            } catch (Exception e) {
                long durationMs = System.currentTimeMillis() - startTime;
                String errorMsg = "Invalid HTTP request construction: " + e.getMessage();
                recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
                return JobExecutionResult.permanentFailure(errorMsg);
            }

            // Derive per-hop timeout from the remaining total deadline
            OkHttpClient hopClient = httpClient.newBuilder()
                .callTimeout(remainingNanos, TimeUnit.NANOSECONDS)
                .readTimeout(remainingNanos, TimeUnit.NANOSECONDS)
                .writeTimeout(remainingNanos, TimeUnit.NANOSECONDS)
                .build();

            Response response = null;
            try {
                // Execute synchronously on the Virtual Thread (unmounts carrier thread cleanly)
                response = hopClient.newCall(request).execute();

                int statusCode = response.code();

                // Handle Redirects
                if (isRedirectStatus(statusCode)) {
                    if (hopCount >= MAX_REDIRECT_HOPS) {
                        response.close();
                        long durationMs = System.currentTimeMillis() - startTime;
                        String errorMsg = "Redirect loop exceeded maximum limit of " + MAX_REDIRECT_HOPS + " hops";
                        recordAttempt(job, startedAt, durationMs, statusCode, null, errorMsg);
                        return JobExecutionResult.permanentFailure(errorMsg);
                    }

                    String location = response.header("Location");
                    if (location == null || location.isBlank()) {
                        // Redirect without location header; treat as non-redirect terminal response
                        return processTerminalResponse(job, startedAt, startTime, response);
                    }

                    HttpUrl currentHttpUrl = response.request().url();
                    HttpUrl targetHttpUrl = currentHttpUrl.resolve(location);
                    response.close();

                    if (targetHttpUrl == null) {
                        long durationMs = System.currentTimeMillis() - startTime;
                        String errorMsg = "Invalid redirect target URL: " + location;
                        recordAttempt(job, startedAt, durationMs, statusCode, null, errorMsg);
                        return JobExecutionResult.permanentFailure(errorMsg);
                    }

                    // Re-check scheme: must be http or https
                    String targetScheme = targetHttpUrl.scheme();
                    if (!targetScheme.equalsIgnoreCase("http") && !targetScheme.equalsIgnoreCase("https")) {
                        long durationMs = System.currentTimeMillis() - startTime;
                        log.warn("Job {} redirect to disallowed scheme: {}", job.getId(), targetScheme);
                        recordAttempt(job, startedAt, durationMs, statusCode, null, GENERIC_SSRF_ERROR);
                        return JobExecutionResult.permanentFailure(GENERIC_SSRF_ERROR);
                    }

                    // Strictly block HTTPS -> HTTP downgrade
                    if (currentHttpUrl.isHttps() && !targetHttpUrl.isHttps()) {
                        long durationMs = System.currentTimeMillis() - startTime;
                        log.warn("Job {} blocked HTTPS to HTTP redirect downgrade to: {}", job.getId(), targetHttpUrl);
                        recordAttempt(job, startedAt, durationMs, statusCode, null, GENERIC_SSRF_ERROR);
                        return JobExecutionResult.permanentFailure(GENERIC_SSRF_ERROR);
                    }

                    // Fast-fail check on redirect URL syntax & port
                    try {
                        ssrfValidator.validateUrl(targetHttpUrl.toString());
                    } catch (SsrfException e) {
                        long durationMs = System.currentTimeMillis() - startTime;
                        log.warn("Job {} redirect target blocked by SSRF: {}", job.getId(), e.getMessage());
                        recordAttempt(job, startedAt, durationMs, statusCode, null, GENERIC_SSRF_ERROR);
                        return JobExecutionResult.permanentFailure(GENERIC_SSRF_ERROR);
                    }

                    // Cross-origin header stripping:
                    // Strip ALL user-supplied headers except Content-Type, Accept, User-Agent
                    boolean isCrossOrigin = !currentHttpUrl.host().equalsIgnoreCase(targetHttpUrl.host())
                        || currentHttpUrl.port() != targetHttpUrl.port()
                        || !currentHttpUrl.scheme().equalsIgnoreCase(targetHttpUrl.scheme());

                    if (isCrossOrigin) {
                        currentHeaders.entrySet().removeIf(
                            entry -> !PRESERVED_CROSS_ORIGIN_HEADERS.contains(entry.getKey().toLowerCase())
                        );
                    }

                    // Method mutation rules
                    if (statusCode == 301 || statusCode == 302 || statusCode == 303) {
                        currentMethod = "GET";
                        currentBody = null;
                        currentHeaders.remove("content-type");
                    }
                    // 307 and 308 preserve method and body

                    currentUrl = targetHttpUrl.toString();
                    hopCount++;
                    continue; // Loop to next redirect hop
                }

                // Non-redirect response: terminal processing
                return processTerminalResponse(job, startedAt, startTime, response);

            } catch (SsrfException e) {
                if (response != null) response.close();
                long durationMs = System.currentTimeMillis() - startTime;
                log.warn("SSRF Blocked for job {}: {}", job.getId(), e.getMessage());
                recordAttempt(job, startedAt, durationMs, null, null, GENERIC_SSRF_ERROR);
                return JobExecutionResult.permanentFailure(GENERIC_SSRF_ERROR);

            } catch (UnknownHostException e) {
                if (response != null) response.close();
                long durationMs = System.currentTimeMillis() - startTime;
                // If the SsrfAwareDns threw UnknownHostException for SSRF rejection
                if (GENERIC_SSRF_ERROR.equals(e.getMessage())) {
                    log.warn("DNS SSRF Blocked for job {}: {}", job.getId(), e.getMessage());
                    recordAttempt(job, startedAt, durationMs, null, null, GENERIC_SSRF_ERROR);
                    return JobExecutionResult.permanentFailure(GENERIC_SSRF_ERROR);
                }
                String errorMsg = "DNS resolution failed: " + e.getMessage();
                log.warn("Job {} DNS resolution failed: {}", job.getId(), errorMsg);
                recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
                return JobExecutionResult.retryableFailure(errorMsg);

            } catch (SocketTimeoutException e) {
                if (response != null) response.close();
                long durationMs = System.currentTimeMillis() - startTime;
                String errorMsg = "HTTP timeout: " + e.getMessage();
                log.warn("Job {} timed out after {}ms: {}", job.getId(), durationMs, errorMsg);
                recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
                return JobExecutionResult.retryableFailure(errorMsg);

            } catch (ConnectException e) {
                if (response != null) response.close();
                long durationMs = System.currentTimeMillis() - startTime;
                String errorMsg = "Connection refused or unreachable: " + e.getMessage();
                log.warn("Job {} connection failed: {}", job.getId(), errorMsg);
                recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
                return JobExecutionResult.retryableFailure(errorMsg);

            } catch (SocketException e) {
                if (response != null) response.close();
                long durationMs = System.currentTimeMillis() - startTime;
                String errorMsg = "Socket error: " + e.getMessage();
                log.warn("Job {} socket error: {}", job.getId(), errorMsg);
                recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
                return JobExecutionResult.retryableFailure(errorMsg);

            } catch (IOException e) {
                if (response != null) response.close();
                long durationMs = System.currentTimeMillis() - startTime;
                // Check if an underlying SsrfException caused this
                if (e.getCause() instanceof SsrfException || (e.getMessage() != null && e.getMessage().contains(GENERIC_SSRF_ERROR))) {
                    log.warn("Job {} SSRF blocked in I/O: {}", job.getId(), e.getMessage());
                    recordAttempt(job, startedAt, durationMs, null, null, GENERIC_SSRF_ERROR);
                    return JobExecutionResult.permanentFailure(GENERIC_SSRF_ERROR);
                }
                String errorMsg = "I/O error during execution: " + e.getMessage();
                log.error("Job {} I/O error: {}", job.getId(), errorMsg, e);
                recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
                return JobExecutionResult.retryableFailure(errorMsg);

            } catch (Exception e) {
                if (response != null) response.close();
                long durationMs = System.currentTimeMillis() - startTime;
                String errorMsg = "Unexpected execution error: " + e.getMessage();
                log.error("Job {} unexpected error: {}", job.getId(), errorMsg, e);
                recordAttempt(job, startedAt, durationMs, null, null, errorMsg);
                return JobExecutionResult.retryableFailure(errorMsg);
            }
        }
    }

    private JobExecutionResult processTerminalResponse(JobEntity job,
                                                       Instant startedAt,
                                                       long startTime,
                                                       Response response) throws IOException {
        long durationMs = System.currentTimeMillis() - startTime;
        int statusCode = response.code();

        // Cap bytes read (including decompressed stream) to prevent memory exhaustion
        String snippet = null;
        ResponseBody responseBody = response.body();
        if (responseBody != null) {
            BufferedSource source = responseBody.source();
            source.request(MAX_READ_BYTES);
            Buffer buffer = source.getBuffer();
            long bytesToRead = Math.min(buffer.size(), MAX_READ_BYTES);
            byte[] rawBytes = buffer.readByteArray(bytesToRead);
            String rawString = new String(rawBytes, StandardCharsets.UTF_8);
            snippet = truncateSnippet(rawString);
        }
        response.close();

        recordAttempt(job, startedAt, durationMs, statusCode, snippet, null);
        log.info("HTTP call for job {} completed with status {} in {}ms", job.getId(), statusCode, durationMs);

        return classifyResponse(statusCode, snippet, durationMs);
    }

    private Request buildRequest(String url, String method, Map<String, String> headers, String body) {
        Request.Builder builder = new Request.Builder().url(url);

        // Add headers
        headers.forEach((k, v) -> {
            if (k != null && v != null && !FORBIDDEN_INBOUND_HEADERS.contains(k.trim().toLowerCase())) {
                builder.header(k.trim(), v.trim());
            }
        });

        // Set body based on method
        if (method.equals("GET") || method.equals("HEAD")) {
            builder.method(method, null);
        } else {
            String contentType = headers.getOrDefault("content-type", "application/json; charset=utf-8");
            MediaType mediaType = MediaType.parse(contentType);
            byte[] bodyBytes = (body != null) ? body.getBytes(StandardCharsets.UTF_8) : new byte[0];
            RequestBody requestBody = RequestBody.create(bodyBytes, mediaType);
            builder.method(method, requestBody);
        }

        return builder.build();
    }

    private boolean isRedirectStatus(int code) {
        return code == 301 || code == 302 || code == 303 || code == 307 || code == 308;
    }

    private JobExecutionResult classifyResponse(int statusCode, String snippet, long durationMs) {
        if (statusCode >= 200 && statusCode < 300) {
            return JobExecutionResult.success(snippet);
        }

        if (statusCode == 429) {
            return JobExecutionResult.retryableFailure("Downstream rate limited (HTTP 429 Too Many Requests)");
        }

        if (statusCode >= 500 && statusCode < 600) {
            return JobExecutionResult.retryableFailure("HTTP " + statusCode + " Server Error");
        }

        if (statusCode >= 400 && statusCode < 500) {
            return JobExecutionResult.permanentFailure("HTTP " + statusCode + " Client Error");
        }

        return JobExecutionResult.retryableFailure("Unexpected HTTP status: " + statusCode);
    }

    private String truncateSnippet(String body) {
        if (body == null || body.length() <= MAX_SNIPPET_LENGTH) {
            return body;
        }
        return body.substring(0, MAX_SNIPPET_LENGTH) +
               "\n[TRUNCATED: original length was " + body.length() + " chars]";
    }

    private void recordAttempt(JobEntity job,
                               Instant startedAt,
                               long durationMs,
                               Integer statusCode,
                               String responseSnippet,
                               String errorMessage) {
        try {
            JobAttemptEntity attempt = new JobAttemptEntity(
                UuidV7.generate(),
                job.getId(),
                job.getReplayCount(),
                job.getAttempts(),
                startedAt,
                durationMs,
                statusCode,
                responseSnippet,
                errorMessage
            );
            jobAttemptRepository.save(attempt);
        } catch (Exception e) {
            log.error("Failed to save job attempt audit row for job {}: {}", job.getId(), e.getMessage(), e);
        }
    }
}
