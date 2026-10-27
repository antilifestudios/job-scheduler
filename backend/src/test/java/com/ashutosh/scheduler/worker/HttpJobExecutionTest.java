package com.ashutosh.scheduler.worker;

import com.ashutosh.scheduler.TestcontainersConfiguration;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.JobAttemptEntity;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import com.ashutosh.scheduler.domain.repo.JobAttemptRepository;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import com.ashutosh.scheduler.worker.engine.JobClaimRepository;
import com.ashutosh.scheduler.worker.engine.JobWorkerEngine;
import com.ashutosh.scheduler.worker.handler.HttpJobHandler;
import com.ashutosh.scheduler.worker.handler.JobHandlerRegistry;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@ActiveProfiles("test")
class HttpJobExecutionTest {

    @Autowired
    private JobClaimRepository claimRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JobAttemptRepository jobAttemptRepository;

    @Autowired
    private HttpJobHandler httpJobHandler;

    @Autowired
    private JobWorkerEngine jobWorkerEngine;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private UserEntity testUser;
    private HttpServer mockHttpServer;
    private int mockServerPort;

    @BeforeEach
    void setup() throws IOException {
        jobAttemptRepository.deleteAll();
        jobRepository.deleteAll();
        userRepository.deleteAll();

        testUser = new UserEntity(UuidV7.generate(), "http-test-user");
        userRepository.save(testUser);

        // Bind to localhost on an ephemeral port
        mockHttpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mockHttpServer.setExecutor(null);
        mockHttpServer.start();
        mockServerPort = mockHttpServer.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        if (mockHttpServer != null) {
            mockHttpServer.stop(0);
        }
    }

    @Test
    @DisplayName("200 OK: outbound HTTP succeeds, job marked SUCCEEDED, attempt recorded in job_attempts")
    void testSuccessfulHttpExecution200Ok() {
        String responseBody = "{\"status\":\"processed\",\"batchId\":42}";
        mockHttpServer.createContext("/webhook", exchange -> {
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        UUID jobId = UuidV7.generate();
        insertHttpJob(jobId, "http://127.0.0.1:" + mockServerPort + "/webhook", "POST",
            "{\"custom-header\":\"val\"}", "{\"orderId\":\"12345\"}", 3, 5000);

        int claimed = jobWorkerEngine.pollAndDispatch();
        assertThat(claimed).isEqualTo(1);

        awaitJobStatus(jobId, JobStatus.SUCCEEDED);

        JobEntity job = jobRepository.findById(jobId).orElseThrow();
        assertThat(job.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(job.getAttempts()).isEqualTo(1);

        List<JobAttemptEntity> attempts = jobAttemptRepository.findByJobIdOrderByAttemptNoAsc(jobId);
        assertThat(attempts).hasSize(1);
        JobAttemptEntity attempt = attempts.get(0);
        assertThat(attempt.getStatusCode()).isEqualTo(200);
        assertThat(attempt.getAttemptNo()).isEqualTo(1);
        assertThat(attempt.getResponseSnippet()).isEqualTo(responseBody);
        assertThat(attempt.getError()).isNull();
        assertThat(attempt.getDurationMs()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("503 Service Unavailable: classified as retryable, reschedules to PENDING with backoff")
    void testServer503ErrorReschedulesToPending() {
        mockHttpServer.createContext("/downstream-crash", exchange -> {
            byte[] bytes = "Backend database connection timeout".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        UUID jobId = UuidV7.generate();
        insertHttpJob(jobId, "http://127.0.0.1:" + mockServerPort + "/downstream-crash", "GET",
            "{}", null, 3, 5000);

        jobWorkerEngine.pollAndDispatch();

        awaitJobStatus(jobId, JobStatus.PENDING);

        JobEntity job = jobRepository.findById(jobId).orElseThrow();
        assertThat(job.getStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(job.getAttempts()).isEqualTo(1);
        assertThat(job.getRunAt()).isAfter(Instant.now());
        assertThat(job.getLastError()).contains("HTTP 503 Server Error");

        List<JobAttemptEntity> attempts = jobAttemptRepository.findByJobIdOrderByAttemptNoAsc(jobId);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).getStatusCode()).isEqualTo(503);
        assertThat(attempts.get(0).getResponseSnippet()).contains("Backend database connection timeout");
    }

    @Test
    @DisplayName("400 Bad Request: classified as permanent failure, transitions directly to DEAD on attempt 1")
    void testClient400ErrorTransitionsDirectlyToDead() {
        mockHttpServer.createContext("/bad-request", exchange -> {
            byte[] bytes = "Invalid account parameter format".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(400, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        UUID jobId = UuidV7.generate();
        insertHttpJob(jobId, "http://127.0.0.1:" + mockServerPort + "/bad-request", "POST",
            "{}", "bad payload", 5, 5000);

        jobWorkerEngine.pollAndDispatch();

        awaitJobStatus(jobId, JobStatus.DEAD);

        JobEntity job = jobRepository.findById(jobId).orElseThrow();
        assertThat(job.getStatus()).isEqualTo(JobStatus.DEAD);
        assertThat(job.getAttempts()).isEqualTo(1); // Failed on 1st attempt without wasting 5 attempts
        assertThat(job.getLastError()).contains("HTTP 400 Client Error");

        List<JobAttemptEntity> attempts = jobAttemptRepository.findByJobIdOrderByAttemptNoAsc(jobId);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).getStatusCode()).isEqualTo(400);
        assertThat(attempts.get(0).getResponseSnippet()).contains("Invalid account parameter format");
    }

    @Test
    @DisplayName("429 Too Many Requests: classified as retryable rate limit, reschedules to PENDING")
    void testRateLimit429ReschedulesToPending() {
        mockHttpServer.createContext("/rate-limited", exchange -> {
            byte[] bytes = "Rate limit exceeded. Try again later.".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(429, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        UUID jobId = UuidV7.generate();
        insertHttpJob(jobId, "http://127.0.0.1:" + mockServerPort + "/rate-limited", "POST",
            "{}", "data", 3, 5000);

        jobWorkerEngine.pollAndDispatch();

        awaitJobStatus(jobId, JobStatus.PENDING);

        JobEntity job = jobRepository.findById(jobId).orElseThrow();
        assertThat(job.getStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(job.getLastError()).contains("HTTP 429 Too Many Requests");

        List<JobAttemptEntity> attempts = jobAttemptRepository.findByJobIdOrderByAttemptNoAsc(jobId);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).getStatusCode()).isEqualTo(429);
    }

    @Test
    @DisplayName("Timeout: read timeout triggered, classified as retryable failure, reschedules to PENDING")
    void testTimeoutReschedulesToPending() {
        mockHttpServer.createContext("/slow-endpoint", exchange -> {
            try {
                Thread.sleep(600); // Exceeds 150ms timeout configured below
            } catch (InterruptedException ignored) {}
            byte[] bytes = "Too late".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        UUID jobId = UuidV7.generate();
        insertHttpJob(jobId, "http://127.0.0.1:" + mockServerPort + "/slow-endpoint", "GET",
            "{}", null, 3, 150); // 150ms timeout

        jobWorkerEngine.pollAndDispatch();

        awaitJobStatus(jobId, JobStatus.PENDING);

        JobEntity job = jobRepository.findById(jobId).orElseThrow();
        assertThat(job.getStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(job.getLastError()).contains("timeout");

        List<JobAttemptEntity> attempts = jobAttemptRepository.findByJobIdOrderByAttemptNoAsc(jobId);
        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0).getStatusCode()).isNull();
        assertThat(attempts.get(0).getError()).contains("timeout");
    }

    @Test
    @DisplayName("Response Snippet Truncation: 100 KB payload safely truncated to 4 KB in job_attempts")
    void testResponseBodyTruncation() {
        String hugeResponse = "X".repeat(100_000); // 100 KB string
        mockHttpServer.createContext("/huge-dump", exchange -> {
            byte[] bytes = hugeResponse.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });

        UUID jobId = UuidV7.generate();
        insertHttpJob(jobId, "http://127.0.0.1:" + mockServerPort + "/huge-dump", "GET",
            "{}", null, 3, 5000);

        jobWorkerEngine.pollAndDispatch();

        awaitJobStatus(jobId, JobStatus.SUCCEEDED);

        List<JobAttemptEntity> attempts = jobAttemptRepository.findByJobIdOrderByAttemptNoAsc(jobId);
        assertThat(attempts).hasSize(1);
        String snippet = attempts.get(0).getResponseSnippet();
        assertThat(snippet).isNotNull();
        // Must be capped around 4KB + marker, NOT the full 100,000 characters
        assertThat(snippet.length()).isLessThan(4200);
        assertThat(snippet).contains("[TRUNCATED");
    }

    private void insertHttpJob(UUID jobId, String url, String method, String headersJson,
                               String body, int maxAttempts, int timeoutMs) {
        String payloadJson = String.format(
            "{\"url\":\"%s\",\"method\":\"%s\",\"headers\":%s,\"timeoutMs\":%d}",
            url, method, headersJson, timeoutMs
        );

        jdbcTemplate.update("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, attempts, max_attempts)
            VALUES (?, ?, 'default', 'HTTP', ?::jsonb, ?, 'PENDING', 0, now() - INTERVAL '5 seconds', 0, ?)
            """, jobId, testUser.getId(), payloadJson, body, maxAttempts);
    }

    private void awaitJobStatus(UUID jobId, JobStatus expectedStatus) {
        long deadline = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < deadline) {
            JobEntity job = jobRepository.findById(jobId).orElse(null);
            if (job != null && job.getStatus() == expectedStatus) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {}
        }
    }
}
