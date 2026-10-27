package com.ashutosh.scheduler.security;

import com.ashutosh.scheduler.TestcontainersConfiguration;
import com.ashutosh.scheduler.common.util.CryptoUtils;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.ApiKeyEntity;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import com.ashutosh.scheduler.domain.repo.ApiKeyRepository;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import com.ashutosh.scheduler.producer.dto.CreateJobRequest;
import com.ashutosh.scheduler.security.apikey.CreateApiKeyRequest;
import com.ashutosh.scheduler.security.ratelimit.TokenBucketRateLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@AutoConfigureMockMvc
class Phase7AuthMultiTenancyAndQuotasTest {

    private static final Logger log = LoggerFactory.getLogger(Phase7AuthMultiTenancyAndQuotasTest.class);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ApiKeyRepository apiKeyRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TokenBucketRateLimiter rateLimiter;

    @Autowired
    private ApiKeyAuthenticationFilter apiKeyFilter;

    private UserEntity userA;
    private String userAApiKey;
    private ApiKeyEntity userAKeyEntity;

    private UserEntity userB;
    private String userBApiKey;

    @BeforeEach
    void setup() {
        rateLimiter.clear();
        apiKeyFilter.getLastUsedThrottleMap().clear();

        jobRepository.deleteAll();
        apiKeyRepository.deleteAll();
        userRepository.deleteAll();

        // User A
        userA = new UserEntity(UuidV7.generate(), "tenant_a");
        userA.setMaxActiveJobs(100);
        userA.setRateLimitPerMinute(60);
        userRepository.save(userA);

        userAApiKey = CryptoUtils.generateApiKey();
        userAKeyEntity = new ApiKeyEntity(
            UuidV7.generate(),
            userA,
            CryptoUtils.sha256Hex(userAApiKey),
            CryptoUtils.extractPrefix(userAApiKey),
            "Primary Key A"
        );
        apiKeyRepository.save(userAKeyEntity);

        // User B
        userB = new UserEntity(UuidV7.generate(), "tenant_b");
        userB.setMaxActiveJobs(100);
        userB.setRateLimitPerMinute(60);
        userRepository.save(userB);

        userBApiKey = CryptoUtils.generateApiKey();
        ApiKeyEntity keyB = new ApiKeyEntity(
            UuidV7.generate(),
            userB,
            CryptoUtils.sha256Hex(userBApiKey),
            CryptoUtils.extractPrefix(userBApiKey),
            "Primary Key B"
        );
        apiKeyRepository.save(keyB);
    }

    @Test
    @DisplayName("1. Atomic Quota Concurrency: 50 parallel submits with 5 slots free; exactly 5 succeed, 45 fail with quota_exceeded, no Retry-After")
    void testAtomicQuotaConcurrency_50ParallelSubmits_5SlotsFree() throws Exception {
        // Configure tenant with max_active_jobs = 5
        userA.setMaxActiveJobs(5);
        userRepository.save(userA);

        int totalRequests = 50;
        ExecutorService executor = Executors.newFixedThreadPool(16);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger quotaExceededCount = new AtomicInteger(0);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < totalRequests; i++) {
            final int index = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    CreateJobRequest req = new CreateJobRequest(
                        "https://httpbin.org/post",
                        "POST",
                        Map.of(),
                        "{\"index\":" + index + "}",
                        "default",
                        0,
                        null,
                        3,
                        null
                    );
                    MvcResult result = mockMvc.perform(post("/api/jobs")
                            .header("X-API-Key", userAApiKey)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(req)))
                        .andReturn();

                    int status = result.getResponse().getStatus();
                    if (status == 201) {
                        successCount.incrementAndGet();
                    } else if (status == 429) {
                        quotaExceededCount.incrementAndGet();
                        String responseBody = result.getResponse().getContentAsString();
                        assertThat(responseBody).contains("quota_exceeded");
                        // Strictly assert that Retry-After and X-RateLimit-* are NOT present on quota_exceeded
                        assertThat(result.getResponse().getHeader("Retry-After")).isNull();
                        assertThat(result.getResponse().getHeader("X-RateLimit-Limit")).isNull();
                    }
                } catch (Exception e) {
                    log.error("Submission failed: {}", e.getMessage(), e);
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();

        // Exactly 5 must succeed, 45 must be rejected
        assertThat(successCount.get()).isEqualTo(5);
        assertThat(quotaExceededCount.get()).isEqualTo(45);

        // Database active job count must NEVER exceed 5
        long actualActiveJobs = jobRepository.countActiveJobsByUserId(userA.getId());
        assertThat(actualActiveJobs).isEqualTo(5);
    }

    @Test
    @DisplayName("2. Idempotency replay under quota: When active jobs == max_active_jobs, replay returns 200, NEVER 429")
    void testIdempotencyReplayUnderQuota() throws Exception {
        userA.setMaxActiveJobs(1);
        userRepository.save(userA);

        String idempotencyKey = "unique-order-999";
        CreateJobRequest req = new CreateJobRequest(
            "https://httpbin.org/post",
            "POST",
            Map.of(),
            "{\"order\":999}",
            "default",
            0,
            null,
            3,
            idempotencyKey
        );

        // First submit: consumes the 1 available slot -> 201 Created
        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(req)))
            .andExpect(status().isCreated());

        assertThat(jobRepository.countActiveJobsByUserId(userA.getId())).isEqualTo(1);

        // Replay with identical payload and key: MUST return 200 OK (NOT 429 quota_exceeded)
        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(req)))
            .andExpect(status().isOk())
            .andExpect(header().string("Idempotent-Replayed", "true"))
            .andExpect(jsonPath("$.idempotencyKey").value(idempotencyKey));
    }

    @Test
    @DisplayName("3. Rate Limiter: Bursts exceeding rateLimitPerMinute return 429 rate_limited with Retry-After and X-RateLimit-*")
    void testRateLimitingBurst() throws Exception {
        userA.setRateLimitPerMinute(3);
        userRepository.save(userA);

        // 3 requests allowed
        for (int i = 0; i < 3; i++) {
            mockMvc.perform(get("/api/jobs")
                    .header("X-API-Key", userAApiKey))
                .andExpect(status().isOk())
                .andExpect(header().string("X-RateLimit-Limit", "3"))
                .andExpect(header().exists("X-RateLimit-Remaining"))
                .andExpect(header().exists("X-RateLimit-Reset"));
        }

        // 4th request must be rejected with rate_limited
        mockMvc.perform(get("/api/jobs")
                .header("X-API-Key", userAApiKey))
            .andExpect(status().isTooManyRequests())
            .andExpect(header().exists("Retry-After"))
            .andExpect(header().string("X-RateLimit-Limit", "3"))
            .andExpect(header().string("X-RateLimit-Remaining", "0"))
            .andExpect(jsonPath("$.code").value("rate_limited"))
            .andExpect(jsonPath("$.retryAfterSeconds").isNumber());
    }

    @Test
    @DisplayName("4. API Key Lifecycle: Cap active keys at 5, block revoking last active key, authenticate with active, reject revoked")
    void testApiKeyLifecycleAndEdgeCases() throws Exception {
        // User A starts with 1 key. Create 4 more keys (total 5)
        for (int i = 2; i <= 5; i++) {
            CreateApiKeyRequest createReq = new CreateApiKeyRequest("Key " + i);
            mockMvc.perform(post("/api/keys")
                    .header("X-API-Key", userAApiKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(createReq)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.rawKey").isString())
                .andExpect(jsonPath("$.keyPrefix").isString());
        }

        assertThat(apiKeyRepository.countByUserIdAndRevokedAtIsNull(userA.getId())).isEqualTo(5);

        // Attempting to create a 6th active key must fail with 409 max_keys_exceeded
        mockMvc.perform(post("/api/keys")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new CreateApiKeyRequest("Key 6"))))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.code").value("max_keys_exceeded"));

        // List keys: returns 5 keys without exposing raw keys
        MvcResult listResult = mockMvc.perform(get("/api/keys")
                .header("X-API-Key", userAApiKey))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.length()").value(5))
            .andExpect(jsonPath("$[0].rawKey").doesNotExist())
            .andReturn();

        // Revoke 4 keys so only userAKeyEntity remains
        List<ApiKeyEntity> allKeys = apiKeyRepository.findByUserIdOrderByCreatedAtDesc(userA.getId());
        for (ApiKeyEntity k : allKeys) {
            if (!k.getId().equals(userAKeyEntity.getId())) {
                mockMvc.perform(delete("/api/keys/" + k.getId())
                        .header("X-API-Key", userAApiKey))
                    .andExpect(status().isOk());
            }
        }

        assertThat(apiKeyRepository.countByUserIdAndRevokedAtIsNull(userA.getId())).isEqualTo(1);

        // Attempting to revoke the last active key MUST fail with 409 cannot_revoke_last_key
        mockMvc.perform(delete("/api/keys/" + userAKeyEntity.getId())
                .header("X-API-Key", userAApiKey))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.code").value("cannot_revoke_last_key"));

        // Create a new key
        MvcResult newKeyResult = mockMvc.perform(post("/api/keys")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(new CreateApiKeyRequest("Replacement Key"))))
            .andExpect(status().isCreated())
            .andReturn();

        String rawReplacementKey = objectMapper.readTree(newKeyResult.getResponse().getContentAsString()).get("rawKey").asText();

        // Now revoking userAKeyEntity must succeed because 2 keys existed
        mockMvc.perform(delete("/api/keys/" + userAKeyEntity.getId())
                .header("X-API-Key", userAApiKey))
            .andExpect(status().isOk());

        // Subsequent call with revoked userAApiKey must fail with 401 Unauthorized
        mockMvc.perform(get("/api/jobs")
                .header("X-API-Key", userAApiKey))
            .andExpect(status().isUnauthorized());

        // Call with new replacement key must succeed
        mockMvc.perform(get("/api/jobs")
                .header("X-API-Key", rawReplacementKey))
            .andExpect(status().isOk());
    }

    @Test
    @DisplayName("5. Throttled last_used_at: Rapid authenticated requests update in-memory map without database spam")
    void testThrottledLastUsedAt() throws Exception {
        UUID keyId = userAKeyEntity.getId();

        // Fire 5 rapid requests
        for (int i = 0; i < 5; i++) {
            mockMvc.perform(get("/api/jobs")
                    .header("X-API-Key", userAApiKey))
                .andExpect(status().isOk());
        }

        // Verify key exists in the throttle map
        assertThat(apiKeyFilter.getLastUsedThrottleMap()).containsKey(keyId);
        Long firstRecordedTime = apiKeyFilter.getLastUsedThrottleMap().get(keyId);
        assertThat(firstRecordedTime).isNotNull();

        // Immediate subsequent request within 60s keeps the exact same throttle timestamp
        mockMvc.perform(get("/api/jobs")
                .header("X-API-Key", userAApiKey))
            .andExpect(status().isOk());

        assertThat(apiKeyFilter.getLastUsedThrottleMap().get(keyId)).isEqualTo(firstRecordedTime);
    }

    @Test
    @DisplayName("6. Tenant Isolation: Tenant B cannot access or cancel Tenant A's jobs (returns 404, never 403)")
    void testTenantIsolation() throws Exception {
        // Tenant A creates a job
        CreateJobRequest req = new CreateJobRequest(
            "https://httpbin.org/post",
            "POST",
            Map.of(),
            "{\"tenant\":\"a\"}",
            "default",
            0,
            null,
            3,
            null
        );

        MvcResult createResult = mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(req)))
            .andExpect(status().isCreated())
            .andReturn();

        String jobId = objectMapper.readTree(createResult.getResponse().getContentAsString()).get("id").asText();

        // Tenant B attempts to get Tenant A's job -> MUST return 404 Not Found
        mockMvc.perform(get("/api/jobs/" + jobId)
                .header("X-API-Key", userBApiKey))
            .andExpect(status().isNotFound());

        // Tenant B attempts to cancel Tenant A's job -> MUST return 404 Not Found
        mockMvc.perform(post("/api/jobs/" + jobId + "/cancel")
                .header("X-API-Key", userBApiKey))
            .andExpect(status().isNotFound());

        // Tenant A can get and cancel their own job
        mockMvc.perform(get("/api/jobs/" + jobId)
                .header("X-API-Key", userAApiKey))
            .andExpect(status().isOk());
    }

    @Test
    @DisplayName("7. Job Cancellation: PENDING jobs can be cancelled; RUNNING/DEAD jobs reject cancellation with 409")
    void testJobCancellation() throws Exception {
        CreateJobRequest req = new CreateJobRequest(
            "https://httpbin.org/post",
            "POST",
            Map.of(),
            "{\"test\":\"cancel\"}",
            "default",
            0,
            null,
            3,
            null
        );

        MvcResult createResult = mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(req)))
            .andExpect(status().isCreated())
            .andReturn();

        String jobId = objectMapper.readTree(createResult.getResponse().getContentAsString()).get("id").asText();

        // Cancel PENDING job -> returns 200 OK
        mockMvc.perform(post("/api/jobs/" + jobId + "/cancel")
                .header("X-API-Key", userAApiKey))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("DEAD"))
            .andExpect(jsonPath("$.lastError").value("Cancelled by user"));

        // Second cancel attempt on already DEAD job -> returns 409 Conflict
        mockMvc.perform(post("/api/jobs/" + jobId + "/cancel")
                .header("X-API-Key", userAApiKey))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.code").value("job_cancellation_conflict"));
    }

    @Test
    @DisplayName("8. Usage Endpoint: /api/tenants/me/usage reports accurate quota limits and live job counts")
    void testUsageEndpoint() throws Exception {
        userA.setMaxActiveJobs(25);
        userA.setRateLimitPerMinute(120);
        userRepository.save(userA);

        // Create 2 jobs
        for (int i = 0; i < 2; i++) {
            CreateJobRequest req = new CreateJobRequest(
                "https://httpbin.org/post",
                "POST",
                Map.of(),
                "{\"item\":" + i + "}",
                "default",
                0,
                null,
                3,
                null
            );
            mockMvc.perform(post("/api/jobs")
                    .header("X-API-Key", userAApiKey)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isCreated());
        }

        mockMvc.perform(get("/api/tenants/me/usage")
                .header("X-API-Key", userAApiKey))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.maxActiveJobs").value(25))
            .andExpect(jsonPath("$.rateLimitPerMinute").value(120))
            .andExpect(jsonPath("$.activeJobs").value(2))
            .andExpect(jsonPath("$.pendingJobs").value(2))
            .andExpect(jsonPath("$.runningJobs").value(0));
    }

    @Test
    @DisplayName("9. EXPLAIN (ANALYZE, BUFFERS): Verify active jobs count query performance and index behavior on 100k+ historical rows")
    void testExplainAnalyzeBuffersOn100kRows() {
        // Seed 100,000 SUCCEEDED historical jobs for userA
        jdbcTemplate.execute("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, attempts, max_attempts, created_at, updated_at)
            SELECT
                gen_random_uuid(),
                '""" + userA.getId() + """
            ',
                'default',
                'HTTP',
                '{"url":"https://example.com"}'::jsonb,
                'historical body',
                'SUCCEEDED',
                0,
                now() - INTERVAL '1 day',
                1,
                3,
                now() - INTERVAL '1 day',
                now() - INTERVAL '1 day'
            FROM generate_series(1, 100000);
            """);

        // Seed 5 PENDING and 2 RUNNING active jobs
        jdbcTemplate.execute("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, attempts, max_attempts, created_at, updated_at)
            SELECT
                gen_random_uuid(),
                '""" + userA.getId() + """
            ',
                'default',
                'HTTP',
                '{"url":"https://example.com"}'::jsonb,
                'active body',
                'PENDING',
                0,
                now(),
                0,
                3,
                now(),
                now()
            FROM generate_series(1, 5);
            """);

        jdbcTemplate.execute("""
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority, run_at, attempts, max_attempts, created_at, updated_at)
            SELECT
                gen_random_uuid(),
                '""" + userA.getId() + """
            ',
                'default',
                'HTTP',
                '{"url":"https://example.com"}'::jsonb,
                'running body',
                'RUNNING',
                0,
                now(),
                1,
                3,
                now(),
                now()
            FROM generate_series(1, 2);
            """);

        jdbcTemplate.execute("ANALYZE jobs;");

        // Run EXPLAIN (ANALYZE, BUFFERS) on the active count query
        String explainSql = """
            EXPLAIN (ANALYZE, BUFFERS)
            SELECT count(*) FROM jobs
            WHERE user_id = '""" + userA.getId() + """
            ' AND status IN ('PENDING', 'RUNNING');
            """;

        List<String> planLines = jdbcTemplate.queryForList(explainSql, String.class);
        String fullPlan = String.join("\n", planLines);

        log.info("EXPLAIN (ANALYZE, BUFFERS) Active Quota Count on 100k rows:\n{}", fullPlan);
        System.out.println("=== EXPLAIN (ANALYZE, BUFFERS) ACTIVE QUOTA QUERY ===");
        System.out.println(fullPlan);

        assertThat(fullPlan).contains("Index");
    }
}
