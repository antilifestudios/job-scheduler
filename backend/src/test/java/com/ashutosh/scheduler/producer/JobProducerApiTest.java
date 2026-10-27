package com.ashutosh.scheduler.producer;

import com.ashutosh.scheduler.TestcontainersConfiguration;
import com.ashutosh.scheduler.common.util.CryptoUtils;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.ApiKeyEntity;
import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.repo.ApiKeyRepository;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import com.ashutosh.scheduler.producer.dto.CreateJobRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@AutoConfigureMockMvc
class JobProducerApiTest {

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

    private UserEntity userA;
    private String userAApiKey;

    private UserEntity userB;
    private String userBApiKey;

    @BeforeEach
    void setupUsers() {
        jobRepository.deleteAll();
        apiKeyRepository.deleteAll();
        userRepository.deleteAll();

        // Create User A
        userA = new UserEntity(UuidV7.generate(), "user_a");
        userRepository.save(userA);
        userAApiKey = CryptoUtils.generateApiKey();
        ApiKeyEntity keyA = new ApiKeyEntity(
            UuidV7.generate(), userA, CryptoUtils.sha256Hex(userAApiKey),
            CryptoUtils.extractPrefix(userAApiKey), "Key A"
        );
        apiKeyRepository.save(keyA);

        // Create User B
        userB = new UserEntity(UuidV7.generate(), "user_b");
        userRepository.save(userB);
        userBApiKey = CryptoUtils.generateApiKey();
        ApiKeyEntity keyB = new ApiKeyEntity(
            UuidV7.generate(), userB, CryptoUtils.sha256Hex(userBApiKey),
            CryptoUtils.extractPrefix(userBApiKey), "Key B"
        );
        apiKeyRepository.save(keyB);
    }

    @Test
    @DisplayName("Unauthenticated request should return 401 Unauthorized")
    void testUnauthenticatedRequestGets401() throws Exception {
        mockMvc.perform(get("/api/jobs"))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("Revoked API key should return 401 Unauthorized")
    void testRevokedKeyGets401() throws Exception {
        String rawKey = CryptoUtils.generateApiKey();
        ApiKeyEntity revokedKey = new ApiKeyEntity(
            UuidV7.generate(), userA, CryptoUtils.sha256Hex(rawKey),
            CryptoUtils.extractPrefix(rawKey), "Revoked Key"
        );
        revokedKey.setRevokedAt(Instant.now());
        apiKeyRepository.save(revokedKey);

        mockMvc.perform(get("/api/jobs")
                .header("X-API-Key", rawKey))
            .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("Cross-tenant read should return 404 Not Found")
    void testCrossTenantReadGets404() throws Exception {
        // User A creates a job
        CreateJobRequest request = new CreateJobRequest(
            "https://api.example.com/webhook", "POST", Map.of("X-Tenant", "A"),
            "{\"data\":\"secret\"}", "default", 5, null, 3, null
        );

        String responseStr = mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
            .andExpect(status().isCreated())
            .andReturn().getResponse().getContentAsString();

        String jobId = objectMapper.readTree(responseStr).get("id").asText();

        // User B attempts to read User A's job -> MUST return 404
        mockMvc.perform(get("/api/jobs/" + jobId)
                .header("X-API-Key", userBApiKey))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.error").value("Not Found"));
    }

    @Test
    @DisplayName("Same idempotency key with different body/request should return 409 Conflict")
    void testSameKeyDifferentBodyGets409() throws Exception {
        String idemKey = "idem-order-999";

        CreateJobRequest request1 = new CreateJobRequest(
            "https://api.example.com/charge", "POST", Map.of(),
            "{\"amount\":100}", "default", 0, null, 3, idemKey
        );

        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request1)))
            .andExpect(status().isCreated());

        // Same idempotency key, but different body
        CreateJobRequest request2 = new CreateJobRequest(
            "https://api.example.com/charge", "POST", Map.of(),
            "{\"amount\":200}", "default", 0, null, 3, idemKey
        );

        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request2)))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.error").value("Conflict"));
    }

    @Test
    @DisplayName("Validation: CRLF in header names/values, forbidden headers, NUL byte, and priority bounds should return 400")
    void testValidationErrorsGet400() throws Exception {
        // 1. CRLF in header name
        CreateJobRequest crlfHeaderName = new CreateJobRequest(
            "https://example.com", "POST", Map.of("Bad\r\nName", "value"),
            null, "default", 0, null, 3, null
        );
        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(crlfHeaderName)))
            .andExpect(status().isBadRequest());

        // 2. CRLF in header value
        CreateJobRequest crlfHeaderValue = new CreateJobRequest(
            "https://example.com", "POST", Map.of("X-Custom", "val\nue"),
            null, "default", 0, null, 3, null
        );
        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(crlfHeaderValue)))
            .andExpect(status().isBadRequest());

        // 3. Forbidden header Host
        CreateJobRequest forbiddenHost = new CreateJobRequest(
            "https://example.com", "POST", Map.of("Host", "attacker.com"),
            null, "default", 0, null, 3, null
        );
        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(forbiddenHost)))
            .andExpect(status().isBadRequest());

        // 4. Forbidden header Proxy-Authorization
        CreateJobRequest forbiddenProxy = new CreateJobRequest(
            "https://example.com", "POST", Map.of("Proxy-Authorization", "token"),
            null, "default", 0, null, 3, null
        );
        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(forbiddenProxy)))
            .andExpect(status().isBadRequest());

        // 5. NUL byte in body
        CreateJobRequest nulBody = new CreateJobRequest(
            "https://example.com", "POST", Map.of(),
            "binary\0payload", "default", 0, null, 3, null
        );
        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(nulBody)))
            .andExpect(status().isBadRequest());

        // 6. Priority out of range (> 10)
        CreateJobRequest highPriority = new CreateJobRequest(
            "https://example.com", "POST", Map.of(),
            "body", "default", 11, null, 3, null
        );
        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(highPriority)))
            .andExpect(status().isBadRequest());

        // 7. Header and JSON idempotency keys disagree
        CreateJobRequest keyMismatch = new CreateJobRequest(
            "https://example.com", "POST", Map.of(),
            "body", "default", 5, null, 3, "key-json"
        );
        mockMvc.perform(post("/api/jobs")
                .header("X-API-Key", userAApiKey)
                .header("Idempotency-Key", "key-header")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(keyMismatch)))
            .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Two concurrent identical idempotent POSTs should create exactly one row and both succeed")
    void testConcurrentIdenticalIdempotentPosts() throws Exception {
        String idemKey = "concurrent-order-456";
        CreateJobRequest request = new CreateJobRequest(
            "https://api.example.com/pay", "POST", Map.of("X-Key", "Val"),
            "{\"txn\":123}", "default", 5, null, 3, idemKey
        );
        String requestJson = objectMapper.writeValueAsString(request);

        int concurrency = 2;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CountDownLatch latch = new CountDownLatch(1);

        CompletableFuture<MvcResult> future1 = CompletableFuture.supplyAsync(() -> {
            try {
                latch.await();
                return mockMvc.perform(post("/api/jobs")
                        .header("X-API-Key", userAApiKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                    .andReturn();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);

        CompletableFuture<MvcResult> future2 = CompletableFuture.supplyAsync(() -> {
            try {
                latch.await();
                return mockMvc.perform(post("/api/jobs")
                        .header("X-API-Key", userAApiKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                    .andReturn();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);

        // Release latch to start both simultaneously
        latch.countDown();

        MvcResult result1 = future1.get();
        MvcResult result2 = future2.get();

        int status1 = result1.getResponse().getStatus();
        int status2 = result2.getResponse().getStatus();

        // Exactly one created (201) and one replayed (200), or both successful
        assertThat(List.of(status1, status2)).containsExactlyInAnyOrder(201, 200);

        // Verify exactly one row was created in the database
        Integer rowCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM jobs WHERE user_id = ? AND idempotency_key = ?",
            Integer.class,
            userA.getId(), idemKey
        );
        assertThat(rowCount).isEqualTo(1);

        executor.shutdown();
    }
}
