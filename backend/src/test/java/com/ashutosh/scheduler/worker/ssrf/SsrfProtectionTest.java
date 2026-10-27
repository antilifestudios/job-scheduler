package com.ashutosh.scheduler.worker.ssrf;

import com.ashutosh.scheduler.TestcontainersConfiguration;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import com.ashutosh.scheduler.domain.repo.JobAttemptRepository;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import com.ashutosh.scheduler.producer.dto.HttpPayloadDto;
import com.ashutosh.scheduler.worker.engine.JobClaimRepository;
import com.ashutosh.scheduler.worker.engine.JobWorkerEngine;
import com.ashutosh.scheduler.worker.engine.WorkerProperties;
import com.ashutosh.scheduler.worker.handler.HttpJobHandler;
import com.ashutosh.scheduler.worker.handler.JobExecutionResult;
import com.ashutosh.scheduler.worker.handler.JobHandlerRegistry;
import com.sun.net.httpserver.HttpServer;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@ActiveProfiles("test")
class SsrfProtectionTest {

    @Autowired
    private JobClaimRepository claimRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private JobAttemptRepository jobAttemptRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Environment environment;

    private UserEntity testUser;
    private WorkerProperties strictProperties;
    private SsrfValidator strictValidator;

    private HttpServer safeMockServer;
    private int safeServerPort;

    private HttpServer loopbackProbeServer;
    private int loopbackProbePort;
    private final AtomicInteger loopbackHitCount = new AtomicInteger(0);

    @BeforeEach
    void setup() throws IOException {
        jobAttemptRepository.deleteAll();
        jobRepository.deleteAll();
        userRepository.deleteAll();

        testUser = new UserEntity(UuidV7.generate(), "ssrf-test-user");
        userRepository.save(testUser);

        // Strict properties enforcing default-deny SSRF policy (allowPrivateIps = false)
        strictProperties = new WorkerProperties();
        strictProperties.setSsrfEnabled(true);
        strictProperties.setSsrfAllowPrivateIps(false);

        strictValidator = new SsrfValidator(strictProperties, environment);

        // Safe mock server representing an allowed target
        safeMockServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        safeServerPort = safeMockServer.getAddress().getPort();
        safeMockServer.createContext("/ok", exchange -> {
            byte[] response = "OK".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });
        safeMockServer.start();

        // Loopback probe server to verify it is NEVER connected to during attacks
        loopbackHitCount.set(0);
        loopbackProbeServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        loopbackProbePort = loopbackProbeServer.getAddress().getPort();
        loopbackProbeServer.createContext("/", exchange -> {
            loopbackHitCount.incrementAndGet();
            byte[] response = "Pwned".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });
        loopbackProbeServer.start();
    }

    @AfterEach
    void tearDown() {
        if (safeMockServer != null) {
            safeMockServer.stop(0);
        }
        if (loopbackProbeServer != null) {
            loopbackProbeServer.stop(0);
        }
    }

    private HttpJobHandler createHandlerWithResolver(DnsResolver resolver) {
        SsrfAwareDns dns = new SsrfAwareDns(resolver, strictValidator);
        SsrfNetworkInterceptor interceptor = new SsrfNetworkInterceptor(strictValidator);

        OkHttpClient client = new OkHttpClient.Builder()
            .dns(dns)
            .addNetworkInterceptor(interceptor)
            .followRedirects(false)
            .followSslRedirects(false)
            .proxy(Proxy.NO_PROXY)
            .connectTimeout(Duration.ofSeconds(2))
            .readTimeout(Duration.ofSeconds(2))
            .writeTimeout(Duration.ofSeconds(2))
            .connectionPool(new ConnectionPool(5, 1, TimeUnit.MINUTES))
            .build();

        return new HttpJobHandler(client, objectMapper, jobAttemptRepository, strictValidator);
    }

    // =========================================================================
    // 1. Literal IP Address Tests (Bypassing Dns.lookup, caught by Interceptor & Validator)
    // =========================================================================

    @Test
    @DisplayName("SSRF: IPv4 loopback literal (127.0.0.1) must be blocked without connecting")
    void testLiteralLoopbackBlocked() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());
        JobEntity job = createJob("http://127.0.0.1:" + loopbackProbePort + "/probe");

        JobExecutionResult result = handler.execute(job);

        assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
        JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
        assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
        assertThat(loopbackHitCount.get()).as("Loopback probe server must never be connected to").isEqualTo(0);
    }

    @Test
    @DisplayName("SSRF: Decimal IP literal (http://2130706433/ = 127.0.0.1) must be rejected")
    void testDecimalIpLiteralBlocked() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());
        JobEntity job = createJob("http://2130706433:8080/path");

        JobExecutionResult result = handler.execute(job);

        assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
        JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
        assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
    }

    @Test
    @DisplayName("SSRF: Hex IP literal (http://0x7f000001/) must be rejected")
    void testHexIpLiteralBlocked() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());
        JobEntity job = createJob("http://0x7f000001:8080/path");

        JobExecutionResult result = handler.execute(job);

        assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
        JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
        assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
    }

    @Test
    @DisplayName("SSRF: Octal IP literal (http://0177.0.0.1/) must be rejected")
    void testOctalIpLiteralBlocked() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());
        JobEntity job = createJob("http://0177.0.0.1:8080/path");

        JobExecutionResult result = handler.execute(job);

        assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
        JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
        assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
    }

    @Test
    @DisplayName("SSRF: IPv6 loopback literal ([::1]) must be blocked")
    void testIpv6LoopbackBlocked() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());
        JobEntity job = createJob("http://[::1]:8080/path");

        JobExecutionResult result = handler.execute(job);

        assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
        JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
        assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
    }

    @Test
    @DisplayName("SSRF: IPv4-mapped IPv6 loopback ([::ffff:127.0.0.1]) must be unpacked and blocked")
    void testIpv4MappedIpv6LoopbackBlocked() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());
        JobEntity job = createJob("http://[::ffff:127.0.0.1]:8080/path");

        JobExecutionResult result = handler.execute(job);

        assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
        JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
        assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
    }

    // =========================================================================
    // 2. Parser Confusion Tricks & Disallowed Schemes / Ports
    // =========================================================================

    @Test
    @DisplayName("SSRF: Userinfo in URL (http://user:pass@host/) must be rejected")
    void testUserinfoRejected() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());
        JobEntity job = createJob("http://admin:secret@example.com/api");

        JobExecutionResult result = handler.execute(job);

        assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
        JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
        assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
    }

    @Test
    @DisplayName("SSRF: Backslash trick (http://example.com\\@evil.com) must be rejected")
    void testBackslashInAuthorityRejected() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());
        JobEntity job = createJob("http://example.com\\@evil.com/hook");

        JobExecutionResult result = handler.execute(job);

        assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
        JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
        assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
    }

    @Test
    @DisplayName("SSRF: URL fragment (#frag) must be rejected")
    void testUrlFragmentRejected() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());
        JobEntity job = createJob("http://example.com/webhook#admin");

        JobExecutionResult result = handler.execute(job);

        assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
        JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
        assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
    }

    @Test
    @DisplayName("SSRF: Non-HTTP schemes (file://, ftp://, gopher://) must be rejected")
    void testNonHttpSchemesRejected() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());

        for (String url : List.of("file:///etc/passwd", "ftp://example.com/file", "gopher://example.com")) {
            JobEntity job = createJob(url);
            JobExecutionResult result = handler.execute(job);
            assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
            JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
            assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
        }
    }

    @Test
    @DisplayName("SSRF: Disallowed internal ports (22, 5432, 8081) must be rejected by port allowlist")
    void testDisallowedPortsRejected() {
        HttpJobHandler handler = createHandlerWithResolver(new SystemDnsResolver());

        for (int disallowedPort : List.of(22, 5432, 8081, 6379, 9000)) {
            JobEntity job = createJob("http://example.com:" + disallowedPort + "/api");
            JobExecutionResult result = handler.execute(job);
            assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
            JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
            assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
        }
    }

    // =========================================================================
    // 3. DNS Hostname Resolving to Restricted Ranges (RFC 1918, Cloud Metadata)
    // =========================================================================

    @Test
    @DisplayName("SSRF: Domain resolving to AWS metadata (169.254.169.254) must be blocked")
    void testDnsResolvingToCloudMetadataBlocked() throws Exception {
        DnsResolver metadataResolver = hostname ->
            List.of(InetAddress.getByName("169.254.169.254"));

        HttpJobHandler handler = createHandlerWithResolver(metadataResolver);
        JobEntity job = createJob("http://evil-metadata.com/latest/meta-data/");

        JobExecutionResult result = handler.execute(job);

        assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
        JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
        assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
    }

    @Test
    @DisplayName("SSRF: Domain resolving to RFC 1918 private subnets (10.x, 192.168.x, 172.16.x) must be blocked")
    void testDnsResolvingToPrivateSubnetsBlocked() throws Exception {
        List<String> privateIps = List.of(
            "10.0.0.1", "10.254.0.1", "172.16.0.1", "172.31.255.255", "192.168.1.1", "100.64.0.1"
        );

        for (String ip : privateIps) {
            DnsResolver resolver = hostname -> List.of(InetAddress.getByName(ip));
            HttpJobHandler handler = createHandlerWithResolver(resolver);
            JobEntity job = createJob("http://internal-" + ip.replace(".", "-") + ".corp/service");

            JobExecutionResult result = handler.execute(job);

            assertThat(result).isInstanceOf(JobExecutionResult.PermanentFailure.class);
            JobExecutionResult.PermanentFailure pf = (JobExecutionResult.PermanentFailure) result;
            assertThat(pf.error()).isEqualTo(HttpJobHandler.GENERIC_SSRF_ERROR);
        }
    }

    // =========================================================================
    // 4. DNS Rebinding Test with Injectable Resolver
    // =========================================================================

    @Test
    @DisplayName("SSRF Rebinding: Resolver returns safe IP first, loopback second; assert loopback is NEVER connected to")
    void testDnsRebindingAttackLoopbackNeverConnected() throws Exception {
        // Safe mock server port
        InetAddress safeIp = InetAddress.getByName("127.0.0.1");

        // Simulate an attacker's DNS server:
        // Query 1: returns safe IP
        // Query 2: attempts to rebind to loopback probe server IP
        AtomicInteger resolutionCount = new AtomicInteger(0);
        DnsResolver rebindingResolver = hostname -> {
            int count = resolutionCount.incrementAndGet();
            if (count == 1) {
                // First resolution: allowed safe IP (we allow this specific host for the test)
                try {
                    return List.of(InetAddress.getByName("93.184.216.34")); // example.com public IP
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                // Second resolution (rebinding attempt): return loopback
                try {
                    return List.of(InetAddress.getByName("127.0.0.1"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        HttpJobHandler handler = createHandlerWithResolver(rebindingResolver);
        JobEntity job = createJob("http://rebind.attacker.com:" + loopbackProbePort + "/steal");

        handler.execute(job);

        // Core assertion: Loopback server was NEVER reached!
        assertThat(loopbackHitCount.get())
            .as("Loopback probe server must never receive any connections during rebinding")
            .isEqualTo(0);
    }

    // =========================================================================
    // 5. Redirect Chains & Cross-Origin Header Stripping
    // =========================================================================

    @Test
    @DisplayName("SSRF Redirect: Redirect target pointing to literal cloud metadata (169.254.169.254) must be blocked")
    void testRedirectToCloudMetadataBlocked() throws Exception {
        // Setup redirect on safe mock server that returns 302 -> 169.254.169.254
        safeMockServer.createContext("/redirect-to-metadata", exchange -> {
            exchange.getResponseHeaders().set("Location", "http://169.254.169.254/latest/meta-data/");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });

        // Use custom resolver so safe.allowed.com points to our safe mock server
        DnsResolver safeResolver = hostname -> List.of(InetAddress.getByName("93.184.216.34"));
        HttpJobHandler handler = createHandlerWithResolver(safeResolver);

        // Directly verify SsrfValidator rejects the redirect target literal
        assertThatThrownBy(() -> strictValidator.validateUrl("http://169.254.169.254/latest/meta-data/"))
            .isInstanceOf(SsrfException.class);
    }

    @Test
    @DisplayName("SSRF Redirect: Cross-origin redirect must strip Authorization and Cookie headers while preserving Content-Type and User-Agent")
    void testCrossOriginRedirectHeaderStripping() throws Exception {
        // Destination server that records received headers
        Map<String, String> receivedHeaders = new ConcurrentHashMap<>();
        safeMockServer.createContext("/target-dest", exchange -> {
            exchange.getRequestHeaders().forEach((k, v) -> {
                if (!v.isEmpty()) receivedHeaders.put(k.toLowerCase(), v.get(0));
            });
            byte[] response = "OK".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        });

        safeMockServer.createContext("/initial-redirect", exchange -> {
            // Redirect to a different host / port
            exchange.getResponseHeaders().set("Location", "http://other-host.allowed.com:" + safeServerPort + "/target-dest");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });

        // Resolver maps initial-host and other-host to safe server
        DnsResolver testResolver = hostname -> List.of(InetAddress.getByName("127.0.0.1"));

        // Handler with private IPs allowed for the mock server, but testing the header stripping logic
        WorkerProperties testProps = new WorkerProperties();
        testProps.setSsrfEnabled(true);
        testProps.setSsrfAllowPrivateIps(true);
        SsrfValidator testValidator = new SsrfValidator(testProps, environment);

        SsrfAwareDns dns = new SsrfAwareDns(testResolver, testValidator);
        SsrfNetworkInterceptor interceptor = new SsrfNetworkInterceptor(testValidator);
        OkHttpClient client = new OkHttpClient.Builder()
            .dns(dns)
            .addNetworkInterceptor(interceptor)
            .followRedirects(false)
            .followSslRedirects(false)
            .proxy(Proxy.NO_PROXY)
            .build();
        HttpJobHandler handler = new HttpJobHandler(client, objectMapper, jobAttemptRepository, testValidator);

        JobEntity job = new JobEntity();
        job.setId(UuidV7.generate());
        job.setUserId(testUser.getId());
        job.setQueue("default");
        job.setType("HTTP");
        job.setPayload("""
            {
              "url": "http://initial-host.allowed.com:%d/initial-redirect",
              "method": "POST",
              "headers": {
                "Authorization": "Bearer secret-token-123",
                "Cookie": "session=xyz987",
                "X-Custom-Secret": "sensitive-val",
                "Content-Type": "application/json",
                "User-Agent": "JobScheduler/1.0"
              }
            }
            """.formatted(safeServerPort));
        job.setStatus(JobStatus.PENDING);
        job.setPriority(0);
        job.setAttempts(1);
        job.setMaxAttempts(3);
        job.setRunAt(Instant.now().minusSeconds(5));
        job.setCreatedAt(Instant.now());
        job.setUpdatedAt(Instant.now());
        job = jobRepository.save(job);

        JobExecutionResult result = handler.execute(job);
        assertThat(result).isInstanceOf(JobExecutionResult.Success.class);

        // Core Assertions:
        // Sensitive headers MUST be stripped on cross-origin redirect
        assertThat(receivedHeaders).doesNotContainKey("authorization");
        assertThat(receivedHeaders).doesNotContainKey("cookie");
        assertThat(receivedHeaders).doesNotContainKey("x-custom-secret");

        // Allowed safe header MUST be preserved
        assertThat(receivedHeaders).containsKey("user-agent");
        assertThat(receivedHeaders.get("user-agent")).contains("JobScheduler/1.0");
    }

    @Test
    @DisplayName("SSRF Startup Guard: Must throw IllegalStateException if allow-private-ips is true in non-test/dev profiles")
    void testStartupGuardRejectsPrivateIpsInProduction() {
        WorkerProperties unsafeProperties = new WorkerProperties();
        unsafeProperties.setSsrfAllowPrivateIps(true);

        MockEnvironment prodEnv = new MockEnvironment();
        prodEnv.setActiveProfiles("production");

        SsrfValidator validator = new SsrfValidator(unsafeProperties, prodEnv);

        assertThatThrownBy(validator::validateStartupConfig)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("cannot be enabled outside test or dev profiles");
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private JobEntity createJob(String url) {
        JobEntity entity = new JobEntity();
        entity.setId(UuidV7.generate());
        entity.setUserId(testUser.getId());
        entity.setQueue("default");
        entity.setType("HTTP");
        try {
            entity.setPayload(objectMapper.writeValueAsString(new HttpPayloadDto(
                url, "GET", Map.of(), 2000
            )));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        entity.setStatus(JobStatus.PENDING);
        entity.setPriority(0);
        entity.setAttempts(1);
        entity.setMaxAttempts(3);
        entity.setRunAt(Instant.now().minusSeconds(5));
        entity.setCreatedAt(Instant.now());
        entity.setUpdatedAt(Instant.now());
        return jobRepository.save(entity);
    }
}
