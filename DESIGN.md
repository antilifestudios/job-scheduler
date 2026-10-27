# Server-Side Request Forgery (SSRF) Defense — Design & Threat Model

> **Status:** Production Architecture Specification  
> **Document Scope:** Network Security, DNS Pinning, HTTP Client Hardening, Egress Filtering

---

## 1. Executive Summary & Threat Model

In an asynchronous HTTP job scheduler, client applications submit arbitrary destination URLs (`POST /api/jobs`) to be executed by background worker nodes. Without rigorous egress protection, outbound HTTP requests expose internal infrastructure to severe security vulnerabilities:

1. **Cloud Instance Metadata Extraction (IMDSv1/v2)**:
   - Target: `http://169.254.169.254/latest/meta-data/iam/security-credentials/`
   - Impact: Theft of temporary IAM credentials, leading to total cloud account compromise.
2. **Localhost & Internal Service Port Scanning**:
   - Target: `http://localhost:8081/actuator/env`, `http://127.0.0.1:5432` (PostgreSQL), `http://127.0.0.1:6379` (Redis).
   - Impact: Extraction of environment secrets, database manipulation, or lateral pivot within the host.
3. **Internal VPC Microservice Pivoting**:
   - Target: Probing private subnets (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`).
   - Impact: Exploiting unauthenticated internal APIs, monitoring consoles, or orchestrator control planes.
4. **DNS Rebinding Attacks (TOCTOU)**:
   - Attack: An attacker-controlled nameserver serves a domain with `TTL=0`. The initial DNS lookup returns a benign public IP (e.g. `93.184.216.34`), passing pre-flight checks. During socket connection, the client re-resolves the domain, receiving `169.254.169.254` or `127.0.0.1`.
   - Impact: Complete bypass of naive user-space DNS pre-resolution checks.
5. **Open Redirect SSRF**:
   - Attack: An external legitimate public web server issues an HTTP redirect (`302 Found`) with `Location: http://169.254.169.254/`.
   - Impact: Bypasses ingestion-time URL validation.
6. **Parser Confusion & Obfuscated IP Literals**:
   - Formats: Decimal (`http://2130706433/`), Octal (`http://0177.0.0.1/`), Hex (`http://0x7f000001/`), Backslash authority tricks (`http://example.com\@evil.com`), and IPv4-mapped IPv6 (`http://[::ffff:127.0.0.1]`).

---

## 2. Actual Guarantees Provided by the Application Layer

Our application-level SSRF defense establishes the following core invariants:

### The Single-Resolution & Hook Invariant
> **Security Invariant:**  
> *Every lookup used for a connection is validated and no path resolves outside the hook.*

Standard Java `java.net.http.HttpClient` re-resolves hostnames internally at socket connection time and lacks any public SPI to plug a custom DNS resolver. Consequently, it cannot defend against DNS rebinding.

We replace `java.net.http.HttpClient` with **Square OkHttp** (`com.squareup.okhttp3:okhttp:4.12.0`), which provides an official `okhttp3.Dns` interface:
1. When OkHttp establishes a connection to a hostname, it invokes `SsrfAwareDns.lookup(hostname)`.
2. `SsrfAwareDns` resolves the hostname via our pluggable `DnsResolver` with a bounded 5-second timeout.
3. Every resolved `InetAddress` is evaluated against our default-deny IP rules. If *any* address violates the policy, `UnknownHostException` is thrown immediately, aborting the connection before a single TCP packet is transmitted.
4. OkHttp connects **only** to the returned, validated `InetAddress`es.
5. In tests, injected resolvers (returning a public IP on query 1 and `127.0.0.1` on query 2) deterministically verify that the loopback address is **never connected to**.

### The Network Interceptor Guard (IP Literals & Hook Gap Coverage)
OkHttp's internal `RouteSelector` short-circuits `Dns.lookup` when the host string parses as an IP literal (`if (socketHost.canParseAsIpAddress()) listOf(InetAddress.getByName(socketHost))`).  
To close this gap unconditionally:
1. `SsrfValidator.validateUrl` fast-fails literal IPs during string parsing.
2. A custom OkHttp Network Interceptor (`SsrfNetworkInterceptor`) hooks into `chain.connection().socket().getInetAddress()` at the wire level:
   - Before any HTTP request bytes (headers or body) are written to the socket, the network interceptor extracts the actual connected `InetAddress` and re-validates it against the default-deny rules.
   - If an IP literal or route bypasses DNS resolution, the network interceptor immediately aborts the socket and throws `SsrfException`.

### TLS SNI & Hostname Verification
Even though the socket connects to an authoritatively pinned `InetAddress`, OkHttp's SSL/TLS engine preserves the original hostname in the Server Name Indication (SNI) extension and validates the peer certificate's Subject Alternative Names (SANs) using `OkHostnameVerifier`.

---

## 3. Authoritative IP Range Specification (Default-Deny)

The scheduler permits connections **only to Global Unicast Addresses**. All other ranges are blocked at the `InetAddress` object level.

### IPv4 Blocklist
| CIDR Block | Definition / RFC | Security Rationale |
| :--- | :--- | :--- |
| `0.0.0.0/8` | Current network (RFC 1122) | Non-routable broadcast/self-reference |
| `10.0.0.0/8` | Private network (RFC 1918) | Internal VPC / microservices |
| `100.64.0.0/10` | Shared Address Space / CGNAT (RFC 6598) | Carrier-grade NAT / infrastructure |
| `127.0.0.0/8` | Loopback (RFC 1122) | Host internal services (Actuator, DB) |
| `169.254.0.0/16` | Link-Local / Cloud Metadata (RFC 3927) | AWS/GCP/Azure instance metadata |
| `172.16.0.0/12` | Private network (RFC 1918) | Internal VPC / container bridges |
| `192.0.0.0/24` | IETF Protocol Assignments (RFC 6890) | Reserved protocol assignment |
| `192.0.2.0/24` | TEST-NET-1 Documentation (RFC 5737) | Non-routable documentation block |
| `192.168.0.0/16` | Private network (RFC 1918) | Internal office/home/VPC routers |
| `198.18.0.0/15` | Benchmarking (RFC 2544) | Network benchmark testing |
| `198.51.100.0/24` | TEST-NET-2 Documentation (RFC 5737) | Non-routable documentation block |
| `203.0.113.0/24` | TEST-NET-3 Documentation (RFC 5737) | Non-routable documentation block |
| `224.0.0.0/4` | Multicast (RFC 5771) | Multicast groups |
| `240.0.0.0/4` | Reserved / Class E (RFC 1112) | Former experimental reserved block |
| `255.255.255.255/32` | Limited Broadcast (RFC 919) | Local subnet broadcast |

### IPv6 Specification
- **Allowed Space**: Strictly restricted to **`2000::/3`** (Global Unicast Address space, binary prefix `001...`).
- Any address with high bits outside `001...` (e.g., `::/128`, `::1/128`, `fc00::/7` ULA, `fe80::/10` link-local, `ff00::/8` multicast) is rejected.
- **Carved-Out Subnet Exceptions within / alongside `2000::/3`**:
  - `::/128` (Unspecified)
  - `::1/128` (Loopback)
  - `::ffff:0:0/96` (IPv4-mapped IPv6): Unpacked into the underlying 4-byte IPv4 address and evaluated against the IPv4 blocklist (e.g. `::ffff:127.0.0.1` and `::ffff:169.254.169.254` are blocked).
  - `64:ff9b::/96` (IPv4/IPv6 translation, RFC 6052)
  - `2001::/32` (Teredo tunneling, RFC 4380)
  - `2001:db8::/32` (Documentation, RFC 3849)
  - `2002::/16` (6to4 tunneling, RFC 3056)
  - `fc00::/7` (Unique Local Address - ULA, RFC 4193)
  - `fe80::/10` (Link-Local Unicast, RFC 4291)
  - `ff00::/8` (Multicast, RFC 4291)

---

## 4. Port Policy & The Abuse Trade-Off

### Allowlist: `80`, `443`, `8080`, `8443`
- Webhook endpoints predominantly run on standard HTTP (`80`), HTTPS (`443`), or common alternative web ports (`8080`, `8443`).
- All other ports (e.g., 22 SSH, 25 SMTP, 53 DNS, 5432 Postgres, 8081 Actuator, 6379 Redis) are blocked unconditionally.

### The Port Policy Trade-Off
- **Allowing Arbitrary High Ports (1024–65535)**:  
  *Pros*: Supports esoteric self-hosted webhooks on arbitrary developer ports.  
  *Cons*: Turns the scheduler into a distributed port-scanning engine. An attacker can submit 65,000 jobs targeting an internal IP or external host, using worker response times and error codes to map open ports and internal service topology.
- **Strict Allowlist (80, 443, 8080, 8443)**:  
  *Pros*: Eliminates port-scanning abuse vectors while satisfying >99% of real-world webhook integrations.  
  *Cons*: External webhooks hosted on non-standard ports (e.g. `8000`, `3000`, `9000`) cannot be reached unless explicitly added to the allowlist configuration.

---

## 5. Secure Manual Redirect Loop

OkHttp's automatic redirects are disabled (`followRedirects(false)`). The worker manages redirects manually with strict security rules:

1. **Hop Limit**: Maximum 5 redirect hops.
2. **Cumulative Total Deadline**: A single timeout (e.g. 10s default, max 60s) spans all hops. Per-hop timeouts derive from `remainingNanos = deadlineNanos - System.nanoTime()`.
3. **Per-Hop DNS Resolution & Interception**: Every redirect target URL is resolved and validated through `SsrfAwareDns` and `SsrfNetworkInterceptor`.
4. **Scheme Re-check & Downgrade Prevention**: Only `http` and `https` are allowed; redirects from `HTTPS -> HTTP` are strictly rejected.
5. **Cross-Origin Header Stripping**:
   - If the target URL changes `scheme`, `host`, or `port`, the worker strips **ALL** user-supplied headers **except** `Content-Type`, `Accept`, and `User-Agent`.
   - Prevents leaking `Authorization` tokens, session cookies, and custom secret headers across domains.
6. **Method Mutation Rules (RFC 9110)**:
   - Status 301, 302, 303: Method is mutated to `GET`, and request body is dropped.
   - Status 307, 308: Original method and body are preserved.
7. **Response Read Cap**:
   - Outbound response streams are capped at 64 KB (65,536 bytes) read from the wire (including decompressed content) before the connection is closed.
   - Diagnostic snippet is capped at 4,096 characters, preventing TOAST table bloat in PostgreSQL.
8. **Proxy Bypass**: Configured with `Proxy.NO_PROXY` to prevent accidental routing through local corporate or environment proxies (`http.proxyHost`).

---

## 6. Error Handling & Startup Safety

1. **Generic User-Facing Error Messages**:
   - When an SSRF attempt is blocked, the user-facing job error (`last_error` and `job_attempts.error_message`) is set strictly to:  
     `"Destination host is not reachable or request rejected"`.
   - Detailed internal IP addresses, CIDR rules, and rebinding diagnostics are logged exclusively to server logs at `WARN` level to prevent information disclosure.
2. **Fail-Fast Startup Guard**:
   - The configuration property `scheduler.worker.ssrf-allow-private-ips` allows disabling IP validation during testing.
   - To prevent catastrophic production misconfigurations, `SsrfValidator.validateStartupConfig()` checks the active Spring profiles on startup. If `ssrf-allow-private-ips=true` and neither `"test"` nor `"dev"` is active, the application fails to start with a fatal `IllegalStateException`.

---

## 7. The True Defense-in-Depth: Network-Level Egress Filtering

> [!IMPORTANT]
> **Application-Level vs. Network-Level Defense**  
> While application-level DNS pinning and interceptors provide essential protection, **application-level code cannot be the sole SSRF barrier in a production enterprise environment.**  
> True production defense-in-depth requires network-level controls enforced by the kernel and cloud hypervisor.

### Production Egress Controls:
1. **Cloud Hypervisor / Security Group Rules**:
   - Outbound Security Groups must deny traffic to `169.254.169.254/32` at the hypervisor level.
   - AWS IMDSv2 must be enforced with `HttpTokens=required` and `HttpPutResponseHopLimit=1`, ensuring containers running in bridge/overlay networks cannot reach metadata even if IP routing exists.
2. **Kubernetes Egress NetworkPolicies (Calico / Cilium / AWS VPC CNI)**:
   - Worker pods must be isolated with egress `NetworkPolicy` objects that allow traffic only to 0.0.0.0/0 with explicit exceptions blocking RFC 1918 subnets and cloud metadata:
   ```yaml
   apiVersion: cilium.io/v2
   kind: CiliumNetworkPolicy
   metadata:
     name: scheduler-worker-egress
   spec:
     endpointSelector:
       matchLabels:
         app: scheduler-worker
     egress:
     - toCIDRSet:
       - cidr: 0.0.0.0/0
         except:
         - 10.0.0.0/8
         - 172.16.0.0/12
         - 192.168.0.0/16
         - 169.254.169.254/32
         - 127.0.0.0/8
       toPorts:
       - ports:
         - port: "80"
         - port: "443"
         - port: "8080"
         - port: "8443"
   ```
3. **Host-Level Iptables / Nftables Rules**:
   - If running bare-metal or on Podman/Docker, drop outbound packets targeting link-local metadata:
     `iptables -A OUTPUT -d 169.254.169.254 -j DROP`

---

## 8. Known Limitations

1. **Internal Services Hosted on Public IPs**:
   - If an organization hosts internal corporate services on publicly routable IP addresses without VPN/mutual TLS, application-level CIDR filtering will permit connections unless an explicit internal domain denylist is maintained.
2. **Shared Outbound Infrastructure & Side-Channel Timing**:
   - Connection attempts to unresponsive private IPs might take longer to time out than refused connections, theoretically enabling network topology timing attacks if connect timeouts are large. We mitigate this by bounding connect timeout to 2–5 seconds.
3. **Client Certificates & Mutual TLS (mTLS)**:
   - Client certificate authentication is currently global per worker rather than per-job. Per-tenant mTLS keys require custom SSLContext mapping.
4. **Leaked-Key-Can-Mint-Keys Limitation**:
   - All tenant API keys currently share full tenant administrative scope. An attacker possessing a valid API key can invoke `POST /api/keys` to mint additional keys or revoke other keys. In an enterprise RBAC model, key scopes (`jobs:write`, `jobs:read`, `keys:admin`) or a separate root account credential would be required to manage keys.

---

## 9. Multi-Tenancy, Quotas & API Key Architecture

### 9.1 Atomic Quota Enforcement Protocol
To prevent noisy neighbors from exhausting the shared worker engine and database storage, each tenant is assigned a concurrency quota (`users.max_active_jobs`, default: 100).

```text
 1. BEGIN TRANSACTION (in JobProducerService.createJob)
 2. SELECT ... FROM users WHERE id = :userId FOR UPDATE;  <-- Serializes concurrent submits for this tenant
 3. IF idempotency_key IS NOT NULL:
       SELECT ... FROM jobs WHERE user_id = :userId AND idempotency_key = :key;
       IF found -> RETURN 200 OK (Idempotent-Replayed: true);  <-- Idempotency check FIRST (never 429!)
 4. SELECT count(*) FROM jobs WHERE user_id = :userId AND status IN ('PENDING', 'RUNNING');
 5. IF count >= user.max_active_jobs:
       ROLLBACK & THROW QuotaExceededException -> RETURN 429 (code: "quota_exceeded", NO rate limit headers)
 6. INSERT INTO jobs (...) VALUES (...) ON CONFLICT (user_id, idempotency_key) DO NOTHING;
 7. COMMIT;
```

**Key Invariants**:
1. **Tenant Serialization**: Locking the tenant row (`SELECT ... FOR UPDATE`) serializes parallel submissions strictly for that specific tenant, leaving other tenants completely unblocked.
2. **Idempotent Replay Immunity**: If an existing job with the same idempotency key is already stored, the server returns HTTP 200 with `Idempotent-Replayed: true` immediately, even if the tenant is currently at 100% quota utilization.
3. **Double-Barrier Insertion**: `ON CONFLICT (user_id, idempotency_key) ... DO NOTHING` remains active as an ACID backstop against race conditions.

### 9.2 Token Bucket Rate Limiting (In-Memory CAS)
- **Per-Instance Architecture**: The rate limiter is strictly in-memory and per-instance. It requires zero Redis or external broker dependencies.
- **Lock-Free Atomic State**: Uses `AtomicReference<BucketState>` where `record BucketState(double tokens, long lastRefillNanos, long lastAccessNanos)`. Token refilling and consumption execute via lock-free CAS loops without thread contention.
- **Injectable Clocks**: Relies on `LongSupplier nanoTimeSupplier` and `Clock`, enabling deterministic testing without thread sleeps.
- **Idle Eviction**: Buckets idle for $>10$ minutes are purged periodically via `@Scheduled` tasks to prevent unbounded memory growth.
- **Distinct Error Contract**:
  - `rate_limited` on 429: Returns `Retry-After: <seconds>`, `X-RateLimit-Limit`, `X-RateLimit-Remaining: 0`, `X-RateLimit-Reset: <epoch_seconds>`.
  - `quota_exceeded` on 429: Emits strictly **NO** `Retry-After` or `X-RateLimit-*` headers.

### 9.3 Index Justification & EXPLAIN (ANALYZE, BUFFERS) Benchmark
To evaluate whether a dedicated partial index `CREATE INDEX idx_jobs_user_active ON jobs (user_id) WHERE status IN ('PENDING', 'RUNNING')` was justified over the existing composite index `CREATE INDEX idx_jobs_user_status ON jobs (user_id, status, created_at DESC)`, we seeded 100,000 completed (`SUCCEEDED`) rows and measured:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM jobs
WHERE user_id = :userId AND status IN ('PENDING', 'RUNNING');
```

**Actual PostgreSQL Execution Results**:
```text
Aggregate  (cost=23.23..23.24 rows=1 width=8) (actual time=0.019..0.020 rows=1.00 loops=1)
  Buffers: shared hit=3
  ->  Index Only Scan using idx_jobs_user_status on jobs  (cost=0.29..23.21 rows=7 width=0) (actual time=0.015..0.016 rows=7.00 loops=1)
        Index Cond: ((user_id = '...'::uuid) AND (status = ANY ('{PENDING,RUNNING}'::text[])))
        Heap Fetches: 7
        Index Searches: 1
        Buffers: shared hit=3
Planning Time: 0.194 ms
Execution Time: 0.031 ms
```

**Architectural Decision**:
- The existing composite index `idx_jobs_user_status` already performs an **Index Only Scan**, seeking directly to the `PENDING` and `RUNNING` btree leaf slices.
- The query executes in **0.031 milliseconds** with only **3 buffer page hits**, completely ignoring the 100,000 historical rows.
- Adding a dedicated partial index would introduce write amplification (maintenance of another index tree on every insert, heartbeat update, and state transition) with zero tangible read latency benefit. Thus, the existing index is superior.

### 9.4 API Key Management & Throttled Usage Tracking
- **Active Key Cap**: Capped at 5 active keys per user (`MAX_ACTIVE_KEYS = 5`). Exceeding this limit returns HTTP 409 `max_keys_exceeded`.
- **Last Key Protection**: Cannot revoke the only active key for a tenant (`cannot_revoke_last_key`).
- **Throttled `last_used_at`**: `ApiKeyAuthenticationFilter` maintains an in-memory `ConcurrentHashMap<UUID, Long>` throttling database writes to at most once per 60 seconds per key, preventing write storms under high throughput.
- **Tenant Isolation**: Every query and operation scopes `user_id = :currentUser`. Unauthorized access attempts return HTTP 404 Not Found to prevent tenant data enumeration.
- **First Key Bootstrapping**:
  - Development / Local: Automatically bootstrapped via `DevBootstrapRunner` (`@Profile("!test")`), which generates an initial key and prints it once to stdout.
  - Production: Provisioned via administrative CLI tools or migration scripts.

---

## 10. Recurring Cron Schedules Architecture (Phase 8)

### 10.1 Daylight Saving Time (DST) Transitions
Recurring cron schedules run against arbitrary IANA timezones (e.g., `America/New_York`, `Europe/London`, `Asia/Kolkata`). Because wall clocks shift during Daylight Saving Time, the scheduler adheres to strict transition invariants:

1. **Spring-Forward (Gap Jump)**:
   - *Example*: In `America/New_York` on Sunday, March 8, 2026, clocks spring forward from `01:59:59 EST` directly to `03:00:00 EDT`. The hour `02:00:00` to `02:59:59` does not exist on the local clock.
   - *Behavior*: A daily schedule configured for `0 30 2 * * *` (02:30 AM) cannot execute on March 8 because that local time does not exist. Spring's `CronExpression` evaluates the local timeline, detects the non-existent hour, and advances to the next valid calendar day: Monday, March 9, 2026 at `02:30:00 EDT`. Schedules targeting valid times on the transition day (e.g., 03:30 AM) execute normally.
2. **Fall-Back (Single Execution Invariant)**:
   - *Example*: In `America/New_York` on Sunday, November 1, 2026, clocks fall back from `02:00:00 EDT` (-04:00) to `01:00:00 EST` (-05:00). The local hour `01:00:00` to `01:59:59` occurs twice in real time (separated by 1 physical hour).
   - *Behavior*: A daily schedule targeting `0 30 1 * * *` (01:30 AM) **must fire strictly ONCE** on that calendar day.
   - *DST Fall-Back Guard*:
     - When the first occurrence at `01:30:00 EDT` (05:30:00 UTC) fires, `CronScheduleProcessor.advanceNextRunAt` inspects `cron.next(nowZdt)`.
     - Spring `CronExpression` calculates the next chronological match, which would mathematically be the second `01:30:00 EST` occurrence (06:30:00 UTC).
     - The guard compares `nextZdt.toLocalDateTime()` against `targetTickZdt.toLocalDateTime()`. Because they are identical (`2026-11-01T01:30:00`), it detects the repeated fall-back hour and automatically advances past it: `nextZdt = cron.next(nextZdt)`.
     - This sets `schedule.next_run_at` directly to `2026-11-02T01:30:00-05:00` (Nov 2).
     - One hour later at `01:30:00 EST` (06:30:00 UTC), the schedule is not due (`next_run_at` is Nov 2), completely preventing duplicate execution.

### 10.2 Transaction Structure & Connection Pool Safety
To guarantee high throughput and zero connection pool exhaustion under small connection pools (e.g., HikariCP `maximum-pool-size=3` with 6+ concurrent sweeper threads):

1. **Non-Transactional Sweeper Polling**:
   - `CronSchedulerEngine.pollOnce()` is intentionally **not** annotated with `@Transactional`.
   - `scheduleRepository.claimDueSchedules(BATCH_SIZE)` executes `SELECT ... FOR UPDATE SKIP LOCKED` inside its own isolated transaction and immediately commits.
   - **Crucial Invariant**: The claim row lock is **released immediately** upon returning candidate IDs to the engine. No database connection is held by `pollOnce()` while iterating over the batch.
2. **Connection Pool Deadlock Elimination**:
   - If `pollOnce()` held an open transaction (Connection 1) and then called `processSchedule` annotated with `@Transactional(propagation = Propagation.REQUIRES_NEW)`, Spring would suspend Connection 1 and checkout Connection 2 from HikariCP.
   - Under concurrent sweepers and small connection pools, multiple threads holding Connection 1 would exhaust the pool waiting for Connection 2, causing total HikariCP deadlock.
   - By releasing the claim connection immediately, every sweeper thread checks out at most **one** connection at a time.
3. **Lock Re-Check Guard in `processSchedule`**:
   - Because the initial claim lock was released, `processSchedule(scheduleId, now)` checks out a connection in its `REQUIRES_NEW` transaction and executes:
     ```sql
     SELECT * FROM schedules WHERE id = :scheduleId FOR UPDATE;
     ```
   - Immediately after acquiring the lock, it re-evaluates:
     ```java
     if (schedule == null || !schedule.isEnabled() || schedule.getNextRunAt().isAfter(now)) {
         return;
     }
     ```
   - If another sweeper thread already processed the schedule and advanced `next_run_at` into the future, the current thread detects `next_run_at > now` and safely bails out without duplicating the job.

### 10.3 Grace Window & Misfire Policy
- **Lateness Measurement**: Lateness is calculated as `now - most recent missed tick` (using `findLatestMissedTick`), rather than total downtime since the oldest missed tick.
- **`SKIP` Policy**: If the scheduler was down for multiple ticks or lateness exceeds `grace_window_seconds`, the schedule skips missed executions, records `last_skip_reason = MISFIRE_SKIP`, and advances `next_run_at` to the first tick strictly after `now`.
- **`FIRE_ONCE` Policy**: If lateness <= `grace_window_seconds`, the scheduler fires exactly **one** catch-up job for the most recent missed tick with deterministic idempotency key `cron:<scheduleId>:<targetTickEpochSecond>`, and advances `next_run_at` strictly into the future. If lateness > `grace_window_seconds`, it skips execution.

### 10.4 Tenant Schedule Cap & Error Contract Alignment
- **Tenant Serialization**: In `ScheduleService.createSchedule`, the tenant row is locked via `SELECT * FROM users WHERE id = :userId FOR UPDATE`.
- **Atomic Cap Enforcement**: The active schedule count is evaluated under the lock (`countByUserId(userId)`). If `count >= maxSchedules` (default: 20), it throws `MaxSchedulesExceededException`.
- **Unified Quota Error Family**:
  - HTTP Status: `429 Too Many Requests`.
  - JSON Code: `"quota_exceeded"` (aligned with job active quota).
  - Body: Includes `maxSchedules` and `currentSchedules`.
  - Headers: Strictly **no** `Retry-After` or `X-RateLimit-*` headers (which are emitted exclusively for token bucket rate limits).

### 10.5 Circuit Breaker & Error Sanitization
- **Permanent Error Isolation**: Permanent validation errors (e.g., malformed template JSON, unsupported HTTP method) auto-disable the schedule immediately (`enabled = false`, `disabled_reason = "PERMANENT_VALIDATION_ERROR"`).
- **Transient Error Threshold**: Transient errors (e.g., database connection timeouts) increment `consecutive_failures`. Upon reaching `maxConsecutiveFailures` (5), the schedule auto-disables (`disabled_reason = "MAX_CONSECUTIVE_FAILURES_EXCEEDED"`).
- **Security Sanitization**: Raw database exceptions (Postgres SQL syntax, connection strings) and Java stack traces are stripped before storage in `last_error`. Only safe, sanitized messages (e.g. `"Database error during schedule execution"`) are persisted and exposed via the API.

