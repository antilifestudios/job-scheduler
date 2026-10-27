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
