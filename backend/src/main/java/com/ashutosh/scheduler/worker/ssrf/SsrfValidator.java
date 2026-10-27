package com.ashutosh.scheduler.worker.ssrf;

import com.ashutosh.scheduler.worker.engine.WorkerProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Authoritative Server-Side Request Forgery (SSRF) validator.
 *
 * Implements a strict default-deny IP policy:
 * - Only global unicast IP addresses are permitted.
 * - All private (RFC 1918), loopback, link-local / cloud metadata (169.254.0.0/16),
 *   CGNAT, multicast, broadcast, and reserved address ranges are blocked.
 * - IPv6 is restricted strictly to 2000::/3 minus documented exception subnets.
 * - IPv4-mapped IPv6 addresses (::ffff:0:0/96) are unpacked and evaluated as IPv4.
 * - Port access is restricted to an allowlist (80, 443, 8080, 8443).
 * - Parser confusion tricks (userinfo, backslashes, fragments, decimal/octal/hex IPs) are fast-failed.
 */
@Component
public class SsrfValidator {

    private static final Logger log = LoggerFactory.getLogger(SsrfValidator.class);

    public static final Set<Integer> ALLOWED_PORTS = Set.of(80, 443, 8080, 8443);

    private static final Pattern DECIMAL_IP_PATTERN = Pattern.compile("^[0-9]+$");
    private static final Pattern HEX_IP_PATTERN = Pattern.compile("(?i).*0x[0-9a-f]+.*");

    private final WorkerProperties properties;
    private final Environment environment;

    // IPv4 Default-Deny Blocklist
    private static final List<Ipv4Cidr> IPV4_DENYLIST = List.of(
        new Ipv4Cidr("0.0.0.0/8", "Current network (RFC 1122)"),
        new Ipv4Cidr("10.0.0.0/8", "RFC 1918 Private"),
        new Ipv4Cidr("100.64.0.0/10", "Shared Address Space / CGNAT (RFC 6598)"),
        new Ipv4Cidr("127.0.0.0/8", "Loopback (RFC 1122)"),
        new Ipv4Cidr("169.254.0.0/16", "Link-Local / Cloud Metadata (RFC 3927)"),
        new Ipv4Cidr("172.16.0.0/12", "RFC 1918 Private"),
        new Ipv4Cidr("192.0.0.0/24", "IETF Protocol Assignments (RFC 6890)"),
        new Ipv4Cidr("192.0.2.0/24", "TEST-NET-1 Documentation (RFC 5737)"),
        new Ipv4Cidr("192.168.0.0/16", "RFC 1918 Private"),
        new Ipv4Cidr("198.18.0.0/15", "Benchmarking (RFC 2544)"),
        new Ipv4Cidr("198.51.100.0/24", "TEST-NET-2 Documentation (RFC 5737)"),
        new Ipv4Cidr("203.0.113.0/24", "TEST-NET-3 Documentation (RFC 5737)"),
        new Ipv4Cidr("224.0.0.0/4", "Multicast (RFC 5771)"),
        new Ipv4Cidr("240.0.0.0/4", "Reserved / Class E (RFC 1112)"),
        new Ipv4Cidr("255.255.255.255/32", "Limited Broadcast (RFC 919)")
    );

    // IPv6 Blocklist (Exceptions carved out of 2000::/3 and known special ranges)
    private static final List<Ipv6Cidr> IPV6_DENYLIST = List.of(
        new Ipv6Cidr("64:ff9b::/96", "IPv4/IPv6 translation (RFC 6052)"),
        new Ipv6Cidr("2001::/32", "Teredo tunneling (RFC 4380)"),
        new Ipv6Cidr("2001:db8::/32", "Documentation (RFC 3849)"),
        new Ipv6Cidr("2002::/16", "6to4 (RFC 3056)"),
        new Ipv6Cidr("fc00::/7", "Unique Local Address ULA (RFC 4193)"),
        new Ipv6Cidr("fe80::/10", "Link-Local Unicast (RFC 4291)"),
        new Ipv6Cidr("ff00::/8", "Multicast (RFC 4291)")
    );

    public SsrfValidator(WorkerProperties properties, Environment environment) {
        this.properties = properties;
        this.environment = environment;
    }

    @PostConstruct
    public void validateStartupConfig() {
        if (properties.isSsrfAllowPrivateIps()) {
            String[] activeProfiles = environment.getActiveProfiles();
            boolean isTestOrDev = Arrays.stream(activeProfiles)
                .anyMatch(p -> p.equalsIgnoreCase("test") || p.equalsIgnoreCase("dev"));
            if (!isTestOrDev) {
                throw new IllegalStateException("FATAL: scheduler.worker.ssrf.allow-private-ips cannot be enabled outside test or dev profiles!");
            }
            log.warn("WARNING: SSRF private IP validation is relaxed because active profile is test/dev.");
        }
    }

    /**
     * Fast-fail URL syntax, parser quirk, and port validation.
     * Note: Authoritative IP checks occur on resolved InetAddress objects.
     */
    public void validateUrl(String rawUrl) throws SsrfException {
        if (rawUrl == null || rawUrl.isBlank()) {
            throw new SsrfException("URL cannot be null or blank");
        }

        // Fast-fail parser confusion tricks
        if (rawUrl.contains("\\")) {
            throw new SsrfException("URL contains disallowed backslash character");
        }
        if (rawUrl.contains("#")) {
            throw new SsrfException("URL contains disallowed fragment component");
        }

        URI uri;
        try {
            uri = URI.create(rawUrl.trim());
        } catch (IllegalArgumentException e) {
            throw new SsrfException("Invalid URI syntax: " + e.getMessage(), e);
        }

        // Scheme validation
        String scheme = uri.getScheme();
        if (scheme == null || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
            throw new SsrfException("Disallowed scheme: '" + scheme + "'. Only http and https are permitted.");
        }

        // Userinfo rejection
        if (uri.getUserInfo() != null || rawUrl.contains("@")) {
            throw new SsrfException("Disallowed userinfo component in URL authority");
        }

        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new SsrfException("URL must contain a valid host component");
        }

        // Port validation
        int port = uri.getPort();
        if (port == -1) {
            port = "https".equalsIgnoreCase(scheme) ? 443 : 80;
        }
        if (!properties.isSsrfAllowPrivateIps() && !ALLOWED_PORTS.contains(port)) {
            throw new SsrfException("Port " + port + " is not allowed. Only ports 80, 443, 8080, 8443 are permitted.");
        }

        // Fast-fail non-standard IP representations (decimal, hex, octal)
        validateHostFormatting(host);
    }

    private void validateHostFormatting(String host) throws SsrfException {
        // Pure numeric decimal IP (e.g., 2130706433)
        if (DECIMAL_IP_PATTERN.matcher(host).matches()) {
            throw new SsrfException("Decimal-encoded IP address literals are forbidden");
        }

        // Hexadecimal IP literals (e.g., 0x7f000001 or dotted hex)
        if (HEX_IP_PATTERN.matcher(host).matches()) {
            throw new SsrfException("Hexadecimal-encoded IP address literals are forbidden");
        }

        // Octal IP literals (leading zeros on dotted IPv4 segments, e.g. 0177.0.0.1)
        if (host.contains(".")) {
            String[] parts = host.split("\\.");
            if (parts.length == 4) {
                for (String part : parts) {
                    if (part.length() > 1 && part.startsWith("0")) {
                        throw new SsrfException("Octal-encoded IP address literals are forbidden");
                    }
                }
            }
        }

        // If host is a standard IP literal, validate it immediately against authoritative IP rules
        String cleanHost = (host.startsWith("[") && host.endsWith("]"))
            ? host.substring(1, host.length() - 1)
            : host;
        try {
            // Check if cleanHost is a valid IP address literal
            InetAddress addr = InetAddress.getByName(cleanHost);
            // If getByName returns without network lookup (an IP literal), validate it immediately
            if (addr.getHostAddress().equalsIgnoreCase(cleanHost) || cleanHost.contains(":")) {
                validateAddress(addr);
            }
        } catch (UnknownHostException ignored) {
            // Host is a domain name, not an IP literal
        }
    }

    /**
     * Authoritative check on an InetAddress object.
     * Enforces the default-deny global unicast policy.
     */
    public void validateAddress(InetAddress address) throws SsrfException {
        if (address == null) {
            throw new SsrfException("InetAddress is null");
        }

        // Allow private IPs ONLY if explicitly enabled in test/dev profiles
        if (properties.isSsrfAllowPrivateIps()) {
            return;
        }

        byte[] rawBytes = address.getAddress();

        // 1. Check for IPv4-mapped IPv6 address (::ffff:0:0/96)
        if (address instanceof Inet6Address && isIpv4Mapped(rawBytes)) {
            byte[] ipv4Bytes = Arrays.copyOfRange(rawBytes, 12, 16);
            validateIpv4Bytes(ipv4Bytes, address.getHostAddress());
            return;
        }

        if (address instanceof Inet4Address) {
            validateIpv4Bytes(rawBytes, address.getHostAddress());
        } else if (address instanceof Inet6Address) {
            validateIpv6Bytes(rawBytes, address.getHostAddress());
        } else {
            throw new SsrfException("Unsupported IP address family: " + address.getClass().getName());
        }
    }

    private void validateIpv4Bytes(byte[] rawBytes, String hostAddress) throws SsrfException {
        long ip = ipToLong(rawBytes);

        for (Ipv4Cidr cidr : IPV4_DENYLIST) {
            if (cidr.matches(ip)) {
                log.warn("SSRF Policy Violation: IP {} is in restricted IPv4 range {} ({})",
                    hostAddress, cidr.cidr, cidr.description);
                throw new SsrfException("Host resolves to restricted IP address");
            }
        }
    }

    private void validateIpv6Bytes(byte[] rawBytes, String hostAddress) throws SsrfException {
        // Rule: IPv6 MUST be within 2000::/3 (Global Unicast Address space: binary 001...)
        // First byte must have high 3 bits matching 001 (i.e. (b[0] & 0xE0) == 0x20)
        int firstByte = rawBytes[0] & 0xFF;
        if ((firstByte & 0xE0) != 0x20) {
            log.warn("SSRF Policy Violation: IPv6 {} is outside 2000::/3 global unicast space", hostAddress);
            throw new SsrfException("Host resolves to restricted IP address");
        }

        // Check against documented exception subnets
        for (Ipv6Cidr cidr : IPV6_DENYLIST) {
            if (cidr.matches(rawBytes)) {
                log.warn("SSRF Policy Violation: IPv6 {} is in restricted IPv6 range {} ({})",
                    hostAddress, cidr.cidr, cidr.description);
                throw new SsrfException("Host resolves to restricted IP address");
            }
        }
    }

    private static boolean isIpv4Mapped(byte[] bytes) {
        if (bytes.length != 16) return false;
        for (int i = 0; i < 10; i++) {
            if (bytes[i] != 0) return false;
        }
        return (bytes[10] & 0xFF) == 0xFF && (bytes[11] & 0xFF) == 0xFF;
    }

    private static long ipToLong(byte[] bytes) {
        return ((bytes[0] & 0xFFL) << 24) |
               ((bytes[1] & 0xFFL) << 16) |
               ((bytes[2] & 0xFFL) << 8)  |
               (bytes[3] & 0xFFL);
    }

    public static final class Ipv4Cidr {
        private final long network;
        private final long mask;
        private final String cidr;
        private final String description;

        public Ipv4Cidr(String cidr, String description) {
            this.cidr = cidr;
            this.description = description;
            String[] parts = cidr.split("/");
            String[] octets = parts[0].split("\\.");
            byte[] bytes = new byte[4];
            for (int i = 0; i < 4; i++) {
                bytes[i] = (byte) Integer.parseInt(octets[i]);
            }
            int prefix = Integer.parseInt(parts[1]);
            this.mask = prefix == 0 ? 0L : (0xFFFFFFFFL << (32 - prefix)) & 0xFFFFFFFFL;
            this.network = ipToLong(bytes) & mask;
        }

        public boolean matches(long ip) {
            return (ip & mask) == network;
        }
    }

    public static final class Ipv6Cidr {
        private final byte[] network;
        private final int prefixLength;
        private final String cidr;
        private final String description;

        public Ipv6Cidr(String cidr, String description) {
            this.cidr = cidr;
            this.description = description;
            String[] parts = cidr.split("/");
            try {
                this.network = InetAddress.getByName(parts[0]).getAddress();
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid IPv6 CIDR: " + cidr, e);
            }
            this.prefixLength = Integer.parseInt(parts[1]);
        }

        public boolean matches(byte[] address) {
            if (address.length != 16) return false;
            int fullBytes = prefixLength / 8;
            for (int i = 0; i < fullBytes; i++) {
                if (address[i] != network[i]) return false;
            }
            int remainingBits = prefixLength % 8;
            if (remainingBits > 0) {
                int mask = (0xFF << (8 - remainingBits)) & 0xFF;
                return (address[fullBytes] & mask) == (network[fullBytes] & mask);
            }
            return true;
        }
    }
}
