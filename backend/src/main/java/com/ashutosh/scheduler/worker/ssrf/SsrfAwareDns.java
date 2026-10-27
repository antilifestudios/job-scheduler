package com.ashutosh.scheduler.worker.ssrf;

import okhttp3.Dns;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Custom OkHttp Dns implementation enforcing the SSRF security invariant:
 * Every lookup used for a connection is validated and no path resolves outside the hook.
 */
@Component
public class SsrfAwareDns implements Dns {

    private static final Logger log = LoggerFactory.getLogger(SsrfAwareDns.class);
    private static final long DNS_TIMEOUT_SECONDS = 5;

    private final DnsResolver resolver;
    private final SsrfValidator validator;

    public SsrfAwareDns(DnsResolver resolver, SsrfValidator validator) {
        this.resolver = resolver;
        this.validator = validator;
    }

    @NotNull
    @Override
    public List<InetAddress> lookup(@NotNull String hostname) throws UnknownHostException {
        if (hostname.isBlank()) {
            throw new UnknownHostException("Hostname cannot be blank");
        }

        // Bounded DNS resolution time (prevents DNS tarpit / slow-hang attacks)
        CompletableFuture<List<InetAddress>> future = CompletableFuture.supplyAsync(() -> {
            try {
                return resolver.resolve(hostname);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        });

        List<InetAddress> addresses;
        try {
            addresses = future.get(DNS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new UnknownHostException("DNS resolution timed out after " + DNS_TIMEOUT_SECONDS + "s for: " + hostname);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException re && re.getCause() instanceof UnknownHostException uhe) {
                throw uhe;
            }
            throw new UnknownHostException("DNS resolution failed for: " + hostname + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnknownHostException("DNS resolution interrupted for: " + hostname);
        }

        if (addresses == null || addresses.isEmpty()) {
            throw new UnknownHostException("No IP addresses returned for host: " + hostname);
        }

        // Invariant: Every lookup used for a connection is validated against the default-deny rules
        for (InetAddress address : addresses) {
            try {
                validator.validateAddress(address);
            } catch (SsrfException e) {
                log.warn("SSRF Blocked: Host '{}' resolved to prohibited address '{}': {}",
                    hostname, address.getHostAddress(), e.getMessage());
                throw new UnknownHostException("Destination host is not reachable or request rejected");
            }
        }

        return addresses;
    }
}
