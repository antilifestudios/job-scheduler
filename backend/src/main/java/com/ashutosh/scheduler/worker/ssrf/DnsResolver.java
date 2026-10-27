package com.ashutosh.scheduler.worker.ssrf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Functional interface for hostname resolution.
 * Allows injecting custom resolvers for testing DNS rebinding and IP pinning.
 */
@FunctionalInterface
public interface DnsResolver {
    List<InetAddress> resolve(String hostname) throws UnknownHostException;
}
