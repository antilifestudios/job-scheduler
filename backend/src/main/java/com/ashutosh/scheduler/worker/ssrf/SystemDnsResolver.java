package com.ashutosh.scheduler.worker.ssrf;

import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

/**
 * Default production DNS resolver delegating directly to the operating system resolver.
 */
@Component
public class SystemDnsResolver implements DnsResolver {

    @Override
    public List<InetAddress> resolve(String hostname) throws UnknownHostException {
        if (hostname == null || hostname.isBlank()) {
            throw new UnknownHostException("Hostname cannot be null or blank");
        }
        return Arrays.asList(InetAddress.getAllByName(hostname));
    }
}
