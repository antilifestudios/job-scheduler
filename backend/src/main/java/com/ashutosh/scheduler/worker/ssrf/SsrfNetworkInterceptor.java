package com.ashutosh.scheduler.worker.ssrf;

import okhttp3.Connection;
import okhttp3.Interceptor;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * OkHttp Network Interceptor that intercepts at the TCP connection boundary.
 *
 * Verifies the actual connected socket address (chain.connection().socket().getInetAddress())
 * against the authoritative default-deny rules before any HTTP request bytes are written.
 * This guarantees that IP literals (which bypass OkHttp Dns.lookup) and any unexpected connection
 * path are unconditionally validated on the wire.
 */
@Component
public class SsrfNetworkInterceptor implements Interceptor {

    private static final Logger log = LoggerFactory.getLogger(SsrfNetworkInterceptor.class);

    private final SsrfValidator validator;

    public SsrfNetworkInterceptor(SsrfValidator validator) {
        this.validator = validator;
    }

    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {
        Connection connection = chain.connection();
        if (connection != null) {
            Socket socket = connection.socket();
            if (socket != null) {
                InetAddress connectedAddress = socket.getInetAddress();
                if (connectedAddress != null) {
                    try {
                        validator.validateAddress(connectedAddress);
                    } catch (SsrfException e) {
                        log.warn("SSRF Network Interceptor Blocked: Connected socket IP '{}' violated SSRF rules: {}",
                            connectedAddress.getHostAddress(), e.getMessage());
                        // Close socket immediately to abort transmission
                        try {
                            socket.close();
                        } catch (Exception ignored) {}
                        throw new SsrfException("Destination host is not reachable or request rejected");
                    }
                }
            }
        }

        return chain.proceed(chain.request());
    }
}
