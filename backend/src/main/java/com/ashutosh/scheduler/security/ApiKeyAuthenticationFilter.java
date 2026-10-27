package com.ashutosh.scheduler.security;

import com.ashutosh.scheduler.common.util.CryptoUtils;
import com.ashutosh.scheduler.domain.entity.ApiKeyEntity;
import com.ashutosh.scheduler.domain.repo.ApiKeyRepository;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public class ApiKeyAuthenticationFilter extends OncePerRequestFilter {

    private final ApiKeyRepository apiKeyRepository;
    private final java.util.concurrent.ConcurrentHashMap<UUID, Long> lastUsedThrottleMap = new java.util.concurrent.ConcurrentHashMap<>();
    private static final long LAST_USED_THROTTLE_MS = 60_000L; // 1 minute throttle

    public ApiKeyAuthenticationFilter(ApiKeyRepository apiKeyRepository) {
        this.apiKeyRepository = apiKeyRepository;
    }

    public java.util.concurrent.ConcurrentHashMap<UUID, Long> getLastUsedThrottleMap() {
        return lastUsedThrottleMap;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String apiKey = extractApiKey(request);

        if (apiKey != null && !apiKey.isBlank()) {
            String keyHash = CryptoUtils.sha256Hex(apiKey.trim());
            Optional<ApiKeyEntity> activeKey = apiKeyRepository.findActiveByKeyHash(keyHash);

            if (activeKey.isPresent()) {
                ApiKeyEntity keyEntity = activeKey.get();
                var userEntity = keyEntity.getUser();
                SecurityUser user = new SecurityUser(
                    userEntity.getId(),
                    userEntity.getUsername(),
                    userEntity.getRateLimitPerMinute(),
                    userEntity.getMaxActiveJobs()
                );
                ApiKeyAuthenticationToken authentication = new ApiKeyAuthenticationToken(user);
                SecurityContextHolder.getContext().setAuthentication(authentication);

                // Throttle last_used_at updates via in-memory map (max 1 update per 60s per key)
                UUID keyId = keyEntity.getId();
                long now = System.currentTimeMillis();
                Long lastUpdated = lastUsedThrottleMap.get(keyId);
                if (lastUpdated == null || (now - lastUpdated) > LAST_USED_THROTTLE_MS) {
                    lastUsedThrottleMap.put(keyId, now);
                    java.util.concurrent.CompletableFuture.runAsync(() -> {
                        try {
                            apiKeyRepository.updateLastUsedAt(keyId, java.time.Instant.ofEpochMilli(now));
                        } catch (Exception ignored) {}
                    });
                }
            }
        }

        filterChain.doFilter(request, response);
    }

    private String extractApiKey(HttpServletRequest request) {
        String apiKeyHeader = request.getHeader("X-API-Key");
        if (apiKeyHeader != null && !apiKeyHeader.isBlank()) {
            return apiKeyHeader;
        }

        String authHeader = request.getHeader("Authorization");
        if (authHeader != null && authHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return authHeader.substring(7);
        }

        return null;
    }
}
