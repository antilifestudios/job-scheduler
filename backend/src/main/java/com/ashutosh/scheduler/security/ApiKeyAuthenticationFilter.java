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

public class ApiKeyAuthenticationFilter extends OncePerRequestFilter {

    private final ApiKeyRepository apiKeyRepository;

    public ApiKeyAuthenticationFilter(ApiKeyRepository apiKeyRepository) {
        this.apiKeyRepository = apiKeyRepository;
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
                SecurityUser user = new SecurityUser(keyEntity.getUser().getId(), keyEntity.getUser().getUsername());
                ApiKeyAuthenticationToken authentication = new ApiKeyAuthenticationToken(user);
                SecurityContextHolder.getContext().setAuthentication(authentication);
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
