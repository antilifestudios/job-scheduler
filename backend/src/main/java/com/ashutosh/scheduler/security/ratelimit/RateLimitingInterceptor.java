package com.ashutosh.scheduler.security.ratelimit;

import com.ashutosh.scheduler.security.SecurityUser;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;

/**
 * Spring MVC HandlerInterceptor enforcing per-tenant rate limits.
 *
 * Distinct machine-readable error codes:
 * - rate_limited on 429 with Retry-After and X-RateLimit-* headers
 * - (quota_exceeded is handled at the service layer and emits NO rate limit headers)
 */
@Component
public class RateLimitingInterceptor implements HandlerInterceptor {

    public static final String RATE_LIMIT_RESULT_ATTR = "RATE_LIMIT_RESULT";

    private final TokenBucketRateLimiter rateLimiter;
    private final ObjectMapper objectMapper;

    public RateLimitingInterceptor(TokenBucketRateLimiter rateLimiter, ObjectMapper objectMapper) {
        this.rateLimiter = rateLimiter;
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof SecurityUser user) {
            ConsumptionResult result = rateLimiter.tryConsume(user.userId(), user.rateLimitPerMinute());

            if (!result.allowed()) {
                response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
                response.setContentType("application/json");
                response.setHeader("Retry-After", String.valueOf(result.retryAfterSeconds()));
                response.setHeader("X-RateLimit-Limit", String.valueOf(result.limit()));
                response.setHeader("X-RateLimit-Remaining", "0");
                response.setHeader("X-RateLimit-Reset", String.valueOf(result.resetEpochSeconds()));

                Map<String, Object> errorBody = Map.of(
                    "timestamp", Instant.now().toString(),
                    "status", HttpStatus.TOO_MANY_REQUESTS.value(),
                    "code", "rate_limited",
                    "message", "Too many requests. Rate limit of " + result.limit() + " requests per minute exceeded.",
                    "retryAfterSeconds", result.retryAfterSeconds()
                );
                objectMapper.writeValue(response.getOutputStream(), errorBody);
                return false;
            }

            request.setAttribute(RATE_LIMIT_RESULT_ATTR, result);
        }

        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) {
        ConsumptionResult result = (ConsumptionResult) request.getAttribute(RATE_LIMIT_RESULT_ATTR);
        if (result != null && response.getStatus() != HttpStatus.TOO_MANY_REQUESTS.value()) {
            response.setHeader("X-RateLimit-Limit", String.valueOf(result.limit()));
            response.setHeader("X-RateLimit-Remaining", String.valueOf(result.remaining()));
            response.setHeader("X-RateLimit-Reset", String.valueOf(result.resetEpochSeconds()));
        }
    }
}
