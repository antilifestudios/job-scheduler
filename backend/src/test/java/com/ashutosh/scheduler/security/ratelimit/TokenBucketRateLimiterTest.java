package com.ashutosh.scheduler.security.ratelimit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class TokenBucketRateLimiterTest {

    @Test
    @DisplayName("Token bucket consumes tokens up to capacity and rejects when exhausted")
    void testTokenBucketConsumptionAndRejection() {
        AtomicLong testNanos = new AtomicLong(1_000_000_000L);
        Clock testClock = Clock.fixed(Instant.ofEpochSecond(1700000000), ZoneId.of("UTC"));

        TokenBucketRateLimiter rateLimiter = new TokenBucketRateLimiter(testNanos::get, testClock);
        UUID tenantId = UUID.randomUUID();

        // Tenant with 5 requests per minute
        int limit = 5;

        // First 5 requests must be allowed
        for (int i = 1; i <= 5; i++) {
            ConsumptionResult result = rateLimiter.tryConsume(tenantId, limit);
            assertThat(result.allowed()).isTrue();
            assertThat(result.limit()).isEqualTo(5);
            assertThat(result.remaining()).isEqualTo(5 - i);
            assertThat(result.retryAfterSeconds()).isEqualTo(0);
        }

        // 6th request must be rejected with rate_limited parameters
        ConsumptionResult rejected = rateLimiter.tryConsume(tenantId, limit);
        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.limit()).isEqualTo(5);
        assertThat(rejected.remaining()).isEqualTo(0);
        assertThat(rejected.retryAfterSeconds()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Token bucket refills tokens as time advances")
    void testTokenBucketRefill() {
        AtomicLong testNanos = new AtomicLong(1_000_000_000L);
        Clock testClock = Clock.fixed(Instant.ofEpochSecond(1700000000), ZoneId.of("UTC"));

        TokenBucketRateLimiter rateLimiter = new TokenBucketRateLimiter(testNanos::get, testClock);
        UUID tenantId = UUID.randomUUID();
        int limit = 60; // 1 token per second

        // Consume 60 tokens
        for (int i = 0; i < 60; i++) {
            rateLimiter.tryConsume(tenantId, limit);
        }

        // Exhausted
        assertThat(rateLimiter.tryConsume(tenantId, limit).allowed()).isFalse();

        // Advance time by 2 seconds (2 * 1_000_000_000 nanos)
        testNanos.addAndGet(2_000_000_000L);

        // Now we should have 2 tokens refilled
        ConsumptionResult r1 = rateLimiter.tryConsume(tenantId, limit);
        assertThat(r1.allowed()).isTrue();

        ConsumptionResult r2 = rateLimiter.tryConsume(tenantId, limit);
        assertThat(r2.allowed()).isTrue();

        // 3rd attempt is rejected again
        assertThat(rateLimiter.tryConsume(tenantId, limit).allowed()).isFalse();
    }

    @Test
    @DisplayName("Idle bucket eviction purges inactive tenant buckets from memory")
    void testIdleBucketEviction() {
        AtomicLong testNanos = new AtomicLong(1_000_000_000L);
        Clock testClock = Clock.fixed(Instant.ofEpochSecond(1700000000), ZoneId.of("UTC"));

        TokenBucketRateLimiter rateLimiter = new TokenBucketRateLimiter(testNanos::get, testClock);
        UUID activeTenant = UUID.randomUUID();
        UUID idleTenant = UUID.randomUUID();

        // Populate both buckets at t = 1s
        rateLimiter.tryConsume(activeTenant, 60);
        rateLimiter.tryConsume(idleTenant, 60);
        assertThat(rateLimiter.getActiveBucketCount()).isEqualTo(2);

        // Advance time by 15 minutes (900 seconds)
        testNanos.addAndGet(900_000_000_000L);

        // Access only the active tenant at t = 15m
        rateLimiter.tryConsume(activeTenant, 60);

        // Evict buckets idle for > 10 minutes (600 seconds)
        long tenMinutesNanos = 600_000_000_000L;
        int evicted = rateLimiter.evictIdleBuckets(tenMinutesNanos);

        assertThat(evicted).isEqualTo(1);
        assertThat(rateLimiter.getActiveBucketCount()).isEqualTo(1);
    }
}
