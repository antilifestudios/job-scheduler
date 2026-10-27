package com.ashutosh.scheduler.security.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * In-memory Token Bucket Rate Limiter.
 *
 * NOTE: Rate limits are strictly per-instance (in-memory) in this architecture.
 * Evicts idle buckets after a configurable idle threshold to prevent memory leaks.
 */
@Component
public class TokenBucketRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(TokenBucketRateLimiter.class);

    // 10 minutes in nanoseconds
    public static final long DEFAULT_IDLE_THRESHOLD_NANOS = 10L * 60L * 1_000_000_000L;

    private final ConcurrentHashMap<UUID, TokenBucket> buckets = new ConcurrentHashMap<>();
    private final LongSupplier nanoTimeSupplier;
    private final Clock clock;

    public TokenBucketRateLimiter() {
        this(System::nanoTime, Clock.systemUTC());
    }

    public TokenBucketRateLimiter(LongSupplier nanoTimeSupplier, Clock clock) {
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.clock = clock;
    }

    /**
     * Attempts to consume 1 token for the specified user.
     *
     * @param userId the tenant ID
     * @param rateLimitPerMinute the tenant's configured limit in RPM
     * @return the result containing allowance status and rate limit headers
     */
    public ConsumptionResult tryConsume(UUID userId, int rateLimitPerMinute) {
        long nowNanos = nanoTimeSupplier.getAsLong();
        TokenBucket bucket = buckets.computeIfAbsent(userId, id -> new TokenBucket(rateLimitPerMinute, nowNanos));
        return bucket.tryConsume(1.0, rateLimitPerMinute, nowNanos, clock);
    }

    /**
     * Evicts buckets that have had no activity for longer than maxIdleNanos.
     *
     * @param maxIdleNanos idle threshold in nanoseconds
     * @return number of evicted buckets
     */
    public int evictIdleBuckets(long maxIdleNanos) {
        long nowNanos = nanoTimeSupplier.getAsLong();
        int initialSize = buckets.size();
        buckets.entrySet().removeIf(entry -> entry.getValue().isIdle(nowNanos, maxIdleNanos));
        int evicted = initialSize - buckets.size();
        if (evicted > 0) {
            log.debug("Evicted {} idle rate limit buckets. Remaining active: {}", evicted, buckets.size());
        }
        return evicted;
    }

    @Scheduled(fixedDelay = 300_000) // Every 5 minutes
    public void scheduledEviction() {
        evictIdleBuckets(DEFAULT_IDLE_THRESHOLD_NANOS);
    }

    public int getActiveBucketCount() {
        return buckets.size();
    }

    public void clear() {
        buckets.clear();
    }
}
