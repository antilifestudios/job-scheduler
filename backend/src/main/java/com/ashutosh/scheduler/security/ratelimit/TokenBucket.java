package com.ashutosh.scheduler.security.ratelimit;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe token bucket using an AtomicReference of immutable state.
 *
 * All rate limits are strictly per-instance (in-memory).
 */
public class TokenBucket {

    public record BucketState(double tokens, long lastRefillNanos, long lastAccessNanos) {}

    private final AtomicReference<BucketState> stateRef;

    public TokenBucket(int initialCapacity, long initialNanos) {
        this.stateRef = new AtomicReference<>(new BucketState(initialCapacity, initialNanos, initialNanos));
    }

    public ConsumptionResult tryConsume(double tokensToConsume, int capacity, long nowNanos, Clock clock) {
        BucketState current;
        BucketState updated;
        boolean allowed;
        long remaining;
        long retryAfterSeconds;
        long resetEpochSeconds;

        double refillRatePerNano = (double) capacity / 60_000_000_000.0;

        do {
            current = stateRef.get();
            long elapsedNanos = Math.max(0, nowNanos - current.lastRefillNanos());
            double refilledTokens = Math.min((double) capacity, current.tokens() + (elapsedNanos * refillRatePerNano));

            // Approximate reset time: seconds until bucket would be completely refilled to capacity
            double missingFromCapacity = (double) capacity - refilledTokens;
            long nanosToFull = (long) Math.ceil(Math.max(0, missingFromCapacity) / refillRatePerNano);
            resetEpochSeconds = clock.instant().getEpochSecond() + Math.max(1, (nanosToFull + 999_999_999L) / 1_000_000_000L);

            if (refilledTokens >= tokensToConsume) {
                double newTokens = refilledTokens - tokensToConsume;
                updated = new BucketState(newTokens, nowNanos, nowNanos);
                allowed = true;
                remaining = (long) Math.floor(newTokens);
                retryAfterSeconds = 0;
            } else {
                // Not enough tokens
                updated = new BucketState(refilledTokens, nowNanos, nowNanos);
                allowed = false;
                remaining = (long) Math.floor(refilledTokens);
                double missing = tokensToConsume - refilledTokens;
                long nanosToWait = (long) Math.ceil(missing / refillRatePerNano);
                retryAfterSeconds = Math.max(1, (nanosToWait + 999_999_999L) / 1_000_000_000L);
            }
        } while (!stateRef.compareAndSet(current, updated));

        if (allowed) {
            return ConsumptionResult.allowed(capacity, remaining, resetEpochSeconds);
        } else {
            return ConsumptionResult.rejected(capacity, remaining, retryAfterSeconds, resetEpochSeconds);
        }
    }

    public boolean isIdle(long nowNanos, long maxIdleNanos) {
        return (nowNanos - stateRef.get().lastAccessNanos()) > maxIdleNanos;
    }

    public BucketState getState() {
        return stateRef.get();
    }
}
