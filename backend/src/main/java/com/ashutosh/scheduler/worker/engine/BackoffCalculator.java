package com.ashutosh.scheduler.worker.engine;

import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Calculates retry delays using Exponential Backoff with Full Jitter.
 *
 * Formula: delay = random(1, min(maxBackoff, baseBackoff * 2^attempt))
 *
 * WHY Full Jitter:
 * Pure exponential backoff synchronizes retrying workers, causing the "thundering herd"
 * problem where hundreds of retrying requests hit recovering downstream APIs simultaneously.
 * Full jitter spreads retry attempts uniformly across the interval, maximizing downstream
 * recovery probability.
 */
@Component
public class BackoffCalculator {

    private final WorkerProperties properties;

    public BackoffCalculator(WorkerProperties properties) {
        this.properties = properties;
    }

    /**
     * Computes backoff delay in seconds for the specified attempt count.
     *
     * @param attempt the attempt number that just failed (e.g. 1 for first failure)
     * @return delay in seconds (>= 1 second, strictly in the future)
     */
    public long calculateDelaySeconds(int attempt) {
        return calculateDelaySeconds(attempt, properties.getBaseBackoffSeconds(), properties.getMaxBackoffSeconds());
    }

    public static long calculateDelaySeconds(int attempt, long baseBackoffSeconds, long maxBackoffSeconds) {
        if (attempt <= 0) {
            attempt = 1;
        }

        // Cap attempt exponent at 30 to prevent 64-bit integer bit-shift overflow
        int shift = Math.min(attempt, 30);
        long multiplier = 1L << shift;

        long ceiling;
        if (multiplier > 0 && baseBackoffSeconds > Long.MAX_VALUE / multiplier) {
            ceiling = maxBackoffSeconds;
        } else {
            ceiling = Math.min(maxBackoffSeconds, baseBackoffSeconds * multiplier);
        }

        if (ceiling <= 1) {
            return 1;
        }

        // Full jitter: uniformly distributed in [1, ceiling]
        return ThreadLocalRandom.current().nextLong(1, ceiling + 1);
    }
}
