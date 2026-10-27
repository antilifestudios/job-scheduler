package com.ashutosh.scheduler.security.ratelimit;

public record ConsumptionResult(
    boolean allowed,
    int limit,
    long remaining,
    long retryAfterSeconds,
    long resetEpochSeconds
) {
    public static ConsumptionResult allowed(int limit, long remaining, long resetEpochSeconds) {
        return new ConsumptionResult(true, limit, remaining, 0, resetEpochSeconds);
    }

    public static ConsumptionResult rejected(int limit, long remaining, long retryAfterSeconds, long resetEpochSeconds) {
        return new ConsumptionResult(false, limit, remaining, retryAfterSeconds, resetEpochSeconds);
    }
}
