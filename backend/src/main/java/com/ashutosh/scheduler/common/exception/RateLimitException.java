package com.ashutosh.scheduler.common.exception;

public class RateLimitException extends RuntimeException {

    private final long retryAfterSeconds;
    private final int limit;
    private final long remaining;
    private final long resetEpochSeconds;

    public RateLimitException(String message, long retryAfterSeconds, int limit, long remaining, long resetEpochSeconds) {
        super(message);
        this.retryAfterSeconds = retryAfterSeconds;
        this.limit = limit;
        this.remaining = remaining;
        this.resetEpochSeconds = resetEpochSeconds;
    }

    public long getRetryAfterSeconds() {
        return retryAfterSeconds;
    }

    public int getLimit() {
        return limit;
    }

    public long getRemaining() {
        return remaining;
    }

    public long getResetEpochSeconds() {
        return resetEpochSeconds;
    }
}
