package com.ashutosh.scheduler.common.exception;

public class QuotaExceededException extends RuntimeException {

    private final int maxActiveJobs;
    private final long activeJobs;

    public QuotaExceededException(String message, int maxActiveJobs, long activeJobs) {
        super(message);
        this.maxActiveJobs = maxActiveJobs;
        this.activeJobs = activeJobs;
    }

    public int getMaxActiveJobs() {
        return maxActiveJobs;
    }

    public long getActiveJobs() {
        return activeJobs;
    }
}
