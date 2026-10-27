package com.ashutosh.scheduler.common.exception;

public class MaxSchedulesExceededException extends RuntimeException {

    private final int maxSchedules;
    private final long currentSchedules;

    public MaxSchedulesExceededException(String message, int maxSchedules, long currentSchedules) {
        super(message);
        this.maxSchedules = maxSchedules;
        this.currentSchedules = currentSchedules;
    }

    public MaxSchedulesExceededException(String message) {
        this(message, 20, 20);
    }

    public int getMaxSchedules() {
        return maxSchedules;
    }

    public long getCurrentSchedules() {
        return currentSchedules;
    }
}
