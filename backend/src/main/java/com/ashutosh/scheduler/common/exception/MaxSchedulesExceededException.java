package com.ashutosh.scheduler.common.exception;

public class MaxSchedulesExceededException extends RuntimeException {
    public MaxSchedulesExceededException(String message) {
        super(message);
    }
}
