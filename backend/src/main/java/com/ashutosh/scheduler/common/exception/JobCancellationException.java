package com.ashutosh.scheduler.common.exception;

public class JobCancellationException extends RuntimeException {
    public JobCancellationException(String message) {
        super(message);
    }
}
