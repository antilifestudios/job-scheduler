package com.ashutosh.scheduler.common.exception;

public class JobOverlapException extends RuntimeException {
    public JobOverlapException(String message) {
        super(message);
    }
}
