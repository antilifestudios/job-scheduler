package com.ashutosh.scheduler.common.exception;

public class MaxApiKeysExceededException extends RuntimeException {
    public MaxApiKeysExceededException(String message) {
        super(message);
    }
}
