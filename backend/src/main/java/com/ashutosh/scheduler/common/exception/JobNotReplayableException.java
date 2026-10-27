package com.ashutosh.scheduler.common.exception;

public class JobNotReplayableException extends RuntimeException {

    public JobNotReplayableException(String message) {
        super(message);
    }
}
