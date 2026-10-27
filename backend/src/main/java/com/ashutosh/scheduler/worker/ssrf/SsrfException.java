package com.ashutosh.scheduler.worker.ssrf;

import java.io.IOException;

/**
 * Thrown when a target URL or resolved IP violates the SSRF security policy.
 */
public class SsrfException extends IOException {

    public SsrfException(String message) {
        super(message);
    }

    public SsrfException(String message, Throwable cause) {
        super(message, cause);
    }
}
