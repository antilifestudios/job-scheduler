package com.ashutosh.scheduler.worker.handler;

/**
 * Explicit outcome returned by a JobHandler execution.
 */
public sealed interface JobExecutionResult {

    record Success(String outputSnippet) implements JobExecutionResult {}

    record RetryableFailure(String error) implements JobExecutionResult {}

    record PermanentFailure(String error) implements JobExecutionResult {}

    static Success success() {
        return new Success(null);
    }

    static Success success(String outputSnippet) {
        return new Success(outputSnippet);
    }

    static RetryableFailure retryableFailure(String error) {
        return new RetryableFailure(error);
    }

    static PermanentFailure permanentFailure(String error) {
        return new PermanentFailure(error);
    }
}
