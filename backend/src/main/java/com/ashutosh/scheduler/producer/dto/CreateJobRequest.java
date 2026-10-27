package com.ashutosh.scheduler.producer.dto;

import java.time.Instant;
import java.util.Map;

public record CreateJobRequest(
    String url,
    String method,
    Map<String, String> headers,
    String body,
    String queue,
    Integer priority,
    Instant runAt,
    Integer maxAttempts,
    String idempotencyKey
) {}
