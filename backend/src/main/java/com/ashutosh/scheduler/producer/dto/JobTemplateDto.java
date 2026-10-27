package com.ashutosh.scheduler.producer.dto;

import java.util.Map;

public record JobTemplateDto(
    String url,
    String method,
    Map<String, String> headers,
    String body,
    String queue,
    Integer priority,
    Integer maxAttempts
) {}
