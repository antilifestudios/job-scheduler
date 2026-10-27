package com.ashutosh.scheduler.producer.dto;

import java.util.Map;

public record HttpPayloadDto(
    String url,
    String method,
    Map<String, String> headers,
    Integer timeoutMs
) {}
