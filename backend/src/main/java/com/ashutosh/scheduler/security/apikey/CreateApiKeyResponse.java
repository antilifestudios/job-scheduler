package com.ashutosh.scheduler.security.apikey;

import java.time.Instant;
import java.util.UUID;

public record CreateApiKeyResponse(
    UUID id,
    String name,
    String keyPrefix,
    String rawKey,
    Instant createdAt
) {}
