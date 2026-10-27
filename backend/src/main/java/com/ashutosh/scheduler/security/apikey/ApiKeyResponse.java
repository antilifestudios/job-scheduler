package com.ashutosh.scheduler.security.apikey;

import com.ashutosh.scheduler.domain.entity.ApiKeyEntity;

import java.time.Instant;
import java.util.UUID;

public record ApiKeyResponse(
    UUID id,
    String name,
    String keyPrefix,
    Instant createdAt,
    Instant revokedAt,
    Instant lastUsedAt
) {
    public static ApiKeyResponse fromEntity(ApiKeyEntity entity) {
        return new ApiKeyResponse(
            entity.getId(),
            entity.getName(),
            entity.getKeyPrefix(),
            entity.getCreatedAt(),
            entity.getRevokedAt(),
            entity.getLastUsedAt()
        );
    }
}
