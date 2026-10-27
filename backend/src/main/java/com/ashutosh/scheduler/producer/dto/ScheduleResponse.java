package com.ashutosh.scheduler.producer.dto;

import com.ashutosh.scheduler.domain.entity.ScheduleEntity;
import com.ashutosh.scheduler.domain.enums.MisfirePolicy;
import com.ashutosh.scheduler.domain.enums.OverlapPolicy;
import com.ashutosh.scheduler.domain.enums.ScheduleSkipReason;

import java.time.Instant;
import java.util.UUID;

public record ScheduleResponse(
    UUID id,
    UUID userId,
    String cronExpression,
    String timezone,
    OverlapPolicy overlapPolicy,
    MisfirePolicy misfirePolicy,
    int graceWindowSeconds,
    String jobTemplate,
    Instant nextRunAt,
    Instant lastFiredAt,
    Instant lastTickAt,
    ScheduleSkipReason lastSkipReason,
    int consecutiveFailures,
    String lastError,
    boolean enabled,
    Instant createdAt,
    Instant updatedAt
) {
    public static ScheduleResponse fromEntity(ScheduleEntity entity) {
        return new ScheduleResponse(
            entity.getId(),
            entity.getUserId(),
            entity.getCronExpression(),
            entity.getTimezone(),
            entity.getOverlapPolicy(),
            entity.getMisfirePolicy(),
            entity.getGraceWindowSeconds(),
            entity.getJobTemplate(),
            entity.getNextRunAt(),
            entity.getLastFiredAt(),
            entity.getLastTickAt(),
            entity.getLastSkipReason(),
            entity.getConsecutiveFailures(),
            entity.getLastError(),
            entity.isEnabled(),
            entity.getCreatedAt(),
            entity.getUpdatedAt()
        );
    }
}
