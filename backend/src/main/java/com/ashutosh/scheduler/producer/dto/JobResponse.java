package com.ashutosh.scheduler.producer.dto;

import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record JobResponse(
    UUID id,
    UUID scheduleId,
    String queue,
    String type,
    JobStatus status,
    int priority,
    Instant runAt,
    int attempts,
    int maxAttempts,
    String lastError,
    String idempotencyKey,
    HttpPayloadDto payload,
    String body,
    Instant createdAt,
    Instant updatedAt
) {
    public static JobResponse fromEntity(JobEntity entity, ObjectMapper objectMapper) {
        HttpPayloadDto payloadDto = null;
        if (entity.getPayload() != null) {
            try {
                payloadDto = objectMapper.readValue(entity.getPayload(), HttpPayloadDto.class);
            } catch (Exception ignored) {
                // Fallback to basic representation if parsing fails
                payloadDto = new HttpPayloadDto("", "", Map.of(), null);
            }
        }

        return new JobResponse(
            entity.getId(),
            entity.getScheduleId(),
            entity.getQueue(),
            entity.getType(),
            entity.getStatus(),
            entity.getPriority(),
            entity.getRunAt(),
            entity.getAttempts(),
            entity.getMaxAttempts(),
            entity.getLastError(),
            entity.getIdempotencyKey(),
            payloadDto,
            entity.getBody(),
            entity.getCreatedAt(),
            entity.getUpdatedAt()
        );
    }
}
