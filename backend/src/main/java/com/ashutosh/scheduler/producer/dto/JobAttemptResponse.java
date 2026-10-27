package com.ashutosh.scheduler.producer.dto;

import com.ashutosh.scheduler.domain.entity.JobAttemptEntity;

import java.time.Instant;
import java.util.UUID;

public record JobAttemptResponse(
    UUID id,
    UUID jobId,
    int replayCount,
    int attemptNo,
    Instant startedAt,
    long durationMs,
    Integer statusCode,
    String responseSnippet,
    String error,
    Instant createdAt
) {
    public static JobAttemptResponse fromEntity(JobAttemptEntity entity) {
        return new JobAttemptResponse(
            entity.getId(),
            entity.getJobId(),
            entity.getReplayCount(),
            entity.getAttemptNo(),
            entity.getStartedAt(),
            entity.getDurationMs(),
            entity.getStatusCode(),
            entity.getResponseSnippet(),
            entity.getError(),
            entity.getCreatedAt()
        );
    }
}
