package com.ashutosh.scheduler.domain.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "job_attempts")
public class JobAttemptEntity {

    @Id
    private UUID id;

    @Column(name = "job_id", nullable = false)
    private UUID jobId;

    @Column(name = "replay_count", nullable = false)
    private int replayCount = 0;

    @Column(name = "attempt_no", nullable = false)
    private int attemptNo;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "duration_ms", nullable = false)
    private long durationMs;

    @Column(name = "status_code")
    private Integer statusCode;

    @Column(name = "response_snippet", columnDefinition = "text")
    private String responseSnippet;

    @Column(columnDefinition = "text")
    private String error;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt = Instant.now();

    public JobAttemptEntity() {}

    public JobAttemptEntity(UUID id, UUID jobId, int attemptNo, Instant startedAt,
                            long durationMs, Integer statusCode, String responseSnippet, String error) {
        this(id, jobId, 0, attemptNo, startedAt, durationMs, statusCode, responseSnippet, error);
    }

    public JobAttemptEntity(UUID id, UUID jobId, int replayCount, int attemptNo, Instant startedAt,
                            long durationMs, Integer statusCode, String responseSnippet, String error) {
        this.id = id;
        this.jobId = jobId;
        this.replayCount = replayCount;
        this.attemptNo = attemptNo;
        this.startedAt = startedAt;
        this.durationMs = durationMs;
        this.statusCode = statusCode;
        this.responseSnippet = responseSnippet;
        this.error = error;
        this.createdAt = Instant.now();
    }

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public UUID getJobId() { return jobId; }
    public void setJobId(UUID jobId) { this.jobId = jobId; }

    public int getReplayCount() { return replayCount; }
    public void setReplayCount(int replayCount) { this.replayCount = replayCount; }

    public int getAttemptNo() { return attemptNo; }
    public void setAttemptNo(int attemptNo) { this.attemptNo = attemptNo; }

    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }

    public long getDurationMs() { return durationMs; }
    public void setDurationMs(long durationMs) { this.durationMs = durationMs; }

    public Integer getStatusCode() { return statusCode; }
    public void setStatusCode(Integer statusCode) { this.statusCode = statusCode; }

    public String getResponseSnippet() { return responseSnippet; }
    public void setResponseSnippet(String responseSnippet) { this.responseSnippet = responseSnippet; }

    public String getError() { return error; }
    public void setError(String error) { this.error = error; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
