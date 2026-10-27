package com.ashutosh.scheduler.domain.entity;

import com.ashutosh.scheduler.domain.enums.MisfirePolicy;
import com.ashutosh.scheduler.domain.enums.OverlapPolicy;
import com.ashutosh.scheduler.domain.enums.ScheduleSkipReason;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "schedules")
public class ScheduleEntity {

    @Id
    private UUID id;

    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "cron_expression", nullable = false, length = 100)
    private String cronExpression;

    @Column(nullable = false, length = 50)
    private String timezone = "UTC";

    @Enumerated(EnumType.STRING)
    @Column(name = "overlap_policy", nullable = false, length = 20)
    private OverlapPolicy overlapPolicy = OverlapPolicy.SKIP;

    @Enumerated(EnumType.STRING)
    @Column(name = "misfire_policy", nullable = false, length = 20)
    private MisfirePolicy misfirePolicy = MisfirePolicy.FIRE_ONCE;

    @Column(name = "grace_window_seconds", nullable = false)
    private int graceWindowSeconds = 300;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "job_template", nullable = false, columnDefinition = "jsonb")
    private String jobTemplate;

    @Column(name = "next_run_at", nullable = false)
    private Instant nextRunAt;

    @Column(name = "last_fired_at")
    private Instant lastFiredAt;

    @Column(name = "last_tick_at")
    private Instant lastTickAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "last_skip_reason", length = 50)
    private ScheduleSkipReason lastSkipReason;

    @Column(name = "consecutive_failures", nullable = false)
    private int consecutiveFailures = 0;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    @Column(name = "disabled_reason")
    private String disabledReason;

    @Column(nullable = false)
    private boolean enabled = true;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    public ScheduleEntity() {}

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public UUID getUserId() { return userId; }
    public void setUserId(UUID userId) { this.userId = userId; }

    public String getCronExpression() { return cronExpression; }
    public void setCronExpression(String cronExpression) { this.cronExpression = cronExpression; }

    public String getTimezone() { return timezone; }
    public void setTimezone(String timezone) { this.timezone = timezone; }

    public OverlapPolicy getOverlapPolicy() { return overlapPolicy; }
    public void setOverlapPolicy(OverlapPolicy overlapPolicy) { this.overlapPolicy = overlapPolicy; }

    public MisfirePolicy getMisfirePolicy() { return misfirePolicy; }
    public void setMisfirePolicy(MisfirePolicy misfirePolicy) { this.misfirePolicy = misfirePolicy; }

    public int getGraceWindowSeconds() { return graceWindowSeconds; }
    public void setGraceWindowSeconds(int graceWindowSeconds) { this.graceWindowSeconds = graceWindowSeconds; }

    public String getJobTemplate() { return jobTemplate; }
    public void setJobTemplate(String jobTemplate) { this.jobTemplate = jobTemplate; }

    public Instant getNextRunAt() { return nextRunAt; }
    public void setNextRunAt(Instant nextRunAt) { this.nextRunAt = nextRunAt; }

    public Instant getLastFiredAt() { return lastFiredAt; }
    public void setLastFiredAt(Instant lastFiredAt) { this.lastFiredAt = lastFiredAt; }

    public Instant getLastTickAt() { return lastTickAt; }
    public void setLastTickAt(Instant lastTickAt) { this.lastTickAt = lastTickAt; }

    public ScheduleSkipReason getLastSkipReason() { return lastSkipReason; }
    public void setLastSkipReason(ScheduleSkipReason lastSkipReason) { this.lastSkipReason = lastSkipReason; }

    public int getConsecutiveFailures() { return consecutiveFailures; }
    public void setConsecutiveFailures(int consecutiveFailures) { this.consecutiveFailures = consecutiveFailures; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    public String getDisabledReason() { return disabledReason; }
    public void setDisabledReason(String disabledReason) { this.disabledReason = disabledReason; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
