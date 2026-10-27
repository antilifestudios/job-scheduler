package com.ashutosh.scheduler.producer.dto;

import java.util.UUID;

public record TenantUsageResponse(
    UUID userId,
    String username,
    int rateLimitPerMinute,
    int maxActiveJobs,
    long activeJobs,
    long pendingJobs,
    long runningJobs
) {}
