package com.ashutosh.scheduler.producer.dto;

import com.ashutosh.scheduler.domain.enums.MisfirePolicy;
import com.ashutosh.scheduler.domain.enums.OverlapPolicy;

public record CreateScheduleRequest(
    String cronExpression,
    String timezone,
    OverlapPolicy overlapPolicy,
    MisfirePolicy misfirePolicy,
    Integer graceWindowSeconds,
    JobTemplateDto jobTemplate
) {}
