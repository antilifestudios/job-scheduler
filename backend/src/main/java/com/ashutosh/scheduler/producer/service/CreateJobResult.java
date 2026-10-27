package com.ashutosh.scheduler.producer.service;

import com.ashutosh.scheduler.producer.dto.JobResponse;

public record CreateJobResult(JobResponse response, boolean isReplayed) {}
