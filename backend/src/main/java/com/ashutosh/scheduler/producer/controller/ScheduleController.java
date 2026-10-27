package com.ashutosh.scheduler.producer.controller;

import com.ashutosh.scheduler.producer.dto.CreateScheduleRequest;
import com.ashutosh.scheduler.producer.dto.JobResponse;
import com.ashutosh.scheduler.producer.dto.PageResponse;
import com.ashutosh.scheduler.producer.dto.ScheduleResponse;
import com.ashutosh.scheduler.producer.dto.UpdateScheduleRequest;
import com.ashutosh.scheduler.producer.service.ScheduleService;
import com.ashutosh.scheduler.security.SecurityUser;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/schedules")
public class ScheduleController {

    private final ScheduleService scheduleService;

    public ScheduleController(ScheduleService scheduleService) {
        this.scheduleService = scheduleService;
    }

    @PostMapping
    public ResponseEntity<ScheduleResponse> createSchedule(
            @AuthenticationPrincipal SecurityUser currentUser,
            @RequestBody CreateScheduleRequest request) {
        ScheduleResponse response = scheduleService.createSchedule(currentUser.userId(), request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping
    public ResponseEntity<PageResponse<ScheduleResponse>> listSchedules(
            @AuthenticationPrincipal SecurityUser currentUser,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        int boundedSize = Math.min(Math.max(size, 1), 100);
        Pageable pageable = PageRequest.of(Math.max(page, 0), boundedSize, Sort.by("createdAt").descending());
        return ResponseEntity.ok(scheduleService.listSchedules(currentUser.userId(), pageable));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ScheduleResponse> getSchedule(
            @AuthenticationPrincipal SecurityUser currentUser,
            @PathVariable UUID id) {
        return ResponseEntity.ok(scheduleService.getSchedule(currentUser.userId(), id));
    }

    @GetMapping("/{id}/jobs")
    public ResponseEntity<PageResponse<JobResponse>> listScheduleJobs(
            @AuthenticationPrincipal SecurityUser currentUser,
            @PathVariable UUID id,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        int boundedSize = Math.min(Math.max(size, 1), 100);
        Pageable pageable = PageRequest.of(Math.max(page, 0), boundedSize, Sort.by("createdAt").descending());
        return ResponseEntity.ok(scheduleService.listScheduleJobs(currentUser.userId(), id, pageable));
    }

    @PatchMapping("/{id}")
    public ResponseEntity<ScheduleResponse> patchSchedule(
            @AuthenticationPrincipal SecurityUser currentUser,
            @PathVariable UUID id,
            @RequestBody UpdateScheduleRequest request) {
        return ResponseEntity.ok(scheduleService.patchSchedule(currentUser.userId(), id, request));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteSchedule(
            @AuthenticationPrincipal SecurityUser currentUser,
            @PathVariable UUID id) {
        scheduleService.deleteSchedule(currentUser.userId(), id);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{id}/trigger")
    public ResponseEntity<JobResponse> triggerSchedule(
            @AuthenticationPrincipal SecurityUser currentUser,
            @PathVariable UUID id,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        JobResponse response = scheduleService.triggerSchedule(currentUser.userId(), id, idempotencyKey);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}
