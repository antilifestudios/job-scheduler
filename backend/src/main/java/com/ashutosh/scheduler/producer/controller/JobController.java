package com.ashutosh.scheduler.producer.controller;

import com.ashutosh.scheduler.domain.enums.JobStatus;
import com.ashutosh.scheduler.producer.dto.CreateJobRequest;
import com.ashutosh.scheduler.producer.dto.JobResponse;
import com.ashutosh.scheduler.producer.dto.PageResponse;
import com.ashutosh.scheduler.producer.service.CreateJobResult;
import com.ashutosh.scheduler.producer.service.JobProducerService;
import com.ashutosh.scheduler.security.SecurityUser;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobProducerService jobProducerService;

    public JobController(JobProducerService jobProducerService) {
        this.jobProducerService = jobProducerService;
    }

    @PostMapping
    public ResponseEntity<JobResponse> createJob(
            @AuthenticationPrincipal SecurityUser currentUser,
            @RequestBody CreateJobRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String headerIdempotencyKey) {

        CreateJobResult result = jobProducerService.createJob(currentUser.userId(), request, headerIdempotencyKey);

        if (result.isReplayed()) {
            return ResponseEntity.ok()
                .header("Idempotent-Replayed", "true")
                .body(result.response());
        }

        return ResponseEntity.status(HttpStatus.CREATED)
            .header(HttpHeaders.LOCATION, "/api/jobs/" + result.response().id())
            .body(result.response());
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobResponse> getJob(
            @AuthenticationPrincipal SecurityUser currentUser,
            @PathVariable UUID id) {
        JobResponse response = jobProducerService.getJobById(id, currentUser.userId());
        return ResponseEntity.ok(response);
    }

    @GetMapping
    public ResponseEntity<PageResponse<JobResponse>> listJobs(
            @AuthenticationPrincipal SecurityUser currentUser,
            @RequestParam(required = false) JobStatus status,
            @RequestParam(required = false) String queue,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        PageResponse<JobResponse> response = jobProducerService.listJobs(currentUser.userId(), status, queue, page, size);
        return ResponseEntity.ok(response);
    }
}
