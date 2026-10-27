package com.ashutosh.scheduler.producer.controller;

import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import com.ashutosh.scheduler.producer.dto.TenantUsageResponse;
import com.ashutosh.scheduler.security.SecurityUser;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TenantController {

    private final UserRepository userRepository;
    private final JobRepository jobRepository;

    public TenantController(UserRepository userRepository, JobRepository jobRepository) {
        this.userRepository = userRepository;
        this.jobRepository = jobRepository;
    }

    @GetMapping({"/api/tenants/me/usage", "/api/usage"})
    public ResponseEntity<TenantUsageResponse> getUsage(@AuthenticationPrincipal SecurityUser currentUser) {
        UserEntity user = userRepository.findById(currentUser.userId())
            .orElseThrow(() -> new IllegalStateException("User not found: " + currentUser.userId()));

        long pendingJobs = jobRepository.countByUserIdAndStatus(user.getId(), JobStatus.PENDING);
        long runningJobs = jobRepository.countByUserIdAndStatus(user.getId(), JobStatus.RUNNING);
        long activeJobs = pendingJobs + runningJobs;

        TenantUsageResponse response = new TenantUsageResponse(
            user.getId(),
            user.getUsername(),
            user.getRateLimitPerMinute(),
            user.getMaxActiveJobs(),
            activeJobs,
            pendingJobs,
            runningJobs
        );

        return ResponseEntity.ok(response);
    }
}
