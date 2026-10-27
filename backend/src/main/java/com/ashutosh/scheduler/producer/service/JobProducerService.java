package com.ashutosh.scheduler.producer.service;

import com.ashutosh.scheduler.common.exception.IdempotencyConflictException;
import com.ashutosh.scheduler.common.exception.ResourceNotFoundException;
import com.ashutosh.scheduler.common.util.CryptoUtils;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import com.ashutosh.scheduler.domain.repo.JobRepository;
import com.ashutosh.scheduler.producer.dto.CreateJobRequest;
import com.ashutosh.scheduler.producer.dto.HttpPayloadDto;
import com.ashutosh.scheduler.producer.dto.JobResponse;
import com.ashutosh.scheduler.producer.dto.PageResponse;
import com.ashutosh.scheduler.producer.validator.JobRequestValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class JobProducerService {

    private static final Logger log = LoggerFactory.getLogger(JobProducerService.class);

    private final JobRepository jobRepository;
    private final JdbcTemplate jdbcTemplate;
    private final JobRequestValidator validator;
    private final ObjectMapper objectMapper;

    public JobProducerService(JobRepository jobRepository,
                              JdbcTemplate jdbcTemplate,
                              JobRequestValidator validator,
                              ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.validator = validator;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public CreateJobResult createJob(UUID userId, CreateJobRequest request, String headerIdempotencyKey) {
        validator.validate(request, headerIdempotencyKey);

        String effectiveIdempotencyKey = resolveIdempotencyKey(request.idempotencyKey(), headerIdempotencyKey);
        String method = (request.method() == null || request.method().isBlank()) ? "POST" : request.method().trim().toUpperCase();
        String queue = (request.queue() == null || request.queue().isBlank()) ? "default" : request.queue().trim();
        int priority = request.priority() == null ? 0 : request.priority();
        int maxAttempts = request.maxAttempts() == null ? 3 : request.maxAttempts();
        Instant runAt = (request.runAt() == null || request.runAt().isBefore(Instant.now())) ? Instant.now() : request.runAt();
        Map<String, String> headers = request.headers() == null ? Map.of() : request.headers();

        String requestHash = CryptoUtils.computeRequestHash(method, request.url(), headers, request.body());

        HttpPayloadDto payloadDto = new HttpPayloadDto(request.url(), method, headers, null);
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payloadDto);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize job payload", e);
        }

        UUID jobId = UuidV7.generate();

        if (effectiveIdempotencyKey != null) {
            String insertSql = """
                INSERT INTO jobs (
                    id, user_id, queue, type, payload, body, status,
                    priority, run_at, attempts, max_attempts, idempotency_key,
                    request_hash, created_at, updated_at
                )
                VALUES (?, ?, ?, 'HTTP', ?::jsonb, ?, 'PENDING', ?, ?, 0, ?, ?, ?, now(), now())
                ON CONFLICT (user_id, idempotency_key) WHERE idempotency_key IS NOT NULL
                DO NOTHING
                RETURNING id
                """;

            List<UUID> returnedIds = jdbcTemplate.query(
                insertSql,
                (rs, rowNum) -> rs.getObject("id", UUID.class),
                jobId, userId, queue, payloadJson, request.body(),
                priority, Timestamp.from(runAt), maxAttempts, effectiveIdempotencyKey, requestHash
            );

            if (returnedIds.isEmpty()) {
                // Conflict detected: fetch existing job
                JobEntity existing = jobRepository.findByUserIdAndIdempotencyKey(userId, effectiveIdempotencyKey)
                    .orElseThrow(() -> new IllegalStateException("Job conflict reported but existing job not found"));

                // Validate request hash
                if (existing.getRequestHash() != null && !existing.getRequestHash().equals(requestHash)) {
                    throw new IdempotencyConflictException(
                        "Idempotency key '" + effectiveIdempotencyKey + "' was previously submitted with a different request payload."
                    );
                }

                // Never log headers or bodies!
                log.info("Idempotent job replayed: id={}, userId={}, queue={}", existing.getId(), userId, existing.getQueue());
                return new CreateJobResult(JobResponse.fromEntity(existing, objectMapper), true);
            }
        } else {
            String insertSql = """
                INSERT INTO jobs (
                    id, user_id, queue, type, payload, body, status,
                    priority, run_at, attempts, max_attempts, idempotency_key,
                    request_hash, created_at, updated_at
                )
                VALUES (?, ?, ?, 'HTTP', ?::jsonb, ?, 'PENDING', ?, ?, 0, ?, ?, ?, now(), now())
                RETURNING id
                """;

            jdbcTemplate.query(
                insertSql,
                (rs, rowNum) -> rs.getObject("id", UUID.class),
                jobId, userId, queue, payloadJson, request.body(),
                priority, Timestamp.from(runAt), maxAttempts, null, requestHash
            );
        }

        // Never log headers or bodies!
        log.info("Job created: id={}, userId={}, queue={}, priority={}", jobId, userId, queue, priority);

        JobEntity createdEntity = jobRepository.findById(jobId)
            .orElseThrow(() -> new IllegalStateException("Newly inserted job could not be retrieved"));

        return new CreateJobResult(JobResponse.fromEntity(createdEntity, objectMapper), false);
    }

    @Transactional(readOnly = true)
    public JobResponse getJobById(UUID jobId, UUID userId) {
        JobEntity entity = jobRepository.findByIdAndUserId(jobId, userId)
            .orElseThrow(() -> new ResourceNotFoundException("Job not found: " + jobId));
        return JobResponse.fromEntity(entity, objectMapper);
    }

    @Transactional(readOnly = true)
    public PageResponse<JobResponse> listJobs(UUID userId, JobStatus status, String queue, int page, int size) {
        // Enforce maximum page size cap of 100
        int cappedSize = Math.min(Math.max(size, 1), 100);
        // Ignore client sort; always enforce created_at DESC matching our partial indexes
        Pageable pageable = PageRequest.of(Math.max(page, 0), cappedSize, Sort.by(Sort.Direction.DESC, "createdAt"));

        Page<JobEntity> jobPage;
        if (status != null && queue != null && !queue.isBlank()) {
            jobPage = jobRepository.findByUserIdAndStatusAndQueue(userId, status, queue.trim(), pageable);
        } else if (status != null) {
            jobPage = jobRepository.findByUserIdAndStatus(userId, status, pageable);
        } else if (queue != null && !queue.isBlank()) {
            jobPage = jobRepository.findByUserIdAndQueue(userId, queue.trim(), pageable);
        } else {
            jobPage = jobRepository.findByUserId(userId, pageable);
        }

        return PageResponse.from(jobPage.map(job -> JobResponse.fromEntity(job, objectMapper)));
    }

    private String resolveIdempotencyKey(String bodyKey, String headerKey) {
        if (headerKey != null && !headerKey.isBlank()) {
            return headerKey.trim();
        }
        if (bodyKey != null && !bodyKey.isBlank()) {
            return bodyKey.trim();
        }
        return null;
    }
}
