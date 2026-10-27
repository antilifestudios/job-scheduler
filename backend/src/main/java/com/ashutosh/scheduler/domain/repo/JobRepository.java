package com.ashutosh.scheduler.domain.repo;

import com.ashutosh.scheduler.domain.entity.JobEntity;
import com.ashutosh.scheduler.domain.enums.JobStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface JobRepository extends JpaRepository<JobEntity, UUID> {

    Optional<JobEntity> findByIdAndUserId(UUID id, UUID userId);

    Optional<JobEntity> findByUserIdAndIdempotencyKey(UUID userId, String idempotencyKey);

    Page<JobEntity> findByUserId(UUID userId, Pageable pageable);

    Page<JobEntity> findByUserIdAndStatus(UUID userId, JobStatus status, Pageable pageable);

    Page<JobEntity> findByUserIdAndQueue(UUID userId, String queue, Pageable pageable);

    Page<JobEntity> findByUserIdAndStatusAndQueue(UUID userId, JobStatus status, String queue, Pageable pageable);

    long countByUserIdAndStatusIn(UUID userId, java.util.Collection<JobStatus> statuses);

    default long countActiveJobsByUserId(UUID userId) {
        return countByUserIdAndStatusIn(userId, java.util.List.of(JobStatus.PENDING, JobStatus.RUNNING));
    }

    long countByUserIdAndStatus(UUID userId, JobStatus status);

    long countByScheduleIdAndStatusIn(UUID scheduleId, java.util.Collection<JobStatus> statuses);

    default boolean hasActiveJobsForSchedule(UUID scheduleId) {
        return countByScheduleIdAndStatusIn(scheduleId, java.util.List.of(JobStatus.PENDING, JobStatus.RUNNING)) > 0;
    }

    Page<JobEntity> findByScheduleIdOrderByCreatedAtDesc(UUID scheduleId, Pageable pageable);
}
