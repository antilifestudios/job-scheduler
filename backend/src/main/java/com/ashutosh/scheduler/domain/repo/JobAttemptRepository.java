package com.ashutosh.scheduler.domain.repo;

import com.ashutosh.scheduler.domain.entity.JobAttemptEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface JobAttemptRepository extends JpaRepository<JobAttemptEntity, UUID> {
    List<JobAttemptEntity> findByJobIdOrderByAttemptNoAsc(UUID jobId);
    List<JobAttemptEntity> findByJobIdOrderByReplayCountAscAttemptNoAsc(UUID jobId);
}
