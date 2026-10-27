package com.ashutosh.scheduler.domain.repo;

import com.ashutosh.scheduler.domain.entity.ApiKeyEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ApiKeyRepository extends JpaRepository<ApiKeyEntity, UUID> {

    @Query("SELECT k FROM ApiKeyEntity k JOIN FETCH k.user WHERE k.keyHash = :keyHash AND k.revokedAt IS NULL")
    Optional<ApiKeyEntity> findActiveByKeyHash(@Param("keyHash") String keyHash);

    Optional<ApiKeyEntity> findByKeyHash(String keyHash);
}
