package com.ashutosh.scheduler.domain.repo;

import com.ashutosh.scheduler.domain.entity.ApiKeyEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ApiKeyRepository extends JpaRepository<ApiKeyEntity, UUID> {

    @Query("SELECT k FROM ApiKeyEntity k JOIN FETCH k.user WHERE k.keyHash = :keyHash AND k.revokedAt IS NULL")
    Optional<ApiKeyEntity> findActiveByKeyHash(@Param("keyHash") String keyHash);

    Optional<ApiKeyEntity> findByKeyHash(String keyHash);

    @Query("SELECT COUNT(k) FROM ApiKeyEntity k WHERE k.user.id = :userId AND k.revokedAt IS NULL")
    long countByUserIdAndRevokedAtIsNull(@Param("userId") UUID userId);

    List<ApiKeyEntity> findByUserIdOrderByCreatedAtDesc(UUID userId);

    Optional<ApiKeyEntity> findByIdAndUserId(UUID id, UUID userId);

    @org.springframework.transaction.annotation.Transactional
    @org.springframework.data.jpa.repository.Modifying
    @Query("UPDATE ApiKeyEntity k SET k.lastUsedAt = :now WHERE k.id = :id")
    int updateLastUsedAt(@Param("id") UUID id, @Param("now") java.time.Instant now);
}
