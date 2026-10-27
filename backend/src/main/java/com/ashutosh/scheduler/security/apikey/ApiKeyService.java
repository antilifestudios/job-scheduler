package com.ashutosh.scheduler.security.apikey;

import com.ashutosh.scheduler.common.exception.CannotRevokeLastKeyException;
import com.ashutosh.scheduler.common.exception.MaxApiKeysExceededException;
import com.ashutosh.scheduler.common.exception.ResourceNotFoundException;
import com.ashutosh.scheduler.common.util.CryptoUtils;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.ApiKeyEntity;
import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.repo.ApiKeyRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class ApiKeyService {

    private static final Logger log = LoggerFactory.getLogger(ApiKeyService.class);

    public static final int MAX_ACTIVE_KEYS = 5;

    private final ApiKeyRepository apiKeyRepository;
    private final UserRepository userRepository;

    public ApiKeyService(ApiKeyRepository apiKeyRepository, UserRepository userRepository) {
        this.apiKeyRepository = apiKeyRepository;
        this.userRepository = userRepository;
    }

    @Transactional
    public CreateApiKeyResponse createApiKey(UUID userId, CreateApiKeyRequest request) {
        UserEntity user = userRepository.findById(userId)
            .orElseThrow(() -> new ResourceNotFoundException("User not found: " + userId));

        long activeCount = apiKeyRepository.countByUserIdAndRevokedAtIsNull(userId);
        if (activeCount >= MAX_ACTIVE_KEYS) {
            throw new MaxApiKeysExceededException(
                "Maximum active API keys limit (" + MAX_ACTIVE_KEYS + ") reached. Revoke an existing key before generating a new one."
            );
        }

        String rawKey = CryptoUtils.generateApiKey();
        String keyHash = CryptoUtils.sha256Hex(rawKey);
        String prefix = CryptoUtils.extractPrefix(rawKey);
        String name = (request != null && request.name() != null && !request.name().isBlank())
            ? request.name().trim()
            : "API Key " + prefix;

        ApiKeyEntity entity = new ApiKeyEntity(UuidV7.generate(), user, keyHash, prefix, name);
        apiKeyRepository.save(entity);

        log.info("Provisioned new API key for userId={}, prefix={}", userId, prefix);

        return new CreateApiKeyResponse(
            entity.getId(),
            entity.getName(),
            entity.getKeyPrefix(),
            rawKey,
            entity.getCreatedAt()
        );
    }

    @Transactional(readOnly = true)
    public List<ApiKeyResponse> listApiKeys(UUID userId) {
        return apiKeyRepository.findByUserIdOrderByCreatedAtDesc(userId).stream()
            .map(ApiKeyResponse::fromEntity)
            .toList();
    }

    @Transactional
    public ApiKeyResponse revokeApiKey(UUID userId, UUID keyId) {
        ApiKeyEntity key = apiKeyRepository.findByIdAndUserId(keyId, userId)
            .orElseThrow(() -> new ResourceNotFoundException("API key not found: " + keyId));

        if (key.isRevoked()) {
            return ApiKeyResponse.fromEntity(key);
        }

        long activeCount = apiKeyRepository.countByUserIdAndRevokedAtIsNull(userId);
        if (activeCount <= 1) {
            throw new CannotRevokeLastKeyException(
                "Cannot revoke the only active API key for this user. Create a new key before revoking this one."
            );
        }

        key.setRevokedAt(Instant.now());
        apiKeyRepository.save(key);

        log.info("Revoked API key: id={}, prefix={}, userId={}", key.getId(), key.getKeyPrefix(), userId);
        return ApiKeyResponse.fromEntity(key);
    }
}
