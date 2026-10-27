package com.ashutosh.scheduler.bootstrap;

import com.ashutosh.scheduler.common.util.CryptoUtils;
import com.ashutosh.scheduler.common.util.UuidV7;
import com.ashutosh.scheduler.domain.entity.ApiKeyEntity;
import com.ashutosh.scheduler.domain.entity.UserEntity;
import com.ashutosh.scheduler.domain.repo.ApiKeyRepository;
import com.ashutosh.scheduler.domain.repo.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Development bootstrap runner.
 *
 * Automatically provisions an initial user and API key if none exist.
 * No API keys are ever stored in database migrations.
 */
@Component
@Profile("!test")
public class DevBootstrapRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DevBootstrapRunner.class);

    private final UserRepository userRepository;
    private final ApiKeyRepository apiKeyRepository;

    public DevBootstrapRunner(UserRepository userRepository, ApiKeyRepository apiKeyRepository) {
        this.userRepository = userRepository;
        this.apiKeyRepository = apiKeyRepository;
    }

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        if (userRepository.count() == 0) {
            UUID userId = UuidV7.generate();
            UserEntity user = new UserEntity(userId, "default-user");
            userRepository.save(user);

            String rawKey = CryptoUtils.generateApiKey();
            String keyHash = CryptoUtils.sha256Hex(rawKey);
            String prefix = CryptoUtils.extractPrefix(rawKey);

            ApiKeyEntity apiKey = new ApiKeyEntity(UuidV7.generate(), user, keyHash, prefix, "Initial Dev Key");
            apiKeyRepository.save(apiKey);

            log.info("\n" +
                "================================================================================\n" +
                "[BOOTSTRAP] Initial user and API key provisioned for local development:\n" +
                "Username: default-user\n" +
                "API Key:  {}\n" +
                "(Keep this key safe. It is hashed in the database and will not be displayed again.)\n" +
                "================================================================================",
                rawKey
            );
        }
    }
}
