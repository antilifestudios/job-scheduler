package com.ashutosh.scheduler.migration;

import com.ashutosh.scheduler.TestcontainersConfiguration;
import com.ashutosh.scheduler.common.util.UuidV7;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class FlywayMigrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("Flyway V1 schema should create all core tables")
    void testCoreTablesExist() {
        List<String> tables = jdbcTemplate.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            String.class
        );

        assertThat(tables).contains("users", "api_keys", "jobs", "job_attempts", "schedules", "flyway_schema_history");
    }

    @Test
    @DisplayName("Core indexes and partial indexes should exist")
    void testIndexesExist() {
        List<String> indexes = jdbcTemplate.queryForList(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'",
            String.class
        );

        assertThat(indexes).contains(
            "idx_jobs_claim",
            "idx_jobs_reaper",
            "idx_jobs_user_idempotency",
            "idx_jobs_user_status",
            "idx_jobs_user_created",
            "idx_job_attempts_job_id",
            "idx_schedules_next_run",
            "idx_api_keys_lookup",
            "idx_api_keys_user",
            "idx_jobs_schedule_active",
            "idx_jobs_schedule_id"
        );
    }

    @Test
    @DisplayName("Should insert user, api_key, and job with UUIDv7, JSONB payload, and raw text body")
    void testInsertUserAndJob() {
        UUID userId = UuidV7.generate();
        UUID keyId = UuidV7.generate();
        UUID jobId = UuidV7.generate();

        jdbcTemplate.update(
            "INSERT INTO users (id, username) VALUES (?, ?)",
            userId, "alice"
        );

        jdbcTemplate.update(
            "INSERT INTO api_keys (id, user_id, key_hash, key_prefix) VALUES (?, ?, ?, ?)",
            keyId, userId, "hash_alice_123", "sk_live_alice"
        );

        jdbcTemplate.update(
            """
            INSERT INTO jobs (id, user_id, queue, type, payload, body, status, priority)
            VALUES (?, ?, 'default', 'HTTP', '{"url":"https://example.com/api","method":"POST"}'::jsonb, 'raw body string', 'PENDING', 10)
            """,
            jobId, userId
        );

        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM jobs WHERE id = ?",
            Integer.class,
            jobId
        );
        assertThat(count).isEqualTo(1);
    }

    @Test
    @DisplayName("Idempotency key constraint should prevent duplicates per user")
    void testIdempotencyKeyConstraint() {
        UUID userId = UuidV7.generate();
        UUID job1 = UuidV7.generate();
        UUID job2 = UuidV7.generate();

        jdbcTemplate.update(
            "INSERT INTO users (id, username) VALUES (?, ?)",
            userId, "bob"
        );

        jdbcTemplate.update(
            """
            INSERT INTO jobs (id, user_id, queue, type, payload, status, idempotency_key)
            VALUES (?, ?, 'default', 'HTTP', '{"url":"https://example.com"}'::jsonb, 'PENDING', 'order-123')
            """,
            job1, userId
        );

        assertThatThrownBy(() -> {
            jdbcTemplate.update(
                """
                INSERT INTO jobs (id, user_id, queue, type, payload, status, idempotency_key)
                VALUES (?, ?, 'default', 'HTTP', '{"url":"https://example.com"}'::jsonb, 'PENDING', 'order-123')
                """,
                job2, userId
            );
        }).isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("CHECK constraint on status should reject invalid job statuses")
    void testStatusCheckConstraint() {
        UUID userId = UuidV7.generate();
        UUID jobId = UuidV7.generate();

        jdbcTemplate.update(
            "INSERT INTO users (id, username) VALUES (?, ?)",
            userId, "charlie"
        );

        assertThatThrownBy(() -> {
            jdbcTemplate.update(
                """
                INSERT INTO jobs (id, user_id, queue, type, payload, status)
                VALUES (?, ?, 'default', 'HTTP', '{"url":"https://example.com"}'::jsonb, 'INVALID_STATUS')
                """,
                jobId, userId
            );
        }).isInstanceOf(DataIntegrityViolationException.class);
    }
}
