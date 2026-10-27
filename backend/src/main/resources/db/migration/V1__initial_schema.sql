-- ============================================================================
-- V1__initial_schema.sql: Core schema for Reliable HTTP Job Scheduler
-- ============================================================================

-- 1. USERS & API KEYS
-- Tenants who submit and own jobs. Per-user ownership checks on every query
-- guarantee multi-tenant isolation.
CREATE TABLE users (
    id UUID PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    api_key_hash VARCHAR(255) NOT NULL UNIQUE,
    rate_limit_per_minute INT NOT NULL DEFAULT 60,
    max_active_jobs INT NOT NULL DEFAULT 100,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- 2. JOBS QUEUE TABLE
-- The central work queue. Uses fillfactor = 70 to reserve 30% page capacity
-- for HOT (Heap-Only Tuple) updates as rows transition states, preventing
-- table and index bloat.
CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    queue VARCHAR(50) NOT NULL DEFAULT 'default',
    type VARCHAR(50) NOT NULL,
    -- Configuration metadata: url, method, headers, timeout options
    payload JSONB NOT NULL,
    -- Verbatim request body: stored as text to avoid altering whitespace,
    -- formatting, or non-JSON payloads during JSONB round-tripping.
    body TEXT,
    -- Status is VARCHAR with CHECK constraint for zero-downtime schema evolution
    -- and seamless JPA EnumType.STRING compatibility without catalog lock issues.
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD')),
    priority INT NOT NULL DEFAULT 0,
    run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    locked_by VARCHAR(100),
    locked_until TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    idempotency_key VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Autovacuum & Bloat tuning for the update-heavy jobs table:
-- Lower vacuum scale factor triggers autovacuum after 2% row churn instead of 20%.
ALTER TABLE jobs SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 100,
    autovacuum_vacuum_cost_limit = 1000,
    autovacuum_vacuum_cost_delay = 2
);

-- Partial index for atomic claim query:
-- Only indexes PENDING jobs eligible for execution. Completed or running jobs are
-- excluded, keeping the index small, RAM-resident, and lightning fast.
CREATE INDEX idx_jobs_claim ON jobs (priority DESC, run_at ASC)
    WHERE status = 'PENDING';

-- Partial index for lease reaper:
-- Quickly locates abandoned jobs whose worker crashed or timed out.
CREATE INDEX idx_jobs_reaper ON jobs (locked_until ASC)
    WHERE status = 'RUNNING';

-- Enforce per-user idempotency key uniqueness:
-- Partial unique index ensures duplicate requests from the same user return the existing job.
CREATE UNIQUE INDEX idx_jobs_user_idempotency ON jobs (user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Per-user listing indexes for dashboard queries and filtering:
CREATE INDEX idx_jobs_user_status ON jobs (user_id, status, created_at DESC);
CREATE INDEX idx_jobs_user_created ON jobs (user_id, created_at DESC);

-- 3. JOB ATTEMPTS (Execution History)
-- Append-only audit log of every execution attempt.
CREATE TABLE job_attempts (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_no INT NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    duration_ms BIGINT NOT NULL,
    status_code INT,
    response_snippet TEXT,
    error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_job_attempts_job_id ON job_attempts (job_id, attempt_no ASC);

-- 4. SCHEDULES (Recurring Cron Jobs)
CREATE TABLE schedules (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    cron_expression VARCHAR(100) NOT NULL,
    job_template JSONB NOT NULL,
    next_run_at TIMESTAMP WITH TIME ZONE NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_schedules_next_run ON schedules (next_run_at ASC)
    WHERE enabled = TRUE;
CREATE INDEX idx_schedules_user_id ON schedules (user_id);
