-- ============================================================================
-- V3__schedules_enhancement.sql: Phase 8 Recurring Cron Schedules Enhancement
-- ============================================================================

-- 1. Enhance schedules table with timezone, policies, auditing, and auto-disable failure tracking
ALTER TABLE schedules
    ADD COLUMN timezone VARCHAR(50) NOT NULL DEFAULT 'UTC',
    ADD COLUMN overlap_policy VARCHAR(20) NOT NULL DEFAULT 'SKIP'
        CHECK (overlap_policy IN ('ALLOW', 'SKIP')),
    ADD COLUMN misfire_policy VARCHAR(20) NOT NULL DEFAULT 'FIRE_ONCE'
        CHECK (misfire_policy IN ('FIRE_ONCE', 'SKIP')),
    ADD COLUMN grace_window_seconds INT NOT NULL DEFAULT 300,
    ADD COLUMN last_fired_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN last_tick_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN last_skip_reason VARCHAR(50),
    ADD COLUMN consecutive_failures INT NOT NULL DEFAULT 0,
    ADD COLUMN last_error TEXT;

-- 2. Link jobs to originating schedule
ALTER TABLE jobs
    ADD COLUMN schedule_id UUID REFERENCES schedules(id) ON DELETE SET NULL;

-- 3. Partial index for lightning-fast O(1) overlap checks on active jobs
CREATE INDEX idx_jobs_schedule_active ON jobs (schedule_id)
    WHERE status IN ('PENDING', 'RUNNING');

CREATE INDEX idx_jobs_schedule_id ON jobs (schedule_id, created_at DESC);
