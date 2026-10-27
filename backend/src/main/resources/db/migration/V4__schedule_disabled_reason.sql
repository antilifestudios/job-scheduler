-- ============================================================================
-- V4__schedule_disabled_reason.sql: Add disabled_reason column to schedules
-- ============================================================================

ALTER TABLE schedules
    ADD COLUMN disabled_reason VARCHAR(100);
