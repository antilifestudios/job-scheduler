-- V5: Add Dead-Letter Queue (DLQ) replay support with execution history preservation
-- and index for terminal job retention cleanup

ALTER TABLE jobs
    ADD COLUMN replay_count INT NOT NULL DEFAULT 0,
    ADD COLUMN replayed_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE job_attempts
    ADD COLUMN replay_count INT NOT NULL DEFAULT 0;

-- Composite index to fetch all attempts for a job ordered by run and attempt sequence
CREATE INDEX idx_job_attempts_job_replay ON job_attempts (job_id, replay_count ASC, attempt_no ASC);

-- Partial index for terminal job retention and batch deletion
CREATE INDEX idx_jobs_terminal_cleanup ON jobs (updated_at)
    WHERE status IN ('SUCCEEDED', 'DEAD');
