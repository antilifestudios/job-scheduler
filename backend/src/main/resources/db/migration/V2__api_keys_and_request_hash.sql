-- ============================================================================
-- V2__api_keys_and_request_hash.sql: Dedicated API keys table & request hash
-- ============================================================================

-- 1. Remove single-key constraint from users table
ALTER TABLE users DROP COLUMN IF EXISTS api_key_hash;

-- 2. Dedicated API Keys table supporting key rotation, prefixes, and revocation
CREATE TABLE api_keys (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    key_hash VARCHAR(64) NOT NULL UNIQUE,       -- SHA-256 hex hash of the secret key
    key_prefix VARCHAR(16) NOT NULL,            -- e.g. "sk_live_a1b2..." for identification in UI
    name VARCHAR(100) NOT NULL DEFAULT 'Default Key',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMP WITH TIME ZONE,        -- If NOT NULL, key is revoked
    last_used_at TIMESTAMP WITH TIME ZONE
);

-- Partial index for active API key lookups
CREATE INDEX idx_api_keys_lookup ON api_keys (key_hash)
    WHERE revoked_at IS NULL;

CREATE INDEX idx_api_keys_user ON api_keys (user_id);

-- 3. Add request_hash to jobs table for idempotency conflict detection
-- If the same idempotency_key is submitted with a different request_hash, return 409 Conflict.
ALTER TABLE jobs ADD COLUMN request_hash VARCHAR(64);
