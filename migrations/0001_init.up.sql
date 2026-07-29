CREATE TABLE IF NOT EXISTS tasks (
    id                  UUID PRIMARY KEY,
    queue               TEXT NOT NULL,
    payload             JSONB NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    idempotency_key     TEXT,

    attempts            INT NOT NULL DEFAULT 0,
    max_attempts        INT NOT NULL DEFAULT 5,

    visibility_timeout_ms BIGINT NOT NULL DEFAULT 30000,
    visible_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    locked_by           TEXT,
    last_heartbeat_at   TIMESTAMPTZ,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduled_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ,

    last_error          TEXT,

    CONSTRAINT chk_status CHECK (status IN ('pending','processing','completed','failed','dead_letter'))
);

-- Enforce idempotency dedupe at DB level per queue (race-safe unique constraint).
-- Partial index: only enforced when key is present.
CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_queue_idempotency
    ON tasks (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Hot path: claiming pending/expired-processing tasks per queue.
CREATE INDEX IF NOT EXISTS idx_tasks_claim
    ON tasks (queue, status, visible_at)
    WHERE status IN ('pending', 'processing');

-- DLQ listing.
CREATE INDEX IF NOT EXISTS idx_tasks_dlq
    ON tasks (queue, status, created_at DESC)
    WHERE status = 'dead_letter';

-- Reclaim scan (expired visibility while processing).
CREATE INDEX IF NOT EXISTS idx_tasks_reclaim
    ON tasks (status, visible_at)
    WHERE status = 'processing';