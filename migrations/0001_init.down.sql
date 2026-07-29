DROP INDEX IF EXISTS idx_tasks_reclaim;
DROP INDEX IF EXISTS idx_tasks_dlq;
DROP INDEX IF EXISTS idx_tasks_claim;
DROP INDEX IF EXISTS uq_tasks_queue_idempotency;
DROP TABLE IF EXISTS tasks;