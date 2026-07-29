# Distributed Task Queue

Production-ready, modular distributed task queue backend written in Go. Built for reliability under load: at-least-once delivery, crash recovery via visibility timeouts, dead letter queues, idempotent producers, and graceful shutdown.

## Architecture

Clean/modular-monolith layout — each layer depends only on interfaces (`internal/ports`), never on concrete implementations.

```
┌─────────────┐      ┌───────────────┐      ┌──────────────┐
│  Producer   │ ───▶ │    Broker      │ ───▶ │    Worker     │
│  (HTTP API) │      │  (Redis lists  │      │     Pool      │
└──────┬──────┘      │   + ZSET)      │      └───────┬──────┘
       │             └───────────────┘              │
       ▼                                             ▼
┌─────────────────────────────────────────────────────────┐
│              Storage (Postgres — source of truth)         │
│   status, attempts, visibility_timeout, idempotency_key   │
└─────────────────────────────────────────────────────────┘
```

- **Producer (`cmd/producer`)** — HTTP API for task submission, dedup lookup, DLQ inspection/replay.
- **Broker (`internal/broker/redis`)** — transport/signaling layer only (pending list, delayed ZSET for backoff, DLQ list, token-bucket rate limiter). Not the source of truth.
- **Storage (`internal/storage/postgres`)** — durable state, atomic claim via `SELECT ... FOR UPDATE SKIP LOCKED`, unique idempotency constraint.
- **Engine (`internal/engine`)** — retry/backoff/timeout/DLQ decision logic, reclaim loop, delayed-task promotion loop.
- **Worker Pool (`internal/worker`)** — concurrency-gated dispatch loop, per-task heartbeat, graceful SIGTERM drain.
- **Domain (`internal/domain`)** — pure business entities/rules, no infra imports.

## Production Features

| Feature | Mechanism |
|---|---|
| Dead Letter Queue | Tasks exceeding `max_attempts` routed to `dead_letter` status + Redis DLQ list; replayable via `POST /tasks/{id}/requeue` |
| Heartbeat & Visibility Timeout | Worker renews `visible_at` every `HEARTBEAT_INTERVAL_SEC`; `ReclaimLoop` resets stale `processing` tasks back to `pending` if a worker crashes |
| Graceful Shutdown | SIGTERM detaches in-flight task context from the dispatch loop; pool drains up to `SHUTDOWN_TIMEOUT_SEC` before force-cancelling |
| Idempotency Key | Unique partial index (`queue`, `idempotency_key`) at DB level; duplicate submissions return the existing task instead of erroring |
| Rate Limiting | Redis Lua-script token bucket, per worker ID |
| Concurrency Control | Buffered channel semaphore, `WORKER_CONCURRENCY`-wide |
| Exponential Backoff | Configurable multiplier/cap in `domain.RetryPolicy`, applied on retry via delayed ZSET |

## Security & Resource Protection

| Protection | Mechanism |
|---|---|
| Request Body Size Limits | Producer HTTP API caps max request body at 1MB to prevent memory bloat and OOM attacks |
| Strict Payload Validation | Type-safe struct parsing and field validation on all inbound requests, guarding against SQL injection and command execution risks |
| API Rate Limiting | IP/token-based rate limiting in front of the Producer API to prevent spam/DoS and public API abuse |
| Context Timeouts | Hard task execution deadlines via `context.WithTimeout` prevent stuck processes or infinite loops from exhausting worker resources |

## Prerequisites

- Go 1.23+
- Docker & Docker Compose
- `golang-migrate` CLI (only if running migrations outside Docker)

## Quick Start

```bash
git clone https://github.com/shyxur/distributed-task-queue.git
cd distributed-task-queue
go mod tidy
go build ./...

docker compose up --build -d postgres redis migrate
docker compose up --build producer worker
```

Health check:

```bash
curl http://localhost:8080/health
```

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/tasks` | Enqueue a task |
| `GET` | `/tasks/{id}` | Fetch task status |
| `POST` | `/tasks/{id}/requeue` | Replay a dead-lettered task |
| `GET` | `/queues/{queue}/dlq` | List dead-lettered tasks |
| `GET` | `/health` | Storage connectivity check |

### Create a task

```bash
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "default",
    "payload": {"hello": "world"},
    "idempotency_key": "order-123-charge",
    "max_attempts": 5,
    "visibility_timeout_sec": 30
  }'
```

Resubmitting the same `idempotency_key` for the same `queue` returns the original task (HTTP 200) instead of creating a duplicate.

## Configuration

All via environment variables (see `internal/config/config.go` for defaults):

| Variable | Default | Description |
|---|---|---|
| `DB_DSN` | `postgres://taskqueue:taskqueue@localhost:5432/taskqueue?sslmode=disable` | Postgres connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `HTTP_PORT` | `8080` | Producer API port |
| `WORKER_CONCURRENCY` | `10` | Max concurrent tasks per worker process |
| `RATE_LIMIT_PER_SEC` | `50` | Token bucket refill rate per worker |
| `HEARTBEAT_INTERVAL_SEC` | `10` | Heartbeat renewal frequency |
| `VISIBILITY_TIMEOUT_SEC` | `30` | Time before an unacked task is considered crashed |
| `TASK_TIMEOUT_SEC` | `60` | Per-task handler execution deadline |
| `SHUTDOWN_TIMEOUT_SEC` | `30` | Grace period for in-flight tasks on SIGTERM |
| `RECLAIM_INTERVAL_SEC` | `15` | Crashed-worker scan frequency |
| `PROMOTE_INTERVAL_SEC` | `5` | Delayed(backoff)-task promotion frequency |
| `QUEUE_NAME` | `default` | Queue this worker process consumes |

## Adding a Job Handler

Implement `domain.JobHandler` and register it in `cmd/worker/main.go`:

```go
type MyHandler struct{}

func (h *MyHandler) QueueName() string { return "emails" }

func (h *MyHandler) Handle(ctx context.Context, payload []byte) error {
    // business logic — return error to trigger retry/backoff,
    // or domain.NewRetryableError(err) to force-retry a normally-fatal error
    return nil
}
```

Scale to multiple queues by registering additional handlers via `engine.HandlerRegistry` and running one `worker.Pool` per queue (either as separate goroutines in one process or separate `cmd/worker` deployments).

## Project Structure

```
cmd/
  producer/        HTTP API entrypoint
  worker/          Worker process entrypoint
internal/
  domain/          Core entities & business rules (no infra deps)
  ports/           Broker/Storage/RateLimiter interfaces
  broker/redis/    Redis transport implementation
  storage/postgres/Postgres persistence implementation
  engine/          Retry/backoff/timeout/DLQ orchestration
  worker/          Worker pool, heartbeat, graceful shutdown
  api/             HTTP handlers & routing
  config/          Environment-based configuration
migrations/        SQL schema migrations
docker/            Dockerfiles for producer/worker
```

## Scaling Notes

- Run multiple `worker` replicas per queue — `ClaimForProcessing` uses `SKIP LOCKED`, so concurrent claims never collide.
- `ReclaimLoop` and `DelayedPromotionLoop` are safe to run redundantly across replicas (idempotent via atomic row locks / `ZREM` race-checks); no leader election required.
- Postgres remains the source of truth — Redis broker failure/restart does not cause data loss, only temporary dispatch delay until the reclaim loop re-surfaces affected tasks.

## License

MIT