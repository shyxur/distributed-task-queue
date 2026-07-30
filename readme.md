# Distributed Task Queue

A production-ready, modular distributed task queue backend written in Go. Built for reliability under load: at-least-once delivery, crash recovery via visibility timeouts, dead letter queues, idempotent producers, hardened API request handling, and graceful shutdown.

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

- **Producer (`cmd/producer`)** — hardened HTTP API for task submission: rate limiting → body-size capping → strict validation → idempotency check → DB persistence → broker enqueue.
- **Broker (`internal/broker/redis`)** — transport/signaling layer only (pending list, delayed ZSET for backoff, DLQ list, token-bucket rate limiter). Not the source of truth.
- **Storage (`internal/storage/postgres`)** — durable state, atomic claim via `SELECT ... FOR UPDATE SKIP LOCKED`, unique idempotency constraint.
- **Engine (`internal/engine`)** — retry/backoff/timeout/DLQ decision logic, reclaim loop, delayed-task promotion loop.
- **Worker Pool (`internal/worker`)** — concurrency-gated dispatch loop, per-task heartbeat, hard task-execution deadline, graceful SIGTERM drain.
- **Domain (`internal/domain`)** — pure business entities/rules, no infra imports.

## Core Flow

### Producer (API) request lifecycle

Every `POST /tasks` request passes through the following pipeline, in order:

1. **Rate limiting** — per-IP token bucket rejects excess traffic before the body is even read.
2. **Body size cap** — request body wrapped in `http.MaxBytesReader`, rejecting oversized payloads early.
3. **Strict validation** — typed struct validation on `queue`, `payload`, `max_attempts`, `visibility_timeout_sec`, `idempotency_key`.
4. **Idempotency check** — if `idempotency_key` already exists for the queue, the existing task is returned instead of creating a duplicate.
5. **Persistence** — task is durably written to Postgres (source of truth) with `status = pending`.
6. **Enqueue** — task ID is pushed onto the Redis broker for worker consumption.

### Workers

Worker processes (`cmd/worker`) run a concurrency-gated dispatch loop per queue:

- Claim a task atomically from storage (`FOR UPDATE SKIP LOCKED` — no double-claiming under concurrency).
- Run the registered `domain.JobHandler` for that queue under a hard `context.WithTimeout` deadline.
- Heartbeat the claim periodically to renew visibility while work is in progress.
- On success → mark completed. On failure → retry with exponential backoff, or route to the Dead Letter Queue once `max_attempts` is exhausted.

Handlers are registered per queue name via `engine.HandlerRegistry`, so a single deployment can run multiple logical workers (e.g. a `default` queue for generic background jobs and an `emails` queue for outbound email/notification processing) either as separate goroutines in one process or as independent `cmd/worker` deployments with `QUEUE_NAME` set accordingly.

### Idempotency Key

Producers may pass an `idempotency_key` with each task. It is enforced with a unique partial index on `(queue, idempotency_key)` at the database level — the true concurrency-safe guarantee, not just an application-side check. Retried submissions (e.g. from a flaky producer client) return the original task and its current status (`HTTP 200`) instead of creating a duplicate.

## Security & Request Protections

All protections live in `internal/api` and are applied via composable middleware, outermost-first: **rate limit → body size cap → routing/validation**. This ordering ensures abusive traffic is rejected as cheaply as possible, before any parsing or business logic runs.

| Protection | Mechanism | Response |
|---|---|---|
| **Rate Limiting** | Per-IP token bucket (`golang.org/x/time/rate`), default 50 req/sec with burst 100; idle visitor entries are TTL-evicted to bound memory | `429 Too Many Requests` |
| **Payload Size Limit** | `http.MaxBytesReader` caps every request body at 1MB, preventing memory-exhaustion (OOM) and oversized-payload DoS attempts | `413 Request Entity Too Large` |
| **Request Validation** | Strict, typed validation of `queue` (required, ≤64 chars), `payload` (required, valid JSON, non-null), `max_attempts` (1–50), `visibility_timeout_sec` (5–3600s), `idempotency_key` (≤255 chars) | `400 Bad Request` with structured `details[]` |
| **Task Execution Timeout** | Every handler invocation runs under `context.WithTimeout(ctx, TASK_TIMEOUT_SEC)`; a stuck or slow handler (e.g. hanging HTTP call) is cancelled and routed through the normal retry/backoff pipeline rather than holding a worker slot indefinitely | Classified as `domain.ErrTaskTimeout`, retried or dead-lettered |

Additional resilience mechanisms (see [Production Features](#production-features)):

| Feature | Mechanism |
|---|---|
| Dead Letter Queue | Tasks exceeding `max_attempts` routed to `dead_letter` status + Redis DLQ list; replayable via `POST /tasks/{id}/requeue` |
| Heartbeat & Visibility Timeout | Worker renews `visible_at` every `HEARTBEAT_INTERVAL_SEC`; `ReclaimLoop` resets stale `processing` tasks back to `pending` if a worker crashes |
| Graceful Shutdown | SIGTERM detaches in-flight task context from the dispatch loop; pool drains up to `SHUTDOWN_TIMEOUT_SEC` before force-cancelling |
| Concurrency Control | Buffered channel semaphore, `WORKER_CONCURRENCY`-wide, per worker process |
| Exponential Backoff | Configurable multiplier/cap in `domain.RetryPolicy`, applied on retry via delayed ZSET |

## Prerequisites

- Go 1.23+
- Docker & Docker Compose
- `golang-migrate` CLI (only if running migrations outside Docker)

## Quick Start

```bash
git clone <https://github.com/shyxur/distributed-task-queue>
cd taskqueue
go mod tidy
go build ./...

docker-compose up --build -d postgres redis migrate
docker-compose up --build producer worker
```

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Storage connectivity check |
| `POST` | `/tasks` | Enqueue a task |
| `GET` | `/tasks/{id}` | Fetch task status |
| `POST` | `/tasks/{id}/requeue` | Replay a dead-lettered task |
| `GET` | `/queues/{queue}/dlq` | List dead-lettered tasks |

### Health check

```bash
curl -i http://localhost:8080/health
```

**Expected:** `200 OK`

```json
{"status": "ok"}
```

### Create a task

```bash
curl -i -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "default",
    "payload": {"hello": "world"},
    "idempotency_key": "order-123-charge",
    "max_attempts": 5,
    "visibility_timeout_sec": 30
  }'
```

**Expected:** `201 Created`

```json
{"id": "b3f1c2a4-...-9e21", "status": "pending"}
```

Resubmitting the same `idempotency_key` for the same `queue` returns the original task:

**Expected:** `200 OK`

### Fetch task detail

```bash
curl -i http://localhost:8080/tasks/b3f1c2a4-...-9e21
```

**Expected:** `200 OK` — full task record (status, attempts, timestamps, last error if any). `404 Not Found` if the ID doesn't exist.

### Validation failure

```bash
curl -i -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{"queue":"","max_attempts":100,"visibility_timeout_sec":1}'
```

**Expected:** `400 Bad Request`

```json
{
  "error": "validation_failed",
  "details": [
    {"field": "queue", "message": "is required"},
    {"field": "payload", "message": "is required and cannot be empty or null"},
    {"field": "max_attempts", "message": "must be between 1 and 50"},
    {"field": "visibility_timeout_sec", "message": "must be between 5 and 3600 seconds"}
  ]
}
```

### Payload too large

```bash
curl -i -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  --data-binary @<(head -c 2000000 /dev/urandom | base64)
```

**Expected:** `413 Request Entity Too Large`

### Rate limit exceeded

```bash
for i in $(seq 1 120); do
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/tasks \
    -X POST -H "Content-Type: application/json" \
    -d '{"queue":"default","payload":{"x":1}}'
done
```

**Expected:** first ~100 requests return `201`/`200`, subsequent requests within the same window return `429 Too Many Requests`.

### Dead letter queue inspection & replay

```bash
curl -i http://localhost:8080/queues/default/dlq
curl -i -X POST http://localhost:8080/tasks/b3f1c2a4-...-9e21/requeue
```

## Configuration

All via environment variables (see `internal/config/config.go` for defaults):

| Variable | Default | Description |
|---|---|---|
| `DB_DSN` | `postgres://taskqueue:taskqueue@localhost:5432/taskqueue?sslmode=disable` | Postgres connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `HTTP_PORT` | `8080` | Producer API port |
| `WORKER_CONCURRENCY` | `10` | Max concurrent tasks per worker process |
| `RATE_LIMIT_PER_SEC` | `50` | Worker-side broker rate limit (dequeue throttling) |
| `HEARTBEAT_INTERVAL_SEC` | `10` | Heartbeat renewal frequency |
| `VISIBILITY_TIMEOUT_SEC` | `30` | Time before an unacked task is considered crashed |
| `TASK_TIMEOUT_SEC` | `60` | Per-task handler execution deadline |
| `SHUTDOWN_TIMEOUT_SEC` | `30` | Grace period for in-flight tasks on SIGTERM |
| `RECLAIM_INTERVAL_SEC` | `15` | Crashed-worker scan frequency |
| `PROMOTE_INTERVAL_SEC` | `5` | Delayed(backoff)-task promotion frequency |
| `QUEUE_NAME` | `default` | Queue this worker process consumes |
| `MAX_BODY_BYTES` | `1048576` (1MB) | Producer API max request body size |
| `API_RATE_LIMIT_PER_SEC` | `50` | Producer API per-IP request rate |
| `API_RATE_LIMIT_BURST` | `100` | Producer API per-IP burst capacity |

## Adding a Job Handler

Implement `domain.JobHandler` and register it in `cmd/worker/main.go`:

```go
type EmailHandler struct{}

func (h *EmailHandler) QueueName() string { return "emails" }

func (h *EmailHandler) Handle(ctx context.Context, payload []byte) error {
    // business logic — return error to trigger retry/backoff,
    // or domain.NewRetryableError(err) to force-retry a normally-fatal error.
    // ctx is already bounded by TASK_TIMEOUT_SEC; respect ctx.Done() in any
    // blocking I/O (HTTP calls, DB writes) to fail fast on timeout.
    return nil
}
```

Scale to multiple queues by registering additional handlers via `engine.HandlerRegistry` and running one `worker.Pool` per queue (either as separate goroutines in one process, e.g. `default` + `emails`, or separate `cmd/worker` deployments differentiated by `QUEUE_NAME`).

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
  api/             HTTP handlers, routing, and security middleware
                    (rate limiting, body size limit, validation)
  config/          Environment-based configuration
migrations/        SQL schema migrations
docker/            Dockerfiles for producer/worker
```

## Scaling Notes

- Run multiple `worker` replicas per queue — `ClaimForProcessing` uses `SKIP LOCKED`, so concurrent claims never collide.
- `ReclaimLoop` and `DelayedPromotionLoop` are safe to run redundantly across replicas (idempotent via atomic row locks / `ZREM` race-checks); no leader election required.
- Postgres remains the source of truth — Redis broker failure/restart does not cause data loss, only temporary dispatch delay until the reclaim loop re-surfaces affected tasks.
- Producer API rate limiting is per-process (in-memory); behind multiple producer replicas, effective global rate = `API_RATE_LIMIT_PER_SEC × replica count` unless fronted by a shared limiter (e.g. an API gateway or Redis-backed limiter).

## License

MIT