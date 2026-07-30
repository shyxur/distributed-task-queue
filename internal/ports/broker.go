package ports

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/shyxur/distributed-task-queue/internal/domain"
)

// Broker abstracts the message transport layer (Redis, SQS, Kafka, etc.).
// Engine/worker layers depend on this interface only — never on a concrete
// broker implementation (Dependency Inversion).
type Broker interface {
	// Enqueue pushes a task reference onto the queue for dispatch.
	Enqueue(ctx context.Context, task *domain.Task) error

	// Dequeue blocks (up to the given timeout) waiting for a task ID to
	// become available on the queue. Returns domain.ErrQueueEmpty on timeout.
	Dequeue(ctx context.Context, queue string, timeout time.Duration) (uuid.UUID, error)

	// Ack confirms successful processing; removes any in-flight tracking.
	Ack(ctx context.Context, queue string, taskID uuid.UUID) error

	// Nack signals failed processing; broker may requeue immediately or
	// leave it for the visibility-timeout reclaimer, depending on delay.
	Nack(ctx context.Context, queue string, taskID uuid.UUID, delay time.Duration) error

	// EnqueueDelayed schedules a task to become visible after `delay`
	// (used for backoff retries).
	EnqueueDelayed(ctx context.Context, task *domain.Task, delay time.Duration) error

	// MoveToDeadLetter routes an exhausted task to its DLQ.
	MoveToDeadLetter(ctx context.Context, queue string, taskID uuid.UUID) error

	// PromoteDueDelayed moves delayed tasks whose ready-time has passed
	// back onto the active queue. Called periodically by a scheduler loop.
	PromoteDueDelayed(ctx context.Context, queue string) (int, error)

	// QueueDepth returns the number of pending tasks — used for
	// rate-limiting/backpressure decisions and metrics.
	QueueDepth(ctx context.Context, queue string) (int64, error)

	// Close releases underlying connections gracefully.
	Close() error
}

// RateLimiter abstracts per-worker throughput control. Kept separate from
// Broker so it can be swapped (token bucket, sliding window) independently.
type RateLimiter interface {
	// Allow returns true if the caller may proceed immediately.
	Allow(ctx context.Context, key string) (bool, error)
	// Wait blocks until a token is available or ctx is cancelled.
	Wait(ctx context.Context, key string) error
}