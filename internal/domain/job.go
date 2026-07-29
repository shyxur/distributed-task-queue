package domain

import (
	"context"
	"time"
)

// JobHandler is the contract user-defined task processors implement.
// Kept infra-agnostic: engine calls Handle, handler doesn't know about
// broker/storage internals.
type JobHandler interface {
	Handle(ctx context.Context, payload []byte) error
	QueueName() string
}

// RetryPolicy defines backoff behavior — engine layer consumes this,
// domain only exposes the contract.
type RetryPolicy struct {
	MaxAttempts       int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
}

// NextBackoff computes exponential backoff with cap, given attempt count.
func (r RetryPolicy) NextBackoff(attempt int) time.Duration {
	backoff := float64(r.InitialBackoff)
	for i := 0; i < attempt; i++ {
		backoff *= r.BackoffMultiplier
		if backoff > float64(r.MaxBackoff) {
			return r.MaxBackoff
		}
	}
	return time.Duration(backoff)
}

// DefaultRetryPolicy provides sane production defaults.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:       5,
		InitialBackoff:    2 * time.Second,
		MaxBackoff:        5 * time.Minute,
		BackoffMultiplier: 2.0,
	}
}

// WorkerConfig controls per-worker concurrency & rate limiting —
// domain-level contract; engine/worker layer implements enforcement.
type WorkerConfig struct {
	WorkerID          string
	Concurrency       int
	RateLimitPerSec   int
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration // grace period for SIGTERM
}