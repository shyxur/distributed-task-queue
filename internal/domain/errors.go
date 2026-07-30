package domain

import "errors"

// Sentinel errors — infra layers wrap these, callers use errors.Is.
var (
	ErrTaskNotFound            = errors.New("task not found")
	ErrDuplicateIdempotencyKey = errors.New("task with idempotency key already exists")
	ErrTaskAlreadyLocked       = errors.New("task is locked by another worker")
	ErrVisibilityExpired       = errors.New("task visibility timeout expired")
	ErrQueueEmpty              = errors.New("queue is empty")
	ErrInvalidPayload          = errors.New("invalid task payload")
	ErrMaxAttemptsReached      = errors.New("max retry attempts reached, routing to DLQ")
	ErrShutdownInProgress      = errors.New("worker is shutting down, rejecting new work")
	ErrRateLimitExceeded       = errors.New("rate limit exceeded for worker")
	ErrInvalidInput            = errors.New("invalid input parameters")
)

// RetryableError wraps an error to signal engine should retry.
// Non-wrapped errors from handlers are treated as fatal (→ DLQ immediately).
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

func NewRetryableError(err error) *RetryableError {
	return &RetryableError{Err: err}
}