package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// TaskStatus represents lifecycle state of a task.
type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusProcessing TaskStatus = "processing"
	StatusCompleted  TaskStatus = "completed"
	StatusFailed     TaskStatus = "failed"
	StatusDeadLetter TaskStatus = "dead_letter"
)

// Task is the core domain entity. Storage/broker layers map to/from this;
// it must not import infra packages (clean architecture boundary).
type Task struct {
	ID             uuid.UUID       `json:"id"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Status         TaskStatus      `json:"status"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"` // dedupe guard

	// Retry/backoff control
	Attempts    int `json:"attempts"`
	MaxAttempts int `json:"max_attempts"`

	// Visibility timeout: task invisible to other workers until this expires
	VisibilityTimeout time.Duration `json:"visibility_timeout"`
	VisibleAt         time.Time     `json:"visible_at"`

	// Ownership for heartbeat tracking
	LockedBy        string     `json:"locked_by,omitempty"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`

	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ScheduledAt time.Time  `json:"scheduled_at"` // supports delayed execution
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	LastError string `json:"last_error,omitempty"`
}

// NewTask constructs a task with sane defaults. Business rules (validation)
// belong here, not in handlers/repositories.
func NewTask(queue string, payload json.RawMessage, idempotencyKey string, maxAttempts int, visibilityTimeout time.Duration) *Task {
	now := time.Now().UTC()
	return &Task{
		ID:                 uuid.New(),
		Queue:              queue,
		Payload:            payload,
		Status:             StatusPending,
		IdempotencyKey:     idempotencyKey,
		Attempts:           0,
		MaxAttempts:        maxAttempts,
		VisibilityTimeout:  visibilityTimeout,
		VisibleAt:          now,
		CreatedAt:          now,
		UpdatedAt:          now,
		ScheduledAt:        now,
	}
}

// IsExhausted reports whether retry budget is spent → should route to DLQ.
func (t *Task) IsExhausted() bool {
	return t.Attempts >= t.MaxAttempts
}

// MarkProcessing transitions task to in-flight state and extends visibility.
func (t *Task) MarkProcessing(workerID string, now time.Time) {
	t.Status = StatusProcessing
	t.LockedBy = workerID
	t.Attempts++
	t.VisibleAt = now.Add(t.VisibilityTimeout)
	t.LastHeartbeatAt = &now
	t.UpdatedAt = now
}

// ExtendVisibility is called by the heartbeat mechanism to renew the lock.
func (t *Task) ExtendVisibility(now time.Time) {
	t.VisibleAt = now.Add(t.VisibilityTimeout)
	t.LastHeartbeatAt = &now
}

// IsVisibilityExpired determines if a crashed worker's lock should be reclaimed.
func (t *Task) IsVisibilityExpired(now time.Time) bool {
	return t.Status == StatusProcessing && now.After(t.VisibleAt)
}