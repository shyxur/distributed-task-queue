package postgres

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/yourorg/taskqueue/internal/domain"
)

// taskRow mirrors the tasks table for scanning; separates DB shape from
// domain.Task (e.g. visibility_timeout stored as ms, domain uses Duration).
type taskRow struct {
	ID                  uuid.UUID
	Queue               string
	Payload             json.RawMessage
	Status              string
	IdempotencyKey      sql.NullString
	Attempts            int
	MaxAttempts         int
	VisibilityTimeoutMs int64
	VisibleAt           time.Time
	LockedBy            sql.NullString
	LastHeartbeatAt     sql.NullTime
	CreatedAt           time.Time
	UpdatedAt           time.Time
	ScheduledAt         time.Time
	CompletedAt         sql.NullTime
	LastError           sql.NullString
}

func (r *taskRow) toDomain() *domain.Task {
	t := &domain.Task{
		ID:                 r.ID,
		Queue:              r.Queue,
		Payload:            r.Payload,
		Status:             domain.TaskStatus(r.Status),
		Attempts:           r.Attempts,
		MaxAttempts:        r.MaxAttempts,
		VisibilityTimeout:  time.Duration(r.VisibilityTimeoutMs) * time.Millisecond,
		VisibleAt:          r.VisibleAt,
		CreatedAt:          r.CreatedAt,
		UpdatedAt:          r.UpdatedAt,
		ScheduledAt:        r.ScheduledAt,
	}
	if r.IdempotencyKey.Valid {
		t.IdempotencyKey = r.IdempotencyKey.String
	}
	if r.LockedBy.Valid {
		t.LockedBy = r.LockedBy.String
	}
	if r.LastHeartbeatAt.Valid {
		ts := r.LastHeartbeatAt.Time
		t.LastHeartbeatAt = &ts
	}
	if r.CompletedAt.Valid {
		ts := r.CompletedAt.Time
		t.CompletedAt = &ts
	}
	if r.LastError.Valid {
		t.LastError = r.LastError.String
	}
	return t
}

const taskColumns = `
	id, queue, payload, status, idempotency_key, attempts, max_attempts,
	visibility_timeout_ms, visible_at, locked_by, last_heartbeat_at,
	created_at, updated_at, scheduled_at, completed_at, last_error
`

// scanTaskRow scans a single row using the column order in taskColumns.
func scanTaskRow(scanner interface{ Scan(...any) error }) (*domain.Task, error) {
	var r taskRow
	err := scanner.Scan(
		&r.ID, &r.Queue, &r.Payload, &r.Status, &r.IdempotencyKey, &r.Attempts, &r.MaxAttempts,
		&r.VisibilityTimeoutMs, &r.VisibleAt, &r.LockedBy, &r.LastHeartbeatAt,
		&r.CreatedAt, &r.UpdatedAt, &r.ScheduledAt, &r.CompletedAt, &r.LastError,
	)
	if err != nil {
		return nil, err
	}
	return r.toDomain(), nil
}