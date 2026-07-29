package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourorg/taskqueue/internal/domain"
	"github.com/yourorg/taskqueue/internal/ports"
)

type PostgresStorage struct {
	pool *pgxpool.Pool
}

var _ ports.Storage = (*PostgresStorage)(nil)

func NewPostgresStorage(ctx context.Context, dsn string) (*PostgresStorage, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	return &PostgresStorage{pool: pool}, nil
}

func (s *PostgresStorage) Create(ctx context.Context, task *domain.Task) error {
	const q = `
		INSERT INTO tasks (id, queue, payload, status, idempotency_key, attempts, max_attempts,
			visibility_timeout_ms, visible_at, created_at, updated_at, scheduled_at)
		VALUES ($1,$2,$3,$4,NULLIF($5,''),$6,$7,$8,$9,$10,$11,$12)
	`
	_, err := s.pool.Exec(ctx, q,
		task.ID, task.Queue, task.Payload, task.Status, task.IdempotencyKey,
		task.Attempts, task.MaxAttempts, task.VisibilityTimeout.Milliseconds(),
		task.VisibleAt, task.CreatedAt, task.UpdatedAt, task.ScheduledAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			return domain.ErrDuplicateIdempotencyKey
		}
		return fmt.Errorf("postgres: create task: %w", err)
	}
	return nil
}

func (s *PostgresStorage) GetByID(ctx context.Context, id uuid.UUID) (*domain.Task, error) {
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM tasks WHERE id = $1`, taskColumns), id)
	t, err := scanTaskRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrTaskNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get task: %w", err)
	}
	return t, nil
}

func (s *PostgresStorage) FindByIdempotencyKey(ctx context.Context, queue, key string) (*domain.Task, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT %s FROM tasks WHERE queue = $1 AND idempotency_key = $2`, taskColumns),
		queue, key)
	t, err := scanTaskRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrTaskNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: find by idempotency key: %w", err)
	}
	return t, nil
}

// ClaimForProcessing uses SELECT ... FOR UPDATE SKIP LOCKED so concurrent
// workers never double-claim the same row, and never block on contention
// (skip to next candidate instead of waiting).
func (s *PostgresStorage) ClaimForProcessing(ctx context.Context, id uuid.UUID, workerID string, now time.Time) (*domain.Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s FROM tasks
		WHERE id = $1 AND status IN ('pending','processing')
		FOR UPDATE SKIP LOCKED
	`, taskColumns), id)

	t, err := scanTaskRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrTaskAlreadyLocked
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: claim select: %w", err)
	}

	t.MarkProcessing(workerID, now)

	_, err = tx.Exec(ctx, `
		UPDATE tasks SET status=$1, locked_by=$2, attempts=$3, visible_at=$4,
			last_heartbeat_at=$5, updated_at=$6
		WHERE id=$7
	`, t.Status, t.LockedBy, t.Attempts, t.VisibleAt, t.LastHeartbeatAt, now, t.ID)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim update: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: claim commit: %w", err)
	}
	return t, nil
}

func (s *PostgresStorage) Heartbeat(ctx context.Context, id uuid.UUID, workerID string, visibilityTimeout time.Duration, now time.Time) error {
	visibleAt := now.Add(visibilityTimeout)
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks SET visible_at=$1, last_heartbeat_at=$2, updated_at=$2
		WHERE id=$3 AND locked_by=$4 AND status='processing'
	`, visibleAt, now, id, workerID)
	if err != nil {
		return fmt.Errorf("postgres: heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrTaskAlreadyLocked
	}
	return nil
}

func (s *PostgresStorage) Complete(ctx context.Context, id uuid.UUID, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status='completed', completed_at=$1, updated_at=$1
		WHERE id=$2
	`, now, id)
	if err != nil {
		return fmt.Errorf("postgres: complete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrTaskNotFound
	}
	return nil
}

func (s *PostgresStorage) Fail(ctx context.Context, id uuid.UUID, errMsg string, nextStatus domain.TaskStatus, visibleAt time.Time, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status=$1, last_error=$2, visible_at=$3, locked_by=NULL,
			last_heartbeat_at=NULL, updated_at=$4
		WHERE id=$5
	`, nextStatus, errMsg, visibleAt, now, id)
	if err != nil {
		return fmt.Errorf("postgres: fail: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrTaskNotFound
	}
	return nil
}

func (s *PostgresStorage) MoveToDeadLetter(ctx context.Context, id uuid.UUID, errMsg string, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status='dead_letter', last_error=$1, locked_by=NULL,
			last_heartbeat_at=NULL, updated_at=$2
		WHERE id=$3
	`, errMsg, now, id)
	if err != nil {
		return fmt.Errorf("postgres: move to dlq: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrTaskNotFound
	}
	return nil
}

// ReclaimExpired finds processing tasks whose visible_at has lapsed (crashed
// worker never heartbeat'd in time) and atomically resets them to pending.
// FOR UPDATE SKIP LOCKED avoids stepping on concurrent reclaimers.
func (s *PostgresStorage) ReclaimExpired(ctx context.Context, queue string, now time.Time, limit int) ([]*domain.Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: reclaim begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT %s FROM tasks
		WHERE queue=$1 AND status='processing' AND visible_at < $2
		ORDER BY visible_at ASC
		LIMIT $3
		FOR UPDATE SKIP LOCKED
	`, taskColumns), queue, now, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: reclaim select: %w", err)
	}

	var reclaimed []*domain.Task
	var ids []uuid.UUID
	for rows.Next() {
		t, err := scanTaskRow(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: reclaim scan: %w", err)
		}
		reclaimed = append(reclaimed, t)
		ids = append(ids, t.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, tx.Commit(ctx)
	}

	_, err = tx.Exec(ctx, `
		UPDATE tasks SET status='pending', locked_by=NULL, last_heartbeat_at=NULL,
			visible_at=$1, updated_at=$1
		WHERE id = ANY($2)
	`, now, ids)
	if err != nil {
		return nil, fmt.Errorf("postgres: reclaim update: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: reclaim commit: %w", err)
	}
	return reclaimed, nil
}

func (s *PostgresStorage) ListDeadLetter(ctx context.Context, queue string, limit, offset int) ([]*domain.Task, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT %s FROM tasks
		WHERE queue=$1 AND status='dead_letter'
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, taskColumns), queue, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("postgres: list dlq: %w", err)
	}
	defer rows.Close()

	var result []*domain.Task
	for rows.Next() {
		t, err := scanTaskRow(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: list dlq scan: %w", err)
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func (s *PostgresStorage) Requeue(ctx context.Context, id uuid.UUID, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status='pending', attempts=0, locked_by=NULL,
			last_heartbeat_at=NULL, last_error=NULL, visible_at=$1, updated_at=$1
		WHERE id=$2
	`, now, id)
	if err != nil {
		return fmt.Errorf("postgres: requeue: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrTaskNotFound
	}
	return nil
}

func (s *PostgresStorage) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *PostgresStorage) Close() error {
	s.pool.Close()
	return nil
}