package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/shyxur/distributed-task-queue/internal/domain"
	"github.com/shyxur/distributed-task-queue/internal/ports"
)

// Key layout:
//   tq:{queue}:pending      -> LIST (queue of task IDs, BRPOPLPUSH source)
//   tq:{queue}:processing   -> LIST (in-flight staging, BRPOPLPUSH dest)
//   tq:{queue}:delayed      -> ZSET (score = ready unix ts, member = task ID)
//   tq:{queue}:dlq          -> LIST (dead letter task IDs)
//   tq:{queue}:inflight:{id}-> STRING marker w/ TTL, used to detect stuck items

type RedisBroker struct {
	client *redis.Client
}

var _ ports.Broker = (*RedisBroker)(nil)

func NewRedisBroker(addr, password string, db int) *RedisBroker {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &RedisBroker{client: client}
}

func pendingKey(queue string) string    { return fmt.Sprintf("tq:%s:pending", queue) }
func processingKey(queue string) string { return fmt.Sprintf("tq:%s:processing", queue) }
func delayedKey(queue string) string    { return fmt.Sprintf("tq:%s:delayed", queue) }
func dlqKey(queue string) string        { return fmt.Sprintf("tq:%s:dlq", queue) }

func (b *RedisBroker) Enqueue(ctx context.Context, task *domain.Task) error {
	return b.client.LPush(ctx, pendingKey(task.Queue), task.ID.String()).Err()
}

func (b *RedisBroker) Dequeue(ctx context.Context, queue string, timeout time.Duration) (uuid.UUID, error) {
	// BRPOPLPUSH: atomically move ID from pending -> processing staging list.
	// The processing list acts as a safety net; ReclaimExpired (storage-side,
	// source of truth) handles actual visibility-timeout logic, but this
	// staging list lets us detect broker-level crashes too if needed later.
	res, err := b.client.BRPopLPush(ctx, pendingKey(queue), processingKey(queue), timeout).Result()
	if errors.Is(err, redis.Nil) {
		return uuid.Nil, domain.ErrQueueEmpty
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("redis dequeue: %w", err)
	}
	id, err := uuid.Parse(res)
	if err != nil {
		return uuid.Nil, fmt.Errorf("redis dequeue: invalid task id %q: %w", res, err)
	}
	return id, nil
}

func (b *RedisBroker) Ack(ctx context.Context, queue string, taskID uuid.UUID) error {
	return b.client.LRem(ctx, processingKey(queue), 1, taskID.String()).Err()
}

func (b *RedisBroker) Nack(ctx context.Context, queue string, taskID uuid.UUID, delay time.Duration) error {
	pipe := b.client.TxPipeline()
	pipe.LRem(ctx, processingKey(queue), 1, taskID.String())
	if delay <= 0 {
		pipe.LPush(ctx, pendingKey(queue), taskID.String())
	} else {
		score := float64(time.Now().Add(delay).Unix())
		pipe.ZAdd(ctx, delayedKey(queue), redis.Z{Score: score, Member: taskID.String()})
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (b *RedisBroker) EnqueueDelayed(ctx context.Context, task *domain.Task, delay time.Duration) error {
	score := float64(time.Now().Add(delay).Unix())
	return b.client.ZAdd(ctx, delayedKey(task.Queue), redis.Z{
		Score:  score,
		Member: task.ID.String(),
	}).Err()
}

func (b *RedisBroker) MoveToDeadLetter(ctx context.Context, queue string, taskID uuid.UUID) error {
	pipe := b.client.TxPipeline()
	pipe.LRem(ctx, processingKey(queue), 1, taskID.String())
	pipe.ZRem(ctx, delayedKey(queue), taskID.String())
	pipe.LPush(ctx, dlqKey(queue), taskID.String())
	_, err := pipe.Exec(ctx)
	return err
}

// PromoteDueDelayed moves ready delayed tasks back to pending. Uses ZRANGEBYSCORE
// + atomic removal per member to avoid double-promotion races across replicas
// running this loop concurrently.
func (b *RedisBroker) PromoteDueDelayed(ctx context.Context, queue string) (int, error) {
	now := float64(time.Now().Unix())
	ids, err := b.client.ZRangeByScore(ctx, delayedKey(queue), &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprintf("%f", now), Count: 100,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("redis promote delayed: %w", err)
	}
	promoted := 0
	for _, id := range ids {
		// ZREM returns 1 only if this instance won the race to remove it.
		removed, err := b.client.ZRem(ctx, delayedKey(queue), id).Result()
		if err != nil {
			return promoted, err
		}
		if removed == 0 {
			continue // another process already promoted it
		}
		if err := b.client.LPush(ctx, pendingKey(queue), id).Err(); err != nil {
			return promoted, err
		}
		promoted++
	}
	return promoted, nil
}

func (b *RedisBroker) QueueDepth(ctx context.Context, queue string) (int64, error) {
	return b.client.LLen(ctx, pendingKey(queue)).Result()
}

func (b *RedisBroker) Close() error {
	return b.client.Close()
}

func (b *RedisBroker) Client() *redis.Client {
	return b.client
}