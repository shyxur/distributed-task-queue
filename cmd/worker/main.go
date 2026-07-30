package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	redisbroker "github.com/shyxur/distributed-task-queue/internal/broker/redis"
	"github.com/shyxur/distributed-task-queue/internal/config"
	"github.com/shyxur/distributed-task-queue/internal/domain"
	"github.com/shyxur/distributed-task-queue/internal/engine"
	"github.com/shyxur/distributed-task-queue/internal/storage/postgres"
	"github.com/shyxur/distributed-task-queue/internal/worker"
	"go.uber.org/zap"
)

// exampleHandler is a placeholder job handler — replace with real business
// logic per queue. Register additional handlers via engine.HandlerRegistry
// if you run multiple queues in one process.
type exampleHandler struct {
	queue string
}

func (h *exampleHandler) QueueName() string { return h.queue }

func (h *exampleHandler) Handle(ctx context.Context, payload []byte) error {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return fmt.Errorf("invalid payload: %w", err) // fatal: won't fix itself on retry, but still consumes retry budget
	}
	// TODO: business logic here.
	return nil
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()
	ctx := context.Background()

	storage, err := postgres.NewPostgresStorage(ctx, cfg.DBDSN)
	if err != nil {
		logger.Fatal("failed to connect to postgres", zap.Error(err))
	}
	defer storage.Close()

	broker := redisbroker.NewRedisBroker(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer broker.Close()

	limiter := redisbroker.NewTokenBucketLimiter(broker.Client(), cfg.RateLimitPerSec, cfg.RateLimitPerSec*2)

	retryPolicy := domain.DefaultRetryPolicy()
	eng := engine.NewEngine(storage, broker, retryPolicy, cfg.TaskTimeout, logger)

	handler := &exampleHandler{queue: cfg.QueueName}

	workerCfg := domain.WorkerConfig{
		WorkerID:          fmt.Sprintf("worker-%s", uuid.New().String()[:8]),
		Concurrency:       cfg.WorkerConcurrency,
		RateLimitPerSec:   cfg.RateLimitPerSec,
		HeartbeatInterval: cfg.HeartbeatInterval,
		ShutdownTimeout:   cfg.ShutdownTimeout,
	}

	pool := worker.NewPool(workerCfg, cfg.QueueName, broker, storage, eng, handler, limiter, logger)

	runCtx, stop := worker.ContextWithSignals(ctx, logger)
	defer stop()

	// Background loops: crashed-worker reclaim + delayed(backoff) promotion.
	go eng.ReclaimLoop(runCtx, cfg.QueueName, cfg.ReclaimInterval)
	go eng.DelayedPromotionLoop(runCtx, cfg.QueueName, cfg.PromoteInterval)

	logger.Info("worker starting", zap.String("worker_id", workerCfg.WorkerID), zap.String("queue", cfg.QueueName))
	pool.Run(runCtx) // blocks until drained after SIGTERM
	logger.Info("worker exited cleanly")
}