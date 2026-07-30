package main

import (
	"context"
	"net/http"
	"time"

	"github.com/shyxur/distributed-task-queue/internal/api"
	redisbroker "github.com/shyxur/distributed-task-queue/internal/broker/redis"
	"github.com/shyxur/distributed-task-queue/internal/config"
	"github.com/shyxur/distributed-task-queue/internal/storage/postgres"
	"go.uber.org/zap"
)

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

	limiter := redisbroker.NewTokenBucketLimiter(broker.Client(), 2, 5)
    handler := api.NewHandler(storage, broker, logger)
    router := api.NewRouter(handler, limiter, logger)

	srv := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	logger.Info("producer HTTP API starting", zap.String("port", cfg.HTTPPort))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("http server failed", zap.Error(err))
	}
}