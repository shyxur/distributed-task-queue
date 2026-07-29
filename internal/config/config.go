package config

import (
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	DBDSN             string
	RedisAddr         string
	RedisPassword     string
	RedisDB           int
	HTTPPort          string
	WorkerConcurrency int
	RateLimitPerSec   int
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration
	VisibilityTimeout time.Duration
	TaskTimeout       time.Duration
	ReclaimInterval   time.Duration
	PromoteInterval   time.Duration
	QueueName         string
}

func Load() *Config {
	v := viper.New()
	v.SetEnvPrefix("")
	v.AutomaticEnv()

	v.SetDefault("DB_DSN", "postgres://taskqueue:taskqueue@localhost:5432/taskqueue?sslmode=disable")
	v.SetDefault("REDIS_ADDR", "localhost:6379")
	v.SetDefault("REDIS_PASSWORD", "")
	v.SetDefault("REDIS_DB", 0)
	v.SetDefault("HTTP_PORT", "8080")
	v.SetDefault("WORKER_CONCURRENCY", 10)
	v.SetDefault("RATE_LIMIT_PER_SEC", 50)
	v.SetDefault("HEARTBEAT_INTERVAL_SEC", 10)
	v.SetDefault("SHUTDOWN_TIMEOUT_SEC", 30)
	v.SetDefault("VISIBILITY_TIMEOUT_SEC", 30)
	v.SetDefault("TASK_TIMEOUT_SEC", 60)
	v.SetDefault("RECLAIM_INTERVAL_SEC", 15)
	v.SetDefault("PROMOTE_INTERVAL_SEC", 5)
	v.SetDefault("QUEUE_NAME", "default")

	return &Config{
		DBDSN:             v.GetString("DB_DSN"),
		RedisAddr:         v.GetString("REDIS_ADDR"),
		RedisPassword:     v.GetString("REDIS_PASSWORD"),
		RedisDB:           v.GetInt("REDIS_DB"),
		HTTPPort:          v.GetString("HTTP_PORT"),
		WorkerConcurrency: v.GetInt("WORKER_CONCURRENCY"),
		RateLimitPerSec:   v.GetInt("RATE_LIMIT_PER_SEC"),
		HeartbeatInterval: time.Duration(v.GetInt("HEARTBEAT_INTERVAL_SEC")) * time.Second,
		ShutdownTimeout:   time.Duration(v.GetInt("SHUTDOWN_TIMEOUT_SEC")) * time.Second,
		VisibilityTimeout: time.Duration(v.GetInt("VISIBILITY_TIMEOUT_SEC")) * time.Second,
		TaskTimeout:       time.Duration(v.GetInt("TASK_TIMEOUT_SEC")) * time.Second,
		ReclaimInterval:   time.Duration(v.GetInt("RECLAIM_INTERVAL_SEC")) * time.Second,
		PromoteInterval:   time.Duration(v.GetInt("PROMOTE_INTERVAL_SEC")) * time.Second,
		QueueName:         v.GetString("QUEUE_NAME"),
	}
}