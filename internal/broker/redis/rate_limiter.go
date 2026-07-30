package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shyxur/distributed-task-queue/internal/ports"
)

// TokenBucketLimiter implements per-worker rate limiting via a Lua script
// for atomicity (read-check-decrement must be a single round trip).
type TokenBucketLimiter struct {
	client     *redis.Client
	ratePerSec int
	burst      int
}

var _ ports.RateLimiter = (*TokenBucketLimiter)(nil)

func NewTokenBucketLimiter(client *redis.Client, ratePerSec, burst int) *TokenBucketLimiter {
	return &TokenBucketLimiter{client: client, ratePerSec: ratePerSec, burst: burst}
}

// tokenBucketScript: refills based on elapsed time since last access,
// caps at burst, consumes 1 token if available.
var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])

if tokens == nil then
  tokens = burst
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(burst, tokens + (elapsed * rate))

local allowed = 0
if tokens >= 1 then
  allowed = 1
  tokens = tokens - 1
end

redis.call("HMSET", key, "tokens", tokens, "ts", now)
redis.call("EXPIRE", key, 60)

return allowed
`)

func (l *TokenBucketLimiter) Allow(ctx context.Context, key string) (bool, error) {
	now := float64(time.Now().UnixMilli()) / 1000.0
	res, err := tokenBucketScript.Run(ctx, l.client, []string{fmt.Sprintf("tq:ratelimit:%s", key)},
		l.ratePerSec, l.burst, now).Int()
	if err != nil {
		return false, fmt.Errorf("rate limiter: %w", err)
	}
	return res == 1, nil
}

func (l *TokenBucketLimiter) Wait(ctx context.Context, key string) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := l.Allow(ctx, key)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}