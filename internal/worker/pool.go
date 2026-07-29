package worker

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yourorg/taskqueue/internal/domain"
	"github.com/yourorg/taskqueue/internal/engine"
	"github.com/yourorg/taskqueue/internal/ports"
	"go.uber.org/zap"
)

// Pool manages a fixed-concurrency set of task-processing goroutines for
// one queue, with rate limiting, heartbeating in-flight tasks, and
// graceful shutdown (drains in-flight work, rejects new dequeues).
type Pool struct {
	cfg      domain.WorkerConfig
	queue    string
	broker   ports.Broker
	storage  ports.Storage
	engine   *engine.Engine
	handler  domain.JobHandler
	limiter  ports.RateLimiter
	logger   *zap.Logger

	sem       chan struct{}    // concurrency gate
	inFlight  sync.Map         // taskID -> context.CancelFunc (for heartbeat-loss cancellation)
	wg        sync.WaitGroup
	shutdown  chan struct{}
	shutdownOnce sync.Once
}

func NewPool(cfg domain.WorkerConfig, queue string, broker ports.Broker, storage ports.Storage, eng *engine.Engine, handler domain.JobHandler, limiter ports.RateLimiter, logger *zap.Logger) *Pool {
	return &Pool{
		cfg:      cfg,
		queue:    queue,
		broker:   broker,
		storage:  storage,
		engine:   eng,
		handler:  handler,
		limiter:  limiter,
		logger:   logger,
		sem:      make(chan struct{}, cfg.Concurrency),
		shutdown: make(chan struct{}),
	}
}

// Run is the main dispatch loop: blocks dequeueing from the broker and
// dispatching to worker goroutines until ctx is cancelled (SIGTERM), then
// drains in-flight work up to ShutdownTimeout before returning.
func (p *Pool) Run(ctx context.Context) {
	p.logger.Info("worker pool starting", zap.String("queue", p.queue), zap.String("worker_id", p.cfg.WorkerID), zap.Int("concurrency", p.cfg.Concurrency))

	dispatchLoop:
	for {
		select {
		case <-ctx.Done():
			break dispatchLoop
		default:
		}

		// Rate limit before even attempting a dequeue — backpressure at the source.
		if p.limiter != nil {
			if err := p.limiter.Wait(ctx, p.cfg.WorkerID); err != nil {
				if ctx.Err() != nil {
					break dispatchLoop
				}
				continue
			}
		}

		// Acquire concurrency slot BEFORE dequeue so we never hold a claimed
		// task without capacity to run it.
		select {
		case p.sem <- struct{}{}:
		case <-ctx.Done():
			break dispatchLoop
		}

		taskID, err := p.broker.Dequeue(ctx, p.queue, 2*time.Second)
		if err != nil {
			<-p.sem // release slot, nothing to do
			if ctx.Err() != nil {
				break dispatchLoop
			}
			continue // timeout / queue empty, loop again
		}

		p.wg.Add(1)
		go p.process(ctx, taskID)
	}

	p.gracefulDrain()
}

// process claims, executes (with heartbeat loop), and releases the slot.
func (p *Pool) process(parentCtx context.Context, taskID uuid.UUID) {
	defer p.wg.Done()
	defer func() { <-p.sem }()

	// Detach task lifetime from dispatch-loop ctx cancellation so an
	// in-flight task can finish gracefully during shutdown; ShutdownTimeout
	// is enforced separately in gracefulDrain via wg.Wait timeout.
	taskCtx, cancel := context.WithCancel(context.Background())
	p.inFlight.Store(taskID, cancel)
	defer func() {
		p.inFlight.Delete(taskID)
		cancel()
	}()

	now := time.Now().UTC()
	task, err := p.storage.ClaimForProcessing(taskCtx, taskID, p.cfg.WorkerID, now)
	if err != nil {
		p.logger.Warn("worker: claim failed", zap.String("task_id", taskID.String()), zap.Error(err))
		return
	}

	hbCtx, hbCancel := context.WithCancel(taskCtx)
	defer hbCancel()
	go p.heartbeatLoop(hbCtx, task)

	result := p.engine.Execute(taskCtx, task, p.cfg.WorkerID, p.handler)
	if result.Err != nil && result.Outcome != "dead_letter" {
		p.logger.Info("worker: task failed, will retry",
			zap.String("task_id", taskID.String()), zap.Error(result.Err))
	}
}

// heartbeatLoop periodically renews visibility while the task is in-flight.
// If a heartbeat fails with ErrTaskAlreadyLocked, another worker has
// reclaimed this task (we exceeded visibility timeout) — cancel local
// execution to avoid duplicate side effects racing with the new owner.
func (p *Pool) heartbeatLoop(ctx context.Context, task *domain.Task) {
	ticker := time.NewTicker(p.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			err := p.storage.Heartbeat(ctx, task.ID, p.cfg.WorkerID, task.VisibilityTimeout, now)
			if err != nil {
				p.logger.Error("worker: heartbeat lost ownership, cancelling task",
					zap.String("task_id", task.ID.String()), zap.Error(err))
				if cancelAny, ok := p.inFlight.Load(task.ID); ok {
					cancelAny.(context.CancelFunc)()
				}
				return
			}
		}
	}
}

// gracefulDrain waits for in-flight tasks to finish, bounded by
// cfg.ShutdownTimeout. Tasks still running after the deadline are forcibly
// cancelled (their handler should respect ctx cancellation) and left for
// the visibility-timeout reclaimer to pick up on another worker.
func (p *Pool) gracefulDrain() {
	p.logger.Info("worker pool draining", zap.String("worker_id", p.cfg.WorkerID), zap.Duration("timeout", p.cfg.ShutdownTimeout))

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("worker pool drained cleanly")
	case <-time.After(p.cfg.ShutdownTimeout):
		p.logger.Warn("worker pool shutdown timeout exceeded, force-cancelling in-flight tasks")
		p.inFlight.Range(func(key, value any) bool {
			value.(context.CancelFunc)()
			return true
		})
		<-done // wait for goroutines to actually exit after cancellation
	}
	p.shutdownOnce.Do(func() { close(p.shutdown) })
}

// Done returns a channel closed once the pool has fully shut down.
func (p *Pool) Done() <-chan struct{} {
	return p.shutdown
}