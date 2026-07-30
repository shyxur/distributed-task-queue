package engine

import (
	"fmt"
	"sync"

	"github.com/shyxur/distributed-task-queue/internal/domain"
)

// HandlerRegistry maps queue name -> JobHandler. Concurrency-safe for
// dynamic registration (rare) and lookup (hot path).
type HandlerRegistry struct {
	mu       sync.RWMutex
	handlers map[string]domain.JobHandler
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: make(map[string]domain.JobHandler)}
}

func (r *HandlerRegistry) Register(h domain.JobHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[h.QueueName()]; exists {
		return fmt.Errorf("handler already registered for queue %q", h.QueueName())
	}
	r.handlers[h.QueueName()] = h
	return nil
}

func (r *HandlerRegistry) Get(queue string) (domain.JobHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[queue]
	return h, ok
}

func (r *HandlerRegistry) Queues() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	queues := make([]string, 0, len(r.handlers))
	for q := range r.handlers {
		queues = append(queues, q)
	}
	return queues
}