package api

import (
	"net/http"
	"strings"

	"github.com/shyxur/distributed-task-queue/internal/ports"
	"go.uber.org/zap"
)

func NewRouter(h *Handler, limiter ports.RateLimiter, logger *zap.Logger) http.Handler {
    mux := http.NewServeMux()

	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("POST /tasks", h.CreateTask)

	mux.HandleFunc("/tasks/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/tasks/")
		if path == "" {
			http.NotFound(w, r)
			return
		}

		parts := strings.Split(path, "/")
		if len(parts) == 1 {
			if r.Method == http.MethodGet {
				h.GetTask(w, r, parts[0])
				return
			}
		} else if len(parts) == 2 && parts[1] == "requeue" {
			if r.Method == http.MethodPost {
				h.RequeueDeadLetter(w, r, parts[0])
				return
			}
		}

		http.NotFound(w, r)
	})

	mux.HandleFunc("/queues/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/queues/")
		parts := strings.Split(path, "/")

		if len(parts) == 2 && parts[1] == "dlq" && r.Method == http.MethodGet {
			h.ListDeadLetter(w, r, parts[0])
			return
		}

		http.NotFound(w, r)
	})

	var handler http.Handler = mux
	handler = RateLimitMiddleware(limiter)(handler)
	handler = LoggingMiddleware(logger)(handler)
	handler = RecoveryMiddleware(logger)(handler)

	return handler
}