package api

import (
	"net/http"
	"strings"
)

// NewRouter builds a minimal stdlib-only mux (no external router dep needed
// for this surface area). Swap for chi/gin later without touching handlers.
func NewRouter(h *Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("POST /tasks", h.CreateTask)

	mux.HandleFunc("GET /tasks/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/tasks/")
		if strings.HasSuffix(id, "/requeue") {
			return // handled below
		}
		h.GetTask(w, r, id)
	})
	mux.HandleFunc("POST /tasks/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/tasks/")
		id = strings.TrimSuffix(id, "/requeue")
		h.RequeueDeadLetter(w, r, id)
	})
	mux.HandleFunc("GET /queues/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/queues/")
		queue := strings.TrimSuffix(path, "/dlq")
		h.ListDeadLetter(w, r, queue)
	})

	return loggingMiddleware(mux)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
}