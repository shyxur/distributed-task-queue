package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/shyxur/distributed-task-queue/internal/domain"
	"github.com/shyxur/distributed-task-queue/internal/ports"
	"go.uber.org/zap"
)

type Handler struct {
	storage ports.Storage
	broker  ports.Broker
	logger  *zap.Logger
}

func NewHandler(storage ports.Storage, broker ports.Broker, logger *zap.Logger) *Handler {
	return &Handler{storage: storage, broker: broker, logger: logger}
}

type createTaskRequest struct {
	Queue string `json:"queue" validate:"required,max=64"`
	Payload            json.RawMessage `json:"payload" validate:"required"`
	IdempotencyKey     string          `json:"idempotency_key,omitempty" validate:"omitempty,max=128"`
	MaxAttempts        int             `json:"max_attempts,omitempty" validate:"omitempty,gte=1,lte=100"`
	VisibilityTimeoutS int             `json:"visibility_timeout_sec,omitempty" validate:"omitempty,gte=1,lte=86400"`
}

type createTaskResponse struct {
	ID     uuid.UUID `json:"id"`
	Status string    `json:"status"`
}

func (h *Handler) CreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	if errs := validateRequest(&req); errs != nil {
        writeJSON(w, http.StatusBadRequest, map[string]any{
            "error":   "validation failed",
            "details": errs,
        })
        return
    }
	
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = domain.DefaultRetryPolicy().MaxAttempts
	}
	visTimeout := 30 * time.Second
	if req.VisibilityTimeoutS > 0 {
		visTimeout = time.Duration(req.VisibilityTimeoutS) * time.Second
	}

	if req.IdempotencyKey != "" {
		existing, err := h.storage.FindByIdempotencyKey(r.Context(), req.Queue, req.IdempotencyKey)
		if err == nil {
			writeJSON(w, http.StatusOK, createTaskResponse{ID: existing.ID, Status: string(existing.Status)})
			return
		}
		if !errors.Is(err, domain.ErrTaskNotFound) {
			h.logger.Error("create task: idempotency lookup failed", zap.Error(err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}

	task := domain.NewTask(req.Queue, req.Payload, req.IdempotencyKey, req.MaxAttempts, visTimeout)

	if err := h.storage.Create(r.Context(), task); err != nil {
		if errors.Is(err, domain.ErrDuplicateIdempotencyKey) {
			existing, findErr := h.storage.FindByIdempotencyKey(r.Context(), req.Queue, req.IdempotencyKey)
			if findErr == nil {
				writeJSON(w, http.StatusOK, createTaskResponse{ID: existing.ID, Status: string(existing.Status)})
				return
			}
		}
		h.logger.Error("create task: storage create failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if err := h.broker.Enqueue(r.Context(), task); err != nil {
		h.logger.Error("create task: broker enqueue failed", zap.String("task_id", task.ID.String()), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "task persisted but enqueue failed, will be retried by reclaim scan")
		return
	}

	writeJSON(w, http.StatusCreated, createTaskResponse{ID: task.ID, Status: string(task.Status)})
}

func (h *Handler) GetTask(w http.ResponseWriter, r *http.Request, id string) {
	taskID, err := uuid.Parse(id)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id format")
		return
	}
	task, err := h.storage.GetByID(r.Context(), taskID)
	if errors.Is(err, domain.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (h *Handler) ListDeadLetter(w http.ResponseWriter, r *http.Request, queue string) {
	if queue == "" {
		writeError(w, http.StatusBadRequest, "queue name is required")
		return
	}
	tasks, err := h.storage.ListDeadLetter(r.Context(), queue, 50, 0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

func (h *Handler) RequeueDeadLetter(w http.ResponseWriter, r *http.Request, id string) {
	taskID, err := uuid.Parse(id)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid task id format")
		return
	}
	if err := h.storage.Requeue(r.Context(), taskID, time.Now().UTC()); err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	task, err := h.storage.GetByID(r.Context(), taskID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if err := h.broker.Enqueue(r.Context(), task); err != nil {
		writeError(w, http.StatusInternalServerError, "requeued in storage but enqueue failed")
		return
	}
	writeJSON(w, http.StatusOK, createTaskResponse{ID: task.ID, Status: string(task.Status)})
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
    writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}