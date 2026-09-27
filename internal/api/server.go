// Package api exposes TickFlow's HTTP surface: job submission, status queries,
// and a live snapshot feed for the dashboard.
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/portfolio/tickflow/internal/queue"
	"github.com/portfolio/tickflow/internal/worker"
)

// Server wires the queue and worker pool to HTTP.
type Server struct {
	q    *queue.Queue
	pool *worker.Pool
}

// New builds an API server.
func New(q *queue.Queue, pool *worker.Pool) *Server {
	return &Server{q: q, pool: pool}
}

// Routes registers endpoints and static files.
func (s *Server) Routes(staticDir string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/jobs", s.handleJobs)
	mux.HandleFunc("/api/snapshot", s.handleSnapshot)
	mux.Handle("/", http.FileServer(http.Dir(staticDir)))
	return mux
}

// enqueueRequest is the body for POST /api/jobs.
type enqueueRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	MaxAttempts int             `json:"max_attempts"`
	DelaySecs   int             `json:"delay_secs"` // schedule for the future
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Type == "" {
		http.Error(w, "type is required", http.StatusBadRequest)
		return
	}
	job := &queue.Job{
		Type:        req.Type,
		Payload:     req.Payload,
		Priority:    req.Priority,
		MaxAttempts: req.MaxAttempts,
	}
	if req.DelaySecs > 0 {
		job.RunAt = time.Now().Add(time.Duration(req.DelaySecs) * time.Second)
	}
	created := s.q.Enqueue(job)
	writeJSON(w, http.StatusAccepted, created)
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	snap := s.q.Snapshot(50)
	writeJSON(w, http.StatusOK, struct {
		Queue   queue.Snapshot `json:"queue"`
		Workers worker.Metrics `json:"workers"`
	}{Queue: snap, Workers: s.pool.Metrics()})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
