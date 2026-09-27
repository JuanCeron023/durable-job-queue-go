// Package worker runs a pool of goroutines that pull jobs from the queue and
// execute registered handlers. Handlers are keyed by job type, so TickFlow can
// process many kinds of background work (emails, PDF generation, webhooks)
// through one uniform, retry-aware pipeline.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/portfolio/tickflow/internal/queue"
)

// Handler processes a job payload. Returning an error triggers the queue's
// retry/backoff logic; returning nil marks the job succeeded.
type Handler func(ctx context.Context, payload json.RawMessage) error

// Pool is a set of worker goroutines bound to a queue and handler registry.
type Pool struct {
	q        *queue.Queue
	handlers map[string]Handler
	size     int

	mu        sync.RWMutex
	processed int64
	failed    int64
}

// NewPool creates a pool of the given size over q.
func NewPool(q *queue.Queue, size int) *Pool {
	if size <= 0 {
		size = 4
	}
	return &Pool{q: q, handlers: make(map[string]Handler), size: size}
}

// Register binds a handler to a job type. Not safe to call after Start.
func (p *Pool) Register(jobType string, h Handler) {
	p.handlers[jobType] = h
}

// Start launches the workers. They run until ctx is cancelled.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.size; i++ {
		go p.run(ctx, i)
	}
}

func (p *Pool) run(ctx context.Context, id int) {
	// Poll with a short timer as a fallback so scheduled jobs (RunAt in the
	// future) are picked up promptly even without an explicit signal.
	timer := time.NewTicker(200 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.q.Ready():
			p.drain(ctx)
		case <-timer.C:
			p.drain(ctx)
		}
	}
}

// drain processes all currently-runnable jobs.
func (p *Pool) drain(ctx context.Context) {
	for {
		job, ok := p.q.Dequeue()
		if !ok {
			return
		}
		p.execute(ctx, job)
	}
}

func (p *Pool) execute(ctx context.Context, job *queue.Job) {
	h, ok := p.handlers[job.Type]
	if !ok {
		p.q.Fail(job, fmt.Sprintf("no handler registered for type %q", job.Type))
		p.count(false)
		return
	}
	// Per-job timeout to prevent a stuck handler from blocking a worker.
	jobCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err := safeCall(h, jobCtx, job.Payload)
	if err != nil {
		log.Printf("job %s (%s) failed attempt %d/%d: %v", job.ID, job.Type, job.Attempts, job.MaxAttempts, err)
		p.q.Fail(job, err.Error())
		p.count(false)
		return
	}
	p.q.Complete(job)
	p.count(true)
}

// safeCall runs a handler and converts panics into errors so one bad job can't
// crash a worker goroutine.
func safeCall(h Handler, ctx context.Context, payload json.RawMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h(ctx, payload)
}

func (p *Pool) count(success bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.processed++
	if !success {
		p.failed++
	}
}

// Metrics is a snapshot of pool counters.
type Metrics struct {
	Workers   int   `json:"workers"`
	Processed int64 `json:"processed"`
	Failed    int64 `json:"failed"`
}

// Metrics returns pool statistics for the dashboard.
func (p *Pool) Metrics() Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return Metrics{Workers: p.size, Processed: p.processed, Failed: p.failed}
}
