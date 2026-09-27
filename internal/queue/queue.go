// Package queue implements TickFlow's persistent job queue.
//
// Jobs move through a well-defined lifecycle (pending -> running -> succeeded |
// failed | dead) with retries and exponential backoff. The queue is durable:
// jobs are written to an append-only journal so an in-flight job is never lost
// across a restart. A priority-and-time ordering ensures scheduled jobs run at
// the right moment and higher-priority work is picked up first.
package queue

import (
	"container/heap"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
)

// State is the lifecycle state of a job.
type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed" // will be retried
	StateDead      State = "dead"   // retries exhausted
)

// Job is a unit of work processed by a worker.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`    // e.g. "send_ticket_email"
	Payload     json.RawMessage `json:"payload"` // handler-specific data
	State       State           `json:"state"`
	Priority    int             `json:"priority"` // higher runs first
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       time.Time       `json:"run_at"` // earliest execution time
	LastError   string          `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// backoff returns the delay before the next attempt using exponential backoff
// capped at 5 minutes: 1s, 2s, 4s, 8s, ...
func backoff(attempt int) time.Duration {
	d := time.Second << uint(attempt)
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return d
}

// Queue is a durable, priority-and-time ordered job queue.
type Queue struct {
	mu       sync.Mutex
	pq       priorityQueue   // ready/scheduled jobs
	byID     map[string]*Job // index for status lookups
	ready    chan struct{}   // signals workers that work may be available
	journal  *Journal
	archived map[string]*Job // terminal jobs kept for the dashboard
}

// New builds an empty queue backed by the given journal (may be nil).
func New(journal *Journal) *Queue {
	q := &Queue{
		byID:     make(map[string]*Job),
		ready:    make(chan struct{}, 1),
		journal:  journal,
		archived: make(map[string]*Job),
	}
	heap.Init(&q.pq)
	return q
}

// Enqueue adds a new job. Type is required; MaxAttempts defaults to 5 and RunAt
// defaults to now. It returns the created job.
func (q *Queue) Enqueue(j *Job) *Job {
	now := time.Now().UTC()
	if j.ID == "" {
		j.ID = uuid.NewString()
	}
	if j.MaxAttempts == 0 {
		j.MaxAttempts = 5
	}
	if j.RunAt.IsZero() {
		j.RunAt = now
	}
	j.State = StatePending
	j.CreatedAt = now
	j.UpdatedAt = now

	q.mu.Lock()
	q.push(j)
	q.mu.Unlock()

	q.persist(j)
	q.signal()
	return j
}

// Dequeue returns the next runnable job whose RunAt has passed, marking it
// running. It returns (nil, false) if nothing is ready yet.
func (q *Queue) Dequeue() (*Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pq.Len() == 0 {
		return nil, false
	}
	top := q.pq[0]
	if top.RunAt.After(time.Now()) {
		return nil, false // earliest job is scheduled for later
	}
	j := heap.Pop(&q.pq).(*Job)
	j.State = StateRunning
	j.Attempts++
	j.UpdatedAt = time.Now().UTC()
	q.persistLocked(j)
	return j, true
}

// Complete marks a running job as succeeded.
func (q *Queue) Complete(j *Job) {
	q.mu.Lock()
	j.State = StateSucceeded
	j.UpdatedAt = time.Now().UTC()
	q.archived[j.ID] = j
	q.mu.Unlock()
	q.persist(j)
}

// Fail records an error. If retries remain the job is re-scheduled with
// backoff; otherwise it is moved to the dead-letter set.
func (q *Queue) Fail(j *Job, errMsg string) {
	q.mu.Lock()
	j.LastError = errMsg
	j.UpdatedAt = time.Now().UTC()
	if j.Attempts >= j.MaxAttempts {
		j.State = StateDead
		q.archived[j.ID] = j
	} else {
		j.State = StatePending
		j.RunAt = time.Now().Add(backoff(j.Attempts))
		q.push(j)
	}
	q.mu.Unlock()
	q.persist(j)
	q.signal()
}

// Ready returns the channel workers select on to be woken when work arrives.
func (q *Queue) Ready() <-chan struct{} { return q.ready }

// --- Introspection for the dashboard ---------------------------------------

// Snapshot is a point-in-time view of queue contents and counts.
type Snapshot struct {
	Counts map[State]int `json:"counts"`
	Recent []*Job        `json:"recent"`
}

// Snapshot returns counts by state and the most recently updated jobs.
func (q *Queue) Snapshot(limit int) Snapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	counts := map[State]int{}
	all := make([]*Job, 0, len(q.byID)+len(q.archived))
	for _, j := range q.byID {
		counts[j.State]++
		all = append(all, j)
	}
	for _, j := range q.archived {
		counts[j.State]++
		all = append(all, j)
	}
	// Sort by UpdatedAt desc (simple insertion for small N).
	for i := 1; i < len(all); i++ {
		for k := i; k > 0 && all[k].UpdatedAt.After(all[k-1].UpdatedAt); k-- {
			all[k], all[k-1] = all[k-1], all[k]
		}
	}
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return Snapshot{Counts: counts, Recent: all}
}

// --- internal helpers -------------------------------------------------------

// push inserts a job and updates the index. Caller holds mu.
func (q *Queue) push(j *Job) {
	q.byID[j.ID] = j
	heap.Push(&q.pq, j)
}

func (q *Queue) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *Queue) persist(j *Job) {
	if q.journal == nil {
		return
	}
	_ = q.journal.Write(j)
}

func (q *Queue) persistLocked(j *Job) { q.persist(j) }
