package queue

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Journal is an append-only durability log for jobs. Each line is a JSON
// snapshot of a job at a state transition. On startup, replaying the journal
// reconstructs the queue: the last record for each job id wins, so completed
// jobs stay completed and in-flight jobs are re-queued.
type Journal struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// OpenJournal opens or creates the journal file at path.
func OpenJournal(path string) (*Journal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Journal{f: f, w: bufio.NewWriter(f)}, nil
}

// Write appends a JSON snapshot of j and flushes.
func (jr *Journal) Write(j *Job) error {
	jr.mu.Lock()
	defer jr.mu.Unlock()
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if _, err := jr.w.Write(append(b, '\n')); err != nil {
		return err
	}
	return jr.w.Flush()
}

// Close flushes and closes the journal.
func (jr *Journal) Close() error {
	jr.mu.Lock()
	defer jr.mu.Unlock()
	_ = jr.w.Flush()
	return jr.f.Close()
}

// Replay reconstructs queue state from the journal. Jobs left in a non-terminal
// state (pending/running) are re-enqueued as pending so they run again; a job
// that was running at crash time is retried, giving at-least-once semantics.
func (jr *Journal) Replay(q *Queue) error {
	if _, err := jr.f.Seek(0, 0); err != nil {
		return err
	}
	latest := make(map[string]*Job)
	sc := bufio.NewScanner(jr.f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var j Job
		if err := json.Unmarshal(sc.Bytes(), &j); err != nil {
			continue
		}
		cp := j
		latest[j.ID] = &cp
	}
	for _, j := range latest {
		switch j.State {
		case StateSucceeded, StateDead:
			q.archived[j.ID] = j
		default:
			// Re-run anything that wasn't terminal.
			j.State = StatePending
			if j.RunAt.IsZero() {
				j.RunAt = time.Now()
			}
			q.push(j)
		}
	}
	if _, err := jr.f.Seek(0, 2); err != nil {
		return err
	}
	q.signal()
	return nil
}
