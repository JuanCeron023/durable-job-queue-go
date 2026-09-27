package queue

import (
	"testing"
	"time"
)

func TestEnqueueDequeueComplete(t *testing.T) {
	q := New(nil)
	q.Enqueue(&Job{Type: "x", MaxAttempts: 3})

	j, ok := q.Dequeue()
	if !ok {
		t.Fatal("expected a runnable job")
	}
	if j.State != StateRunning || j.Attempts != 1 {
		t.Fatalf("expected running/attempt=1, got %s/%d", j.State, j.Attempts)
	}
	q.Complete(j)
	if j.State != StateSucceeded {
		t.Fatalf("expected succeeded, got %s", j.State)
	}
}

func TestRetryThenDeadLetter(t *testing.T) {
	q := New(nil)
	q.Enqueue(&Job{Type: "x", MaxAttempts: 2})

	// First failure -> should be re-scheduled (pending), not dead.
	j, _ := q.Dequeue()
	q.Fail(j, "boom")
	if j.State != StatePending {
		t.Fatalf("after 1st failure expected pending, got %s", j.State)
	}
	// Its RunAt is in the future due to backoff; force it runnable.
	q.mu.Lock()
	for _, job := range q.byID {
		job.RunAt = time.Now().Add(-time.Second)
	}
	q.mu.Unlock()

	// Second failure -> retries exhausted -> dead.
	j2, ok := q.Dequeue()
	if !ok {
		t.Fatal("expected job to be runnable after backoff override")
	}
	q.Fail(j2, "boom again")
	if j2.State != StateDead {
		t.Fatalf("after maxattempts expected dead, got %s", j2.State)
	}
}

func TestPriorityOrdering(t *testing.T) {
	q := New(nil)
	now := time.Now().Add(-time.Second)
	q.Enqueue(&Job{Type: "low", Priority: 1, RunAt: now})
	q.Enqueue(&Job{Type: "high", Priority: 10, RunAt: now})

	j, _ := q.Dequeue()
	if j.Type != "high" {
		t.Fatalf("expected high-priority job first, got %s", j.Type)
	}
}

func TestScheduledJobNotReadyYet(t *testing.T) {
	q := New(nil)
	q.Enqueue(&Job{Type: "future", RunAt: time.Now().Add(time.Hour)})
	if _, ok := q.Dequeue(); ok {
		t.Fatal("scheduled job should not be runnable yet")
	}
}
