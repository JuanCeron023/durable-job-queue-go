// Command tickflow runs the TickFlow background-job engine: a durable,
// retrying, priority-and-schedule-aware worker system with a live dashboard.
//
// The concrete use case baked into the demo handlers is a small event venue
// selling tickets: when a ticket is sold, the site enqueues a
// "send_ticket_email" job. TickFlow delivers it, retries with exponential
// backoff if the mail provider hiccups, and moves it to a dead-letter set only
// after exhausting retries — so no customer silently misses their ticket.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/portfolio/tickflow/internal/api"
	"github.com/portfolio/tickflow/internal/queue"
	"github.com/portfolio/tickflow/internal/worker"
)

func main() {
	var (
		httpAddr    = flag.String("http", ":8081", "dashboard/API listen address")
		workers     = flag.Int("workers", 4, "number of worker goroutines")
		journalPath = flag.String("journal", "", "durability journal path (empty = disabled)")
		staticDir   = flag.String("static", "web/static", "dashboard static dir")
	)
	flag.Parse()

	var journal *queue.Journal
	if *journalPath != "" {
		j, err := queue.OpenJournal(*journalPath)
		if err != nil {
			log.Fatalf("open journal: %v", err)
		}
		journal = j
		defer journal.Close()
	}

	q := queue.New(journal)
	if journal != nil {
		if err := journal.Replay(q); err != nil {
			log.Fatalf("replay journal: %v", err)
		}
		log.Printf("journal replayed from %s", *journalPath)
	}

	pool := worker.NewPool(q, *workers)
	registerHandlers(pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	srv := api.New(q, pool)
	httpSrv := &http.Server{Addr: *httpAddr, Handler: srv.Routes(*staticDir)}
	go func() {
		log.Printf("TickFlow dashboard on http://localhost%s (%d workers)", *httpAddr, *workers)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
	cancel()
	shutCtx, sc := context.WithTimeout(context.Background(), 2*time.Second)
	defer sc()
	_ = httpSrv.Shutdown(shutCtx)
}

// registerHandlers wires demo job types. In a real deployment these would call
// out to an email provider, a PDF renderer, a payment gateway, etc.
func registerHandlers(pool *worker.Pool) {
	pool.Register("send_ticket_email", func(ctx context.Context, payload json.RawMessage) error {
		var p struct {
			To      string `json:"to"`
			EventID string `json:"event_id"`
		}
		_ = json.Unmarshal(payload, &p)
		// Simulate a flaky mail provider: ~35% transient failure rate, so the
		// retry/backoff machinery is visibly exercised in the dashboard.
		time.Sleep(300 * time.Millisecond)
		if rand.Float64() < 0.35 {
			return errors.New("mail provider timeout")
		}
		log.Printf("ticket email sent to %s for event %s", p.To, p.EventID)
		return nil
	})

	pool.Register("generate_report", func(ctx context.Context, payload json.RawMessage) error {
		time.Sleep(500 * time.Millisecond)
		log.Printf("report generated: %s", string(payload))
		return nil
	})
}
