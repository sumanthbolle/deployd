package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/sumanthbolle/deployd/internal/queue"
	"github.com/sumanthbolle/deployd/internal/telemetry"
)

// JobKind is the queue kind for "reconcile this service".
const JobKind = "reconcile"

// Pool runs N workers that claim reconcile jobs from the queue.
type Pool struct {
	Queue        *queue.Queue
	Reconciler   *Reconciler
	Log          *slog.Logger
	Workers      int
	ID           string        // identifies this replica in jobs.locked_by
	PollInterval time.Duration // how long to sleep when the queue is empty
	PassTimeout  time.Duration // upper bound for one reconcile pass
	Requeue      time.Duration // delay between passes while progressing
}

// Run blocks until ctx is cancelled, then waits for in-flight passes to end.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := range p.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.loop(ctx, fmt.Sprintf("%s/%d", p.ID, i))
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.reportDepth(ctx)
	}()
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context, worker string) {
	for ctx.Err() == nil {
		job, err := p.Queue.Claim(ctx, worker)
		if errors.Is(err, queue.ErrEmpty) {
			sleep(ctx, jitter(p.PollInterval))
			continue
		}
		if err != nil {
			if ctx.Err() == nil {
				p.Log.Error("claim failed", "err", err)
			}
			sleep(ctx, jitter(p.PollInterval))
			continue
		}
		p.handle(ctx, job)
	}
}

func (p *Pool) handle(ctx context.Context, job queue.Job) {
	// Finish the pass even if shutdown starts mid-way: use a fresh context
	// with its own deadline rather than the cancelled parent.
	passCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.PassTimeout)
	defer cancel()

	out, err := p.Reconciler.Reconcile(passCtx, job.Key)
	switch {
	case err != nil:
		p.Log.Error("reconcile failed", "service", job.Key, "attempt", job.Attempts, "err", err)
		err = p.Queue.Fail(passCtx, job, err)
	case out == Progressing:
		err = p.Queue.Retry(passCtx, job.ID, p.Requeue)
	default:
		err = p.Queue.Complete(passCtx, job.ID)
	}
	if err != nil {
		p.Log.Error("update job failed", "job", job.ID, "err", err)
	}
}

func (p *Pool) reportDepth(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		if n, err := p.Queue.Depth(ctx); err == nil {
			telemetry.QueueDepth.Set(float64(n))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// jitter spreads polls out so replicas do not hit the database in lockstep.
func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int64N(int64(d)))
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
