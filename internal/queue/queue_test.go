package queue_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sumanthbolle/deployd/internal/queue"
	"github.com/sumanthbolle/deployd/internal/testdb"
)

func TestEnqueueIsDeduplicated(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool, time.Minute)
	ctx := context.Background()

	for range 3 {
		if err := q.Enqueue(ctx, pool, "reconcile", "web", 0); err != nil {
			t.Fatal(err)
		}
	}
	if n, _ := q.Depth(ctx); n != 1 {
		t.Fatalf("depth = %d, want 1", n)
	}
}

func TestClaimHandsEachJobToOneWorker(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool, time.Minute)
	ctx := context.Background()

	const jobs = 20
	for i := range jobs {
		if err := q.Enqueue(ctx, pool, "reconcile", string(rune('a'+i)), 0); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, err := q.Claim(ctx, string(rune('A'+w)))
				if errors.Is(err, queue.ErrEmpty) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				seen[j.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("job %d claimed %d times", id, n)
		}
	}
}

// A spec change that arrives while its job is running must not be lost.
func TestEnqueueWhileRunningRequeues(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool, time.Minute)
	ctx := context.Background()

	if err := q.Enqueue(ctx, pool, "reconcile", "web", 0); err != nil {
		t.Fatal(err)
	}
	j, err := q.Claim(ctx, "w1")
	if err != nil {
		t.Fatal(err)
	}
	// New change arrives mid-pass.
	if err := q.Enqueue(ctx, pool, "reconcile", "web", 0); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(ctx, j.ID); err != nil {
		t.Fatal(err)
	}
	again, err := q.Claim(ctx, "w1")
	if err != nil {
		t.Fatalf("expected the job to be runnable again, got %v", err)
	}
	if again.ID != j.ID {
		t.Fatalf("got job %d, want %d", again.ID, j.ID)
	}
	if err := q.Complete(ctx, again.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Claim(ctx, "w1"); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("queue should be empty, got %v", err)
	}
}

func TestExpiredLeaseIsReclaimed(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool, 50*time.Millisecond)
	ctx := context.Background()

	q.Enqueue(ctx, pool, "reconcile", "web", 0) //nolint:errcheck
	if _, err := q.Claim(ctx, "crashed-worker"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Claim(ctx, "w2"); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("job should still be leased, got %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	j, err := q.Claim(ctx, "w2")
	if err != nil {
		t.Fatalf("expected reclaim after lease expiry, got %v", err)
	}
	if j.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", j.Attempts)
	}
}

func TestFailBacksOffThenDies(t *testing.T) {
	pool := testdb.New(t)
	q := queue.New(pool, time.Minute)
	ctx := context.Background()

	q.Enqueue(ctx, pool, "reconcile", "web", 0)        //nolint:errcheck
	pool.Exec(ctx, `UPDATE jobs SET max_attempts = 2`) //nolint:errcheck
	for attempt := 1; attempt <= 2; attempt++ {
		pool.Exec(ctx, `UPDATE jobs SET run_at = now() - interval '1s'`) //nolint:errcheck
		j, err := q.Claim(ctx, "w1")
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if err := q.Fail(ctx, j, errors.New("boom")); err != nil {
			t.Fatal(err)
		}
	}
	var dead bool
	var lastErr string
	if err := pool.QueryRow(ctx, `SELECT dead, last_error FROM jobs`).Scan(&dead, &lastErr); err != nil {
		t.Fatal(err)
	}
	if !dead || lastErr != "boom" {
		t.Fatalf("dead=%v last_error=%q, want dead with error", dead, lastErr)
	}
}

func TestBackoff(t *testing.T) {
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 4: 8 * time.Second, 20: time.Minute} {
		if got := queue.Backoff(attempt); got != want {
			t.Errorf("Backoff(%d) = %v, want %v", attempt, got, want)
		}
	}
}
