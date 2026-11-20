// Package queue is a small durable work queue on PostgreSQL.
//
// Workers claim jobs with SELECT ... FOR UPDATE SKIP LOCKED, so many workers
// (in one process or many replicas) can pull from the same table without
// blocking each other or taking the same job. A claimed job carries a lease;
// if the worker dies, the lease expires and another worker picks the job up.
package queue

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrEmpty = errors.New("queue: no job ready")

type Job struct {
	ID          int64
	Kind        string
	Key         string
	Attempts    int
	MaxAttempts int
}

type Queue struct {
	pool  *pgxpool.Pool
	lease time.Duration
}

func New(pool *pgxpool.Pool, lease time.Duration) *Queue {
	return &Queue{pool: pool, lease: lease}
}

// Querier lets Enqueue join the caller's transaction, so "save the new spec"
// and "schedule work for it" commit or fail together.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Enqueue schedules (kind, key) to run after delay. There is at most one
// pending job per (kind, key):
//   - if one is waiting, it is moved earlier if this request is sooner;
//   - if one is running, it is flagged requeue, so Complete runs it again
//     instead of finishing it. Without this flag a change made while a
//     reconcile is in flight would be lost.
func (q *Queue) Enqueue(ctx context.Context, db Querier, kind, key string, delay time.Duration) error {
	var id int64
	return db.QueryRow(ctx, `
		INSERT INTO jobs (kind, key, run_at) VALUES ($1, $2, now() + $3::interval)
		ON CONFLICT (kind, key) WHERE NOT done DO UPDATE SET
			run_at  = LEAST(jobs.run_at, EXCLUDED.run_at),
			requeue = jobs.requeue OR COALESCE(jobs.locked_until > now(), false)
		RETURNING id`, kind, key, delay.String()).Scan(&id)
}

// Claim takes the oldest ready job and leases it to worker.
func (q *Queue) Claim(ctx context.Context, worker string) (Job, error) {
	var j Job
	err := q.pool.QueryRow(ctx, `
		UPDATE jobs SET locked_by = $1, locked_until = now() + $2::interval, attempts = attempts + 1
		WHERE id = (
			SELECT id FROM jobs
			WHERE NOT done AND run_at <= now() AND (locked_until IS NULL OR locked_until < now())
			ORDER BY run_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1)
		RETURNING id, kind, key, attempts, max_attempts`, worker, q.lease.String()).
		Scan(&j.ID, &j.Kind, &j.Key, &j.Attempts, &j.MaxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, ErrEmpty
	}
	return j, err
}

// Complete finishes a job, or reschedules it at once if it was flagged
// requeue while it ran.
func (q *Queue) Complete(ctx context.Context, id int64) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE jobs SET
			done = NOT requeue,
			run_at = CASE WHEN requeue THEN now() ELSE run_at END,
			attempts = CASE WHEN requeue THEN 0 ELSE attempts END,
			requeue = false, locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE id = $1`, id)
	return err
}

// Retry releases the job to run again after delay without counting it as a
// failure. The reconciler uses it while it waits for instances to settle.
func (q *Queue) Retry(ctx context.Context, id int64, delay time.Duration) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE jobs SET run_at = CASE WHEN requeue THEN now() ELSE now() + $2::interval END,
			attempts = 0, requeue = false,
			locked_by = NULL, locked_until = NULL
		WHERE id = $1`, id, delay.String())
	return err
}

// Fail records the error and retries with exponential backoff. After
// max_attempts the job is marked dead and kept for inspection.
func (q *Queue) Fail(ctx context.Context, j Job, cause error) error {
	delay := Backoff(j.Attempts)
	_, err := q.pool.Exec(ctx, `
		UPDATE jobs SET
			last_error = $2,
			run_at = now() + $3::interval,
			dead = attempts >= max_attempts,
			done = attempts >= max_attempts,
			locked_by = NULL, locked_until = NULL
		WHERE id = $1`, j.ID, cause.Error(), delay.String())
	return err
}

// Depth returns the number of jobs not yet done (for the queue-depth metric).
func (q *Queue) Depth(ctx context.Context) (int, error) {
	var n int
	err := q.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE NOT done`).Scan(&n)
	return n, err
}

// Backoff is 1s, 2s, 4s ... capped at 1 minute.
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
	if d > time.Minute {
		d = time.Minute
	}
	return d
}
