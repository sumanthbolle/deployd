// Package store holds the desired state (services), rollout history and
// running instances in PostgreSQL.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

// Service statuses.
const (
	StatusProgressing = "Progressing"
	StatusReady       = "Ready"
	StatusDegraded    = "Degraded"
)

// Rollout states.
const (
	RolloutInProgress = "InProgress"
	RolloutSucceeded  = "Succeeded"
	RolloutRolledBack = "RolledBack"
	RolloutSuperseded = "Superseded" // a newer image arrived before this one finished
	RolloutFailed     = "Failed"     // failed with no earlier image to return to
)

type Service struct {
	Name               string
	Image              string
	Replicas           int
	Generation         int64
	ObservedGeneration int64
	Status             string
	UpdatedAt          time.Time
}

type Rollout struct {
	ID           int64
	Service      string
	FromImage    string
	ToImage      string
	State        string
	FailedChecks int
	CreatedAt    time.Time
	FinishedAt   *time.Time
}

type Instance struct {
	ID        int64
	Service   string
	Image     string
	Healthy   bool
	CreatedAt time.Time
}

type Store struct {
	Pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{Pool: pool} }

// Querier is satisfied by both *pgxpool.Pool and pgx.Tx, so the same helpers
// work inside and outside a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const serviceCols = `name, image, replicas, generation, observed_generation, status, updated_at`

func scanService(row pgx.Row) (Service, error) {
	var s Service
	err := row.Scan(&s.Name, &s.Image, &s.Replicas, &s.Generation, &s.ObservedGeneration, &s.Status, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, ErrNotFound
	}
	return s, err
}

// CreateService inserts the service and its first rollout in one transaction.
func (st *Store) CreateService(ctx context.Context, tx pgx.Tx, name, image string, replicas int) (Service, error) {
	s, err := scanService(tx.QueryRow(ctx,
		`INSERT INTO services (name, image, replicas) VALUES ($1, $2, $3) RETURNING `+serviceCols,
		name, image, replicas))
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return s, ErrAlreadyExists
	}
	if err != nil {
		return s, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO rollouts (service, to_image) VALUES ($1, $2)`, name, image)
	return s, err
}

// UpdateService changes the desired state. If the image changes, any rollout
// still in progress is superseded and a new one starts from the current image.
func (st *Store) UpdateService(ctx context.Context, tx pgx.Tx, name, image string, replicas int) (Service, error) {
	cur, err := scanService(tx.QueryRow(ctx, `SELECT `+serviceCols+` FROM services WHERE name = $1 FOR UPDATE`, name))
	if err != nil {
		return cur, err
	}
	if image == "" {
		image = cur.Image
	}
	if replicas == 0 {
		replicas = cur.Replicas
	}
	if image == cur.Image && replicas == cur.Replicas {
		return cur, nil // nothing changed: no new generation
	}
	if image != cur.Image {
		// Roll back to the last image known to be good. If a rollout is still
		// running, its target was never proven, so keep its starting image.
		from := cur.Image
		active, err := st.ActiveRollout(ctx, tx, name)
		switch {
		case err == nil:
			from = active.FromImage
			if err := st.FinishRollout(ctx, tx, active.ID, RolloutSuperseded); err != nil {
				return cur, err
			}
		case !errors.Is(err, ErrNotFound):
			return cur, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO rollouts (service, from_image, to_image) VALUES ($1, $2, $3)`,
			name, from, image); err != nil {
			return cur, err
		}
	}
	return scanService(tx.QueryRow(ctx, `UPDATE services
		SET image = $2, replicas = $3, generation = generation + 1, status = $4, updated_at = now()
		WHERE name = $1 RETURNING `+serviceCols, name, image, replicas, StatusProgressing))
}

func (st *Store) GetService(ctx context.Context, q Querier, name string) (Service, error) {
	return scanService(q.QueryRow(ctx, `SELECT `+serviceCols+` FROM services WHERE name = $1`, name))
}

func (st *Store) ListServices(ctx context.Context) ([]Service, error) {
	rows, err := st.Pool.Query(ctx, `SELECT `+serviceCols+` FROM services ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Service
	for rows.Next() {
		s, err := scanService(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// SetObserved records that the reconciler applied generation gen. The
// generation check stops a slow reconcile from marking a newer spec as done.
func (st *Store) SetObserved(ctx context.Context, q Querier, name string, gen int64, status string) error {
	_, err := q.Exec(ctx, `UPDATE services SET observed_generation = $2, status = $3, updated_at = now()
		WHERE name = $1 AND generation = $2`, name, gen, status)
	return err
}

func (st *Store) SetStatus(ctx context.Context, q Querier, name, status string) error {
	_, err := q.Exec(ctx, `UPDATE services SET status = $2, updated_at = now() WHERE name = $1`, name, status)
	return err
}

const rolloutCols = `id, service, from_image, to_image, state, failed_checks, created_at, finished_at`

func scanRollout(row pgx.Row) (Rollout, error) {
	var r Rollout
	err := row.Scan(&r.ID, &r.Service, &r.FromImage, &r.ToImage, &r.State, &r.FailedChecks, &r.CreatedAt, &r.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// ActiveRollout returns the in-progress rollout, or ErrNotFound.
func (st *Store) ActiveRollout(ctx context.Context, q Querier, service string) (Rollout, error) {
	return scanRollout(q.QueryRow(ctx, `SELECT `+rolloutCols+` FROM rollouts
		WHERE service = $1 AND state = $2`, service, RolloutInProgress))
}

func (st *Store) LatestRollout(ctx context.Context, q Querier, service string) (Rollout, error) {
	return scanRollout(q.QueryRow(ctx, `SELECT `+rolloutCols+` FROM rollouts
		WHERE service = $1 ORDER BY id DESC LIMIT 1`, service))
}

func (st *Store) ListRollouts(ctx context.Context, service string) ([]Rollout, error) {
	rows, err := st.Pool.Query(ctx, `SELECT `+rolloutCols+` FROM rollouts WHERE service = $1 ORDER BY id DESC`, service)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Rollout
	for rows.Next() {
		r, err := scanRollout(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (st *Store) FinishRollout(ctx context.Context, q Querier, id int64, state string) error {
	_, err := q.Exec(ctx, `UPDATE rollouts SET state = $2, finished_at = now() WHERE id = $1`, id, state)
	return err
}

func (st *Store) IncrementFailedChecks(ctx context.Context, q Querier, id int64) (int, error) {
	var n int
	err := q.QueryRow(ctx, `UPDATE rollouts SET failed_checks = failed_checks + 1 WHERE id = $1
		RETURNING failed_checks`, id).Scan(&n)
	return n, err
}

// RollBack points the service back at the previous image and closes the
// rollout. It bumps the generation so the reconciler treats it as new work.
func (st *Store) RollBack(ctx context.Context, tx pgx.Tx, r Rollout) error {
	if err := st.FinishRollout(ctx, tx, r.ID, RolloutRolledBack); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE services SET image = $2, generation = generation + 1, status = $3,
		updated_at = now() WHERE name = $1`, r.Service, r.FromImage, StatusDegraded)
	return err
}

func (st *Store) ListInstances(ctx context.Context, q Querier, service string) ([]Instance, error) {
	rows, err := q.Query(ctx, `SELECT id, service, image, healthy, created_at FROM instances
		WHERE service = $1 ORDER BY id`, service)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Instance
	for rows.Next() {
		var i Instance
		if err := rows.Scan(&i.ID, &i.Service, &i.Image, &i.Healthy, &i.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// InTx runs fn in a transaction and commits if fn returns nil.
func (st *Store) InTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := st.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(tx); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}
