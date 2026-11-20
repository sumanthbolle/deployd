package reconcile

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/sumanthbolle/deployd/internal/runtime"
	"github.com/sumanthbolle/deployd/internal/store"
	"github.com/sumanthbolle/deployd/internal/telemetry"
)

// Outcome of one reconcile pass.
type Outcome int

const (
	Done        Outcome = iota // nothing more to do until the spec changes
	Progressing                // work remains: run again shortly
)

type Reconciler struct {
	Store   *store.Store
	Runtime runtime.Runtime
	Log     *slog.Logger
	// MaxSurge is how many extra instances may run during a rolling update.
	MaxSurge int
	// FailureThreshold is how many passes a new image may stay unhealthy
	// before the rollout is rolled back.
	FailureThreshold int
}

// Reconcile runs one pass for one service. It is idempotent: running it twice
// with no change in between does nothing the second time.
func (r *Reconciler) Reconcile(ctx context.Context, name string) (out Outcome, err error) {
	ctx, span := telemetry.Tracer.Start(ctx, "reconcile")
	span.SetAttributes(attribute.String("service", name))
	start := time.Now()
	defer func() {
		telemetry.ReconcileSeconds.Observe(time.Since(start).Seconds())
		switch {
		case err != nil:
			telemetry.ReconcileTotal.WithLabelValues("error").Inc()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		case out == Progressing:
			telemetry.ReconcileTotal.WithLabelValues("progressing").Inc()
		default:
			telemetry.ReconcileTotal.WithLabelValues("converged").Inc()
		}
		span.End()
	}()

	err = r.Store.InTx(ctx, func(tx pgx.Tx) error {
		out, err = r.pass(ctx, tx, name)
		return err
	})
	return out, err
}

// pass runs inside one transaction. The service row is locked FOR UPDATE, so
// even if two workers somehow hold jobs for the same service, their passes
// run one after the other rather than interleaving.
func (r *Reconciler) pass(ctx context.Context, tx pgx.Tx, name string) (Outcome, error) {
	if _, err := tx.Exec(ctx, `SELECT 1 FROM services WHERE name = $1 FOR UPDATE`, name); err != nil {
		return Done, err
	}
	svc, err := r.Store.GetService(ctx, tx, name)
	if errors.Is(err, store.ErrNotFound) {
		return Done, nil // deleted: nothing to do
	}
	if err != nil {
		return Done, err
	}
	log := r.Log.With("service", name, "generation", svc.Generation)

	if err := r.Runtime.Probe(ctx, tx, name); err != nil {
		return Done, err
	}
	insts, err := r.Store.ListInstances(ctx, tx, name)
	if err != nil {
		return Done, err
	}

	rollout, err := r.Store.ActiveRollout(ctx, tx, name)
	hasRollout := err == nil
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return Done, err
	}

	// Rollout health gate: count passes where the new image is unhealthy.
	if hasRollout && unhealthyOf(insts, svc.Image) > 0 {
		n, err := r.Store.IncrementFailedChecks(ctx, tx, rollout.ID)
		if err != nil {
			return Done, err
		}
		if n >= r.FailureThreshold {
			if rollout.FromImage == "" {
				// First deploy: there is no earlier image to return to.
				log.Warn("rollout failed with no previous image", "image", svc.Image)
				telemetry.RolloutsTotal.WithLabelValues(store.RolloutFailed).Inc()
				if err := r.Store.FinishRollout(ctx, tx, rollout.ID, store.RolloutFailed); err != nil {
					return Done, err
				}
				return Done, r.Store.SetStatus(ctx, tx, name, store.StatusDegraded)
			}
			log.Warn("rolling back", "from", svc.Image, "to", rollout.FromImage, "failed_checks", n)
			telemetry.RolloutsTotal.WithLabelValues(store.RolloutRolledBack).Inc()
			if err := r.Store.RollBack(ctx, tx, rollout); err != nil {
				return Done, err
			}
			return Progressing, nil // next pass converges on the old image
		}
	}

	plan := MakePlan(svc.Image, svc.Replicas, r.MaxSurge, insts)
	if plan.Converged {
		if hasRollout {
			log.Info("rollout succeeded", "image", svc.Image)
			telemetry.RolloutsTotal.WithLabelValues(store.RolloutSucceeded).Inc()
			if err := r.Store.FinishRollout(ctx, tx, rollout.ID, store.RolloutSucceeded); err != nil {
				return Done, err
			}
		}
		latest, err := r.Store.LatestRollout(ctx, tx, name)
		status := store.StatusReady
		if err == nil && (latest.State == store.RolloutRolledBack || latest.State == store.RolloutFailed) {
			status = store.StatusDegraded // running fine, but the last change did not land
		}
		return Done, r.Store.SetObserved(ctx, tx, name, svc.Generation, status)
	}

	for _, id := range plan.Stop {
		if err := r.Runtime.Stop(ctx, tx, id); err != nil {
			return Done, err
		}
	}
	for range plan.Start {
		if err := r.Runtime.Start(ctx, tx, name, svc.Image); err != nil {
			return Done, err
		}
	}
	log.Debug("applied plan", "started", plan.Start, "stopped", len(plan.Stop))
	return Progressing, nil
}

func unhealthyOf(insts []store.Instance, image string) int {
	n := 0
	for _, in := range insts {
		if in.Image == image && !in.Healthy {
			n++
		}
	}
	return n
}
