// Package runtime is where instances actually run.
//
// A real control plane would call Kubernetes, a VM API or a bare-metal agent
// here. This project ships a simulated runtime backed by a PostgreSQL table so
// the whole system runs on a laptop and every behaviour is testable. Swap in a
// real implementation by satisfying the Runtime interface.
package runtime

import (
	"context"
	"strings"

	"github.com/sumanthbolle/deployd/internal/store"
)

type Runtime interface {
	// Start launches one instance of image. It starts unhealthy.
	Start(ctx context.Context, q store.Querier, service, image string) error
	// Stop removes one instance.
	Stop(ctx context.Context, q store.Querier, id int64) error
	// Probe runs health checks and updates each instance's health.
	Probe(ctx context.Context, q store.Querier, service string) error
}

// Simulated marks an instance healthy on its first probe, unless its image
// tag contains "broken". That gives tests and demos a way to force a failed
// rollout: deploy "web:2-broken" and watch the reconciler roll back.
type Simulated struct{}

func (Simulated) Start(ctx context.Context, q store.Querier, service, image string) error {
	_, err := q.Exec(ctx, `INSERT INTO instances (service, image) VALUES ($1, $2)`, service, image)
	return err
}

func (Simulated) Stop(ctx context.Context, q store.Querier, id int64) error {
	_, err := q.Exec(ctx, `DELETE FROM instances WHERE id = $1`, id)
	return err
}

func (Simulated) Probe(ctx context.Context, q store.Querier, service string) error {
	insts, err := (&store.Store{}).ListInstances(ctx, q, service)
	if err != nil {
		return err
	}
	for _, in := range insts {
		healthy := !strings.Contains(in.Image, "broken")
		if healthy != in.Healthy {
			if _, err := q.Exec(ctx, `UPDATE instances SET healthy = $2 WHERE id = $1`, in.ID, healthy); err != nil {
				return err
			}
		}
	}
	return nil
}
