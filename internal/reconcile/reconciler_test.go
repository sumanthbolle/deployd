package reconcile_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/sumanthbolle/deployd/internal/reconcile"
	"github.com/sumanthbolle/deployd/internal/runtime"
	"github.com/sumanthbolle/deployd/internal/store"
	"github.com/sumanthbolle/deployd/internal/testdb"
)

type harness struct {
	t  *testing.T
	st *store.Store
	r  *reconcile.Reconciler
}

func newHarness(t *testing.T) *harness {
	st := store.New(testdb.New(t))
	return &harness{t: t, st: st, r: &reconcile.Reconciler{
		Store: st, Runtime: runtime.Simulated{}, MaxSurge: 1, FailureThreshold: 3,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
}

func (h *harness) create(name, image string, replicas int) {
	h.t.Helper()
	err := h.st.InTx(context.Background(), func(tx pgx.Tx) error {
		_, err := h.st.CreateService(context.Background(), tx, name, image, replicas)
		return err
	})
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) update(name, image string, replicas int) {
	h.t.Helper()
	err := h.st.InTx(context.Background(), func(tx pgx.Tx) error {
		_, err := h.st.UpdateService(context.Background(), tx, name, image, replicas)
		return err
	})
	if err != nil {
		h.t.Fatal(err)
	}
}

// settle runs passes until the reconciler reports Done, like the worker does.
func (h *harness) settle(name string) int {
	h.t.Helper()
	for pass := 1; pass <= 50; pass++ {
		out, err := h.r.Reconcile(context.Background(), name)
		if err != nil {
			h.t.Fatal(err)
		}
		if out == reconcile.Done {
			return pass
		}
	}
	h.t.Fatal("did not settle in 50 passes")
	return 0
}

func (h *harness) state(name string) (store.Service, []store.Instance, store.Rollout) {
	h.t.Helper()
	ctx := context.Background()
	svc, err := h.st.GetService(ctx, h.st.Pool, name)
	if err != nil {
		h.t.Fatal(err)
	}
	insts, err := h.st.ListInstances(ctx, h.st.Pool, name)
	if err != nil {
		h.t.Fatal(err)
	}
	r, err := h.st.LatestRollout(ctx, h.st.Pool, name)
	if err != nil {
		h.t.Fatal(err)
	}
	return svc, insts, r
}

func assertRunning(t *testing.T, insts []store.Instance, image string, n int) {
	t.Helper()
	if len(insts) != n {
		t.Fatalf("%d instances, want %d", len(insts), n)
	}
	for _, in := range insts {
		if in.Image != image || !in.Healthy {
			t.Fatalf("instance %d: image=%s healthy=%v, want healthy %s", in.ID, in.Image, in.Healthy, image)
		}
	}
}

func TestCreateThenRollingUpdate(t *testing.T) {
	h := newHarness(t)
	h.create("web", "web:1", 3)
	h.settle("web")

	svc, insts, r := h.state("web")
	assertRunning(t, insts, "web:1", 3)
	if svc.Status != store.StatusReady || svc.ObservedGeneration != svc.Generation {
		t.Fatalf("service %+v, want Ready and observed", svc)
	}
	if r.State != store.RolloutSucceeded {
		t.Fatalf("first rollout %s, want Succeeded", r.State)
	}

	h.update("web", "web:2", 0)
	h.settle("web")
	svc, insts, r = h.state("web")
	assertRunning(t, insts, "web:2", 3)
	if svc.Status != store.StatusReady || r.State != store.RolloutSucceeded || r.FromImage != "web:1" {
		t.Fatalf("after update: status=%s rollout=%+v", svc.Status, r)
	}

	// Idempotent: another pass with nothing changed is a no-op.
	if passes := h.settle("web"); passes != 1 {
		t.Fatalf("converged service took %d passes, want 1", passes)
	}
}

func TestBrokenImageIsRolledBack(t *testing.T) {
	h := newHarness(t)
	h.create("api", "api:1", 2)
	h.settle("api")

	h.update("api", "api:2-broken", 0)
	h.settle("api")

	svc, insts, r := h.state("api")
	assertRunning(t, insts, "api:1", 2)
	if svc.Image != "api:1" {
		t.Fatalf("desired image %s, want rollback to api:1", svc.Image)
	}
	if r.State != store.RolloutRolledBack || r.FailedChecks < 3 {
		t.Fatalf("rollout %+v, want RolledBack after 3 failed checks", r)
	}
	if svc.Status != store.StatusDegraded {
		t.Fatalf("status %s, want Degraded so operators notice", svc.Status)
	}
}

func TestBrokenFirstDeployFails(t *testing.T) {
	h := newHarness(t)
	h.create("jobs", "jobs:1-broken", 1)
	h.settle("jobs")

	svc, _, r := h.state("jobs")
	if r.State != store.RolloutFailed || svc.Status != store.StatusDegraded {
		t.Fatalf("status=%s rollout=%s, want Degraded and Failed", svc.Status, r.State)
	}
}

func TestScaleUpAndDown(t *testing.T) {
	h := newHarness(t)
	h.create("web", "web:1", 2)
	h.settle("web")

	h.update("web", "", 5)
	h.settle("web")
	_, insts, _ := h.state("web")
	assertRunning(t, insts, "web:1", 5)

	h.update("web", "", 1)
	h.settle("web")
	_, insts, _ = h.state("web")
	assertRunning(t, insts, "web:1", 1)
}

// A second image change during a rollout supersedes it; a rollback then
// returns to the last image that was proven healthy, not the abandoned one.
func TestSupersededRolloutRollsBackToLastGood(t *testing.T) {
	h := newHarness(t)
	h.create("web", "web:1", 2)
	h.settle("web")

	h.update("web", "web:2", 0)
	h.r.Reconcile(context.Background(), "web") //nolint:errcheck // one pass only: rollout still in progress
	h.update("web", "web:3-broken", 0)
	h.settle("web")

	svc, insts, r := h.state("web")
	if svc.Image != "web:1" || r.FromImage != "web:1" {
		t.Fatalf("image=%s rollout.from=%s, want web:1", svc.Image, r.FromImage)
	}
	assertRunning(t, insts, "web:1", 2)
}
