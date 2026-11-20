package reconcile

import (
	"slices"
	"testing"

	"github.com/sumanthbolle/deployd/internal/store"
)

func inst(id int64, image string, healthy bool) store.Instance {
	return store.Instance{ID: id, Image: image, Healthy: healthy}
}

func TestMakePlan(t *testing.T) {
	tests := []struct {
		name      string
		replicas  int
		insts     []store.Instance
		wantStart int
		wantStop  []int64
		converged bool
	}{
		{
			name:      "fresh service starts all replicas",
			replicas:  3,
			wantStart: 3,
		},
		{
			name:      "already converged",
			replicas:  2,
			insts:     []store.Instance{inst(1, "v2", true), inst(2, "v2", true)},
			converged: true,
		},
		{
			name:      "rolling update begins with one surge instance and stops nothing",
			replicas:  3,
			insts:     []store.Instance{inst(1, "v1", true), inst(2, "v1", true), inst(3, "v1", true)},
			wantStart: 1,
		},
		{
			name:     "new instance not yet healthy: wait",
			replicas: 3,
			insts: []store.Instance{inst(1, "v1", true), inst(2, "v1", true), inst(3, "v1", true),
				inst(4, "v2", false)},
		},
		{
			name:     "new instance healthy: retire oldest, start next",
			replicas: 3,
			insts: []store.Instance{inst(1, "v1", true), inst(2, "v1", true), inst(3, "v1", true),
				inst(4, "v2", true)},
			wantStart: 1,
			wantStop:  []int64{1},
		},
		{
			name:     "unhealthy old instances are removed",
			replicas: 1,
			insts:    []store.Instance{inst(1, "v1", false), inst(2, "v2", true)},
			wantStop: []int64{1},
		},
		{
			name:     "scale down removes newest first",
			replicas: 1,
			insts:    []store.Instance{inst(1, "v2", true), inst(2, "v2", true), inst(3, "v2", true)},
			wantStop: []int64{3, 2},
		},
		{
			name:      "scale up",
			replicas:  4,
			insts:     []store.Instance{inst(1, "v2", true), inst(2, "v2", true)},
			wantStart: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := MakePlan("v2", tt.replicas, 1, tt.insts)
			if p.Converged != tt.converged {
				t.Errorf("Converged = %v, want %v", p.Converged, tt.converged)
			}
			if p.Start != tt.wantStart {
				t.Errorf("Start = %d, want %d", p.Start, tt.wantStart)
			}
			if !slices.Equal(p.Stop, tt.wantStop) {
				t.Errorf("Stop = %v, want %v", p.Stop, tt.wantStop)
			}
		})
	}
}

// TestRollingUpdateNeverDropsCapacity simulates a full rollout pass by pass
// and checks the invariant that matters in production: healthy capacity never
// falls below the replica count, and the update finishes.
func TestRollingUpdateNeverDropsCapacity(t *testing.T) {
	const replicas = 5
	var insts []store.Instance
	var nextID int64
	for range replicas {
		nextID++
		insts = append(insts, inst(nextID, "v1", true))
	}

	for pass := 1; pass <= 50; pass++ {
		// Probe: everything started last pass is healthy now.
		for i := range insts {
			insts[i].Healthy = true
		}
		healthy := 0
		for _, in := range insts {
			if in.Healthy {
				healthy++
			}
		}
		if healthy < replicas {
			t.Fatalf("pass %d: healthy capacity %d < %d", pass, healthy, replicas)
		}

		p := MakePlan("v2", replicas, 1, insts)
		if p.Converged {
			return
		}
		insts = slices.DeleteFunc(insts, func(in store.Instance) bool { return slices.Contains(p.Stop, in.ID) })
		for range p.Start {
			nextID++
			insts = append(insts, inst(nextID, "v2", false))
		}
		if len(insts) > replicas+1 {
			t.Fatalf("pass %d: %d instances exceeds surge limit", pass, len(insts))
		}
	}
	t.Fatal("rollout did not converge in 50 passes")
}
