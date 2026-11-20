// Package reconcile drives running instances toward the desired state.
package reconcile

import (
	"sort"

	"github.com/sumanthbolle/deployd/internal/store"
)

// Plan is what one reconcile pass should do. Planning is a pure function of
// (desired state, observed state), which keeps the tricky part easy to test.
type Plan struct {
	Start     int     // instances of the desired image to start
	Stop      []int64 // instance IDs to stop
	Converged bool    // running state already matches desired state
}

// MakePlan computes a rolling update with maxUnavailable = 0: healthy old
// instances are only stopped once enough new ones are healthy to replace
// them, and at most maxSurge extra instances run during the change.
func MakePlan(image string, replicas, maxSurge int, insts []store.Instance) Plan {
	var newOK, newBad, oldOK, oldBad []store.Instance
	for _, in := range insts {
		switch {
		case in.Image == image && in.Healthy:
			newOK = append(newOK, in)
		case in.Image == image:
			newBad = append(newBad, in)
		case in.Healthy:
			oldOK = append(oldOK, in)
		default:
			oldBad = append(oldBad, in)
		}
	}

	if len(newOK) == replicas && len(newBad) == 0 && len(oldOK) == 0 && len(oldBad) == 0 {
		return Plan{Converged: true}
	}

	var p Plan
	// Unhealthy instances of an old image serve nothing: always remove them.
	for _, in := range oldBad {
		p.Stop = append(p.Stop, in.ID)
	}

	// Too many instances of the desired image (scale down): drop unhealthy
	// ones first, then the newest healthy ones.
	newAll := append(append([]store.Instance{}, newBad...), byNewest(newOK)...)
	keepNew := newAll
	if extra := len(newAll) - replicas; extra > 0 {
		for _, in := range newAll[:extra] {
			p.Stop = append(p.Stop, in.ID)
		}
		keepNew = newAll[extra:]
	}
	keptHealthy := 0
	for _, in := range keepNew {
		if in.Healthy {
			keptHealthy++
		}
	}

	// Retire old healthy instances only while total healthy stays >= replicas.
	if surplus := keptHealthy + len(oldOK) - replicas; surplus > 0 {
		old := byOldest(oldOK)
		for i := 0; i < surplus && i < len(old); i++ {
			p.Stop = append(p.Stop, old[i].ID)
		}
	}

	// Start new instances, never exceeding replicas + maxSurge in total.
	running := len(insts) - len(p.Stop)
	want := replicas - len(keepNew)
	room := replicas + maxSurge - running
	p.Start = max(0, min(want, room))
	return p
}

func byOldest(in []store.Instance) []store.Instance {
	out := append([]store.Instance{}, in...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func byNewest(in []store.Instance) []store.Instance {
	out := append([]store.Instance{}, in...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	return out
}
