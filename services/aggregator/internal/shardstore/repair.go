package shardstore

import (
	"aggregator/internal/hazard"
	"context"
	"sync"
	"time"

	"telemetry"
)

type RepairStats struct {
	Scanned     int
	Replicated  int
	OwnersFixed int
	Removed     int
	Unavailable []string
}

var (
	repairReplicated = telemetry.Counter("store_repair_replicated", "salinan yang dibuat oleh perbaikan penempatan data")
	repairRemoved    = telemetry.Counter("store_repair_removed", "salinan usang yang dihapus oleh perbaikan penempatan data")
)

type holder struct {
	unit string
	rec  KeyRecord
}

func (r *Router) RepairOnce(ctx context.Context) (RepairStats, error) {
	r.repairMu.Lock()
	defer r.repairMu.Unlock()

	var mu sync.Mutex
	byID := map[string][]holder{}
	failed := r.each(ctx, "store.repair.scan", r.cfg.RepairTimeout, func(uctx context.Context, u Unit) error {
		var local []KeyRecord
		if err := u.Keys(uctx, func(k KeyRecord) error {
			local = append(local, k)
			return nil
		}); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, k := range local {
			byID[k.HazardID] = append(byID[k.HazardID], holder{unit: u.Name(), rec: k})
		}
		return nil
	})
	if len(failed) == len(r.units) {
		return RepairStats{Unavailable: failed}, ErrUnavailable
	}

	stats := RepairStats{Scanned: len(byID), Unavailable: failed}
	allUp := len(failed) == 0
	down := r.downSet()
	tombs := r.loadTombstones(ctx)

	for id, holders := range byID {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		desired := r.ring.Owners(id, r.rf)

		best := holders[0]
		for _, h := range holders[1:] {
			if h.rec.IngestedAt.After(best.rec.IngestedAt) {
				best = h
			}
		}
		if tombstoned(tombs, id, best.rec.IngestedAt) {
			for _, h := range holders {
				uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
				if err := r.units[h.unit].Delete(uctx, id); err == nil {
					stats.Removed++
				}
				cancel()
			}
			continue
		}
		heldBy := map[string]holder{}
		for _, h := range holders {
			heldBy[h.unit] = h
		}

		var targets []string
		for _, name := range desired {
			if down[name] {
				continue
			}
			h, ok := heldBy[name]
			if !ok || h.rec.IngestedAt.Before(best.rec.IngestedAt) {
				targets = append(targets, name)
			}
		}

		replicated := map[string]bool{}
		if len(targets) > 0 {
			ev, err := r.getFrom(ctx, best.unit, id)
			if err == nil && ev != nil {
				for _, name := range targets {
					uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
					err := r.units[name].Upsert(uctx, *ev, desired)
					cancel()
					if err != nil {
						r.markDown(name, err)
						continue
					}
					replicated[name] = true
					stats.Replicated++
				}
			}
		}

		for _, name := range desired {
			h, ok := heldBy[name]
			if !ok || replicated[name] || down[name] || sameOwners(h.rec.Owners, desired) {
				continue
			}
			uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
			err := r.units[name].SetOwners(uctx, id, desired)
			cancel()
			if err == nil {
				stats.OwnersFixed++
			}
		}

		if !allUp {
			continue
		}
		complete := true
		for _, name := range desired {
			h, ok := heldBy[name]
			current := ok && !h.rec.IngestedAt.Before(best.rec.IngestedAt)
			if !current && !replicated[name] {
				complete = false
				break
			}
		}
		if !complete {
			continue
		}
		for _, h := range holders {
			if contains(desired, h.unit) {
				continue
			}
			uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
			err := r.units[h.unit].Delete(uctx, id)
			cancel()
			if err == nil {
				stats.Removed++
			}
		}
	}

	repairReplicated(ctx, int64(stats.Replicated))
	repairRemoved(ctx, int64(stats.Removed))
	if allUp {
		r.repairNeeded.Store(false)
	}
	return stats, nil
}

func (r *Router) getFrom(ctx context.Context, unit, id string) (*hazard.Event, error) {
	uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
	defer cancel()
	ev, err := r.units[unit].Get(uctx, id)
	if err != nil {
		r.markDown(unit, err)
	}
	return ev, err
}

func sameOwners(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (r *Router) Run(ctx context.Context) {
	ping := time.NewTicker(r.cfg.PingInterval)
	defer ping.Stop()
	repair := time.NewTicker(r.cfg.RepairInterval)
	defer repair.Stop()

	runRepair := func() {
		rctx, cancel := context.WithTimeout(ctx, r.cfg.RepairTimeout)
		defer cancel()
		stats, err := r.RepairOnce(rctx)
		switch {
		case err != nil:
			r.cfg.Log.Warn("perbaikan penyimpanan gagal", "err", err)
		case stats.Replicated > 0 || stats.Removed > 0 || stats.OwnersFixed > 0:
			r.cfg.Log.Info("perbaikan penyimpanan selesai", "scanned", stats.Scanned, "replicated", stats.Replicated,
				"owners_fixed", stats.OwnersFixed, "removed", stats.Removed, "unavailable", stats.Unavailable)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			r.pingAll(ctx)
			if r.repairNeeded.Load() && len(r.downSet()) < len(r.units) {
				runRepair()
			}
		case <-repair.C:
			runRepair()
		}
	}
}

func (r *Router) pingAll(ctx context.Context) {
	var wg sync.WaitGroup
	for name, u := range r.units {
		wg.Add(1)
		go func(name string, u Unit) {
			defer wg.Done()
			uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
			defer cancel()
			if err := u.Ping(uctx); err != nil {
				r.markDown(name, err)
				return
			}
			if r.markUp(name) {
				r.cfg.Log.Info("unit penyimpanan pulih, perbaikan dijadwalkan", "unit", name)
				r.repairNeeded.Store(true)
			}
		}(name, u)
	}
	wg.Wait()
}
