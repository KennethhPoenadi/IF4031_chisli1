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
	Failed      int // salinan yang gagal dibuat; unit tidak dilepas dari status rebuilding bila > 0
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

// scanKeys memindai kunci setiap unit secara streaming dan hanya menampung bucket yang diminta.
// Pada lintasan pertama (buckets == 1) yang melewati batas, penampungan dihentikan dan hanya jumlahnya
// yang dihitung, sehingga jumlah lintasan dapat ditentukan tanpa memori berlebih.
func (r *Router) scanKeys(ctx context.Context, bucket, buckets, limit int) (byID map[string][]holder, total int, overflow bool, failed []string) {
	var mu sync.Mutex
	byID = map[string][]holder{}
	failed = r.each(ctx, "store.repair.scan", r.cfg.RepairTimeout, func(uctx context.Context, u Unit) error {
		local := map[string]KeyRecord{}
		seen, over := 0, false
		if err := u.Keys(uctx, func(k KeyRecord) error {
			seen++
			if over {
				return nil
			}
			if buckets > 1 && repairBucket(k.HazardID, buckets) != bucket {
				return nil
			}
			local[k.HazardID] = k
			if buckets == 1 && len(local) > limit {
				over, local = true, nil
			}
			return nil
		}); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if seen > total {
			total = seen
		}
		if over {
			overflow = true
			return nil
		}
		for id, k := range local {
			byID[id] = append(byID[id], holder{unit: u.Name(), rec: k})
		}
		return nil
	})
	return byID, total, overflow, failed
}

// RepairOnce menyamakan penempatan HazardEvent dengan ring saat ini. Memori dibatasi: kunci dipindai
// secara streaming dan diproses per bucket hash, sehingga puncak memori mengikuti repairEntriesPerPass,
// bukan jumlah seluruh event.
func (r *Router) RepairOnce(ctx context.Context) (RepairStats, error) {
	r.repairMu.Lock()
	defer r.repairMu.Unlock()
	started := time.Now()

	limit := r.repairLimit
	if limit <= 0 {
		limit = repairEntriesPerPass
	}
	first, total, overflow, failed := r.scanKeys(ctx, 0, 1, limit)
	if len(failed) == len(r.units) {
		return RepairStats{Unavailable: failed}, ErrUnavailable
	}
	buckets := 1
	if overflow {
		buckets = max(2, min((total+limit-1)/limit, maxRepairBuckets))
	}

	stats := RepairStats{Unavailable: failed}
	allUp := true
	tombs := r.loadTombstones(ctx)
	for b := 0; b < buckets; b++ {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		byID, passFailed := first, failed
		if buckets > 1 {
			var passTotal int
			var passOver bool
			byID, passTotal, passOver, passFailed = r.scanKeys(ctx, b, buckets, limit)
			_, _ = passTotal, passOver
			if len(passFailed) == len(r.units) {
				return stats, ErrUnavailable
			}
			stats.Unavailable = passFailed
		}
		if len(passFailed) != 0 {
			allUp = false
		}
		stats.Scanned += len(byID)
		if err := r.repairHazards(ctx, byID, len(passFailed) == 0, tombs, &stats); err != nil {
			return stats, err
		}
	}

	repairReplicated(ctx, int64(stats.Replicated))
	repairRemoved(ctx, int64(stats.Removed))
	if allUp {
		r.repairNeeded.Store(false)
		if stats.Failed == 0 {
			r.finishRebuild(ctx, started)
		}
	}
	return stats, nil
}

// repairWorkers membatasi penyalinan paralel: cukup untuk mempercepat pengisian unit kosong tanpa
// membanjiri penyimpanan yang juga melayani lalu lintas normal.
const repairWorkers = 8

func (r *Router) repairHazards(ctx context.Context, byID map[string][]holder, allUp bool, tombs map[string]time.Time, stats *RepairStats) error {
	down := r.downSet()
	type item struct {
		id      string
		holders []holder
	}
	items := make(chan item)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < repairWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range items {
				var local RepairStats
				r.repairOne(ctx, it.id, it.holders, allUp, down, tombs, &local)
				mu.Lock()
				stats.Replicated += local.Replicated
				stats.OwnersFixed += local.OwnersFixed
				stats.Removed += local.Removed
				stats.Failed += local.Failed
				mu.Unlock()
			}
		}()
	}
	for id, holders := range byID {
		if ctx.Err() != nil {
			break
		}
		items <- item{id: id, holders: holders}
	}
	close(items)
	wg.Wait()
	return ctx.Err()
}

func (r *Router) repairOne(ctx context.Context, id string, holders []holder, allUp bool, down map[string]bool, tombs map[string]time.Time, stats *RepairStats) {
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
		return
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
		if err != nil || ev == nil {
			stats.Failed++
		}
		if err == nil && ev != nil {
			for _, name := range targets {
				uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
				err := r.units[name].Upsert(uctx, *ev, desired)
				cancel()
				if err != nil {
					r.markDown(name, err)
					stats.Failed++
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
		return
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
		return
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

	lastRun := time.Now()
	runRepair := func() {
		lastRun = time.Now()
		rctx, cancel := context.WithTimeout(ctx, r.cfg.RepairTimeout)
		defer cancel()
		stats, err := r.RepairOnce(rctx)
		switch {
		case err != nil:
			r.cfg.Log.Warn("perbaikan penyimpanan gagal", "err", err)
		case stats.Replicated > 0 || stats.Removed > 0 || stats.OwnersFixed > 0 || stats.Failed > 0:
			r.cfg.Log.Info("perbaikan penyimpanan selesai", "scanned", stats.Scanned, "replicated", stats.Replicated,
				"owners_fixed", stats.OwnersFixed, "removed", stats.Removed, "failed", stats.Failed,
				"unavailable", stats.Unavailable, "rebuilding", r.Rebuilding())
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			r.pingAll(ctx)
			if !r.repairAllowed() {
				r.syncRebuilt(ctx)
				continue
			}
			if (r.repairNeeded.Load() || len(r.Rebuilding()) > 0) && len(r.downSet()) < len(r.units) {
				runRepair()
			}
		case <-repair.C:
			if !r.repairAllowed() {
				continue
			}
			// Pemindaian penuh mahal. Tanpa pemicu (unit kembali, penulisan sebagian) cukup berkala sebagai
			// jaring pengaman, bukan tiap RepairInterval.
			if r.repairNeeded.Load() || time.Since(lastRun) >= repairIdleInterval {
				runRepair()
			}
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
