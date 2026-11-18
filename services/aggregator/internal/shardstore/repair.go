package shardstore

import (
	"aggregator/internal/hazard"
	"context"
	"sort"
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
	// Tiap putaran mulai dari bucket berbeda. Bila putaran terpotong batas waktu, bucket di belakang tidak
	// boleh kelaparan sementara putaran berikutnya mengulang bucket depan yang sudah beres.
	start := int(r.repairRotor.Add(1)) % buckets
	for i := 0; i < buckets; i++ {
		b := (i + start) % buckets
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

// repairWorkers membatasi pekerjaan paralel pada tahap pembersihan per event.
const repairWorkers = 4

// repairBatchSize adalah jumlah event per perjalanan ke penyimpanan saat menyalin. 500 menjaga satu BulkWrite
// tetap kecil (beberapa ratus KB) sehingga tidak melewati batas waktu unit yang juga melayani lalu lintas.
const repairBatchSize = 500

// repairBatchTimeout lebih longgar daripada UnitTimeout: satu batch memuat ratusan event. Kegagalan batch
// akibat beban tidak menandai unit mati (pingAll yang menilai itu); event yang gagal dicoba lagi pada
// perbaikan berikutnya.
const repairBatchTimeout = 30 * time.Second

type repairPlan struct {
	id      string
	holders []holder
	desired []string
	best    holder
	heldBy  map[string]holder
	targets []string
}

// repairHazards memperbaiki satu lintasan dalam tiga fase: (1) rencana per event tanpa I/O, (2) replikasi
// berbatch dari unit sumber ke unit target, (3) pembersihan (pemilik dan salinan usang) per event.
func (r *Router) repairHazards(ctx context.Context, byID map[string][]holder, allUp bool, tombs map[string]time.Time, stats *RepairStats) error {
	down := r.downSet()
	var plans []*repairPlan
	for id, holders := range byID {
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
		p := &repairPlan{id: id, holders: holders, best: best, desired: r.ring.Owners(id, r.rf), heldBy: map[string]holder{}}
		for _, h := range holders {
			p.heldBy[h.unit] = h
		}
		for _, name := range p.desired {
			if down[name] {
				continue
			}
			h, ok := p.heldBy[name]
			if !ok || h.rec.IngestedAt.Before(best.rec.IngestedAt) {
				p.targets = append(p.targets, name)
			}
		}
		plans = append(plans, p)
	}

	replicated := r.replicateBatches(ctx, plans, stats)

	items := make(chan *repairPlan)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < repairWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range items {
				var local RepairStats
				r.cleanupOne(ctx, p, replicated[p.id], allUp, down, &local)
				mu.Lock()
				stats.OwnersFixed += local.OwnersFixed
				stats.Removed += local.Removed
				mu.Unlock()
			}
		}()
	}
	for _, p := range plans {
		if ctx.Err() != nil {
			break
		}
		items <- p
	}
	close(items)
	wg.Wait()
	return ctx.Err()
}

// replicateBatches menyalin event ke unit target secara berbatch dan mengembalikan, per event, unit target
// yang berhasil.
func (r *Router) replicateBatches(ctx context.Context, plans []*repairPlan, stats *RepairStats) map[string]map[string]bool {
	replicated := map[string]map[string]bool{}
	bySource := map[string][]*repairPlan{}
	for _, p := range plans {
		if len(p.targets) > 0 {
			bySource[p.best.unit] = append(bySource[p.best.unit], p)
		}
	}
	sources := make([]string, 0, len(bySource))
	for s := range bySource {
		sources = append(sources, s)
	}
	sort.Strings(sources)

	for _, src := range sources {
		list := bySource[src]
		for start := 0; start < len(list); start += repairBatchSize {
			if ctx.Err() != nil {
				return replicated
			}
			chunk := list[start:min(start+repairBatchSize, len(list))]
			ids := make([]string, len(chunk))
			for i, p := range chunk {
				ids[i] = p.id
			}
			gctx, cancel := context.WithTimeout(ctx, repairBatchTimeout)
			events, err := getMany(gctx, r.units[src], ids)
			cancel()
			byID := make(map[string]hazard.Event, len(events))
			for _, ev := range events {
				byID[ev.HazardID] = ev
			}
			perTarget := map[string][]UpsertItem{}
			owners := map[string][]string{}
			for _, p := range chunk {
				ev, ok := byID[p.id]
				if !ok || err != nil {
					stats.Failed++
					continue
				}
				for _, t := range p.targets {
					perTarget[t] = append(perTarget[t], UpsertItem{Event: ev, Owners: p.desired})
				}
				owners[p.id] = p.desired
			}
			for target, batch := range perTarget {
				uctx, ucancel := context.WithTimeout(ctx, repairBatchTimeout)
				uerr := upsertMany(uctx, r.units[target], batch)
				ucancel()
				if uerr != nil {
					stats.Failed += len(batch)
					r.cfg.Log.Warn("batch perbaikan gagal ditulis", "target", target, "event", len(batch), "err", uerr)
					continue
				}
				for _, it := range batch {
					if replicated[it.Event.HazardID] == nil {
						replicated[it.Event.HazardID] = map[string]bool{}
					}
					replicated[it.Event.HazardID][target] = true
					stats.Replicated++
				}
			}
		}
	}
	return replicated
}

func (r *Router) cleanupOne(ctx context.Context, p *repairPlan, replicated map[string]bool, allUp bool, down map[string]bool, stats *RepairStats) {
	for _, name := range p.desired {
		h, ok := p.heldBy[name]
		if !ok || replicated[name] || down[name] || sameOwners(h.rec.Owners, p.desired) {
			continue
		}
		uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
		err := r.units[name].SetOwners(uctx, p.id, p.desired)
		cancel()
		if err == nil {
			stats.OwnersFixed++
		}
	}

	if !allUp {
		return
	}
	for _, name := range p.desired {
		h, ok := p.heldBy[name]
		current := ok && !h.rec.IngestedAt.Before(p.best.rec.IngestedAt)
		if !current && !replicated[name] {
			return
		}
	}
	for _, h := range p.holders {
		if contains(p.desired, h.unit) {
			continue
		}
		uctx, cancel := context.WithTimeout(ctx, r.cfg.UnitTimeout)
		err := r.units[h.unit].Delete(uctx, p.id)
		cancel()
		if err == nil {
			stats.Removed++
		}
	}
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

	// Nol berarti belum pernah memperbaiki: instance yang baru menjadi leader menjalankan satu perbaikan penuh
	// pada tick pertama. Status "perlu perbaikan" hanya ada di memori leader sebelumnya dan hilang saat leader
	// berganti, sehingga tanpa ini sisa perbaikan menunggu jaring pengaman berkala.
	var lastRun time.Time
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
				r.markDownCtx(ctx, name, err)
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
