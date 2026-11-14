package fire

import (
	"sort"
	"time"

	"satellite-ingestor/internal/detection"
)

// menghitung ulang kebakaran dari deteksi aktif dan mengembalikan perubahan
// sejak Flush terakhir: kebakaran baru atau berubah (upsert), kebakaran yang hilang
// karena bergabung, terpecah, menyusut di bawah 2 deteksi, atau ditarik (delete), serta
// deteksi yang harus disimpan.
func (e *Engine) Flush() Changes {
	var ch Changes
	w := e.Watermark()
	var finalBefore time.Time
	if !w.IsZero() {
		finalBefore = w.Add(-MaxGap)
	}
	now := e.now().UTC()

	next := map[string]string{} // kebakaran aktif setelah batch ini
	seen := map[string]bool{}   // semua kebakaran yang masih ada (termasuk yang baru final)
	singles := 0
	for _, members := range Components(e.active) {
		latest := members[0].Time
		for _, m := range members[1:] {
			if m.Time.After(latest) {
				latest = m.Time
			}
		}
		final := !finalBefore.IsZero() && latest.Before(finalBefore)

		group := "single:" + members[0].Key
		if len(members) >= 2 {
			ev := ToEvent(members, e.provinces)
			fp := fingerprint(ev)
			group = ev.HazardID
			seen[ev.HazardID] = true
			if e.emitted[ev.HazardID] != fp {
				ev.IngestedAt = now
				ch.UpsertFires = append(ch.UpsertFires, ev)
			}
			if final {
				ch.FinalizedFires = append(ch.FinalizedFires, ev.HazardID)
			} else {
				next[ev.HazardID] = fp
			}
		} else if final {
			e.stats["final_single_detections"]++
		} else {
			singles++
		}

		if final {
			ch.FinalDetections = append(ch.FinalDetections, e.freeze(group, members)...)
		}
	}

	for id := range e.emitted {
		if !seen[id] {
			ch.DeleteFires = append(ch.DeleteFires, id)
		}
	}
	sort.Strings(ch.DeleteFires)
	e.emitted = next

	e.stats["single_detections_active"] = int64(singles)
	e.stats["fires_active"] = int64(len(next))
	e.stats["fires_finalized"] += int64(len(ch.FinalizedFires))
	e.stats["detections_active"] = int64(len(e.active))
	e.stats["detections_frozen"] = int64(len(e.frozen))

	putKeys := make([]string, 0, len(e.dirtyPut))
	for k := range e.dirtyPut {
		putKeys = append(putKeys, k)
	}
	sort.Strings(putKeys)
	for _, k := range putKeys {
		ch.PutDetections = append(ch.PutDetections, e.dirtyPut[k])
	}
	for k := range e.dirtyDel {
		ch.DeleteDetections = append(ch.DeleteDetections, k)
	}
	sort.Strings(ch.DeleteDetections)
	sort.Slice(ch.UpsertFires, func(i, j int) bool { return ch.UpsertFires[i].HazardID < ch.UpsertFires[j].HazardID })
	sort.Slice(ch.FinalDetections, func(i, j int) bool { return ch.FinalDetections[i].Key < ch.FinalDetections[j].Key })
	ch.Late = e.late

	e.dirtyPut = map[string]detection.Detection{}
	e.dirtyDel = map[string]bool{}
	e.late = nil
	return ch
}
