package shardstore

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

// Unit yang kembali setelah tidak terjangkau mungkin kembali dengan disk kosong (node mati dan datanya
// dihapus). Selama perbaikan belum selesai, unit itu hidup tetapi belum memegang salinannya, sehingga tidak
// boleh dihitung sebagai penutup data: tanpa ini, kehilangan unit kedua membuat sebagian event tidak
// terbaca padahal jawaban tetap mengaku lengkap.
//
// Perbaikan hanya dijalankan oleh leader (lihat SetRepairGate). Leader mencatat penanda "rebuild selesai"
// di penyimpanan dokumen yang direplikasi, dan replika lain membacanya untuk melepas status rebuilding.

const rebuildCollection = "store_rebuilt"

// rebuildSkew menoleransi perbedaan waktu antar-replika saat menyadari unit pulih (ping tiap beberapa detik).
const rebuildSkew = 30 * time.Second

// RebuildMarks menyimpan kapan perbaikan penuh yang bersih untuk sebuah unit dimulai.
type RebuildMarks interface {
	MarkRebuilt(ctx context.Context, unit string, repairStartedAt time.Time) error
	RebuiltAt(ctx context.Context, unit string) (time.Time, error)
}

type rebuildMark struct {
	RepairStartedAt time.Time `json:"repair_started_at"`
}

func (s *DocStore) MarkRebuilt(ctx context.Context, unit string, repairStartedAt time.Time) error {
	b, err := json.Marshal(rebuildMark{RepairStartedAt: repairStartedAt.UTC()})
	if err != nil {
		return err
	}
	return s.Put(ctx, Doc{Collection: rebuildCollection, Key: unit, Data: b})
}

func (s *DocStore) RebuiltAt(ctx context.Context, unit string) (time.Time, error) {
	d, err := s.Get(ctx, rebuildCollection, unit)
	if err != nil || d == nil {
		return time.Time{}, err
	}
	var m rebuildMark
	if err := json.Unmarshal(d.Data, &m); err != nil {
		return time.Time{}, err
	}
	return m.RepairStartedAt, nil
}

func (r *Router) SetRebuildMarks(m RebuildMarks) { r.marks = m }

// SetRepairGate membatasi perbaikan pada instance yang diizinkan (leader). Tanpa gerbang, semua replika
// Aggregator memindai dan menyalin data bersamaan dan membebani penyimpanan berlipat ganda.
func (r *Router) SetRepairGate(allowed func() bool) { r.repairGate = allowed }

func (r *Router) repairAllowed() bool { return r.repairGate == nil || r.repairGate() }

// coverageDown adalah unit yang tidak boleh dihitung sebagai penutup data: tidak terjangkau atau sedang
// dibangun ulang.
func (r *Router) coverageDown() map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]bool, len(r.down)+len(r.rebuilding))
	for k, v := range r.down {
		if v {
			out[k] = true
		}
	}
	for k := range r.rebuilding {
		out[k] = true
	}
	return out
}

// Rebuilding mengembalikan unit yang sedang dibangun ulang.
func (r *Router) Rebuilding() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.rebuilding))
	for k := range r.rebuilding {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// finishRebuild melepas unit yang mulai dibangun ulang sebelum perbaikan bersih yang dimulai pada started,
// lalu mencatat penandanya agar replika lain ikut melepas.
func (r *Router) finishRebuild(ctx context.Context, started time.Time) {
	r.mu.Lock()
	var done []string
	for u, since := range r.rebuilding {
		if !since.After(started) {
			delete(r.rebuilding, u)
			done = append(done, u)
		}
	}
	r.mu.Unlock()
	sort.Strings(done)
	for _, u := range done {
		r.cfg.Log.Info("unit penyimpanan selesai dibangun ulang", "unit", u)
		if r.marks != nil {
			if err := r.marks.MarkRebuilt(ctx, u, started); err != nil {
				r.cfg.Log.Warn("penanda rebuild gagal disimpan", "unit", u, "err", err)
			}
		}
	}
}

// syncRebuilt dipakai replika yang bukan leader: unit dilepas dari status rebuilding bila leader sudah
// mencatat perbaikan bersih yang dimulai setelah unit itu pulih.
func (r *Router) syncRebuilt(ctx context.Context) {
	if r.marks == nil {
		return
	}
	r.mu.Lock()
	pending := make(map[string]time.Time, len(r.rebuilding))
	for u, since := range r.rebuilding {
		pending[u] = since
	}
	r.mu.Unlock()
	for u, since := range pending {
		at, err := r.marks.RebuiltAt(ctx, u)
		if err != nil || at.IsZero() || at.Before(since.Add(-rebuildSkew)) {
			continue
		}
		r.mu.Lock()
		if cur, ok := r.rebuilding[u]; ok && cur.Equal(since) {
			delete(r.rebuilding, u)
		}
		r.mu.Unlock()
	}
}
