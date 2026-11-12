package shardstore

import (
	"context"
	"time"
)

// memberi tahu router event mana yang sudah dihapus dan kapan.
type Tombstones interface {
	HazardTombstones(ctx context.Context) (map[string]time.Time, error)
	Put(ctx context.Context, d Doc) error
	NextVersion() int64
}

// mengaktifkan penghapusan HazardEvent. Tanpa ini Delete selalu gagal.
func (r *Router) SetTombstones(t Tombstones) { r.tombs = t }

func (r *Router) loadTombstones(ctx context.Context) map[string]time.Time {
	if r.tombs == nil {
		return nil
	}
	m, err := r.tombs.HazardTombstones(ctx)
	if err != nil {
		r.cfg.Log.Warn("daftar tombstone tidak terbaca, perbaikan memakai data apa adanya", "err", err)
		return nil
	}
	return m
}

func tombstoned(tombs map[string]time.Time, id string, ingestedAt time.Time) bool {
	t, ok := tombs[id]
	return ok && !ingestedAt.After(t)
}

// menghapus HazardEvent dari seluruh unit yang terjangkau dan mencatat tombstone,
// sehingga salinan pada unit yang sedang mati tidak dihidupkan kembali oleh perbaikan.
func (r *Router) Delete(ctx context.Context, hazardID string) error {
	if r.tombs == nil {
		return ErrUnavailable
	}
	if err := r.tombs.Put(ctx, Doc{Collection: hazardTombstoneCollection, Key: hazardID, Deleted: false,
		Version: r.tombs.NextVersion()}); err != nil {
		return err
	}
	failed := r.each(ctx, "store.delete", r.cfg.UnitTimeout, func(uctx context.Context, u Unit) error {
		return u.Delete(uctx, hazardID)
	})
	if len(failed) > 0 {
		r.repairNeeded.Store(true)
	}
	return nil
}
