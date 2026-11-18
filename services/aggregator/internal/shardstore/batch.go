package shardstore

import (
	"context"

	"aggregator/internal/hazard"
)

// UpsertItem adalah satu event beserta pemilik yang seharusnya, untuk penulisan massal.
type UpsertItem struct {
	Event  hazard.Event
	Owners []string
}

// BatchUnit bersifat opsional. Unit yang mengimplementasikannya menyalin banyak event dalam satu perjalanan
// ke penyimpanan (satu query $in dan satu BulkWrite) sehingga perbaikan unit kosong berjalan puluhan kali
// lebih cepat daripada satu Get dan satu Upsert per event. Unit tanpa antarmuka ini memakai jalur per event.
type BatchUnit interface {
	GetMany(ctx context.Context, ids []string) ([]hazard.Event, error)
	UpsertMany(ctx context.Context, items []UpsertItem) error
}

func getMany(ctx context.Context, u Unit, ids []string) ([]hazard.Event, error) {
	if b, ok := u.(BatchUnit); ok {
		return b.GetMany(ctx, ids)
	}
	out := make([]hazard.Event, 0, len(ids))
	for _, id := range ids {
		ev, err := u.Get(ctx, id)
		if err != nil {
			return out, err
		}
		if ev != nil {
			out = append(out, *ev)
		}
	}
	return out, nil
}

func upsertMany(ctx context.Context, u Unit, items []UpsertItem) error {
	if b, ok := u.(BatchUnit); ok {
		return b.UpsertMany(ctx, items)
	}
	for _, it := range items {
		if err := u.Upsert(ctx, it.Event, it.Owners); err != nil {
			return err
		}
	}
	return nil
}
