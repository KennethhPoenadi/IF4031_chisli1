package store

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// indexGuard memastikan indeks dibuat sekali, tetapi dapat dibuat ulang. Unit penyimpanan dapat kembali dari
// kegagalan dengan disk kosong (node mati dan datanya dihapus). Bila Aggregator tetap yakin indeksnya ada, unit
// itu tidak pernah mendapat indeks lagi: setiap upsert berdasarkan hazard_id memindai seluruh koleksi, batch
// perbaikan melewati batas waktu, dan unit tidak pernah terisi kembali. Maka kegagalan Ping membatalkan
// keyakinan itu (invalidate), dan Ping berikutnya yang berhasil membuat indeks lagi (CreateIndexes idempoten).
type indexGuard struct {
	mu   sync.Mutex
	done bool
}

func (g *indexGuard) ensure(create func() error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.done {
		return nil
	}
	if err := create(); err != nil {
		return err
	}
	g.done = true
	return nil
}

func (g *indexGuard) invalidate() {
	g.mu.Lock()
	g.done = false
	g.mu.Unlock()
}

// dedupeHazardIDs menghapus dokumen ganda dengan hazard_id sama (menyisakan yang ingested_at-nya terbaru).
// Selama unit tidak punya indeks unik, penulisan bersamaan dapat membuat duplikat, dan duplikat itu membuat
// pembuatan indeks unik gagal selamanya.
func dedupeHazardIDs(ctx context.Context, coll *mongo.Collection) (int64, error) {
	cur, err := coll.Aggregate(ctx, mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$hazard_id"},
			{Key: "docs", Value: bson.D{{Key: "$push", Value: bson.D{{Key: "id", Value: "$_id"}, {Key: "t", Value: "$ingested_at"}}}}},
			{Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
		{{Key: "$match", Value: bson.D{{Key: "n", Value: bson.D{{Key: "$gt", Value: 1}}}}}},
	})
	if err != nil {
		return 0, err
	}
	defer cur.Close(ctx)
	var removed int64
	for cur.Next(ctx) {
		var g struct {
			Docs []struct {
				ID any `bson:"id"`
				T  any `bson:"t"`
			} `bson:"docs"`
		}
		if err := cur.Decode(&g); err != nil {
			return removed, err
		}
		keep := 0
		for i, d := range g.Docs {
			if later(d.T, g.Docs[keep].T) {
				keep = i
			}
		}
		for i, d := range g.Docs {
			if i == keep {
				continue
			}
			res, err := coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: d.ID}})
			if err != nil {
				return removed, err
			}
			removed += res.DeletedCount
		}
	}
	return removed, cur.Err()
}

// later menyatakan apakah waktu a lebih baru daripada b. Nilai BSON datetime didekode sebagai bson.DateTime.
func later(a, b any) bool {
	da, oka := a.(bson.DateTime)
	db, okb := b.(bson.DateTime)
	if !oka || !okb {
		return false
	}
	return da > db
}
