package store

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"aggregator/internal/shardstore"
)

// koleksi Mongo tempat seluruh dokumen ingest disimpan
// (pesan gagal, laporan lapangan, deteksi satelit, checkpoint, tombstone).
const DocCollectionName = "ingest_docs"

type storedDoc struct {
	Collection string            `bson:"c"`
	Key        string            `bson:"k"`
	Version    int64             `bson:"v"`
	Deleted    bool              `bson:"d"`
	SortKey    string            `bson:"s"`
	Index      map[string]string `bson:"i,omitempty"`
	Data       []byte            `bson:"data,omitempty"`
	Owners     []string          `bson:"o"`
}

func toDoc(s storedDoc) shardstore.Doc {
	return shardstore.Doc{Collection: s.Collection, Key: s.Key, Version: s.Version, Deleted: s.Deleted,
		SortKey: s.SortKey, Index: s.Index, Data: s.Data, Owners: s.Owners}
}

func (u *MongoUnit) ensureDocIndexes(ctx context.Context) error {
	return u.docIndexes.ensure(func() error {
		_, err := u.docs.Indexes().CreateMany(ctx, []mongo.IndexModel{
			{Keys: bson.D{{Key: "c", Value: 1}, {Key: "k", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "c", Value: 1}, {Key: "s", Value: 1}, {Key: "k", Value: 1}}},
			{Keys: bson.D{{Key: "i.$**", Value: 1}}},
			// Hanya tombstone yang diindeks: pembersihan mencari {d: true, v < cutoff} tanpa memindai seluruh koleksi.
			{Keys: bson.D{{Key: "d", Value: 1}, {Key: "v", Value: 1}},
				Options: options.Index().SetName("tombstone_v").SetPartialFilterExpression(bson.D{{Key: "d", Value: true}})},
		})
		return err
	})
}

func (u *MongoUnit) PutDoc(ctx context.Context, d shardstore.Doc) error {
	if err := u.ensureDocIndexes(ctx); err != nil {
		return err
	}
	doc := storedDoc{Collection: d.Collection, Key: d.Key, Version: d.Version, Deleted: d.Deleted,
		SortKey: d.SortKey, Index: d.Index, Data: d.Data, Owners: d.Owners}
	_, err := u.docs.ReplaceOne(ctx,
		bson.M{"c": d.Collection, "k": d.Key, "v": bson.M{"$lte": d.Version}},
		doc, options.Replace().SetUpsert(true))
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

func (u *MongoUnit) GetDoc(ctx context.Context, collection, key string) (*shardstore.Doc, error) {
	var s storedDoc
	err := u.docs.FindOne(ctx, bson.M{"c": collection, "k": key}).Decode(&s)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d := toDoc(s)
	return &d, nil
}

func (u *MongoUnit) ScanDocs(ctx context.Context, q shardstore.DocQuery, fn func(shardstore.Doc) error) error {
	clauses := bson.A{}
	if !q.AllCollections {
		clauses = append(clauses, bson.M{"c": q.Collection})
	}
	for k, v := range q.Eq {
		clauses = append(clauses, bson.M{"i." + k: v})
	}
	if q.AfterKey != "" {
		clauses = append(clauses, bson.M{"$or": bson.A{
			bson.M{"s": bson.M{"$gt": q.AfterSort}},
			bson.M{"s": q.AfterSort, "k": bson.M{"$gt": q.AfterKey}},
		}})
	}
	filter := bson.M{}
	if len(clauses) > 0 {
		filter["$and"] = clauses
	}
	// Pemindaian semua koleksi (repair) tidak memerlukan urutan tertentu. Mengurutkan menurut (s, k) tanpa
	// filter koleksi tidak didukung indeks mana pun, sehingga Mongo mengurutkan seluruh isi di server (dan
	// menulis ke disk). Urutan (c, k) mengikuti indeks unik yang ada: pemindaian murni berbasis indeks.
	sortSpec := bson.D{{Key: "s", Value: 1}, {Key: "k", Value: 1}}
	if q.AllCollections {
		sortSpec = bson.D{{Key: "c", Value: 1}, {Key: "k", Value: 1}}
	}
	opts := options.Find().SetSort(sortSpec).SetBatchSize(1000)
	if q.Limit > 0 {
		opts.SetLimit(int64(q.Limit))
	}
	if q.WithoutData {
		opts.SetProjection(bson.M{"data": 0})
	}
	cur, err := u.docs.Find(ctx, filter, opts)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var s storedDoc
		if err := cur.Decode(&s); err != nil {
			return err
		}
		if err := fn(toDoc(s)); err != nil {
			return err
		}
	}
	return cur.Err()
}

func (u *MongoUnit) DeleteDocCopy(ctx context.Context, collection, key string) error {
	_, err := u.docs.DeleteOne(ctx, bson.M{"c": collection, "k": key})
	return err
}

var _ shardstore.DocUnit = (*MongoUnit)(nil)

// PurgeTombstones menghapus fisik tombstone dengan versi lebih kecil dari olderThan, paling banyak limit dokumen.
func (u *MongoUnit) PurgeTombstones(ctx context.Context, olderThan int64, limit int) (int64, error) {
	if err := u.ensureDocIndexes(ctx); err != nil {
		return 0, err
	}
	filter := bson.M{"d": true, "v": bson.M{"$lt": olderThan}}
	cur, err := u.docs.Find(ctx, filter, options.Find().SetLimit(int64(limit)).SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return 0, err
	}
	var ids []any
	for cur.Next(ctx) {
		var row struct {
			ID any `bson:"_id"`
		}
		if err := cur.Decode(&row); err != nil {
			_ = cur.Close(ctx)
			return 0, err
		}
		ids = append(ids, row.ID)
	}
	if err := cur.Err(); err != nil {
		_ = cur.Close(ctx)
		return 0, err
	}
	_ = cur.Close(ctx)
	if len(ids) == 0 {
		return 0, nil
	}
	// Syarat d dan v diulang pada penghapusan: dokumen yang diperbarui di antara Find dan Delete tidak ikut terhapus.
	res, err := u.docs.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}, "d": true, "v": bson.M{"$lt": olderThan}})
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}
