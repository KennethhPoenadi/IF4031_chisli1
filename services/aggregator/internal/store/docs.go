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
	u.indexMu.Lock()
	defer u.indexMu.Unlock()
	if u.docIndexed {
		return nil
	}
	_, err := u.docs.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "c", Value: 1}, {Key: "k", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "c", Value: 1}, {Key: "s", Value: 1}, {Key: "k", Value: 1}}},
		{Keys: bson.D{{Key: "i.$**", Value: 1}}},
	})
	if err == nil {
		u.docIndexed = true
	}
	return err
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
	opts := options.Find().SetSort(bson.D{{Key: "s", Value: 1}, {Key: "k", Value: 1}})
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
