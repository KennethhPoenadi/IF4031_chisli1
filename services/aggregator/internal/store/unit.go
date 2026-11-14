package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"aggregator/internal/coastalstate"
	"aggregator/internal/hazard"
	"aggregator/internal/shardstore"
)

type MongoUnit struct {
	name              string
	client            *mongo.Client
	collection        *mongo.Collection
	coastalState      *mongo.Collection
	coastalRejections *mongo.Collection

	indexMu sync.Mutex
	indexed bool
}

type stored struct {
	hazard.Event `bson:",inline"`
	Owners       []string `bson:"owners"`
}

func NewMongoUnit(name, uri, dbName, collectionName string) (*MongoUnit, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(2 * time.Second))
	if err != nil {
		return nil, err
	}
	db := client.Database(dbName)
	return &MongoUnit{
		name: name, client: client, collection: db.Collection(collectionName),
		coastalState:      db.Collection("coastal_state"),
		coastalRejections: db.Collection("coastal_rejections"),
	}, nil
}

func (u *MongoUnit) Name() string { return u.name }

func (u *MongoUnit) Close(ctx context.Context) error { return u.client.Disconnect(ctx) }

func (u *MongoUnit) ensureIndexes(ctx context.Context) error {
	u.indexMu.Lock()
	defer u.indexMu.Unlock()
	if u.indexed {
		return nil
	}
	_, err := u.collection.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "hazard_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "ingested_at", Value: -1}, {Key: "hazard_id", Value: -1}}},
		{Keys: bson.D{{Key: "hazard_type", Value: 1}, {Key: "ingested_at", Value: -1}}},
		{Keys: bson.D{{Key: "severity", Value: 1}, {Key: "ingested_at", Value: -1}}},
		{Keys: bson.D{{Key: "source", Value: 1}, {Key: "ingested_at", Value: -1}}},
	})
	if err == nil {
		u.indexed = true
	}
	return err
}

func (u *MongoUnit) Ping(ctx context.Context) error {
	if err := u.client.Ping(ctx, nil); err != nil {
		return err
	}
	return u.ensureIndexes(ctx)
}

func (u *MongoUnit) Upsert(ctx context.Context, ev hazard.Event, owners []string) error {
	if err := u.ensureIndexes(ctx); err != nil {
		return err
	}
	ev.IngestedAt = ev.IngestedAt.UTC()
	ev.OccurredAt = ev.OccurredAt.UTC()
	_, err := u.collection.ReplaceOne(ctx, bson.M{"hazard_id": ev.HazardID}, stored{Event: ev, Owners: owners},
		options.Replace().SetUpsert(true))
	return err
}

func (u *MongoUnit) Get(ctx context.Context, hazardID string) (*hazard.Event, error) {
	var doc stored
	err := u.collection.FindOne(ctx, bson.M{"hazard_id": hazardID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &doc.Event, nil
}

func (u *MongoUnit) Query(ctx context.Context, q shardstore.UnitQuery) ([]hazard.Event, error) {
	clauses := bson.A{}
	f := q.Filter
	if f.HazardType != "" {
		clauses = append(clauses, bson.M{"hazard_type": f.HazardType})
	}
	if f.Severity != "" {
		clauses = append(clauses, bson.M{"severity": f.Severity})
	}
	if f.Source != "" {
		clauses = append(clauses, bson.M{"source": f.Source})
	}
	if f.OccurredAfter != nil {
		clauses = append(clauses, bson.M{"occurred_at": bson.M{"$gt": f.OccurredAfter.UTC()}})
	}
	if f.IngestedSince != nil {
		clauses = append(clauses, bson.M{"ingested_at": bson.M{"$gte": f.IngestedSince.UTC()}})
	}

	cmp, dir := "$lt", -1
	if q.Order == shardstore.OldestFirst {
		cmp, dir = "$gt", 1
	}
	if q.After != nil {
		t := q.After.IngestedAt.UTC()
		clauses = append(clauses, bson.M{"$or": bson.A{
			bson.M{"ingested_at": bson.M{cmp: t}},
			bson.M{"ingested_at": t, "hazard_id": bson.M{cmp: q.After.HazardID}},
		}})
	}

	filter := bson.M{}
	if len(clauses) > 0 {
		filter["$and"] = clauses
	}
	opts := options.Find().SetSort(bson.D{{Key: "ingested_at", Value: dir}, {Key: "hazard_id", Value: dir}})
	if q.Limit > 0 {
		opts.SetLimit(int64(q.Limit))
	}
	cur, err := u.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var docs []stored
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]hazard.Event, len(docs))
	for i := range docs {
		out[i] = docs[i].Event
	}
	return out, nil
}

func (u *MongoUnit) Keys(ctx context.Context, fn func(shardstore.KeyRecord) error) error {
	opts := options.Find().SetProjection(bson.M{"hazard_id": 1, "source": 1, "source_ref_id": 1, "ingested_at": 1, "owners": 1})
	cur, err := u.collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var doc stored
		if err := cur.Decode(&doc); err != nil {
			return err
		}
		if err := fn(shardstore.KeyRecord{HazardID: doc.HazardID, Source: string(doc.Source),
			SourceRefID: doc.SourceRefID, IngestedAt: doc.IngestedAt, Owners: doc.Owners}); err != nil {
			return err
		}
	}
	return cur.Err()
}

func (u *MongoUnit) SetOwners(ctx context.Context, hazardID string, owners []string) error {
	_, err := u.collection.UpdateOne(ctx, bson.M{"hazard_id": hazardID}, bson.M{"$set": bson.M{"owners": owners}})
	return err
}

func (u *MongoUnit) Delete(ctx context.Context, hazardID string) error {
	_, err := u.collection.DeleteOne(ctx, bson.M{"hazard_id": hazardID})
	return err
}

func (u *MongoUnit) LoadCoastalCursor(ctx context.Context) (coastalstate.Cursor, error) {
	var cursor coastalstate.Cursor
	err := u.coastalState.FindOne(ctx, bson.M{"_id": "sensor-pantai-cursor"}).Decode(&cursor)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return coastalstate.Cursor{}, nil
	}
	return cursor, err
}

func (u *MongoUnit) SaveCoastalCursor(ctx context.Context, cursor coastalstate.Cursor) error {
	_, err := u.coastalState.ReplaceOne(ctx, bson.M{"_id": "sensor-pantai-cursor"}, bson.M{
		"_id": "sensor-pantai-cursor", "value": cursor.Value,
		"revision": cursor.Revision, "updated_at": cursor.UpdatedAt.UTC(),
	}, options.Replace().SetUpsert(true))
	return err
}

func (u *MongoUnit) QuarantineCoastal(ctx context.Context, rejection coastalstate.Rejection) error {
	payload, err := json.Marshal(struct {
		Observation map[string]any `json:"observation"`
		Reason      string         `json:"reason"`
	}{Observation: rejection.Observation, Reason: rejection.Reason})
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	id := hex.EncodeToString(digest[:])
	_, err = u.coastalRejections.ReplaceOne(ctx, bson.M{"_id": id}, bson.M{
		"_id": id, "observation": rejection.Observation, "reason": rejection.Reason,
		"correlation_id": rejection.CorrelationID, "rejected_at": rejection.RejectedAt.UTC(),
	}, options.Replace().SetUpsert(true))
	return err
}

var _ shardstore.Unit = (*MongoUnit)(nil)
var _ coastalstate.Unit = (*MongoUnit)(nil)

func (u *MongoUnit) Aggregate(ctx context.Context, q shardstore.AggregateQuery, down []string) (shardstore.AggregateResult, error) {
	downArr := bson.A{}
	for _, d := range down {
		downArr = append(downArr, d)
	}
	bucket := "hour"
	if q.Bucket == "day" {
		bucket = "day"
	}
	tsunami := bson.D{{Key: "$and", Value: bson.A{
		bson.D{{Key: "$eq", Value: bson.A{"$hazard_type", string(hazard.TypeSeismic)}}},
		bson.D{{Key: "$ne", Value: bson.A{
			bson.D{{Key: "$type", Value: "$attributes." + hazard.TsunamiWarningKey}}, "missing"}}},
	}}}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "occurred_at", Value: bson.D{
			{Key: "$gte", Value: q.From.UTC()}, {Key: "$lt", Value: q.To.UTC()}}}}}},
		{{Key: "$addFields", Value: bson.D{{Key: "_eff", Value: bson.D{{Key: "$ifNull", Value: bson.A{
			bson.D{{Key: "$arrayElemAt", Value: bson.A{
				bson.D{{Key: "$filter", Value: bson.D{
					{Key: "input", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$owners", bson.A{}}}}},
					{Key: "cond", Value: bson.D{{Key: "$not", Value: bson.A{
						bson.D{{Key: "$in", Value: bson.A{"$$this", downArr}}}}}}},
				}}}, 0}}},
			u.name,
		}}}}}}},
		{{Key: "$match", Value: bson.D{{Key: "_eff", Value: u.name}}}},
		{{Key: "$addFields", Value: bson.D{{Key: "_cat", Value: bson.D{{Key: "$cond", Value: bson.A{
			tsunami, hazard.CategoryTsunami, "$hazard_type"}}}}}}},
		{{Key: "$facet", Value: bson.D{
			{Key: "series", Value: bson.A{bson.D{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "t", Value: bson.D{{Key: "$dateTrunc", Value: bson.D{{Key: "date", Value: "$occurred_at"}, {Key: "unit", Value: bucket}}}}},
					{Key: "c", Value: "$_cat"}}},
				{Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}}}}}},
			{Key: "severity", Value: bson.A{bson.D{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$severity"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}}}}}},
			{Key: "area", Value: bson.A{bson.D{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$area_name"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}}}}}},
		}}},
	}

	cur, err := u.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return shardstore.AggregateResult{}, err
	}
	defer cur.Close(ctx)

	var docs []struct {
		Series []struct {
			ID struct {
				T time.Time `bson:"t"`
				C string    `bson:"c"`
			} `bson:"_id"`
			N int `bson:"n"`
		} `bson:"series"`
		Severity []struct {
			ID string `bson:"_id"`
			N  int    `bson:"n"`
		} `bson:"severity"`
		Area []struct {
			ID string `bson:"_id"`
			N  int    `bson:"n"`
		} `bson:"area"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return shardstore.AggregateResult{}, err
	}

	out := shardstore.NewAggregateResult()
	for _, d := range docs {
		for _, s := range d.Series {
			t := s.ID.T.UTC().Unix()
			if out.Series[t] == nil {
				out.Series[t] = map[string]int{}
			}
			out.Series[t][s.ID.C] += s.N
			out.Total += s.N
		}
		for _, s := range d.Severity {
			out.Severity[s.ID] += s.N
		}
		for _, a := range d.Area {
			out.Area[a.ID] += a.N
		}
	}
	return out, nil
}
