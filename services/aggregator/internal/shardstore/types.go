package shardstore

import (
	"context"
	"errors"
	"time"

	"aggregator/internal/hazard"
)

var ErrUnavailable = errors.New("shardstore: tidak ada unit penyimpanan yang terjangkau")
var ErrBadCursor = errors.New("shardstore: cursor tidak valid")

type Order int

const (
	NewestFirst Order = iota
	OldestFirst
)

type Filter struct {
	HazardType    string
	Severity      string
	Source        string
	OccurredAfter *time.Time
	IngestedSince *time.Time
}

type ListQuery struct {
	Filter Filter
	Limit  int
	Cursor string
	Order  Order
}

type Page struct {
	Events      []hazard.Event
	NextCursor  string
	Partial     bool
	Unavailable []string
}

type Position struct {
	IngestedAt time.Time
	HazardID   string
}

type UnitQuery struct {
	Filter Filter
	After  *Position
	Limit  int
	Order  Order
}

type KeyRecord struct {
	HazardID    string
	Source      string
	SourceRefID string
	IngestedAt  time.Time
	Owners      []string
}

type AggregateQuery struct {
	From, To time.Time
	Bucket   string
}

type AggregateResult struct {
	Series   map[int64]map[string]int
	Severity map[string]int
	Area     map[string]int
	Total    int
}

func NewAggregateResult() AggregateResult {
	return AggregateResult{Series: map[int64]map[string]int{}, Severity: map[string]int{}, Area: map[string]int{}}
}

func (a *AggregateResult) Merge(other AggregateResult) {
	for t, cats := range other.Series {
		if a.Series[t] == nil {
			a.Series[t] = map[string]int{}
		}
		for c, n := range cats {
			a.Series[t][c] += n
		}
	}
	for k, n := range other.Severity {
		a.Severity[k] += n
	}
	for k, n := range other.Area {
		a.Area[k] += n
	}
	a.Total += other.Total
}

type Unit interface {
	Name() string
	Upsert(ctx context.Context, ev hazard.Event, owners []string) error
	Get(ctx context.Context, hazardID string) (*hazard.Event, error)
	Query(ctx context.Context, q UnitQuery) ([]hazard.Event, error)
	Keys(ctx context.Context, fn func(KeyRecord) error) error
	SetOwners(ctx context.Context, hazardID string, owners []string) error
	Delete(ctx context.Context, hazardID string) error
	Aggregate(ctx context.Context, q AggregateQuery, down []string) (AggregateResult, error)
	Ping(ctx context.Context) error
}

type NodeResolver interface {
	NodeOf(ctx context.Context, unit string) (string, error)
}

// AnalyticsFutureTolerance: kejadian yang occurred_at-nya lebih jauh dari ini di depan waktu diterima memakai jam sumber yang tidak
// sejalan dengan jam BNPB (mis. Feed Satelit memakai waktu virtual yang dipercepat 60x, sehingga occurred_at berada di tahun 2028).
const AnalyticsFutureTolerance = time.Hour

// AnalyticsTime adalah waktu yang dipakai dashboard analitik untuk rentang dan pengelompokan: occurred_at, kecuali bila occurred_at
// berada lebih dari AnalyticsFutureTolerance di depan ingested_at; saat itu ingested_at dipakai. Tanpa aturan ini kejadian dari
// sumber berjam virtual tidak pernah masuk rentang waktu mana pun dan hilang dari seluruh panel.
func AnalyticsTime(occurred, ingested time.Time) time.Time {
	if !ingested.IsZero() && occurred.After(ingested.Add(AnalyticsFutureTolerance)) {
		return ingested
	}
	return occurred
}
