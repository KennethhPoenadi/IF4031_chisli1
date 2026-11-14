package shardstore

import (
	"context"
	"sync"
	"testing"
	"time"

	"aggregator/internal/coastalstate"
)

type coastalFakeUnit struct {
	*fakeUnit
	coastalMu sync.Mutex
	cursor    coastalstate.Cursor
	rejected  map[string]coastalstate.Rejection
}

func newCoastalFake(name string, cursor coastalstate.Cursor) *coastalFakeUnit {
	return &coastalFakeUnit{fakeUnit: newFake(name), cursor: cursor, rejected: map[string]coastalstate.Rejection{}}
}

func (f *coastalFakeUnit) LoadCoastalCursor(context.Context) (coastalstate.Cursor, error) {
	f.coastalMu.Lock()
	defer f.coastalMu.Unlock()
	return f.cursor, nil
}

func (f *coastalFakeUnit) SaveCoastalCursor(_ context.Context, cursor coastalstate.Cursor) error {
	f.coastalMu.Lock()
	defer f.coastalMu.Unlock()
	f.cursor = cursor
	return nil
}

func (f *coastalFakeUnit) QuarantineCoastal(_ context.Context, rejection coastalstate.Rejection) error {
	f.coastalMu.Lock()
	defer f.coastalMu.Unlock()
	f.rejected[rejection.Reason] = rejection
	return nil
}

func newCoastalRouter(t *testing.T, units ...*coastalFakeUnit) *Router {
	t.Helper()
	raw := make([]Unit, len(units))
	for i, unit := range units {
		raw[i] = unit
	}
	router, err := New(raw, quietCfg())
	if err != nil {
		t.Fatal(err)
	}
	return router
}

func TestCoastalCursorReplaysOldestReplicaThenRepairsAllOnSave(t *testing.T) {
	one := newCoastalFake("store-0", coastalstate.Cursor{Value: "cursor-3", Revision: 3})
	two := newCoastalFake("store-1", coastalstate.Cursor{Value: "cursor-2", Revision: 2})
	router := newCoastalRouter(t, one, two)

	got, err := router.LoadCursor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != "cursor-2" {
		t.Fatalf("cursor = %q, want oldest safe cursor", got)
	}
	if err := router.SaveCursor(context.Background(), "cursor-4"); err != nil {
		t.Fatal(err)
	}
	for _, unit := range []*coastalFakeUnit{one, two} {
		cursor, err := unit.LoadCoastalCursor(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if cursor.Value != "cursor-4" || cursor.Revision != 4 || cursor.UpdatedAt.IsZero() {
			t.Fatalf("%s cursor = %#v, want cursor-4 revision 4", unit.Name(), cursor)
		}
	}
}

func TestCoastalCursorRejectsConflictingValuesAtSameRevision(t *testing.T) {
	router := newCoastalRouter(t,
		newCoastalFake("store-0", coastalstate.Cursor{Value: "left", Revision: 7}),
		newCoastalFake("store-1", coastalstate.Cursor{Value: "right", Revision: 7}),
	)
	if _, err := router.LoadCursor(context.Background()); err == nil {
		t.Fatal("LoadCursor accepted conflicting cursor values at the same revision")
	}
}

func TestCoastalQuarantineIsReplicated(t *testing.T) {
	one := newCoastalFake("store-0", coastalstate.Cursor{})
	two := newCoastalFake("store-1", coastalstate.Cursor{})
	router := newCoastalRouter(t, one, two)
	rejection := coastalstate.Rejection{
		Observation: map[string]any{"observation_id": "bad-1"}, Reason: "invalid wave height",
		CorrelationID: "corr-1", RejectedAt: time.Now().UTC(),
	}
	if err := router.Quarantine(context.Background(), rejection); err != nil {
		t.Fatal(err)
	}
	for _, unit := range []*coastalFakeUnit{one, two} {
		unit.coastalMu.Lock()
		got, ok := unit.rejected[rejection.Reason]
		unit.coastalMu.Unlock()
		if !ok || got.CorrelationID != rejection.CorrelationID {
			t.Fatalf("%s rejection was not replicated: %#v", unit.Name(), got)
		}
	}
}
