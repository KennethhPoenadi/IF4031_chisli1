package shardstore

import (
	"context"
	"sync"
	"testing"

	"aggregator/internal/pollstate"
)

type pollStateFakeUnit struct {
	*fakeUnit
	mu      sync.Mutex
	cursors map[string]pollstate.Cursor
}

func newPollStateFake(name string, cursors map[string]pollstate.Cursor) *pollStateFakeUnit {
	return &pollStateFakeUnit{fakeUnit: newFake(name), cursors: cursors}
}

func (f *pollStateFakeUnit) LoadPollCursor(_ context.Context, source string) (pollstate.Cursor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cursors[source], nil
}

func (f *pollStateFakeUnit) SavePollCursor(_ context.Context, source string, cursor pollstate.Cursor) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cursors[source] = cursor
	return nil
}

func newPollStateRouter(t *testing.T, units ...*pollStateFakeUnit) *Router {
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

func TestPollCursorLoadsOldestSafeReplicaAndRepairsOnSave(t *testing.T) {
	one := newPollStateFake("store-0", map[string]pollstate.Cursor{"bmkg": {Value: "cursor-3", Revision: 3}})
	two := newPollStateFake("store-1", map[string]pollstate.Cursor{"bmkg": {Value: "cursor-2", Revision: 2}})
	router := newPollStateRouter(t, one, two)

	got, err := router.LoadPollCursor(context.Background(), "bmkg")
	if err != nil {
		t.Fatal(err)
	}
	if got != "cursor-2" {
		t.Fatalf("cursor = %q, want oldest safe cursor-2", got)
	}
	if err := router.SavePollCursor(context.Background(), "bmkg", "cursor-4"); err != nil {
		t.Fatal(err)
	}
	for _, unit := range []*pollStateFakeUnit{one, two} {
		cursor, err := unit.LoadPollCursor(context.Background(), "bmkg")
		if err != nil {
			t.Fatal(err)
		}
		if cursor.Value != "cursor-4" || cursor.Revision != 4 || cursor.UpdatedAt.IsZero() {
			t.Fatalf("%s cursor = %#v, want cursor-4 revision 4", unit.Name(), cursor)
		}
	}
}

func TestPollCursorKeepsSourcesIndependent(t *testing.T) {
	unit := newPollStateFake("store-0", map[string]pollstate.Cursor{
		"bmkg":  {Value: "bmkg-1", Revision: 1},
		"pvmbg": {Value: "pvmbg-7", Revision: 7},
	})
	router := newPollStateRouter(t, unit)

	if err := router.SavePollCursor(context.Background(), "bmkg", "bmkg-2"); err != nil {
		t.Fatal(err)
	}
	got, err := router.LoadPollCursor(context.Background(), "pvmbg")
	if err != nil {
		t.Fatal(err)
	}
	if got != "pvmbg-7" {
		t.Fatalf("PVMBG cursor changed to %q after BMKG save", got)
	}
}

func TestPollCursorRejectsConflictingValuesAtSameRevision(t *testing.T) {
	router := newPollStateRouter(t,
		newPollStateFake("store-0", map[string]pollstate.Cursor{"bmkg": {Value: "left", Revision: 4}}),
		newPollStateFake("store-1", map[string]pollstate.Cursor{"bmkg": {Value: "right", Revision: 4}}),
	)
	if _, err := router.LoadPollCursor(context.Background(), "bmkg"); err == nil {
		t.Fatal("LoadPollCursor accepted conflicting cursor values at the same revision")
	}
}
