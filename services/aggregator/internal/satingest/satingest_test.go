package satingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/shardstore"
)

type rec struct {
	mu    sync.Mutex
	ops   []string
	docs  map[string]shardstore.Doc
	saved map[string]hazard.Event
	fail  string
}

func newRec() *rec {
	return &rec{docs: map[string]shardstore.Doc{}, saved: map[string]hazard.Event{}}
}

func (r *rec) log(op string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
	if r.fail == op {
		return errors.New("gagal: " + op)
	}
	return nil
}

func (r *rec) Put(_ context.Context, d shardstore.Doc) error {
	if err := r.log("put:" + d.Collection); err != nil {
		return err
	}
	r.mu.Lock()
	r.docs[d.Collection+"/"+d.Key] = d
	r.mu.Unlock()
	return nil
}

func (r *rec) Get(_ context.Context, c, k string) (*shardstore.Doc, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.docs[c+"/"+k]
	if !ok || d.Deleted {
		return nil, nil
	}
	return &d, nil
}

func (r *rec) Delete(ctx context.Context, c, k string) error {
	return r.Put(ctx, shardstore.Doc{Collection: c, Key: k, Deleted: true})
}

func (r *rec) ScanAll(_ context.Context, q shardstore.DocQuery) ([]shardstore.Doc, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []shardstore.Doc
	for _, d := range r.docs {
		if d.Collection == q.Collection && !d.Deleted {
			out = append(out, d)
		}
	}
	return out, false, nil
}

func (r *rec) Save(_ context.Context, ev hazard.Event) error {
	if err := r.log("save"); err != nil {
		return err
	}
	r.mu.Lock()
	r.saved[ev.HazardID] = ev
	r.mu.Unlock()
	return nil
}

type events struct{ *rec }

func (e events) Delete(_ context.Context, id string) error {
	if err := e.log("delete-event"); err != nil {
		return err
	}
	e.mu.Lock()
	delete(e.saved, id)
	e.mu.Unlock()
	return nil
}

func fire(id string) hazard.Event {
	return hazard.Event{HazardID: "hz-" + id, SourceRefID: id, Source: hazard.SourceSatelit, HazardType: hazard.TypeWildfire, Severity: hazard.SeverityWaspada}
}

func TestApplyWritesCheckpointLastAndIsIdempotent(t *testing.T) {
	r := newRec()
	s := New(r, events{r}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b := Batch{Offset: 41, MaxEventTime: time.Now().UTC(),
		PutDetections: []Detection{{Key: "N20|2026-11-12|243|-2.5|113.8", Offset: 40}, {Key: "N21|2026-11-12|245|-2.5|113.8", Offset: 41}},
		UpsertFires:   []hazard.Event{fire("fire-aaa")}, DeleteFires: []string{"hz-fire-old"},
		ActiveFires: map[string]string{"hz-fire-aaa": "fp"}}
	for i := 0; i < 2; i++ {
		if err := s.Apply(context.Background(), b, "c"); err != nil {
			t.Fatal(err)
		}
	}
	if last := r.ops[len(r.ops)-1]; last != "put:"+CollCheckpoint {
		t.Fatalf("operasi terakhir = %s, checkpoint harus ditulis paling akhir", last)
	}
	cp, err := s.Checkpoint(context.Background())
	if err != nil || cp.Offset != 41 {
		t.Fatalf("checkpoint %+v %v", cp, err)
	}
	dets, _, _ := s.ActiveDetections(context.Background(), 40)
	if len(dets) != 1 {
		t.Fatalf("deteksi dengan offset <= 40 = %d", len(dets))
	}
}

func TestApplyFailureLeavesCheckpointUntouched(t *testing.T) {
	r := newRec()
	r.fail = "save"
	s := New(r, events{r}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := s.Apply(context.Background(), Batch{Offset: 7, UpsertFires: []hazard.Event{fire("fire-x")}}, "c")
	if err == nil {
		t.Fatal("kegagalan simpan harus dikembalikan")
	}
	if cp, _ := s.Checkpoint(context.Background()); cp != nil {
		t.Fatal("checkpoint tidak boleh maju bila batch gagal")
	}
}

func TestApplyRejectsNonWildfireEvents(t *testing.T) {
	r := newRec()
	s := New(r, events{r}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	bad := fire("fire-x")
	bad.Source = hazard.SourceBMKG
	if err := s.Apply(context.Background(), Batch{UpsertFires: []hazard.Event{bad}}, "c"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err=%v", err)
	}
	if err := s.Apply(context.Background(), Batch{DeleteFires: []string{"hz-eq-1"}}, "c"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("menghapus event non-kebakaran harus ditolak, err=%v", err)
	}
}
