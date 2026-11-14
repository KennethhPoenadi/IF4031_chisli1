package fieldreport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"aggregator/internal/shardstore"
)

type mem struct {
	mu   sync.Mutex
	docs map[string]shardstore.Doc
	fail bool
}

func (m *mem) Put(_ context.Context, d shardstore.Doc) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.docs[d.Key] = d
	return nil
}

func (m *mem) PutIfAbsent(_ context.Context, d shardstore.Doc) (bool, *shardstore.Doc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return false, nil, errors.New("store down")
	}
	if cur, ok := m.docs[d.Key]; ok {
		return false, &cur, nil
	}
	m.docs[d.Key] = d
	return true, nil, nil
}

func (m *mem) ScanAll(context.Context, shardstore.DocQuery) ([]shardstore.Doc, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]shardstore.Doc, 0, len(m.docs))
	for _, d := range m.docs {
		out = append(out, d)
	}
	return out, false, nil
}

func msg(id string) Message {
	return Message{ReportID: id, ClientID: "field-demo", ReceivedAt: time.Now().UTC(), Report: json.RawMessage(`{"text":"jembatan putus"}`)}
}

func TestRedeliveryIsNotProcessedTwice(t *testing.T) {
	m := &mem{docs: map[string]shardstore.Doc{}}
	p := New(m, slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		created, err := p.Process(ctx, msg("rep-1"))
		if err != nil {
			t.Fatal(err)
		}
		if created != (i == 0) {
			t.Fatalf("pengiriman ke-%d created=%v", i+1, created)
		}
	}
	_, _ = p.Process(ctx, msg("rep-2"))
	s, ids, err := p.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Reports != 2 || s.MaxProcessCount != 1 || s.DuplicateDeliveries != 2 || len(ids) != 2 {
		t.Fatalf("%+v %v", s, ids)
	}
}

func TestDecodeRejectsBrokenMessages(t *testing.T) {
	for _, body := range []string{"{", `{"report_id":""}`, "{\"report_id\":\"\xff\"}", `{"report_id":"x"}`} {
		if _, err := Decode([]byte(body)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Decode(%q) err=%v", body, err)
		}
	}
	if _, err := Decode([]byte(`{"report_id":"x","report":{"a":1}}`)); err != nil {
		t.Fatal(err)
	}
}

func TestStoreFailureIsReportedForRetry(t *testing.T) {
	m := &mem{docs: map[string]shardstore.Doc{}, fail: true}
	p := New(m, slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	if _, err := p.Process(context.Background(), msg("rep-1")); err == nil {
		t.Fatal("kegagalan simpan harus dikembalikan agar pesan di-requeue, bukan di-ack")
	}
}
