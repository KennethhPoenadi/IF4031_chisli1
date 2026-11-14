package dlq

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"aggregator/internal/shardstore"
)

type memDocs struct {
	mu   sync.Mutex
	docs map[string]shardstore.Doc
	v    int64
}

func newMem() *memDocs { return &memDocs{docs: map[string]shardstore.Doc{}} }

func (m *memDocs) Put(_ context.Context, d shardstore.Doc) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.v++
	d.Version = m.v
	m.docs[d.Collection+"/"+d.Key] = d
	return nil
}

func (m *memDocs) Get(_ context.Context, c, k string) (*shardstore.Doc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.docs[c+"/"+k]
	if !ok || d.Deleted {
		return nil, nil
	}
	return &d, nil
}

func (m *memDocs) Delete(ctx context.Context, c, k string) error {
	return m.Put(ctx, shardstore.Doc{Collection: c, Key: k, Deleted: true})
}

func (m *memDocs) Scan(_ context.Context, q shardstore.DocQuery) (shardstore.DocPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []shardstore.Doc
	for _, d := range m.docs {
		if d.Deleted || d.Collection != q.Collection {
			continue
		}
		match := true
		for k, v := range q.Eq {
			if d.Index[k] != v {
				match = false
			}
		}
		if match && (q.AfterKey == "" || d.SortKey > q.AfterSort || (d.SortKey == q.AfterSort && d.Key > q.AfterKey)) {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SortKey != out[j].SortKey {
			return out[i].SortKey < out[j].SortKey
		}
		return out[i].Key < out[j].Key
	})
	page := shardstore.DocPage{}
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
		page.NextCursor = shardstore.EncodeDocCursor(out[len(out)-1].SortKey, out[len(out)-1].Key)
	}
	page.Docs = out
	return page, nil
}

func (m *memDocs) ScanAll(ctx context.Context, q shardstore.DocQuery) ([]shardstore.Doc, bool, error) {
	q.Limit = 0
	p, err := m.Scan(ctx, q)
	return p.Docs, false, err
}

func entry(ref, payload, status string, at time.Time) Entry {
	e := Entry{Source: "PVMBG", Kind: KindPVMBGReport, SourceRefID: ref, Status: status, Reason: "schema_incompatible: x", ReceivedAt: at}
	e.SetPayload([]byte(payload))
	return e
}

func TestRecordIsIdempotentAndListedAsAppendixA(t *testing.T) {
	d := New(newMem())
	ctx := context.Background()
	t0 := time.Date(2026, 11, 12, 2, 40, 11, 0, time.UTC)
	e := entry("r1", `{"a":1}`, StatusParked, t0)
	for i := 0; i < 3; i++ {
		if err := d.Record(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	items, next, err := d.ListUnprocessed(ctx, "PVMBG", 100, "")
	if err != nil || next != "" || len(items) != 1 {
		t.Fatalf("items=%v next=%q err=%v", items, next, err)
	}
	if items[0].Status != "parked" || items[0].Retryable || !items[0].ReceivedAt.Equal(t0) {
		t.Fatalf("%+v", items[0])
	}
	if items, _, _ := d.ListUnprocessed(ctx, "BMKG", 100, ""); len(items) != 0 {
		t.Fatal("filter source tidak bekerja")
	}
}

func TestInvalidUTF8PayloadSurvivesExactly(t *testing.T) {
	raw := []byte("{\"x\":\"\xff\xfe\"}")
	var e Entry
	e.SetPayload(raw)
	got, err := e.RawPayload()
	if err != nil || string(got) != string(raw) || e.PayloadEncoding != "base64" {
		t.Fatalf("payload berubah: %q %v", got, err)
	}
}

func TestRetryTransientThenParkAfterMaxAttempts(t *testing.T) {
	d := New(newMem())
	d.BaseBackoff = 0
	ctx := context.Background()
	if err := d.Record(ctx, entry("r1", `{}`, StatusRetrying, time.Now())); err != nil {
		t.Fatal(err)
	}
	fail := map[string]Processor{KindPVMBGReport: func(context.Context, Entry) error { return errors.New("store down") }}
	for i := 0; i < 10; i++ {
		if _, err := d.RetryDue(ctx, fail); err != nil {
			t.Fatal(err)
		}
	}
	items, _, _ := d.ListUnprocessed(ctx, "", 10, "")
	if len(items) != 1 || items[0].Status != StatusParked || items[0].Attempts != d.MaxAttempts {
		t.Fatalf("%+v", items)
	}
}

func TestRetryRecoversAndDisappears(t *testing.T) {
	d := New(newMem())
	d.BaseBackoff = 0
	ctx := context.Background()
	_ = d.Record(ctx, entry("r1", `{}`, StatusRetrying, time.Now()))
	ok := map[string]Processor{KindPVMBGReport: func(context.Context, Entry) error { return nil }}
	if n, err := d.RetryDue(ctx, ok); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if items, _, _ := d.ListUnprocessed(ctx, "", 10, ""); len(items) != 0 {
		t.Fatal("pesan pulih tidak boleh muncul lagi")
	}
}

func TestReprocessKeepsOrderPerSourceRefAndBlocksAfterFailure(t *testing.T) {
	d := New(newMem())
	ctx := context.Background()
	base := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	// r1: v1 lalu v2 (harus diproses berurutan). r2: v1 gagal, v2 harus ikut tertahan.
	_ = d.Record(ctx, entry("r1", `"r1-v2"`, StatusParked, base.Add(2*time.Second)))
	_ = d.Record(ctx, entry("r1", `"r1-v1"`, StatusParked, base.Add(1*time.Second)))
	_ = d.Record(ctx, entry("r2", `"r2-bad"`, StatusParked, base.Add(3*time.Second)))
	_ = d.Record(ctx, entry("r2", `"r2-v2"`, StatusParked, base.Add(4*time.Second)))

	var order []string
	proc := map[string]Processor{KindPVMBGReport: func(_ context.Context, e Entry) error {
		if e.Payload == `"r2-bad"` {
			return Permanent("schema_incompatible: masih rusak")
		}
		order = append(order, e.Payload)
		return nil
	}}
	res, err := d.Reprocess(ctx, "PVMBG", proc)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`"r1-v1"`, `"r1-v2"`}
	if len(order) != 2 || order[0] != want[0] || order[1] != want[1] {
		t.Fatalf("urutan = %v", order)
	}
	if res.Recovered != 2 || res.Blocked != 1 || res.StillParked != 2 {
		t.Fatalf("%+v", res)
	}
	// Menjalankan ulang tidak memproses yang sudah pulih.
	order = nil
	if _, err := d.Reprocess(ctx, "PVMBG", proc); err != nil || len(order) != 0 {
		t.Fatalf("reprocess ulang memproses ganda: %v", order)
	}
}

func TestHasParked(t *testing.T) {
	d := New(newMem())
	ctx := context.Background()
	_ = d.Record(ctx, entry("r9", `{}`, StatusParked, time.Now()))
	if ok, _ := d.HasParked(ctx, "PVMBG", "r9"); !ok {
		t.Fatal("r9 seharusnya tertahan")
	}
	if ok, _ := d.HasParked(ctx, "PVMBG", "r10"); ok {
		t.Fatal("r10 tidak tertahan")
	}
}
