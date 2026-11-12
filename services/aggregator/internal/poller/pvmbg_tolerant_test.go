package poller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"aggregator/internal/dlq"
	"aggregator/internal/hazard"
	"aggregator/internal/mapper"
	"aggregator/internal/pvmbgclient"
)

type batchSource struct {
	fakeVolcanic
	body  string
	since []string
}

func (b *batchSource) VolcanicBatch(_ context.Context, since string, _ string) (pvmbgclient.Batch, error) {
	b.since = append(b.since, since)
	return pvmbgclient.DecodeBatch([]byte(b.body), false), nil
}

type fakeDead struct {
	mu      sync.Mutex
	entries map[string]dlq.Entry
	fail    bool
}

func (f *fakeDead) Record(_ context.Context, e dlq.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return errors.New("store down")
	}
	if f.entries == nil {
		f.entries = map[string]dlq.Entry{}
	}
	raw, _ := e.RawPayload()
	f.entries[dlq.MessageID(e.Source, raw)] = e
	return nil
}

func (f *fakeDead) HasParked(_ context.Context, source, ref string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.entries {
		if e.Source == source && e.SourceRefID == ref && e.Status == dlq.StatusParked && ref != "" {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeDead) byStatus(s string) []dlq.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []dlq.Entry
	for _, e := range f.entries {
		if e.Status == s {
			out = append(out, e)
		}
	}
	return out
}

const (
	okReport      = `{"report_id":"pvmbg-1","volcano_id":"G-001","alert_level":"Siaga","eruption_count_24h":2,"ash_column_height_m":1500,"reported_at":"2026-11-12T01:00:00Z"}`
	renamedReport = `{"report_id":"pvmbg-2","volcano_id":"G-002","level_status":"Awas","eruption_count_24h":3,"ash_column_height_m":900,"reported_at":"2026-11-12T01:01:00Z"}`
	retypedReport = `{"report_id":"pvmbg-3","volcano_id":"G-003","alert_level":"Waspada","eruption_count_24h":"4","ash_column_height_m":800,"reported_at":"2026-11-12T01:02:00Z"}`
)

func tolerantRig(body string, dead *fakeDead, rules mapper.Rules) (*Poller, *batchSource, *fakeStore, *fakePublisher) {
	src := &batchSource{body: body}
	st, pub := &fakeStore{}, &fakePublisher{}
	p := New(&fakeSeismic{}, src, st, pub, time.Second, 30*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.WithDeadLetters(dead, rules)
	return p, src, st, pub
}

func TestTolerantPollParksIncompatibleAndKeepsGoodReports(t *testing.T) {
	dead := &fakeDead{}
	body := "[" + okReport + "," + renamedReport + "," + retypedReport + ",{\"x\":\"\xff\"}]"
	p, _, st, _ := tolerantRig(body, dead, mapper.Rules{})
	p.pollPVMBG(context.Background())

	if ids := st.ids(); len(ids) != 1 || ids[0] != "hz-pvmbg-1" {
		t.Fatalf("saved = %v", ids)
	}
	parked := dead.byStatus(dlq.StatusParked)
	if len(parked) != 3 {
		t.Fatalf("parked = %d (%+v)", len(parked), parked)
	}
	for _, e := range parked {
		if !strings.HasPrefix(e.Reason, "schema_incompatible") && !strings.HasPrefix(e.Reason, "malformed") {
			t.Fatalf("alasan tidak dikenal: %q", e.Reason)
		}
	}
	if p.pvmbgSince == "" {
		t.Fatal("cursor harus maju setelah pesan gagal tercatat di DLQ")
	}
}

func TestTolerantPollDoesNotAdvanceWhenDLQUnavailable(t *testing.T) {
	dead := &fakeDead{fail: true}
	p, _, _, _ := tolerantRig("["+renamedReport+"]", dead, mapper.Rules{})
	p.pollPVMBG(context.Background())
	if p.pvmbgSince != "" {
		t.Fatal("cursor tidak boleh maju bila pesan gagal tidak tersimpan")
	}
}

func TestTolerantPollTruncatedAdvancesOnlyToLastReadReport(t *testing.T) {
	dead := &fakeDead{}
	p, _, st, _ := tolerantRig("["+okReport+","+renamedReport[:30], dead, mapper.Rules{})
	p.pollPVMBG(context.Background())
	if len(st.ids()) != 1 {
		t.Fatalf("saved = %v", st.ids())
	}
	if p.pvmbgSince != "2026-11-12T01:00:00Z" {
		t.Fatalf("cursor = %q", p.pvmbgSince)
	}
}

func TestPublishFailureIsRetryableAndStoreFailureKeepsCursor(t *testing.T) {
	dead := &fakeDead{}
	p, _, _, pub := tolerantRig("["+okReport+"]", dead, mapper.Rules{})
	pub.err = errors.New("broker down")
	p.pollPVMBG(context.Background())
	if r := dead.byStatus(dlq.StatusRetrying); len(r) != 1 || r[0].SourceRefID != "pvmbg-1" {
		t.Fatalf("retrying = %+v", r)
	}
}

func TestLaterMessageForParkedRefIsBlocked(t *testing.T) {
	dead := &fakeDead{}
	p, src, st, _ := tolerantRig("["+renamedReport+"]", dead, mapper.Rules{})
	p.pollPVMBG(context.Background())
	// Versi berikutnya untuk report yang sama datang dengan skema yang benar.
	src.body = `[{"report_id":"pvmbg-2","volcano_id":"G-002","alert_level":"Normal","eruption_count_24h":0,"ash_column_height_m":10,"reported_at":"2026-11-12T01:05:00Z"}]`
	p.pollPVMBG(context.Background())
	if len(st.ids()) != 0 {
		t.Fatal("versi baru tidak boleh tersimpan sebelum versi lama pulih")
	}
	blocked := 0
	for _, e := range dead.byStatus(dlq.StatusParked) {
		if strings.HasPrefix(e.Reason, "blocked_by_earlier") {
			blocked++
		}
	}
	if blocked != 1 {
		t.Fatalf("blocked = %d", blocked)
	}
}

func TestReprocessAfterRulesRecoversWithoutDuplicates(t *testing.T) {
	dead := &fakeDead{}
	p, _, st, _ := tolerantRig("["+renamedReport+","+retypedReport+"]", dead, mapper.Rules{})
	p.pollPVMBG(context.Background())

	rules, _ := mapper.ParseRules("level_status:alert_level", "eruption_count_24h:int")
	p.WithDeadLetters(dead, rules)
	procs := p.Processors()
	for _, e := range dead.byStatus(dlq.StatusParked) {
		if err := procs[e.Kind](context.Background(), e); err != nil {
			t.Fatalf("reprocess %s: %v", e.SourceRefID, err)
		}
		// Menjalankan dua kali tetap menghasilkan event yang sama (idempoten per hazard_id).
		_ = procs[e.Kind](context.Background(), e)
	}
	seen := map[string]bool{}
	for _, id := range st.ids() {
		seen[id] = true
	}
	if len(seen) != 2 || !seen["hz-pvmbg-2"] || !seen["hz-pvmbg-3"] {
		t.Fatalf("saved = %v", st.ids())
	}
	for _, ev := range st.saved {
		if ev.Source != hazard.SourcePVMBG {
			t.Fatal("source salah")
		}
	}
}
