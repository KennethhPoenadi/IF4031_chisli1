package shardstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"aggregator/internal/hazard"
)

type fakeUnit struct {
	name string
	mu   sync.Mutex
	data map[string]hazard.Event
	own  map[string][]string

	failing    bool
	blockQuery bool
}

func newFake(name string) *fakeUnit {
	return &fakeUnit{name: name, data: map[string]hazard.Event{}, own: map[string][]string{}}
}

var errDown = errors.New("unit mati")

func (f *fakeUnit) Name() string { return f.name }

func (f *fakeUnit) set(failing bool) {
	f.mu.Lock()
	f.failing = failing
	f.mu.Unlock()
}

func (f *fakeUnit) wipe() {
	f.mu.Lock()
	f.data = map[string]hazard.Event{}
	f.own = map[string][]string{}
	f.mu.Unlock()
}

func (f *fakeUnit) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.data)
}

func (f *fakeUnit) has(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.data[id]
	return ok
}

func (f *fakeUnit) Upsert(_ context.Context, ev hazard.Event, owners []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return errDown
	}
	f.data[ev.HazardID] = ev
	f.own[ev.HazardID] = append([]string(nil), owners...)
	return nil
}

func (f *fakeUnit) Get(_ context.Context, id string) (*hazard.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return nil, errDown
	}
	ev, ok := f.data[id]
	if !ok {
		return nil, nil
	}
	return &ev, nil
}

func (f *fakeUnit) Query(ctx context.Context, q UnitQuery) ([]hazard.Event, error) {
	f.mu.Lock()
	failing, block := f.failing, f.blockQuery
	f.mu.Unlock()
	if failing {
		return nil, errDown
	}
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	var out []hazard.Event
	for _, ev := range f.data {
		fl := q.Filter
		if fl.HazardType != "" && string(ev.HazardType) != fl.HazardType {
			continue
		}
		if fl.Severity != "" && string(ev.Severity) != fl.Severity {
			continue
		}
		if fl.Source != "" && string(ev.Source) != fl.Source {
			continue
		}
		if fl.OccurredAfter != nil && !ev.OccurredAt.After(*fl.OccurredAfter) {
			continue
		}
		if fl.IngestedSince != nil && ev.IngestedAt.Before(*fl.IngestedSince) {
			continue
		}
		if q.After != nil {
			cur := hazard.Event{IngestedAt: q.After.IngestedAt, HazardID: q.After.HazardID}
			if !less(cur, ev, q.Order) {
				continue
			}
		}
		out = append(out, ev)
	}
	sort.Slice(out, func(i, j int) bool { return less(out[i], out[j], q.Order) })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

func (f *fakeUnit) Keys(_ context.Context, fn func(KeyRecord) error) error {
	f.mu.Lock()
	if f.failing {
		f.mu.Unlock()
		return errDown
	}
	recs := make([]KeyRecord, 0, len(f.data))
	for id, ev := range f.data {
		recs = append(recs, KeyRecord{HazardID: id, Source: string(ev.Source), SourceRefID: ev.SourceRefID,
			IngestedAt: ev.IngestedAt, Owners: f.own[id]})
	}
	f.mu.Unlock()
	for _, r := range recs {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeUnit) SetOwners(_ context.Context, id string, owners []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return errDown
	}
	f.own[id] = append([]string(nil), owners...)
	return nil
}

func (f *fakeUnit) Delete(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return errDown
	}
	delete(f.data, id)
	delete(f.own, id)
	return nil
}

func truncate(t time.Time, bucket string) time.Time {
	t = t.UTC()
	if bucket == "day" {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	}
	return t.Truncate(time.Hour)
}

// Aggregate meniru semantik Mongo: hanya event yang pemilik efektifnya unit ini yang dihitung.
func (f *fakeUnit) Aggregate(_ context.Context, q AggregateQuery, down []string) (AggregateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return AggregateResult{}, errDown
	}
	isDown := map[string]bool{}
	for _, d := range down {
		isDown[d] = true
	}
	out := NewAggregateResult()
	for id, ev := range f.data {
		eff := f.name
		for _, o := range f.own[id] {
			if !isDown[o] {
				eff = o
				break
			}
		}
		if eff != f.name || ev.OccurredAt.Before(q.From) || !ev.OccurredAt.Before(q.To) {
			continue
		}
		bucket := truncate(ev.OccurredAt, q.Bucket).Unix()
		if out.Series[bucket] == nil {
			out.Series[bucket] = map[string]int{}
		}
		out.Series[bucket][ev.Category()]++
		out.Severity[string(ev.Severity)]++
		out.Area[ev.AreaName]++
		out.Total++
	}
	return out, nil
}

func (f *fakeUnit) Ping(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return errDown
	}
	return nil
}

type staticNodes map[string]string

func (s staticNodes) NodeOf(_ context.Context, unit string) (string, error) {
	if n, ok := s[unit]; ok {
		return n, nil
	}
	return "", errors.New("tidak diketahui")
}

func quietCfg() Config {
	return Config{ReplicationFactor: 2, VNodes: 128, UnitTimeout: 200 * time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func newCluster(t *testing.T, names ...string) (*Router, []*fakeUnit) {
	t.Helper()
	fakes := make([]*fakeUnit, len(names))
	units := make([]Unit, len(names))
	for i, n := range names {
		fakes[i] = newFake(n)
		units[i] = fakes[i]
	}
	r, err := New(units, quietCfg())
	if err != nil {
		t.Fatal(err)
	}
	return r, fakes
}

var t0 = time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)

func event(i int) hazard.Event {
	types := []hazard.Type{hazard.TypeSeismic, hazard.TypeVolcanic, hazard.TypeWildfire, hazard.TypeCoastal}
	return hazard.Event{
		HazardID:    fmt.Sprintf("hz-%06d", i),
		Source:      hazard.SourceBMKG,
		SourceRefID: fmt.Sprintf("ref-%06d", i),
		HazardType:  types[i%len(types)],
		Severity:    hazard.SeverityWaspada,
		AreaName:    "Area",
		OccurredAt:  t0.Add(time.Duration(i) * time.Second),
		// beberapa event sengaja berbagi ingested_at agar tie-break hazard_id teruji
		IngestedAt: t0.Add(time.Duration(i/3) * time.Second),
	}
}

func load(t *testing.T, r *Router, n int) []hazard.Event {
	t.Helper()
	events := make([]hazard.Event, n)
	for i := range events {
		events[i] = event(i)
		if err := r.SaveHazardEvent(events[i]); err != nil {
			t.Fatal(err)
		}
	}
	return events
}

func TestRingOwnersAreDistinctAndStable(t *testing.T) {
	ring := NewRing([]string{"a", "b", "c"}, 64)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("k%d", i)
		owners := ring.Owners(key, 2)
		if len(owners) != 2 || owners[0] == owners[1] {
			t.Fatalf("owners(%s) = %v, want 2 unit berbeda", key, owners)
		}
		if again := ring.Owners(key, 2); strings.Join(again, ",") != strings.Join(owners, ",") {
			t.Fatalf("owners tidak stabil: %v vs %v", owners, again)
		}
	}
	if got := ring.Owners("k", 9); len(got) != 3 {
		t.Fatalf("rf melebihi jumlah unit harus dibatasi, dapat %v", got)
	}
}

func TestRingSpreadsKeysEvenlyRegardlessOfKeyPattern(t *testing.T) {
	ring := NewRing([]string{"a", "b", "c"}, 128)
	counts := map[string]int{}
	const n = 30000
	for i := 0; i < n; i++ {
		// kunci berurutan, menyerupai gempa yang menumpuk pada satu wilayah dan satu rentang waktu
		counts[ring.Owners(fmt.Sprintf("hz-jalur-sunda-%08d", i), 1)[0]]++
	}
	for u, c := range counts {
		if share := float64(c) / n; share < 0.25 || share > 0.42 {
			t.Fatalf("unit %s memegang %.0f%% kunci, terlalu tidak merata: %v", u, share*100, counts)
		}
	}
}

func TestRingAddingAUnitMovesOnlyAFractionOfKeys(t *testing.T) {
	before := NewRing([]string{"a", "b", "c"}, 128)
	after := NewRing([]string{"a", "b", "c", "d"}, 128)
	moved := 0
	const n = 20000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%d", i)
		if before.Owners(key, 1)[0] != after.Owners(key, 1)[0] {
			moved++
		}
	}
	if share := float64(moved) / n; share > 0.40 {
		t.Fatalf("%.0f%% kunci berpindah saat menambah satu unit, seharusnya sekitar 25%%", share*100)
	}
}

func TestSaveReplicatesToTwoDifferentUnits(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	events := load(t, r, 300)

	total := 0
	for _, f := range fakes {
		total += f.count()
		if f.count() == len(events) {
			t.Fatalf("unit %s menyimpan seluruh data", f.name)
		}
	}
	if total != 2*len(events) {
		t.Fatalf("total salinan = %d, want %d (RF=2)", total, 2*len(events))
	}
	for _, ev := range events {
		holders := 0
		for _, f := range fakes {
			if f.has(ev.HazardID) {
				holders++
			}
		}
		if holders != 2 {
			t.Fatalf("%s memiliki %d salinan, want 2", ev.HazardID, holders)
		}
	}
}

func TestListEventsPaginatesInCorrectOrderWithoutDuplicates(t *testing.T) {
	r, _ := newCluster(t, "store-0", "store-1", "store-2")
	events := load(t, r, 200)

	want := append([]hazard.Event(nil), events...)
	sort.Slice(want, func(i, j int) bool { return less(want[i], want[j], NewestFirst) })

	var got []string
	cursor := ""
	pages := 0
	for {
		page, err := r.ListEvents(context.Background(), ListQuery{Limit: 50, Cursor: cursor, Order: NewestFirst})
		if err != nil {
			t.Fatal(err)
		}
		if page.Partial {
			t.Fatal("tidak boleh partial saat semua unit sehat")
		}
		for _, ev := range page.Events {
			got = append(got, ev.HazardID)
		}
		pages++
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if pages != 4 {
		t.Fatalf("jumlah halaman = %d, want 4", pages)
	}
	if len(got) != len(want) {
		t.Fatalf("total event = %d, want %d (duplikat atau hilang)", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i].HazardID {
			t.Fatalf("urutan salah pada indeks %d: %s, want %s", i, got[i], want[i].HazardID)
		}
	}
}

func TestListEventsOldestFirstAndFilters(t *testing.T) {
	r, _ := newCluster(t, "store-0", "store-1", "store-2")
	load(t, r, 80)

	page, err := r.ListEvents(context.Background(), ListQuery{Limit: 1000, Order: OldestFirst,
		Filter: Filter{HazardType: "WILDFIRE"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 20 {
		t.Fatalf("event WILDFIRE = %d, want 20", len(page.Events))
	}
	for i := 1; i < len(page.Events); i++ {
		if less(page.Events[i], page.Events[i-1], OldestFirst) {
			t.Fatal("urutan menaik dilanggar")
		}
	}
}

func TestListEventsRejectsBadCursor(t *testing.T) {
	r, _ := newCluster(t, "store-0", "store-1", "store-2")
	if _, err := r.ListEvents(context.Background(), ListQuery{Cursor: "bukan-cursor"}); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("err = %v, want ErrBadCursor", err)
	}
}

func TestOneNodeDownStillReturnsEveryEventAndIsNotPartial(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	events := load(t, r, 150)

	fakes[1].set(true)
	page, err := r.ListEvents(context.Background(), ListQuery{Limit: 1000, Order: NewestFirst})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != len(events) {
		t.Fatalf("event = %d, want %d: satu node mati tidak boleh menghilangkan data (RF=2)", len(page.Events), len(events))
	}
	if page.Partial {
		t.Fatal("jawaban lengkap tidak boleh ditandai partial")
	}
	if len(page.Unavailable) != 1 || page.Unavailable[0] != "store-1" {
		t.Fatalf("unavailable = %v, want [store-1]", page.Unavailable)
	}
}

func TestTwoNodesDownIsMarkedPartialInsteadOfFailing(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	load(t, r, 150)

	fakes[0].set(true)
	fakes[1].set(true)
	page, err := r.ListEvents(context.Background(), ListQuery{Limit: 1000, Order: NewestFirst})
	if err != nil {
		t.Fatalf("err = %v, want jawaban sebagian, bukan error", err)
	}
	if !page.Partial {
		t.Fatal("partial harus true karena sebagian data tidak punya salinan terjangkau")
	}
	if len(page.Events) == 0 {
		t.Fatal("event yang masih terjangkau tetap harus dikembalikan")
	}
}

func TestAllNodesDownReturnsErrUnavailable(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	load(t, r, 10)
	for _, f := range fakes {
		f.set(true)
	}
	if _, err := r.ListEvents(context.Background(), ListQuery{Limit: 10}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if err := r.SaveHazardEvent(event(1)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("save err = %v, want ErrUnavailable", err)
	}
	if err := r.Ping(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ping err = %v, want ErrUnavailable", err)
	}
}

func TestStuckNodeDoesNotHangTheRequest(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	events := load(t, r, 60)
	fakes[2].mu.Lock()
	fakes[2].blockQuery = true
	fakes[2].mu.Unlock()

	start := time.Now()
	page, err := r.ListEvents(context.Background(), ListQuery{Limit: 1000, Order: NewestFirst})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("permintaan menggantung %v karena satu unit macet", elapsed)
	}
	if len(page.Events) != len(events) {
		t.Fatalf("event = %d, want %d", len(page.Events), len(events))
	}

	// permintaan berikutnya langsung melewati unit yang sudah ditandai tidak terjangkau
	start = time.Now()
	if _, err := r.ListEvents(context.Background(), ListQuery{Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("unit yang ditandai down seharusnya dilewati seketika, makan %v", time.Since(start))
	}
}

func TestWritesContinueWhileOneNodeIsDownAndRepairCompletesThemLater(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	fakes[0].set(true)
	events := load(t, r, 120) // tidak boleh gagal walau store-0 mati
	if len(events) != 120 {
		t.Fatal("penulisan harus berjalan terus")
	}

	fakes[0].set(false)
	r.pingAll(context.Background()) // pemeriksaan kesehatan menandai store-0 pulih
	if _, err := r.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		owners := r.Owners(ev.HazardID)
		for _, name := range owners {
			f := fakeByName(fakes, name)
			if !f.has(ev.HazardID) {
				t.Fatalf("%s belum ada di pemilik %s setelah perbaikan", ev.HazardID, name)
			}
		}
	}
}

func fakeByName(fakes []*fakeUnit, name string) *fakeUnit {
	for _, f := range fakes {
		if f.name == name {
			return f
		}
	}
	return nil
}

func TestWipedNodeIsRebuiltWithoutLosingAnyEvent(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	events := load(t, r, 300)

	// node mati dan seluruh datanya dihapus
	victim := fakes[1]
	victim.set(true)
	victim.wipe()

	// selama node mati, semua event tetap terbaca
	page, err := r.ListEvents(context.Background(), ListQuery{Limit: 1000, Order: NewestFirst})
	if err != nil || len(page.Events) != len(events) {
		t.Fatalf("selama node mati: %d event, err=%v, want %d", len(page.Events), err, len(events))
	}

	// node kembali dengan disk kosong
	victim.set(false)
	r.pingAll(context.Background())
	stats, err := r.RepairOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Replicated == 0 {
		t.Fatal("perbaikan seharusnya menyalin data ke node yang kosong")
	}
	if victim.count() == 0 {
		t.Fatal("node pengganti tetap kosong")
	}

	// kembali mampu bertahan dari kehilangan satu node berikutnya
	for _, f := range fakes {
		if f == victim {
			continue
		}
		f.set(true)
		page, err := r.ListEvents(context.Background(), ListQuery{Limit: 1000, Order: NewestFirst})
		f.set(false)
		r.pingAll(context.Background())
		if err != nil || len(page.Events) != len(events) {
			t.Fatalf("setelah pulih, mematikan %s menghasilkan %d event (err=%v), want %d", f.name, len(page.Events), err, len(events))
		}
	}
}

func TestAddingANodeRebalancesDataAndRemovesStaleCopies(t *testing.T) {
	r3, fakes := newCluster(t, "store-0", "store-1", "store-2")
	events := load(t, r3, 600)

	// operator menambah store-3: router baru dibangun dari konfigurasi unit yang diperbarui
	fresh := newFake("store-3")
	units := []Unit{fakes[0], fakes[1], fakes[2], fresh}
	r4, err := New(units, quietCfg())
	if err != nil {
		t.Fatal(err)
	}

	// selama rebalancing, pembacaan tetap lengkap tanpa menunggu perbaikan
	page, err := r4.ListEvents(context.Background(), ListQuery{Limit: 1000, Order: NewestFirst})
	if err != nil || len(page.Events) != len(events) {
		t.Fatalf("sebelum perbaikan: %d event, err=%v, want %d", len(page.Events), err, len(events))
	}
	// penulisan berjalan terus dan langsung memakai cincin baru
	extra := event(10_000)
	if err := r4.SaveHazardEvent(extra); err != nil {
		t.Fatal(err)
	}

	stats, err := r4.RepairOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fresh.count() == 0 || stats.Replicated == 0 {
		t.Fatalf("node baru tidak ikut menanggung data: %d event, stats=%+v", fresh.count(), stats)
	}

	all := append(events, extra)
	total := 0
	for _, f := range append(fakes, fresh) {
		total += f.count()
	}
	if total != 2*len(all) {
		t.Fatalf("total salinan = %d, want %d: salinan usang belum terhapus atau ada yang hilang", total, 2*len(all))
	}
	for _, ev := range all {
		for _, name := range r4.Owners(ev.HazardID) {
			var f *fakeUnit
			for _, c := range append(fakes, fresh) {
				if c.name == name {
					f = c
				}
			}
			if !f.has(ev.HazardID) {
				t.Fatalf("%s tidak ada di pemilik %s", ev.HazardID, name)
			}
		}
	}
	// beban terbagi ke empat node, tidak ada yang memegang seluruh data
	for _, f := range append(fakes, fresh) {
		if f.count() >= len(all) {
			t.Fatalf("%s menyimpan seluruh data", f.name)
		}
	}
}

func TestChecksumMatchesTheSpecAndSurvivesOneNodeDown(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	events := load(t, r, 90)

	refs := make([]string, 0, len(events))
	for _, ev := range events {
		refs = append(refs, ev.SourceRefID)
	}
	sort.Strings(refs)
	sum := sha256.Sum256([]byte(strings.Join(refs, "\n")))
	want := hex.EncodeToString(sum[:])

	got, err := r.Checksum(context.Background(), "BMKG")
	if err != nil {
		t.Fatal(err)
	}
	if got.Count != len(events) || got.Checksum != want || got.Partial {
		t.Fatalf("checksum = %+v, want count=%d checksum=%s", got, len(events), want)
	}

	fakes[0].set(true)
	got, err = r.Checksum(context.Background(), "BMKG")
	if err != nil {
		t.Fatal(err)
	}
	if got.Checksum != want || got.Partial {
		t.Fatalf("satu node mati: checksum = %+v, want %s dan tidak partial", got, want)
	}

	other, err := r.Checksum(context.Background(), "PVMBG")
	if err != nil || other.Count != 0 {
		t.Fatalf("sumber lain: %+v err=%v, want count 0", other, err)
	}
	emptySum := sha256.Sum256(nil)
	if other.Checksum != hex.EncodeToString(emptySum[:]) {
		t.Fatalf("checksum himpunan kosong = %s", other.Checksum)
	}
}

func TestLocationReportsPrimaryThenReplicaWithNodeNames(t *testing.T) {
	fakes := []*fakeUnit{newFake("store-0"), newFake("store-1"), newFake("store-2")}
	cfg := quietCfg()
	cfg.Nodes = staticNodes{"store-0": "node-a", "store-1": "node-b", "store-2": "node-c"}
	r, err := New([]Unit{fakes[0], fakes[1], fakes[2]}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ev := event(7)
	if err := r.SaveHazardEvent(ev); err != nil {
		t.Fatal(err)
	}

	res, err := r.Location(context.Background(), ev.HazardID)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Found || res.Partial || len(res.Locations) != 2 {
		t.Fatalf("res = %+v, want 2 lokasi lengkap", res)
	}
	owners := r.Owners(ev.HazardID)
	if res.Locations[0].Role != "primary" || res.Locations[0].Unit != owners[0] {
		t.Fatalf("lokasi pertama = %+v, want primary %s", res.Locations[0], owners[0])
	}
	if res.Locations[1].Role != "replica" || res.Locations[1].Unit != owners[1] {
		t.Fatalf("lokasi kedua = %+v, want replica %s", res.Locations[1], owners[1])
	}
	if res.Locations[0].Node == "" || res.Locations[1].Node == "" || res.Locations[0].Node == res.Locations[1].Node {
		t.Fatalf("node = %+v, harus berbeda dan terisi", res.Locations)
	}

	missing, err := r.Location(context.Background(), "hz-tidak-ada")
	if err != nil || missing.Found || missing.Partial {
		t.Fatalf("missing = %+v err=%v, want tidak ditemukan dan tidak partial", missing, err)
	}
}

func TestSingleUnitRouterBehavesLikeAPlainStore(t *testing.T) {
	r, _ := newCluster(t, "store-0")
	events := load(t, r, 40)
	if r.ReplicationFactor() != 1 {
		t.Fatalf("rf = %d, want 1 untuk satu unit", r.ReplicationFactor())
	}
	page, err := r.ListEvents(context.Background(), ListQuery{Limit: 100})
	if err != nil || len(page.Events) != len(events) || page.Partial {
		t.Fatalf("page = %d event partial=%v err=%v", len(page.Events), page.Partial, err)
	}
}

func TestNewRejectsDuplicateUnitNames(t *testing.T) {
	if _, err := New([]Unit{newFake("a"), newFake("a")}, quietCfg()); err == nil {
		t.Fatal("nama unit ganda harus ditolak")
	}
	if _, err := New(nil, quietCfg()); err == nil {
		t.Fatal("tanpa unit harus ditolak")
	}
}

func TestRepairLetsTheNewestVersionWin(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	ev := event(1)
	if err := r.SaveHazardEvent(ev); err != nil {
		t.Fatal(err)
	}
	owners := r.Owners(ev.HazardID)
	stale, fresh := fakeByName(fakes, owners[0]), fakeByName(fakes, owners[1])

	// replika kedua memegang versi lebih baru (mis. pembaruan kebakaran yang hanya sampai di satu unit)
	updated := ev
	updated.Severity = hazard.SeverityAwas
	updated.IngestedAt = ev.IngestedAt.Add(time.Minute)
	if err := fresh.Upsert(context.Background(), updated, owners); err != nil {
		t.Fatal(err)
	}

	if _, err := r.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := stale.Get(context.Background(), ev.HazardID)
	if got == nil || got.Severity != hazard.SeverityAwas {
		t.Fatalf("versi usang tidak diperbarui: %+v", got)
	}
}

// groundTruth menghitung agregasi langsung dari daftar event, tanpa penyimpanan tersebar.
func groundTruth(events []hazard.Event, q AggregateQuery) AggregateResult {
	out := NewAggregateResult()
	for _, ev := range events {
		if ev.OccurredAt.Before(q.From) || !ev.OccurredAt.Before(q.To) {
			continue
		}
		bucket := truncate(ev.OccurredAt, q.Bucket).Unix()
		if out.Series[bucket] == nil {
			out.Series[bucket] = map[string]int{}
		}
		out.Series[bucket][ev.Category()]++
		out.Severity[string(ev.Severity)]++
		out.Area[ev.AreaName]++
		out.Total++
	}
	return out
}

func sameAggregate(t *testing.T, got, want AggregateResult) {
	t.Helper()
	if got.Total != want.Total {
		t.Fatalf("total = %d, want %d", got.Total, want.Total)
	}
	for k, n := range want.Severity {
		if got.Severity[k] != n {
			t.Fatalf("severity %s = %d, want %d", k, got.Severity[k], n)
		}
	}
	for k, n := range want.Area {
		if got.Area[k] != n {
			t.Fatalf("area %s = %d, want %d", k, got.Area[k], n)
		}
	}
	for ts, cats := range want.Series {
		for c, n := range cats {
			if got.Series[ts][c] != n {
				t.Fatalf("series[%d][%s] = %d, want %d", ts, c, got.Series[ts][c], n)
			}
		}
	}
	gotSum := 0
	for _, cats := range got.Series {
		for _, n := range cats {
			gotSum += n
		}
	}
	if gotSum != want.Total {
		t.Fatalf("jumlah series = %d, want %d (ada yang terhitung ganda atau hilang)", gotSum, want.Total)
	}
}

func analyticsEvent(i int) hazard.Event {
	ev := event(i)
	ev.AreaName = []string{"Sumatera Barat", "Jawa Barat", "Kalimantan Tengah"}[i%3]
	ev.Severity = []hazard.Severity{hazard.SeverityNormal, hazard.SeverityWaspada, hazard.SeveritySiaga, hazard.SeverityAwas}[i%4]
	ev.OccurredAt = t0.Add(time.Duration(i) * 7 * time.Minute)
	if ev.HazardType == hazard.TypeSeismic && i%8 == 0 {
		ev.Attributes = map[string]any{hazard.TsunamiWarningKey: "Siaga"}
	}
	return ev
}

func loadAnalytics(t *testing.T, r *Router, n int) []hazard.Event {
	t.Helper()
	events := make([]hazard.Event, n)
	for i := range events {
		events[i] = analyticsEvent(i)
		if err := r.SaveHazardEvent(events[i]); err != nil {
			t.Fatal(err)
		}
	}
	return events
}

var analyticsRange = AggregateQuery{From: t0.Add(-time.Hour), To: t0.Add(100 * time.Hour), Bucket: "hour"}

func TestAnalyticsMatchesDirectCountWithoutDoubleCountingReplicas(t *testing.T) {
	r, _ := newCluster(t, "store-0", "store-1", "store-2")
	events := loadAnalytics(t, r, 400)

	got, err := r.Analytics(context.Background(), analyticsRange)
	if err != nil {
		t.Fatal(err)
	}
	sameAggregate(t, got.AggregateResult, groundTruth(events, analyticsRange))
	if got.Partial || got.Rebalancing || len(got.Unavailable) != 0 {
		t.Fatalf("keadaan normal: partial=%v rebalancing=%v unavailable=%v", got.Partial, got.Rebalancing, got.Unavailable)
	}
	if got.Series[truncate(t0, "hour").Unix()][hazard.CategoryTsunami] == 0 {
		t.Fatal("kategori TSUNAMI seharusnya terhitung terpisah dari SEISMIC")
	}
}

func TestAnalyticsStaysExactWhenOneNodeIsDown(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	events := loadAnalytics(t, r, 400)

	for _, f := range fakes {
		f.set(true)
		r.markDown(f.name, errDown)
		got, err := r.Analytics(context.Background(), analyticsRange)
		if err != nil {
			t.Fatal(err)
		}
		sameAggregate(t, got.AggregateResult, groundTruth(events, analyticsRange))
		if got.Partial {
			t.Fatalf("satu node mati (%s) tidak boleh membuat angka partial", f.name)
		}
		if len(got.Unavailable) != 1 || got.Unavailable[0] != f.name {
			t.Fatalf("unavailable = %v, want [%s]", got.Unavailable, f.name)
		}
		f.set(false)
		r.markUp(f.name)
	}
}

func TestAnalyticsRecoversWhenAUnitDiesDuringTheCalculation(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	events := loadAnalytics(t, r, 300)

	// unit belum ditandai down, tetapi gagal saat dihitung: hitungan diulang dengan replikanya
	fakes[1].set(true)
	got, err := r.Analytics(context.Background(), analyticsRange)
	if err != nil {
		t.Fatal(err)
	}
	sameAggregate(t, got.AggregateResult, groundTruth(events, analyticsRange))
}

func TestAnalyticsIsMarkedPartialWhenDataIsUnreachable(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	events := loadAnalytics(t, r, 300)

	fakes[0].set(true)
	fakes[1].set(true)
	r.markDown("store-0", errDown)
	r.markDown("store-1", errDown)
	got, err := r.Analytics(context.Background(), analyticsRange)
	if err != nil {
		t.Fatalf("err = %v, want angka sebagian, bukan error", err)
	}
	if !got.Partial {
		t.Fatal("angka harus ditandai partial")
	}
	if got.Total >= groundTruth(events, analyticsRange).Total {
		t.Fatalf("total %d seharusnya lebih kecil dari data sebenarnya karena sebagian tidak terjangkau", got.Total)
	}
}

func TestAnalyticsFlagsRebalancingUntilRepairCompletes(t *testing.T) {
	r, fakes := newCluster(t, "store-0", "store-1", "store-2")
	fakes[0].set(true)
	events := loadAnalytics(t, r, 150) // sebagian event hanya tersimpan di satu salinan
	fakes[0].set(false)
	r.pingAll(context.Background())

	got, err := r.Analytics(context.Background(), analyticsRange)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Rebalancing {
		t.Fatal("harus ditandai rebalancing selama ada salinan yang belum lengkap")
	}

	if _, err := r.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err = r.Analytics(context.Background(), analyticsRange)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rebalancing {
		t.Fatal("setelah perbaikan selesai, tanda rebalancing harus hilang")
	}
	sameAggregate(t, got.AggregateResult, groundTruth(events, analyticsRange))
}

func TestAnalyticsDailyBucketsAndRangeFilter(t *testing.T) {
	r, _ := newCluster(t, "store-0", "store-1", "store-2")
	events := loadAnalytics(t, r, 500)
	q := AggregateQuery{From: t0.Add(10 * time.Hour), To: t0.Add(30 * time.Hour), Bucket: "day"}
	got, err := r.Analytics(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	want := groundTruth(events, q)
	if want.Total == 0 || want.Total == len(events) {
		t.Fatalf("rentang uji tidak menyaring apa pun: %d dari %d", want.Total, len(events))
	}
	sameAggregate(t, got.AggregateResult, want)
}
