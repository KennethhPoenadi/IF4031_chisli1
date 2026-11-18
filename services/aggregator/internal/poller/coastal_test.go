package poller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/pantaiclient"
	"aggregator/internal/resilience"
)

type fakeCoastalSource struct {
	stations     pantaiclient.StationPage
	stationsErr  error
	stationCalls int
	fetch        func(cursor string) (pantaiclient.Page, error)
	fetchCursors []string
}

func (f *fakeCoastalSource) Stations(context.Context) (pantaiclient.StationPage, error) {
	f.stationCalls++
	return f.stations, f.stationsErr
}

func (f *fakeCoastalSource) Fetch(_ context.Context, cursor string) (pantaiclient.Page, error) {
	f.fetchCursors = append(f.fetchCursors, cursor)
	if f.fetch == nil {
		return pantaiclient.Page{}, nil
	}
	return f.fetch(cursor)
}

type fakeCoastalState struct {
	cursor        string
	loadErr       error
	saveErr       error
	quarantineErr error
	savedCursors  []string
	rejections    []CoastalRejection
}

func (f *fakeCoastalState) LoadCursor(context.Context) (string, error) {
	return f.cursor, f.loadErr
}

func (f *fakeCoastalState) SaveCursor(_ context.Context, cursor string) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.cursor = cursor
	f.savedCursors = append(f.savedCursors, cursor)
	return nil
}

func (f *fakeCoastalState) Quarantine(_ context.Context, rejection CoastalRejection) error {
	if f.quarantineErr != nil {
		return f.quarantineErr
	}
	f.rejections = append(f.rejections, rejection)
	return nil
}

func coastalStation() map[string]any {
	return map[string]any{
		"station_id": "station-1", "name": "Padang", "province": "Sumatera Barat",
		"latitude": -0.9492, "longitude": 100.3543,
	}
}

func coastalObservation(id string, critical bool) map[string]any {
	return map[string]any{
		"observation_id": id, "station_id": "station-1", "observed_at": "2026-11-10T08:30:00Z",
		"sea_level_anomaly_m": 0.42, "wave_height_m": 1.8, "quality": "ok", "critical": critical,
	}
}

func configureCoastalTest(t *testing.T, p *Poller, source *fakeCoastalSource, state *fakeCoastalState, breaker *resilience.Breaker, now *time.Time) {
	t.Helper()
	if err := p.ConfigureCoastal(CoastalConfig{
		Source: source, StateStore: state, Breaker: breaker, PollInterval: time.Second,
		StaleAfter: time.Minute, StationCacheTTL: time.Hour, Now: func() time.Time { return *now },
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPollCoastalPrioritizesCriticalQuarantinesInvalidAndAdvancesCursor(t *testing.T) {
	now := time.Date(2026, 11, 10, 8, 30, 10, 0, time.UTC)
	r := newRig()
	source := &fakeCoastalSource{stations: pantaiclient.StationPage{Stations: []map[string]any{coastalStation()}}}
	source.fetch = func(cursor string) (pantaiclient.Page, error) {
		if cursor == "cursor-1" {
			invalid := coastalObservation("invalid", false)
			invalid["station_id"] = "missing"
			return pantaiclient.Page{
				GeneratedAt:  now.Format(time.RFC3339),
				Observations: []map[string]any{coastalObservation("normal", false), invalid, coastalObservation("critical", true)},
				NextCursor:   "cursor-2",
			}, nil
		}
		return pantaiclient.Page{GeneratedAt: now.Format(time.RFC3339), Observations: []map[string]any{}}, nil
	}
	state := &fakeCoastalState{}
	breaker := resilience.New(resilience.Config{FailureThreshold: 2})
	configureCoastalTest(t, r.poller, source, state, breaker, &now)

	if got := r.poller.pollCoastal(context.Background(), "cursor-1"); got != "cursor-2" {
		t.Fatalf("cursor = %q; want cursor-2", got)
	}
	if got := r.store.ids(); len(got) != 2 || got[0] != "hz-critical" || got[1] != "hz-normal" {
		t.Fatalf("saved event order = %v", got)
	}
	if len(state.rejections) != 1 || state.rejections[0].Observation["observation_id"] != "invalid" || state.rejections[0].Reason == "" {
		t.Fatalf("rejections = %#v", state.rejections)
	}
	if len(state.savedCursors) != 1 || state.savedCursors[0] != "cursor-2" {
		t.Fatalf("saved cursors = %v", state.savedCursors)
	}
	if breaker.State() != resilience.Closed {
		t.Fatalf("breaker state = %v", breaker.State())
	}
}

func TestFollowerDoesNotPollCoastalUpstream(t *testing.T) {
	now := time.Date(2026, 11, 10, 8, 30, 10, 0, time.UTC)
	r := newRig()
	source := &fakeCoastalSource{stations: pantaiclient.StationPage{Stations: []map[string]any{coastalStation()}}}
	state := &fakeCoastalState{}
	breaker := resilience.New(resilience.Config{FailureThreshold: 2})
	configureCoastalTest(t, r.poller, source, state, breaker, &now)
	r.poller.WithLeadership(&fakeLeadership{leader: false})

	if got := r.poller.pollCoastal(context.Background(), "cursor-1"); got != "cursor-1" {
		t.Fatalf("cursor = %q, want unchanged cursor-1", got)
	}
	if source.stationCalls != 0 || len(source.fetchCursors) != 0 {
		t.Fatalf("follower called Sensor Pantai: stations=%d fetch=%d", source.stationCalls, len(source.fetchCursors))
	}
	if len(state.savedCursors) != 0 || len(state.rejections) != 0 {
		t.Fatal("follower changed Sensor Pantai state")
	}
}

func TestPollCoastalSourceFailureOpensBreakerAndFailsFast(t *testing.T) {
	now := time.Date(2026, 11, 10, 8, 30, 10, 0, time.UTC)
	r := newRig()
	source := &fakeCoastalSource{stations: pantaiclient.StationPage{Stations: []map[string]any{coastalStation()}}}
	source.fetch = func(string) (pantaiclient.Page, error) { return pantaiclient.Page{}, errors.New("sensor unavailable") }
	state := &fakeCoastalState{}
	breaker := resilience.New(resilience.Config{FailureThreshold: 1, OpenDuration: time.Minute, Now: func() time.Time { return now }})
	configureCoastalTest(t, r.poller, source, state, breaker, &now)

	r.poller.pollCoastal(context.Background(), "cursor-1")
	r.poller.pollCoastal(context.Background(), "cursor-1")
	if len(source.fetchCursors) != 1 {
		t.Fatalf("fetch calls = %d; want 1", len(source.fetchCursors))
	}
	if breaker.State() != resilience.Open {
		t.Fatalf("breaker state = %v", breaker.State())
	}
}

func TestPollCoastalDownstreamFailureDoesNotOpenSensorBreaker(t *testing.T) {
	now := time.Date(2026, 11, 10, 8, 30, 10, 0, time.UTC)
	r := newRig()
	source := &fakeCoastalSource{stations: pantaiclient.StationPage{Stations: []map[string]any{coastalStation()}}}
	source.fetch = func(string) (pantaiclient.Page, error) {
		return pantaiclient.Page{GeneratedAt: now.Format(time.RFC3339), Observations: []map[string]any{coastalObservation("critical", true)}}, nil
	}
	state := &fakeCoastalState{}
	breaker := resilience.New(resilience.Config{FailureThreshold: 1})
	configureCoastalTest(t, r.poller, source, state, breaker, &now)
	r.store.err = func(_ hazard.Event) error { return errors.New("store unavailable") }

	r.poller.pollCoastal(context.Background(), "cursor-1")
	if breaker.State() != resilience.Closed {
		t.Fatalf("store failure changed breaker state to %v", breaker.State())
	}
}

func TestPollCoastalUsesCachedStationsDuringRefreshFailure(t *testing.T) {
	now := time.Date(2026, 11, 10, 8, 30, 10, 0, time.UTC)
	r := newRig()
	source := &fakeCoastalSource{stations: pantaiclient.StationPage{Stations: []map[string]any{coastalStation()}}}
	source.fetch = func(string) (pantaiclient.Page, error) {
		return pantaiclient.Page{GeneratedAt: now.Format(time.RFC3339), Observations: []map[string]any{}}, nil
	}
	state := &fakeCoastalState{}
	breaker := resilience.New(resilience.Config{FailureThreshold: 2})
	configureCoastalTest(t, r.poller, source, state, breaker, &now)
	r.poller.coastalStationCacheTTL = time.Second

	r.poller.pollCoastal(context.Background(), "")
	now = now.Add(2 * time.Second)
	source.stationsErr = errors.New("station endpoint unavailable")
	r.poller.pollCoastal(context.Background(), "")
	if source.stationCalls != 2 {
		t.Fatalf("station calls = %d; want 2", source.stationCalls)
	}
	if breaker.State() != resilience.Closed {
		t.Fatalf("one failed refresh changed breaker state to %v", breaker.State())
	}
}

func coastalRig(t *testing.T, now *time.Time, generated *time.Time) (*rig, *resilience.Breaker) {
	t.Helper()
	r := newRig()
	source := &fakeCoastalSource{stations: pantaiclient.StationPage{Stations: []map[string]any{coastalStation()}}}
	source.fetch = func(string) (pantaiclient.Page, error) {
		return pantaiclient.Page{GeneratedAt: generated.Format(time.RFC3339),
			Observations: []map[string]any{coastalObservation(fmt.Sprintf("obs-%d", generated.Unix()), false)}}, nil
	}
	breaker := resilience.New(resilience.Config{FailureThreshold: 1})
	configureCoastalTest(t, r.poller, source, &fakeCoastalState{}, breaker, now)
	return r, breaker
}

// Sensor berjalan pada jam virtual enam hari di belakang jam lokal dan dipercepat. Data tetap diterima
// karena jamnya terus maju.
func TestCoastalVirtualClockFarBehindWallClockIsNotStale(t *testing.T) {
	now := time.Date(2026, 11, 18, 2, 20, 0, 0, time.UTC)
	generated := time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)
	r, breaker := coastalRig(t, &now, &generated)
	for i := 0; i < 5; i++ {
		r.poller.pollCoastal(context.Background(), "")
		now = now.Add(time.Second)
		generated = generated.Add(time.Minute) // 60x lebih cepat dari jam lokal
	}
	if len(r.store.ids()) != 5 || breaker.State() != resilience.Closed {
		t.Fatalf("tersimpan %d event, breaker %v; jam virtual yang maju tidak boleh dianggap basi", len(r.store.ids()), breaker.State())
	}
}

// Sensor tetap menjawab tetapi jamnya berhenti: data basi ditolak dan breaker terbuka.
func TestCoastalFrozenSensorClockIsRejectedAsStale(t *testing.T) {
	now := time.Date(2026, 11, 18, 2, 20, 0, 0, time.UTC)
	generated := time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)
	r, breaker := coastalRig(t, &now, &generated)
	r.poller.pollCoastal(context.Background(), "")
	now = now.Add(30 * time.Second)
	r.poller.pollCoastal(context.Background(), "") // belum melewati batas satu menit
	if breaker.State() != resilience.Closed {
		t.Fatal("jam yang diam kurang dari batas belum boleh dianggap basi")
	}
	now = now.Add(2 * time.Minute)
	r.poller.pollCoastal(context.Background(), "")
	if breaker.State() == resilience.Closed {
		t.Fatal("jam sensor yang diam lebih dari batas harus ditolak sebagai data basi")
	}
}

// Sensor dimulai ulang sehingga jamnya mundur: garis waktu baru diterima, bukan ditolak selamanya.
func TestCoastalSensorRestartWithEarlierClockIsAccepted(t *testing.T) {
	now := time.Date(2026, 11, 18, 2, 20, 0, 0, time.UTC)
	generated := time.Date(2026, 11, 12, 5, 0, 0, 0, time.UTC)
	r, breaker := coastalRig(t, &now, &generated)
	r.poller.pollCoastal(context.Background(), "")
	generated = time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	now = now.Add(time.Second)
	r.poller.pollCoastal(context.Background(), "")
	if len(r.store.ids()) != 2 || breaker.State() != resilience.Closed {
		t.Fatalf("tersimpan %d event, breaker %v; jam yang mundur harus diterima sebagai garis waktu baru", len(r.store.ids()), breaker.State())
	}
}
