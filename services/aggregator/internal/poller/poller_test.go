package poller

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/pvmbgclient"
)

type fakeSeismic struct {
	mu          sync.Mutex
	events      func(since string) ([]map[string]any, error)
	warnings    func(since string) ([]map[string]any, error)
	eventSince  []string
	warnSince   []string
	eventCorrID []string
	warnCorrID  []string
}

func (f *fakeSeismic) SeismicEvents(_ context.Context, since string, correlationID string) ([]map[string]any, error) {
	f.mu.Lock()
	f.eventSince = append(f.eventSince, since)
	f.eventCorrID = append(f.eventCorrID, correlationID)
	fn := f.events
	f.mu.Unlock()
	if fn == nil {
		return nil, nil
	}
	return fn(since)
}

func (f *fakeSeismic) TsunamiWarnings(_ context.Context, since string, correlationID string) ([]map[string]any, error) {
	f.mu.Lock()
	f.warnSince = append(f.warnSince, since)
	f.warnCorrID = append(f.warnCorrID, correlationID)
	fn := f.warnings
	f.mu.Unlock()
	if fn == nil {
		return nil, nil
	}
	return fn(since)
}

func (f *fakeSeismic) eventCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.eventSince)
}

type fakeVolcanic struct {
	mu      sync.Mutex
	reports func(ctx context.Context, since string) ([]map[string]any, error)
	since   []string
	corrID  []string
}

func (f *fakeVolcanic) VolcanicReports(ctx context.Context, since string, correlationID string) ([]map[string]any, error) {
	f.mu.Lock()
	f.since = append(f.since, since)
	f.corrID = append(f.corrID, correlationID)
	fn := f.reports
	f.mu.Unlock()
	if fn == nil {
		return nil, nil
	}
	return fn(ctx, since)
}

func (f *fakeVolcanic) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.since)
}

type fakeStore struct {
	mu       sync.Mutex
	saved    []hazard.Event
	attempts int
	err      func(hazard.Event) error
}

func (f *fakeStore) SaveHazardEvent(event hazard.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.err != nil {
		if err := f.err(event); err != nil {
			return err
		}
	}
	f.saved = append(f.saved, event)
	return nil
}

func (f *fakeStore) ids() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.saved))
	for _, e := range f.saved {
		out = append(out, e.HazardID)
	}
	return out
}

type fakePublisher struct {
	mu     sync.Mutex
	events []hazard.Event
	corrID []string
	err    error
}

func (f *fakePublisher) Publish(_ context.Context, event hazard.Event, correlationID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, event)
	f.corrID = append(f.corrID, correlationID)
	return nil
}

type rig struct {
	poller *Poller
	bmkg   *fakeSeismic
	pvmbg  *fakeVolcanic
	store  *fakeStore
	pub    *fakePublisher
}

func newRig() *rig {
	r := &rig{bmkg: &fakeSeismic{}, pvmbg: &fakeVolcanic{}, store: &fakeStore{}, pub: &fakePublisher{}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r.poller = New(r.bmkg, r.pvmbg, r.store, r.pub, 5*time.Millisecond, 30*time.Second, log)
	return r
}

func seismicRaw(id string, magnitude float64) map[string]any {
	return map[string]any{
		"event_id":          id,
		"magnitude":         magnitude,
		"depth_km":          10.0,
		"epicenter_lat":     -6.5,
		"epicenter_lon":     105.2,
		"region_name":       "Selat Sunda",
		"occurred_at":       "2026-10-08T01:02:03Z",
		"potential_tsunami": false,
	}
}

func warningRaw(relatedID, level string) map[string]any {
	return map[string]any{
		"warning_id":       "tsunami-" + relatedID,
		"related_event_id": relatedID,
		"threat_level":     level,
	}
}

func volcanicRaw(id string) map[string]any {
	return map[string]any{
		"report_id":           id,
		"volcano_id":          "G-001",
		"alert_level":         "Siaga",
		"eruption_count_24h":  2.0,
		"ash_column_height_m": 1500.0,
		"reported_at":         "2026-10-08T01:02:03Z",
	}
}

func staticEvents(events ...map[string]any) func(string) ([]map[string]any, error) {
	return func(string) ([]map[string]any, error) { return events, nil }
}

func staticReports(reports ...map[string]any) func(context.Context, string) ([]map[string]any, error) {
	return func(context.Context, string) ([]map[string]any, error) { return reports, nil }
}

func failingReports(err error) func(context.Context, string) ([]map[string]any, error) {
	return func(context.Context, string) ([]map[string]any, error) { return nil, err }
}

// ---- BMKG ----

func TestPollBMKGPersistsPublishesAndAdvancesCursor(t *testing.T) {
	r := newRig()
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 5.5), seismicRaw("eq-2", 4.0))

	before := time.Now().UTC().Add(-time.Second)
	r.poller.pollBMKG(context.Background())

	if got, want := r.store.ids(), []string{"hz-eq-1", "hz-eq-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("saved = %v, want %v", got, want)
	}
	if len(r.pub.events) != 2 {
		t.Fatalf("published %d events, want 2", len(r.pub.events))
	}
	if r.bmkg.eventSince[0] != "" {
		t.Fatalf("first poll must ask for everything, got since=%q", r.bmkg.eventSince[0])
	}

	cursor, err := time.Parse(cursorFormat, r.poller.bmkgSince)
	if err != nil {
		t.Fatalf("cursor %q is not %s: %v", r.poller.bmkgSince, cursorFormat, err)
	}
	if cursor.Before(before) || cursor.After(time.Now().UTC().Add(time.Second)) {
		t.Fatalf("cursor %s is not close to now", cursor)
	}

	firstCursor := r.poller.bmkgSince

	r.poller.pollBMKG(context.Background())
	if r.bmkg.eventSince[1] != firstCursor || r.bmkg.warnSince[1] != firstCursor {
		t.Fatalf("second poll must send the first cursor %q to both endpoints, got events=%q warnings=%q",
			firstCursor, r.bmkg.eventSince[1], r.bmkg.warnSince[1])
	}
}

func TestPollBMKGAppliesTsunamiWarningToSeverity(t *testing.T) {
	r := newRig()
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 4.0))
	r.bmkg.warnings = func(string) ([]map[string]any, error) {
		return []map[string]any{warningRaw("eq-1", "Awas")}, nil
	}

	r.poller.pollBMKG(context.Background())

	if len(r.store.saved) != 1 || r.store.saved[0].Severity != hazard.SeverityAwas {
		t.Fatalf("saved = %#v, want one AWAS event", r.store.saved)
	}
}

func TestPollBMKGForwardsOneCorrelationIDThroughTheWholeCycle(t *testing.T) {
	r := newRig()
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 5.5))

	r.poller.pollBMKG(context.Background())

	id := r.bmkg.eventCorrID[0]
	if len(id) != 32 {
		t.Fatalf("correlation id %q should be 32 hex chars", id)
	}
	if r.bmkg.warnCorrID[0] != id || r.pub.corrID[0] != id {
		t.Fatalf("correlation id changed inside one cycle: events=%q warnings=%q publish=%q", id, r.bmkg.warnCorrID[0], r.pub.corrID[0])
	}

	r.poller.pollBMKG(context.Background())
	if r.bmkg.eventCorrID[1] == id {
		t.Fatal("each polling cycle must get its own correlation id")
	}
}

func TestPollBMKGSkipsTheCycleWhenEventsFail(t *testing.T) {
	r := newRig()
	r.bmkg.events = func(string) ([]map[string]any, error) { return nil, errors.New("bmkg down") }

	r.poller.pollBMKG(context.Background())

	if len(r.store.saved) != 0 || r.poller.bmkgSince != "" {
		t.Fatalf("a failed poll must not save anything or move the cursor: saved=%v cursor=%q", r.store.ids(), r.poller.bmkgSince)
	}
	if len(r.bmkg.warnSince) != 0 {
		t.Fatal("warnings must not be requested after the events call failed")
	}
}

func TestPollBMKGSkipsTheCycleWhenWarningsFailToAvoidWrongSeverity(t *testing.T) {
	r := newRig()
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 4.0))
	r.bmkg.warnings = func(string) ([]map[string]any, error) { return nil, errors.New("warnings down") }

	r.poller.pollBMKG(context.Background())

	if len(r.store.saved) != 0 || len(r.pub.events) != 0 {
		t.Fatalf("an event must not be stored without its warning: saved=%v", r.store.ids())
	}
	if r.poller.bmkgSince != "" {
		t.Fatal("cursor must not advance when the cycle was skipped")
	}
}

func TestPollBMKGRefetchesEventForLateTsunamiWarning(t *testing.T) {
	r := newRig()
	r.poller.bmkgSince = "2026-01-01T00:00:00Z"
	r.bmkg.events = func(since string) ([]map[string]any, error) {
		if since == "" {
			return []map[string]any{seismicRaw("new-1", 4.0), seismicRaw("old-1", 4.0), seismicRaw("old-2", 4.0)}, nil
		}
		return []map[string]any{seismicRaw("new-1", 4.0)}, nil
	}
	r.bmkg.warnings = func(string) ([]map[string]any, error) {
		return []map[string]any{warningRaw("old-1", "Awas")}, nil
	}

	r.poller.pollBMKG(context.Background())

	if got, want := r.store.ids(), []string{"hz-new-1", "hz-old-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("saved = %v, want %v (old-2 has no warning and must not be re-ingested)", got, want)
	}
	if r.store.saved[1].Severity != hazard.SeverityAwas {
		t.Fatalf("late warning must set severity AWAS, got %q", r.store.saved[1].Severity)
	}
	if got, want := r.bmkg.eventSince, []string{"2026-01-01T00:00:00Z", ""}; !reflect.DeepEqual(got, want) {
		t.Fatalf("event calls = %q, want %q", got, want)
	}
}

func TestPollBMKGDoesNotRefetchWhenWarningEventIsInTheBatch(t *testing.T) {
	r := newRig()
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 4.0))
	r.bmkg.warnings = func(string) ([]map[string]any, error) {
		return []map[string]any{warningRaw("eq-1", "Siaga"), {"warning_id": "no-related-id"}}, nil
	}

	r.poller.pollBMKG(context.Background())

	if r.bmkg.eventCalls() != 1 {
		t.Fatalf("event endpoint called %d times, want 1", r.bmkg.eventCalls())
	}
}

func TestPollBMKGAbortsWhenRefetchFails(t *testing.T) {
	r := newRig()
	r.poller.bmkgSince = "2026-01-01T00:00:00Z"
	r.bmkg.events = func(since string) ([]map[string]any, error) {
		if since == "" {
			return nil, errors.New("refetch failed")
		}
		return []map[string]any{seismicRaw("new-1", 4.0)}, nil
	}
	r.bmkg.warnings = func(string) ([]map[string]any, error) {
		return []map[string]any{warningRaw("old-1", "Awas")}, nil
	}

	r.poller.pollBMKG(context.Background())

	if len(r.store.saved) != 0 || r.poller.bmkgSince != "2026-01-01T00:00:00Z" {
		t.Fatalf("aborted cycle must keep the old cursor and save nothing: saved=%v cursor=%q", r.store.ids(), r.poller.bmkgSince)
	}
}

func TestPollBMKGSkipsMalformedEventButKeepsTheRest(t *testing.T) {
	r := newRig()
	broken := seismicRaw("eq-bad", 5.5)
	delete(broken, "event_id")
	r.bmkg.events = staticEvents(broken, seismicRaw("eq-ok", 5.5))

	r.poller.pollBMKG(context.Background())

	if got, want := r.store.ids(), []string{"hz-eq-ok"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("saved = %v, want %v", got, want)
	}
	if r.poller.bmkgSince == "" {
		t.Fatal("a malformed record is skipped permanently, so the cursor still advances")
	}
}

func TestPollBMKGKeepsCursorWhenStoreFails(t *testing.T) {
	r := newRig()
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 5.5), seismicRaw("eq-2", 5.5))
	r.store.err = func(hazard.Event) error { return errors.New("mongo down") }

	r.poller.pollBMKG(context.Background())

	if r.store.attempts != 1 {
		t.Fatalf("store attempts = %d, want 1 (stop at the first failure)", r.store.attempts)
	}
	if len(r.pub.events) != 0 {
		t.Fatal("nothing may be published for an event that was not stored")
	}
	if r.poller.bmkgSince != "" {
		t.Fatal("cursor must stay put so the batch is retried")
	}
}

func TestPollBMKGKeepsCursorWhenPublishFails(t *testing.T) {
	r := newRig()
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 5.5))
	r.pub.err = errors.New("broker down")

	r.poller.pollBMKG(context.Background())

	if got := r.store.ids(); len(got) != 1 {
		t.Fatalf("the event is stored before publishing, saved = %v", got)
	}
	if r.poller.bmkgSince != "" {
		t.Fatal("cursor must stay put so the publish is retried")
	}
}

func TestPollWithoutPublisherStillStoresEvents(t *testing.T) {
	r := newRig()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := New(r.bmkg, r.pvmbg, r.store, nil, time.Second, time.Second, log)
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 5.5))

	p.pollBMKG(context.Background())

	if len(r.store.saved) != 1 || p.bmkgSince == "" {
		t.Fatalf("saved=%v cursor=%q", r.store.ids(), p.bmkgSince)
	}
}

// ---- PVMBG ----

func TestPollPVMBGKeepsNewFieldsInAttributesAndStaysHealthy(t *testing.T) {
	r := newRig()
	withConfidence := volcanicRaw("pvmbg-2")
	withConfidence["confidence_level"] = 0.87
	r.pvmbg.reports = staticReports(volcanicRaw("pvmbg-1"), withConfidence)

	r.poller.pollPVMBG(context.Background())

	if got, want := r.store.ids(), []string{"hz-pvmbg-1", "hz-pvmbg-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("saved = %v, want %v", got, want)
	}
	if _, ok := r.store.saved[0].Attributes["confidence_level"]; ok {
		t.Fatal("a record from the old schema must not get a confidence_level")
	}
	if got := r.store.saved[1].Attributes["confidence_level"]; got != 0.87 {
		t.Fatalf("confidence_level = %v, want 0.87 in attributes", got)
	}
	if r.poller.pvmbgSince == "" {
		t.Fatal("cursor must advance after a successful cycle")
	}

	status := r.poller.PVMBGStatus()
	if status.SourceStatus != SourceHealthy || status.Stale || status.StaleSince != nil || status.LastSuccessAt == nil {
		t.Fatalf("unexpected status after success: %#v", status)
	}
}

func TestPollPVMBGOutageDegradesWithoutTouchingSeismicData(t *testing.T) {
	r := newRig()
	r.pvmbg.reports = failingReports(pvmbgclient.ErrOutage)

	r.poller.pollPVMBG(context.Background())

	status := r.poller.PVMBGStatus()
	if status.SourceStatus != SourceDegraded || status.StaleSince == nil {
		t.Fatalf("outage must mark the source degraded with stale_since: %#v", status)
	}
	if status.Stale {
		t.Fatal("data is only reported stale after the stale-after threshold")
	}
	if len(r.store.saved) != 0 || r.poller.pvmbgSince != "" {
		t.Fatal("a failed cycle must not save anything or move the cursor")
	}
}

func TestPollPVMBGGenericFailureAlsoDegrades(t *testing.T) {
	r := newRig()
	r.pvmbg.reports = failingReports(errors.New("timeout"))

	r.poller.pollPVMBG(context.Background())

	if status := r.poller.PVMBGStatus(); status.SourceStatus != SourceDegraded || status.StaleSince == nil {
		t.Fatalf("status = %#v", status)
	}
}

func TestPVMBGStaleSinceMarksTheFirstFailureAndRecoveryClearsIt(t *testing.T) {
	r := newRig()
	r.pvmbg.reports = failingReports(pvmbgclient.ErrOutage)

	if status := r.poller.PVMBGStatus(); status.SourceStatus != SourceHealthy || status.LastSuccessAt != nil || status.StaleSince != nil {
		t.Fatalf("initial status = %#v", status)
	}

	r.poller.pollPVMBG(context.Background())
	firstFailure := time.Now().UTC().Add(-time.Minute)
	r.poller.statusMu.Lock()
	r.poller.staleSince = firstFailure
	r.poller.statusMu.Unlock()

	r.poller.pollPVMBG(context.Background())
	status := r.poller.PVMBGStatus()
	if status.StaleSince == nil || !status.StaleSince.Equal(firstFailure) {
		t.Fatalf("stale_since must keep marking the first failure, got %v want %v", status.StaleSince, firstFailure)
	}
	if !status.Stale {
		t.Fatal("after staleAfter has elapsed the data must be reported stale")
	}

	r.pvmbg.reports = staticReports(volcanicRaw("pvmbg-1"))
	r.poller.pollPVMBG(context.Background())

	status = r.poller.PVMBGStatus()
	if status.SourceStatus != SourceHealthy || status.Stale || status.StaleSince != nil || status.LastSuccessAt == nil {
		t.Fatalf("recovery must clear the degraded state without a restart: %#v", status)
	}
	if got := r.store.ids(); !reflect.DeepEqual(got, []string{"hz-pvmbg-1"}) {
		t.Fatalf("saved = %v", got)
	}
}

func TestPollPVMBGSkipsMalformedReportAndKeepsCursorWhenStoreFails(t *testing.T) {
	r := newRig()
	broken := volcanicRaw("pvmbg-bad")
	broken["alert_level"] = "Darurat"
	r.pvmbg.reports = staticReports(broken, volcanicRaw("pvmbg-1"))

	r.poller.pollPVMBG(context.Background())
	if got := r.store.ids(); !reflect.DeepEqual(got, []string{"hz-pvmbg-1"}) {
		t.Fatalf("saved = %v", got)
	}
	if r.poller.pvmbgSince == "" {
		t.Fatal("cursor should advance past a skipped malformed record")
	}

	r2 := newRig()
	r2.pvmbg.reports = staticReports(volcanicRaw("pvmbg-1"))
	r2.store.err = func(hazard.Event) error { return errors.New("mongo down") }
	r2.poller.pollPVMBG(context.Background())
	if r2.poller.pvmbgSince != "" {
		t.Fatal("cursor must stay put when the store fails")
	}
	if status := r2.poller.PVMBGStatus(); status.SourceStatus != SourceHealthy {
		t.Fatal("a local store failure is not a PVMBG outage")
	}
}

func TestPollPVMBGForwardsCorrelationIDAndCursor(t *testing.T) {
	r := newRig()
	r.pvmbg.reports = staticReports(volcanicRaw("pvmbg-1"))

	r.poller.pollPVMBG(context.Background())
	r.poller.pollPVMBG(context.Background())

	if r.pvmbg.since[0] != "" || r.pvmbg.since[1] == "" {
		t.Fatalf("since values = %q, want first empty then the cursor", r.pvmbg.since)
	}
	if r.pub.corrID[0] != r.pvmbg.corrID[0] || r.pvmbg.corrID[0] == r.pvmbg.corrID[1] {
		t.Fatalf("correlation ids: call=%q publish=%q next=%q", r.pvmbg.corrID[0], r.pub.corrID[0], r.pvmbg.corrID[1])
	}
}

// ---- Run ----

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRunKeepsPollingBMKGWhilePVMBGIsBlocked(t *testing.T) {
	r := newRig()
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 5.5))
	r.pvmbg.reports = func(ctx context.Context, _ string) ([]map[string]any, error) {
		<-ctx.Done() // a very slow PVMBG that never answers
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.poller.Run(ctx)
		close(done)
	}()

	waitFor(t, "BMKG to be polled repeatedly", func() bool { return r.bmkg.eventCalls() >= 5 })
	if r.pvmbg.calls() != 1 {
		t.Fatalf("PVMBG calls = %d, want exactly 1 (still blocked in its first poll)", r.pvmbg.calls())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}

func TestRunPollsBothSourcesPeriodically(t *testing.T) {
	r := newRig()
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 5.5))
	r.pvmbg.reports = staticReports(volcanicRaw("pvmbg-1"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.poller.Run(ctx)
		close(done)
	}()

	waitFor(t, "both sources to be polled several times", func() bool {
		return r.bmkg.eventCalls() >= 3 && r.pvmbg.calls() >= 3
	})
	cancel()
	<-done
}

func TestNewCorrelationIDIsRandomHex(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		id := newCorrelationID()
		if _, err := hex.DecodeString(id); err != nil || len(id) != 32 {
			t.Fatalf("id %q is not 32 hex characters", id)
		}
		if seen[id] {
			t.Fatalf("duplicate correlation id %q", id)
		}
		seen[id] = true
	}
}

// ---- latensi panggilan keluar ----

func parseLogs(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, rec)
	}
	return out
}

func findLog(records []map[string]any, msg, operation string) map[string]any {
	for _, rec := range records {
		if rec["msg"] == msg && rec["operation"] == operation {
			return rec
		}
	}
	return nil
}

func rigWithLogs() (*rig, *bytes.Buffer) {
	r := newRig()
	var buf bytes.Buffer
	r.poller = New(r.bmkg, r.pvmbg, r.store, r.pub, time.Second, time.Second, slog.New(slog.NewJSONHandler(&buf, nil)))
	return r, &buf
}

func TestMongoAndRabbitMQCallsAreLoggedWithLatencyAndCorrelationID(t *testing.T) {
	r, buf := rigWithLogs()
	r.bmkg.events = staticEvents(seismicRaw("eq-1", 5.5))

	r.poller.pollBMKG(context.Background())

	records := parseLogs(t, buf)
	for msg, operation := range map[string]string{"mongodb call": "save_hazard_event", "rabbitmq call": "publish"} {
		rec := findLog(records, msg, operation)
		if rec == nil {
			t.Fatalf("no %q log for %s in %v", msg, operation, records)
		}
		if d, ok := rec["duration_ms"].(float64); !ok || d < 0 {
			t.Fatalf("%s: duration_ms missing or invalid: %v", operation, rec["duration_ms"])
		}
		if rec["success"] != true || rec["hazard_id"] != "hz-eq-1" || rec["correlation_id"] != r.bmkg.eventCorrID[0] {
			t.Fatalf("%s: unexpected record %v", operation, rec)
		}
		if rec["target"] != strings.Fields(msg)[0] {
			t.Fatalf("%s: target = %v", operation, rec["target"])
		}
	}
}

func TestFailedMongoAndRabbitMQCallsAreLoggedAsUnsuccessful(t *testing.T) {
	t.Run("mongo fails, so nothing is published", func(t *testing.T) {
		r, buf := rigWithLogs()
		r.bmkg.events = staticEvents(seismicRaw("eq-1", 5.5))
		r.store.err = func(hazard.Event) error { return errors.New("mongo down") }

		r.poller.pollBMKG(context.Background())

		records := parseLogs(t, buf)
		rec := findLog(records, "mongodb call failed", "save_hazard_event")
		if rec == nil || rec["success"] != false || rec["level"] != "WARN" || rec["err"] == nil || rec["duration_ms"] == nil {
			t.Fatalf("unexpected failure log: %v", rec)
		}
		if findLog(records, "rabbitmq call", "publish") != nil || findLog(records, "rabbitmq call failed", "publish") != nil {
			t.Fatal("no publish may be attempted, so none may be logged, after the save failed")
		}
	})

	t.Run("broker fails after a successful save", func(t *testing.T) {
		r, buf := rigWithLogs()
		r.bmkg.events = staticEvents(seismicRaw("eq-1", 5.5))
		r.pub.err = errors.New("broker down")

		r.poller.pollBMKG(context.Background())

		records := parseLogs(t, buf)
		if rec := findLog(records, "mongodb call", "save_hazard_event"); rec == nil || rec["success"] != true {
			t.Fatalf("the save must be logged as successful: %v", rec)
		}
		rec := findLog(records, "rabbitmq call failed", "publish")
		if rec == nil || rec["success"] != false || rec["err"] == nil || rec["duration_ms"] == nil || rec["correlation_id"] == "" {
			t.Fatalf("unexpected failure log: %v", rec)
		}
	})
}
