package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"dashboard-updater/internal/config"
	"dashboard-updater/internal/hazard"
)

type fakeAcker struct {
	acks, nacks, rejects int
	requeue              bool
}

func (f *fakeAcker) Ack(uint64, bool) error { f.acks++; return nil }
func (f *fakeAcker) Nack(_ uint64, _ bool, requeue bool) error {
	f.nacks++
	f.requeue = requeue
	return nil
}
func (f *fakeAcker) Reject(uint64, bool) error { f.rejects++; return nil }

func delivery(t *testing.T, acker *fakeAcker, ev hazard.Event) amqp.Delivery {
	t.Helper()
	body, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return amqp.Delivery{Acknowledger: acker, Body: body, RoutingKey: "hazard.seismic.awas", CorrelationId: "corr-77"}
}

func event(id string, severity hazard.Severity) hazard.Event {
	return hazard.Event{
		HazardID: id, Source: hazard.SourceBMKG, HazardType: hazard.TypeSeismic, Severity: severity,
		AreaName: "Selat Sunda", OccurredAt: time.Now().UTC(), IngestedAt: time.Now().UTC(),
	}
}

func newLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, nil)), &buf
}

func TestHandleAppliesTheEventAndAcknowledgesIt(t *testing.T) {
	state, seen := NewState(), NewSeenSet(10)
	acker := &fakeAcker{}
	log, logs := newLogger()

	handle(seen, state, delivery(t, acker, event("hz-1", hazard.SeverityAwas)), log)

	if acker.acks != 1 || acker.nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want a single ack after processing", acker.acks, acker.nacks)
	}
	if snap := state.Snapshot(10); snap.Total != 1 || snap.BySeverity[hazard.SeverityAwas] != 1 {
		t.Fatalf("snapshot = %#v", snap)
	}
	if !seen.Has("hz-1|AWAS") {
		t.Fatal("the processed event must be remembered for deduplication")
	}
	for _, want := range []string{`"hazard_id":"hz-1"`, `"correlation_id":"corr-77"`, `"routing_key":"hazard.seismic.awas"`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log %q lacks %s", logs.String(), want)
		}
	}
}

func TestHandleIsIdempotentForRedeliveredEvents(t *testing.T) {
	state, seen := NewState(), NewSeenSet(10)
	log, logs := newLogger()
	acker := &fakeAcker{}

	handle(seen, state, delivery(t, acker, event("hz-1", hazard.SeverityAwas)), log)
	handle(seen, state, delivery(t, acker, event("hz-1", hazard.SeverityAwas)), log)

	if acker.acks != 2 {
		t.Fatalf("a duplicate must still be acknowledged or the broker would redeliver forever, acks=%d", acker.acks)
	}
	if state.Snapshot(10).Total != 1 {
		t.Fatal("a duplicate must not be applied twice")
	}
	if !strings.Contains(logs.String(), "duplicate skipped") {
		t.Fatal("skipping a duplicate must be visible in the log")
	}
}

func TestHandleProcessesASeverityEscalationOfTheSameHazard(t *testing.T) {
	state, seen := NewState(), NewSeenSet(10)
	log, _ := newLogger()
	acker := &fakeAcker{}

	handle(seen, state, delivery(t, acker, event("hz-1", hazard.SeverityWaspada)), log)
	handle(seen, state, delivery(t, acker, event("hz-1", hazard.SeverityAwas)), log)

	snap := state.Snapshot(10)
	if snap.Total != 1 || snap.BySeverity[hazard.SeverityAwas] != 1 || snap.BySeverity[hazard.SeverityWaspada] != 0 {
		t.Fatalf("the escalated severity must replace the old one: %#v", snap)
	}
}

func TestHandleDropsUnparsableMessagesWithoutRequeue(t *testing.T) {
	state, seen := NewState(), NewSeenSet(10)
	acker := &fakeAcker{}
	log, logs := newLogger()

	handle(seen, state, amqp.Delivery{Acknowledger: acker, Body: []byte("{not json"), CorrelationId: "corr-bad"}, log)

	if acker.nacks != 1 || acker.requeue || acker.acks != 0 {
		t.Fatalf("nacks=%d requeue=%v acks=%d: a poison message must be rejected without requeue", acker.nacks, acker.requeue, acker.acks)
	}
	if state.Snapshot(10).Total != 0 {
		t.Fatal("an unparsable message must not change the state")
	}
	if !strings.Contains(logs.String(), "dropping unparsable message") || !strings.Contains(logs.String(), "corr-bad") {
		t.Fatalf("log = %q", logs.String())
	}
}

func TestSeenSetEvictsTheOldestEntryFirst(t *testing.T) {
	seen := NewSeenSet(3)
	for _, k := range []string{"a", "b", "c"} {
		seen.Add(k)
	}
	seen.Add("a") // re-adding must not refresh or duplicate the entry
	seen.Add("d") // evicts the oldest, "a"

	if seen.Has("a") {
		t.Fatal("the oldest entry must be evicted once the set is full")
	}
	for _, k := range []string{"b", "c", "d"} {
		if !seen.Has(k) {
			t.Fatalf("%q was evicted too early", k)
		}
	}
	if seen.Has("zzz") {
		t.Fatal("unknown keys must not be reported as seen")
	}
}

func TestSeenSetStaysBoundedUnderLoad(t *testing.T) {
	seen := NewSeenSet(100)
	for i := 0; i < 5000; i++ {
		seen.Add(fmt.Sprintf("k-%d", i))
	}
	if len(seen.set) != 100 || len(seen.order) != 100 {
		t.Fatalf("set=%d order=%d, want both bounded at 100", len(seen.set), len(seen.order))
	}
}

func TestRunReportsAnUnreachableBroker(t *testing.T) {
	var connected atomic.Bool
	log, _ := newLogger()

	err := Run(context.Background(), config.Config{RabbitURL: "amqp://nobody:nopass@127.0.0.1:1/"}, NewSeenSet(1), NewState(), &connected, log)

	if err == nil || !strings.Contains(err.Error(), "dial") {
		t.Fatalf("error = %v, want a dial error so main can retry", err)
	}
	if connected.Load() {
		t.Fatal("the consumer must not report itself connected")
	}
}
