package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"notifier/internal/hazard"
)

type fakeAcker struct {
	acks, nacks int
	requeue     bool
}

func (f *fakeAcker) Ack(uint64, bool) error { f.acks++; return nil }
func (f *fakeAcker) Nack(_ uint64, _ bool, requeue bool) error {
	f.nacks++
	f.requeue = requeue
	return nil
}
func (f *fakeAcker) Reject(uint64, bool) error { return nil }

func newLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, nil)), &buf
}

func delivery(t *testing.T, acker *fakeAcker, id string, severity hazard.Severity) amqp.Delivery {
	t.Helper()
	body, err := json.Marshal(hazard.Event{
		HazardID: id, HazardType: hazard.TypeSeismic, Severity: severity, AreaName: "Selat Sunda",
		OccurredAt: time.Now().UTC(), IngestedAt: time.Now().UTC().Add(-time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	return amqp.Delivery{Acknowledger: acker, Body: body, RoutingKey: "hazard.seismic.awas", CorrelationId: "corr-42", Redelivered: true}
}

func TestHandleDispatchesAlertsAtOrAboveTheThreshold(t *testing.T) {
	tests := []struct {
		severity  hazard.Severity
		wantAlert bool
	}{
		{hazard.SeverityNormal, false},
		{hazard.SeverityWaspada, true},
		{hazard.SeveritySiaga, true},
		{hazard.SeverityAwas, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.severity), func(t *testing.T) {
			log, logs := newLogger()
			acker := &fakeAcker{}
			seen := newSeenSet(10)

			handle(context.Background(), config{minSeverity: hazard.SeverityWaspada}, seen, delivery(t, acker, "hz-1", tt.severity), log)

			if acker.acks != 1 || acker.nacks != 0 {
				t.Fatalf("acks=%d nacks=%d", acker.acks, acker.nacks)
			}
			if got := strings.Contains(logs.String(), "ALERT dispatched"); got != tt.wantAlert {
				t.Fatalf("alert dispatched = %v, want %v\n%s", got, tt.wantAlert, logs.String())
			}
			if !tt.wantAlert && !strings.Contains(logs.String(), "below alert threshold") {
				t.Fatalf("a sub-threshold event must still be logged: %s", logs.String())
			}
			for _, want := range []string{`"correlation_id":"corr-42"`, `"redelivered":true`, `"hazard_id":"hz-1"`, `"age_ms"`} {
				if !strings.Contains(logs.String(), want) {
					t.Fatalf("log lacks %s: %s", want, logs.String())
				}
			}
			if !seen.has("hz-1|" + string(tt.severity)) {
				t.Fatal("the event must be remembered after processing")
			}
		})
	}
}

func TestHandleIsIdempotentAndTreatsEscalationAsNew(t *testing.T) {
	log, logs := newLogger()
	acker := &fakeAcker{}
	seen := newSeenSet(10)
	cfg := config{minSeverity: hazard.SeverityWaspada}

	handle(context.Background(), cfg, seen, delivery(t, acker, "hz-1", hazard.SeverityWaspada), log)
	handle(context.Background(), cfg, seen, delivery(t, acker, "hz-1", hazard.SeverityWaspada), log)
	if got := strings.Count(logs.String(), "ALERT dispatched"); got != 1 {
		t.Fatalf("a redelivered event must alert only once, got %d alerts", got)
	}
	if !strings.Contains(logs.String(), "duplicate skipped") || acker.acks != 2 {
		t.Fatalf("the duplicate must be acknowledged and logged: acks=%d", acker.acks)
	}

	handle(context.Background(), cfg, seen, delivery(t, acker, "hz-1", hazard.SeverityAwas), log)
	if got := strings.Count(logs.String(), "ALERT dispatched"); got != 2 {
		t.Fatalf("an escalation of the same hazard must alert again, got %d alerts", got)
	}
}

func TestHandleDropsUnparsableMessagesWithoutRequeue(t *testing.T) {
	log, logs := newLogger()
	acker := &fakeAcker{}

	handle(context.Background(), config{}, newSeenSet(10), amqp.Delivery{Acknowledger: acker, Body: []byte("garbage"), CorrelationId: "corr-bad"}, log)

	if acker.nacks != 1 || acker.requeue || acker.acks != 0 {
		t.Fatalf("nacks=%d requeue=%v acks=%d", acker.nacks, acker.requeue, acker.acks)
	}
	if !strings.Contains(logs.String(), "dropping unparsable message") || !strings.Contains(logs.String(), "corr-bad") {
		t.Fatalf("log = %q", logs.String())
	}
}

func TestHandleSimulatesSlowProcessingAndAbortsOnShutdown(t *testing.T) {
	t.Run("waits for the configured delay then acknowledges", func(t *testing.T) {
		log, _ := newLogger()
		acker := &fakeAcker{}
		started := time.Now()

		handle(context.Background(), config{minSeverity: hazard.SeverityWaspada, processDelay: 40 * time.Millisecond},
			newSeenSet(10), delivery(t, acker, "hz-1", hazard.SeverityAwas), log)

		if time.Since(started) < 40*time.Millisecond || acker.acks != 1 {
			t.Fatalf("elapsed=%s acks=%d", time.Since(started), acker.acks)
		}
	})

	t.Run("shutdown during processing leaves the message unacknowledged for redelivery", func(t *testing.T) {
		log, _ := newLogger()
		acker := &fakeAcker{}
		seen := newSeenSet(10)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		handle(ctx, config{minSeverity: hazard.SeverityWaspada, processDelay: time.Hour}, seen, delivery(t, acker, "hz-1", hazard.SeverityAwas), log)

		if acker.acks != 0 || acker.nacks != 0 || seen.has("hz-1|AWAS") {
			t.Fatalf("an interrupted message must not be acked or remembered: acks=%d nacks=%d", acker.acks, acker.nacks)
		}
	})
}

func TestSeenSetEvictsTheOldestEntry(t *testing.T) {
	seen := newSeenSet(3)
	for _, k := range []string{"a", "b", "c"} {
		seen.add(k)
	}
	seen.add("a")
	seen.add("d")

	if seen.has("a") || !seen.has("b") || !seen.has("c") || !seen.has("d") {
		t.Fatal("the oldest entry must be the one evicted")
	}

	big := newSeenSet(50)
	for i := 0; i < 2000; i++ {
		big.add(fmt.Sprintf("k-%d", i))
	}
	if len(big.set) != 50 || len(big.order) != 50 {
		t.Fatalf("set=%d order=%d, want both bounded at 50", len(big.set), len(big.order))
	}
}

func clearOptionalEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"RABBITMQ_EXCHANGE", "NOTIFIER_QUEUE", "NOTIFIER_BINDING_KEYS", "NOTIFIER_MIN_SEVERITY",
		"NOTIFIER_PROCESS_DELAY_MS", "NOTIFIER_PREFETCH", "NOTIFIER_HEALTH_PORT"} {
		t.Setenv(key, "")
	}
}

func TestLoadConfigRequiresTheBrokerURL(t *testing.T) {
	clearOptionalEnv(t)
	t.Setenv("RABBITMQ_URL", "")
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() must refuse to start without RABBITMQ_URL")
	}
}

func TestLoadConfigDefaultsAndOverrides(t *testing.T) {
	clearOptionalEnv(t)
	t.Setenv("RABBITMQ_URL", "amqp://u:p@broker:5672/")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.exchange != "hazard_events" || cfg.queue != "notifier-queue" || cfg.minSeverity != hazard.SeverityWaspada ||
		cfg.processDelay != 0 || cfg.prefetch != 10 || cfg.healthPort != "8093" || len(cfg.bindingKeys) != 1 || cfg.bindingKeys[0] != "hazard.#" {
		t.Fatalf("defaults = %#v", cfg)
	}

	t.Setenv("NOTIFIER_BINDING_KEYS", " hazard.seismic.awas , hazard.volcanic.* ")
	t.Setenv("NOTIFIER_MIN_SEVERITY", "siaga")
	t.Setenv("NOTIFIER_PROCESS_DELAY_MS", "250")
	t.Setenv("NOTIFIER_QUEUE", "q-custom")
	cfg, _ = loadConfig()
	if len(cfg.bindingKeys) != 2 || cfg.bindingKeys[0] != "hazard.seismic.awas" || cfg.bindingKeys[1] != "hazard.volcanic.*" {
		t.Fatalf("binding keys = %q, want them split and trimmed", cfg.bindingKeys)
	}
	if cfg.minSeverity != hazard.SeveritySiaga || cfg.processDelay != 250*time.Millisecond || cfg.queue != "q-custom" {
		t.Fatalf("overrides = %#v", cfg)
	}

	t.Setenv("NOTIFIER_PREFETCH", "-4")
	if cfg, _ = loadConfig(); cfg.prefetch != 10 {
		t.Fatalf("an invalid prefetch must fall back to the default, got %d", cfg.prefetch)
	}
}

func freePort(t *testing.T, network string) (net.Listener, string) {
	t.Helper()
	l, err := net.Listen("tcp", network)
	if err != nil {
		t.Fatal(err)
	}
	return l, fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
}

func fetch(t *testing.T, url string) (int, string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			return resp.StatusCode, string(body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET %s: %v", url, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestServeHealthTracksTheBrokerConnection(t *testing.T) {
	l, port := freePort(t, "127.0.0.1:0")
	l.Close()

	var connected atomic.Bool
	log, _ := newLogger()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		serveHealth(ctx, port, &connected, log)
		close(done)
	}()
	base := "http://127.0.0.1:" + port + "/health"

	if code, body := fetch(t, base); code != http.StatusServiceUnavailable || body != `{"status":"disconnected"}` {
		t.Fatalf("while disconnected: %d %s", code, body)
	}
	connected.Store(true)
	if code, body := fetch(t, base); code != http.StatusOK || body != `{"status":"ok"}` {
		t.Fatalf("while connected: %d %s", code, body)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("serveHealth did not stop after the context was cancelled")
	}
}

func TestServeHealthLogsWhenThePortIsTaken(t *testing.T) {
	l, port := freePort(t, ":0") // occupy every interface, like serveHealth does
	defer l.Close()

	var connected atomic.Bool
	log, logs := newLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		serveHealth(ctx, port, &connected, log)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("serveHealth must return when it cannot listen")
	}
	if !strings.Contains(logs.String(), "health server failed") {
		t.Fatalf("log = %q", logs.String())
	}
}

func TestRunReportsAnUnreachableBroker(t *testing.T) {
	var connected atomic.Bool
	log, _ := newLogger()

	err := run(context.Background(), config{rabbitURL: "amqp://nobody:nopass@127.0.0.1:1/"}, newSeenSet(1), &connected, log)

	if err == nil || !strings.Contains(err.Error(), "dial") {
		t.Fatalf("error = %v, want a dial error so main can retry", err)
	}
	if connected.Load() {
		t.Fatal("the consumer must not report itself connected")
	}
}

func TestGetEnvAndGetInt(t *testing.T) {
	t.Setenv("NOTIFIER_TEST_STR", "value")
	t.Setenv("NOTIFIER_TEST_EMPTY", "")
	if getEnv("NOTIFIER_TEST_STR", "fb") != "value" || getEnv("NOTIFIER_TEST_EMPTY", "fb") != "fb" {
		t.Fatal("getEnv must prefer the variable and fall back when it is empty")
	}

	t.Setenv("NOTIFIER_TEST_INT", "7")
	if getInt("NOTIFIER_TEST_INT", 1) != 7 {
		t.Fatal("getInt must parse valid numbers")
	}
	for _, bad := range []string{"x", "-1"} {
		t.Setenv("NOTIFIER_TEST_INT", bad)
		if getInt("NOTIFIER_TEST_INT", 5) != 5 {
			t.Fatalf("getInt(%q) must fall back", bad)
		}
	}
}
