package health

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"dashboard-updater/internal/consumer"
	"dashboard-updater/internal/hazard"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return fmt.Sprint(l.Addr().(*net.TCPAddr).Port)
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

func TestServeReportsConnectionStateAndExposesTheDashboardState(t *testing.T) {
	var connected atomic.Bool
	state := consumer.NewState()
	state.Apply(hazard.Event{HazardID: "hz-1", HazardType: hazard.TypeSeismic, Severity: hazard.SeverityAwas, AreaName: "Selat Sunda"})

	port := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Serve(ctx, port, &connected, state, slog.New(slog.NewJSONHandler(io.Discard, nil)))
		close(done)
	}()
	base := "http://127.0.0.1:" + port

	if code, body := fetch(t, base+"/health"); code != http.StatusServiceUnavailable || body != `{"status":"disconnected"}` {
		t.Fatalf("while disconnected: %d %s", code, body)
	}
	connected.Store(true)
	if code, body := fetch(t, base+"/health"); code != http.StatusOK || body != `{"status":"ok"}` {
		t.Fatalf("while connected: %d %s", code, body)
	}

	code, body := fetch(t, base+"/state")
	var snap consumer.Snapshot
	if err := json.Unmarshal([]byte(body), &snap); err != nil || code != http.StatusOK || snap.Total != 1 || snap.BySeverity[hazard.SeverityAwas] != 1 {
		t.Fatalf("/state: %d %s (%v)", code, body, err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not stop after the context was cancelled")
	}
}

func TestServeLogsAndReturnsWhenThePortIsTaken(t *testing.T) {
	// Serve listens on every interface, so the port must be occupied on every interface too.
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	port := fmt.Sprint(l.Addr().(*net.TCPAddr).Port)

	var logs bytes.Buffer
	var connected atomic.Bool
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		Serve(ctx, port, &connected, consumer.NewState(), slog.New(slog.NewJSONHandler(&logs, nil)))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Serve must return when it cannot listen")
	}
	if !bytes.Contains(logs.Bytes(), []byte("health server failed")) {
		t.Fatalf("log = %q", logs.String())
	}
}
