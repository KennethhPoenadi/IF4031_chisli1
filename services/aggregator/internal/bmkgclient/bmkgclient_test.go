package bmkgclient

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testKey = "test-bmkg-key"

func newClient(baseURL string, timeout time.Duration) (*Client, *bytes.Buffer) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	return New(baseURL, testKey, timeout, log), &logs
}

func TestSeismicEventsSendsCredentialsCorrelationIDAndCursor(t *testing.T) {
	var gotPath, gotSince, gotKey, gotCorr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotSince = r.URL.Path, r.URL.Query().Get("since")
		gotKey, gotCorr = r.Header.Get("X-BMKG-Key"), r.Header.Get("X-Correlation-ID")
		w.Write([]byte(`[{"event_id":"eq-1","magnitude":5.5,"brand_new_field":"kept"}]`))
	}))
	defer srv.Close()

	client, logs := newClient(srv.URL, time.Second)
	since := "2026-10-08T01:02:03.123456789+07:00"
	events, err := client.SeismicEvents(context.Background(), since, "corr-1")
	if err != nil {
		t.Fatalf("SeismicEvents() error = %v", err)
	}

	if gotPath != "/seismic-events" || gotSince != since {
		t.Fatalf("request path=%q since=%q, want /seismic-events and the cursor unchanged", gotPath, gotSince)
	}
	if gotKey != testKey || gotCorr != "corr-1" {
		t.Fatalf("headers key=%q correlation=%q", gotKey, gotCorr)
	}
	if len(events) != 1 || events[0]["event_id"] != "eq-1" || events[0]["brand_new_field"] != "kept" {
		t.Fatalf("events = %#v; unknown fields must survive decoding", events)
	}
	for _, want := range []string{`"duration_ms"`, `"correlation_id":"corr-1"`, `"status":200`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log %q lacks %s", logs.String(), want)
		}
	}
	if strings.Contains(logs.String(), testKey) {
		t.Fatal("the API key must never be logged")
	}
}

func TestTsunamiWarningsUsesItsOwnEndpoint(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.URL.RawQuery != "" {
			t.Errorf("an empty cursor must not add a query string, got %q", r.URL.RawQuery)
		}
		w.Write([]byte(`[{"warning_id":"w-1","related_event_id":"eq-1","threat_level":"Awas"}]`))
	}))
	defer srv.Close()

	client, _ := newClient(srv.URL, time.Second)
	warnings, err := client.TsunamiWarnings(context.Background(), "", "corr-2")
	if err != nil {
		t.Fatalf("TsunamiWarnings() error = %v", err)
	}
	if gotPath != "/tsunami-warnings" || len(warnings) != 1 || warnings[0]["threat_level"] != "Awas" {
		t.Fatalf("path=%q warnings=%#v", gotPath, warnings)
	}
}

func TestClientReportsUpstreamFailures(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string
	}{
		{name: "unauthorized", handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }, wantErr: "HTTP 401"},
		{name: "forbidden", handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }, wantErr: "HTTP 403"},
		{name: "server error", handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }, wantErr: "HTTP 500"},
		{name: "not json", handler: func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("<html>")) }, wantErr: "invalid character"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()
			client, _ := newClient(srv.URL, time.Second)

			events, err := client.SeismicEvents(context.Background(), "", "corr")
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
			if events != nil {
				t.Fatalf("events = %#v, want nil on failure", events)
			}
		})
	}
}

func TestClientTimesOutOnSlowUpstream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(500 * time.Millisecond):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	client, logs := newClient(srv.URL, 30*time.Millisecond)
	started := time.Now()
	if _, err := client.SeismicEvents(context.Background(), "", "corr"); err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(started); elapsed > 400*time.Millisecond {
		t.Fatalf("call took %s, the client timeout was not applied", elapsed)
	}
	if !strings.Contains(logs.String(), "bmkg call failed") || !strings.Contains(logs.String(), `"duration_ms"`) {
		t.Fatalf("a failed call must still be logged with its latency, got %q", logs.String())
	}
}

func TestClientFailsWhenUpstreamIsUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	client, _ := newClient(url, time.Second)
	if _, err := client.TsunamiWarnings(context.Background(), "", "corr"); err == nil {
		t.Fatal("expected a connection error")
	}
}

func TestClientRejectsMalformedBaseURL(t *testing.T) {
	client, _ := newClient("http://exa mple.com", time.Second)
	if _, err := client.SeismicEvents(context.Background(), "", "corr"); err == nil {
		t.Fatal("expected a request construction error")
	}
}

func TestClientHonoursContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, _ := newClient(srv.URL, time.Second)
	if _, err := client.SeismicEvents(ctx, "", "corr"); err == nil {
		t.Fatal("expected an error for a cancelled context")
	}
}

func TestClientFailsWhenTheResponseBodyIsTruncated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.Write([]byte("[{"))
	}))
	defer srv.Close()

	client, _ := newClient(srv.URL, time.Second)
	if _, err := client.SeismicEvents(context.Background(), "", "corr"); err == nil {
		t.Fatal("expected an error for a truncated body")
	}
}
