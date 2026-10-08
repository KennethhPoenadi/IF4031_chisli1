package upstream

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

const testKey = "aggregator-key-for-tests"

func newClient(baseURL string, timeout time.Duration) (*Client, *bytes.Buffer) {
	var logs bytes.Buffer
	return New(baseURL, testKey, timeout, 4, slog.New(slog.NewJSONHandler(&logs, nil))), &logs
}

func TestEventsForwardsCredentialsCorrelationIDAndQuery(t *testing.T) {
	var gotKey, gotCorr, gotAccept, gotQuery, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey, gotCorr, gotAccept = r.Header.Get("X-Internal-Key"), r.Header.Get("X-Correlation-ID"), r.Header.Get("Accept")
		gotQuery, gotPath = r.URL.RawQuery, r.URL.Path
		w.Write([]byte(`{"data":[{"hazard_id":"hz-1"}],"meta":{"sources":{"pvmbg":{"stale":true}}}}`))
	}))
	defer srv.Close()

	client, logs := newClient(srv.URL, time.Second)
	res, err := client.Events(context.Background(), url.Values{"limit": {"5"}, "severity": {"AWAS"}}, "corr-9")
	if err != nil {
		t.Fatalf("Events() error = %v", err)
	}

	if gotPath != "/hazard-events" || gotKey != testKey || gotCorr != "corr-9" || gotAccept != "application/json" {
		t.Fatalf("path=%q key=%q corr=%q accept=%q", gotPath, gotKey, gotCorr, gotAccept)
	}
	if gotQuery != "limit=5&severity=AWAS" {
		t.Fatalf("query = %q", gotQuery)
	}
	if len(res.Events) != 1 || res.Events[0]["hazard_id"] != "hz-1" {
		t.Fatalf("events = %#v", res.Events)
	}
	if !strings.Contains(string(res.Meta), `"stale":true`) {
		t.Fatalf("meta must be passed through untouched, got %s", res.Meta)
	}
	for _, want := range []string{`"target":"aggregator"`, `"duration_ms"`, `"correlation_id":"corr-9"`, `"status":200`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log %q lacks %s", logs.String(), want)
		}
	}
	if strings.Contains(logs.String(), testKey) {
		t.Fatal("the internal key must never be logged")
	}
}

func TestEventsAcceptsABareArrayAndNoQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			t.Errorf("unexpected query %q", r.URL.RawQuery)
		}
		w.Write([]byte(`  [{"hazard_id":"hz-1"},{"hazard_id":"hz-2"}]`))
	}))
	defer srv.Close()

	client, _ := newClient(srv.URL, time.Second)
	res, err := client.Events(context.Background(), nil, "corr")
	if err != nil || len(res.Events) != 2 || res.Meta != nil {
		t.Fatalf("res=%#v err=%v", res, err)
	}
}

func TestEventsReportsUpstreamStatusAsStatusError(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		client, _ := newClient(srv.URL, time.Second)

		_, err := client.Events(context.Background(), nil, "corr")
		srv.Close()

		var statusErr *StatusError
		if !errors.As(err, &statusErr) || statusErr.Status != status {
			t.Fatalf("status %d: error = %v", status, err)
		}
		if !strings.Contains(err.Error(), "aggregator returned HTTP") {
			t.Fatalf("error text = %q", err.Error())
		}
	}
}

func TestEventsRejectsUnusableResponses(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "empty body", body: "   ", wantErr: "empty response"},
		{name: "broken array", body: `[{"hazard_id":`, wantErr: "unreadable aggregator response"},
		{name: "broken envelope", body: `{"data": 12}`, wantErr: "unreadable aggregator response"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(tt.body)) }))
			defer srv.Close()
			client, _ := newClient(srv.URL, time.Second)

			if _, err := client.Events(context.Background(), nil, "corr"); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestEventsFailsOnTruncatedBodyTimeoutAndUnreachableHost(t *testing.T) {
	t.Run("truncated body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			w.Write([]byte("[{"))
		}))
		defer srv.Close()
		client, _ := newClient(srv.URL, time.Second)
		if _, err := client.Events(context.Background(), nil, "corr"); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("timeout is logged with its latency", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(500 * time.Millisecond):
			case <-r.Context().Done():
			}
		}))
		defer srv.Close()
		client, logs := newClient(srv.URL, 30*time.Millisecond)

		started := time.Now()
		if _, err := client.Events(context.Background(), nil, "corr"); err == nil {
			t.Fatal("expected a timeout")
		}
		if time.Since(started) > 400*time.Millisecond {
			t.Fatal("the configured timeout was not applied")
		}
		if !strings.Contains(logs.String(), "upstream call failed") || !strings.Contains(logs.String(), `"duration_ms"`) {
			t.Fatalf("log = %q", logs.String())
		}
	})

	t.Run("unreachable host", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		target := srv.URL
		srv.Close()
		client, _ := newClient(target, time.Second)
		if _, err := client.Events(context.Background(), nil, "corr"); err == nil {
			t.Fatal("expected a connection error")
		}
	})

	t.Run("malformed base URL", func(t *testing.T) {
		client, _ := newClient("http://exa mple.com", time.Second)
		if _, err := client.Events(context.Background(), nil, "corr"); err == nil {
			t.Fatal("expected a request construction error")
		}
	})
}

func TestConnectionPoolMatchesTheRequestLimit(t *testing.T) {
	client := New("http://aggregator", testKey, 1500*time.Millisecond, 37, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)))

	transport, ok := client.http.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T", client.http.Transport)
	}
	if transport.MaxConnsPerHost != 37 || transport.MaxIdleConnsPerHost != 37 || transport.MaxIdleConns != 37 {
		t.Fatalf("pool = %d/%d/%d, want all 37", transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost, transport.MaxIdleConns)
	}
	if transport.ResponseHeaderTimeout != 1500*time.Millisecond || client.http.Timeout != 1500*time.Millisecond {
		t.Fatalf("timeouts = %s/%s", transport.ResponseHeaderTimeout, client.http.Timeout)
	}
	if http.DefaultTransport.(*http.Transport).MaxConnsPerHost == 37 {
		t.Fatal("the shared default transport must not be modified")
	}
}
