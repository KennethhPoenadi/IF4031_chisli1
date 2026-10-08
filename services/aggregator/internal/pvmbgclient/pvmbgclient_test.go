package pvmbgclient

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testToken = "test-pvmbg-token"

func newClient(baseURL string, timeout time.Duration) (*Client, *bytes.Buffer) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	return New(baseURL, testToken, timeout, log), &logs
}

func TestVolcanicReportsSendsBearerTokenCorrelationIDAndCursor(t *testing.T) {
	var gotPath, gotSince, gotAuth, gotCorr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotSince = r.URL.Path, r.URL.Query().Get("since")
		gotAuth, gotCorr = r.Header.Get("Authorization"), r.Header.Get("X-Correlation-ID")
		w.Write([]byte(`[{"report_id":"r-1","alert_level":"Siaga","confidence_level":0.9}]`))
	}))
	defer srv.Close()

	client, logs := newClient(srv.URL, time.Second)
	since := "2026-10-08T01:02:03.5+00:00"
	reports, err := client.VolcanicReports(context.Background(), since, "corr-1")
	if err != nil {
		t.Fatalf("VolcanicReports() error = %v", err)
	}

	if gotPath != "/volcanic-reports" || gotSince != since {
		t.Fatalf("path=%q since=%q", gotPath, gotSince)
	}
	if gotAuth != "Bearer "+testToken || gotCorr != "corr-1" {
		t.Fatalf("authorization=%q correlation=%q", gotAuth, gotCorr)
	}
	if len(reports) != 1 || reports[0]["confidence_level"] != 0.9 {
		t.Fatalf("reports = %#v; a field added later must survive decoding", reports)
	}
	for _, want := range []string{`"duration_ms"`, `"correlation_id":"corr-1"`, `"status":200`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log %q lacks %s", logs.String(), want)
		}
	}
	if strings.Contains(logs.String(), testToken) {
		t.Fatal("the token must never be logged")
	}
}

func TestVolcanicReportsWithoutCursorAddsNoQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			t.Errorf("unexpected query %q", r.URL.RawQuery)
		}
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	client, _ := newClient(srv.URL, time.Second)
	reports, err := client.VolcanicReports(context.Background(), "", "corr")
	if err != nil || len(reports) != 0 {
		t.Fatalf("reports=%v err=%v", reports, err)
	}
}

func TestServiceUnavailableIsReportedAsOutage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client, _ := newClient(srv.URL, time.Second)
	if _, err := client.VolcanicReports(context.Background(), "", "corr"); !errors.Is(err, ErrOutage) {
		t.Fatalf("error = %v, want ErrOutage", err)
	}
}

func TestClientReportsOtherFailuresAsPlainErrors(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string
	}{
		{name: "unauthorized", handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }, wantErr: "HTTP 401"},
		{name: "forbidden", handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }, wantErr: "HTTP 403"},
		{name: "not json", handler: func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("oops")) }, wantErr: "invalid character"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()
			client, _ := newClient(srv.URL, time.Second)

			_, err := client.VolcanicReports(context.Background(), "", "corr")
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
			if errors.Is(err, ErrOutage) {
				t.Fatal("only a 503 may be treated as an outage")
			}
		})
	}
}

func TestClientTimesOutAndLogsTheLatency(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(500 * time.Millisecond):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	client, logs := newClient(srv.URL, 30*time.Millisecond)
	if _, err := client.VolcanicReports(context.Background(), "", "corr"); err == nil {
		t.Fatal("expected a timeout error")
	}
	if !strings.Contains(logs.String(), "pvmbg call failed") || !strings.Contains(logs.String(), `"duration_ms"`) {
		t.Fatalf("log = %q", logs.String())
	}
}

func TestClientFailsWhenUnreachableOrMisconfigured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	client, _ := newClient(url, time.Second)
	if _, err := client.VolcanicReports(context.Background(), "", "corr"); err == nil {
		t.Fatal("expected a connection error")
	}

	bad, _ := newClient("http://exa mple.com", time.Second)
	if _, err := bad.VolcanicReports(context.Background(), "", "corr"); err == nil {
		t.Fatal("expected a request construction error")
	}
}

func TestClientFailsWhenTheResponseBodyIsTruncated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.Write([]byte("[{"))
	}))
	defer srv.Close()

	client, _ := newClient(srv.URL, time.Second)
	if _, err := client.VolcanicReports(context.Background(), "", "corr"); err == nil {
		t.Fatal("expected an error for a truncated body")
	}
}
