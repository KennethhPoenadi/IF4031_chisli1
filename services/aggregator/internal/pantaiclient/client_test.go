package pantaiclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, baseURL, token string
	}{
		{name: "missing URL", token: "secret"},
		{name: "relative URL", baseURL: "/sensor", token: "secret"},
		{name: "unsupported scheme", baseURL: "ftp://sensor.example", token: "secret"},
		{name: "credentials in URL", baseURL: "http://user:pass@sensor.example", token: "secret"},
		{name: "query in URL", baseURL: "http://sensor.example?profile=test", token: "secret"},
		{name: "missing token", baseURL: "http://sensor.example", token: "   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(Config{BaseURL: tc.baseURL, Token: tc.token}); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestFetchAuthAndCursor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/observations" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("accept = %q", r.Header.Get("Accept"))
		}
		if r.URL.Query().Get("cursor") != "page 2" || r.URL.Query().Get("limit") != "20" {
			t.Errorf("query = %v", r.URL.Query())
		}
		w.Write([]byte(`{"generated_at":"2026-11-10T08:30:00Z","items":[{"observation_id":"obs-1"}],"next_cursor":"page 3","vendor_version":"1.2"}`))
	}))
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL, Token: " secret ", PageLimit: 20})
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.Fetch(context.Background(), "page 2")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Observations) != 1 || p.NextCursor != "page 3" || p.GeneratedAt != "2026-11-10T08:30:00Z" {
		t.Fatalf("page = %#v", p)
	}
}

func TestStationsUsesAuthenticatedBoundedCatalog(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/stations" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.Write([]byte(`{"items":[{"station_id":"s1","latitude":-7.7,"longitude":108.6},{"station_id":"s2","latitude":-6.1,"longitude":106.8}],"vendor_version":"1.2"}`))
	}))
	defer srv.Close()
	c, err := New(Config{BaseURL: srv.URL, Token: "secret", PageLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.Stations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Stations) != 2 || page.Stations[0]["station_id"] != "s1" {
		t.Fatalf("station page = %#v", page)
	}
}

func TestFetchRejectsMalformedUnboundedAndInvalidPages(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		max        int64
		wantErr    error
	}{
		{"oversize", `{"items":[]}`, 5, ErrResponseTooLarge},
		{"missing items", `{"generated_at":"2026-11-10T08:30:00Z"}`, 1000, nil},
		{"invalid generated at", `{"generated_at":"yesterday","items":[]}`, 1000, nil},
		{"too many items", `{"generated_at":"2026-11-10T08:30:00Z","items":[{},{}]}`, 1000, nil},
		{"trailing data", `{"generated_at":"2026-11-10T08:30:00Z","items":[]} {}`, 1000, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(tc.body)) }))
			defer srv.Close()
			c, err := New(Config{BaseURL: srv.URL, Token: "secret", MaxBody: tc.max, PageLimit: 1, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Fetch(context.Background(), "")
			if err == nil {
				t.Fatal("expected invalid page error")
			}
			if tc.wantErr != nil && err != tc.wantErr {
				t.Fatalf("error = %v; want %v", err, tc.wantErr)
			}
		})
	}
}

func TestFetchReturnsTypedHTTPError(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":"unavailable"}`))
			}))
			defer srv.Close()

			c, err := New(Config{BaseURL: srv.URL, Token: "secret"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Fetch(context.Background(), "")
			var httpErr *HTTPError
			if !errors.As(err, &httpErr) || httpErr.StatusCode != status {
				t.Fatalf("error = %v; want HTTP %d", err, status)
			}
		})
	}
}

func TestFetchEnforcesTimeoutWithInjectedHTTPClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	c, err := New(Config{BaseURL: srv.URL, Token: "secret", Timeout: 20 * time.Millisecond, HTTPClient: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Fetch(context.Background(), "")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v; want context deadline exceeded", err)
	}
}
