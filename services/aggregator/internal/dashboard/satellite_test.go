package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeSat struct{}

func (fakeSat) Progress(context.Context) (map[string]any, error) {
	return map[string]any{"available": true, "lag_events": 5, "ingest_p95_seconds": 1.2}, nil
}

func TestSatelliteProgressEndpointRequiresTokenAndReportsLag(t *testing.T) {
	h := New(nil, "rahasia").WithSatellite(fakeSat{})
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/dashboard/api/satellite", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("tanpa token harus 401, dapat %d", rec.Code)
	}

	req := httptest.NewRequest("GET", "/dashboard/api/satellite", nil)
	req.Header.Set("Authorization", "Bearer rahasia")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"lag_events":5`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestSatelliteProgressUnavailableWhenNotConfigured(t *testing.T) {
	h := New(nil, "rahasia")
	mux := http.NewServeMux()
	h.Register(mux)
	req := httptest.NewRequest("GET", "/dashboard/api/satellite", nil)
	req.Header.Set("Authorization", "Bearer rahasia")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"available":false`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
