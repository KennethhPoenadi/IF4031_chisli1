package httpx

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRejectedRequestsAreLoggedSampledButOtherStatusesAlways(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	rejectedSeen.Store(0)
	h429 := Observe(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTooManyRequests) }))
	for i := 0; i < 250; i++ {
		h429.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	}
	if n := strings.Count(buf.String(), `"msg":"request"`); n != 3 { // permintaan ke-1, 101, 201
		t.Fatalf("429 harus dicatat sampel: %d baris dari 250", n)
	}
	buf.Reset()
	h200 := Observe(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	for i := 0; i < 5; i++ {
		h200.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	}
	if n := strings.Count(buf.String(), `"msg":"request"`); n != 5 {
		t.Fatalf("status selain 429 harus dicatat penuh: %d dari 5", n)
	}
}
