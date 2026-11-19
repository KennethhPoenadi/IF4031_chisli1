package httpx

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Deadline tulis harus bisa diperpanjang lewat pembungkus statusWriter; tanpa Unwrap http.ResponseController mengembalikan ErrNotSupported.
func TestStatusWriterUnwrapsForResponseController(t *testing.T) {
	rec := httptest.NewRecorder()
	var w http.ResponseWriter = &statusWriter{ResponseWriter: rec}
	u, ok := w.(interface{ Unwrap() http.ResponseWriter })
	if !ok {
		t.Fatal("statusWriter harus punya Unwrap")
	}
	if u.Unwrap() != http.ResponseWriter(rec) {
		t.Fatal("Unwrap harus mengembalikan ResponseWriter asli")
	}
}
