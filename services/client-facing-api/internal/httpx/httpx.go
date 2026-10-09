package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

type ctxKey int

const (
	keyCorr ctxKey = iota
	keyInfo
)

type Info struct {
	Client string
}

func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(keyCorr).(string)
	return id
}

func SetClient(ctx context.Context, client string) {
	if info, ok := ctx.Value(keyInfo).(*Info); ok {
		info.Client = client
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func Observe(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			id := r.Header.Get("X-Correlation-ID")
			if !validID(id) {
				id = newID()
			}
			w.Header().Set("X-Correlation-ID", id)

			info := &Info{}
			ctx := context.WithValue(context.WithValue(r.Context(), keyCorr, id), keyInfo, info)
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic in handler", "panic", rec, "correlation_id", id)
					sw.status = http.StatusInternalServerError
					WriteError(w, http.StatusInternalServerError, "internal_error", "internal error")
				}
				log.Info("request", "method", r.Method, "path", r.URL.Path, "status", sw.status,
					"duration_ms", time.Since(start).Milliseconds(), "client", info.Client, "correlation_id", id)
			}()
			next.ServeHTTP(sw, r.WithContext(ctx))
		})
	}
}

func Limit(max int) func(http.Handler) http.Handler {
	sem := make(chan struct{}, max)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				next.ServeHTTP(w, r)
			default:
				w.Header().Set("Retry-After", "1")
				WriteError(w, http.StatusTooManyRequests, "too_many_requests", "server is at capacity, retry shortly")
			}
		})
	}
}

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func WriteError(w http.ResponseWriter, status int, code, message string) {
	WriteJSON(w, status, map[string]string{"error": code, "message": message})
}

// NewCorrelationID creates an ID for calls that are not part of a client request, such as background syncs.
func NewCorrelationID() string { return newID() }

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func validID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}
