package httpx

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

type ctxKey int

const correlationIDKey ctxKey = iota

func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDKey).(string)
	return id
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func Observe(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started := time.Now()
			id := r.Header.Get("X-Correlation-ID")
			if id == "" {
				id = newCorrelationID()
			}
			w.Header().Set("X-Correlation-ID", id)

			ctx := context.WithValue(r.Context(), correlationIDKey, id)
			wrapped := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(wrapped, r.WithContext(ctx))

			log.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", wrapped.status,
				"duration_ms", time.Since(started).Milliseconds(),
				"correlation_id", id,
			)
		})
	}
}

func RequireInternalKey(expected string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := r.Header.Get("X-Internal-Key")
			if provided == "" {
				WriteError(w, http.StatusUnauthorized, "missing X-Internal-Key")
				return
			}
			if subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
				WriteError(w, http.StatusForbidden, "invalid internal credential")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{"error": message})
}

func WriteJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func newCorrelationID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000")
	}
	return hex.EncodeToString(value[:])
}
