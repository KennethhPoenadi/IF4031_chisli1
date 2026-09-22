package bmkg

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

type Server struct {
	apiKey string
	delay  time.Duration
	store  *Store
	sleep  func(time.Duration)
	logs   io.Writer
	logMu  sync.Mutex
}

func NewServer(config Config, store *Store, logs io.Writer) *Server {
	if logs == nil {
		logs = io.Discard
	}
	return &Server{
		apiKey: config.APIKey,
		delay:  config.Delay,
		store:  store,
		sleep:  time.Sleep,
		logs:   logs,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /seismic-events", s.authorize(s.seismicEvents))
	mux.HandleFunc("GET /tsunami-warnings", s.authorize(s.tsunamiWarnings))
	return s.observe(mux)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "bmkg-mock"})
}

func (s *Server) seismicEvents(w http.ResponseWriter, r *http.Request) {
	since, ok := parseSince(w, r)
	if !ok {
		return
	}
	s.sleep(s.delay)
	writeJSON(w, http.StatusOK, s.store.EventsSince(since))
}

func (s *Server) tsunamiWarnings(w http.ResponseWriter, r *http.Request) {
	since, ok := parseSince(w, r)
	if !ok {
		return
	}
	s.sleep(s.delay)
	writeJSON(w, http.StatusOK, s.store.WarningsSince(since))
}

func (s *Server) authorize(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provided := r.Header.Get("X-BMKG-Key")
		if provided == "" {
			writeError(w, http.StatusUnauthorized, "missing X-BMKG-Key")
			return
		}
		if provided != s.apiKey {
			writeError(w, http.StatusForbidden, "invalid BMKG credential")
			return
		}
		next(w, r)
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		correlationID := r.Header.Get("X-Correlation-ID")
		if correlationID == "" {
			correlationID = newCorrelationID()
		}
		w.Header().Set("X-Correlation-ID", correlationID)
		wrapped := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		record := map[string]any{
			"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
			"service":        "bmkg-mock",
			"correlation_id": correlationID,
			"method":         r.Method,
			"path":           r.URL.Path,
			"status":         wrapped.status,
			"duration_ms":    time.Since(started).Milliseconds(),
		}
		s.logMu.Lock()
		_ = json.NewEncoder(s.logs).Encode(record)
		s.logMu.Unlock()
	})
}

func parseSince(w http.ResponseWriter, r *http.Request) (*time.Time, bool) {
	value := r.URL.Query().Get("since")
	if value == "" {
		return nil, true
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		writeError(w, http.StatusBadRequest, "since must be an RFC3339 timestamp")
		return nil, false
	}
	return &parsed, true
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
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
