package pvmbg

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Server struct {
	token    string
	delayMin time.Duration
	delayMax time.Duration
	store    *Store
	sleep    func(time.Duration)
	logs     io.Writer
	logMu    sync.Mutex
}

func NewServer(config Config, store *Store, logs io.Writer) *Server {
	if logs == nil {
		logs = io.Discard
	}
	return &Server{
		token:    config.Token,
		delayMin: config.DelayMin,
		delayMax: config.DelayMax,
		store:    store,
		sleep:    time.Sleep,
		logs:     logs,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /volcanic-reports", s.authorize(s.volcanicReports))
	mux.HandleFunc("POST /admin/schema-version", s.authorize(s.schemaVersion))
	mux.HandleFunc("POST /admin/outage", s.authorize(s.outage))
	return s.observe(mux)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	if s.store.Outage() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "outage", "service": "pvmbg-mock"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "pvmbg-mock"})
}

func (s *Server) volcanicReports(w http.ResponseWriter, r *http.Request) {
	if s.store.Outage() {
		writeError(w, http.StatusServiceUnavailable, "PVMBG is in outage mode")
		return
	}
	since, ok := parseSince(w, r)
	if !ok {
		return
	}
	s.sleep(s.store.RandomDelay(s.delayMin, s.delayMax))
	writeJSON(w, http.StatusOK, s.store.ReportsSince(since))
}

func (s *Server) schemaVersion(w http.ResponseWriter, r *http.Request) {
	enabled, ok := decodeEnabled(w, r)
	if !ok {
		return
	}
	s.store.SetSchemaEnabled(enabled)
	writeJSON(w, http.StatusOK, map[string]bool{"enabled": s.store.SchemaEnabled()})
}

func (s *Server) outage(w http.ResponseWriter, r *http.Request) {
	enabled, ok := decodeEnabled(w, r)
	if !ok {
		return
	}
	s.store.SetOutage(enabled)
	writeJSON(w, http.StatusOK, map[string]bool{"enabled": s.store.Outage()})
}

func (s *Server) authorize(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		value := r.Header.Get("Authorization")
		if value == "" {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		if value != "Bearer "+s.token {
			writeError(w, http.StatusForbidden, "invalid PVMBG credential")
			return
		}
		next(w, r)
	}
}

func decodeEnabled(w http.ResponseWriter, r *http.Request) (bool, bool) {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1024))
	decoder.DisallowUnknownFields()
	var request struct {
		Enabled *bool `json:"enabled"`
	}
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "body must be JSON with an enabled boolean")
		return false, false
	}
	if request.Enabled == nil {
		writeError(w, http.StatusBadRequest, "enabled is required")
		return false, false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "body must contain one JSON object")
		return false, false
	}
	return *request.Enabled, true
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
			"service":        "pvmbg-mock",
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
		return strings.ReplaceAll(time.Now().UTC().Format(time.RFC3339Nano), ":", "")
	}
	return hex.EncodeToString(value[:])
}
