package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/httpx"
	"aggregator/internal/poller"
	"aggregator/internal/store"
)

var (
	validHazardTypes = map[string]bool{"SEISMIC": true, "VOLCANIC": true}
	validSeverities  = map[string]bool{"NORMAL": true, "WASPADA": true, "SIAGA": true, "AWAS": true}
)

const maxLimit = 500

type BrokerStatus interface {
	Connected() bool
}

type EventReader interface {
	GetHazardEvents(limit int64, filter store.Filter) ([]hazard.Event, error)
	Ping(ctx context.Context) error
}

type SourceStatus interface {
	PVMBGStatus() poller.PVMBGStatus
}

type Handler struct {
	hazardStore EventReader
	poller      SourceStatus
	broker      BrokerStatus
	log         *slog.Logger
}

func NewHandler(hazardStore EventReader, p SourceStatus, b BrokerStatus, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{hazardStore: hazardStore, poller: p, broker: b, log: log}
}

// logMongoCall records the latency of one outgoing call to the Canonical Store.
func (h *Handler) logMongoCall(r *http.Request, operation string, start time.Time, err error) {
	attrs := []any{"target", "mongodb", "operation", operation, "duration_ms", time.Since(start).Milliseconds(),
		"success", err == nil, "correlation_id", httpx.CorrelationID(r.Context())}
	if err != nil {
		h.log.Warn("mongodb call failed", append(attrs, "err", err)...)
		return
	}
	h.log.Info("mongodb call", attrs...)
}

func (h *Handler) GetPVMBGStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.poller.PVMBGStatus())
}

func (h *Handler) GetHazardEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit := int64(50)
	if limitParam := q.Get("limit"); limitParam != "" {
		parsed, err := strconv.ParseInt(limitParam, 10, 64)
		if err == nil && parsed > 0 {
			limit = min(parsed, maxLimit)
		}
	}

	filter := store.Filter{}
	if v := q.Get("hazard_type"); v != "" {
		if !validHazardTypes[v] {
			http.Error(w, "hazard_type must be SEISMIC or VOLCANIC", http.StatusBadRequest)
			return
		}
		filter.HazardType = v
	}
	if v := q.Get("severity"); v != "" {
		if !validSeverities[v] {
			http.Error(w, "severity must be NORMAL, WASPADA, SIAGA or AWAS", http.StatusBadRequest)
			return
		}
		filter.Severity = v
	}
	if v := q.Get("since"); v != "" {
		parsed, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, "since must be an RFC3339 timestamp", http.StatusBadRequest)
			return
		}
		filter.Since = &parsed
	}

	queryStart := time.Now()
	events, err := h.hazardStore.GetHazardEvents(limit, filter)
	h.logMongoCall(r, "find_hazard_events", queryStart, err)
	if err != nil {
		http.Error(w, "failed to fetch hazard events", http.StatusInternalServerError)
		return
	}

	response := struct {
		Data []any `json:"data"`
		Meta any   `json:"meta"`
	}{
		Data: toAnySlice(events),
		Meta: map[string]any{
			"sources": map[string]any{
				"pvmbg": h.poller.PVMBGStatus(),
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func toAnySlice[T any](items []T) []any {
	out := make([]any, len(items))
	for i, item := range items {
		out[i] = item
	}
	return out
}

// health gagal (503) hanya kalau Canonical Store tidak terjangkau.
// broker yg putus dialporin tp ga buat aggregator dianggep mati, pembacaan data
// tetep bisa dilayanin dan publish akan reconnect sendiri.
func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	pingStart := time.Now()
	pingErr := h.hazardStore.Ping(ctx)
	h.logMongoCall(r, "ping", pingStart, pingErr)
	mongoOK := pingErr == nil
	brokerOK := h.broker.Connected()

	status, code := "ok", http.StatusOK
	switch {
	case !mongoOK:
		status, code = "unavailable", http.StatusServiceUnavailable
	case !brokerOK:
		status = "degraded"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"status":   status,
		"mongo":    mongoOK,
		"rabbitmq": brokerOK,
	})
}

func (h *Handler) Routes(internalKey string, observe func(http.Handler) http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.Handle("GET /hazard-events", httpx.RequireInternalKey(internalKey)(http.HandlerFunc(h.GetHazardEvents)))
	mux.Handle("GET /pvmbg-status", httpx.RequireInternalKey(internalKey)(http.HandlerFunc(h.GetPVMBGStatus)))
	return observe(mux)
}
