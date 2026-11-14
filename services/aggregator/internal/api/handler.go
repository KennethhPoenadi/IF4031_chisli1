package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"aggregator/internal/audit"
	"aggregator/internal/dashboard"
	"aggregator/internal/httpx"
	"aggregator/internal/poller"
	"aggregator/internal/shardstore"
)

var (
	validHazardTypes = map[string]bool{"SEISMIC": true, "VOLCANIC": true, "WILDFIRE": true, "COASTAL": true}
	validSeverities  = map[string]bool{"NORMAL": true, "WASPADA": true, "SIAGA": true, "AWAS": true}
)

const maxLimit = 500

type BrokerStatus interface {
	Connected() bool
}

type EventReader interface {
	ListEvents(ctx context.Context, q shardstore.ListQuery) (shardstore.Page, error)
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
	audit       *audit.Handler
	dash        *dashboard.Handler
	extra       []func(mux *http.ServeMux)
}

// menambahkan rute dari paket lain (ingest, DLQ, laporan lapangan).
func (h *Handler) WithRoutes(register func(mux *http.ServeMux)) *Handler {
	h.extra = append(h.extra, register)
	return h
}

func (h *Handler) WithDashboard(d *dashboard.Handler) *Handler {
	h.dash = d
	return h
}

func (h *Handler) WithAudit(a *audit.Handler) *Handler {
	h.audit = a
	return h
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

	filter := shardstore.Filter{}
	if v := q.Get("hazard_type"); v != "" {
		if !validHazardTypes[v] {
			http.Error(w, "hazard_type must be SEISMIC, VOLCANIC, WILDFIRE or COASTAL", http.StatusBadRequest)
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
		filter.OccurredAfter = &parsed
	}

	queryStart := time.Now()
	page, err := h.hazardStore.ListEvents(r.Context(), shardstore.ListQuery{
		Filter: filter, Limit: int(limit), Cursor: q.Get("cursor"), Order: shardstore.NewestFirst,
	})
	h.logMongoCall(r, "find_hazard_events", queryStart, err)
	switch {
	case errors.Is(err, shardstore.ErrBadCursor):
		http.Error(w, "cursor is not valid", http.StatusBadRequest)
		return
	case errors.Is(err, shardstore.ErrUnavailable):
		writeJSONError(w, http.StatusServiceUnavailable, "store_unavailable", "penyimpanan tidak terjangkau")
		return
	case err != nil:
		http.Error(w, "failed to fetch hazard events", http.StatusInternalServerError)
		return
	}

	unavailable := page.Unavailable
	if unavailable == nil {
		unavailable = []string{}
	}
	meta := map[string]any{
		"sources": map[string]any{
			"pvmbg": h.poller.PVMBGStatus(),
		},
		"partial":           page.Partial,
		"unavailable_units": unavailable,
	}
	if page.NextCursor != "" {
		meta["next_cursor"] = page.NextCursor
	}
	response := struct {
		Data []any `json:"data"`
		Meta any   `json:"meta"`
	}{
		Data: toAnySlice(page.Events),
		Meta: meta,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": message}})
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
	if h.dash != nil {
		h.dash.Register(mux)
	}
	if h.audit != nil {
		h.audit.Register(mux)
	}
	for _, register := range h.extra {
		register(mux)
	}
	return observe(mux)
}
