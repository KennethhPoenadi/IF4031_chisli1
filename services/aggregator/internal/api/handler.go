package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"aggregator/internal/httpx"
	"aggregator/internal/poller"
	"aggregator/internal/store"
)

var (
	validHazardTypes = map[string]bool{"SEISMIC": true, "VOLCANIC": true}
	validSeverities  = map[string]bool{"NORMAL": true, "WASPADA": true, "SIAGA": true, "AWAS": true}
)

type Handler struct {
	hazardStore *store.HazardStore
	poller      *poller.Poller
}

func NewHandler(hazardStore *store.HazardStore, p *poller.Poller) *Handler {
	return &Handler{hazardStore: hazardStore, poller: p}
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
			limit = parsed
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

	events, err := h.hazardStore.GetHazardEvents(limit, filter)
	if err != nil {
		http.Error(w, "failed to fetch hazard events", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func (h *Handler) Routes(internalKey string, observe func(http.Handler) http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.Handle("GET /hazard-events", httpx.RequireInternalKey(internalKey)(http.HandlerFunc(h.GetHazardEvents)))
	mux.Handle("GET /pvmbg-status", httpx.RequireInternalKey(internalKey)(http.HandlerFunc(h.GetPVMBGStatus)))
	return observe(mux)
}
