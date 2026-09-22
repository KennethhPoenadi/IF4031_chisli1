package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"aggregator/internal/store"
)

type Handler struct {
	hazardStore *store.HazardStore
}

func NewHandler(hazardStore *store.HazardStore) *Handler {
	return &Handler{hazardStore: hazardStore}
}

func (h *Handler) GetHazardEvents(w http.ResponseWriter, r *http.Request) {
	limit := int64(50)
	if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
		parsed, err := strconv.ParseInt(limitParam, 10, 64)
		if err == nil && parsed > 0 {
			limit = parsed
		}
	}

	events, err := h.hazardStore.GetHazardEvents(limit)
	if err != nil {
		http.Error(w, "failed to fetch hazard events", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}
