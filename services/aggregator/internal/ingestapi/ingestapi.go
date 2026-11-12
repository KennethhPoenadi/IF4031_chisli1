package ingestapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"aggregator/internal/dlq"
	"aggregator/internal/fieldreport"
	"aggregator/internal/shardstore"
)

type Handler struct {
	DLQ        *dlq.DLQ
	Processors func() map[string]dlq.Processor
	Reports    *fieldreport.Processor
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func (h *Handler) Register(mux *http.ServeMux, auth func(http.Handler) http.Handler) {
	mux.Handle("GET /internal/unprocessed", auth(http.HandlerFunc(h.list)))
	mux.Handle("GET /internal/unprocessed/{id}", auth(http.HandlerFunc(h.get)))
	mux.Handle("POST /internal/unprocessed/reprocess", auth(http.HandlerFunc(h.reprocess)))
	mux.Handle("GET /internal/field-reports/stats", auth(http.HandlerFunc(h.reportStats)))
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeErr(w, http.StatusBadRequest, "bad_request", "limit harus 1 sampai 1000")
			return
		}
		limit = n
	}
	status := q.Get("status")
	if status != "" && status != dlq.StatusParked && status != dlq.StatusRetrying {
		writeErr(w, http.StatusBadRequest, "bad_request", "status harus retrying atau parked")
		return
	}
	items, next, partial, err := h.DLQ.List(r.Context(), q.Get("source"), status, limit, q.Get("cursor"))
	switch {
	case errors.Is(err, shardstore.ErrBadCursor):
		writeErr(w, http.StatusBadRequest, "bad_request", "cursor tidak valid")
		return
	case err != nil:
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "penyimpanan tidak terjangkau")
		return
	}
	var nextCursor any
	if next != "" {
		nextCursor = next
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": nextCursor, "partial": partial})
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	e, err := h.DLQ.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "penyimpanan tidak terjangkau")
		return
	}
	if e == nil {
		writeErr(w, http.StatusNotFound, "not_found", "pesan tidak ditemukan atau sudah pulih")
		return
	}
	writeJSON(w, http.StatusOK, e)
}

func (h *Handler) reprocess(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	res, err := h.DLQ.Reprocess(ctx, r.URL.Query().Get("source"), h.Processors())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *Handler) reportStats(w http.ResponseWriter, r *http.Request) {
	if h.Reports == nil {
		writeErr(w, http.StatusNotFound, "not_found", "jalur laporan tidak aktif")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	stats, ids, err := h.Reports.Stats(ctx)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}
	out := map[string]any{"stats": stats}
	if r.URL.Query().Get("ids") == "true" {
		out["report_ids"] = ids
	}
	writeJSON(w, http.StatusOK, out)
}
