package audit

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/httpx"
	"aggregator/internal/shardstore"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
)

type Store interface {
	ListEvents(ctx context.Context, q shardstore.ListQuery) (shardstore.Page, error)
	Checksum(ctx context.Context, source string) (shardstore.ChecksumResult, error)
	Location(ctx context.Context, hazardID string) (shardstore.LocationResult, error)
}

type Message struct {
	MessageID  string    `json:"message_id"`
	Source     string    `json:"source"`
	ReceivedAt time.Time `json:"received_at"`
	Reason     string    `json:"reason"`
	Retryable  bool      `json:"retryable"`
	Attempts   int       `json:"attempts"`
	Status     string    `json:"status"`
}

type UnprocessedLister interface {
	ListUnprocessed(ctx context.Context, source string, limit int, cursor string) (items []Message, nextCursor string, err error)
}

type NoUnprocessed struct{}

func (NoUnprocessed) ListUnprocessed(context.Context, string, int, string) ([]Message, string, error) {
	return []Message{}, "", nil
}

type Handler struct {
	store       Store
	unprocessed UnprocessedLister
	token       string
	log         *slog.Logger
}

func (h *Handler) WithLogger(l *slog.Logger) *Handler {
	h.log = l
	return h
}

func New(store Store, unprocessed UnprocessedLister, token string) *Handler {
	if unprocessed == nil {
		unprocessed = NoUnprocessed{}
	}
	return &Handler{store: store, unprocessed: unprocessed, token: token}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle("GET /internal/audit/events", h.auth(http.HandlerFunc(h.events)))
	mux.Handle("GET /internal/audit/checksum", h.auth(http.HandlerFunc(h.checksum)))
	mux.Handle("GET /internal/audit/location", h.auth(http.HandlerFunc(h.location)))
	mux.Handle("GET /internal/audit/unprocessed", h.auth(http.HandlerFunc(h.listUnprocessed)))
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func (h *Handler) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || h.token == "" || !httpx.TokenEqual(got, h.token) {
			if h.log != nil {
				host, _, _ := net.SplitHostPort(r.RemoteAddr)
				h.log.Warn("autentikasi audit api gagal", "path", r.URL.Path, "remote", host,
					"correlation_id", httpx.CorrelationID(r.Context()))
			}
			writeError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func validSource(s string) bool {
	for _, v := range hazard.ValidSources {
		if string(v) == s {
			return true
		}
	}
	return false
}

func parseLimit(q string) (int, bool) {
	if q == "" {
		return defaultLimit, true
	}
	n, err := strconv.Atoi(q)
	if err != nil || n < 1 || n > maxLimit {
		return 0, false
	}
	return n, true
}

func unavailable(list []string) []string {
	if list == nil {
		return []string{}
	}
	return list
}

func (h *Handler) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, shardstore.ErrBadCursor):
		writeError(w, http.StatusBadRequest, "bad_request", "cursor tidak valid")
	case errors.Is(err, shardstore.ErrUnavailable):
		writeError(w, http.StatusServiceUnavailable, "unavailable", "penyimpanan tidak terjangkau")
	default:
		writeError(w, http.StatusServiceUnavailable, "unavailable", "gagal membaca penyimpanan")
	}
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, ok := parseLimit(q.Get("limit"))
	if !ok {
		writeError(w, http.StatusBadRequest, "bad_request", "limit harus bilangan bulat 1 sampai 1000")
		return
	}
	filter := shardstore.Filter{}
	if v := q.Get("source"); v != "" {
		if !validSource(v) {
			writeError(w, http.StatusBadRequest, "bad_request", "source tidak dikenal")
			return
		}
		filter.Source = v
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "since harus berformat ISO 8601")
			return
		}
		filter.IngestedSince = &t
	}

	page, err := h.store.ListEvents(r.Context(), shardstore.ListQuery{
		Filter: filter, Limit: limit, Cursor: q.Get("cursor"), Order: shardstore.OldestFirst,
	})
	if err != nil {
		h.fail(w, err)
		return
	}

	items := make([]hazard.Event, len(page.Events))
	for i, ev := range page.Events {
		ev.OccurredAt, ev.IngestedAt = ev.OccurredAt.UTC(), ev.IngestedAt.UTC()
		if ev.Attributes == nil {
			ev.Attributes = map[string]any{}
		}
		items[i] = ev
	}
	var next any
	if page.NextCursor != "" {
		next = page.NextCursor
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":             items,
		"next_cursor":       next,
		"partial":           page.Partial,
		"unavailable_units": unavailable(page.Unavailable),
	})
}

func (h *Handler) checksum(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	if source != "" && !validSource(source) {
		writeError(w, http.StatusBadRequest, "bad_request", "source tidak dikenal")
		return
	}
	res, err := h.store.Checksum(r.Context(), source)
	if err != nil {
		h.fail(w, err)
		return
	}
	label := source
	if label == "" {
		label = "ALL"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"source":            label,
		"count":             res.Count,
		"checksum":          res.Checksum,
		"algorithm":         "sha256",
		"as_of":             res.AsOf.UTC().Format(time.RFC3339),
		"partial":           res.Partial,
		"unavailable_units": unavailable(res.Unavailable),
	})
}

func (h *Handler) location(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("hazard_id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "hazard_id wajib diisi")
		return
	}
	res, err := h.store.Location(r.Context(), id)
	if err != nil {
		h.fail(w, err)
		return
	}
	if !res.Found && !res.Partial {
		writeError(w, http.StatusNotFound, "not_found", "hazard_id tidak ditemukan")
		return
	}
	locations := res.Locations
	if locations == nil {
		locations = []shardstore.Placement{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"hazard_id":         id,
		"locations":         locations,
		"partial":           res.Partial,
		"unavailable_units": unavailable(res.Unavailable),
	})
}

func (h *Handler) listUnprocessed(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, ok := parseLimit(q.Get("limit"))
	if !ok {
		writeError(w, http.StatusBadRequest, "bad_request", "limit harus bilangan bulat 1 sampai 1000")
		return
	}
	source := q.Get("source")
	if source != "" && !validSource(source) {
		writeError(w, http.StatusBadRequest, "bad_request", "source tidak dikenal")
		return
	}
	items, next, err := h.unprocessed.ListUnprocessed(r.Context(), source, limit, q.Get("cursor"))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "gagal membaca pesan yang belum diproses")
		return
	}
	if items == nil {
		items = []Message{}
	}
	var nextCursor any
	if next != "" {
		nextCursor = next
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": nextCursor})
}
