package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"client-facing-api/internal/authn"
	"client-facing-api/internal/httpx"
	"client-facing-api/internal/upstream"
)

const (
	scopeSummary = "read:summary"
	scopeRaw     = "read:raw"
)

type Handler struct {
	agg *upstream.Client
}

func NewHandler(agg *upstream.Client) *Handler { return &Handler{agg: agg} }

type response struct {
	Data  []map[string]any `json:"data"`
	Count int              `json:"count"`
	Meta  json.RawMessage  `json:"meta,omitempty"`
}

func (h *Handler) GetHazardEvents(w http.ResponseWriter, r *http.Request) {
	claims := authn.ClaimsFrom(r.Context())
	q := r.URL.Query()

	fields, err := resolveFields(q.Get("fields"), claims.HasScope(scopeRaw))
	if err != nil {
		var fe *forbiddenError
		var ue *unknownError
		switch {
		case errors.As(err, &fe):
			httpx.WriteError(w, http.StatusForbidden, "forbidden_field", fe.Error())
		case errors.As(err, &ue):
			httpx.WriteError(w, http.StatusBadRequest, "unknown_field", ue.Error())
		default:
			httpx.WriteError(w, http.StatusBadRequest, "invalid_fields", err.Error())
		}
		return
	}

	up, err := buildUpstreamQuery(q)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}

	res, err := h.agg.Events(r.Context(), up, httpx.CorrelationID(r.Context()))
	if err != nil {
		w.Header().Set("Retry-After", "2")
		msg := "hazard data is temporarily unavailable"
		if errors.Is(err, context.DeadlineExceeded) {
			msg = "hazard data source did not answer in time"
		}
		httpx.WriteError(w, http.StatusServiceUnavailable, "upstream_unavailable", msg)
		return
	}

	out := make([]map[string]any, 0, len(res.Events))
	for _, ev := range res.Events {
		out = append(out, project(ev, fields))
	}
	httpx.WriteJSON(w, http.StatusOK, response{Data: out, Count: len(out), Meta: res.Meta})
}

var (
	validTypes      = map[string]bool{"SEISMIC": true, "VOLCANIC": true}
	validSeverities = map[string]bool{"NORMAL": true, "WASPADA": true, "SIAGA": true, "AWAS": true}
)

func buildUpstreamQuery(q url.Values) (url.Values, error) {
	up := url.Values{}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			return nil, errors.New("limit must be an integer between 1 and 200")
		}
		up.Set("limit", v)
	}
	if v := q.Get("hazard_type"); v != "" {
		if !validTypes[v] {
			return nil, errors.New("hazard_type must be SEISMIC or VOLCANIC")
		}
		up.Set("hazard_type", v)
	}
	if v := q.Get("severity"); v != "" {
		if !validSeverities[v] {
			return nil, errors.New("severity must be NORMAL, WASPADA, SIAGA or AWAS")
		}
		up.Set("severity", v)
	}
	if v := q.Get("since"); v != "" {
		if _, err := time.Parse(time.RFC3339, v); err != nil {
			return nil, errors.New("since must be an RFC3339 timestamp")
		}
		up.Set("since", v)
	}
	return up, nil
}

func (h *Handler) Routes(v *authn.Verifier, limit func(http.Handler) http.Handler, observe func(http.Handler) http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /hazard-events", v.Require(scopeSummary)(limit(http.HandlerFunc(h.GetHazardEvents))))
	return observe(mux)
}
