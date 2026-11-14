package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"time"
	"unicode/utf8"

	"client-facing-api/internal/authn"
	"client-facing-api/internal/httpx"
	"telemetry"
)

const (
	scopeFieldReports = "write:field-reports"
	scopeOps          = "ops:unprocessed"
	maxReportBytes    = 16 << 10
)

type ReportPublisher interface {
	PublishFieldReport(ctx context.Context, reportID string, body []byte, correlationID string) error
}

type OpsUpstream interface {
	Forward(ctx context.Context, method, pathAndQuery, correlationID string) (int, []byte, error)
}

var idempotencyKey = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

var (
	reportsAccepted = telemetry.Counter("field_reports_accepted", "laporan Tim Lapangan yang dibalas 202")
	reportsRejected = telemetry.Counter("field_reports_rejected", "laporan Tim Lapangan yang ditolak, per alasan")
)

func (h *Handler) WithFieldReports(p ReportPublisher, timeout time.Duration) *Handler {
	h.reports, h.publishTimeout = p, timeout
	return h
}

func (h *Handler) WithOps(o OpsUpstream) *Handler {
	h.ops = o
	return h
}

type fieldReportMessage struct {
	ReportID      string          `json:"report_id"`
	ClientID      string          `json:"client_id"`
	ReceivedAt    time.Time       `json:"received_at"`
	CorrelationID string          `json:"correlation_id"`
	Report        json.RawMessage `json:"report"`
}

func newReportID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// PostFieldReport menerima laporan Tim Lapangan. Balasan 202 berarti laporan sudah
// tersimpan di broker. Pemrosesan hilir berlangsung terpisah sehingga lambat atau
// matinya pemrosesan hilir tidak memperlambat balasan ini.
func (h *Handler) PostFieldReport(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if h.reports == nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "field_reports_disabled", "jalur laporan belum dikonfigurasi")
		return
	}
	claims := authn.ClaimsFrom(ctx)
	key := r.Header.Get("Idempotency-Key")
	if key != "" && !idempotencyKey.MatchString(key) {
		reportsRejected(ctx, 1, "reason", "bad_idempotency_key")
		httpx.WriteError(w, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key harus 1-128 karakter [A-Za-z0-9._:-]")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxReportBytes))
	if err != nil {
		reportsRejected(ctx, 1, "reason", "too_large")
		httpx.WriteError(w, http.StatusRequestEntityTooLarge, "report_too_large", "laporan maksimal 16 KiB")
		return
	}
	var probe map[string]any
	if !utf8.Valid(body) || json.Unmarshal(body, &probe) != nil || len(probe) == 0 {
		reportsRejected(ctx, 1, "reason", "invalid_json")
		httpx.WriteError(w, http.StatusBadRequest, "invalid_report", "laporan harus objek JSON tidak kosong dengan UTF-8 valid")
		return
	}

	client := ""
	if claims != nil {
		client = claims.Subject
	}
	reportID := "fr-" + newReportID()
	if key != "" {
		// Kunci dari klien dinamai per klien agar dua klien tidak saling bertabrakan.
		reportID = client + ":" + key
	}
	now := time.Now().UTC()
	cid := httpx.CorrelationID(ctx)
	msg, _ := json.Marshal(fieldReportMessage{ReportID: reportID, ClientID: client, ReceivedAt: now, CorrelationID: cid, Report: body})

	pctx, cancel := context.WithTimeout(ctx, h.publishTimeout)
	defer cancel()
	if err := h.reports.PublishFieldReport(pctx, reportID, msg, cid); err != nil {
		reportsRejected(ctx, 1, "reason", "broker_unavailable")
		w.Header().Set("Retry-After", "1")
		msg := "laporan belum dapat diterima, kirim ulang dengan Idempotency-Key yang sama"
		if errors.Is(err, context.DeadlineExceeded) {
			msg = "broker tidak mengonfirmasi tepat waktu, kirim ulang dengan Idempotency-Key yang sama"
		}
		httpx.WriteError(w, http.StatusServiceUnavailable, "not_accepted", msg)
		return
	}
	reportsAccepted(ctx, 1)
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"report_id": reportID, "status": "accepted", "received_at": now.Format(time.RFC3339Nano),
	})
}

// meneruskan permintaan Internal Ops ke Aggregator
func (h *Handler) forwardOps(internalPath string, withID bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.ops == nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "ops_disabled", "endpoint ops belum dikonfigurasi")
			return
		}
		path := internalPath
		if withID {
			id := r.PathValue("id")
			if !idempotencyKey.MatchString(id) {
				httpx.WriteError(w, http.StatusBadRequest, "invalid_id", "message_id tidak valid")
				return
			}
			path += "/" + id
		}
		if q := r.URL.RawQuery; q != "" {
			path += "?" + r.URL.Query().Encode()
		}
		status, body, err := h.ops.Forward(r.Context(), r.Method, path, httpx.CorrelationID(r.Context()))
		if err != nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "upstream_unavailable", "aggregator tidak terjangkau")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
}

func (h *Handler) registerM2(mux *http.ServeMux, v *authn.Verifier, limit func(http.Handler) http.Handler) {
	mux.Handle("POST /field-reports", v.Require(scopeFieldReports)(limit(http.HandlerFunc(h.PostFieldReport))))
	ops := func(hf http.HandlerFunc) http.Handler { return v.Require(scopeOps)(limit(hf)) }
	mux.Handle("GET /ops/unprocessed", ops(h.forwardOps("/internal/unprocessed", false)))
	mux.Handle("GET /ops/unprocessed/{id}", ops(h.forwardOps("/internal/unprocessed", true)))
	mux.Handle("POST /ops/unprocessed/reprocess", ops(h.forwardOps("/internal/unprocessed/reprocess", false)))
	mux.Handle("GET /ops/field-reports/stats", ops(h.forwardOps("/internal/field-reports/stats", false)))
}
