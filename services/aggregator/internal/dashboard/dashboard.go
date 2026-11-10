package dashboard

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/httpx"
	"aggregator/internal/shardstore"
)

//go:embed dashboard.html
var page []byte

var (
	scriptRe = regexp.MustCompile(`(?s)<script>(.*?)</script>`)
	styleRe  = regexp.MustCompile(`(?s)<style>(.*?)</style>`)
)

func hashesOf(re *regexp.Regexp, doc []byte) string {
	var out []string
	for _, m := range re.FindAllSubmatch(doc, -1) {
		sum := sha256.Sum256(m[1])
		out = append(out, "'sha256-"+base64.StdEncoding.EncodeToString(sum[:])+"'")
	}
	if len(out) == 0 {
		return "'none'"
	}
	return strings.Join(out, " ")
}

// contentSecurityPolicy membangun CSP untuk sebuah dokumen: skrip dan gaya inline hanya boleh yang hash-nya
// persis sama dengan isi dokumen. Tanpa 'unsafe-inline', sisipan skrip (XSS) tidak akan dijalankan browser.
func contentSecurityPolicy(doc []byte) string {
	return "default-src 'none'; script-src " + hashesOf(scriptRe, doc) + "; style-src " + hashesOf(styleRe, doc) +
		"; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
}

var pageCSP = contentSecurityPolicy(page)

type Source interface {
	Analytics(ctx context.Context, q shardstore.AggregateQuery) (shardstore.Analytics, error)
}

type Category struct {
	Key   string `json:"key"`
	Label string `json:"label"`
}

var Categories = []Category{
	{hazard.CategorySeismic, "Gempa"},
	{hazard.CategoryTsunami, "Tsunami"},
	{hazard.CategoryVolcanic, "Gunung berapi"},
	{hazard.CategoryWildfire, "Kebakaran hutan"},
	{hazard.CategoryCoastal, "Pesisir"},
}

var severityOrder = []string{"AWAS", "SIAGA", "WASPADA", "NORMAL"}

const (
	maxSpan       = 366 * 24 * time.Hour
	hourlyMaxSpan = 72 * time.Hour
)

// Handler melayani halaman dan API ringkasan dashboard.
type Handler struct {
	src   Source
	token string
	now   func() time.Time
	log   *slog.Logger
}

// New membuat Handler. token kosong berarti API ringkasan menolak semua permintaan. Token dashboard sengaja
// terpisah dari token Audit API: orang yang hanya melihat dashboard tidak boleh memegang token penguji.
func New(src Source, token string) *Handler {
	return &Handler{src: src, token: token, now: time.Now}
}

// WithLogger mencatat percobaan autentikasi yang gagal (tanpa pernah mencatat token).
func (h *Handler) WithLogger(l *slog.Logger) *Handler {
	h.log = l
	return h
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /dashboard", h.page)
	mux.HandleFunc("GET /dashboard/{$}", h.page)
	mux.HandleFunc("GET /dashboard/api/summary", h.summary)
}

func (h *Handler) page(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", pageCSP)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Write(page)
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

type rangeSpec struct {
	from, to time.Time
	bucket   string
}

var presets = map[string]time.Duration{
	"1h": time.Hour, "6h": 6 * time.Hour, "24h": 24 * time.Hour,
	"7d": 7 * 24 * time.Hour, "30d": 30 * 24 * time.Hour, "90d": 90 * 24 * time.Hour,
}

func (h *Handler) parseRange(q map[string][]string) (rangeSpec, string) {
	get := func(k string) string {
		if v := q[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	now := h.now().UTC()
	var rs rangeSpec

	if fromS, toS := get("from"), get("to"); fromS != "" || toS != "" {
		from, err1 := time.Parse(time.RFC3339, fromS)
		to, err2 := time.Parse(time.RFC3339, toS)
		if err1 != nil || err2 != nil {
			return rs, "from dan to harus berformat ISO 8601"
		}
		rs.from, rs.to = from.UTC(), to.UTC()
	} else {
		name := get("range")
		if name == "" {
			name = "24h"
		}
		d, ok := presets[name]
		if !ok {
			return rs, "range harus salah satu dari 1h, 6h, 24h, 7d, 30d, 90d"
		}
		rs.to, rs.from = now, now.Add(-d)
	}
	if !rs.to.After(rs.from) {
		return rs, "to harus setelah from"
	}
	if rs.to.Sub(rs.from) > maxSpan {
		return rs, "rentang waktu maksimal 366 hari"
	}
	rs.bucket = "day"
	if rs.to.Sub(rs.from) <= hourlyMaxSpan {
		rs.bucket = "hour"
	}
	return rs, ""
}

func truncate(t time.Time, bucket string) time.Time {
	t = t.UTC()
	if bucket == "day" {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	}
	return t.Truncate(time.Hour)
}

type seriesPoint struct {
	T      string         `json:"t"`
	Counts map[string]int `json:"counts"`
}

type countRow struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}

type areaRow struct {
	Area  string `json:"area"`
	Count int    `json:"count"`
}

func (h *Handler) summary(w http.ResponseWriter, r *http.Request) {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || h.token == "" || !httpx.TokenEqual(got, h.token) {
		if h.log != nil {
			host, _, _ := net.SplitHostPort(r.RemoteAddr)
			h.log.Warn("autentikasi dashboard gagal", "path", r.URL.Path, "remote", host,
				"correlation_id", httpx.CorrelationID(r.Context()))
		}
		writeError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
		return
	}
	rs, msg := h.parseRange(r.URL.Query())
	if msg != "" {
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}

	res, err := h.src.Analytics(r.Context(), shardstore.AggregateQuery{From: rs.from, To: rs.to, Bucket: rs.bucket})
	if err != nil {
		if errors.Is(err, shardstore.ErrUnavailable) {
			writeError(w, http.StatusServiceUnavailable, "unavailable", "penyimpanan tidak terjangkau")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "unavailable", "gagal menghitung agregasi")
		return
	}

	// bucket yang berurutan tanpa celah, agar garis tidak melompati periode tanpa kejadian
	step := time.Hour
	if rs.bucket == "day" {
		step = 24 * time.Hour
	}
	points := []seriesPoint{}
	totals := map[string]int{}
	for t := truncate(rs.from, rs.bucket); t.Before(rs.to); t = t.Add(step) {
		counts := map[string]int{}
		for _, c := range Categories {
			n := res.Series[t.Unix()][c.Key]
			counts[c.Key] = n
			totals[c.Key] += n
		}
		points = append(points, seriesPoint{T: t.Format(time.RFC3339), Counts: counts})
	}

	severity := make([]countRow, 0, len(severityOrder))
	for _, s := range severityOrder {
		severity = append(severity, countRow{Key: s, Count: res.Severity[s]})
	}
	areas := make([]areaRow, 0, len(res.Area))
	for a, n := range res.Area {
		areas = append(areas, areaRow{Area: a, Count: n})
	}
	sort.Slice(areas, func(i, j int) bool {
		if areas[i].Count != areas[j].Count {
			return areas[i].Count > areas[j].Count
		}
		return areas[i].Area < areas[j].Area
	})

	unavailable := res.Unavailable
	if unavailable == nil {
		unavailable = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"as_of":             res.AsOf.Format(time.RFC3339),
		"range":             map[string]string{"from": rs.from.Format(time.RFC3339), "to": rs.to.Format(time.RFC3339), "bucket": rs.bucket},
		"categories":        Categories,
		"series":            points,
		"totals":            totals,
		"total":             res.Total,
		"severity":          severity,
		"areas":             areas,
		"partial":           res.Partial,
		"rebalancing":       res.Rebalancing,
		"unavailable_units": unavailable,
	})
}
