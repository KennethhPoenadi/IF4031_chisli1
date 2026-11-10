package dashboard

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/shardstore"
)

const token = "dash-token"

type fakeSource struct {
	res    shardstore.Analytics
	err    error
	gotQ   shardstore.AggregateQuery
	called int
}

func (f *fakeSource) Analytics(_ context.Context, q shardstore.AggregateQuery) (shardstore.Analytics, error) {
	f.called++
	f.gotQ = q
	return f.res, f.err
}

var fixedNow = time.Date(2026, 11, 8, 12, 30, 0, 0, time.UTC)

func newHandler(src Source) http.Handler {
	h := New(src, token)
	h.now = func() time.Time { return fixedNow }
	mux := http.NewServeMux()
	h.Register(mux)
	return mux
}

func call(h http.Handler, target, auth string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func result() shardstore.Analytics {
	agg := shardstore.NewAggregateResult()
	t10 := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC).Unix()
	t11 := time.Date(2026, 11, 8, 11, 0, 0, 0, time.UTC).Unix()
	agg.Series[t10] = map[string]int{hazard.CategorySeismic: 3, hazard.CategoryTsunami: 1}
	agg.Series[t11] = map[string]int{hazard.CategoryWildfire: 4}
	agg.Severity["AWAS"], agg.Severity["WASPADA"] = 1, 7
	agg.Area["Jawa Barat"], agg.Area["Aceh"], agg.Area["Bali"] = 4, 4, 0
	agg.Total = 8
	return shardstore.Analytics{AggregateResult: agg, AsOf: fixedNow}
}

func TestPageIsServedWithoutDataAndWithStrictSecurityHeaders(t *testing.T) {
	rec := call(newHandler(&fakeSource{}), "/dashboard", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("status=%d type=%s", rec.Code, rec.Header().Get("Content-Type"))
	}
	csp := rec.Header().Get("Content-Security-Policy")
	if !strings.Contains(csp, "default-src 'none'") || rec.Header().Get("X-Content-Type-Options") != "nosniff" ||
		rec.Header().Get("X-Frame-Options") != "DENY" || rec.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Fatal("header keamanan hilang")
	}
	if strings.Contains(csp, "unsafe-inline") {
		t.Fatalf("CSP tidak boleh memakai 'unsafe-inline': %s", csp)
	}
	if strings.Count(csp, "'sha256-") != 2 || !strings.Contains(csp, "frame-ancestors 'none'") {
		t.Fatalf("CSP harus mengizinkan skrip dan gaya inline hanya lewat hash: %s", csp)
	}
	body := rec.Body.String()
	for _, want := range []string{"Dashboard Analitik", "per tingkat severity", "per wilayah", "/dashboard/api/summary"} {
		if !strings.Contains(body, want) {
			t.Fatalf("halaman tidak memuat %q", want)
		}
	}
	// namespace SVG (http://www.w3.org/2000/svg) bukan sumber daya yang dimuat; yang dilarang adalah
	// skrip, stylesheet, gambar, atau font dari luar, karena dashboard harus jalan tanpa internet.
	for _, forbidden := range []string{`src="http`, `href="http`, `src='http`, `href='http`, "@import", "url(http", "url(//", `src="//`} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("halaman memuat sumber daya eksternal: %s", forbidden)
		}
	}
	if rec := call(newHandler(&fakeSource{}), "/dashboard/", ""); rec.Code != http.StatusOK {
		t.Fatalf("/dashboard/ status = %d", rec.Code)
	}
}

func TestSummaryRequiresTheToken(t *testing.T) {
	src := &fakeSource{res: result()}
	for _, auth := range []string{"", "Bearer salah", "Basic " + token} {
		rec := call(newHandler(src), "/dashboard/api/summary", auth)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%q: status %d, want 401", auth, rec.Code)
		}
	}
	if src.called != 0 {
		t.Fatal("permintaan tanpa token tidak boleh sampai ke penyimpanan")
	}
	disabled := New(src, "")
	mux := http.NewServeMux()
	disabled.Register(mux)
	if rec := call(mux, "/dashboard/api/summary", "Bearer "); rec.Code != http.StatusUnauthorized {
		t.Fatalf("tanpa token terkonfigurasi: status %d, want 401", rec.Code)
	}
}

type summary struct {
	AsOf  string `json:"as_of"`
	Range struct {
		From, To, Bucket string
	} `json:"range"`
	Categories []Category `json:"categories"`
	Series     []struct {
		T      string         `json:"t"`
		Counts map[string]int `json:"counts"`
	} `json:"series"`
	Totals   map[string]int `json:"totals"`
	Total    int            `json:"total"`
	Severity []struct {
		Key   string `json:"key"`
		Count int    `json:"count"`
	} `json:"severity"`
	Areas []struct {
		Area  string `json:"area"`
		Count int    `json:"count"`
	} `json:"areas"`
	Partial     bool     `json:"partial"`
	Rebalancing bool     `json:"rebalancing"`
	Unavailable []string `json:"unavailable_units"`
}

func fetch(t *testing.T, src *fakeSource, query string) summary {
	t.Helper()
	rec := call(newHandler(src), "/dashboard/api/summary"+query, "Bearer "+token)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	var s summary
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		t.Fatalf("bukan JSON: %v\n%s", err, rec.Body.String())
	}
	return s
}

func TestSummaryFillsEveryBucketAndAllFiveCategories(t *testing.T) {
	s := fetch(t, &fakeSource{res: result()}, "?range=6h")
	if s.Range.Bucket != "hour" {
		t.Fatalf("bucket = %s, want hour untuk 6 jam", s.Range.Bucket)
	}
	// 06:30 sampai 12:30 menghasilkan bucket 06:00 sampai 12:00 = 7 titik, tanpa celah
	if len(s.Series) != 7 {
		t.Fatalf("jumlah titik = %d, want 7", len(s.Series))
	}
	if len(s.Categories) != 5 {
		t.Fatalf("kategori = %d, want 5 (gempa, tsunami, gunung berapi, kebakaran hutan, pesisir)", len(s.Categories))
	}
	for _, p := range s.Series {
		for _, c := range s.Categories {
			if _, ok := p.Counts[c.Key]; !ok {
				t.Fatalf("bucket %s tidak memuat kategori %s: garis akan putus", p.T, c.Key)
			}
		}
	}
	if s.Totals[hazard.CategorySeismic] != 3 || s.Totals[hazard.CategoryTsunami] != 1 || s.Totals[hazard.CategoryWildfire] != 4 {
		t.Fatalf("totals = %v", s.Totals)
	}
	if s.Total != 8 {
		t.Fatalf("total = %d", s.Total)
	}
}

func TestSummarySeverityIsOrderedAndAreasAreSortedByCount(t *testing.T) {
	s := fetch(t, &fakeSource{res: result()}, "")
	keys := []string{}
	for _, r := range s.Severity {
		keys = append(keys, r.Key)
	}
	if strings.Join(keys, ",") != "AWAS,SIAGA,WASPADA,NORMAL" {
		t.Fatalf("urutan severity = %v", keys)
	}
	if len(s.Areas) != 3 || s.Areas[0].Area != "Aceh" || s.Areas[1].Area != "Jawa Barat" || s.Areas[2].Area != "Bali" {
		t.Fatalf("areas = %+v, want urut jumlah menurun lalu nama", s.Areas)
	}
}

func TestSummaryChoosesBucketFromTheRangeAndPassesTheWindowToTheStore(t *testing.T) {
	src := &fakeSource{res: result()}
	if s := fetch(t, src, "?range=7d"); s.Range.Bucket != "day" {
		t.Fatalf("7 hari harus memakai bucket harian, dapat %s", s.Range.Bucket)
	}
	if src.gotQ.Bucket != "day" || !src.gotQ.To.Equal(fixedNow) || !src.gotQ.From.Equal(fixedNow.Add(-7*24*time.Hour)) {
		t.Fatalf("query ke store = %+v", src.gotQ)
	}
	fetch(t, src, "?from=2026-11-08T00:00:00Z&to=2026-11-08T06:00:00Z")
	if src.gotQ.Bucket != "hour" || src.gotQ.From.Hour() != 0 || src.gotQ.To.Hour() != 6 {
		t.Fatalf("rentang kustom = %+v", src.gotQ)
	}
}

func TestSummaryRejectsBadRanges(t *testing.T) {
	src := &fakeSource{}
	for _, q := range []string{"?range=2h", "?from=kemarin&to=hari-ini", "?from=2026-11-08T06:00:00Z&to=2026-11-08T00:00:00Z",
		"?from=2020-01-01T00:00:00Z&to=2026-01-01T00:00:00Z", "?from=2026-11-08T00:00:00Z"} {
		rec := call(newHandler(src), "/dashboard/api/summary"+q, "Bearer "+token)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d, want 400", q, rec.Code)
		}
	}
	if src.called != 0 {
		t.Fatal("rentang tidak valid tidak boleh sampai ke penyimpanan")
	}
}

func TestSummaryMarksIncompleteNumbersExplicitly(t *testing.T) {
	res := result()
	res.Partial, res.Unavailable = true, []string{"store-0", "store-1"}
	s := fetch(t, &fakeSource{res: res}, "")
	if !s.Partial || len(s.Unavailable) != 2 {
		t.Fatalf("partial=%v unavailable=%v: angka tidak lengkap harus ditandai", s.Partial, s.Unavailable)
	}

	res = result()
	res.Rebalancing = true
	if s := fetch(t, &fakeSource{res: res}, ""); !s.Rebalancing || s.Partial {
		t.Fatalf("rebalancing=%v partial=%v", s.Rebalancing, s.Partial)
	}

	if s := fetch(t, &fakeSource{res: result()}, ""); s.Partial || s.Rebalancing || s.Unavailable == nil {
		t.Fatalf("keadaan normal: partial=%v rebalancing=%v unavailable=%v", s.Partial, s.Rebalancing, s.Unavailable)
	}
}

func TestSummaryAnswersWith503WhenNoStorageUnitIsReachable(t *testing.T) {
	rec := call(newHandler(&fakeSource{err: shardstore.ErrUnavailable}), "/dashboard/api/summary", "Bearer "+token)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body struct {
		Error struct{ Code string } `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("bukan JSON: %v\n%s", err, rec.Body.String())
	}
	if body.Error.Code != "unavailable" {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestPageHasNoInlineStyleAttributesOrEventHandlersOrTokenInURL(t *testing.T) {
	body := string(page)
	for _, forbidden := range []string{` style="`, " onclick=", " onload=", " onerror=", "location.hash", "location.search"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("halaman memuat %q: bertentangan dengan CSP hash atau menaruh token di URL", forbidden)
		}
	}
}

func TestCSPHashChangesWhenTheInlineScriptChanges(t *testing.T) {
	a := contentSecurityPolicy([]byte("<style>a{}</style><script>one()</script>"))
	b := contentSecurityPolicy([]byte("<style>a{}</style><script>two()</script>"))
	if a == b {
		t.Fatal("hash skrip harus berubah saat isi skrip berubah, kalau tidak CSP tidak melindungi apa pun")
	}
}

func TestFailedAuthIsLoggedWithoutTheToken(t *testing.T) {
	var buf strings.Builder
	h := New(&fakeSource{}, token).WithLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	mux := http.NewServeMux()
	h.Register(mux)
	call(mux, "/dashboard/api/summary", "Bearer tebakan-token-rahasia")
	logged := buf.String()
	if !strings.Contains(logged, "autentikasi dashboard gagal") {
		t.Fatalf("percobaan gagal tidak tercatat: %q", logged)
	}
	if strings.Contains(logged, "tebakan-token-rahasia") || strings.Contains(logged, token) {
		t.Fatalf("log tidak boleh memuat token: %s", logged)
	}
}
