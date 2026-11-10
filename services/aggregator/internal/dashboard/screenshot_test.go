package dashboard

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/shardstore"
)

// Tes visual opsional: dilewati kecuali DASH_SCREENSHOT_DIR diisi dan Chrome tersedia.
// Menyalakan dashboard dengan data contoh lalu mengambil tangkapan layar (terang, gelap, dan data tidak lengkap).
//
//	DASH_SCREENSHOT_DIR=/tmp/dash go test ./internal/dashboard -run TestScreenshots -v
func TestScreenshots(t *testing.T) {
	dir := os.Getenv("DASH_SCREENSHOT_DIR")
	if dir == "" {
		t.Skip("DASH_SCREENSHOT_DIR tidak diisi")
	}
	chrome := os.Getenv("CHROME_BIN")
	if chrome == "" {
		chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
	}
	if _, err := os.Stat(chrome); err != nil {
		t.Skip("Chrome tidak ditemukan")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	agg := shardstore.NewAggregateResult()
	start := now.Add(-24 * time.Hour).Truncate(time.Hour)
	for i := 0; i < 25; i++ {
		ts := start.Add(time.Duration(i) * time.Hour).Unix()
		agg.Series[ts] = map[string]int{
			hazard.CategorySeismic:  int(6 + 4*math.Sin(float64(i)/3)),
			hazard.CategoryTsunami:  int(i % 6 / 4),
			hazard.CategoryVolcanic: int(2 + math.Cos(float64(i)/5)),
			hazard.CategoryWildfire: int(10 + 14*math.Sin(float64(i)/6)*math.Sin(float64(i)/6)),
			hazard.CategoryCoastal:  int(i % 5),
		}
		for _, n := range agg.Series[ts] {
			agg.Total += n
		}
	}
	agg.Severity["AWAS"], agg.Severity["SIAGA"], agg.Severity["WASPADA"], agg.Severity["NORMAL"] = 21, 64, 188, 140
	for name, n := range map[string]int{"Kalimantan Tengah": 142, "Riau": 97, "Sumatera Selatan": 74, "Jawa Barat": 38,
		"Sulawesi Tengah": 31, "Maluku Utara": 22, "Nusa Tenggara Timur": 17, "Bali": 9} {
		agg.Area[name] = n
	}

	cases := []struct {
		name        string
		partial     bool
		rebalancing bool
		unavailable []string
		dark        bool
	}{
		{"light-complete", false, false, nil, false},
		{"light-partial", true, false, []string{"store-0", "store-1"}, false},
		{"dark-rebalancing", false, true, []string{"store-1"}, true},
	}
	for _, c := range cases {
		src := &fakeSource{res: shardstore.Analytics{AggregateResult: agg, AsOf: now, Partial: c.partial,
			Rebalancing: c.rebalancing, Unavailable: c.unavailable}}
		h := New(src, token)
		mux := http.NewServeMux()
		h.Register(mux)

		// Token tidak boleh lewat URL, jadi disuntikkan ke sessionStorage lewat satu skrip tambahan. CSP dihitung
		// dengan fungsi produksi yang sama, sehingga tes ini juga membuktikan CSP berbasis hash bekerja di browser.
		doc := strings.Replace(string(page), "<script>", `<script>sessionStorage.setItem("dash-token","`+token+`")</script><script>`, 1)
		if c.dark {
			// Chrome headless mengabaikan flag mode gelap, jadi blok @media gelap dibuka agar token gelapnya terlihat.
			doc = strings.Replace(doc, "@media (prefers-color-scheme: dark) {", "@media all {", 1)
		}
		outer := http.NewServeMux()
		outer.HandleFunc("GET /dashboard", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Content-Security-Policy", contentSecurityPolicy([]byte(doc)))
			w.Write([]byte(doc))
		})
		outer.Handle("/", mux)
		srv := httptest.NewServer(outer)

		out := filepath.Join(dir, c.name+".png")
		args := []string{"--headless=new", "--disable-gpu", "--hide-scrollbars", "--window-size=1200,1250",
			"--virtual-time-budget=6000", "--screenshot=" + out}
		args = append(args, srv.URL+"/dashboard")
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		cmd := exec.CommandContext(ctx, chrome, args...)
		output, err := cmd.CombinedOutput()
		cancel()
		srv.Close()
		if err != nil {
			t.Fatalf("%s: chrome gagal: %v\n%s", c.name, err, output)
		}
		if st, err := os.Stat(out); err != nil || st.Size() < 5000 {
			t.Fatalf("%s: tangkapan layar tidak dibuat", c.name)
		}
		t.Logf("tangkapan layar: %s", out)
	}
}
