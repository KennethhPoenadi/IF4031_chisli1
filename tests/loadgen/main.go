// loadgen: pembangkit beban ringan untuk uji ketahanan client-facing-api dengan puluhan ribu pengguna virtual (VU).
//
// Mengapa bukan k6 saja: k6 menjalankan satu runtime JavaScript per VU (sekitar 0,3-0,4 MB), sehingga 10.000 VU butuh 3-4 GB
// dan tidak muat di mesin penguji yang tersedia. Di sini satu VU = satu goroutine dengan koneksi HTTP/1.1 keep-alive sendiri
// (sekitar 30-40 KB), jadi 10.000 VU cukup sekitar 400 MB. Perilaku tiap VU sama dengan skrip k6 p2-sustained.js: loop tanpa
// jeda yang membaca /hazard-events (SEISMIC dan VOLCANIC bergantian) dengan token client_credentials miliknya sendiri yang
// diperbarui 15 detik sebelum kedaluwarsa (umur token 60 detik). Respons 429 adalah penolakan terkontrol; hanya 5xx, 401,
// kode lain, dan galat jaringan yang dihitung gagal (batas spek < 1%).
//
//	loadgen -api http://api.prod.localtest.me -auth http://auth.prod.localtest.me -ip 100.66.57.46 \
//	        -client-id ... -client-secret ... -vus 5000 -duration 150s -ramp 30s -out hasil.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type stats struct {
	ok2xx, rej429, unauth401, err5xx, other, netErr atomic.Int64
	authReq, authFail                               atomic.Int64
	active                                          atomic.Int64
	buckets                                         [64]atomic.Int64 // histogram latensi, skala logaritmik (1 ms dasar)
}

func bucketOf(d time.Duration) int {
	ms := float64(d) / float64(time.Millisecond)
	if ms < 1 {
		return 0
	}
	b := int(math.Log2(ms)*4) + 1 // 4 bucket per penggandaan
	if b > 63 {
		b = 63
	}
	return b
}

func bucketUpperMs(b int) float64 {
	if b == 0 {
		return 1
	}
	return math.Pow(2, float64(b)/4)
}

func (s *stats) percentile(p float64) float64 {
	var total int64
	for i := range s.buckets {
		total += s.buckets[i].Load()
	}
	if total == 0 {
		return 0
	}
	target := int64(math.Ceil(float64(total) * p))
	var cum int64
	for i := range s.buckets {
		cum += s.buckets[i].Load()
		if cum >= target {
			return bucketUpperMs(i)
		}
	}
	return bucketUpperMs(63)
}

type result struct {
	VUs            int      `json:"vus"`
	DurasiDetik    float64  `json:"durasi_detik"`
	Permintaan     int64    `json:"permintaan"`
	RPS            float64  `json:"rps"`
	Status2xx      int64    `json:"status_2xx"`
	Status429      int64    `json:"status_429_ditolak_terkontrol"`
	Status401      int64    `json:"status_401"`
	Status5xx      int64    `json:"status_5xx"`
	StatusLain     int64    `json:"status_lain"`
	GalatJaringan  int64    `json:"galat_jaringan"`
	GagalBukan429  float64  `json:"gagal_bukan_429_persen"`
	P50Ms          float64  `json:"p50_ms"`
	P95Ms          float64  `json:"p95_ms"`
	P99Ms          float64  `json:"p99_ms"`
	TokenDiminta   int64    `json:"token_diminta"`
	TokenGagal     int64    `json:"token_gagal"`
	MemoriProsesMB float64  `json:"memori_heap_mb"`
	Lolos          bool     `json:"lolos"`
	Catatan        []string `json:"catatan,omitempty"`
}

func main() {
	api := flag.String("api", "http://localhost:8080", "alamat client-facing-api")
	authURL := flag.String("auth", "http://localhost:8091", "alamat auth-service")
	ip := flag.String("ip", "", "alamat IP ingress; semua koneksi dibelokkan ke sini (nama host tetap dipakai sebagai header Host)")
	clientID := flag.String("client-id", os.Getenv("CLIENT_ID"), "client_id (bawaan: env CLIENT_ID)")
	clientSecret := flag.String("client-secret", os.Getenv("CLIENT_SECRET"), "client_secret (bawaan: env CLIENT_SECRET)")
	vus := flag.Int("vus", 1000, "jumlah pengguna virtual")
	duration := flag.Duration("duration", 120*time.Second, "lama beban penuh sesudah ramp-up")
	ramp := flag.Duration("ramp", 20*time.Second, "waktu menaikkan VU dari 0 ke target secara bertahap")
	timeout := flag.Duration("timeout", 30*time.Second, "batas waktu satu permintaan")
	maxFail := flag.Float64("max-fail", 1.0, "batas kegagalan bukan-429 dalam persen")
	out := flag.String("out", "", "berkas JSON hasil")
	paths := flag.String("paths", "/hazard-events?hazard_type=SEISMIC,/hazard-events?hazard_type=VOLCANIC", "daftar jalur dipisah koma")
	flag.Parse()
	if *clientID == "" {
		fmt.Fprintln(os.Stderr, "client-id wajib")
		os.Exit(2)
	}

	apiBase := strings.TrimRight(*api, "/")
	authBase := strings.TrimRight(*authURL, "/")
	pathList := strings.Split(*paths, ",")

	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if *ip != "" {
				_, port, _ := net.SplitHostPort(addr)
				addr = net.JoinHostPort(*ip, port)
			}
			return dialer.DialContext(ctx, network, addr)
		},
		MaxIdleConns:          *vus + 100,
		MaxIdleConnsPerHost:   *vus + 100,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true,
		ResponseHeaderTimeout: *timeout,
		ReadBufferSize:        4 << 10,
		WriteBufferSize:       4 << 10,
	}
	client := &http.Client{Transport: tr, Timeout: *timeout}

	var st stats
	var wg sync.WaitGroup
	start := time.Now()
	stopAt := start.Add(*ramp + *duration)
	ctx, cancel := context.WithDeadline(context.Background(), stopAt)
	defer cancel()

	fetchToken := func() (string, time.Time, bool) {
		st.authReq.Add(1)
		form := url.Values{"grant_type": {"client_credentials"}, "client_id": {*clientID}, "client_secret": {*clientSecret}}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, authBase+"/token", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := client.Do(req)
		if err != nil {
			st.authFail.Add(1)
			return "", time.Time{}, false
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			io.Copy(io.Discard, resp.Body)
			st.authFail.Add(1)
			return "", time.Time{}, false
		}
		var b struct {
			AccessToken string `json:"access_token"`
			ExpiresIn   int    `json:"expires_in"`
		}
		if json.NewDecoder(resp.Body).Decode(&b) != nil || b.AccessToken == "" {
			st.authFail.Add(1)
			return "", time.Time{}, false
		}
		ttl := time.Duration(b.ExpiresIn) * time.Second
		if ttl <= 0 {
			ttl = 60 * time.Second
		}
		return b.AccessToken, time.Now().Add(ttl), true
	}

	vu := func(id int) {
		defer wg.Done()
		// ramp-up: VU ke-id mulai pada id/vus x ramp, ditambah sebaran kecil agar permintaan token tidak serentak
		delay := time.Duration(float64(*ramp)*float64(id)/float64(*vus)) + time.Duration(rand.Int63n(int64(200*time.Millisecond)))
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
		st.active.Add(1)
		defer st.active.Add(-1)
		var token string
		var exp time.Time
		for n := id; ctx.Err() == nil; n++ {
			if token == "" || time.Now().After(exp.Add(-15*time.Second)) {
				t, e, ok := fetchToken()
				if ok {
					token, exp = t, e
				} else if token == "" {
					time.Sleep(time.Second)
					continue
				}
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+pathList[n%len(pathList)], nil)
			req.Header.Set("Authorization", "Bearer "+token)
			t0 := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				st.netErr.Add(1)
				st.buckets[bucketOf(time.Since(t0))].Add(1)
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			st.buckets[bucketOf(time.Since(t0))].Add(1)
			switch c := resp.StatusCode; {
			case c >= 200 && c < 300:
				st.ok2xx.Add(1)
			case c == 429:
				st.rej429.Add(1)
			case c == 401:
				st.unauth401.Add(1)
			case c >= 500:
				st.err5xx.Add(1)
			default:
				st.other.Add(1)
			}
		}
	}

	for i := 0; i < *vus; i++ {
		wg.Add(1)
		go vu(i)
	}

	// laporan berkala
	tick := time.NewTicker(5 * time.Second)
	var prev [6]int64
	go func() {
		for range tick.C {
			cur := [6]int64{st.ok2xx.Load(), st.rej429.Load(), st.unauth401.Load(), st.err5xx.Load(), st.other.Load(), st.netErr.Load()}
			var dtot, dbad int64
			for i := range cur {
				dtot += cur[i] - prev[i]
			}
			dbad = (cur[2] - prev[2]) + (cur[3] - prev[3]) + (cur[4] - prev[4]) + (cur[5] - prev[5])
			prev0 := prev[0]
			prev = cur
			var ms runtimeMem
			ms.read()
			fmt.Printf("t+%4.0fs vu=%d rps=%.0f 2xx=%d 429=%d gagal=%d (%.2f%%) heap=%.0fMB\n",
				time.Since(start).Seconds(), st.active.Load(), float64(dtot)/5, cur[0]-prev0, cur[1], dbad,
				100*float64(dbad)/math.Max(1, float64(dtot)), ms.heapMB)
		}
	}()

	wg.Wait()
	tick.Stop()
	elapsed := time.Since(start).Seconds()

	total := st.ok2xx.Load() + st.rej429.Load() + st.unauth401.Load() + st.err5xx.Load() + st.other.Load() + st.netErr.Load()
	bad := st.unauth401.Load() + st.err5xx.Load() + st.other.Load() + st.netErr.Load()
	var ms runtimeMem
	ms.read()
	r := result{
		VUs: *vus, DurasiDetik: math.Round(elapsed*10) / 10, Permintaan: total, RPS: math.Round(float64(total) / elapsed),
		Status2xx: st.ok2xx.Load(), Status429: st.rej429.Load(), Status401: st.unauth401.Load(), Status5xx: st.err5xx.Load(),
		StatusLain: st.other.Load(), GalatJaringan: st.netErr.Load(),
		GagalBukan429: math.Round(10000*float64(bad)/math.Max(1, float64(total))) / 100,
		P50Ms:         st.percentile(0.50), P95Ms: st.percentile(0.95), P99Ms: st.percentile(0.99),
		TokenDiminta: st.authReq.Load(), TokenGagal: st.authFail.Load(), MemoriProsesMB: ms.heapMB,
	}
	r.Lolos = r.GagalBukan429 < *maxFail && total > 0
	if r.TokenGagal > 0 {
		r.Catatan = append(r.Catatan, fmt.Sprintf("%d permintaan token gagal", r.TokenGagal))
	}
	fmt.Println()
	fmt.Printf("== loadgen: %d VU, %.0f detik\n", r.VUs, r.DurasiDetik)
	fmt.Printf("   permintaan %d (%.0f/detik): 2xx=%d  429(ditolak terkontrol)=%d  401=%d  5xx=%d  lain=%d  galat_jaringan=%d\n",
		r.Permintaan, r.RPS, r.Status2xx, r.Status429, r.Status401, r.Status5xx, r.StatusLain, r.GalatJaringan)
	fmt.Printf("   gagal bukan-429: %.2f%% (batas %.1f%%)   latensi p50=%.0fms p95=%.0fms p99=%.0fms\n",
		r.GagalBukan429, *maxFail, r.P50Ms, r.P95Ms, r.P99Ms)
	fmt.Printf("   token: %d diminta, %d gagal   memori heap pembangkit: %.0f MB\n", r.TokenDiminta, r.TokenGagal, r.MemoriProsesMB)
	if r.Lolos {
		fmt.Println("HASIL: LOLOS")
	} else {
		fmt.Println("HASIL: GAGAL")
	}
	if *out != "" {
		b, _ := json.MarshalIndent(r, "", "  ")
		os.WriteFile(*out, b, 0o644)
	}
	if !r.Lolos {
		os.Exit(1)
	}
}
