// record merekam Feed Satelit (JSONL) untuk mempelajari perilakunya:
// keterlambatan, duplikat, urutan, bentuk update/retract/heartbeat, dan pemutusan koneksi.
//
//	SATELLITE_TOKEN=... go run ./cmd/record -out feed.jsonl -duration 45m
//
// Setiap baris berisi {"kind":"event"|"session"|"disconnect", ...}. Ringkasan keterlambatan
// (waktu virtual dikurangi waktu deteksi) dicetak di akhir.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"time"

	"satellite-ingestor/internal/detection"
	"satellite-ingestor/internal/feed"
)

func main() {
	out := flag.String("out", "feed.jsonl", "file keluaran")
	base := flag.String("url", envOr("SATELLITE_BASE_URL", "https://satellite.naufarrel.dev"), "alamat Feed Satelit")
	from := flag.Int64("from", -1, "offset awal (-1 = dari awal)")
	duration := flag.Duration("duration", 30*time.Minute, "lama merekam")
	flag.Parse()
	token := os.Getenv("SATELLITE_TOKEN")
	if token == "" {
		fmt.Fprintln(os.Stderr, "SATELLITE_TOKEN wajib diisi")
		os.Exit(2)
	}
	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	c := feed.New(*base, token, 60*time.Second)
	var vnow time.Time
	var delays []float64
	counts := map[string]int{}
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			if s, err := c.Session(ctx); err == nil {
				_ = enc.Encode(map[string]any{"kind": "session", "at": time.Now().UTC(), "session": s})
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	last := *from
	for ctx.Err() == nil {
		var err error
		last, err = c.Stream(ctx, last, func(e feed.Event) error {
			counts[e.Type]++
			if v, ok := detection.VirtualNow(e.Data); ok {
				vnow = v
			}
			if e.Type == "detection" && !vnow.IsZero() {
				if d, err := detection.Parse(e.Data); err == nil {
					delays = append(delays, vnow.Sub(d.Time).Minutes())
				}
			}
			return enc.Encode(map[string]any{"kind": "event", "at": e.ReceivedAt, "id": e.ID, "type": e.Type, "data": json.RawMessage(e.Data)})
		})
		_ = enc.Encode(map[string]any{"kind": "disconnect", "at": time.Now().UTC(), "last": last, "err": fmt.Sprint(err)})
		if ctx.Err() == nil {
			time.Sleep(2 * time.Second)
		}
	}
	fmt.Printf("kejadian per jenis: %v, offset terakhir: %d\n", counts, last)
	if len(delays) > 0 {
		sort.Float64s(delays)
		q := func(p float64) float64 { return delays[min(len(delays)-1, int(p*float64(len(delays))))] }
		fmt.Printf("keterlambatan deteksi (menit, waktu virtual - waktu deteksi): p50=%.1f p95=%.1f p99=%.1f p99.9=%.1f max=%.1f\n",
			q(0.5), q(0.95), q(0.99), q(0.999), delays[len(delays)-1])
		fmt.Println("Pakai p99.9 + margin sebagai SATELLITE_LATENESS_MINUTES.")
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
