// Package bulkload memuat katalog kejadian historis (mis. katalog gempa) langsung ke penyimpanan
// terdistribusi lewat jalur yang sama dengan data biasa: dipetakan oleh mapper kanonik, lalu ditulis
// melalui router sehingga penempatan, replikasi, dan sebaran node identik dengan data hidup.
//
// Pemuatan bersifat idempoten (hazard_id stabil, penulisan upsert) sehingga batch yang diulang tidak
// menggandakan data. Event historis tidak dipublikasikan ke broker agar tidak membanjiri notifikasi
// dan dashboard dengan kejadian lama.
package bulkload

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"aggregator/internal/hazard"
	"aggregator/internal/mapper"
)

const (
	MaxBatch       = 5000
	maxBodyBytes   = 16 << 20
	workers        = 16
	maxRejectNotes = 20
	requestTimeout = 90 * time.Second
)

// Saver adalah bagian router penyimpanan yang dibutuhkan pemuat.
type Saver interface {
	Save(ctx context.Context, ev hazard.Event) error
}

type Handler struct {
	Store Saver
}

type request struct {
	Events []map[string]any `json:"events"`
}

type Rejection struct {
	Index  int    `json:"index"`
	Reason string `json:"reason"`
}

type Result struct {
	Received   int         `json:"received"`
	Stored     int         `json:"stored"`
	Rejected   int         `json:"rejected"`
	Failed     int         `json:"failed"`
	Rejections []Rejection `json:"rejections"`
}

func (h *Handler) Register(mux *http.ServeMux, auth func(http.Handler) http.Handler) {
	mux.Handle("POST /internal/ingest/events", auth(http.HandlerFunc(h.handle)))
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

func (h *Handler) handle(w http.ResponseWriter, r *http.Request) {
	var req request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "body tidak terbaca: "+err.Error())
		return
	}
	if len(req.Events) == 0 || len(req.Events) > MaxBatch {
		writeErr(w, http.StatusBadRequest, "bad_request", "events harus berisi 1 sampai 5000 objek")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()

	res := h.Load(ctx, req.Events)
	status := http.StatusOK
	if res.Failed > 0 {
		// Sebagian gagal ditulis (penyimpanan tidak terjangkau): pengirim mengulang batch yang sama.
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, res)
}

// Load memetakan lalu menyimpan tiap objek. Objek yang tidak valid ditolak beserta alasannya, tidak
// dibuang diam-diam, dan tidak menghentikan objek lain.
func (h *Handler) Load(ctx context.Context, raws []map[string]any) Result {
	res := Result{Received: len(raws), Rejections: []Rejection{}}
	type job struct {
		idx int
		ev  hazard.Event
	}
	jobs := make(chan job)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				err := h.Store.Save(ctx, j.ev)
				mu.Lock()
				if err != nil {
					res.Failed++
				} else {
					res.Stored++
				}
				mu.Unlock()
			}
		}()
	}

	for i, raw := range raws {
		ev, err := mapper.MapSeismicEvent(raw, nil)
		if err != nil {
			mu.Lock()
			res.Rejected++
			if len(res.Rejections) < maxRejectNotes {
				res.Rejections = append(res.Rejections, Rejection{Index: i, Reason: err.Error()})
			}
			mu.Unlock()
			continue
		}
		select {
		case jobs <- job{idx: i, ev: ev}:
		case <-ctx.Done():
			mu.Lock()
			res.Failed++
			mu.Unlock()
		}
	}
	close(jobs)
	wg.Wait()
	sort.Slice(res.Rejections, func(a, b int) bool { return res.Rejections[a].Index < res.Rejections[b].Index })
	return res
}
