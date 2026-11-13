package feed

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func sse(w http.ResponseWriter, id int, typ, data string) {
	fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", id, typ, data)
	w.(http.Flusher).Flush()
}

func TestStreamResumesFromLastOffsetAndSkipsDuplicates(t *testing.T) {
	var gotLast []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		gotLast = append(gotLast, r.Header.Get("Last-Event-ID"))
		from, _ := strconv.Atoi(r.URL.Query().Get("from"))
		w.Header().Set("Content-Type", "text/event-stream")
		// Server inklusif: mengulang offset "from".
		for i := from; i < from+3; i++ {
			sse(w, i, "detection", fmt.Sprintf(`{"n":%d}`, i))
		}
		// Koneksi diputus server.
	}))
	defer srv.Close()
	c := New(srv.URL, "tok", 2*time.Second)
	var ids []int64
	last := int64(-1)
	for i := 0; i < 3; i++ {
		var err error
		last, err = c.Stream(context.Background(), last, func(e Event) error { ids = append(ids, e.ID); return nil })
		if err == nil {
			t.Fatal("koneksi yang diputus harus mengembalikan error")
		}
	}
	want := "0 1 2 3 4 5 6"
	if got := strings.Trim(fmt.Sprint(ids), "[]"); got != want {
		t.Fatalf("offset = %s, ingin %s (tanpa duplikat, tanpa celah)", got, want)
	}
	if gotLast[1] != "2" || gotLast[2] != "4" {
		t.Fatalf("Last-Event-ID = %v", gotLast)
	}
}

func TestStreamDetectsGapAndHeartbeatTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if r.URL.Query().Get("gap") != "" {
			sse(w, 5, "detection", "{}")
			sse(w, 7, "detection", "{}")
			return
		}
		sse(w, 0, "heartbeat", "{}")
		<-r.Context().Done() // diam
	}))
	defer srv.Close()

	c := New(srv.URL, "t", time.Second)
	// Celah: setelah offset 5 datang 7.
	gc := New(srv.URL, "t", time.Second)
	gc.http.Transport = rewrite{http.DefaultTransport, "gap=1"}
	last, err := gc.Stream(context.Background(), 4, func(Event) error { return nil })
	if !errors.Is(err, ErrGap) || last != 5 {
		t.Fatalf("last=%d err=%v", last, err)
	}

	start := time.Now()
	_, err = c.Stream(context.Background(), -1, func(Event) error { return nil })
	if !errors.Is(err, ErrHeartbeatTimeout) || time.Since(start) > 5*time.Second {
		t.Fatalf("err=%v setelah %v", err, time.Since(start))
	}
}

type rewrite struct {
	rt    http.RoundTripper
	extra string
}

func (r rewrite) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.RawQuery += "&" + r.extra
	return r.rt.RoundTrip(req)
}

func TestStatusErrorsAreReported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":"too_many_connections"}}`))
	}))
	defer srv.Close()
	_, err := New(srv.URL, "t", time.Second).Stream(context.Background(), -1, func(Event) error { return nil })
	var se *StatusError
	if !errors.As(err, &se) || se.Code != http.StatusTooManyRequests {
		t.Fatalf("err=%v", err)
	}
}
