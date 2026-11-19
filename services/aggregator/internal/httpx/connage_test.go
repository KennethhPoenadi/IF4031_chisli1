package httpx

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRotateConnectionsOpensNewConnectionsAfterMaxAge(t *testing.T) {
	var conns atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	RotateConnections(srv.Config, 50*time.Millisecond)
	srv.Start()
	defer srv.Close()

	client := srv.Client()
	get := func() {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	for i := 0; i < 5; i++ {
		get()
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("koneksi muda harus dipakai ulang: %d koneksi", n)
	}
	time.Sleep(80 * time.Millisecond)
	get() // dijawab dengan Connection: close
	get() // koneksi baru
	if n := conns.Load(); n < 2 {
		t.Fatalf("koneksi tua harus diganti: %d koneksi", n)
	}
}
