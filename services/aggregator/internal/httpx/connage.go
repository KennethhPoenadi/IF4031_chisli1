package httpx

import (
	"context"
	"net"
	"net/http"
	"time"
)

type connStartKey struct{}

// RotateConnections membuat klien keep-alive menyambung ulang secara berkala. kube-proxy menyeimbangkan beban per koneksi, bukan
// per permintaan; klien yang terus memakai koneksi lama (mis. pool HTTP client-facing-api) tidak pernah sampai ke instance yang
// baru ditambahkan, sehingga menambah replika tidak menambah kapasitas. Koneksi yang umurnya sudah melewati maxAge dijawab dengan
// "Connection: close" sesudah respons selesai; permintaan berikutnya membuka koneksi baru ke instance mana pun yang sedang hidup.
// maxAge <= 0 mematikannya.
func RotateConnections(srv *http.Server, maxAge time.Duration) {
	if maxAge <= 0 {
		return
	}
	prev := srv.ConnContext
	srv.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		if prev != nil {
			ctx = prev(ctx, c)
		}
		return context.WithValue(ctx, connStartKey{}, time.Now())
	}
	next := srv.Handler
	srv.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if t, ok := r.Context().Value(connStartKey{}).(time.Time); ok && time.Since(t) > maxAge {
			w.Header().Set("Connection", "close")
		}
		next.ServeHTTP(w, r)
	})
}
