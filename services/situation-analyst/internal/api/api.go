package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"sync/atomic"

	"situation-analyst/internal/engine"
)

type Source interface{ Latest() *engine.Result }

func tokenEqual(a, b string) bool {
	ha, hb := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(ha[:], hb[:]) == 1
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func Routes(src Source, internalKey string, connected *atomic.Bool) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		status := "ok"
		if connected != nil && !connected.Load() {
			status = "degraded"
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": status})
	})

	mux.HandleFunc("GET /analysis/latest", func(w http.ResponseWriter, r *http.Request) {
		provided := r.Header.Get("X-Internal-Key")
		if provided == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing X-Internal-Key"})
			return
		}
		if !tokenEqual(provided, internalKey) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "invalid internal credential"})
			return
		}
		res := src.Latest()
		if res == nil {
			writeJSON(w, http.StatusOK, map[string]any{"available": false, "reason": "belum ada analisis"})
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	return mux
}
