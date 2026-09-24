package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

type Handler struct {
	svc         *Service
	internalKey string
	log         *slog.Logger
}

func NewHandler(svc *Service, internalKey string, log *slog.Logger) *Handler {
	return &Handler{svc: svc, internalKey: internalKey, log: log}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /token", h.token)
	mux.HandleFunc("GET /public-key", h.publicKey)
	mux.HandleFunc("GET /revocations", h.revocations)
	return h.logging(mux)
}

func (h *Handler) token(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")

	var (
		pair TokenPair
		err  error
	)
	grant := r.PostForm.Get("grant_type")
	switch grant {
	case "client_credentials":
		pair, err = h.svc.Login(r.PostForm.Get("client_id"), r.PostForm.Get("client_secret"))
	case "refresh_token":
		pair, err = h.svc.Refresh(r.PostForm.Get("refresh_token"))
	default:
		writeError(w, http.StatusBadRequest, "unsupported_grant_type")
		return
	}

	switch {
	case errors.Is(err, ErrInvalidClient):
		h.log.Warn("login rejected", "grant", grant, "client_id", r.PostForm.Get("client_id"), "correlation_id", corrID(r))
		writeError(w, http.StatusUnauthorized, "invalid_client")
	case errors.Is(err, ErrTokenReuse):
		h.log.Warn("refresh token reuse, session revoked", "correlation_id", corrID(r))
		writeError(w, http.StatusUnauthorized, "invalid_grant")
	case errors.Is(err, ErrInvalidGrant):
		h.log.Warn("refresh rejected", "correlation_id", corrID(r))
		writeError(w, http.StatusUnauthorized, "invalid_grant")
	case err != nil:
		h.log.Error("token issue failed", "err", err, "correlation_id", corrID(r))
		writeError(w, http.StatusInternalServerError, "server_error")
	default:
		h.log.Info("token issued", "grant", grant, "client_id", pair.ClientID, "scope", pair.Scope, "correlation_id", corrID(r))
		writeJSON(w, http.StatusOK, pair)
	}
}

func (h *Handler) publicKey(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"alg":        "EdDSA",
		"public_key": base64.StdEncoding.EncodeToString(h.svc.PublicKey()),
	})
}

func (h *Handler) revocations(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Internal-Key")), []byte(h.internalKey)) != 1 {
		writeError(w, http.StatusUnauthorized, "invalid_internal_key")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revoked": h.svc.Revocations()})
}

type ctxWriter struct {
	http.ResponseWriter
	status int
}

func (w *ctxWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (h *Handler) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Correlation-ID")
		if !validCorrID(id) {
			id = newCorrID()
			r.Header.Set("X-Correlation-ID", id)
		}
		w.Header().Set("X-Correlation-ID", id)
		cw := &ctxWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(cw, r)
		h.log.Info("request", "method", r.Method, "path", r.URL.Path, "status", cw.status,
			"duration_ms", time.Since(start).Milliseconds(), "correlation_id", id)
	})
}

func corrID(r *http.Request) string { return r.Header.Get("X-Correlation-ID") }

func newCorrID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func validCorrID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code})
}
