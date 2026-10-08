package authn

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testInternalKey = "internal-key-for-sync-tests"

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (v *Verifier) isRevoked(jti string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	_, ok := v.revoked[jti]
	return ok
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func fastRetries(t *testing.T) {
	t.Helper()
	old := publicKeyRetryDelay
	publicKeyRetryDelay = time.Millisecond
	t.Cleanup(func() { publicKeyRetryDelay = old })
}

func publicKeyServer(t *testing.T, pub ed25519.PublicKey, failFirst int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if int(calls.Add(1)) <= failFirst {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"alg": "EdDSA", "public_key": base64.StdEncoding.EncodeToString(pub)})
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestFetchPublicKeyRetriesUntilAuthServiceIsUp(t *testing.T) {
	fastRetries(t)
	pub, _ := newKeyPair(t)
	srv, calls := publicKeyServer(t, pub, 2)

	got, err := FetchPublicKey(context.Background(), srv.URL, 5, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)))
	if err != nil || !got.Equal(pub) {
		t.Fatalf("key = %v, err = %v", got, err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3 (two failures then success)", calls.Load())
	}
}

func TestFetchPublicKeyGivesUpAfterTheAttemptBudget(t *testing.T) {
	fastRetries(t)
	pub, _ := newKeyPair(t)
	srv, calls := publicKeyServer(t, pub, 100)

	if _, err := FetchPublicKey(context.Background(), srv.URL, 3, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))); err == nil {
		t.Fatal("expected an error once the attempts are exhausted")
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want exactly the 3 allowed attempts", calls.Load())
	}
}

func TestFetchPublicKeyStopsWhenContextIsCancelled(t *testing.T) {
	old := publicKeyRetryDelay
	publicKeyRetryDelay = time.Hour
	t.Cleanup(func() { publicKeyRetryDelay = old })
	pub, _ := newKeyPair(t)
	srv, _ := publicKeyServer(t, pub, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := FetchPublicKey(ctx, srv.URL, 10, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want the context error", err)
	}
}

func TestFetchOnceRejectsUnusableKeyResponses(t *testing.T) {
	pub, _ := newKeyPair(t)
	good := base64.StdEncoding.EncodeToString(pub)

	tests := []struct {
		name string
		body string
		code int
	}{
		{name: "server error", code: http.StatusInternalServerError},
		{name: "not json", body: "nope", code: http.StatusOK},
		{name: "wrong algorithm", body: fmt.Sprintf(`{"alg":"HS256","public_key":%q}`, good), code: http.StatusOK},
		{name: "wrong key length", body: fmt.Sprintf(`{"alg":"EdDSA","public_key":%q}`, base64.StdEncoding.EncodeToString([]byte("short"))), code: http.StatusOK},
		{name: "not base64", body: `{"alg":"EdDSA","public_key":"%%%"}`, code: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.code)
				w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			if _, err := fetchOnce(context.Background(), srv.Client(), srv.URL); err == nil {
				t.Fatal("expected an error")
			}
		})
	}

	t.Run("unreachable and malformed URL", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		target := srv.URL
		srv.Close()
		if _, err := fetchOnce(context.Background(), http.DefaultClient, target); err == nil {
			t.Fatal("expected a connection error")
		}
		if _, err := fetchOnce(context.Background(), http.DefaultClient, "http://exa mple.com"); err == nil {
			t.Fatal("expected a request construction error")
		}
	})
}

func TestFetchRevocationsSendsTheInternalKeyAndHandlesFailures(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-Internal-Key")
		w.Write([]byte(`{"revoked":[{"jti":"a","exp":123}]}`))
	}))
	list, err := fetchRevocations(context.Background(), srv.Client(), srv.URL, testInternalKey)
	srv.Close()
	if err != nil || len(list) != 1 || list[0].JTI != "a" || list[0].Exp != 123 || gotKey != testInternalKey {
		t.Fatalf("list=%v err=%v key=%q", list, err, gotKey)
	}

	for name, handler := range map[string]http.HandlerFunc{
		"unauthorized": func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) },
		"not json":     func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("nope")) },
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(handler)
			defer srv.Close()
			if _, err := fetchRevocations(context.Background(), srv.Client(), srv.URL, testInternalKey); err == nil {
				t.Fatal("expected an error")
			}
		})
	}

	if _, err := fetchRevocations(context.Background(), http.DefaultClient, "http://exa mple.com", testInternalKey); err == nil {
		t.Fatal("expected a request construction error")
	}
}

func TestSyncRevocationsAppliesPrunesAndSurvivesOutages(t *testing.T) {
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, `{"revoked":[{"jti":"live","exp":%d},{"jti":"already-expired","exp":%d}]}`,
			time.Now().Add(time.Hour).Unix(), time.Now().Add(-time.Hour).Unix())
	}))
	defer srv.Close()

	pub, _ := newKeyPair(t)
	v := NewVerifier(pub, testIssuer)
	v.revoked["stale-entry"] = time.Now().Add(-time.Minute) // must be pruned on the next successful sync

	logs := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		v.SyncRevocations(ctx, srv.URL, testInternalKey, 3*time.Millisecond, slog.New(slog.NewJSONHandler(logs, nil)))
		close(done)
	}()

	eventually(t, "the revocation list to be applied", func() bool { return v.isRevoked("live") })
	eventually(t, "expired entries to be pruned", func() bool { return !v.isRevoked("stale-entry") && !v.isRevoked("already-expired") })

	fail.Store(true)
	eventually(t, "the outage to be logged", func() bool { return strings.Contains(logs.String(), "revocation sync failing") })
	time.Sleep(30 * time.Millisecond)
	if !v.isRevoked("live") {
		t.Fatal("when auth-service is unreachable the last known revocation list must be kept")
	}
	if n := strings.Count(logs.String(), "revocation sync failing"); n != 1 {
		t.Fatalf("an outage must be logged once, not on every tick, got %d lines", n)
	}

	fail.Store(false)
	eventually(t, "the recovery to be logged", func() bool { return strings.Contains(logs.String(), "revocation sync recovered") })

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SyncRevocations did not stop after the context was cancelled")
	}
}
