package analysis

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const key = "kunci-internal-uji-RAHASIA"

func TestLatestSendsTheInternalKeyAndReturnsTheJSON(t *testing.T) {
	var gotKey, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey, gotPath = r.Header.Get("X-Internal-Key"), r.URL.Path
		_, _ = w.Write([]byte(`{"available":true,"summary":"ok"}`))
	}))
	defer srv.Close()
	c, err := New(srv.URL+"/", key, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := c.Latest(context.Background())
	if err != nil || !strings.Contains(string(raw), `"summary":"ok"`) {
		t.Fatalf("raw=%s err=%v", raw, err)
	}
	if gotKey != key || gotPath != "/analysis/latest" {
		t.Fatalf("key ok=%v path=%q", gotKey == key, gotPath)
	}
}

func TestNotConfiguredReturnsNilClient(t *testing.T) {
	c, err := New("  ", key, 0, nil)
	if c != nil || err != nil {
		t.Fatalf("c=%v err=%v", c, err)
	}
	if _, err := New("ftp://x", key, 0, nil); err == nil {
		t.Fatal("skema selain http dan https harus ditolak")
	}
}

func TestEveryFailureIsAnErrorThatNeverLeaksTheKey(t *testing.T) {
	cases := map[string]http.HandlerFunc{
		"500": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom "+r.Header.Get("X-Internal-Key"), 500)
		},
		"403":        func(w http.ResponseWriter, r *http.Request) { http.Error(w, "x", http.StatusForbidden) },
		"bukan json": func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("<html>")) },
		"array":      func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("[1,2]")) },
		"terlalu besar": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"x":"` + strings.Repeat("a", 70<<10) + `"}`))
		},
	}
	for name, h := range cases {
		srv := httptest.NewServer(h)
		c, _ := New(srv.URL, key, time.Second, nil)
		_, err := c.Latest(context.Background())
		srv.Close()
		if err == nil {
			t.Fatalf("%s: seharusnya error", name)
		}
		if strings.Contains(err.Error(), key) {
			t.Fatalf("%s: error membocorkan kunci: %v", name, err)
		}
	}
}

func TestASlowAnalystIsCutOffQuickly(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	c, _ := New(srv.URL, key, 150*time.Millisecond, nil)
	start := time.Now()
	if _, err := c.Latest(context.Background()); err == nil {
		t.Fatal("seharusnya timeout")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("analyst yang lambat menahan dashboard %v", time.Since(start))
	}
}

func TestUnreachableAnalystIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()
	c, _ := New(url, key, time.Second, nil)
	if _, err := c.Latest(context.Background()); err == nil || strings.Contains(err.Error(), key) {
		t.Fatalf("err=%v", err)
	}
}

func TestRedirectsAreNotFollowed(t *testing.T) {
	hits := 0
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"a":1}`))
	}))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	c, _ := New(srv.URL, key, time.Second, nil)
	if _, err := c.Latest(context.Background()); err == nil || hits != 0 {
		t.Fatalf("err=%v hits=%d: kunci internal tidak boleh ikut ke alamat lain", err, hits)
	}
}
