package leader

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

var kubeTestNow = time.Date(2026, 11, 16, 3, 4, 5, 0, time.UTC)

func testKubeStore(t *testing.T, handler http.HandlerFunc) *KubeStore {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	store, err := NewKubeStore(KubeStoreConfig{
		BaseURL: server.URL, Namespace: "disaster", Name: "aggregator",
		Token:  func() (string, error) { return "service-token", nil },
		Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestNewKubeStoreRejectsInvalidConfiguration(t *testing.T) {
	validToken := func() (string, error) { return "token", nil }
	validClient := &http.Client{}
	cases := []KubeStoreConfig{
		{BaseURL: "://bad", Namespace: "ns", Name: "lease", Token: validToken, Client: validClient},
		{BaseURL: "ftp://kubernetes", Namespace: "ns", Name: "lease", Token: validToken, Client: validClient},
		{BaseURL: "https://kubernetes", Name: "lease", Token: validToken, Client: validClient},
		{BaseURL: "https://kubernetes", Namespace: "ns", Token: validToken, Client: validClient},
		{BaseURL: "https://kubernetes", Namespace: "ns", Name: "lease", Client: validClient},
		{BaseURL: "https://kubernetes", Namespace: "ns", Name: "lease", Token: validToken},
	}
	for i, cfg := range cases {
		if _, err := NewKubeStore(cfg); err == nil {
			t.Fatalf("case %d accepted invalid configuration", i)
		}
	}
}

func writeKubeLease(t *testing.T, w http.ResponseWriter, status int, lease Lease) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Date", kubeTestNow.Format(http.TimeFormat))
	w.WriteHeader(status)
	raw, err := encodeLease("disaster", "aggregator", lease)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(raw); err != nil {
		t.Fatal(err)
	}
}

func TestKubeStoreGetUsesAuthenticatedLeaseEndpointAndServerTime(t *testing.T) {
	want := Lease{
		UID: "lease-uid", ResourceVersion: "12", Holder: "pod-a", Transitions: 3,
		AcquiredAt: kubeTestNow.Add(-time.Minute), RenewedAt: kubeTestNow.Add(-5 * time.Second),
		Duration: 15 * time.Second,
	}
	store := testKubeStore(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/apis/coordination.k8s.io/v1/namespaces/disaster/leases/aggregator" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer service-token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		writeKubeLease(t, w, http.StatusOK, want)
	})

	got, serverNow, err := store.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Token() != "lease-uid:3" || got.Holder != "pod-a" || !serverNow.Equal(kubeTestNow) {
		t.Fatalf("lease=%#v serverNow=%s", got, serverNow)
	}
}

func TestKubeStoreCreateSendsLeaseUsingObservedServerTime(t *testing.T) {
	desired := Lease{
		Holder: "pod-a", Transitions: 1, AcquiredAt: kubeTestNow,
		RenewedAt: kubeTestNow, Duration: 15 * time.Second,
	}
	store := testKubeStore(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/apis/coordination.k8s.io/v1/namespaces/disaster/leases" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var body kubeLease
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Spec.HolderIdentity != "pod-a" || body.Spec.RenewTime != formatMicroTime(kubeTestNow) ||
			body.Spec.LeaseTransitions != 1 || body.Metadata.ResourceVersion != "" {
			t.Fatalf("unexpected body: %#v", body)
		}
		created := desired
		created.UID, created.ResourceVersion = "lease-uid", "1"
		writeKubeLease(t, w, http.StatusCreated, created)
	})

	created, serverNow, err := store.Create(context.Background(), desired)
	if err != nil {
		t.Fatal(err)
	}
	if created.Token() != "lease-uid:1" || !serverNow.Equal(kubeTestNow) {
		t.Fatalf("created=%#v serverNow=%s", created, serverNow)
	}
}

func TestKubeStoreUpdateUsesResourceVersionForCAS(t *testing.T) {
	current := Lease{
		UID: "lease-uid", ResourceVersion: "7", Holder: "pod-b", Transitions: 2,
		AcquiredAt: kubeTestNow.Add(-time.Minute), RenewedAt: kubeTestNow.Add(-20 * time.Second),
		Duration: 15 * time.Second,
	}
	desired := current
	desired.Holder, desired.Transitions = "pod-a", 3
	desired.AcquiredAt, desired.RenewedAt = kubeTestNow, kubeTestNow
	store := testKubeStore(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/apis/coordination.k8s.io/v1/namespaces/disaster/leases/aggregator" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var body kubeLease
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Metadata.ResourceVersion != "7" || body.Spec.LeaseTransitions != 3 || body.Spec.HolderIdentity != "pod-a" {
			t.Fatalf("CAS body = %#v", body)
		}
		updated := desired
		updated.ResourceVersion = "8"
		writeKubeLease(t, w, http.StatusOK, updated)
	})

	updated, _, err := store.Update(context.Background(), current, desired)
	if err != nil || updated.ResourceVersion != "8" || updated.Token() != "lease-uid:3" {
		t.Fatalf("Update() = %#v, %v", updated, err)
	}
}

func TestKubeStoreMapsNotFoundAndConflict(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   error
	}{
		{http.StatusNotFound, ErrLeaseNotFound},
		{http.StatusConflict, ErrLeaseConflict},
	} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			store := testKubeStore(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Date", kubeTestNow.Format(http.TimeFormat))
				w.WriteHeader(tc.status)
			})
			_, serverNow, err := store.Get(context.Background())
			if !errors.Is(err, tc.want) || !serverNow.Equal(kubeTestNow) {
				t.Fatalf("Get() error=%v serverNow=%s", err, serverNow)
			}
		})
	}
}

func TestKubeStoreRejectsInvalidServerTime(t *testing.T) {
	store := testKubeStore(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Date", "not-a-date")
		w.WriteHeader(http.StatusNotFound)
	})
	if _, _, err := store.Get(context.Background()); err == nil || errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("Get() error = %v, want invalid Date error", err)
	}
}
