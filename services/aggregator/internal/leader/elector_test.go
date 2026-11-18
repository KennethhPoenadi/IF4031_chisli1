package leader

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeStore struct {
	now         time.Time
	lease       Lease
	notFound    bool
	createErr   error
	updateErr   error
	createCalls int
	updateCalls int
	getCalls    int
}

func (f *fakeStore) Get(context.Context) (Lease, time.Time, error) {
	f.getCalls++
	if f.notFound {
		return Lease{}, f.now, ErrLeaseNotFound
	}
	return f.lease, f.now, nil
}

func (f *fakeStore) Create(_ context.Context, desired Lease) (Lease, time.Time, error) {
	f.createCalls++
	if f.createErr != nil {
		return Lease{}, f.now, f.createErr
	}
	desired.UID = "lease-uid"
	desired.ResourceVersion = "1"
	f.lease, f.notFound = desired, false
	return desired, f.now, nil
}

func (f *fakeStore) Update(_ context.Context, current, desired Lease) (Lease, time.Time, error) {
	f.updateCalls++
	if f.updateErr != nil {
		return Lease{}, f.now, f.updateErr
	}
	if current.ResourceVersion != f.lease.ResourceVersion {
		return Lease{}, f.now, ErrLeaseConflict
	}
	desired.ResourceVersion = current.ResourceVersion + "x"
	f.lease = desired
	return desired, f.now, nil
}

func newElector(t *testing.T, store Store, identity string) *Elector {
	t.Helper()
	e, err := New(Config{
		Identity: identity, LeaseDuration: 15 * time.Second,
		RetryPeriod: 5 * time.Second, Store: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestNewRejectsUnsafeConfiguration(t *testing.T) {
	validStore := &fakeStore{}
	cases := []Config{
		{LeaseDuration: 15 * time.Second, RetryPeriod: 5 * time.Second, Store: validStore},
		{Identity: "pod-a", LeaseDuration: time.Second, RetryPeriod: 500 * time.Millisecond, Store: validStore},
		{Identity: "pod-a", LeaseDuration: 15 * time.Second, RetryPeriod: 15 * time.Second, Store: validStore},
		{Identity: "pod-a", LeaseDuration: 15 * time.Second, RetryPeriod: 5 * time.Second},
	}
	for i, cfg := range cases {
		if _, err := New(cfg); err == nil {
			t.Fatalf("case %d accepted unsafe configuration", i)
		}
	}
}

func TestStepCreatesFirstLeaseAndIssuesFencingToken(t *testing.T) {
	store := &fakeStore{now: time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC), notFound: true}
	e := newElector(t, store, "pod-a")

	e.step(context.Background())

	got := e.Snapshot()
	if !got.Leader || got.Holder != "pod-a" || got.Token != "lease-uid:1" {
		t.Fatalf("unexpected snapshot: %#v", got)
	}
	if store.createCalls != 1 || store.updateCalls != 0 {
		t.Fatalf("create=%d update=%d", store.createCalls, store.updateCalls)
	}
}

func TestStepUsesServerTimeAndDoesNotTakeActiveForeignLease(t *testing.T) {
	serverNow := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	store := &fakeStore{now: serverNow, lease: Lease{
		UID: "lease-uid", ResourceVersion: "7", Holder: "pod-b", Transitions: 4,
		RenewedAt: serverNow.Add(-5 * time.Second), Duration: 15 * time.Second,
	}}
	e := newElector(t, store, "pod-a")

	e.step(context.Background())

	got := e.Snapshot()
	if got.Leader || got.Holder != "pod-b" || store.updateCalls != 0 {
		t.Fatalf("active foreign lease was taken: snapshot=%#v updates=%d", got, store.updateCalls)
	}
}

func TestExpiredLeaseTakeoverIncrementsFencingToken(t *testing.T) {
	serverNow := time.Date(2026, 11, 14, 10, 0, 30, 0, time.UTC)
	store := &fakeStore{now: serverNow, lease: Lease{
		UID: "lease-uid", ResourceVersion: "9", Holder: "pod-b", Transitions: 4,
		RenewedAt: serverNow.Add(-20 * time.Second), Duration: 15 * time.Second,
	}}
	e := newElector(t, store, "pod-a")

	e.step(context.Background())

	got := e.Snapshot()
	if !got.Leader || got.Token != "lease-uid:5" || store.updateCalls != 1 {
		t.Fatalf("takeover failed: snapshot=%#v updates=%d", got, store.updateCalls)
	}
}

func TestActiveLeaderRenewsWithoutChangingFencingToken(t *testing.T) {
	serverNow := time.Date(2026, 11, 14, 10, 0, 10, 0, time.UTC)
	store := &fakeStore{now: serverNow, lease: Lease{
		UID: "lease-uid", ResourceVersion: "3", Holder: "pod-a", Transitions: 2,
		RenewedAt: serverNow.Add(-5 * time.Second), Duration: 15 * time.Second,
	}}
	e := newElector(t, store, "pod-a")

	e.step(context.Background())

	got := e.Snapshot()
	if !got.Leader || got.Token != "lease-uid:2" || store.updateCalls != 1 {
		t.Fatalf("renewal failed: snapshot=%#v updates=%d", got, store.updateCalls)
	}
	if !store.lease.RenewedAt.Equal(serverNow) {
		t.Fatalf("renewed at %s, want server time %s", store.lease.RenewedAt, serverNow)
	}
}

func TestConfirmReturnsCurrentFencingToken(t *testing.T) {
	serverNow := time.Date(2026, 11, 14, 10, 0, 10, 0, time.UTC)
	store := &fakeStore{now: serverNow, lease: Lease{
		UID: "lease-uid", ResourceVersion: "3", Holder: "pod-a", Transitions: 2,
		RenewedAt: serverNow.Add(-5 * time.Second), Duration: 15 * time.Second,
	}}
	e := newElector(t, store, "pod-a")
	e.observe(store.lease, serverNow, true)

	token, err := e.Confirm(context.Background())
	if err != nil || token != "lease-uid:2" {
		t.Fatalf("Confirm() = %q, %v", token, err)
	}
}

func TestConfirmRejectsOldLeaderAfterFreezeAndTakeover(t *testing.T) {
	serverNow := time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)
	store := &fakeStore{now: serverNow, lease: Lease{
		UID: "lease-uid", ResourceVersion: "1", Holder: "pod-a", Transitions: 1,
		RenewedAt: serverNow, Duration: 15 * time.Second,
	}}
	e := newElector(t, store, "pod-a")
	e.observe(store.lease, serverNow, true)

	store.now = serverNow.Add(20 * time.Second)
	store.lease = Lease{
		UID: "lease-uid", ResourceVersion: "2", Holder: "pod-b", Transitions: 2,
		RenewedAt: store.now, Duration: 15 * time.Second,
	}

	if _, err := e.Confirm(context.Background()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Confirm() error = %v, want ErrNotLeader", err)
	}
	if e.Snapshot().Leader {
		t.Fatal("old leader remained active after takeover")
	}
}

func TestUpdateConflictNeverPublishesLeadership(t *testing.T) {
	serverNow := time.Date(2026, 11, 14, 10, 0, 30, 0, time.UTC)
	store := &fakeStore{now: serverNow, updateErr: ErrLeaseConflict, lease: Lease{
		UID: "lease-uid", ResourceVersion: "9", Holder: "pod-b", Transitions: 4,
		RenewedAt: serverNow.Add(-20 * time.Second), Duration: 15 * time.Second,
	}}
	e := newElector(t, store, "pod-a")

	e.step(context.Background())

	if e.Snapshot().Leader {
		t.Fatal("conflicting takeover published leadership")
	}
}

func TestFailedCreateIsReportedInSnapshotAndNeverGrantsLeadership(t *testing.T) {
	store := &fakeStore{now: time.Date(2026, 11, 18, 1, 56, 4, 0, time.UTC), notFound: true,
		createErr: errors.New("kubernetes Lease API returned HTTP 400")}
	e := newElector(t, store, "pod-a")
	e.step(context.Background())
	snap := e.Snapshot()
	if snap.Leader || snap.Token != "" {
		t.Fatalf("snapshot = %+v, kegagalan tidak boleh memberi kepemimpinan", snap)
	}
	if snap.LastError == "" {
		t.Fatal("kegagalan membuat Lease harus tercatat agar terlihat di log")
	}
	store.createErr = nil
	e.step(context.Background())
	if snap = e.Snapshot(); !snap.Leader || snap.LastError != "" {
		t.Fatalf("setelah pulih snapshot = %+v, want leader tanpa error", snap)
	}
}
