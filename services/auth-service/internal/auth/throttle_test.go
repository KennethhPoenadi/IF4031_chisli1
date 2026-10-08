package auth

import (
	"testing"
	"time"
)

func TestLoginThrottleBlocksAfterMaxFailuresAndRecovers(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	throttle := newLoginThrottle(3, time.Minute)
	throttle.now = clock.Now

	for i := 0; i < 2; i++ {
		throttle.fail("10.0.0.1")
	}
	if throttle.blocked("10.0.0.1") {
		t.Fatal("peer must not be blocked below the failure limit")
	}

	throttle.fail("10.0.0.1")
	if !throttle.blocked("10.0.0.1") {
		t.Fatal("peer must be blocked once the failure limit is reached")
	}
	if throttle.blocked("10.0.0.2") {
		t.Fatal("failures of one peer must not block another peer")
	}

	clock.Advance(time.Minute + time.Second)
	if throttle.blocked("10.0.0.1") {
		t.Fatal("peer must be unblocked once the failures leave the window")
	}
}

func TestLoginThrottleResetClearsFailures(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	throttle := newLoginThrottle(2, time.Minute)
	throttle.now = clock.Now

	throttle.fail("10.0.0.1")
	throttle.fail("10.0.0.1")
	if !throttle.blocked("10.0.0.1") {
		t.Fatal("peer should be blocked")
	}

	throttle.reset("10.0.0.1")
	if throttle.blocked("10.0.0.1") {
		t.Fatal("a successful login must clear the failure count")
	}
}
