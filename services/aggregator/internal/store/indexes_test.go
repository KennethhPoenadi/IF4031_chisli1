package store

import (
	"errors"
	"testing"
)

func TestIndexGuardCreatesOnceUntilInvalidated(t *testing.T) {
	var g indexGuard
	calls := 0
	create := func() error { calls++; return nil }

	for i := 0; i < 5; i++ {
		if err := g.ensure(create); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("create dipanggil %d kali, want 1 selama unit sehat", calls)
	}

	// Unit gagal lalu kembali (mungkin dengan disk kosong): indeks harus dibuat ulang.
	g.invalidate()
	if err := g.ensure(create); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("create dipanggil %d kali, want 2 setelah invalidate", calls)
	}
}

func TestIndexGuardRetriesAfterAFailedCreate(t *testing.T) {
	var g indexGuard
	calls := 0
	fail := true
	create := func() error {
		calls++
		if fail {
			return errors.New("belum siap")
		}
		return nil
	}
	if err := g.ensure(create); err == nil {
		t.Fatal("kegagalan membuat indeks harus dikembalikan")
	}
	fail = false
	if err := g.ensure(create); err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d, indeks yang gagal dibuat harus dicoba lagi", err, calls)
	}
}

func TestLaterComparesBSONDatetimes(t *testing.T) {
	if later(nil, nil) {
		t.Fatal("nilai bukan datetime tidak boleh dianggap lebih baru")
	}
}
