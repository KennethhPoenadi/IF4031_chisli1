package shardstore

import (
	"context"
	"testing"
)

func distCluster(t *testing.T) (*Router, []*fakeUnit) {
	t.Helper()
	fakes := []*fakeUnit{newFake("store-0"), newFake("store-1"), newFake("store-2")}
	cfg := quietCfg()
	cfg.Nodes = staticNodes{"store-0": "node-a", "store-1": "node-b", "store-2": "node-b"}
	r, err := New([]Unit{fakes[0], fakes[1], fakes[2]}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r, fakes
}

func TestDistributionCountsPrimaryAndReplicaPerUnitAndNode(t *testing.T) {
	r, fakes := distCluster(t)
	load(t, r, 300)

	d, err := r.Distribution(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if d.Total != 300 || d.Partial || d.Replication != 2 {
		t.Fatalf("d = %+v, want 300 event unik, tidak partial, RF 2", d)
	}

	copies, primary := 0, 0
	for _, u := range d.Units {
		if u.Copies != fakes[idx(u.Unit)].count() {
			t.Fatalf("unit %s: copies = %d, penyimpanan memegang %d", u.Unit, u.Copies, fakes[idx(u.Unit)].count())
		}
		if u.Primary+u.Replica != u.Copies {
			t.Fatalf("unit %s: primary+replica = %d, want %d", u.Unit, u.Primary+u.Replica, u.Copies)
		}
		copies += u.Copies
		primary += u.Primary
	}
	if copies != 600 {
		t.Fatalf("total salinan = %d, want 600 (300 event x RF 2)", copies)
	}
	if primary != 300 {
		t.Fatalf("total primary = %d, want tepat satu primary per event (300)", primary)
	}

	if len(d.Nodes) != 2 {
		t.Fatalf("nodes = %+v, want 2 node (store-1 dan store-2 berbagi node-b)", d.Nodes)
	}
	var b NodeShare
	for _, n := range d.Nodes {
		if n.Node == "node-b" {
			b = n
		}
	}
	if len(b.Units) != 2 || b.Copies != d.Units[1].Copies+d.Units[2].Copies {
		t.Fatalf("node-b = %+v, want gabungan store-1 dan store-2", b)
	}
	if d.MaxMinRatio < 1 {
		t.Fatalf("MaxMinRatio = %v, want >= 1", d.MaxMinRatio)
	}
}

func TestDistributionIsPartialAndStillAnswersWhenNodesAreDown(t *testing.T) {
	r, fakes := distCluster(t)
	load(t, r, 120)

	fakes[0].set(true)
	d, err := r.Distribution(context.Background())
	if err != nil {
		t.Fatalf("satu unit mati tidak boleh membuat distribusi gagal: %v", err)
	}
	if d.Partial {
		t.Fatalf("satu unit mati dengan RF 2 harus tetap lengkap, d = %+v", d)
	}
	if len(d.Unavailable) != 1 || d.Unavailable[0] != "store-0" {
		t.Fatalf("unavailable = %v, want [store-0]", d.Unavailable)
	}
	if d.Total != 120 {
		t.Fatalf("total = %d, want 120 (replika menutup unit yang mati)", d.Total)
	}

	fakes[1].set(true)
	d, err = r.Distribution(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !d.Partial {
		t.Fatalf("dua unit mati harus ditandai partial, d = %+v", d)
	}

	fakes[2].set(true)
	if _, err := r.Distribution(context.Background()); err == nil {
		t.Fatal("semua unit mati harus mengembalikan ErrUnavailable")
	}
}

func idx(name string) int { return int(name[len(name)-1] - '0') }
