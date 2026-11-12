package window

import (
	"fmt"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func TestSnapshotSumsTheWindowAndRanksAreas(t *testing.T) {
	c := &clock{t: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	w := New(30, c.now)
	for i := 0; i < 3; i++ {
		w.Add("WILDFIRE", "AWAS", "Riau")
	}
	w.Add("SEISMIC", "NORMAL", "Aceh")
	c.t = c.t.Add(2 * time.Minute)
	w.Add("WILDFIRE", "SIAGA", "Riau")
	w.Add("WILDFIRE", "SIAGA", "Jambi")

	s := w.Snapshot(15)
	if s.Total != 6 || s.ByCategory["WILDFIRE"] != 5 || s.ByCategory["SEISMIC"] != 1 {
		t.Fatalf("snapshot = %+v", s)
	}
	if s.BySeverity["AWAS"] != 3 || s.BySeverity["SIAGA"] != 2 {
		t.Fatalf("severity = %v", s.BySeverity)
	}
	if len(s.TopAreas) != 3 || s.TopAreas[0].Area != "Riau" || s.TopAreas[0].Count != 4 {
		t.Fatalf("top areas = %+v", s.TopAreas)
	}
}

func TestOldEventsFallOutOfTheWindow(t *testing.T) {
	c := &clock{t: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	w := New(30, c.now)
	w.Add("WILDFIRE", "AWAS", "Riau")
	c.t = c.t.Add(20 * time.Minute)
	w.Add("SEISMIC", "NORMAL", "Aceh")

	if s := w.Snapshot(15); s.Total != 1 || s.ByCategory["WILDFIRE"] != 0 {
		t.Fatalf("event berumur 20 menit tidak boleh masuk jendela 15 menit: %+v", s)
	}
	if s := w.Snapshot(30); s.Total != 2 {
		t.Fatalf("jendela 30 menit harus memuat keduanya: %+v", s)
	}
	c.t = c.t.Add(40 * time.Minute)
	if s := w.Snapshot(15); s.Total != 0 {
		t.Fatalf("semuanya kedaluwarsa: %+v", s)
	}
	if len(w.buckets) != 0 {
		t.Fatalf("bucket kedaluwarsa harus dibuang dari memori, tersisa %d", len(w.buckets))
	}
}

func TestTopAreasIsCappedAndAreaCardinalityIsBounded(t *testing.T) {
	c := &clock{t: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
	w := New(30, c.now)
	for i := 0; i < 2000; i++ {
		w.Add("WILDFIRE", "NORMAL", fmt.Sprintf("area-%04d", i))
	}
	s := w.Snapshot(15)
	if len(s.TopAreas) != TopAreasLimit {
		t.Fatalf("top areas = %d, want %d", len(s.TopAreas), TopAreasLimit)
	}
	if s.Total != 2000 {
		t.Fatalf("total tetap menghitung semua event, dapat %d", s.Total)
	}
	if n := len(w.buckets[w.minute()].byArea); n > maxAreasPerBucket {
		t.Fatalf("jumlah wilayah per bucket = %d, melebihi batas %d", n, maxAreasPerBucket)
	}
}

func TestEmptyWindowHasNoNilMaps(t *testing.T) {
	w := New(30, nil)
	s := w.Snapshot(15)
	if s.Total != 0 || s.ByCategory == nil || s.BySeverity == nil {
		t.Fatalf("snapshot kosong = %+v", s)
	}
}
