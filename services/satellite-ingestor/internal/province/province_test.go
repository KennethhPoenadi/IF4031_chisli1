package province

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNearestProvince(t *testing.T) {
	tb := Builtin()
	cases := map[[2]float64]string{
		{-2.51234, 113.80450}: "Kalimantan Tengah", // contoh Lampiran A
		{0.5, 101.4}:          "Riau",
		{-6.2, 106.8}:         "DKI Jakarta",
	}
	for p, want := range cases {
		if got := tb.Nearest(p[0], p[1]); got != want {
			t.Fatalf("Nearest(%v) = %s, ingin %s", p, got, want)
		}
	}
}

func TestLoadCustomTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.csv")
	if err := os.WriteFile(path, []byte("name,lat,lon\nA,0,0\nB,10,10\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tb, err := Load(path)
	if err != nil || tb.Nearest(9, 9) != "B" {
		t.Fatalf("%v", err)
	}
	if _, err := Load(filepath.Join(t.TempDir(), "tidak-ada.csv")); err == nil {
		t.Fatal("file yang tidak ada harus gagal")
	}
}

func TestHaversine(t *testing.T) {
	if d := HaversineKM(0, 0, 0, 1); d < 111 || d > 111.4 {
		t.Fatalf("1 derajat bujur di khatulistiwa = %v km", d)
	}
}
