
//// Tabel bawaan berisi titik pusat 38 provinsi dari tabel referensi BNPB
// (data.bnpb.go.id). Tabel lain dapat dimuat lewat PROVINCE_TABLE_PATH (CSV: nama,lat,lon).
package province

import (
	_ "embed"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

//go:embed provinces.csv
var builtin string

type Province struct {
	Name     string
	Lat, Lon float64
}

type Table struct{ items []Province }

func Builtin() *Table {
	t, err := parse(strings.NewReader(builtin))
	if err != nil {
		panic("tabel provinsi bawaan rusak: " + err.Error())
	}
	return t
}

// Load membaca tabel dari file; path kosong berarti tabel bawaan.
func Load(path string) (*Table, error) {
	if path == "" {
		return Builtin(), nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parse(f)
}

func parse(r io.Reader) (*Table, error) {
	cr := csv.NewReader(r)
	cr.Comment = '#'
	rows, err := cr.ReadAll()
	if err != nil {
		return nil, err
	}
	t := &Table{}
	for i, row := range rows {
		if len(row) != 3 {
			return nil, fmt.Errorf("baris %d: butuh 3 kolom", i+1)
		}
		if i == 0 && strings.EqualFold(strings.TrimSpace(row[0]), "name") {
			continue
		}
		lat, err1 := strconv.ParseFloat(strings.TrimSpace(row[1]), 64)
		lon, err2 := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("baris %d: koordinat tidak valid", i+1)
		}
		t.items = append(t.items, Province{Name: strings.TrimSpace(row[0]), Lat: lat, Lon: lon})
	}
	if len(t.items) == 0 {
		return nil, fmt.Errorf("tabel provinsi kosong")
	}
	return t, nil
}

// HaversineKM menghitung jarak dua titik di permukaan bumi dalam kilometer.
func HaversineKM(lat1, lon1, lat2, lon2 float64) float64 {
	const r = 6371.0088
	toRad := math.Pi / 180
	dLat := (lat2 - lat1) * toRad
	dLon := (lon2 - lon1) * toRad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1*toRad)*math.Cos(lat2*toRad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * r * math.Asin(math.Min(1, math.Sqrt(a)))
}

// Nearest mengembalikan provinsi dengan titik pusat terdekat. Seri diputus menurut nama.
func (t *Table) Nearest(lat, lon float64) string {
	best, bestD := "", math.Inf(1)
	for _, p := range t.items {
		d := HaversineKM(lat, lon, p.Lat, p.Lon)
		if d < bestD || (d == bestD && p.Name < best) {
			best, bestD = p.Name, d
		}
	}
	return best
}
