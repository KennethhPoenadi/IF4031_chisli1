package shardstore

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
)

type Ring struct {
	points []ringPoint
	units  []string
}

type ringPoint struct {
	hash uint64
	unit string
}

func hash64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

// NewRing membangun cincin dari daftar unit. vnodes adalah jumlah titik per unit.
func NewRing(units []string, vnodes int) *Ring {
	if vnodes < 1 {
		vnodes = 1
	}
	r := &Ring{units: append([]string(nil), units...)}
	sort.Strings(r.units)
	for _, u := range r.units {
		for i := 0; i < vnodes; i++ {
			r.points = append(r.points, ringPoint{hash: hash64(u + "#" + strconv.Itoa(i)), unit: u})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		return r.points[i].unit < r.points[j].unit
	})
	return r
}

// Units mengembalikan nama unit terurut.
func (r *Ring) Units() []string { return append([]string(nil), r.units...) }

// Owners mengembalikan hingga rf unit berbeda yang memiliki key. Elemen pertama adalah primary.
func (r *Ring) Owners(key string, rf int) []string {
	if len(r.points) == 0 || rf < 1 {
		return nil
	}
	if rf > len(r.units) {
		rf = len(r.units)
	}
	h := hash64(key)
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	return r.ownersFrom(start, rf)
}

func (r *Ring) ownersFrom(start, rf int) []string {
	owners := make([]string, 0, rf)
	seen := make(map[string]bool, rf)
	for i := 0; len(owners) < rf && i < len(r.points); i++ {
		p := r.points[(start+i)%len(r.points)]
		if !seen[p.unit] {
			seen[p.unit] = true
			owners = append(owners, p.unit)
		}
	}
	return owners
}

// Covered memberi tahu apakah setiap kunci masih punya minimal satu pemilik di luar down.
// Dipakai untuk menentukan apakah sebuah jawaban lengkap saat sebagian unit tidak terjangkau.
func (r *Ring) Covered(down map[string]bool, rf int) bool {
	if len(down) == 0 {
		return true
	}
	if rf > len(r.units) {
		rf = len(r.units)
	}
	for i := range r.points {
		allDown := true
		for _, u := range r.ownersFrom(i, rf) {
			if !down[u] {
				allDown = false
				break
			}
		}
		if allDown {
			return false
		}
	}
	return true
}
