package fire

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"satellite-ingestor/internal/detection"
	"satellite-ingestor/internal/province"
)

var base = time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)

// det membuat deteksi pada (lat, lon) dengan selisih menit dari base.
func det(t *testing.T, sat string, minutes int, lat, lon float64, conf string, frp float64) detection.Detection {
	t.Helper()
	at := base.Add(time.Duration(minutes) * time.Minute)
	payload := fmt.Sprintf(`{"satellite":%q,"acq_date":%q,"acq_time":%d,"latitude":%.5f,"longitude":%.5f,"confidence":%q,"frp":%g}`,
		sat, at.Format("2006-01-02"), at.Hour()*100+at.Minute(), lat, lon, conf, frp)
	d, err := detection.Parse([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// offsetKM menggeser titik ke timur sejauh km.
func offsetKM(lat, lon, km float64) (float64, float64) {
	return lat, lon + km/(111.32*math.Cos(lat*math.Pi/180))
}

func newEngine() *Engine {
	e := New(90*time.Minute, province.Builtin())
	e.now = func() time.Time { return base.Add(24 * time.Hour) }
	return e
}

func TestChainAcrossSatellitesFormsOneFire(t *testing.T) {
	e := newEngine()
	lat, lon := -2.5, 113.8
	bLat, bLon := offsetKM(lat, lon, 0.9)
	cLat, cLon := offsetKM(bLat, bLon, 0.9) // A-C 1,8 km: hanya terhubung lewat B
	e.Add(det(t, "N20", 0, lat, lon, "n", 5))
	e.Add(det(t, "N21", 10, bLat, bLon, "h", 6))
	e.Add(det(t, "N20", 20, cLat, cLon, "n", 7))
	ch := e.Flush()
	if len(ch.UpsertFires) != 1 {
		t.Fatalf("kebakaran = %d, ingin 1", len(ch.UpsertFires))
	}
	ev := ch.UpsertFires[0]
	if ev.Attributes["detection_count"] != 3 || ev.AreaName != "Kalimantan Tengah" || ev.Severity != "WASPADA" {
		t.Fatalf("%+v", ev)
	}
	if sats := ev.Attributes["satellites"].([]string); !reflect.DeepEqual(sats, []string{"N20", "N21"}) {
		t.Fatalf("satellites = %v", sats)
	}
	if ev.Attributes["max_confidence"] != "h" || !ev.OccurredAt.Equal(base) {
		t.Fatalf("%+v", ev)
	}
}

func TestBoundariesAreInclusive(t *testing.T) {
	cases := []struct {
		name    string
		km      float64
		minutes int
		want    int
	}{
		{"jarak 0,99 km", 0.99, 0, 1},
		{"jarak 1,01 km", 1.01, 0, 0},
		{"selisih tepat 360 menit", 0.1, 360, 1},
		{"selisih 361 menit", 0.1, 361, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEngine()
			e.lateness = 1000 * time.Hour
			lat, lon := -1.0, 112.0
			l2, o2 := offsetKM(lat, lon, tc.km)
			e.Add(det(t, "N20", 0, lat, lon, "n", 1))
			e.Add(det(t, "N20", tc.minutes, l2, o2, "n", 1))
			if got := len(e.Flush().UpsertFires); got != tc.want {
				t.Fatalf("kebakaran = %d, ingin %d", got, tc.want)
			}
		})
	}
}

func TestLowConfidenceAndSingleDetectionsAreNotFires(t *testing.T) {
	e := newEngine()
	e.Add(det(t, "N20", 0, -1, 112, "n", 1))
	e.Add(det(t, "N20", 1, -1, 112.001, "l", 1)) // low: tidak ikut
	e.Add(det(t, "N20", 2, -3, 115, "n", 1))     // tunggal
	ch := e.Flush()
	if len(ch.UpsertFires) != 0 {
		t.Fatalf("tidak boleh ada kebakaran: %+v", ch.UpsertFires)
	}
	s := e.Stats()
	if s["single_detections_active"] != 2 || s["low_confidence"] != 1 {
		t.Fatalf("%v", s)
	}
}

func TestDuplicateCountedOnce(t *testing.T) {
	e := newEngine()
	d := det(t, "N20", 0, -1, 112, "n", 1)
	if e.Add(d) != Accepted || e.Add(d) != Duplicate {
		t.Fatal("deteksi yang sama harus dihitung sekali")
	}
}

func TestSeverityThresholds(t *testing.T) {
	for _, tc := range []struct {
		n    int
		frp  float64
		want string
	}{{2, 0, "WASPADA"}, {6, 0, "SIAGA"}, {2, 30, "SIAGA"}, {20, 0, "AWAS"}, {2, 100, "AWAS"}, {5, 29.99, "WASPADA"}} {
		if got := Severity(tc.n, tc.frp); got != tc.want {
			t.Fatalf("Severity(%d,%v)=%s ingin %s", tc.n, tc.frp, got, tc.want)
		}
	}
}

func TestMergeSplitAndRetractUpdateExistingEvents(t *testing.T) {
	e := newEngine()
	lat, lon := -1.0, 112.0
	l1, o1 := offsetKM(lat, lon, 0.5)
	l3, o3 := offsetKM(lat, lon, 1.6) // b-c 1,1 km: dua kebakaran terpisah
	l4, o4 := offsetKM(lat, lon, 2.1)
	a, b := det(t, "N20", 0, lat, lon, "n", 1), det(t, "N20", 1, l1, o1, "n", 1)
	c, d := det(t, "N21", 2, l3, o3, "n", 1), det(t, "N21", 3, l4, o4, "n", 1)
	e.Add(a)
	e.Add(b)
	e.Add(c)
	e.Add(d)
	ch := e.Flush()
	if len(ch.UpsertFires) != 2 {
		t.Fatalf("awal: 2 kebakaran terpisah, dapat %d", len(ch.UpsertFires))
	}
	// Deteksi jembatan menggabungkan keduanya: satu event diperbarui, satu dihapus.
	lb, ob := offsetKM(lat, lon, 1.05) // b-jembatan 0,55 km, jembatan-c 0,55 km
	bridge := det(t, "N20", 4, lb, ob, "h", 1)
	e.Add(bridge)
	ch = e.Flush()
	if len(ch.UpsertFires) != 1 || len(ch.DeleteFires) != 1 {
		t.Fatalf("merge: upsert=%d delete=%d", len(ch.UpsertFires), len(ch.DeleteFires))
	}
	merged := ch.UpsertFires[0].HazardID
	if merged != "hz-"+FireID([]detection.Detection{a}) {
		t.Fatal("kebakaran gabungan memakai identitas deteksi paling awal")
	}
	// Jembatan ditarik: kembali terpecah dua; event gabungan diperbarui, satu dibuat lagi.
	if e.Retract(bridge.Key) != Retracted {
		t.Fatal("retract gagal")
	}
	ch = e.Flush()
	if len(ch.UpsertFires) != 2 || len(ch.DeleteFires) != 0 || len(ch.DeleteDetections) != 1 {
		t.Fatalf("split: upsert=%d delete=%d deldet=%d", len(ch.UpsertFires), len(ch.DeleteFires), len(ch.DeleteDetections))
	}
	// Menyusut di bawah 2 deteksi: event dihapus, bukan dibiarkan.
	e.Retract(c.Key)
	ch = e.Flush()
	if len(ch.DeleteFires) != 1 {
		t.Fatalf("menyusut: delete=%d", len(ch.DeleteFires))
	}
	// Update FRP mengubah severity event yang sama.
	b.FRP = 40
	e.Update(b)
	ch = e.Flush()
	if len(ch.UpsertFires) != 1 || ch.UpsertFires[0].HazardID != merged || ch.UpsertFires[0].Severity != "SIAGA" {
		t.Fatalf("update: %+v", ch.UpsertFires)
	}
}

func TestLateDetectionIsRecordedNotSilentlyDropped(t *testing.T) {
	e := newEngine()
	e.Add(det(t, "N20", 600, -1, 112, "n", 1))
	if got := e.Add(det(t, "N20", 400, -1, 112.001, "n", 1)); got != Late {
		t.Fatalf("deteksi 200 menit di belakang dengan batas 90 menit harus terlambat, dapat %s", got)
	}
	if got := e.Add(det(t, "N20", 550, -1, 112.002, "n", 1)); got != Accepted {
		t.Fatalf("deteksi dalam batas tetap diterima, dapat %s", got)
	}
	ch := e.Flush()
	if len(ch.Late) != 1 || e.Stats()["late"] != 1 || len(ch.UpsertFires) != 1 {
		t.Fatalf("late=%d stats=%v fires=%d", len(ch.Late), e.Stats(), len(ch.UpsertFires))
	}
}

func TestFinalizedFireLeavesMemoryWithoutDeletion(t *testing.T) {
	e := newEngine()
	e.Add(det(t, "N20", 0, -1, 112, "n", 1))
	e.Add(det(t, "N20", 5, -1, 112.001, "n", 1))
	e.Flush()
	e.ObserveVirtualNow(base.Add(10 * time.Hour)) // watermark melewati 360 menit sesudahnya
	ch := e.Flush()
	if len(ch.FinalizedFires) != 1 || len(ch.DeleteFires) != 0 || len(ch.FinalDetections) != 2 || e.ActiveDetections() != 0 {
		t.Fatalf("%+v active=%d", ch, e.ActiveDetections())
	}
}

type event struct {
	kind string
	d    detection.Detection
}

// replay memproses aliran dengan flush setiap batch dan mengembalikan kebakaran akhir
// (id -> event JSON tanpa ingested_at) sebagaimana tersimpan di Aggregator.
func replay(t *testing.T, stream []event, crashAt map[int]bool) map[string]string {
	t.Helper()
	stored := map[string]string{}
	persisted := map[string]detection.Detection{}
	var cpEmitted map[string]string
	var cpMax, cpVirtual time.Time
	cpOffset := -1
	e := newEngine()
	var pendingFinal []detection.Detection
	apply := func(ch Changes) {
		// Tanda final dari batch sebelumnya ditulis lebih dulu (lihat Changes.FinalDetections).
		touched := map[string]bool{}
		for _, d := range ch.PutDetections {
			touched[d.Key] = true
		}
		for _, k := range ch.DeleteDetections {
			touched[k] = true
		}
		for _, d := range pendingFinal {
			if !touched[d.Key] {
				persisted[d.Key] = d
			}
		}
		pendingFinal = ch.FinalDetections
		for _, f := range ch.UpsertFires {
			f.IngestedAt = time.Time{}
			b, _ := json.Marshal(f)
			stored[f.HazardID] = string(b)
		}
		for _, id := range ch.DeleteFires {
			delete(stored, id)
		}
		for _, d := range ch.PutDetections {
			persisted[d.Key] = d
		}
		for _, k := range ch.DeleteDetections {
			delete(persisted, k)
		}
	}
	for i := 0; i < len(stream); {
		end := min(i+7, len(stream))
		for j := i; j < end; j++ {
			ev := stream[j]
			ev.d.Offset = int64(j)
			switch ev.kind {
			case "detection":
				e.Add(ev.d)
			case "update":
				e.Update(ev.d)
			case "retract":
				e.Retract(ev.d.Key)
			}
		}
		apply(e.Flush())
		if crashAt[i] {
			// Mati setelah menulis hasil tetapi sebelum checkpoint: ulang dari checkpoint.
			// Tanda final yang belum sempat ditulis ikut hilang bersama proses.
			pendingFinal = nil
			e = newEngine()
			var dets []detection.Detection
			for _, d := range persisted {
				if int(d.Offset) <= cpOffset {
					dets = append(dets, d)
				}
			}
			e.Restore(dets, cpMax, cpVirtual, cpEmitted, nil)
			apply(e.Flush())
			delete(crashAt, i)
			i = cpOffset + 1
			continue
		}
		cpOffset, cpMax, cpVirtual, cpEmitted = end-1, e.MaxEventTime(), e.VirtualNow(), e.ActiveFires()
		i = end
	}
	return stored
}

func syntheticStream(t *testing.T, seed int64) []event {
	rng := rand.New(rand.NewSource(seed))
	var out []event
	var accepted []detection.Detection
	sats := []string{"N20", "N21", "SNPP"}
	for i := 0; i < 400; i++ {
		switch r := rng.Intn(20); {
		case r == 0 && len(accepted) > 0:
			out = append(out, event{"retract", accepted[rng.Intn(len(accepted))]})
		case r == 1 && len(accepted) > 0:
			u := accepted[rng.Intn(len(accepted))]
			u.FRP = rng.Float64() * 50
			out = append(out, event{"update", u})
		case r == 2 && len(accepted) > 0:
			out = append(out, event{"detection", accepted[rng.Intn(len(accepted))]})
		default:
			cluster := rng.Intn(25)
			lat := -3 + float64(cluster%5)*0.4 + rng.Float64()*0.01
			lon := 110 + float64(cluster/5)*0.4 + rng.Float64()*0.01
			conf := []string{"l", "n", "h"}[rng.Intn(3)]
			d := det(t, sats[rng.Intn(3)], i*2-rng.Intn(60), lat, lon, conf, rng.Float64()*10)
			accepted = append(accepted, d)
			out = append(out, event{"detection", d})
		}
	}
	return out
}

func TestCrashRecoveryProducesIdenticalResult(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		stream := syntheticStream(t, seed)
		want := replay(t, stream, map[int]bool{})
		if len(want) == 0 {
			t.Fatal("aliran sintetis harus menghasilkan kebakaran")
		}
		rng := rand.New(rand.NewSource(seed * 100))
		crashes := map[int]bool{}
		for i := 0; i < 20; i++ {
			crashes[rng.Intn(len(stream)/7)*7] = true
		}
		got := replay(t, stream, crashes)
		if !reflect.DeepEqual(keys(got), keys(want)) {
			t.Fatalf("seed %d: kebakaran berbeda setelah crash\nwant %v\ngot  %v", seed, keys(want), keys(got))
		}
		for id := range want {
			if got[id] != want[id] {
				t.Fatalf("seed %d: isi %s berbeda", seed, id)
			}
		}
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
