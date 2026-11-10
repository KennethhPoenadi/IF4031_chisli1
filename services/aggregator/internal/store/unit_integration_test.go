package store_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"aggregator/internal/hazard"
	"aggregator/internal/shardstore"
	"aggregator/internal/store"
)

// Tes integrasi terhadap MongoDB sungguhan. Dilewati kecuali MONGO_TEST_URIS diisi dengan tiga URI yang
// dipisah koma, mis. MONGO_TEST_URIS=mongodb://localhost:27101,mongodb://localhost:27102,mongodb://localhost:27103
// Tiap URI adalah satu unit penyimpanan mandiri. Lihat README untuk perintah docker-nya.
func uris(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("MONGO_TEST_URIS")
	if raw == "" {
		t.Skip("MONGO_TEST_URIS tidak diisi, tes integrasi MongoDB dilewati")
	}
	list := strings.Split(raw, ",")
	if len(list) < 3 {
		t.Fatal("MONGO_TEST_URIS butuh minimal tiga URI")
	}
	return list
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func evt(i int) hazard.Event {
	types := []hazard.Type{hazard.TypeSeismic, hazard.TypeVolcanic, hazard.TypeWildfire, hazard.TypeCoastal}
	base := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	return hazard.Event{
		HazardID: fmt.Sprintf("hz-%06d", i), Source: hazard.SourceBMKG, SourceRefID: fmt.Sprintf("ref-%06d", i),
		HazardType: types[i%4], Severity: hazard.SeverityWaspada, AreaName: "Area",
		Latitude: -2.5, Longitude: 113.8,
		OccurredAt: base.Add(time.Duration(i) * time.Second),
		IngestedAt: base.Add(time.Duration(i/3) * time.Second), // ada yang berbagi waktu: tie-break hazard_id
		Attributes: map[string]any{"i": int32(i)},
	}
}

func evtAnalytics(i int) hazard.Event {
	ev := evt(i)
	ev.AreaName = []string{"Sumatera Barat", "Jawa Barat", "Kalimantan Tengah"}[i%3]
	ev.Severity = []hazard.Severity{hazard.SeverityNormal, hazard.SeverityWaspada, hazard.SeveritySiaga, hazard.SeverityAwas}[i%4]
	if ev.HazardType == hazard.TypeSeismic && i%8 == 0 {
		ev.Attributes[hazard.TsunamiWarningKey] = "Siaga"
	}
	return ev
}

// expectedAnalytics menghitung langsung dari daftar event, sebagai pembanding angka dashboard.
func expectedAnalytics(events []hazard.Event) (total int, byCat, bySeverity, byArea map[string]int) {
	byCat, bySeverity, byArea = map[string]int{}, map[string]int{}, map[string]int{}
	for _, ev := range events {
		total++
		byCat[ev.Category()]++
		bySeverity[string(ev.Severity)]++
		byArea[ev.AreaName]++
	}
	return
}

func checkAnalytics(t *testing.T, label string, got shardstore.Analytics, events []hazard.Event) {
	t.Helper()
	total, byCat, bySeverity, byArea := expectedAnalytics(events)
	gotCat := map[string]int{}
	for _, cats := range got.Series {
		for c, n := range cats {
			gotCat[c] += n
		}
	}
	if got.Total != total {
		t.Fatalf("%s: total = %d, want %d", label, got.Total, total)
	}
	for _, pair := range []struct {
		name      string
		got, want map[string]int
	}{{"kategori", gotCat, byCat}, {"severity", got.Severity, bySeverity}, {"area", got.Area, byArea}} {
		for k, n := range pair.want {
			if pair.got[k] != n {
				t.Fatalf("%s: %s %s = %d, want %d", label, pair.name, k, pair.got[k], n)
			}
		}
	}
}

type cluster struct {
	router *shardstore.Router
	units  []*store.MongoUnit
	dbName string
	uris   []string
}

func newCluster(t *testing.T, uris []string, dbName string) *cluster {
	t.Helper()
	c := &cluster{dbName: dbName, uris: uris}
	var units []shardstore.Unit
	for i, uri := range uris {
		u, err := store.NewMongoUnit(fmt.Sprintf("store-%d", i), uri, dbName, "hazard_events")
		if err != nil {
			t.Fatal(err)
		}
		c.units = append(c.units, u)
		units = append(units, u)
	}
	r, err := shardstore.New(units, shardstore.Config{ReplicationFactor: 2, VNodes: 128,
		UnitTimeout: 3 * time.Second, Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	c.router = r
	t.Cleanup(func() {
		for _, u := range c.units {
			u.Close(context.Background())
		}
	})
	return c
}

// rawCount menghitung dokumen langsung di sebuah server, tanpa lewat router.
func rawCount(t *testing.T, uri, dbName string) int64 {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	n, err := client.Database(dbName).Collection("hazard_events").CountDocuments(context.Background(), bson.M{})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func dropDB(t *testing.T, uri, dbName string) {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	if err := client.Database(dbName).Drop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestShardedStoreAgainstRealMongoDB(t *testing.T) {
	all := uris(t)
	three := all[:3]
	dbName := fmt.Sprintf("m2_test_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, u := range all {
			dropDB(t, u, dbName)
		}
	})
	ctx := context.Background()
	c := newCluster(t, three, dbName)

	const n = 400
	events := make([]hazard.Event, n)
	for i := range events {
		events[i] = evtAnalytics(i)
		if err := c.router.SaveHazardEvent(events[i]); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	// 1. tidak ada node yang menyimpan seluruh data, total salinan = 2n
	var total int64
	counts := make([]int64, 3)
	for i, u := range three {
		counts[i] = rawCount(t, u, dbName)
		total += counts[i]
		if counts[i] >= n {
			t.Fatalf("node %d menyimpan seluruh data (%d)", i, counts[i])
		}
	}
	if total != 2*n {
		t.Fatalf("total salinan = %d, want %d; sebaran = %v", total, 2*n, counts)
	}
	t.Logf("sebaran data per node: %v (total %d, RF=2)", counts, total)

	// 2. pagination lintas node: urutan benar, tanpa duplikat
	want := make([]string, n)
	sorted := append([]hazard.Event(nil), events...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].IngestedAt.Equal(sorted[j].IngestedAt) {
			return sorted[i].IngestedAt.After(sorted[j].IngestedAt)
		}
		return sorted[i].HazardID > sorted[j].HazardID
	})
	for i, ev := range sorted {
		want[i] = ev.HazardID
	}
	var got []string
	cursor := ""
	for {
		page, err := c.router.ListEvents(ctx, shardstore.ListQuery{Limit: 50, Cursor: cursor, Order: shardstore.NewestFirst})
		if err != nil {
			t.Fatal(err)
		}
		if page.Partial {
			t.Fatal("tidak boleh partial saat semua node sehat")
		}
		for _, ev := range page.Events {
			got = append(got, ev.HazardID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("urutan atau isi pagination salah: %d event, want %d", len(got), len(want))
	}

	// 2b. angka dashboard analitik sama dengan hitungan langsung dan tidak menghitung replika dua kali
	aq := shardstore.AggregateQuery{From: time.Date(2026, 11, 8, 9, 0, 0, 0, time.UTC),
		To: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC), Bucket: "hour"}
	an, err := c.router.Analytics(ctx, aq)
	if err != nil {
		t.Fatal(err)
	}
	checkAnalytics(t, "analitik normal", an, events)
	if an.Partial || an.Rebalancing {
		t.Fatalf("analitik normal tidak boleh partial/rebalancing: %+v", an)
	}

	before, err := c.router.Checksum(ctx, "BMKG")
	if err != nil || before.Count != n {
		t.Fatalf("checksum awal: %+v err=%v", before, err)
	}

	// 3. satu node mati (alamat tidak terjangkau): semua event tetap terbaca, cepat, tidak partial
	dead := newCluster(t, []string{three[0], "mongodb://127.0.0.1:1", three[2]}, dbName)
	start := time.Now()
	page, err := dead.router.ListEvents(ctx, shardstore.ListQuery{Limit: 1000, Order: shardstore.NewestFirst})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != n || page.Partial || len(page.Unavailable) != 1 || page.Unavailable[0] != "store-1" {
		t.Fatalf("satu node mati: %d event partial=%v unavailable=%v", len(page.Events), page.Partial, page.Unavailable)
	}
	t.Logf("jawaban dengan satu node mati: %d event dalam %v", len(page.Events), time.Since(start))
	anDead, err := dead.router.Analytics(ctx, aq)
	if err != nil {
		t.Fatal(err)
	}
	checkAnalytics(t, "analitik satu node mati", anDead, events)
	if anDead.Partial || len(anDead.Unavailable) != 1 {
		t.Fatalf("satu node mati: partial=%v unavailable=%v", anDead.Partial, anDead.Unavailable)
	}

	// 4. node dimatikan dan seluruh datanya dihapus, lalu kembali kosong: perbaikan memulihkan semuanya
	dropDB(t, three[1], dbName)
	if got := rawCount(t, three[1], dbName); got != 0 {
		t.Fatalf("node korban masih berisi %d dokumen", got)
	}
	stats, err := c.router.RepairOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Replicated == 0 {
		t.Fatalf("perbaikan tidak menyalin apa pun: %+v", stats)
	}
	if got := rawCount(t, three[1], dbName); got != counts[1] {
		t.Fatalf("node pengganti berisi %d dokumen, want %d (sama seperti sebelum dihapus)", got, counts[1])
	}
	after, err := c.router.Checksum(ctx, "BMKG")
	if err != nil || after.Count != before.Count || after.Checksum != before.Checksum {
		t.Fatalf("checksum berubah setelah pemulihan: sebelum %+v sesudah %+v", before, after)
	}
	t.Logf("pemulihan: %+v, checksum tetap %s", stats, after.Checksum)

	// 5. tambah node baru (kosong): ikut menanggung data, salinan lama yang usang terhapus
	if len(all) < 4 {
		t.Log("URI keempat tidak diberikan, rebalancing dilewati")
		return
	}
	four := newCluster(t, all[:4], dbName)
	extra := evt(9999)
	if err := four.router.SaveHazardEvent(extra); err != nil { // penulisan jalan terus saat rebalancing
		t.Fatal(err)
	}
	stats, err = four.router.RepairOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	newCount := rawCount(t, all[3], dbName)
	if newCount == 0 {
		t.Fatalf("node baru tidak menanggung data: %+v", stats)
	}
	var total4 int64
	for _, u := range all[:4] {
		total4 += rawCount(t, u, dbName)
	}
	if total4 != 2*(n+1) {
		t.Fatalf("total salinan = %d, want %d setelah rebalancing", total4, 2*(n+1))
	}
	t.Logf("rebalancing: node baru memegang %d dokumen, stats=%+v", newCount, stats)
	final, err := four.router.Checksum(ctx, "BMKG")
	if err != nil || final.Count != n+1 {
		t.Fatalf("checksum akhir: %+v err=%v", final, err)
	}
}
