package shardstore

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// menambahkan dukungan dokumen pada fakeUnit.
type docFake struct {
	*fakeUnit
	dmu  sync.Mutex
	docs map[string]Doc
}

func newDocFake(name string) *docFake {
	return &docFake{fakeUnit: newFake(name), docs: map[string]Doc{}}
}

func (f *docFake) down() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failing
}

func (f *docFake) PutDoc(_ context.Context, d Doc) error {
	if f.down() {
		return errDown
	}
	f.dmu.Lock()
	defer f.dmu.Unlock()
	k := d.Collection + "/" + d.Key
	if cur, ok := f.docs[k]; ok && cur.Version > d.Version {
		return nil
	}
	f.docs[k] = d
	return nil
}

func (f *docFake) GetDoc(_ context.Context, c, k string) (*Doc, error) {
	if f.down() {
		return nil, errDown
	}
	f.dmu.Lock()
	defer f.dmu.Unlock()
	d, ok := f.docs[c+"/"+k]
	if !ok {
		return nil, nil
	}
	return &d, nil
}

func (f *docFake) ScanDocs(_ context.Context, q DocQuery, fn func(Doc) error) error {
	if f.down() {
		return errDown
	}
	f.dmu.Lock()
	var out []Doc
	for _, d := range f.docs {
		if !q.AllCollections && d.Collection != q.Collection {
			continue
		}
		ok := true
		for k, v := range q.Eq {
			if d.Index[k] != v {
				ok = false
			}
		}
		if ok && (q.AfterKey == "" || posLess(q.AfterSort, q.AfterKey, d.SortKey, d.Key)) {
			out = append(out, d)
		}
	}
	f.dmu.Unlock()
	sort.Slice(out, func(i, j int) bool { return posLess(out[i].SortKey, out[i].Key, out[j].SortKey, out[j].Key) })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	for _, d := range out {
		if err := fn(d); err != nil {
			return err
		}
	}
	return nil
}

func (f *docFake) DeleteDocCopy(_ context.Context, c, k string) error {
	f.dmu.Lock()
	defer f.dmu.Unlock()
	delete(f.docs, c+"/"+k)
	return nil
}

func (f *docFake) docCount(coll string) int {
	f.dmu.Lock()
	defer f.dmu.Unlock()
	n := 0
	for _, d := range f.docs {
		if d.Collection == coll {
			n++
		}
	}
	return n
}

func newDocCluster(t *testing.T, names ...string) (*Router, *DocStore, []*docFake) {
	t.Helper()
	fakes := make([]*docFake, len(names))
	units := make([]Unit, len(names))
	docUnits := make([]DocUnit, len(names))
	for i, n := range names {
		fakes[i] = newDocFake(n)
		units[i], docUnits[i] = fakes[i], fakes[i]
	}
	r, err := New(units, quietCfg())
	if err != nil {
		t.Fatal(err)
	}
	ds, err := NewDocStore(r, docUnits)
	if err != nil {
		t.Fatal(err)
	}
	r.SetTombstones(ds)
	return r, ds, fakes
}

func TestDocStoreReplicatesAndScansInOrderWithPagination(t *testing.T) {
	_, ds, fakes := newDocCluster(t, "a", "b", "c")
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if err := ds.Put(ctx, Doc{Collection: "x", Key: fmt.Sprintf("k%02d", i), SortKey: fmt.Sprintf("%03d", 49-i),
			Index: map[string]string{"even": fmt.Sprint(i%2 == 0)}}); err != nil {
			t.Fatal(err)
		}
	}
	total := 0
	for _, f := range fakes {
		total += f.docCount("x")
	}
	if total != 100 {
		t.Fatalf("salinan = %d, ingin 100 (RF=2)", total)
	}

	var got []string
	q := DocQuery{Collection: "x", Limit: 7}
	for {
		page, err := ds.Scan(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range page.Docs {
			got = append(got, d.SortKey)
		}
		if page.NextCursor == "" {
			break
		}
		q.AfterSort, q.AfterKey, _ = DecodeDocCursor(page.NextCursor)
	}
	if len(got) != 50 || !sort.StringsAreSorted(got) {
		t.Fatalf("hasil scan %d, terurut=%v", len(got), sort.StringsAreSorted(got))
	}
	even, _, err := ds.ScanAll(ctx, DocQuery{Collection: "x", Eq: map[string]string{"even": "true"}})
	if err != nil || len(even) != 25 {
		t.Fatalf("filter Eq = %d err=%v", len(even), err)
	}
}

func TestDocStoreTombstoneWinsAndRepairDoesNotResurrect(t *testing.T) {
	_, ds, fakes := newDocCluster(t, "a", "b", "c")
	ctx := context.Background()
	_ = ds.Put(ctx, Doc{Collection: "x", Key: "k"})
	owners := ds.owners("x", "k")
	var down *docFake
	for _, f := range fakes {
		if f.name == owners[0] {
			down = f
		}
	}
	// Satu pemilik mati, dokumen dihapus, lalu pemilik itu hidup lagi membawa salinan lama.
	down.set(true)
	if err := ds.Delete(ctx, "x", "k"); err != nil {
		t.Fatal(err)
	}
	down.set(false)
	ds.r.markUp(down.name)
	if d, err := ds.Get(ctx, "x", "k"); err != nil || d != nil {
		t.Fatalf("dokumen terhapus masih terbaca: %+v %v", d, err)
	}
	if err := ds.RepairOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if page, _ := ds.Scan(ctx, DocQuery{Collection: "x"}); len(page.Docs) != 0 {
		t.Fatal("perbaikan menghidupkan kembali dokumen yang dihapus")
	}
	for _, f := range fakes {
		if d, _ := f.GetDoc(ctx, "x", "k"); d != nil && !d.Deleted && contains(owners, f.name) {
			t.Fatalf("pemilik %s masih menyimpan salinan hidup", f.name)
		}
	}
}

func TestPutIfAbsentReportsDuplicate(t *testing.T) {
	_, ds, _ := newDocCluster(t, "a", "b")
	ctx := context.Background()
	created, _, err := ds.PutIfAbsent(ctx, Doc{Collection: "r", Key: "rep-1", Data: []byte("1")})
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	created, existing, err := ds.PutIfAbsent(ctx, Doc{Collection: "r", Key: "rep-1", Data: []byte("2")})
	if err != nil || created || string(existing.Data) != "1" {
		t.Fatalf("duplikat tidak terdeteksi: created=%v existing=%+v", created, existing)
	}
}

func TestRouterDeleteSurvivesRepairFromStaleReplica(t *testing.T) {
	r, _, fakes := newDocCluster(t, "a", "b", "c")
	ctx := context.Background()
	ev := event(1)
	ev.IngestedAt = time.Now().UTC().Add(-time.Minute)
	if err := r.SaveHazardEvent(ev); err != nil {
		t.Fatal(err)
	}
	owners := r.Owners(ev.HazardID)
	var stale *docFake
	for _, f := range fakes {
		if f.name == owners[1] {
			stale = f
		}
	}
	stale.set(true)
	if err := r.Delete(ctx, ev.HazardID); err != nil {
		t.Fatal(err)
	}
	stale.set(false)
	r.markUp(stale.name)
	if !stale.has(ev.HazardID) {
		t.Fatal("prasyarat: replika yang mati masih membawa salinan lama")
	}
	sum, err := r.Checksum(ctx, "")
	if err != nil || sum.Count != 0 {
		t.Fatalf("checksum menghitung event terhapus: %+v %v", sum, err)
	}
	if _, err := r.RepairOnce(ctx); err != nil {
		t.Fatal(err)
	}
	for _, f := range fakes {
		if f.has(ev.HazardID) {
			t.Fatalf("unit %s masih menyimpan event terhapus setelah perbaikan", f.name)
		}
	}
	// Event dengan id yang sama yang dibuat ulang sesudah dihapus tetap hidup.
	ev.IngestedAt = time.Now().UTC().Add(time.Second)
	if err := r.SaveHazardEvent(ev); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RepairOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if sum, _ := r.Checksum(ctx, ""); sum.Count != 1 {
		t.Fatalf("event yang dibuat ulang hilang: %+v", sum)
	}
}
