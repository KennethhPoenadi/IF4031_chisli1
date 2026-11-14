package shardstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"telemetry"
)

type Doc struct {
	Collection string
	Key        string
	Version    int64
	Deleted    bool
	SortKey    string
	Index      map[string]string
	Data       []byte
	Owners     []string
}

type DocQuery struct {
	Collection string
	Eq map[string]string
	AfterSort string
	AfterKey  string
	Limit     int
	WithoutData bool
	AllCollections bool
}

type DocUnit interface {
	Name() string
	PutDoc(ctx context.Context, d Doc) error
	GetDoc(ctx context.Context, collection, key string) (*Doc, error)
	ScanDocs(ctx context.Context, q DocQuery, fn func(Doc) error) error
	DeleteDocCopy(ctx context.Context, collection, key string) error
}

var docDegraded = telemetry.Counter("store_doc_writes_degraded", "dokumen ingest yang tersimpan kurang dari jumlah salinan yang seharusnya")

// menulis dan membaca Doc di atas Router yang sama.
type DocStore struct {
	r     *Router
	units map[string]DocUnit

	lastVersion  atomic.Int64
	repairNeeded atomic.Bool
	repairMu     sync.Mutex
}

func NewDocStore(r *Router, units []DocUnit) (*DocStore, error) {
	m := make(map[string]DocUnit, len(units))
	for _, u := range units {
		if _, ok := r.units[u.Name()]; !ok {
			return nil, fmt.Errorf("shardstore: unit dokumen %q tidak dikenal router", u.Name())
		}
		m[u.Name()] = u
	}
	if len(m) != len(r.units) {
		return nil, fmt.Errorf("shardstore: setiap unit wajib mendukung dokumen")
	}
	return &DocStore{r: r, units: m}, nil
}

func docRingKey(collection, key string) string { return collection + "\x00" + key }

// menghasilkan versi yang naik monoton di dalam satu proses.
func (s *DocStore) NextVersion() int64 {
	for {
		now := time.Now().UnixNano()
		last := s.lastVersion.Load()
		if now <= last {
			now = last + 1
		}
		if s.lastVersion.CompareAndSwap(last, now) {
			return now
		}
	}
}

func (s *DocStore) owners(collection, key string) []string {
	return s.r.ring.Owners(docRingKey(collection, key), s.r.rf)
}

// menyimpan dokumen ke seluruh pemiliknya. Berhasil bila minimal satu salinan
// tersimpan; salinan yang kurang dilengkapi oleh perbaikan.
func (s *DocStore) Put(ctx context.Context, d Doc) error {
	if d.Collection == "" || d.Key == "" {
		return fmt.Errorf("shardstore: dokumen wajib memiliki collection dan key")
	}
	if d.Version == 0 {
		d.Version = s.NextVersion()
	}
	owners := s.owners(d.Collection, d.Key)
	d.Owners = owners

	var wg sync.WaitGroup
	var ok atomic.Int32
	for _, name := range owners {
		if s.r.isDown(name) {
			continue
		}
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			uctx, cancel := context.WithTimeout(ctx, s.r.cfg.UnitTimeout)
			defer cancel()
			uctx, end := telemetry.StartSpan(uctx, "store.doc.put", "db.system", "mongodb", "store.unit", name, "doc.collection", d.Collection)
			err := s.units[name].PutDoc(uctx, d)
			end(err)
			if err != nil {
				s.r.markDown(name, err)
				return
			}
			ok.Add(1)
		}(name)
	}
	wg.Wait()

	if ok.Load() == 0 {
		for _, name := range s.r.ring.Owners(docRingKey(d.Collection, d.Key), len(s.r.units)) {
			if contains(owners, name) || s.r.isDown(name) {
				continue
			}
			uctx, cancel := context.WithTimeout(ctx, s.r.cfg.UnitTimeout)
			err := s.units[name].PutDoc(uctx, d)
			cancel()
			if err != nil {
				s.r.markDown(name, err)
				continue
			}
			docDegraded(ctx, 1)
			s.repairNeeded.Store(true)
			return nil
		}
		return fmt.Errorf("%w: pemilik dokumen %v", ErrUnavailable, owners)
	}
	if int(ok.Load()) < len(owners) {
		docDegraded(ctx, 1)
		s.repairNeeded.Store(true)
	}
	return nil
}

// menulis tombstone dengan versi baru.
func (s *DocStore) Delete(ctx context.Context, collection, key string) error {
	return s.Put(ctx, Doc{Collection: collection, Key: key, Deleted: true})
}

// membaca semua salinan yang terjangkau dan mengembalikan versi terbaru.
// Mengembalikan nil bila tidak ada atau sudah dihapus.
func (s *DocStore) Get(ctx context.Context, collection, key string) (*Doc, error) {
	candidates := s.r.ring.Owners(docRingKey(collection, key), len(s.r.units))
	var best *Doc
	reached := 0
	for _, name := range candidates {
		if s.r.isDown(name) {
			continue
		}
		uctx, cancel := context.WithTimeout(ctx, s.r.cfg.UnitTimeout)
		d, err := s.units[name].GetDoc(uctx, collection, key)
		cancel()
		if err != nil {
			s.r.markDown(name, err)
			continue
		}
		reached++
		if d != nil && (best == nil || d.Version > best.Version) {
			best = d
		}
	}
	if reached == 0 {
		return nil, ErrUnavailable
	}
	if best == nil || best.Deleted {
		return nil, nil
	}
	return best, nil
}

// menyimpan dokumen hanya bila belum ada salinan hidup.
// created=false berarti dokumen sudah ada sebelumnya (duplikat).
func (s *DocStore) PutIfAbsent(ctx context.Context, d Doc) (created bool, existing *Doc, err error) {
	cur, err := s.Get(ctx, d.Collection, d.Key)
	if err != nil {
		return false, nil, err
	}
	if cur != nil {
		return false, cur, nil
	}
	if err := s.Put(ctx, d); err != nil {
		return false, nil, err
	}
	return true, nil, nil
}

type DocPage struct {
	Docs        []Doc
	NextCursor  string
	Partial     bool
	Unavailable []string
}

type docCursor struct {
	S string `json:"s"`
	K string `json:"k"`
}

func EncodeDocCursor(sortKey, key string) string {
	b, _ := json.Marshal(docCursor{S: sortKey, K: key})
	return base64.RawURLEncoding.EncodeToString(b)
}

func DecodeDocCursor(c string) (string, string, error) {
	if c == "" {
		return "", "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return "", "", ErrBadCursor
	}
	var dc docCursor
	if err := json.Unmarshal(raw, &dc); err != nil || dc.K == "" {
		return "", "", ErrBadCursor
	}
	return dc.S, dc.K, nil
}

func posLess(aS, aK, bS, bK string) bool {
	if aS != bS {
		return aS < bS
	}
	return aK < bK
}

// membaca dokumen hidup lintas seluruh unit, terurut menurut (SortKey, Key),
// dengan pagination. Tombstone menang atas salinan lama.
func (s *DocStore) Scan(ctx context.Context, q DocQuery) (DocPage, error) {
	if q.Limit < 1 {
		q.Limit = 100
	}
	unitQ := q
	unitQ.Limit = q.Limit + 1

	type unitResult struct {
		docs      []Doc
		truncated bool
	}
	var mu sync.Mutex
	results := map[string]unitResult{}
	failed := s.r.each(ctx, "store.doc.scan", s.r.cfg.UnitTimeout*2, func(uctx context.Context, u Unit) error {
		du := s.units[u.Name()]
		var local []Doc
		if err := du.ScanDocs(uctx, unitQ, func(d Doc) error {
			local = append(local, d)
			return nil
		}); err != nil {
			return err
		}
		mu.Lock()
		results[u.Name()] = unitResult{docs: local, truncated: len(local) > q.Limit}
		mu.Unlock()
		return nil
	})
	if len(failed) == len(s.r.units) {
		return DocPage{}, ErrUnavailable
	}

	// Batas aman: posisi terkecil di antara unit yang hasilnya terpotong.
	var cutS, cutK string
	hasCut := false
	for _, res := range results {
		if !res.truncated {
			continue
		}
		last := res.docs[len(res.docs)-1]
		if !hasCut || posLess(last.SortKey, last.Key, cutS, cutK) {
			cutS, cutK, hasCut = last.SortKey, last.Key, true
		}
	}

	merged := map[string]Doc{}
	for _, res := range results {
		for _, d := range res.docs {
			if hasCut && posLess(cutS, cutK, d.SortKey, d.Key) {
				continue
			}
			id := docRingKey(d.Collection, d.Key)
			if cur, ok := merged[id]; !ok || d.Version > cur.Version {
				merged[id] = d
			}
		}
	}
	live := make([]Doc, 0, len(merged))
	for _, d := range merged {
		if !d.Deleted {
			live = append(live, d)
		}
	}
	sort.Slice(live, func(i, j int) bool { return posLess(live[i].SortKey, live[i].Key, live[j].SortKey, live[j].Key) })

	page := DocPage{Unavailable: failed, Partial: !s.r.ring.Covered(s.r.downSet(), s.r.rf)}
	switch {
	case len(live) > q.Limit:
		live = live[:q.Limit]
		last := live[len(live)-1]
		page.NextCursor = EncodeDocCursor(last.SortKey, last.Key)
	case hasCut:
		if len(live) > 0 {
			last := live[len(live)-1]
			page.NextCursor = EncodeDocCursor(last.SortKey, last.Key)
		} else {
			page.NextCursor = EncodeDocCursor(cutS, cutK)
		}
	}
	page.Docs = live
	return page, nil
}

// membaca seluruh dokumen hidup yang cocok (mengikuti seluruh halaman).
func (s *DocStore) ScanAll(ctx context.Context, q DocQuery) ([]Doc, bool, error) {
	var out []Doc
	partial := false
	if q.Limit < 1 {
		q.Limit = 1000
	}
	for {
		page, err := s.Scan(ctx, q)
		if err != nil {
			return nil, false, err
		}
		out = append(out, page.Docs...)
		partial = partial || page.Partial
		if page.NextCursor == "" {
			return out, partial, nil
		}
		q.AfterSort, q.AfterKey, _ = DecodeDocCursor(page.NextCursor)
	}
}

type docHolder struct {
	unit string
	doc  Doc
}

// menyamakan salinan dokumen dengan pemilik yang seharusnya, termasuk tombstone.
func (s *DocStore) RepairOnce(ctx context.Context) error {
	s.repairMu.Lock()
	defer s.repairMu.Unlock()

	var mu sync.Mutex
	byID := map[string][]docHolder{}
	failed := s.r.each(ctx, "store.doc.repair.scan", s.r.cfg.RepairTimeout, func(uctx context.Context, u Unit) error {
		var local []Doc
		if err := s.units[u.Name()].ScanDocs(uctx, DocQuery{AllCollections: true, WithoutData: true}, func(d Doc) error {
			local = append(local, d)
			return nil
		}); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, d := range local {
			id := docRingKey(d.Collection, d.Key)
			byID[id] = append(byID[id], docHolder{unit: u.Name(), doc: d})
		}
		return nil
	})
	if len(failed) == len(s.r.units) {
		return ErrUnavailable
	}
	allUp := len(failed) == 0
	down := s.r.downSet()

	for _, holders := range byID {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		best := holders[0]
		for _, h := range holders[1:] {
			if h.doc.Version > best.doc.Version {
				best = h
			}
		}
		desired := s.owners(best.doc.Collection, best.doc.Key)
		held := map[string]Doc{}
		for _, h := range holders {
			held[h.unit] = h.doc
		}
		var full *Doc
		complete := true
		for _, name := range desired {
			if down[name] {
				complete = false
				continue
			}
			if d, ok := held[name]; ok && d.Version >= best.doc.Version {
				continue
			}
			if full == nil {
				uctx, cancel := context.WithTimeout(ctx, s.r.cfg.UnitTimeout)
				d, err := s.units[best.unit].GetDoc(uctx, best.doc.Collection, best.doc.Key)
				cancel()
				if err != nil || d == nil {
					complete = false
					break
				}
				full = d
			}
			cp := *full
			cp.Owners = desired
			uctx, cancel := context.WithTimeout(ctx, s.r.cfg.UnitTimeout)
			err := s.units[name].PutDoc(uctx, cp)
			cancel()
			if err != nil {
				s.r.markDown(name, err)
				complete = false
			}
		}
		if !allUp || !complete {
			continue
		}
		for _, h := range holders {
			if contains(desired, h.unit) {
				continue
			}
			uctx, cancel := context.WithTimeout(ctx, s.r.cfg.UnitTimeout)
			_ = s.units[h.unit].DeleteDocCopy(uctx, h.doc.Collection, h.doc.Key)
			cancel()
		}
	}
	if allUp {
		s.repairNeeded.Store(false)
	}
	return nil
}

// menjalankan perbaikan dokumen secara berkala.
func (s *DocStore) Run(ctx context.Context) {
	t := time.NewTicker(s.r.cfg.RepairInterval)
	defer t.Stop()
	fast := time.NewTicker(s.r.cfg.PingInterval * 5)
	defer fast.Stop()
	run := func() {
		rctx, cancel := context.WithTimeout(ctx, s.r.cfg.RepairTimeout)
		defer cancel()
		if err := s.RepairOnce(rctx); err != nil && ctx.Err() == nil {
			s.r.cfg.Log.Warn("perbaikan dokumen ingest gagal", "err", err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-fast.C:
			if s.repairNeeded.Load() || s.r.repairNeeded.Load() {
				run()
			}
		case <-t.C:
			run()
		}
	}
}

const hazardTombstoneCollection = "hazard_tombstones"

// mengembalikan waktu penghapusan setiap hazard_id yang pernah dihapus.
func (s *DocStore) HazardTombstones(ctx context.Context) (map[string]time.Time, error) {
	docs, _, err := s.ScanAll(ctx, DocQuery{Collection: hazardTombstoneCollection, WithoutData: true})
	if err != nil {
		return nil, err
	}
	out := make(map[string]time.Time, len(docs))
	for _, d := range docs {
		out[d.Key] = time.Unix(0, d.Version).UTC()
	}
	return out, nil
}
