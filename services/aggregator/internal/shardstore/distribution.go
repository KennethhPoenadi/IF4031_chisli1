package shardstore

import (
	"context"
	"sort"
	"sync"
	"time"
)

// UnitShare adalah isi satu unit penyimpanan: berapa salinan yang dipegangnya, berapa di antaranya
// berperan primary menurut ring saat ini, dan sebarannya per sumber.
type UnitShare struct {
	Unit     string
	Node     string
	Copies   int
	Primary  int
	Replica  int
	BySource map[string]int
}

// NodeShare menjumlahkan UnitShare yang berada pada node Kubernetes yang sama.
type NodeShare struct {
	Node    string
	Units   []string
	Copies  int
	Primary int
	Replica int
}

type Distribution struct {
	// Total adalah jumlah HazardEvent unik (satu event dihitung sekali walau punya beberapa salinan).
	Total       int
	Replication int
	Units       []UnitShare
	Nodes       []NodeShare
	// MaxMinRatio adalah salinan terbanyak dibagi tersedikit di antara node yang terjangkau.
	// 1,0 berarti rata sempurna. 0 berarti tidak dapat dihitung (kurang dari dua node atau ada node kosong).
	MaxMinRatio float64
	AsOf        time.Time
	Partial     bool
	Unavailable []string
}

// Distribution melaporkan sebaran data per unit dan per node. Primary dan replica dinilai dari ring saat
// ini, bukan dari daftar pemilik yang tersimpan, sehingga hasilnya menggambarkan penempatan yang seharusnya
// dan tetap benar sesudah node ditambah atau dibangun ulang.
func (r *Router) Distribution(ctx context.Context) (Distribution, error) {
	var mu sync.Mutex
	shares := map[string]*UnitShare{}
	unique := map[string]struct{}{}
	tombs := r.loadTombstones(ctx)

	failed := r.each(ctx, "store.distribution", r.cfg.RepairTimeout, func(uctx context.Context, u Unit) error {
		share := &UnitShare{Unit: u.Name(), BySource: map[string]int{}}
		ids := []string{}
		err := u.Keys(uctx, func(k KeyRecord) error {
			if tombstoned(tombs, k.HazardID, k.IngestedAt) {
				return nil
			}
			share.Copies++
			share.BySource[k.Source]++
			if owners := r.Owners(k.HazardID); len(owners) > 0 && owners[0] == u.Name() {
				share.Primary++
			} else {
				share.Replica++
			}
			ids = append(ids, k.HazardID)
			return nil
		})
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		shares[share.Unit] = share
		for _, id := range ids {
			unique[id] = struct{}{}
		}
		return nil
	})
	if len(failed) == len(r.units) {
		return Distribution{}, ErrUnavailable
	}

	out := Distribution{
		Total:       len(unique),
		Replication: r.rf,
		AsOf:        time.Now().UTC(),
		Partial:     !r.ring.Covered(r.coverageDown(), r.rf),
		Unavailable: failed,
	}
	byNode := map[string]*NodeShare{}
	for _, name := range r.UnitNames() {
		share, ok := shares[name]
		if !ok {
			continue
		}
		if r.cfg.Nodes != nil {
			if n, err := r.cfg.Nodes.NodeOf(ctx, name); err == nil {
				share.Node = n
			}
		}
		out.Units = append(out.Units, *share)
		key := share.Node
		if key == "" {
			key = share.Unit
		}
		ns := byNode[key]
		if ns == nil {
			ns = &NodeShare{Node: key}
			byNode[key] = ns
		}
		ns.Units = append(ns.Units, share.Unit)
		ns.Copies += share.Copies
		ns.Primary += share.Primary
		ns.Replica += share.Replica
	}
	for _, ns := range byNode {
		out.Nodes = append(out.Nodes, *ns)
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].Node < out.Nodes[j].Node })

	if len(out.Nodes) >= 2 {
		lo, hi := out.Nodes[0].Copies, out.Nodes[0].Copies
		for _, n := range out.Nodes[1:] {
			if n.Copies < lo {
				lo = n.Copies
			}
			if n.Copies > hi {
				hi = n.Copies
			}
		}
		if lo > 0 {
			out.MaxMinRatio = float64(hi) / float64(lo)
		}
	}
	return out, nil
}
