// compare membandingkan HazardEvent WILDFIRE yang tersimpan (lewat Audit API) dengan
// hasil referensi asisten. Kebakaran dicocokkan menurut himpunan
// detection_keys, bukan menurut ID, sehingga selisih yang dilaporkan bermakna.
//
//	AUDIT_TOKEN=... go run ./cmd/compare -api http://ops.prod.localtest.me -ref referensi.json
//
// File referensi berupa array JSON (atau objek {"items":[...]}) berisi kebakaran dengan
// detection_keys di tingkat atas atau di dalam attributes.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

type fireRec struct {
	ID       string
	Severity string
	Area     string
	Keys     []string
}

func keysOf(m map[string]any) []string {
	raw, ok := m["detection_keys"]
	if !ok {
		if attrs, ok2 := m["attributes"].(map[string]any); ok2 {
			raw = attrs["detection_keys"]
		}
	}
	list, _ := raw.([]any)
	out := make([]string, 0, len(list))
	for _, k := range list {
		if s, ok := k.(string); ok {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func toRec(m map[string]any) fireRec {
	s := func(k string) string { v, _ := m[k].(string); return v }
	id := s("source_ref_id")
	if id == "" {
		id = s("id")
	}
	return fireRec{ID: id, Severity: s("severity"), Area: s("area_name"), Keys: keysOf(m)}
}

func loadRef(path string) ([]fireRec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var items []map[string]any
	if err := json.Unmarshal(b, &items); err != nil {
		var wrapped struct {
			Items []map[string]any `json:"items"`
		}
		if err2 := json.Unmarshal(b, &wrapped); err2 != nil {
			return nil, err
		}
		items = wrapped.Items
	}
	out := make([]fireRec, 0, len(items))
	for _, it := range items {
		out = append(out, toRec(it))
	}
	return out, nil
}

func loadOurs(api, token string) ([]fireRec, bool, error) {
	client := &http.Client{Timeout: 60 * time.Second}
	var out []fireRec
	partial := false
	cursor := ""
	for {
		q := url.Values{"source": {"SATELIT"}, "limit": {"1000"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		req, _ := http.NewRequest(http.MethodGet, strings.TrimRight(api, "/")+"/internal/audit/events?"+q.Encode(), nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			return nil, false, err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, false, fmt.Errorf("audit API HTTP %d: %s", resp.StatusCode, body)
		}
		var page struct {
			Items      []map[string]any `json:"items"`
			NextCursor *string          `json:"next_cursor"`
			Partial    bool             `json:"partial"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, false, err
		}
		partial = partial || page.Partial
		for _, it := range page.Items {
			out = append(out, toRec(it))
		}
		if page.NextCursor == nil || *page.NextCursor == "" {
			return out, partial, nil
		}
		cursor = *page.NextCursor
	}
}

func main() {
	api := flag.String("api", "http://localhost:8090", "alamat Aggregator (Audit API)")
	ref := flag.String("ref", "", "file hasil referensi asisten")
	flag.Parse()
	token := os.Getenv("AUDIT_TOKEN")
	if *ref == "" || token == "" {
		fmt.Fprintln(os.Stderr, "butuh -ref dan AUDIT_TOKEN")
		os.Exit(2)
	}
	refs, err := loadRef(*ref)
	if err != nil {
		fmt.Fprintln(os.Stderr, "referensi:", err)
		os.Exit(1)
	}
	ours, partial, err := loadOurs(*api, token)
	if err != nil {
		fmt.Fprintln(os.Stderr, "audit:", err)
		os.Exit(1)
	}
	sig := func(f fireRec) string { return strings.Join(f.Keys, "\n") }
	refBy := map[string]fireRec{}
	for _, r := range refs {
		refBy[sig(r)] = r
	}
	ourBy := map[string]fireRec{}
	for _, o := range ours {
		ourBy[sig(o)] = o
	}
	// Kunci deteksi -> kebakaran referensi, untuk menjelaskan kebakaran yang tidak sama persis.
	keyRef := map[string]string{}
	for _, r := range refs {
		for _, k := range r.Keys {
			keyRef[k] = r.ID
		}
	}
	match, sevDiff, areaDiff := 0, 0, 0
	var onlyOurs, onlyRef []fireRec
	for s, o := range ourBy {
		r, ok := refBy[s]
		if !ok {
			onlyOurs = append(onlyOurs, o)
			continue
		}
		match++
		if r.Severity != "" && r.Severity != o.Severity {
			sevDiff++
			fmt.Printf("severity berbeda: %s kami=%s referensi=%s\n", o.ID, o.Severity, r.Severity)
		}
		if r.Area != "" && r.Area != o.Area {
			areaDiff++
			fmt.Printf("area berbeda: %s kami=%s referensi=%s\n", o.ID, o.Area, r.Area)
		}
	}
	for s, r := range refBy {
		if _, ok := ourBy[s]; !ok {
			onlyRef = append(onlyRef, r)
		}
	}
	fmt.Printf("\nkebakaran: kami=%d referensi=%d sama persis=%d hanya-kami=%d hanya-referensi=%d severity-beda=%d area-beda=%d partial=%v\n",
		len(ours), len(refs), match, len(onlyOurs), len(onlyRef), sevDiff, areaDiff, partial)
	for _, o := range onlyOurs {
		overlap := map[string]bool{}
		for _, k := range o.Keys {
			if id, ok := keyRef[k]; ok {
				overlap[id] = true
			}
		}
		fmt.Printf("hanya-kami %s (%d deteksi), beririsan dengan referensi: %v\n", o.ID, len(o.Keys), sortedSet(overlap))
	}
	for _, r := range onlyRef {
		fmt.Printf("hanya-referensi %s (%d deteksi)\n", r.ID, len(r.Keys))
	}
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
