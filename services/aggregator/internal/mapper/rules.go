package mapper

import (
	"fmt"
	"strconv"
	"strings"
)

type Rules struct {
	Aliases map[string]string
	Coerce map[string]string
}

// membaca format "baru:lama,baru2:lama2" dan "field:int,field2:float".
func ParseRules(aliases, coerce string) (Rules, error) {
	r := Rules{Aliases: map[string]string{}, Coerce: map[string]string{}}
	for _, pair := range splitPairs(aliases) {
		from, to, ok := strings.Cut(pair, ":")
		if !ok || strings.TrimSpace(from) == "" || strings.TrimSpace(to) == "" {
			return Rules{}, fmt.Errorf("alias %q harus berbentuk field_baru:field_lama", pair)
		}
		r.Aliases[strings.TrimSpace(from)] = strings.TrimSpace(to)
	}
	for _, pair := range splitPairs(coerce) {
		field, typ, ok := strings.Cut(pair, ":")
		typ = strings.TrimSpace(typ)
		if !ok || strings.TrimSpace(field) == "" || (typ != "int" && typ != "float" && typ != "string") {
			return Rules{}, fmt.Errorf("konversi %q harus berbentuk field:int|float|string", pair)
		}
		r.Coerce[strings.TrimSpace(field)] = typ
	}
	return r, nil
}

func splitPairs(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (r Rules) Empty() bool { return len(r.Aliases) == 0 && len(r.Coerce) == 0 }

// mengembalikan salinan raw dengan aturan diterapkan. Field asli tidak diubah.
func (r Rules) Apply(raw map[string]any) map[string]any {
	if r.Empty() {
		return raw
	}
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		out[k] = v
	}
	for from, to := range r.Aliases {
		v, ok := out[from]
		if !ok {
			continue
		}
		if _, exists := out[to]; !exists {
			out[to] = v
		}
		delete(out, from)
	}
	for field, typ := range r.Coerce {
		v, ok := out[field]
		if !ok {
			continue
		}
		if c, ok := coerce(v, typ); ok {
			out[field] = c
		}
	}
	return out
}

func coerce(v any, typ string) (any, bool) {
	switch typ {
	case "int", "float":
		switch n := v.(type) {
		case float64:
			return n, true
		case string:
			f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
			if err != nil {
				return nil, false
			}
			return f, true
		}
	case "string":
		switch n := v.(type) {
		case string:
			return n, true
		case float64:
			return strconv.FormatFloat(n, 'f', -1, 64), true
		case bool:
			return strconv.FormatBool(n), true
		}
	}
	return nil, false
}

func SchemaReason(err error) string {
	return "schema_incompatible: " + err.Error()
}
