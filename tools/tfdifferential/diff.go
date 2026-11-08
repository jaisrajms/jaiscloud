package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
)

// absent marks a value that exists on one side only.
type absent struct{}

func (absent) String() string { return "<absent>" }

// Diff is one difference between the real-GCP golden and the emulator run.
type Diff struct {
	Path     string
	Real     any
	Emulator any
}

// DiffDoc compares two normalized documents and returns every difference in a
// stable order (sorted path). The result is empty when the emulator matches the
// recording.
func DiffDoc(real, emulator Doc) []Diff {
	a := canonicalDoc(real)
	b := canonicalDoc(emulator)
	var out []Diff
	diffValue("", a, b, &out)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// canonicalDoc routes both sides through JSON so a golden decoded from disk and
// a run built in memory have identical Go types ([]string vs []any, json.Number
// vs float64).
func canonicalDoc(d Doc) any {
	raw, err := json.Marshal(map[string]any{
		"resources": d.Resources,
		"plan":      d.Plan,
		"replan":    d.Replan,
	})
	if err != nil {
		return map[string]any{"resources": d.Resources, "plan": d.Plan, "replan": d.Replan}
	}
	var out any
	if err := decodeJSON(raw, &out); err != nil {
		return map[string]any{"resources": d.Resources, "plan": d.Plan, "replan": d.Replan}
	}
	return out
}

func diffValue(path string, real, emulator any, out *[]Diff) {
	switch rv := real.(type) {
	case map[string]any:
		ev, ok := emulator.(map[string]any)
		if !ok {
			*out = append(*out, Diff{Path: path, Real: real, Emulator: emulator})
			return
		}
		keys := map[string]bool{}
		for k := range rv {
			keys[k] = true
		}
		for k := range ev {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			child := joinPath(path, k)
			aVal, aOK := rv[k]
			bVal, bOK := ev[k]
			switch {
			case !aOK:
				*out = append(*out, Diff{Path: child, Real: absent{}, Emulator: bVal})
			case !bOK:
				*out = append(*out, Diff{Path: child, Real: aVal, Emulator: absent{}})
			default:
				diffValue(child, aVal, bVal, out)
			}
		}
	case []any:
		ev, ok := emulator.([]any)
		if !ok {
			*out = append(*out, Diff{Path: path, Real: real, Emulator: emulator})
			return
		}
		if len(rv) != len(ev) {
			*out = append(*out, Diff{
				Path:     path + ".length",
				Real:     json.Number(strconv.Itoa(len(rv))),
				Emulator: json.Number(strconv.Itoa(len(ev))),
			})
		}
		n := len(rv)
		if len(ev) < n {
			n = len(ev)
		}
		for i := 0; i < n; i++ {
			diffValue(fmt.Sprintf("%s[%d]", path, i), rv[i], ev[i], out)
		}
	default:
		if numbersEqual(real, emulator) {
			return
		}
		if !reflect.DeepEqual(real, emulator) {
			*out = append(*out, Diff{Path: path, Real: real, Emulator: emulator})
		}
	}
}

func joinPath(parent, key string) string {
	if parent == "" {
		return key
	}
	return parent + "." + key
}

// numbersEqual compares json.Number/float64 values numerically so 1 and 1.0 do
// not diff.
func numbersEqual(a, b any) bool {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	return aok && bok && af == bf
}

func toFloat(v any) (float64, bool) {
	switch vv := v.(type) {
	case json.Number:
		f, err := vv.Float64()
		return f, err == nil
	case float64:
		return vv, true
	case int:
		return float64(vv), true
	case int64:
		return float64(vv), true
	default:
		return 0, false
	}
}
