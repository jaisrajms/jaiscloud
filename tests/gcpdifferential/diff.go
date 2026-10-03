//go:build gcp_differential

package gcpdifferential

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// Divergence is one differential finding between the real-GCP golden and the
// emulator replay. Kind is one of:
//
//	status_mismatch, missing_field, extra_field, type_mismatch,
//	value_mismatch, array_length_mismatch, body_mismatch
type Divergence struct {
	Service  string `json:"service"`
	Op       string `json:"op"`
	Method   string `json:"method"`
	Path     string `json:"path"`
	Kind     string `json:"kind"`
	Severity string `json:"severity"`
	Location string `json:"location"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
}

// DiffExchanges compares a golden exchange (real GCP) against a replayed one
// (emulator) and returns the divergences.
func DiffExchanges(golden, actual Exchange) []Divergence {
	base := Divergence{Service: golden.Service, Op: golden.Op, Method: golden.Method, Path: golden.Path}
	var divs []Divergence

	add := func(kind, severity, location, expected, actual string) {
		d := base
		d.Kind, d.Severity, d.Location, d.Expected, d.Actual = kind, severity, location, expected, actual
		divs = append(divs, d)
	}

	if golden.Status != actual.Status {
		add("status_mismatch", "high", "status", strconv.Itoa(golden.Status), strconv.Itoa(actual.Status))
	}

	gBody := decode(golden.Response)
	aBody := decode(actual.Response)
	switch {
	case gBody == nil && aBody != nil:
		add("body_mismatch", "high", "response", "empty", truncate(canonical(aBody)))
	case gBody != nil && aBody == nil:
		add("body_mismatch", "high", "response", truncate(canonical(gBody)), "empty")
	case gBody != nil && aBody != nil:
		divs = append(divs, diffValue(base, "response", gBody, aBody)...)
	}

	if len(golden.Request) > 0 || len(actual.Request) > 0 {
		gReq, aReq := decode(golden.Request), decode(actual.Request)
		// Request bodies are produced by the harness, so any difference is a
		// harness/normalization artifact rather than an emulator divergence.
		// They are reported at info severity for visibility only.
		for _, d := range diffValue(base, "request", gReq, aReq) {
			d.Severity = "info"
			divs = append(divs, d)
		}
	}
	return divs
}

func decode(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return v
}

func diffValue(base Divergence, loc string, g, a any) []Divergence {
	var divs []Divergence
	add := func(kind, severity, location, expected, actual string) {
		d := base
		d.Kind, d.Severity, d.Location, d.Expected, d.Actual = kind, severity, location, expected, actual
		divs = append(divs, d)
	}

	gm, gok := g.(map[string]any)
	am, aok := a.(map[string]any)
	if gok || aok {
		if !gok || !aok {
			add("type_mismatch", "high", loc, typeName(g), typeName(a))
			return divs
		}
		keys := make([]string, 0, len(gm))
		for k := range gm {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			gv := gm[k]
			av, ok := am[k]
			if !ok {
				add("missing_field", "high", joinLoc(loc, k), truncate(canonical(gv)), "absent")
				continue
			}
			divs = append(divs, diffValue(base, joinLoc(loc, k), gv, av)...)
		}
		extra := make([]string, 0)
		for k := range am {
			if _, ok := gm[k]; !ok {
				extra = append(extra, k)
			}
		}
		sort.Strings(extra)
		for _, k := range extra {
			add("extra_field", "info", joinLoc(loc, k), "absent", truncate(canonical(am[k])))
		}
		return divs
	}

	ga, gok := g.([]any)
	aa, aok := a.([]any)
	if gok || aok {
		if !gok || !aok {
			add("type_mismatch", "high", loc, typeName(g), typeName(a))
			return divs
		}
		if len(ga) != len(aa) {
			add("array_length_mismatch", "medium", loc, strconv.Itoa(len(ga)), strconv.Itoa(len(aa)))
		}
		n := len(ga)
		if len(aa) < n {
			n = len(aa)
		}
		for i := 0; i < n; i++ {
			divs = append(divs, diffValue(base, fmt.Sprintf("%s[%d]", loc, i), ga[i], aa[i])...)
		}
		return divs
	}

	if typeName(g) != typeName(a) {
		add("type_mismatch", "high", loc, typeName(g), typeName(a))
		return divs
	}
	if canonical(g) != canonical(a) {
		sev := "medium"
		if isMessageField(loc) {
			sev = "low"
		}
		add("value_mismatch", sev, loc, truncate(canonical(g)), truncate(canonical(a)))
	}
	return divs
}

// isMessageField reports whether loc points at a human-readable message, where
// wording differences are informative but not contract violations.
func isMessageField(loc string) bool {
	return hasSuffix(loc, ".message") || loc == "message" ||
		hasSuffix(loc, ".description") || loc == "description"
}

func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func joinLoc(base, k string) string {
	if base == "" {
		return k
	}
	return base + "." + k
}

func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case float64:
		return "number"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return fmt.Sprintf("%T", v)
	}
}

func truncate(s string) string {
	const max = 240
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
