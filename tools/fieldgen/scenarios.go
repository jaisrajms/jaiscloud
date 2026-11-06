//go:build gcp_conformance

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Scenario generation (FC3).
//
// field-coverage.csv classifies every definition field, but a classification is
// only a claim until a test exercises it. This file derives, from the same
// universe and the same CSV, the data the generated round-trip suite
// (tests/gcpparity/fieldcoverage*.go) drives:
//
//   - one value per `roundtrip` field, chosen from the definition's wire type so
//     it is a non-default value (a `false`, `0`, `""` or `[]` would be invisible
//     to the parity normalizer, which drops zero members);
//   - a second value per writable scalar field, for the one-step-per-field
//     update;
//   - one write-attempt value per `output-only` (attempted at create) and
//     `immutable` (attempted at update) field, so the negative test has
//     something to write.
//
// A field whose type does not determine a usable value (a bucket location must
// be a real region, a CIDR range must parse, a KMS key must exist) is pinned in
// scenario-values.yaml with a reason. A new definition field with no entry
// still gets a type-driven value, so it cannot enter the catalog without a
// scenario: tests/gcpparity then fails because no driver places it.

// ScenarioValue is one generated value the round-trip suite must assert.
type ScenarioValue struct {
	Service   string
	Resource  string
	Transport string
	Field     string // the CSV field path
	Path      string // the same path with `[]` marking a repeated ancestor
	Kind      string // type token from the definition
	Repeated  bool
	Value     string // JSON literal to create/serve; empty for a message or a reference
	Ref       string // runtime-resolved reference token (kms, pubsub-topic, ...)
	Update    bool   // a per-field update step exists
	UpdateVal string // JSON literal for the update step
	Behavior  string
	Coverage  string
}

// ScenarioWriteAttempt is one negative write the suite performs: a value the
// API declares server-owned (attempted at create) or create-only (attempted at
// update). The suite accepts either an ignored value or INVALID_ARGUMENT.
type ScenarioWriteAttempt struct {
	Service   string
	Resource  string
	Transport string
	Field     string
	Path      string
	Kind      string
	At        string // "create" for output-only, "update" for immutable
	Value     string // JSON literal
	Coverage  string
}

// ScenarioSet is the whole generated dataset.
type ScenarioSet struct {
	Values   []ScenarioValue
	Attempts []ScenarioWriteAttempt
}

// valueHint pins a value the definition's type does not determine.
type valueHint struct {
	Field       string `yaml:"field"`
	Value       string `yaml:"value"`        // JSON literal
	UpdateValue string `yaml:"update_value"` // distinct JSON literal for the update step
	Ref         string `yaml:"ref"`          // runtime-resolved reference, mutually exclusive with value
	Reason      string `yaml:"reason"`
}

type hintFile struct {
	Values []valueHint `yaml:"values"`
}

// LoadValueHints reads scenario-values.yaml.
func LoadValueHints(path string) (map[string]valueHint, error) {
	out := map[string]valueHint{}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, fmt.Errorf("read value hints (%s): %w", path, err)
	}
	var f hintFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse value hints (%s): %w", path, err)
	}
	for i, h := range f.Values {
		if h.Field == "" {
			return nil, fmt.Errorf("%s: hint %d names no field", path, i+1)
		}
		if h.Reason == "" {
			return nil, fmt.Errorf("%s: hint %s has no reason (say why the type-driven value is wrong)", path, h.Field)
		}
		if (h.Value == "") == (h.Ref == "") {
			return nil, fmt.Errorf("%s: hint %s must set exactly one of value or ref", path, h.Field)
		}
		for name, literal := range map[string]string{"value": h.Value, "update_value": h.UpdateValue} {
			if literal == "" {
				continue
			}
			var v any
			if err := json.Unmarshal([]byte(literal), &v); err != nil {
				return nil, fmt.Errorf("%s: hint %s %s %q is not a JSON literal: %w", path, h.Field, name, literal, err)
			}
		}
		if _, dup := out[h.Field]; dup {
			return nil, fmt.Errorf("%s: duplicate hint for %s", path, h.Field)
		}
		out[h.Field] = h
	}
	return out, nil
}

// BuildScenarios joins the definition universe, the committed classifications
// and the value hints into the generated dataset.
func BuildScenarios(universe *Universe, rows []Row, hints map[string]valueHint) (*ScenarioSet, error) {
	byKey := make(map[string]Field, len(universe.Fields))
	for _, f := range universe.Fields {
		byKey[f.Key()] = f
	}
	classified := make(map[string]Row, len(rows))
	for _, r := range rows {
		classified[r.Key()] = r
	}

	set := &ScenarioSet{}
	usedHints := map[string]bool{}
	for _, f := range universe.Fields {
		row, ok := classified[f.Key()]
		if !ok || row.Coverage == "" {
			// An unclassified row is already a hard error for -check; it
			// contributes no scenario until it is classified.
			continue
		}
		hint := hints[f.Key()]
		if hint.Field != "" {
			usedHints[f.Key()] = true
		}
		switch row.Coverage {
		case CoverageRoundtrip:
			v, err := scenarioValue(f, row, hint)
			if err != nil {
				return nil, err
			}
			set.Values = append(set.Values, v)
		case CoverageOutputOnly:
			att, err := scenarioAttempt(f, row, hint, "create")
			if err != nil {
				return nil, err
			}
			set.Attempts = append(set.Attempts, att)
		case CoverageImmutable:
			att, err := scenarioAttempt(f, row, hint, "update")
			if err != nil {
				return nil, err
			}
			set.Attempts = append(set.Attempts, att)
		}
	}
	for key := range hints {
		if !usedHints[key] {
			return nil, fmt.Errorf("value hint %s is stale: no classified roundtrip/immutable/output-only definition field has that key", key)
		}
	}
	sort.Slice(set.Values, func(i, j int) bool { return valueKey(set.Values[i]) < valueKey(set.Values[j]) })
	sort.Slice(set.Attempts, func(i, j int) bool { return attemptKey(set.Attempts[i]) < attemptKey(set.Attempts[j]) })
	return set, nil
}

func valueKey(v ScenarioValue) string {
	return strings.Join([]string{v.Service, v.Transport, v.Resource, v.Field}, "/")
}

func attemptKey(a ScenarioWriteAttempt) string {
	return strings.Join([]string{a.Service, a.Transport, a.Resource, a.At, a.Field}, "/")
}

// scenarioValue derives the create value (and, for a writable scalar, the
// update value) for one roundtrip row.
func scenarioValue(f Field, row Row, hint valueHint) (ScenarioValue, error) {
	v := ScenarioValue{
		Service:   f.Service,
		Resource:  f.Resource,
		Transport: f.Transport,
		Field:     f.Name,
		Path:      scenarioPath(f),
		Kind:      f.Type,
		Repeated:  f.Repeated,
		Behavior:  row.Behavior,
		Coverage:  row.Coverage,
	}
	if f.Type == "message" {
		// A message row is a presence assertion: its children carry the
		// values. The driver marks it used when it places a child (or when it
		// supplies the required shape for a message the catalog stops at).
		return v, nil
	}
	ref := hint.Ref
	if ref == "" && hint.Value == "" {
		ref = referenceToken(row.Behavior)
	}
	if ref != "" {
		// The value must name a resource that exists in the referenced
		// store, so it is resolved at run time (tests/gcpparity's prelude)
		// instead of being frozen into the generated file.
		if !knownReference(ref) {
			return v, fmt.Errorf("%s: reference %q has no scenario resolver (add one to tests/gcpparity/fieldcoverage.go)", f.Key(), ref)
		}
		v.Ref = ref
		return v, nil
	}
	raw, err := typeValue(f, hint, 0)
	if err != nil {
		return v, err
	}
	v.Value = raw
	if writableScalar(f) {
		upd, err := typeValue(f, hint, 1)
		if err != nil {
			return v, err
		}
		if upd == raw {
			// The field's domain admits exactly one valid value here (a hint
			// pinned it), so a masked update could not prove a change. The
			// round-trip assertion still covers the field; no update step is
			// emitted.
			return v, nil
		}
		v.Update = true
		v.UpdateVal = upd
	}
	return v, nil
}

// scenarioAttempt derives the negative write for an output-only (create) or
// immutable (update) row.
func scenarioAttempt(f Field, row Row, hint valueHint, at string) (ScenarioWriteAttempt, error) {
	a := ScenarioWriteAttempt{
		Service:   f.Service,
		Resource:  f.Resource,
		Transport: f.Transport,
		Field:     f.Name,
		Path:      scenarioPath(f),
		Kind:      f.Type,
		At:        at,
		Coverage:  row.Coverage,
	}
	if f.Type == "message" {
		// A whole server-owned message (creation_record) has no scalar to
		// write; its children carry the attempt.
		return a, nil
	}
	raw, err := typeValue(f, hint, 1)
	if err != nil {
		return a, err
	}
	a.Value = raw
	return a, nil
}

// scenarioPath renders the definition path with `[]` after every repeated
// ancestor, so the runtime builder knows where an array element starts. The
// leaf's own repetition is expressed by its (array or object) JSON value.
func scenarioPath(f Field) string {
	var out []string
	for i, s := range f.Segments {
		name := s.Name
		if s.Repeated && i < len(f.Segments)-1 {
			name += "[]"
		}
		out = append(out, name)
	}
	return strings.Join(out, ".")
}

// writableScalar reports whether a per-field update step makes sense for the
// field: a scalar the definition does not declare server-owned or create-only.
func writableScalar(f Field) bool {
	if f.Type == "message" || f.Type == "any" || f.Type == "struct" {
		return false
	}
	for _, token := range strings.Split(f.Behavior, "|") {
		switch token {
		case "output_only", "input_only", "immutable", "read_only":
			return false
		}
	}
	return true
}

// typeValue returns a non-default JSON literal for a definition field. variant 0
// is the create value, variant 1 a distinct second value for the update/negative
// step.
func typeValue(f Field, hint valueHint, variant int) (string, error) {
	if variant == 1 && hint.UpdateValue != "" {
		return hint.UpdateValue, nil
	}
	if hint.Value != "" {
		return hint.Value, nil
	}
	leaf := f.Segments[len(f.Segments)-1].Name
	switch f.Type {
	case "bool":
		if variant == 0 {
			return "true", nil
		}
		return "false", nil
	case "int32", "int64", "uint32", "uint64":
		if f.Repeated {
			if variant == 0 {
				return "[47]", nil
			}
			return "[11]", nil
		}
		if variant == 0 {
			return "47", nil
		}
		return "11", nil
	case "float", "double":
		if f.Repeated {
			if variant == 0 {
				return "[3.5]", nil
			}
			return "[7.5]", nil
		}
		if variant == 0 {
			return "3.5", nil
		}
		return "7.5", nil
	case "enum":
		name := pickEnum(f.EnumValues, variant)
		if name == "" {
			return "", fmt.Errorf("%s: the definition declares an enum with no values", f.Key())
		}
		lit, _ := json.Marshal(name)
		if f.Repeated {
			return "[" + string(lit) + "]", nil
		}
		return string(lit), nil
	case "timestamp":
		if variant == 0 {
			return `"2030-01-01T00:00:00Z"`, nil
		}
		return `"2031-02-03T04:05:06Z"`, nil
	case "duration":
		if f.Repeated {
			if variant == 0 {
				return `["3600s"]`, nil
			}
			return `["7200s"]`, nil
		}
		if variant == 0 {
			return `"3600s"`, nil
		}
		return `"7200s"`, nil
	case "bytes":
		if variant == 0 {
			return `"ZmMtYnl0ZXM="`, nil
		}
		return `"ZmMtYnl0ZXMtMg=="`, nil
	case "map":
		if variant == 0 {
			return `{"fc-key":"fc-value"}`, nil
		}
		return `{"fc-key":"fc-value-2"}`, nil
	case "string":
		slug := slugify(leaf)
		if f.Repeated {
			if variant == 0 {
				return `["fc-` + slug + `"]`, nil
			}
			return `["fc2-` + slug + `"]`, nil
		}
		if variant == 0 {
			return `"fc-` + slug + `"`, nil
		}
		return `"fc2-` + slug + `"`, nil
	}
	return "", fmt.Errorf("%s: no scenario value rule for type %q", f.Key(), f.Type)
}

// referenceToken extracts the target type's resolver token from a definition
// behavior token list (ref:<type>).
func referenceToken(behavior string) string {
	for _, token := range strings.Split(behavior, "|") {
		if strings.HasPrefix(token, "ref:") {
			return refTokenFor(strings.TrimPrefix(token, "ref:"))
		}
	}
	return ""
}

// refTokenFor maps a google.api.resource_reference target type onto the resolver
// token the parity suite implements. An empty result means the type has no
// resolver and a roundtrip field referencing it must be pinned in
// scenario-values.yaml instead.
func refTokenFor(target string) string {
	switch target {
	case "cloudkms.googleapis.com/CryptoKey":
		return "kms"
	case "pubsub.googleapis.com/Topic":
		return "pubsub-topic"
	case "storage.googleapis.com/Bucket":
		return "gcs-bucket"
	}
	return ""
}

// knownReference reports whether the parity suite resolves this reference token.
func knownReference(ref string) bool {
	switch ref {
	case "kms", "pubsub-topic", "gcs-bucket":
		return true
	}
	return false
}

// pickEnum chooses a non-default enum member: index 1 for the create value,
// index 2 (or 0 when the enum is short) for the second value.
func pickEnum(values []string, variant int) string {
	if len(values) == 0 {
		return ""
	}
	if variant == 0 {
		if len(values) > 1 {
			return values[1]
		}
		return values[0]
	}
	if len(values) > 2 {
		return values[2]
	}
	return values[0]
}

func slugify(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}
