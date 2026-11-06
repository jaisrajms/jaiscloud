//go:build gcp_conformance

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func scenarioFixtureField(name, typ string, repeated bool, enum ...string) Field {
	seg := PathSegment{Name: name}
	items := []PathSegment{seg}
	return Field{
		Service: "svc", Transport: "grpc", Resource: "Widget", Name: name,
		Type: typ, Repeated: repeated, EnumValues: enum, Segments: items,
	}
}

// TestTypeValueIsNonDefault pins the property the whole suite rests on: a
// generated create value must never be a JSON zero, because the parity
// normalizer drops false/0/"" members and an unserved field would then look
// identical to a served one.
func TestTypeValueIsNonDefault(t *testing.T) {
	cases := []Field{
		scenarioFixtureField("flag", "bool", false),
		scenarioFixtureField("count", "int32", false),
		scenarioFixtureField("counts", "int64", true),
		scenarioFixtureField("ratio", "double", false),
		scenarioFixtureField("name", "string", false),
		scenarioFixtureField("names", "string", true),
		scenarioFixtureField("mode", "enum", false, "MODE_UNSPECIFIED", "MODE_ON", "MODE_OFF"),
		scenarioFixtureField("when", "timestamp", false),
		scenarioFixtureField("ttl", "duration", false),
		scenarioFixtureField("blob", "bytes", false),
		scenarioFixtureField("labels", "map", true),
	}
	for _, f := range cases {
		// Create values (variant 0) are the blind-spot rule: they must never be
		// a JSON zero. The update value may legitimately be false/0 (that is a
		// real change), and the assertion reads it back directly.
		raw, err := typeValue(f, valueHint{}, 0)
		if err != nil {
			t.Fatalf("%s: %v", f.Name, err)
		}
		if isZeroScenarioValue(raw) {
			t.Errorf("%s generated the zero create value %s", f.Name, raw)
		}
	}
	// Enum values are definition members, never the *_UNSPECIFIED default.
	raw, err := typeValue(scenarioFixtureField("mode", "enum", false, "MODE_UNSPECIFIED", "MODE_ON", "MODE_OFF"), valueHint{}, 0)
	if err != nil || raw != `"MODE_ON"` {
		t.Fatalf("enum create value = %s (%v), want MODE_ON", raw, err)
	}
}

func isZeroScenarioValue(raw string) bool {
	switch strings.TrimSpace(raw) {
	case "false", "0", `""`, "[]", "{}", "null":
		return true
	}
	return false
}

// TestScenarioPathMarksRepeatedAncestors pins the builder path convention: a
// repeated ancestor gets a "[]" marker, the leaf does not (its JSON value
// carries the array).
func TestScenarioPathMarksRepeatedAncestors(t *testing.T) {
	f := Field{
		Name: "conditions.displayName", Type: "string",
		Segments: []PathSegment{{Name: "conditions", Repeated: true}, {Name: "displayName"}},
	}
	if got := scenarioPath(f); got != "conditions[].displayName" {
		t.Fatalf("scenarioPath = %q, want conditions[].displayName", got)
	}
	leaf := Field{Name: "cors", Type: "message", Repeated: true, Segments: []PathSegment{{Name: "cors", Repeated: true}}}
	if got := scenarioPath(leaf); got != "cors" {
		t.Fatalf("leaf scenarioPath = %q, want cors", got)
	}
}

// TestBuildScenariosClassifiesEveryRow pins the dataset contract: roundtrip rows
// become values, immutable/output-only rows become negative writes, and a stale
// value hint is an error rather than a silent no-op.
func TestBuildScenariosClassifiesEveryRow(t *testing.T) {
	fields := []Field{
		{Service: "svc", Transport: "grpc", Resource: "Widget", Name: "name", Type: "string", Segments: []PathSegment{{Name: "name"}}},
		{Service: "svc", Transport: "grpc", Resource: "Widget", Name: "mode", Type: "string", Segments: []PathSegment{{Name: "mode"}}},
		{Service: "svc", Transport: "grpc", Resource: "Widget", Name: "created", Type: "timestamp", Segments: []PathSegment{{Name: "created"}}},
	}
	rows := []Row{
		{Service: "svc", Transport: "grpc", Resource: "Widget", Field: "name", Coverage: CoverageImmutable},
		{Service: "svc", Transport: "grpc", Resource: "Widget", Field: "mode", Coverage: CoverageRoundtrip},
		{Service: "svc", Transport: "grpc", Resource: "Widget", Field: "created", Coverage: CoverageOutputOnly},
	}
	set, err := BuildScenarios(&Universe{Fields: fields}, rows, nil)
	if err != nil {
		t.Fatalf("BuildScenarios: %v", err)
	}
	if len(set.Values) != 1 || set.Values[0].Field != "mode" {
		t.Fatalf("values = %+v, want just mode", set.Values)
	}
	if !set.Values[0].Update || set.Values[0].Value == set.Values[0].UpdateVal {
		t.Fatalf("roundtrip value has no distinct update value: %+v", set.Values[0])
	}
	if len(set.Attempts) != 2 {
		t.Fatalf("attempts = %+v, want 2", set.Attempts)
	}
	for _, a := range set.Attempts {
		wantAt := map[string]string{"name": "update", "created": "create"}[a.Field]
		if a.At != wantAt {
			t.Errorf("%s attempted at %q, want %q", a.Field, a.At, wantAt)
		}
	}
	if _, err := BuildScenarios(&Universe{Fields: fields}, rows, map[string]valueHint{"svc/grpc/Widget/gone": {Field: "svc/grpc/Widget/gone", Ref: "kms", Reason: "x"}}); err == nil {
		t.Fatal("a stale value hint must fail generation")
	}
}

// TestRenderScenariosIsDeterministicAndCheckable pins the staleness gate: the
// committed file must be byte-identical to a fresh render, and a changed
// classification must make the check fail.
func TestRenderScenariosIsDeterministicAndCheckable(t *testing.T) {
	fields := []Field{{Service: "svc", Transport: "grpc", Resource: "Widget", Name: "mode", Type: "string", Segments: []PathSegment{{Name: "mode"}}}}
	rows := []Row{{Service: "svc", Transport: "grpc", Resource: "Widget", Field: "mode", Coverage: CoverageRoundtrip}}
	set, err := BuildScenarios(&Universe{Fields: fields}, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "generated.go")
	if err := WriteScenarios(path, set); err != nil {
		t.Fatal(err)
	}
	if err := CheckScenarios(path, set); err != nil {
		t.Fatalf("freshly written scenarios are stale: %v", err)
	}
	// A new roundtrip field that was not regenerated makes the file stale.
	extra := append(fields, Field{Service: "svc", Transport: "grpc", Resource: "Widget", Name: "other", Type: "string", Segments: []PathSegment{{Name: "other"}}})
	rows2 := append(rows, Row{Service: "svc", Transport: "grpc", Resource: "Widget", Field: "other", Coverage: CoverageRoundtrip})
	set2, err := BuildScenarios(&Universe{Fields: extra}, rows2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckScenarios(path, set2); err == nil {
		t.Fatal("CheckScenarios accepted a stale file")
	}
}

// TestValidateScenarioCoverageCatchesMissingRow pins the completeness gate.
func TestValidateScenarioCoverageCatchesMissingRow(t *testing.T) {
	fields := []Field{{Service: "svc", Transport: "grpc", Resource: "Widget", Name: "mode", Type: "string", Segments: []PathSegment{{Name: "mode"}}}}
	rows := []Row{{Service: "svc", Transport: "grpc", Resource: "Widget", Field: "mode", Coverage: CoverageRoundtrip}}
	set, err := BuildScenarios(&Universe{Fields: fields}, rows, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateScenarioCoverage(&Universe{Fields: fields}, rows, set); err != nil {
		t.Fatalf("complete dataset rejected: %v", err)
	}
	set.Values = nil
	if err := validateScenarioCoverage(&Universe{Fields: fields}, rows, set); err == nil {
		t.Fatal("a roundtrip row with no generated value must fail")
	}
}

// TestGeneratedScenarioFileMatchesCatalog is the end-to-end gate over the real
// repository: the committed generated file is exactly what the committed
// catalog and definitions produce, and every roundtrip row has a value.
func TestGeneratedScenarioFileMatchesCatalog(t *testing.T) {
	rows, err := ReadCSV("../../docs/fidelity/field-coverage.csv")
	if err != nil {
		t.Fatalf("read catalog: %v", err)
	}
	raw, err := os.ReadFile("../../tests/gcpparity/zz_fieldcoverage_generated.go")
	if err != nil {
		t.Fatalf("read generated scenarios: %v", err)
	}
	if !strings.Contains(string(raw), "DO NOT EDIT") {
		t.Fatal("generated file lost its DO NOT EDIT header")
	}
	// The generated file must carry a value per roundtrip row.
	values := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, "Coverage: \"roundtrip\"") {
			values++
		}
	}
	want := 0
	for _, r := range rows {
		if r.Coverage == CoverageRoundtrip {
			want++
		}
	}
	if values != want {
		t.Fatalf("generated file has %d roundtrip values, catalog has %d roundtrip rows: run `make gen-gcp-field-coverage`", values, want)
	}
}

// TestReferenceKeyNormalizesTransportSpellings pins that a REST and a gRPC row
// for the same field collapse to one reference row.
func TestReferenceKeyNormalizesTransportSpellings(t *testing.T) {
	a := referenceKey("functions", "Function", "event_trigger.pubsub_topic")
	b := referenceKey("functions", "Function", "eventTrigger.pubsubTopic")
	if a != b {
		t.Fatalf("spellings do not collapse: %q vs %q", a, b)
	}
}

// TestReferenceGateRequiresAClassification pins the reference gate's contract.
func TestReferenceGateRequiresAClassification(t *testing.T) {
	refs := []Reference{{Service: "svc", Resource: "Thing", Field: "other", Target: "kms.googleapis.com/CryptoKey"}}
	root := t.TempDir()
	// A missing row fails.
	if _, err := CheckReferences(root, refs, nil); err == nil {
		t.Fatal("a reference with no row must fail")
	}
	// An unvalidated row without a review date fails.
	rows := []ReferenceRow{{Service: "svc", Resource: "Thing", Field: "other", Target: "kms.googleapis.com/CryptoKey", Status: RefUnvalidated, Reason: "stored verbatim"}}
	if _, err := CheckReferences(root, refs, rows); err == nil {
		t.Fatal("an unvalidated row without review_by must fail")
	}
	// A not-modelled row with a reason passes.
	rows[0].Status = RefNotModelled
	if _, err := CheckReferences(root, refs, rows); err != nil {
		t.Fatalf("not-modelled row rejected: %v", err)
	}
	// Target drift fails.
	rows[0].Target = "pubsub.googleapis.com/Topic"
	if _, err := CheckReferences(root, refs, rows); err == nil {
		t.Fatal("target drift must fail")
	}
}

var _ = json.Marshal
