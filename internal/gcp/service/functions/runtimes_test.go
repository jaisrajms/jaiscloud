package functions

import (
	"testing"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

func newRuntimeService() *Service {
	return NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
}

func TestListRuntimesCatalog(t *testing.T) {
	s := newRuntimeService()
	rs, err := s.ListRuntimes("proj", "us-central1", "")
	if err != nil {
		t.Fatalf("ListRuntimes: %v", err)
	}
	if len(rs) == 0 {
		t.Fatal("empty runtime catalog")
	}
	var found bool
	for _, rt := range rs {
		if rt.Name == "nodejs20" {
			found = true
			if rt.Environment != "GEN_2" || rt.Stage != "GA" {
				t.Fatalf("nodejs20 = %+v, want GEN_2/GA", rt)
			}
		}
	}
	if !found {
		t.Fatal("nodejs20 missing from catalog")
	}
}

func TestListRuntimesRequiresLocation(t *testing.T) {
	s := newRuntimeService()
	if _, err := s.ListRuntimes("proj", "", ""); err == nil {
		t.Fatal("expected InvalidArgument for missing location")
	}
}

func TestListRuntimesFilter(t *testing.T) {
	s := newRuntimeService()
	all, _ := s.ListRuntimes("proj", "us-central1", "")

	// environment filter
	gen2, err := s.ListRuntimes("proj", "us-central1", `environment="GEN_2"`)
	if err != nil {
		t.Fatalf("environment filter: %v", err)
	}
	if len(gen2) != len(all) {
		t.Fatalf("environment=GEN_2 returned %d, want %d", len(gen2), len(all))
	}

	// name filter
	one, err := s.ListRuntimes("proj", "us-central1", `name="nodejs20"`)
	if err != nil {
		t.Fatalf("name filter: %v", err)
	}
	if len(one) != 1 || one[0].DisplayName != "Node.js 20" {
		t.Fatalf("name filter = %+v", one)
	}

	// two clauses joined by AND
	both, err := s.ListRuntimes("proj", "us-central1", `environment="GEN_2" AND stage="GA"`)
	if err != nil {
		t.Fatalf("AND filter: %v", err)
	}
	if len(both) == 0 {
		t.Fatal("AND filter returned nothing")
	}
	for _, rt := range both {
		if rt.Environment != "GEN_2" || rt.Stage != "GA" {
			t.Fatalf("AND filter leaked %+v", rt)
		}
	}

	// OR of two name clauses
	or, err := s.ListRuntimes("proj", "us-central1", `name="nodejs20" OR name="python311"`)
	if err != nil {
		t.Fatalf("OR filter: %v", err)
	}
	if len(or) != 2 {
		t.Fatalf("OR filter returned %d, want 2", len(or))
	}

	// NOT: the whole catalog is GEN_2, so NOT GEN_2 is empty.
	notGen, err := s.ListRuntimes("proj", "us-central1", `NOT environment="GEN_2"`)
	if err != nil {
		t.Fatalf("NOT filter: %v", err)
	}
	if len(notGen) != 0 {
		t.Fatalf("NOT filter returned %d, want 0", len(notGen))
	}

	// parentheses group an OR under an AND
	paren, err := s.ListRuntimes("proj", "us-central1", `(name="nodejs20" OR name="nodejs22") AND stage="GA"`)
	if err != nil {
		t.Fatalf("parenthesised filter: %v", err)
	}
	if len(paren) != 2 {
		t.Fatalf("parenthesised filter returned %d, want 2", len(paren))
	}

	// AIP-160 "has": name substring
	has, err := s.ListRuntimes("proj", "us-central1", `name:"nodejs"`)
	if err != nil {
		t.Fatalf("has filter: %v", err)
	}
	if len(has) != 4 {
		t.Fatalf("name:nodejs returned %d, want 4", len(has))
	}

	// lexicographic ordering: stage >= "GA" excludes the DEPRECATED runtimes
	ge, err := s.ListRuntimes("proj", "us-central1", `stage>="GA"`)
	if err != nil {
		t.Fatalf("ordering filter: %v", err)
	}
	if len(ge) == 0 || len(ge) == len(all) {
		t.Fatalf("stage>=GA returned %d, want a strict subset of %d", len(ge), len(all))
	}
	for _, rt := range ge {
		if rt.Stage == "DEPRECATED" {
			t.Fatalf("stage>=GA leaked DEPRECATED runtime %+v", rt)
		}
	}

	// AIP-160: OR binds tighter than AND. This groups as
	// name="nodejs20" AND (stage="GA" OR stage="DEPRECATED") → just nodejs20,
	// not the conventional (nodejs20 AND GA) OR DEPRECATED which would add the
	// two deprecated runtimes.
	prec, err := s.ListRuntimes("proj", "us-central1", `name="nodejs20" AND stage="GA" OR stage="DEPRECATED"`)
	if err != nil {
		t.Fatalf("precedence filter: %v", err)
	}
	if len(prec) != 1 || prec[0].Name != "nodejs20" {
		t.Fatalf("OR-tighter precedence broken: %+v", prec)
	}
	explicit, err := s.ListRuntimes("proj", "us-central1", `name="nodejs20" AND (stage="GA" OR stage="DEPRECATED")`)
	if err != nil || len(explicit) != len(prec) {
		t.Fatalf("implicit/explicit precedence mismatch: %v vs %v", prec, explicit)
	}

	// a filter that matches nothing yields an empty (non-nil) slice
	none, err := s.ListRuntimes("proj", "us-central1", `name="does-not-exist"`)
	if err != nil {
		t.Fatalf("no-match filter: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("expected no runtimes, got %d", len(none))
	}
}

func TestListRuntimesFilterErrors(t *testing.T) {
	s := newRuntimeService()
	for _, filter := range []string{
		`bogus="x"`,             // unknown field
		`name`,                  // missing operator
		`name="x" AND`,          // dangling AND
		`name=="x"`,             // double equals is not an operator
		`name="a" XOR name="b"`, // unsupported keyword
		`starts_with("node")`,   // AIP-160 function call is not modelled
		`(name="a"`,             // unbalanced parenthesis
		`name="a" OR`,           // dangling OR
	} {
		if _, err := s.ListRuntimes("proj", "us-central1", filter); err == nil {
			t.Fatalf("filter %q: expected InvalidArgument", filter)
		}
	}
}

func TestRuntimeJSON(t *testing.T) {
	rt := Runtime{
		Name:            "nodejs18",
		DisplayName:     "Node.js 18",
		Stage:           "DEPRECATED",
		Environment:     "GEN_2",
		Warnings:        []string{"deprecated"},
		DeprecationDate: map[string]any{"year": 2025, "month": 4, "day": 30},
	}
	m := RuntimeJSON(rt)
	if m["name"] != "nodejs18" || m["displayName"] != "Node.js 18" ||
		m["stage"] != "DEPRECATED" || m["environment"] != "GEN_2" {
		t.Fatalf("unexpected runtime JSON: %v", m)
	}
	if w, _ := m["warnings"].([]string); len(w) != 1 {
		t.Fatalf("warnings = %v", m["warnings"])
	}
	if d, _ := m["deprecationDate"].(map[string]any); d["year"] != 2025 {
		t.Fatalf("deprecationDate = %v", m["deprecationDate"])
	}
	if _, ok := m["decommissionDate"]; ok {
		t.Fatalf("decommissionDate should be omitted when nil")
	}
}
