package aip160

import (
	"testing"
	"time"
)

// TestCompileRejectBranches drives every rejection branch of the generic
// compiler so a malformed clause is always a descriptive error.
func TestCompileRejectBranches(t *testing.T) {
	cases := []string{
		`= 1`,                     // field is not an identifier
		`labels."quoted"=x`,       // operator missing after a quoted key
		`name`,                    // no operator/value
		`name=`,                   // no value
		`name!=*`,                 // "*" is not allowed with "!="
		`state<ENABLED`,           // comparison on a string field
		`labels.team>alpha`,       // comparison on a label
		`labels=alpha`,            // bare labels takes only "*"
		`labels>team`,             // bare labels takes only ":"/"="
		`labels.=x`,               // empty label key
		`labels.team=`,            // label value missing
		`createTime!=*`,           // "*" is not allowed with "!=" on a time
		`createTime~"2026-01-01"`, // "~" is not a token
		`createTime=`,             // time value missing
		`name="a" =`,              // unexpected token between clauses
	}
	for _, expr := range cases {
		if _, err := Compile("test.list", expr, filterFields()); err == nil {
			t.Errorf("Compile(%q) succeeded, want an error", expr)
		}
	}
}

// TestCompilePresentStarMatchesValueFields exercises the `field:*` presence
// forms that return a predicate rather than an error.
func TestCompilePresentStarMatchesValueFields(t *testing.T) {
	full := filterRec{
		Name:    "projects/p/secrets/s",
		Labels:  map[string]string{"team": "alpha"},
		Created: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	empty := filterRec{}
	for _, tc := range []struct {
		expr string
		rec  filterRec
		want bool
	}{
		{`state:*`, full, false}, // state unset
		{`name:*`, full, true},   // name set
		{`name:*`, empty, false}, // name empty
		{`labels.team:*`, full, true},
		{`createTime:*`, full, true},
		{`createTime:*`, empty, false},
		{`createTime<="2026-01-03"`, full, true},
		{`createTime!="2025-01-01"`, full, true},
		{`createTime="2026-01-02"`, full, true},
		{`labels.team!=*`, full, false},
		{`labels.missing!=*`, full, true},
		{`labels.missing:nope`, full, false}, // a missing key contains nothing
	} {
		if got := mustCompile(t, tc.expr).Match(tc.rec); got != tc.want {
			t.Errorf("Match(%q) on %+v = %v, want %v", tc.expr, tc.rec, got, tc.want)
		}
	}
}

// TestCompileDefaultsDesc covers the fallback error text when a surface names no
// field set.
func TestCompileDefaultsDesc(t *testing.T) {
	if _, err := Compile("test.list", `bogus=1`, Fields[filterRec]{}); err == nil {
		t.Fatal("expected an error for a fieldless surface")
	}
}

// TestOrderSpecTieAndDescSort covers the exported tie accessor, a descending
// multi-key sort and a composite cursor.
func TestOrderSpecTieAndDescSort(t *testing.T) {
	spec := NewOrderSpec(
		func(r filterRec) string { return r.Name },
		OrderField[filterRec]{Name: "name", Value: func(r filterRec) string { return r.Name }},
		OrderField[filterRec]{Name: "state", Value: func(r filterRec) string { return r.State }},
	)
	if got := spec.Tie(filterRec{Name: "b"}); got != "b" {
		t.Fatalf("Tie = %q, want b", got)
	}

	recs := []filterRec{{Name: "a"}, {Name: "c"}, {Name: "b"}}
	order, err := spec.Parse("name desc")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	order.Sort(recs)
	if recs[0].Name != "c" || recs[1].Name != "b" || recs[2].Name != "a" {
		t.Fatalf("descending sort = %v, want c,b,a", recs)
	}

	// A composite cursor resumes after the item it was minted from, and rejects
	// a malformed payload.
	cur := order.Cursor(recs[0])
	if order.AfterCursor(recs[0], cur) {
		t.Error("the item a cursor was minted from must not sort after itself")
	}
	if !order.AfterCursor(recs[1], cur) {
		t.Error("a smaller item must sort after a descending cursor")
	}
	if !order.AfterCursor(recs[0], "not-base64!!") {
		t.Error("a malformed cursor payload must restart the scan (after every item)")
	}
	if !order.AfterCursor(recs[0], "a.a") {
		t.Error("an undecodable cursor part must restart the scan (after every item)")
	}

	// Equal primary keys fall through to the tiebreak key.
	constant := NewOrderSpec(
		func(r filterRec) string { return r.Name },
		OrderField[filterRec]{Name: "name", Value: func(filterRec) string { return "" }},
	)
	tiedOrder, err := constant.Parse("name")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tied := []filterRec{{Name: "b"}, {Name: "a"}}
	tiedOrder.Sort(tied)
	if tied[0].Name != "a" || tied[1].Name != "b" {
		t.Fatalf("tiebreak sort = %v, want a,b", tied)
	}
}

// TestLabelKeyOf covers the label-key extraction directly, including the
// non-label and dot-less forms the compiler never reaches.
func TestLabelKeyOf(t *testing.T) {
	cases := map[string]string{
		"labels.team":   "team",
		"Labels.team":   "team",
		"labels.my_key": "my_key",
		"labels.":       "",
		"nolabels":      "",
		"other.example": "",
	}
	for in, want := range cases {
		if got := labelKeyOf(in); got != want {
			t.Errorf("labelKeyOf(%q) = %q, want %q", in, got, want)
		}
	}
}
