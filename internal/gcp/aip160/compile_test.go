package aip160

import (
	"testing"
	"time"
)

// filterRec is a stand-in list item for the generic Compile tests.
type filterRec struct {
	Name    string
	State   string
	Labels  map[string]string
	Created time.Time
}

func filterFields() Fields[filterRec] {
	return Fields[filterRec]{
		Desc:   "name, state, labels.<key>, labels and createTime",
		Name:   func(r filterRec) string { return r.Name },
		Labels: func(r filterRec) map[string]string { return r.Labels },
		Strings: map[string]func(filterRec) string{
			"state": func(r filterRec) string { return r.State },
		},
		Times: map[string]func(filterRec) time.Time{
			"createtime": func(r filterRec) time.Time { return r.Created },
		},
	}
}

func mustCompile(t *testing.T, expr string) Filter[filterRec] {
	t.Helper()
	f, err := Compile("test.list", expr, filterFields())
	if err != nil {
		t.Fatalf("Compile(%q): %v", expr, err)
	}
	return f
}

func TestCompileMatch(t *testing.T) {
	day := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	rec := filterRec{
		Name:    "projects/p/locations/global/keyRings/cloud-ring",
		State:   "ENABLED",
		Labels:  map[string]string{"team": "alpha", "env": "prod"},
		Created: day,
	}
	cases := []struct {
		expr string
		want bool
	}{
		{``, true},
		{`name="projects/p/locations/global/keyRings/cloud-ring"`, true},
		{`name="other"`, false},
		{`name:"cloud"`, true},        // substring
		{`name:"CLOUD"`, true},        // substring is case-insensitive
		{`name!="other"`, true},       //
		{`state=ENABLED`, true},       // bare identifier value
		{`state="DISABLED"`, false},   //
		{`labels.team=alpha`, true},   // label equality
		{`labels.env=dev`, false},     //
		{`labels.team:*`, true},       // label present
		{`labels.missing:*`, false},   //
		{`labels.env != *`, false},    // label present, so "!= *" is false
		{`labels.missing != *`, true}, //
		{`labels:*`, true},            // any label exists
		{`name:cloud AND state=ENABLED`, true},
		{`name:cloud state=ENABLED`, true}, // implicit AND
		{`name:cloud AND state=DISABLED`, false},
		{`createTime>"2026-01-01"`, true},
		{`createTime<"2026-01-01"`, false},
		{`createTime>="2026-01-02T03:04:05Z"`, true},
		{`createTime:*`, true},
	}
	for _, tc := range cases {
		got := mustCompile(t, tc.expr).Match(rec)
		if got != tc.want {
			t.Errorf("Match(%q) = %v, want %v", tc.expr, got, tc.want)
		}
	}
}

func TestCompileEmptyMatchesAll(t *testing.T) {
	var zero filterRec
	for _, expr := range []string{"", "   ", "\t"} {
		if !mustCompile(t, expr).Match(zero) {
			t.Errorf("empty filter %q must match every item", expr)
		}
	}
}

func TestCompileRejects(t *testing.T) {
	cases := []string{
		`name="a" OR name="b"`,    // OR unsupported
		`NOT name="a"`,            // NOT unsupported
		`name="a" AND`,            // dangling AND
		`unknown=1`,               // unknown field
		`state>ENABLED`,           // comparison on a string field
		`createTime:"2026-01-01"`, // containment on a time field
		`labels.team`,             // missing operator
		`name=`,                   // missing value
		`name`,                    // missing operator/value
		`createTime="not-a-time"`, // unparseable time
		`labels.team<alpha`,       // comparison on a label
		`"unterminated`,           // lexical error
	}
	for _, expr := range cases {
		if _, err := Compile("test.list", expr, filterFields()); err == nil {
			t.Errorf("Compile(%q) succeeded, want an error", expr)
		}
	}
}

func TestCompileRejectsNameWhenUnsupported(t *testing.T) {
	f := Fields[filterRec]{
		Desc: "state",
		Strings: map[string]func(filterRec) string{
			"state": func(r filterRec) string { return r.State },
		},
	}
	if _, err := Compile("test.list", `name="x"`, f); err == nil {
		t.Error("Compile accepted a name clause on a surface with no name field")
	}
}
