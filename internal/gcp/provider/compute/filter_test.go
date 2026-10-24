package compute

import (
	"errors"
	"testing"

	"jaiscloud/internal/model"
)

func TestParseFilter(t *testing.T) {
	ok := []struct {
		in    string
		path  string
		op    string
		value string
	}{
		{"name = vm-1", "name", "=", "vm-1"},
		{"name=vm-1", "name", "=", "vm-1"},
		{"name != vm-1", "name", "!=", "vm-1"},
		{"name!=vm-1", "name", "!=", "vm-1"},
		{`name = "vm 1"`, "name", "=", "vm 1"},
		{"name = 'vm-1'", "name", "=", "vm-1"},
		{"labels.env = test", "labels.env", "=", "test"},
		{"labels.env:*", "labels.env", ":", ""},
		{"labels.env : *", "labels.env", ":", ""},
	}
	for _, tc := range ok {
		f, err := parseFilter(tc.in)
		if err != nil {
			t.Fatalf("parseFilter(%q) error: %v", tc.in, err)
		}
		if f.path != tc.path || f.op != tc.op || f.value != tc.value {
			t.Errorf("parseFilter(%q) = {%q %q %q}, want {%q %q %q}",
				tc.in, f.path, f.op, f.value, tc.path, tc.op, tc.value)
		}
	}

	bad := []string{
		"",
		"name",
		"= vm-1",
		"name =",
		"name > vm-1",           // comparison operators are outside the subset
		"name eq vm-1",          // regex form is outside the subset
		"name ~ vm-1",           // unknown operator
		"(name = a) OR (b = c)", // parens/OR are outside the subset
		"labels.env:test",       // ':' only supports the key-defined form
		"labels.env:",           // ':' requires the '*' operand
		"ba d = 1",              // malformed field path
		// Compound expressions must be rejected, never absorbed into the operand.
		"name = web1 AND status = RUNNING",
		"name != web1 OR name = web2",
		`name = "a" AND name = "b"`,
		"name = a b", // two unquoted tokens
		"name = a=b", // a second operator in the operand
		"name = a:b",
	}
	for _, in := range bad {
		if _, err := parseFilter(in); err == nil {
			t.Errorf("parseFilter(%q) = nil error, want failure", in)
		}
	}

	// A quoted literal may legitimately contain spaces and punctuation.
	if f, err := parseFilter(`name = "a AND b=c"`); err != nil {
		t.Errorf("quoted literal with punctuation must parse, got %v", err)
	} else if f.value != "a AND b=c" {
		t.Errorf("quoted literal value = %q", f.value)
	}
}

func TestFilterMatch(t *testing.T) {
	m := map[string]any{
		"name":   "vm-1",
		"status": "RUNNING",
		"labels": map[string]any{"env": "test", "empty": ""},
		"sizeGb": "20",
	}
	cases := []struct {
		expr string
		want bool
	}{
		{"name = vm-1", true},
		{"name = vm-2", false},
		{"name != vm-2", true},
		{"name != vm-1", false},
		{"status = RUNNING", true},
		{"status = TERMINATED", false},
		{"labels.env = test", true},
		{"labels.env = prod", false},
		{"labels.env:*", true},
		{"labels.empty:*", true}, // key-defined means present, even when empty
		{"labels.missing:*", false},
		{"sizeGb = 20", true},
		{"missing = x", false},
		{"missing != x", true},
	}
	for _, tc := range cases {
		f, err := parseFilter(tc.expr)
		if err != nil {
			t.Fatalf("parseFilter(%q): %v", tc.expr, err)
		}
		if got := f.match(m); got != tc.want {
			t.Errorf("%q.match = %v, want %v", tc.expr, got, tc.want)
		}
	}
	// A nil expression (no filter supplied) matches everything.
	if !(*filterExpr)(nil).match(m) {
		t.Error("nil filter must match every resource")
	}
}

func TestApplyFilterMaps(t *testing.T) {
	items := []map[string]any{
		{"name": "a", "status": "RUNNING"},
		{"name": "b", "status": "TERMINATED"},
		{"name": "c", "status": "RUNNING"},
	}

	got, err := applyFilterMaps(newNR(nil), items)
	if err != nil || len(got) != 3 {
		t.Fatalf("no filter: got %d items, err %v", len(got), err)
	}

	got, err = applyFilterMaps(newNR(map[string]any{"filter": "status = RUNNING"}), items)
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(got) != 2 || got[0]["name"] != "a" || got[1]["name"] != "c" {
		t.Fatalf("filtered = %v", got)
	}

	_, err = applyFilterMaps(newNR(map[string]any{"filter": "name > a"}), items)
	if !isCode(err, "InvalidArgument") {
		t.Fatalf("unsupported filter must be InvalidArgument, got %v", err)
	}
}

func TestCompileFilterMissingIsNil(t *testing.T) {
	f, err := compileFilter(newNR(nil))
	if err != nil || f != nil {
		t.Fatalf("compileFilter(no filter) = %v, %v; want nil, nil", f, err)
	}
	var perr *model.ProviderError
	if _, err := compileFilter(newNR(map[string]any{"filter": "oops"})); err == nil {
		t.Fatal("expected an error for a malformed filter")
	} else if !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument, got %v", err)
	} else if !errors.As(err, &perr) || perr.HTTPStatus != 400 {
		t.Fatalf("expected HTTP 400, got %v", err)
	}
}
