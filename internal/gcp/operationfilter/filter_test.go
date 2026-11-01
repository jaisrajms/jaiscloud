package operationfilter

import "testing"

func records() []Record {
	return []Record{
		{Name: "projects/p/locations/us/operations/op1", Done: true, Fields: map[string]string{"verb": "create", "target": "projects/p/workflows/a", "state": "DONE"}},
		{Name: "projects/p/locations/us/operations/op2", Done: false, Fields: map[string]string{"verb": "update", "target": "projects/p/workflows/b", "state": "RUNNING"}},
		{Name: "projects/p/locations/us/operations/op3", Done: true, Fields: map[string]string{"verb": "delete", "target": "projects/p/workflows/a", "state": "DONE"}},
	}
}

func matchCount(t *testing.T, expr string) int {
	t.Helper()
	f, err := Compile(expr, "verb", "target", "state")
	if err != nil {
		t.Fatalf("Compile(%q): %v", expr, err)
	}
	n := 0
	for _, r := range records() {
		if f.Match(r) {
			n++
		}
	}
	return n
}

func TestCompileMatch(t *testing.T) {
	cases := []struct {
		expr string
		want int
	}{
		{"", 3},
		{"done=true", 2},
		{"done=false", 1},
		{"NOT done=true", 1},
		{"-done=true", 1},
		{"done=true AND verb=create", 1},
		{"done=true OR done=false", 3},
		{"done=true AND verb=create OR done=false", 1}, // OR binds tighter than AND
		{"(done=false OR done=true) AND verb=delete", 1},
		{"done=true verb=create", 1}, // implicit AND
		{"name:\"operations/op1\"", 1},
		{"state=RUNNING", 1},
		{"verb:*", 3},
		{"verb!=*", 0},
		{"target:\"workflows/a\"", 2},
		{"name<\"projects/p/locations/us/operations/op2\"", 1},
	}
	for _, tc := range cases {
		if got := matchCount(t, tc.expr); got != tc.want {
			t.Errorf("filter %q matched %d, want %d", tc.expr, got, tc.want)
		}
	}
}

func TestCompileErrors(t *testing.T) {
	for _, expr := range []string{
		"bogus=1",
		"metadata.state=DONE", // not exposed by this caller
		"done=maybe",
		"done<true",
		"done=*",
		"name",
		"name=",
		"=x",
		"done=true AND",
		"done=true OR",
		"(done=true",
		"done=true)",
		"name=\"unterminated",
		"done=true name!=",
	} {
		if _, err := Compile(expr, "verb", "target", "state"); err == nil {
			t.Errorf("Compile(%q) succeeded, want an error", expr)
		}
	}
}
