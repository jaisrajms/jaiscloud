package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestRules drives every rule against the fixtures under testdata/src. Each rule
// fires on a bad fixture (marked with // want) and stays quiet on a good one or
// on a suppressed one.
func TestRules(t *testing.T) {
	gcpPath = "jaiscloud/example"
	// Use an empty allowlist so the committed one cannot mask fixture hits.
	t.Setenv("GCPLINT_ALLOWLIST", filepath.Join(t.TempDir(), "none"))
	allowOnce = sync.Once{}
	allowData = nil

	analysistest.Run(t, analysistest.TestData(), Analyzer,
		"jaiscloud/example/clock",
		"jaiscloud/example/realnow",
		"jaiscloud/example/prefix",
		"jaiscloud/example/sentinel",
		"jaiscloud/example/assert",
		"jaiscloud/example/awsimport",
		"jaiscloud/example/suppress",
		"jaiscloud/example/suppressfile",
		"jaiscloud/example/app",
		"jaiscloud/example/registered",
	)
}

func TestIgnoreMatches(t *testing.T) {
	cases := []struct {
		text string
		rule string
		want bool
	}{
		{"//gcplint:ignore", "clock", true},
		{"//gcplint:ignore clock", "clock", true},
		{"//gcplint:ignore clock", "resource-prefix", false},
		{"//gcplint:ignore clock,resource-prefix", "resource-prefix", true},
		{"//gcplint:ignore // a reason", "clock", true},
		{"//gcplint:ignore-file unsafe-assert", "unsafe-assert", true},
		{"//gcplint:ignore-file unsafe-assert", "clock", false},
		{"a normal comment", "clock", false},
	}
	for _, c := range cases {
		if got := ignoreMatches(c.text, c.rule); got != c.want {
			t.Errorf("ignoreMatches(%q, %q) = %v, want %v", c.text, c.rule, got, c.want)
		}
	}
}

func TestAllowlistMatch(t *testing.T) {
	a := &allowlist{rulePaths: map[string][]string{
		"clock": {"internal/gcp/workflows/expr/expr.go"},
	}}
	if !a.match("/repo/internal/gcp/workflows/expr/expr.go", "clock") {
		t.Error("expected suffix match for clock")
	}
	if a.match("/repo/internal/gcp/workflows/expr/expr.go", "unsafe-assert") {
		t.Error("did not expect clock entry to cover another rule")
	}
	if a.match("/repo/other.go", "clock") {
		t.Error("did not expect unrelated path to match")
	}
}

func TestLoadAllowlistRequiresReason(t *testing.T) {
	p := filepath.Join(t.TempDir(), "allow.txt")
	content := "# header\n" +
		"internal/gcp/a.go:clock   # justified\n" +
		"internal/gcp/b.go:clock\n" + // missing reason -> ignored
		"internal/gcp/c.go\n" // missing rule -> ignored
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GCPLINT_ALLOWLIST", p)
	a := loadAllowlist()
	if !a.match("/repo/internal/gcp/a.go", "clock") {
		t.Error("expected the justified entry to load")
	}
	if a.match("/repo/internal/gcp/b.go", "clock") {
		t.Error("entry without a reason must be ignored")
	}
	if a.match("/repo/internal/gcp/c.go", "clock") {
		t.Error("entry without a rule must be ignored")
	}
}
