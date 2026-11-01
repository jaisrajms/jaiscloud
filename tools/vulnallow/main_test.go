package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// govulncheckJSON is a trimmed sample of `govulncheck -format json` output:
// one advisory reported twice (two call traces), one advisory with no trace at
// all (a module that is required but never reached), and the OSV records —
// which govulncheck emits after the findings, so the parser cannot rely on
// seeing them first.
const govulncheckJSON = `{
  "finding": {"osv": "GO-2099-0001", "trace": [
    {"module": "github.com/example/lib", "package": "github.com/example/lib/ocf", "function": "ocf.Decode"},
    {"module": "github.com/example/lib", "package": "github.com/example/lib/ocf", "function": "ocf.Decode"}]}
}
{
  "finding": {"osv": "GO-2099-0001", "trace": [
    {"module": "github.com/example/lib", "package": "github.com/example/lib", "function": ""}]}
}
{
  "finding": {"osv": "GO-2099-0002", "trace": []}
}
{
  "osv": {"id": "GO-2099-0001", "summary": "Example decoder bug",
          "aliases": ["CVE-2099-0001", "GHSA-aaaa-bbbb-cccc"],
          "affected": [{"package": {"name": "github.com/example/lib", "ecosystem": "Go"}}]}
}
{
  "osv": {"id": "GO-2099-0002", "summary": "Stdlib example bug", "aliases": ["CVE-2099-0002"],
          "affected": [{"package": {"name": "stdlib", "ecosystem": "Go"}}]}
}
`

func TestParseFindingsCollapsesTracesPerAdvisoryAndModule(t *testing.T) {
	got, err := parseFindings(strings.NewReader(govulncheckJSON))
	if err != nil {
		t.Fatalf("parseFindings: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("findings = %d, want 2 (%+v)", len(got), got)
	}
	first := got[0]
	if first.ID != "GO-2099-0001" || first.Module != "github.com/example/lib" {
		t.Fatalf("first finding = %+v", first)
	}
	if !first.Called {
		t.Fatalf("first finding should be marked called: %+v", first)
	}
	if first.Traces != 3 {
		t.Fatalf("traces = %d, want 3", first.Traces)
	}
	if first.Summary != "Example decoder bug" {
		t.Fatalf("summary = %q (OSV records arrive after the findings)", first.Summary)
	}
	if len(first.Aliases) != 2 || !contains(first.Packages, "github.com/example/lib") || !contains(first.Packages, "github.com/example/lib/ocf") {
		t.Fatalf("aliases/packages = %v / %v", first.Aliases, first.Packages)
	}
	second := got[1]
	if second.ID != "GO-2099-0002" || second.Module != "stdlib" || second.Called {
		t.Fatalf("second finding = %+v (module comes from the OSV for trace-less findings)", second)
	}
}

func TestCheckReportsUnlistedAndStale(t *testing.T) {
	findings, err := parseFindings(strings.NewReader(govulncheckJSON))
	if err != nil {
		t.Fatalf("parseFindings: %v", err)
	}
	allow := []allowEntry{
		{ID: "GO-2099-0001", Module: "github.com/example/lib", Reason: "archived upstream", URL: "https://example.test/1"},
		{ID: "GO-2099-0009", Module: "github.com/example/gone", Reason: "archived upstream", URL: "https://example.test/9"},
	}
	unlisted, stale := check(findings, allow)
	if len(unlisted) != 1 || unlisted[0].ID != "GO-2099-0002" {
		t.Fatalf("unlisted = %+v, want the uncited stdlib advisory", unlisted)
	}
	if len(stale) != 1 || stale[0].ID != "GO-2099-0009" {
		t.Fatalf("stale = %+v, want the exemption that no longer matches", stale)
	}
}

func TestCheckCleanAllowListIsGreen(t *testing.T) {
	findings, err := parseFindings(strings.NewReader(govulncheckJSON))
	if err != nil {
		t.Fatalf("parseFindings: %v", err)
	}
	allow := []allowEntry{
		{ID: "GO-2099-0001", Module: "github.com/example/lib", Reason: "r", URL: "https://example.test/1"},
		{ID: "GO-2099-0002", Module: "stdlib", Reason: "r", URL: "https://example.test/2"},
	}
	unlisted, stale := check(findings, allow)
	if len(unlisted) != 0 || len(stale) != 0 {
		t.Fatalf("unlisted = %+v, stale = %+v, want both empty", unlisted, stale)
	}
}

func TestParseFindingsEmptyScan(t *testing.T) {
	// A clean scan emits only config/telemetry objects, never a finding.
	in := `{"config": {"scanner_name": "govulncheck"}}
{"SBOM": {"go_version": "go1.26.10"}}
`
	got, err := parseFindings(strings.NewReader(in))
	if err != nil {
		t.Fatalf("parseFindings: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("findings = %+v, want none", got)
	}
}

func TestLoadAllowListRejectsUncitedEntries(t *testing.T) {
	cases := map[string]string{
		"no id":     "exempt:\n  - module: github.com/example/lib\n    reason: r\n    url: https://example.test\n",
		"no module": "exempt:\n  - id: GO-2099-0001\n    reason: r\n    url: https://example.test\n",
		"no reason": "exempt:\n  - id: GO-2099-0001\n    module: github.com/example/lib\n    url: https://example.test\n",
		"no url":    "exempt:\n  - id: GO-2099-0001\n    module: github.com/example/lib\n    reason: r\n",
		"bad url":   "exempt:\n  - id: GO-2099-0001\n    module: github.com/example/lib\n    reason: r\n    url: example.test\n",
		"duplicate": "exempt:\n  - id: GO-2099-0001\n    module: github.com/example/lib\n    reason: r\n    url: https://example.test\n" +
			"  - id: GO-2099-0001\n    module: github.com/example/lib\n    reason: r\n    url: https://example.test\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "allow.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			if _, err := loadAllowList(path); err == nil {
				t.Fatalf("loadAllowList accepted an uncited entry")
			}
		})
	}
}

func TestLoadAllowListAcceptsCitedEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "allow.yaml")
	body := "exempt:\n  - id: GO-2099-0001\n    module: github.com/example/lib\n    reason: archived upstream\n    url: https://example.test/1\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := loadAllowList(path)
	if err != nil {
		t.Fatalf("loadAllowList: %v", err)
	}
	if len(got) != 1 || got[0].ID != "GO-2099-0001" {
		t.Fatalf("entries = %+v", got)
	}
}
