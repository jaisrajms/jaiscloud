// vulnallow turns `govulncheck` output into a gate with cited exceptions.
//
// govulncheck is the authoritative Go advisory scanner, but it has no ignore
// mechanism: every finding fails the scan. That is the right default, and it is
// also why an advisory that cannot be fixed from inside this repository (an
// archived upstream module, or a fix that would change wire behaviour) would
// otherwise make the security gate permanently red — and an always-red gate is
// no gate at all.
//
// vulnallow reads the JSON stream produced by
//
//	govulncheck -format json ./...
//
// and compares every reported (advisory id, module) pair with a committed
// allowlist. The allowlist is not a mute button:
//
//   - an exception carries the advisory id, the affected module, the upstream
//     issue/advisory URL, and the reason it cannot be fixed here;
//   - it must also be recorded in the owning session row of the wave plan
//     (plan_docs/gcp-remaining-wave-plan.md, SEC1) so the exception is visible
//     where the work is tracked;
//   - a finding with no exception fails the scan, and an exception that no
//     longer matches a finding fails it too, so a fixed advisory forces the
//     stale line out of the file instead of leaving a hole nobody notices.
//
// Usage:
//
//	govulncheck -format json ./... > govulncheck.json
//	go run ./tools/vulnallow -allowlist docs/security/govulncheck-allowlist.yaml < govulncheck.json
//
// Exit code 1 when an unlisted finding or a stale exception is found, 0
// otherwise. -list prints the findings and the allowlist resolution without
// gating.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// finding is one (advisory, module) pair govulncheck reported. govulncheck
// emits one finding object per call trace, so the pairs are collapsed here.
type finding struct {
	ID       string   `json:"id"`
	Module   string   `json:"module"`
	Aliases  []string `json:"aliases"`
	Summary  string   `json:"summary"`
	Called   bool     `json:"called"` // false = the module is required but never reached
	Traces   int      `json:"-"`
	Packages []string `json:"packages,omitempty"`
}

// allowEntry is one cited exception.
type allowEntry struct {
	ID     string `yaml:"id"`
	Module string `yaml:"module"`
	Reason string `yaml:"reason"`
	URL    string `yaml:"url"`
}

// allowList is the file shape: entries plus an optional preamble comment.
type allowList struct {
	Exempt []allowEntry `yaml:"exempt"`
}

// govulnFinding is the subset of govulncheck's JSON finding object this tool
// reads. govulncheck emits a stream of objects; a finding carries only the OSV
// id and the call traces — the affected module and package come from the first
// trace element.
type govulnFinding struct {
	OSV   string  `json:"osv"`
	Trace []trace `json:"trace"`
}

type trace struct {
	Module   string `json:"module"`
	Package  string `json:"package"`
	Function string `json:"function"`
}

type govulnEntry struct {
	Finding *govulnFinding `json:"finding"`
	OSV     *osvEntry      `json:"osv"`
}

type osvEntry struct {
	ID       string      `json:"id"`
	Summary  string      `json:"summary"`
	Aliases  []string    `json:"aliases"`
	Affected []osvAffect `json:"affected"`
}

type osvAffect struct {
	Package struct {
		Name      string `json:"name"`
		Ecosystem string `json:"ecosystem"`
	} `json:"package"`
}

// module returns the OSV's first Go package name, used as the module fallback
// for findings that carry no call trace.
func (o *osvEntry) module() string {
	for _, a := range o.Affected {
		if a.Package.Name != "" {
			return a.Package.Name
		}
	}
	return ""
}

// parseFindings decodes govulncheck's JSON stream into one finding per
// (advisory, module) pair, deduplicated and sorted by id then module.
func parseFindings(r io.Reader) ([]finding, error) {
	dec := json.NewDecoder(r)
	seen := map[string]*finding{}
	osvInfo := map[string]*osvEntry{}
	for {
		var entry govulnEntry
		if err := dec.Decode(&entry); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode govulncheck JSON: %w", err)
		}
		if entry.OSV != nil {
			osvInfo[entry.OSV.ID] = entry.OSV
		}
		f := entry.Finding
		if f == nil || f.OSV == "" {
			continue
		}
		module := ""
		if len(f.Trace) > 0 {
			module = f.Trace[0].Module
		}
		if module == "" && osvInfo[f.OSV] != nil {
			module = osvInfo[f.OSV].module()
		}
		if module == "" {
			module = "stdlib"
		}
		key := f.OSV + "\x00" + module
		cur, ok := seen[key]
		if !ok {
			cur = &finding{ID: f.OSV, Module: module}
			seen[key] = cur
		}
		for _, t := range f.Trace {
			cur.Traces++
			if t.Function != "" {
				cur.Called = true
			}
			if t.Package != "" && !contains(cur.Packages, t.Package) {
				cur.Packages = append(cur.Packages, t.Package)
			}
		}
	}
	out := make([]finding, 0, len(seen))
	for _, f := range seen {
		if o := osvInfo[f.ID]; o != nil {
			f.Summary = o.Summary
			f.Aliases = o.Aliases
		}
		out = append(out, *f)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].Module < out[j].Module
	})
	return out, nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// loadAllowList reads and validates the allowlist file. Every entry must name
// the advisory, the module it affects, why it cannot be fixed here, and where
// the advisory/issue lives — an uncited exception is a configuration error.
func loadAllowList(path string) ([]allowEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var al allowList
	if err := yaml.Unmarshal(data, &al); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	seen := map[string]bool{}
	for i, e := range al.Exempt {
		switch {
		case e.ID == "":
			return nil, fmt.Errorf("%s: exempt[%d] has no id", path, i)
		case e.Module == "":
			return nil, fmt.Errorf("%s: exempt[%d] (%s) has no module", path, i, e.ID)
		case strings.TrimSpace(e.Reason) == "":
			return nil, fmt.Errorf("%s: exempt[%d] (%s) has no reason", path, i, e.ID)
		case !strings.HasPrefix(e.URL, "https://"):
			return nil, fmt.Errorf("%s: exempt[%d] (%s) has no https citation URL", path, i, e.ID)
		}
		key := e.ID + "\x00" + e.Module
		if seen[key] {
			return nil, fmt.Errorf("%s: duplicate exemption for %s in %s", path, e.ID, e.Module)
		}
		seen[key] = true
	}
	return al.Exempt, nil
}

// check splits the reported findings into the cited ones and the ones that must
// fail the gate, and reports allowlist entries that no longer match anything.
func check(findings []finding, allow []allowEntry) (unlisted []finding, stale []allowEntry) {
	allowed := make(map[string]allowEntry, len(allow))
	for _, e := range allow {
		allowed[e.ID+"\x00"+e.Module] = e
	}
	matched := make(map[string]bool, len(allow))
	for _, f := range findings {
		key := f.ID + "\x00" + f.Module
		if _, ok := allowed[key]; ok {
			matched[key] = true
			continue
		}
		unlisted = append(unlisted, f)
	}
	for _, e := range allow {
		if !matched[e.ID+"\x00"+e.Module] {
			stale = append(stale, e)
		}
	}
	return unlisted, stale
}

func main() {
	allowPath := flag.String("allowlist", "docs/security/govulncheck-allowlist.yaml", "path to the cited-exception allowlist")
	list := flag.Bool("list", false, "print the findings without gating")
	flag.Parse()

	findings, err := parseFindings(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "vulnallow: %v\n", err)
		os.Exit(1)
	}
	allow, err := loadAllowList(*allowPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "vulnallow: %v\n", err)
		os.Exit(1)
	}
	unlisted, stale := check(findings, allow)

	if len(findings) == 0 {
		fmt.Println("govulncheck: no vulnerabilities found")
	} else {
		fmt.Printf("govulncheck: %d advisory finding(s)\n", len(findings))
		for _, f := range findings {
			state := "UNLISTED"
			if _, ok := lookup(allow, f); ok {
				state = "CITED"
			}
			called := "not called"
			if f.Called {
				called = "called by this code"
			}
			aliases := ""
			if len(f.Aliases) > 0 {
				aliases = " [" + strings.Join(f.Aliases, ", ") + "]"
			}
			fmt.Printf("  %-9s %-14s %-32s %s%s\n", state, f.ID, f.Module, called, aliases)
			if f.Summary != "" {
				fmt.Printf("            %s\n", f.Summary)
			}
		}
	}

	if *list {
		return
	}

	ok := true
	for _, f := range unlisted {
		ok = false
		fmt.Fprintf(os.Stderr, "\nunlisted advisory: %s affects %s\n", f.ID, f.Module)
		if f.Summary != "" {
			fmt.Fprintf(os.Stderr, "  %s\n", f.Summary)
		}
		fmt.Fprintf(os.Stderr, "  fix it by upgrading the owning dependency, or record it in the SEC1 row of\n")
		fmt.Fprintf(os.Stderr, "  plan_docs/gcp-remaining-wave-plan.md (advisory id, module, upstream URL) and add\n")
		fmt.Fprintf(os.Stderr, "  it to %s with that citation.\n", *allowPath)
	}
	for _, e := range stale {
		ok = false
		fmt.Fprintf(os.Stderr, "\nstale exemption: %s (%s) no longer matches a finding — remove it from %s\n",
			e.ID, e.Module, *allowPath)
	}
	if !ok {
		os.Exit(1)
	}
}

func lookup(allow []allowEntry, f finding) (allowEntry, bool) {
	for _, e := range allow {
		if e.ID == f.ID && e.Module == f.Module {
			return e, true
		}
	}
	return allowEntry{}, false
}
