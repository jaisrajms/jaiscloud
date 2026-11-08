package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Verdict classes for a real-GCP vs emulator difference.
const (
	verdictEmulatorGap   = "emulator-gap"   // the emulator must change; schedule it
	verdictProviderQuirk = "provider-quirk" // the provider (or real GCP) behaves this way; no fix
	verdictEnvironment   = "environment"    // project id/region/org difference, not behavior
	verdictAcceptedRisk  = "accepted-risk"  // known, documented, deliberately not fixed
)

var verdicts = map[string]bool{
	verdictEmulatorGap:   true,
	verdictProviderQuirk: true,
	verdictEnvironment:   true,
	verdictAcceptedRisk:  true,
}

// Classifications is differential/classifications.yaml.
type Classifications struct {
	Rules []Rule `yaml:"rules"`
}

// Rule classifies the differences under one path. `*` is a glob.
type Rule struct {
	Case    string `yaml:"case"`
	Path    string `yaml:"path"`
	Verdict string `yaml:"verdict"`
	ID      string `yaml:"id"`
	Branch  string `yaml:"branch"`
	Reason  string `yaml:"reason"`
}

// Classified is one diff with its rule.
type Classified struct {
	Diff
	Verdict string
	ID      string
	Reason  string
}

// LoadClassifications reads and validates the classification table.
func LoadClassifications(path string) (*Classifications, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read classifications: %w", err)
	}
	var c Classifications
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	seen := map[string]bool{}
	for i, r := range c.Rules {
		where := fmt.Sprintf("%s: rule %d (%s %s)", path, i+1, r.Case, r.Path)
		if r.Case == "" || r.Path == "" {
			return nil, fmt.Errorf("%s: case and path are required", where)
		}
		if !verdicts[r.Verdict] {
			return nil, fmt.Errorf("%s: verdict %q is not one of emulator-gap/provider-quirk/environment/accepted-risk", where, r.Verdict)
		}
		if r.Verdict == verdictEmulatorGap {
			if r.ID == "" || r.Branch == "" {
				return nil, fmt.Errorf("%s: an emulator-gap row needs an id and a branch", where)
			}
		} else if r.Reason == "" {
			return nil, fmt.Errorf("%s: a %s row needs a reason", where, r.Verdict)
		}
		key := r.Case + "\x00" + r.Path
		if seen[key] {
			return nil, fmt.Errorf("%s: duplicate (case, path) rule", where)
		}
		seen[key] = true
	}
	return &c, nil
}

// Match reports whether a rule covers (caseName, path).
func (r Rule) Match(caseName, path string) bool {
	if !globMatch(r.Case, caseName) {
		return false
	}
	return globMatch(r.Path, path)
}

// globMatch matches s against a pattern where `*` matches any run of characters.
func globMatch(pattern, s string) bool {
	var b strings.Builder
	b.WriteString("^")
	for i, part := range strings.Split(pattern, "*") {
		if i > 0 {
			b.WriteString(".*")
		}
		b.WriteString(regexp.QuoteMeta(part))
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(s)
}

// Classify applies the table to a case's diffs. An unclassified diff is
// returned separately; two contradictory rules for one path are an error.
func (c *Classifications) Classify(caseName string, diffs []Diff) (classified []Classified, unclassified []Diff, err error) {
	for _, d := range diffs {
		var matched []Rule
		for _, r := range c.Rules {
			if r.Match(caseName, d.Path) {
				matched = append(matched, r)
			}
		}
		if len(matched) == 0 {
			unclassified = append(unclassified, d)
			continue
		}
		verdict := matched[0].Verdict
		for _, r := range matched[1:] {
			if r.Verdict != verdict {
				return nil, nil, fmt.Errorf("case %s path %s matches contradictory rules (%s vs %s)", caseName, d.Path, verdict, r.Verdict)
			}
		}
		classified = append(classified, Classified{
			Diff:    d,
			Verdict: matched[0].Verdict,
			ID:      matched[0].ID,
			Reason:  matched[0].Reason,
		})
	}
	sort.SliceStable(classified, func(i, j int) bool { return classified[i].Path < classified[j].Path })
	return classified, unclassified, nil
}
