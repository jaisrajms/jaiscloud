//go:build gcp_conformance

package gcpconformance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Report is the machine-readable report.json payload.
type Report struct {
	Total       int            `json:"total"`
	BySeverity  map[string]int `json:"by_severity"`
	ByKind      map[string]int `json:"by_kind"`
	Divergences []Divergence   `json:"divergences"`
}

// SeverityRank orders severities from most to least severe for reporting.
func SeverityRank(sev string) int {
	switch sev {
	case "high":
		return 0
	case "medium":
		return 1
	case "low":
		return 2
	case "info":
		return 3
	default:
		return 4
	}
}

// WriteReport writes report.json (machine-readable) and report.md (grouped by
// severity then service, with expected vs actual) into dir.
func WriteReport(dir string, divs []Divergence) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create report dir: %w", err)
	}
	if divs == nil {
		divs = []Divergence{}
	}

	rep := Report{
		Total:       len(divs),
		BySeverity:  map[string]int{},
		ByKind:      map[string]int{},
		Divergences: divs,
	}
	for _, d := range divs {
		rep.BySeverity[d.Severity]++
		rep.ByKind[d.Kind]++
	}

	jsonData, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	jsonData = append(jsonData, '\n')
	if err := os.WriteFile(filepath.Join(dir, "report.json"), jsonData, 0o644); err != nil {
		return err
	}

	md := renderMarkdown(divs)
	return os.WriteFile(filepath.Join(dir, "report.md"), []byte(md), 0o644)
}

func renderMarkdown(divs []Divergence) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# GCP wire-conformance report\n\n")
	fmt.Fprintf(&b, "Total divergences: **%d**\n\n", len(divs))

	bySeverity := map[string]int{}
	byKind := map[string]int{}
	for _, d := range divs {
		bySeverity[d.Severity]++
		byKind[d.Kind]++
	}
	fmt.Fprintf(&b, "## Summary\n\n")
	fmt.Fprintf(&b, "| Severity | Count |\n| --- | --- |\n")
	for _, sev := range sortedKeys(bySeverity, severityLess) {
		fmt.Fprintf(&b, "| %s | %d |\n", sev, bySeverity[sev])
	}
	fmt.Fprintf(&b, "\n| Kind | Count |\n| --- | --- |\n")
	for _, k := range sortedKeys(byKind, nil) {
		fmt.Fprintf(&b, "| %s | %d |\n", k, byKind[k])
	}

	grouped := map[string][]Divergence{}
	for _, d := range divs {
		grouped[d.Severity] = append(grouped[d.Severity], d)
	}

	for _, sev := range sortedKeys(grouped, severityLess) {
		fmt.Fprintf(&b, "\n## Severity: %s (%d)\n\n", sev, len(grouped[sev]))
		byService := map[string][]Divergence{}
		for _, d := range grouped[sev] {
			svc := d.Service
			if svc == "" {
				svc = "(unknown)"
			}
			byService[svc] = append(byService[svc], d)
		}
		for _, svc := range sortedKeys(byService, nil) {
			fmt.Fprintf(&b, "### Service: %s (%d)\n\n", svc, len(byService[svc]))
			fmt.Fprintf(&b, "| Kind | Location | Expected | Actual |\n| --- | --- | --- | --- |\n")
			for _, d := range byService[svc] {
				fmt.Fprintf(&b, "| %s | %s | %s | %s |\n",
					mdEscape(d.Kind), mdEscape(d.Path), mdEscape(d.Expected), mdEscape(d.Actual))
			}
			fmt.Fprintf(&b, "\n")
		}
	}
	return b.String()
}

func severityLess(a, b string) bool {
	ra, rb := SeverityRank(a), SeverityRank(b)
	if ra != rb {
		return ra < rb
	}
	return a < b
}

func sortedKeys[V any](m map[string]V, less func(a, b string) bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	if less != nil {
		sort.Slice(keys, func(i, j int) bool { return less(keys[i], keys[j]) })
	} else {
		sort.Strings(keys)
	}
	return keys
}

func mdEscape(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}
