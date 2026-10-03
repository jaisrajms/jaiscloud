//go:build gcloud_conformance

package gcloud

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Status is the observed classification of one command.
type Status string

const (
	StatusPass        Status = "pass"
	StatusFail        Status = "fail"
	StatusUnsupported Status = "unsupported"
)

// Summary is a rollup count block.
type Summary struct {
	Total       int `json:"total"`
	Pass        int `json:"pass"`
	Fail        int `json:"fail"`
	Unsupported int `json:"unsupported"`
	Regressions int `json:"regressions"`
}

// Result is one row of the conformance matrix, as recorded in the report.
type CommandResult struct {
	Name       string   `json:"name"`
	Service    string   `json:"service"`
	Args       []string `json:"args"`
	Status     Status   `json:"status"`
	Expected   Expect   `json:"expected"`
	ExitCode   int      `json:"exit_code"`
	DurationMS int64    `json:"duration_ms"`
	Regression bool     `json:"regression"`
	// Error carries the assertion error or the gcloud error text on failure.
	Error string `json:"error,omitempty"`
	// Gap is the documented emulator/gcloud incompatibility, if any.
	Gap string `json:"gap,omitempty"`
	// Stdout and Stderr are bounded excerpts for debugging.
	Stdout string `json:"stdout_excerpt,omitempty"`
	Stderr string `json:"stderr_excerpt,omitempty"`
}

// Report is the machine-readable report.json payload.
type Report struct {
	GeneratedAt       string             `json:"generated_at"`
	GcloudVersion     string             `json:"gcloud_version"`
	Endpoint          string             `json:"endpoint"`
	Project           string             `json:"project"`
	AuthRecipe        []string           `json:"auth_recipe"`
	EndpointOverrides map[string]string  `json:"endpoint_overrides"`
	Summary           Summary            `json:"summary"`
	ByService         map[string]Summary `json:"by_service"`
	Results           []CommandResult    `json:"results"`
}

const excerptLimit = 600

// excerpt bounds a captured stream for inclusion in the report.
func excerpt(s string) string {
	s = strings.TrimRight(s, "\n")
	if len(s) > excerptLimit {
		return s[:excerptLimit] + "…"
	}
	return s
}

// Summarize computes rollup counts over results.
func Summarize(results []CommandResult) (Summary, map[string]Summary) {
	var total Summary
	byService := map[string]Summary{}
	for _, r := range results {
		total.Total++
		svc := byService[r.Service]
		svc.Total++
		switch r.Status {
		case StatusPass:
			total.Pass++
			svc.Pass++
		case StatusFail:
			total.Fail++
			svc.Fail++
		case StatusUnsupported:
			total.Unsupported++
			svc.Unsupported++
		}
		if r.Regression {
			total.Regressions++
			svc.Regressions++
		}
		byService[r.Service] = svc
	}
	return total, byService
}

// WriteReport writes report.json and report.md into dir.
func WriteReport(dir string, rep Report) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create report dir: %w", err)
	}
	if rep.Results == nil {
		rep.Results = []CommandResult{}
	}
	if rep.ByService == nil {
		rep.ByService = map[string]Summary{}
	}
	rep.Summary, rep.ByService = Summarize(rep.Results)

	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(dir, "report.json"), data, 0o644); err != nil {
		return fmt.Errorf("write report.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "report.md"), []byte(renderMarkdown(rep)), 0o644); err != nil {
		return fmt.Errorf("write report.md: %w", err)
	}
	return nil
}

func renderMarkdown(rep Report) string {
	var b strings.Builder
	b.WriteString("# gcloud CLI conformance report\n\n")
	fmt.Fprintf(&b, "Generated: `%s`\n\n", rep.GeneratedAt)
	fmt.Fprintf(&b, "| Field | Value |\n| --- | --- |\n")
	fmt.Fprintf(&b, "| gcloud | `%s` |\n", mdEscape(rep.GcloudVersion))
	fmt.Fprintf(&b, "| Endpoint | `%s` |\n", mdEscape(rep.Endpoint))
	fmt.Fprintf(&b, "| Project | `%s` |\n", mdEscape(rep.Project))
	fmt.Fprintf(&b, "| Auth | `%s` |\n", mdEscape(strings.Join(rep.AuthRecipe, "` `")))
	fmt.Fprintf(&b, "| Regression gate | `pass` commands that fail are fatal; "+
		"documented gaps are not |\n\n")

	fmt.Fprintf(&b, "## Summary\n\n")
	fmt.Fprintf(&b, "| Total | Pass | Fail | Unsupported | Regressions |\n")
	fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: | ---: |\n")
	fmt.Fprintf(&b, "| %d | %d | %d | %d | %d |\n\n",
		rep.Summary.Total, rep.Summary.Pass, rep.Summary.Fail, rep.Summary.Unsupported, rep.Summary.Regressions)

	// Per-service matrix.
	fmt.Fprintf(&b, "## Per-service matrix\n\n")
	services := make([]string, 0, len(rep.ByService))
	for s := range rep.ByService {
		services = append(services, s)
	}
	sort.Strings(services)
	fmt.Fprintf(&b, "| Service | Total | Pass | Fail | Unsupported | Regressions |\n")
	fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, s := range services {
		sm := rep.ByService[s]
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d |\n",
			s, sm.Total, sm.Pass, sm.Fail, sm.Unsupported, sm.Regressions)
	}
	b.WriteString("\n")

	// Full command table.
	fmt.Fprintf(&b, "## Command results\n\n")
	fmt.Fprintf(&b, "| # | Service | Command | Status | Expected | Exit | Regression |\n")
	fmt.Fprintf(&b, "| ---: | --- | --- | --- | --- | ---: | --- |\n")
	for i, r := range rep.Results {
		fmt.Fprintf(&b, "| %d | %s | `gcloud %s` | %s | %s | %d | %s |\n",
			i+1, r.Service, mdEscape(strings.Join(r.Args, " ")),
			r.Status, r.Expected, r.ExitCode, yesNo(r.Regression))
	}
	b.WriteString("\n")

	// Known incompatibilities.
	fmt.Fprintf(&b, "## Known incompatibilities\n\n")
	wrote := false
	for _, r := range rep.Results {
		if r.Gap == "" {
			continue
		}
		wrote = true
		fmt.Fprintf(&b, "### %s (`%s`)\n\n", r.Name, r.Status)
		fmt.Fprintf(&b, "- gcloud: `gcloud %s`\n", strings.Join(r.Args, " "))
		fmt.Fprintf(&b, "- exit: `%d`\n", r.ExitCode)
		fmt.Fprintf(&b, "- why: %s\n", r.Gap)
		if r.Error != "" {
			fmt.Fprintf(&b, "- error: `%s`\n", mdEscape(excerpt(r.Error)))
		}
		b.WriteString("\n")
	}
	if !wrote {
		b.WriteString("_None._\n\n")
	}

	// Failures that are regressions (unexpected) get their own section.
	fmt.Fprintf(&b, "## Unexpected failures (regressions)\n\n")
	wrote = false
	for _, r := range rep.Results {
		if !r.Regression {
			continue
		}
		wrote = true
		fmt.Fprintf(&b, "- **%s** (`gcloud %s`): exit %d — %s\n",
			r.Name, strings.Join(r.Args, " "), r.ExitCode, mdEscape(excerpt(r.Error)))
	}
	if !wrote {
		b.WriteString("_None._\n")
	}
	return b.String()
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func mdEscape(s string) string {
	return strings.ReplaceAll(s, "|", "\\|")
}
