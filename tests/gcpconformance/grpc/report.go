package grpcconformance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Rollup is the suite-wide aggregate ("rollup") across all services.
type Rollup struct {
	Services      int     `json:"services"`
	Checks        int     `json:"checks"`
	Passed        int     `json:"passed"`
	Failed        int     `json:"failed"`
	Unimplemented int     `json:"unimplemented"`
	PassRate      float64 `json:"pass_rate"`
}

// ServiceSummary is the per-service pass/fail/unimplemented rollup.
type ServiceSummary struct {
	Service       string `json:"service"`
	Total         int    `json:"total"`
	Passed        int    `json:"passed"`
	Failed        int    `json:"failed"`
	Unimplemented int    `json:"unimplemented"`
}

// Report is the machine-readable report.json payload.
type Report struct {
	Endpoint      string           `json:"endpoint"`
	Project       string           `json:"project"`
	Total         int              `json:"total"`
	Passed        int              `json:"passed"`
	Failed        int              `json:"failed"`
	Unimplemented int              `json:"unimplemented"`
	Rollup        Rollup           `json:"rollup"`
	Services      []ServiceSummary `json:"services"`
	Results       []Result         `json:"results"`
}

// BuildReport aggregates raw check results into a Report.
func BuildReport(cfg Config, results []Result) Report {
	rep := Report{
		Endpoint: cfg.Endpoint,
		Project:  cfg.Project,
		Results:  results,
		Total:    len(results),
	}
	byService := map[string]*ServiceSummary{}
	for _, r := range results {
		switch r.Status {
		case StatusPass:
			rep.Passed++
		case StatusUnimplemented:
			rep.Unimplemented++
		default:
			rep.Failed++
		}
		s := byService[r.Service]
		if s == nil {
			s = &ServiceSummary{Service: r.Service}
			byService[r.Service] = s
		}
		s.Total++
		switch r.Status {
		case StatusPass:
			s.Passed++
		case StatusUnimplemented:
			s.Unimplemented++
		default:
			s.Failed++
		}
	}

	names := make([]string, 0, len(byService))
	for name := range byService {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		rep.Services = append(rep.Services, *byService[name])
	}

	rep.Rollup = Rollup{
		Services:      len(rep.Services),
		Checks:        rep.Total,
		Passed:        rep.Passed,
		Failed:        rep.Failed,
		Unimplemented: rep.Unimplemented,
		PassRate:      passRate(rep.Passed, rep.Total),
	}
	return rep
}

func passRate(passed, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(passed) / float64(total) * 100
}

// WriteReport writes report.json (machine-readable) and report.md (per-service
// summary + detailed matrix) into dir, creating it if needed.
func WriteReport(dir string, rep Report) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create report dir: %w", err)
	}
	if rep.Results == nil {
		rep.Results = []Result{}
	}
	if rep.Services == nil {
		rep.Services = []ServiceSummary{}
	}

	jsonData, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	jsonData = append(jsonData, '\n')
	if err := os.WriteFile(filepath.Join(dir, "report.json"), jsonData, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "report.md"), []byte(renderMarkdown(rep)), 0o644)
}

func renderMarkdown(rep Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# GCP gRPC conformance report\n\n")
	fmt.Fprintf(&b, "Endpoint: `%s`  \nProject: `%s`\n\n", mdEscape(rep.Endpoint), mdEscape(rep.Project))

	fmt.Fprintf(&b, "## Rollup\n\n")
	fmt.Fprintf(&b, "| Checks | Pass | Fail | Unimplemented | Pass rate |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- | --- |\n")
	fmt.Fprintf(&b, "| %d | %d | %d | %d | %.1f%% |\n\n",
		rep.Rollup.Checks, rep.Rollup.Passed, rep.Rollup.Failed, rep.Rollup.Unimplemented, rep.Rollup.PassRate)

	fmt.Fprintf(&b, "## By service\n\n")
	fmt.Fprintf(&b, "| Service | Total | Pass | Fail | Unimplemented |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- | --- |\n")
	for _, s := range rep.Services {
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d |\n",
			mdEscape(s.Service), s.Total, s.Passed, s.Failed, s.Unimplemented)
	}
	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "## Checks\n\n")
	fmt.Fprintf(&b, "| Service | RPC | Status | Key field | Error |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- | --- |\n")
	for _, r := range rep.Results {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n",
			mdEscape(r.Service), mdEscape(r.RPC), r.Status, mdEscape(r.KeyField), mdEscape(r.Error))
	}
	return b.String()
}

func mdEscape(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}
