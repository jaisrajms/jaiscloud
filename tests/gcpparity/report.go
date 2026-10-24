//go:build gcp_parity

package gcpparity

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Report is the persisted parity result. It records per-service coverage and
// every finding so a reader can see what was compared and what diverged.
type Report struct {
	Target    string          `json:"target"`
	Project   string          `json:"project"`
	Services  []ServiceReport `json:"services"`
	Steps     int             `json:"steps"`
	Mutations int             `json:"mutations"`
	Compared  int             `json:"compared"`
	Findings  int             `json:"findings"`
	Failures  int             `json:"failures"`
	Allowed   int             `json:"allowed"`
	ByKind    map[string]int  `json:"by_kind"`
}

// ServiceReport is one service's slice of the run.
type ServiceReport struct {
	Service   string    `json:"service"`
	Steps     int       `json:"steps"`
	Mutations int       `json:"mutations"`
	Compared  int       `json:"compared"`
	Covered   bool      `json:"covered"`
	Aborted   bool      `json:"aborted,omitempty"`
	Findings  []Finding `json:"findings,omitempty"`
}

const defaultReportDir = "testdata/report"

// buildReport assembles the run report from the per-service results.
func buildReport(cfg Config, results []ServiceResult) Report {
	rep := Report{Target: cfg.REST + " ↔ " + cfg.GRPC, Project: cfg.Project, ByKind: map[string]int{}}
	for _, r := range results {
		sr := ServiceReport{
			Service:   r.Service,
			Steps:     r.Steps,
			Mutations: r.Mutations,
			Compared:  r.Compared,
			Covered:   r.covered(),
			Aborted:   r.Aborted,
			Findings:  r.Findings,
		}
		rep.Services = append(rep.Services, sr)
		rep.Steps += r.Steps
		rep.Mutations += r.Mutations
		rep.Compared += r.Compared
		for _, f := range r.Findings {
			rep.Findings++
			rep.ByKind[f.Kind]++
			if f.Allowed {
				rep.Allowed++
			}
			if f.failing() {
				rep.Failures++
			}
		}
	}
	sort.Slice(rep.Services, func(i, j int) bool { return rep.Services[i].Service < rep.Services[j].Service })
	return rep
}

// writeReport writes parity.{json,md} under dir.
func writeReport(dir string, rep Report) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(dir, "parity.json"), data, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "parity.md"), []byte(renderMarkdown(rep)), 0o644)
}

func renderMarkdown(rep Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# REST↔gRPC parity report\n\n")
	fmt.Fprintf(&b, "- target: %s\n", rep.Target)
	fmt.Fprintf(&b, "- project: %s\n", rep.Project)
	fmt.Fprintf(&b, "- steps: %d (%d mutations, %d cross-transport comparisons)\n", rep.Steps, rep.Mutations, rep.Compared)
	fmt.Fprintf(&b, "- findings: %d (%d failing, %d allowed)\n\n", rep.Findings, rep.Failures, rep.Allowed)
	fmt.Fprintf(&b, "| service | steps | compared | covered | findings |\n|---|---:|---:|:--:|---:|\n")
	for _, s := range rep.Services {
		fmt.Fprintf(&b, "| %s | %d | %d | %s | %d |\n", s.Service, s.Steps, s.Compared, yesNo(s.Covered), len(s.Findings))
	}
	if rep.Findings > 0 {
		fmt.Fprintf(&b, "\n## Findings by kind\n\n")
		for _, k := range sortedFindingKinds(rep.ByKind) {
			fmt.Fprintf(&b, "- %s: %d\n", k, rep.ByKind[k])
		}
		for _, s := range rep.Services {
			for _, f := range s.Findings {
				status := "FAIL"
				if f.Allowed {
					status = "allow"
				} else if !f.failing() {
					status = "info"
				}
				fmt.Fprintf(&b, "\n- [%s] %s/%s %s at %s\n  rest=%s\n  grpc=%s\n", status, s.Service, f.Op, f.Kind, f.Location, f.Expected, f.Actual)
				if f.Reason != "" {
					fmt.Fprintf(&b, "  reason: %s\n", f.Reason)
				}
			}
		}
	}
	return b.String()
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}
