package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// ServiceReport aggregates one service's planned cases.
type ServiceReport struct {
	Service  string         `json:"service"`
	Planned  int            `json:"planned"`
	PlanOK   int            `json:"plan_ok"`
	Failures map[string]int `json:"failures"`
}

// FailureGroup is the triage unit: one class+signature under a service.
type FailureGroup struct {
	Service      string   `json:"service"`
	ResourceType string   `json:"resource_type"`
	Class        string   `json:"class"`
	Signature    string   `json:"signature"`
	Count        int      `json:"count"`
	Example      string   `json:"example"`
	Cases        []string `json:"cases"`
}

// ReasonCount is the excluded-cases breakdown.
type ReasonCount struct {
	Class string `json:"class"`
	Count int    `json:"count"`
}

// Report is the committed evidence (corpus-report.json / .md).
type Report struct {
	Provider        string          `json:"provider"`
	ProviderVersion string          `json:"provider_version"`
	Terraform       string          `json:"terraform"`
	Endpoint        string          `json:"endpoint"`
	Project         string          `json:"project"`
	Corpora         []CorpusRef     `json:"corpora"`
	Cases           int             `json:"cases"`
	Candidate       int             `json:"candidates"`
	Excluded        int             `json:"excluded"`
	ZeroExample     []string        `json:"zero_example_services"`
	Services        []ServiceReport `json:"services"`
	Groups          []FailureGroup  `json:"failure_groups"`
	ExcludedReasons []ReasonCount   `json:"excluded_reasons"`
	Plan            []CaseResult    `json:"plan"`
	Apply           []CaseResult    `json:"apply"`
}

// BuildReport computes the aggregates from the manifest and the results.
func BuildReport(m *Manifest, tfVersion, endpoint, project string, plan, apply []CaseResult, zeroExample []string) *Report {
	r := &Report{
		Provider:        m.Provider,
		ProviderVersion: m.ProviderVersion,
		Terraform:       tfVersion,
		Endpoint:        endpoint,
		Project:         project,
		Corpora:         append([]CorpusRef{}, m.Corpora...),
		Cases:           len(m.Cases),
		ZeroExample:     zeroExample,
		Plan:            plan,
		Apply:           apply,
	}
	reasons := map[string]int{}
	for _, c := range m.Cases {
		if c.Status == StatusExcluded {
			r.Excluded++
			reasons[exclusionClass(c.Reason)]++
		} else {
			r.Candidate++
		}
	}
	for class, n := range reasons {
		r.ExcludedReasons = append(r.ExcludedReasons, ReasonCount{Class: class, Count: n})
	}
	sort.Slice(r.ExcludedReasons, func(i, j int) bool { return r.ExcludedReasons[i].Class < r.ExcludedReasons[j].Class })

	services := map[string]*ServiceReport{}
	groups := map[string]*FailureGroup{}
	for _, c := range plan {
		s := services[c.Service]
		if s == nil {
			s = &ServiceReport{Service: c.Service, Failures: map[string]int{}}
			services[c.Service] = s
		}
		s.Planned++
		if c.Class == "plan-ok" {
			s.PlanOK++
			continue
		}
		s.Failures[c.Class]++
		rt := ""
		if len(c.ResourceTypes) > 0 {
			rt = c.ResourceTypes[0]
		}
		key := c.Service + "\x00" + rt + "\x00" + c.Class + "\x00" + c.Summary
		g := groups[key]
		if g == nil {
			g = &FailureGroup{
				Service: c.Service, ResourceType: rt, Class: c.Class,
				Signature: c.Summary, Example: c.Name,
			}
			groups[key] = g
		}
		g.Count++
		g.Cases = append(g.Cases, c.Name)
	}
	for _, s := range services {
		r.Services = append(r.Services, *s)
	}
	sort.Slice(r.Services, func(i, j int) bool { return r.Services[i].Service < r.Services[j].Service })
	for _, g := range groups {
		sort.Strings(g.Cases)
		r.Groups = append(r.Groups, *g)
	}
	sort.Slice(r.Groups, func(i, j int) bool {
		a, b := r.Groups[i], r.Groups[j]
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		if a.Class != b.Class {
			return a.Class < b.Class
		}
		if a.ResourceType != b.ResourceType {
			return a.ResourceType < b.ResourceType
		}
		return a.Signature < b.Signature
	})
	return r
}

func exclusionClass(reason string) string {
	if strings.HasPrefix(reason, "unsupported-service") {
		return "unsupported-service"
	}
	if strings.HasPrefix(reason, "helper-provider") {
		return "helper-provider"
	}
	if strings.HasPrefix(reason, "beta-provider") {
		return "beta-provider"
	}
	if strings.Contains(reason, "deferred") {
		return "deferred-sample"
	}
	return "other"
}

// WriteJSON writes the report JSON.
func (r *Report) WriteJSON(path string) error {
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// LoadReport reads an existing report so a mode only refreshes its own section.
func LoadReport(path string) *Report {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var r Report
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil
	}
	return &r
}

// WriteMarkdown writes the human triage report.
func (r *Report) WriteMarkdown(path string) error {
	var b strings.Builder
	b.WriteString("# Terraform web-corpus report (TF2)\n\n")
	b.WriteString("Generated by `tools/tfcorpus`. Plan-only results are triage data, not a\n")
	b.WriteString("gate: every failure class below is owned by a ledger row in\n")
	b.WriteString("`plan_docs/gcp-terraform-confidence-wave-plan.md`.\n\n")
	fmt.Fprintf(&b, "- provider: `%s` %s\n", r.Provider, r.ProviderVersion)
	fmt.Fprintf(&b, "- terraform: %s\n", r.Terraform)
	fmt.Fprintf(&b, "- endpoint: %s\n", r.Endpoint)
	fmt.Fprintf(&b, "- project: %s\n", r.Project)
	for _, c := range r.Corpora {
		fmt.Fprintf(&b, "- corpus `%s`: %s @ `%s` (%s, %s)\n", c.Name, c.Repo, c.Commit, c.Path, c.Date)
	}
	fmt.Fprintf(&b, "\n%d manifest cases: %d candidates, %d excluded.\n\n", r.Cases, r.Candidate, r.Excluded)

	if len(r.ExcludedReasons) > 0 {
		b.WriteString("## Excluded cases\n\n| reason | cases |\n|---|---|\n")
		for _, e := range r.ExcludedReasons {
			fmt.Fprintf(&b, "| %s | %d |\n", e.Class, e.Count)
		}
		b.WriteString("\n")
	}

	if len(r.ZeroExample) > 0 {
		b.WriteString("## Services without a corpus example\n\n")
		b.WriteString("These services have a served resource in the TF1 inventory but the pinned\n")
		b.WriteString("corpora contain no Example Usage block for them; the ledger records why.\n\n")
		for _, s := range r.ZeroExample {
			fmt.Fprintf(&b, "- %s\n", s)
		}
		b.WriteString("\n")
	}

	if len(r.Plan) > 0 {
		b.WriteString("## Plan-only triage\n\n| service | planned | plan-ok | failures |\n|---|---|---|---|\n")
		for _, s := range r.Services {
			fail := s.Planned - s.PlanOK
			fmt.Fprintf(&b, "| %s | %d | %d | %d |\n", s.Service, s.Planned, s.PlanOK, fail)
		}
		b.WriteString("\n")

		b.WriteString("### Failure groups\n\n")
		if len(r.Groups) == 0 {
			b.WriteString("Every planned case reported a clean plan.\n\n")
		} else {
			b.WriteString("| service | resource type | class | signature | cases | example |\n|---|---|---|---|---|---|\n")
			for _, g := range r.Groups {
				fmt.Fprintf(&b, "| %s | %s | %s | %s | %d | %s |\n",
					g.Service, code(g.ResourceType), g.Class, mdCell(g.Signature), g.Count, code(g.Example))
			}
			b.WriteString("\n")
		}

		b.WriteString("### Cases\n\n| service | case | class | diagnostic |\n|---|---|---|---|\n")
		for _, c := range r.Plan {
			fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", c.Service, code(c.Name), c.Class, mdCell(c.Summary))
		}
		b.WriteString("\n")
	}

	if len(r.Apply) > 0 {
		b.WriteString("## Apply allowlist\n\n| service | case | result | detail |\n|---|---|---|---|\n")
		for _, c := range r.Apply {
			fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", c.Service, code(c.Name), c.Class, mdCell(c.Summary))
		}
		b.WriteString("\n")
	}

	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func code(s string) string {
	if s == "" {
		return "—"
	}
	return "`" + s + "`"
}

func mdCell(s string) string {
	if s == "" {
		return "—"
	}
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 140 {
		s = s[:140] + "…"
	}
	return s
}
