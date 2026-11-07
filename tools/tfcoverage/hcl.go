package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// resourceRe matches an HCL resource block head. It is deliberately anchored at
// the start of a line (bar indentation) so a commented-out block — "# resource
// ..." — is not counted, and it is used on both .tf fixtures and the .sh module
// cases (whose heredocs contain HCL).
var resourceRe = regexp.MustCompile(`(?m)^[ \t]*resource[ \t]+"([A-Za-z0-9_]+)"[ \t]+"([A-Za-z0-9_.-]+)"[ \t]*\{`)

// planOnlyRe matches a case file that runs the driver in plan-only mode
// (wave-plan TF3-6). Such a case's resources are reported as plan-only:<case>
// rather than module:<case>.
var planOnlyRe = regexp.MustCompile(`(?m)^[ \t]*PLAN_ONLY[ \t]*=[ \t]*["']?1`)

// Exercise is what the committed Terraform fixture and module cases declare,
// keyed by provider resource type. A type can be exercised by more than one
// place (the fixture and a module case, or several resources of one type).
type Exercise map[string][]string

// Sources are the paths the inventory reads for exercised-by evidence.
type Sources struct {
	Fixture  string // tests/integration/gcp/terraform/main.tf
	CasesDir string // tests/integration/gcp/terraform/module-cases
}

// ParseExercise reads the fixture and every module case.
//
// exercised_by values are `fixture:<name>` for a resource declared in the
// fixture root module, `module:<case>` for a resource declared by a pinned
// module case's root HCL, and `plan-only:<case>` when the case opts into the
// plan-only tier. A module's *internal* resources are not attributed here: the
// case root is what the repository owns and what an offline gate can read
// deterministically.
func ParseExercise(src Sources) (Exercise, error) {
	out := Exercise{}
	if src.Fixture != "" {
		resources, err := parseResourceBlocks(src.Fixture)
		if err != nil {
			return nil, fmt.Errorf("fixture: %w", err)
		}
		for _, r := range resources {
			out.add(r.Type, "fixture:"+r.Name)
		}
	}
	cases, err := filepath.Glob(filepath.Join(src.CasesDir, "*.sh"))
	if err != nil {
		return nil, fmt.Errorf("module cases: %w", err)
	}
	sort.Strings(cases)
	for _, file := range cases {
		if strings.HasSuffix(file, "_test.sh") {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("module case %s: %w", file, err)
		}
		caseName := strings.TrimSuffix(filepath.Base(file), ".sh")
		label := "module:" + caseName
		if planOnlyRe.Match(raw) {
			label = "plan-only:" + caseName
		}
		resources, err := parseResourceBlocksRaw(raw, file)
		if err != nil {
			return nil, err
		}
		for _, r := range resources {
			out.add(r.Type, label)
		}
	}
	// A type exercised both through apply and through a plan-only case is
	// applied; keep both entries (a reader sees every case) but the
	// classification prefers apply when any label is module:/fixture:.
	for typ, entries := range out {
		out[typ] = dedupeSorted(entries)
	}
	return out, nil
}

func (e Exercise) add(resourceType, entry string) {
	e[resourceType] = append(e[resourceType], entry)
}

// ExercisedBy formats a type's entries as the CSV column value ("none" when
// nothing declares it).
func (e Exercise) ExercisedBy(resourceType string) string {
	entries := e[resourceType]
	if len(entries) == 0 {
		return "none"
	}
	sorted := dedupeSorted(entries)
	return strings.Join(sorted, ",")
}

type hclResource struct {
	Type string
	Name string
}

func parseResourceBlocks(file string) ([]hclResource, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return parseResourceBlocksRaw(raw, file)
}

func parseResourceBlocksRaw(raw []byte, file string) ([]hclResource, error) {
	_ = file
	var out []hclResource
	seen := map[string]bool{}
	for _, m := range resourceRe.FindAllSubmatch(raw, -1) {
		key := string(m[1]) + "." + string(m[2])
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, hclResource{Type: string(m[1]), Name: string(m[2])})
	}
	return out, nil
}

func dedupeSorted(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
