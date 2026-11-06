//go:build gcp_conformance

// Package main implements the GCP field-coverage inventory generator and gate.
//
// The emulator's operation-level fidelity matrix (tools/fidelitygen) answers
// "is this operation served?", not "is this field served?". A field can be
// present in the vendored proto or Discovery document, absent from the
// emulator's mapping, and still pass every existing gate — that is the defect
// class behind TGM1-3, TGM6, TGM11-1 and TGM14.
//
// fieldgen derives the field universe from the authoritative API definitions
// and emits one row per field and transport:
//
//   - gRPC: the compiled proto descriptors reachable from a scope entry, reading
//     google.api.field_behavior and google.api.resource_reference;
//   - REST: the vendored Discovery schemas reachable from a scope entry,
//     reading readOnly. The vendored storage v1 and Cloud DNS v1 snapshots
//     declare no readOnly flags at all, so those REST rows carry an empty
//     behavior and their server-owned members are classified by review rather
//     than by annotation; behavior drift on those rows cannot be detected from
//     the definition.
//
// The scope (which resources are enumerated) is curated in
// tools/fieldgen/scope.yaml; the classification (how the emulator treats each
// field) is the committed docs/fidelity/field-coverage.csv. `gen` scaffolds and
// refreshes the CSV from the definitions while preserving every classification
// already recorded; `check` fails when a definition field is missing or
// unclassified, a row is stale, or a row's evidence/reason contract is not met.
//
// Run from the repo root:
//
//	go run -tags gcp_conformance ./tools/fieldgen -check
//	go run -tags gcp_conformance ./tools/fieldgen -gen
//
// A Terraform provider-schema input is left as a documented hook for FC6; see
// terraform.go.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	conf "jaiscloud/tests/gcpconformance"
)

const (
	defaultCSVPath        = "docs/fidelity/field-coverage.csv"
	defaultScopePath      = "tools/fieldgen/scope.yaml"
	defaultDiscoveryDir   = "tests/gcpconformance/discovery"
	defaultExemptionsPath = "tests/gcpconformance/testdata/field-coverage-exemptions.json"
	// defaultReferenceCSVPath is the FC4 reference-integrity inventory.
	defaultReferenceCSVPath = "docs/fidelity/reference-coverage.csv"
	// defaultScenarioValuesPath pins the values that the definition's wire type
	// does not determine (a real region, a parseable CIDR, a valid runtime).
	defaultScenarioValuesPath = "tools/fieldgen/scenario-values.yaml"
)

func main() {
	csvPath := flag.String("out", defaultCSVPath, "field-coverage CSV to write (with -gen) or check (with -check)")
	scopePath := flag.String("scope", defaultScopePath, "scope manifest naming the enumerated resources")
	discoveryDir := flag.String("discovery", defaultDiscoveryDir, "vendored Discovery snapshots dir")
	exemptionsPath := flag.String("exemptions", defaultExemptionsPath, "field-coverage exemptions file")
	referencePath := flag.String("references", defaultReferenceCSVPath, "reference-coverage CSV to write (with -gen) or check (with -check)")
	scenarioPath := flag.String("scenarios", defaultScenarioPath, "generated round-trip scenario dataset (written with -gen, staleness-checked with -check)")
	valuesPath := flag.String("scenario-values", defaultScenarioValuesPath, "per-field value hints the definition's type cannot determine")
	gen := flag.Bool("gen", false, "regenerate the CSV from the API definitions, preserving existing classifications")
	check := flag.Bool("check", false, "validate the committed CSV against the API definitions")
	terraformSchema := flag.String("terraform-schema", "", "optional `terraform providers schema -json` dump (FC6 hook; not yet joined)")
	verbose := flag.Bool("v", false, "print per-service counts")
	flag.Parse()

	if *gen == *check {
		fmt.Fprintln(os.Stderr, "fieldgen: pass exactly one of -gen or -check")
		os.Exit(2)
	}

	// The Terraform provider-schema input is parsed (and its shape validated)
	// when supplied so FC6 can rely on the loader, but it contributes no rows
	// yet: the provider-resource -> GCP-resource mapping and the
	// sent-but-unsupported gate land in FC6 (see terraform.go).
	var provider *ProviderSchema
	if *terraformSchema != "" {
		var err error
		provider, err = LoadProviderSchema(*terraformSchema)
		if err != nil {
			fmt.Fprintln(os.Stderr, "fieldgen:", err)
			os.Exit(1)
		}
	}

	// Paths are repo-root-relative unless absolute, so the tool works from any
	// directory inside the module (the Makefile runs it from the root).
	root := repoRoot()
	err := run(options{
		Root:           root,
		CSVPath:        resolvePath(root, *csvPath),
		ScopePath:      resolvePath(root, *scopePath),
		DiscoveryDir:   resolvePath(root, *discoveryDir),
		ExemptionsPath: resolvePath(root, *exemptionsPath),
		ReferencePath:  resolvePath(root, *referencePath),
		ScenarioPath:   resolvePath(root, *scenarioPath),
		ValuesPath:     resolvePath(root, *valuesPath),
		Gen:            *gen,
		Check:          *check,
		Verbose:        *verbose,
		ProviderSchema: provider,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "fieldgen:", err)
		os.Exit(1)
	}
}

type options struct {
	Root           string
	CSVPath        string
	ScopePath      string
	DiscoveryDir   string
	ExemptionsPath string
	ReferencePath  string
	ScenarioPath   string
	ValuesPath     string
	Gen            bool
	Check          bool
	Verbose        bool
	ProviderSchema *ProviderSchema
}

func run(o options) error {
	scope, err := LoadScope(o.ScopePath)
	if err != nil {
		return err
	}
	docs, err := conf.LoadSnapshots(o.DiscoveryDir)
	if err != nil {
		return fmt.Errorf("load Discovery snapshots (%s): %w", o.DiscoveryDir, err)
	}
	universe, err := BuildUniverse(scope, docs)
	if err != nil {
		return err
	}

	if o.ProviderSchema != nil {
		// Documented FC6 hook: the schema is loaded and summarised, but its
		// attributes are not joined to the CSV in FC1/FC2. The join and the
		// sent-but-unsupported failure are FC6's deliverable.
		fmt.Printf("fieldgen: terraform provider schema loaded (%d resource types); attribute join lands in FC6\n",
			o.ProviderSchema.ResourceCount())
	}

	if o.Gen {
		if err := Generate(o.CSVPath, universe); err != nil {
			return err
		}
	}

	rows, err := ReadCSV(o.CSVPath)
	if err != nil {
		return err
	}
	hints, err := LoadValueHints(o.ValuesPath)
	if err != nil {
		return err
	}
	scenarios, err := BuildScenarios(universe, rows, hints)
	if err != nil {
		return err
	}
	if err := validateScenarioCoverage(universe, rows, scenarios); err != nil {
		return err
	}
	refs, err := BuildReferences(scope, universe)
	if err != nil {
		return err
	}
	if o.Gen {
		if err := GenerateReferences(o.ReferencePath, refs); err != nil {
			return err
		}
	} else {
		refRows, rerr := ReadReferenceCSV(o.ReferencePath)
		if rerr != nil {
			return rerr
		}
		validated, rerr := CheckReferences(o.Root, refs, refRows)
		if rerr != nil {
			return rerr
		}
		fmt.Printf("reference-coverage: %d reference field(s), %d validated against the referenced store\n", len(refs), validated)
	}
	if o.Gen {
		if err := WriteScenarios(o.ScenarioPath, scenarios); err != nil {
			return err
		}
		st := scenarioStats(scenarios)
		fmt.Printf("field-coverage: wrote %s — %d %s across %d resource(s) (%d update steps, %d reference values), %d negative write(s)\n",
			o.ScenarioPath, st.Values, plural(st.Values, "generated field value", "generated field values"), st.Resources, st.Updates, st.Refs, st.Attempts)
		return nil
	}
	if err := CheckScenarios(o.ScenarioPath, scenarios); err != nil {
		return err
	}
	exemptions, err := LoadExemptions(o.ExemptionsPath)
	if err != nil {
		return err
	}
	report, err := CheckIn(o.Root, universe, rows, exemptions)
	if err != nil {
		return err
	}
	printReport(report, o.Verbose, o.ExemptionsPath, exemptions)
	return nil
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

func resolvePath(root, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}

// repoRoot walks up from the working directory to the module root, so paths and
// evidence pointers (repo-relative) resolve no matter where the tool is invoked.
func repoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "."
		}
		dir = parent
	}
}

func printReport(r Report, verbose bool, exemptionsPath string, exemptions Exemptions) {
	fmt.Printf("field-coverage: %d field(s) across %d resource(s): %s\n",
		r.Total, r.Resources, formatCounts(r.ByCoverage))
	if verbose {
		for _, svc := range sortedKeys(r.ByService) {
			fmt.Printf("  %-12s %3d field(s)\n", svc, r.ByService[svc])
		}
	}
	if r.Exempt > 0 {
		fmt.Printf("field-coverage: %d field(s) rely on an exemption (%s)\n", r.Exempt, exemptionsPath)
		if verbose {
			keys := make([]string, 0, len(exemptions))
			for k := range exemptions {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Printf("  %s: %s\n", k, exemptions[k])
			}
		}
	}
}

func formatCounts(counts map[string]int) string {
	order := []string{CoverageRoundtrip, CoverageDerived, CoverageImmutable, CoverageOutputOnly, CoverageUnsupported}
	parts := make([]string, 0, len(order))
	for _, k := range order {
		if counts[k] > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", k, counts[k]))
		}
	}
	return strings.Join(parts, " ")
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
