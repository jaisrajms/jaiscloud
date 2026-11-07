// Command tfcoverage builds and checks the Terraform provider coverage
// inventory (wave plan TF1).
//
// The hand-maintained input is tools/tfcoverage/scope.yaml (provider resource
// type -> emulated service + state + reason). This command joins it with the
// committed `terraform providers schema -json` snapshot and the resource blocks
// the fixture and pinned module cases declare, then writes or verifies
// docs/fidelity/terraform-coverage.csv.
//
//	tfcoverage -check                      # the offline PR gate
//	tfcoverage -init                       # regenerate the committed CSV
//	tfcoverage -raw-schema /tmp/schema.json -write-snapshot docs/fidelity/terraform-provider-schema-full.json
//
// Exit codes: 0 ok, 1 a check failed.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	providerschema "jaiscloud/tools/providerschema"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "tfcoverage:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("tfcoverage", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	schemaPath := fs.String("schema", "docs/fidelity/terraform-provider-schema-full.json", "committed compacted provider schema snapshot")
	rawSchema := fs.String("raw-schema", "", "a raw `terraform providers schema -json` dump to compact")
	writeSnapshot := fs.String("write-snapshot", "", "write the compacted snapshot of -raw-schema here and exit")
	scopePath := fs.String("scope", "tools/tfcoverage/scope.yaml", "hand-maintained classification rules")
	coveragePath := fs.String("coverage", "docs/fidelity/terraform-coverage.csv", "generated inventory")
	fixture := fs.String("fixture", "tests/integration/gcp/terraform/main.tf", "Terraform fixture root module")
	casesDir := fs.String("cases", "tests/integration/gcp/terraform/module-cases", "pinned module cases")
	matrixPath := fs.String("matrix", "docs/fidelity/fidelity-matrix.json", "fidelity matrix (service states)")
	todayFlag := fs.String("today", "", "override today's date (YYYY-MM-DD; tests)")
	initFlag := fs.Bool("init", false, "regenerate the committed inventory")
	checkFlag := fs.Bool("check", false, "verify the committed inventory and every row rule")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *rawSchema != "" || *writeSnapshot != "" {
		if *rawSchema == "" || *writeSnapshot == "" {
			return fmt.Errorf("-raw-schema and -write-snapshot must be used together")
		}
		return compactSnapshot(*rawSchema, *writeSnapshot)
	}
	if *initFlag == *checkFlag {
		return fmt.Errorf("choose exactly one of -init or -check")
	}

	today := time.Now().UTC().Truncate(24 * time.Hour)
	if *todayFlag != "" {
		d, err := time.Parse(dateLayout, *todayFlag)
		if err != nil {
			return fmt.Errorf("-today: %w", err)
		}
		today = d
	}

	schema, err := providerschema.Load(*schemaPath)
	if err != nil {
		return err
	}
	scope, err := LoadScope(*scopePath)
	if err != nil {
		return err
	}
	exercise, err := ParseExercise(Sources{Fixture: *fixture, CasesDir: *casesDir})
	if err != nil {
		return err
	}
	matrix, err := LoadMatrix(*matrixPath)
	if err != nil {
		return err
	}
	inv, err := Compute(schema, scope, exercise)
	if err != nil {
		return err
	}
	if err := inv.Validate(scope, today, matrix); err != nil {
		return err
	}

	if *initFlag {
		var buf bytes.Buffer
		if err := inv.WriteCSV(&buf); err != nil {
			return err
		}
		if err := os.WriteFile(*coveragePath, buf.Bytes(), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", *coveragePath, err)
		}
		fmt.Printf("tfcoverage: wrote %s\n", *coveragePath)
		printSummary(inv, schema.ResourceCount())
		return nil
	}

	f, err := os.Open(*coveragePath)
	if err != nil {
		return fmt.Errorf("read %s: %w (run make gen-gcp-terraform-coverage)", *coveragePath, err)
	}
	defer f.Close()
	committed, err := ReadCSV(f)
	if err != nil {
		return err
	}
	diffs := DiffRows(committed, inv.Rows)
	if len(diffs) > 0 {
		return fmt.Errorf("committed inventory is stale (%d difference(s)); run make gen-gcp-terraform-coverage and review the diff:\n  %s",
			len(diffs), strings.Join(joinLimit(diffs, 20), "\n  "))
	}
	printSummary(inv, schema.ResourceCount())
	return nil
}

func compactSnapshot(rawPath, outPath string) error {
	raw, err := os.ReadFile(rawPath)
	if err != nil {
		return fmt.Errorf("read raw schema: %w", err)
	}
	schema, err := providerschema.Parse(raw, rawPath)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(schema.Resources))
	for name := range schema.Resources {
		names = append(names, name)
	}
	sort.Strings(names)
	compact, err := providerschema.Compact(raw, names)
	if err != nil {
		return err
	}
	if err := os.WriteFile(outPath, compact, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", outPath, err)
	}
	fmt.Printf("tfcoverage: wrote %s (%d resource types, %d bytes)\n", outPath, len(names), len(compact))
	return nil
}

func printSummary(inv *Inventory, providerTypes int) {
	exercised, planOnly, none := 0, 0, 0
	for _, r := range inv.Rows {
		switch r.Classification {
		case ClassPlanOnly:
			planOnly++
		default:
			if r.ExercisedBy == "none" {
				none++
			} else {
				exercised++
			}
		}
	}
	fmt.Printf("tfcoverage: %d provider resource types; %d in scope, %d out of scope; %d exercised, %d plan-only, %d unexercised\n",
		providerTypes, len(inv.Rows), inv.OutOfScope, exercised, planOnly, none)

	services := make([]string, 0, len(inv.ServiceRows))
	for svc := range inv.ServiceRows {
		services = append(services, svc)
	}
	sort.Strings(services)
	for _, svc := range services {
		rows := inv.ServiceRows[svc]
		covered := 0
		for _, r := range rows {
			if r.ExercisedBy != "none" {
				covered++
			}
		}
		fmt.Printf("  %-18s %3d rows, %3d exercised, %3d unexercised\n", svc, len(rows), covered, len(rows)-covered)
	}
}

func joinLimit(in []string, n int) []string {
	if len(in) <= n {
		return in
	}
	return append(append([]string{}, in[:n]...), fmt.Sprintf("… (%d more)", len(in)-n))
}
