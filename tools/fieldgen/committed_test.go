//go:build gcp_conformance

package main

import (
	"path/filepath"
	"testing"

	conf "jaiscloud/tests/gcpconformance"
)

// TestCommittedInventoryIsComplete runs the real gate over the committed scope,
// Discovery snapshots and CSV, so the unit-test job catches an unclassified or
// stale row even when the generator binary is not run.
func TestCommittedInventoryIsComplete(t *testing.T) {
	const repo = "../.."
	scope, err := LoadScope(filepath.Join(repo, "tools/fieldgen/scope.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	docs, err := conf.LoadSnapshots(filepath.Join(repo, "tests/gcpconformance/discovery"))
	if err != nil {
		t.Fatal(err)
	}
	universe, err := BuildUniverse(scope, docs)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := ReadCSV(filepath.Join(repo, "docs/fidelity/field-coverage.csv"))
	if err != nil {
		t.Fatal(err)
	}
	exemptions, err := LoadExemptions(filepath.Join(repo, "tests/gcpconformance/testdata/field-coverage-exemptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	rep, err := CheckIn(repo, universe, rows, exemptions)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total == 0 {
		t.Fatal("inventory covers no fields")
	}
}
