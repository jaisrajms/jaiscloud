//go:build gcp_conformance

package main

import (
	"testing"

	conf "jaiscloud/tests/gcpconformance"
)

// cellsForCoverageTest derives the real cell set exactly the way run() does
// (registry + Discovery + conformance reports + overrides + transcript) so the
// coverage tests exercise the same inputs the committed matrix is built from.
func cellsForCoverageTest(t *testing.T) []Cell {
	t.Helper()
	ops := conf.Enumerate()
	docs := loadDocsForTest(t)
	report := loadReportForTest(t)
	grpcReport, err := ReadGRPCReport(testGRPCReportPath)
	if err != nil {
		t.Fatalf("ReadGRPCReport: %v", err)
	}
	ov := loadOverridesForTest(t, ops)
	coverage := restCoverageForTest(t, ops, docs)
	cells, _ := classifyAll(ops, docs, report, grpcReport, ov, coverage)
	return cells
}

const testGRPCReportPath = "../../tests/gcpconformance/grpc/testdata/report/report.json"

func coverageFor(t *testing.T, cov []CoverageEntry, service, transport string) CoverageEntry {
	t.Helper()
	for _, e := range cov {
		if e.Service == service && e.Transport == transport {
			return e
		}
	}
	t.Fatalf("no coverage entry for %s/%s", service, transport)
	return CoverageEntry{}
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// TestCoverageContainer pins the service the plan called out: 60 ga cells but
// only 31 of 71 published REST methods served. Coverage must expose the gap
// without inventing or re-grading a cell.
func TestCoverageContainer(t *testing.T) {
	docs := loadDocsForTest(t)
	cells := cellsForCoverageTest(t)
	cov := BuildCoverage(cells, docs)
	rest := coverageFor(t, cov, "container", "rest")

	if rest.Source != CoverageDiscovery {
		t.Errorf("container REST source = %q, want discovery", rest.Source)
	}
	if rest.Published != 71 {
		t.Errorf("container REST published = %d, want 71 (container.v1 Discovery)", rest.Published)
	}
	if rest.Served != 31 {
		t.Errorf("container REST served = %d, want 31 (the honest count the plan cites)", rest.Served)
	}
	if len(rest.Unserved) != 40 {
		t.Errorf("container REST unserved = %d, want 40", len(rest.Unserved))
	}
	if rest.Served+len(rest.Unserved) != rest.Published {
		t.Errorf("container REST served(%d)+unserved(%d) != published(%d)", rest.Served, len(rest.Unserved), rest.Published)
	}
	if containsString(rest.Unserved, "container.projects.locations.clusters.get") {
		t.Error("a served GKE method is listed as not served")
	}
	if !containsString(rest.Unserved, "container.projects.zones.clusters.get") {
		t.Error("the unserved zonal twin is missing from the not-served list")
	}

	grpc := coverageFor(t, cov, "container", "grpc")
	if grpc.Source != CoverageProto || grpc.Published != 37 || grpc.Served != 37 || grpc.Graded != 37 {
		t.Errorf("container gRPC coverage = %+v, want proto 37/37/37", grpc)
	}

	// The gRPC rollup must not be read as the whole service: 30 of the 37 ga
	// cells are gRPC, so the REST line is the one that exposes the gap.
	var ga int
	for _, c := range cells {
		if c.Service == "container" && c.State == StateGA {
			ga++
		}
	}
	if ga != 60 {
		t.Errorf("container ga cells = %d, want 60", ga)
	}
}

// TestCoverageDataproc pins the second named service and the documented-
// UNIMPLEMENTED boundary: the whole Serverless surface is served (501) but only
// graded by the one Dataproc.Serverless cell, so it is served-but-ungraded,
// never absent. clusters.repair / nodeGroups / autoscalingPolicies are not
// registered at all and stay not-served.
func TestCoverageDataproc(t *testing.T) {
	docs := loadDocsForTest(t)
	cov := BuildCoverage(cellsForCoverageTest(t), docs)
	rest := coverageFor(t, cov, "dataproc", "rest")

	if rest.Published != 136 {
		t.Errorf("dataproc REST published = %d, want 136", rest.Published)
	}
	if rest.Served != 90 {
		t.Errorf("dataproc REST served = %d, want 90", rest.Served)
	}
	if len(rest.Unserved) != 46 {
		t.Errorf("dataproc REST unserved = %d, want 46", len(rest.Unserved))
	}
	if rest.Unimplemented != 62 {
		t.Errorf("dataproc REST served-by-UNIMPLEMENTED = %d, want 62 (the Serverless surface)", rest.Unimplemented)
	}
	for _, want := range []string{
		"dataproc.projects.regions.clusters.repair",
		"dataproc.projects.regions.clusters.nodeGroups.create",
		"dataproc.projects.locations.autoscalingPolicies.list",
	} {
		if !containsString(rest.Unserved, want) {
			t.Errorf("%s: missing from the not-served list", want)
		}
	}
	for _, notWant := range []string{
		"dataproc.projects.locations.batches.create",
		"dataproc.projects.locations.sessions.create",
		"dataproc.projects.locations.sessionTemplates.create",
	} {
		if containsString(rest.Unserved, notWant) {
			t.Errorf("%s: documented UNIMPLEMENTED must be served-but-ungraded, not not-served", notWant)
		}
	}

	grpc := coverageFor(t, cov, "dataproc", "grpc")
	if grpc.Source != CoverageProto || grpc.Published != 36 || grpc.Served != 36 {
		t.Errorf("dataproc gRPC coverage = %+v, want proto 36/36 served", grpc)
	}
	if grpc.Graded != 22 || grpc.Unimplemented != 14 {
		t.Errorf("dataproc gRPC graded=%d unimplemented=%d, want 22/14", grpc.Graded, grpc.Unimplemented)
	}
}

// TestCoverageUnavailableIsExplicit: a service whose published list cannot be
// derived must say so instead of guessing.
func TestCoverageUnavailableIsExplicit(t *testing.T) {
	docs := loadDocsForTest(t)
	cov := BuildCoverage(cellsForCoverageTest(t), docs)

	for _, tc := range []struct{ service, transport string }{
		{"iceberg", "rest"},    // no official Discovery document
		{"operations", "rest"}, // gRPC-only
		{"bigquery", "grpc"},   // REST-only
	} {
		e := coverageFor(t, cov, tc.service, tc.transport)
		if e.Source != CoverageUnavailable {
			t.Errorf("%s/%s source = %q, want unavailable", tc.service, tc.transport, e.Source)
		}
		if e.Note == "" {
			t.Errorf("%s/%s: unavailable source must state why", tc.service, tc.transport)
		}
		if e.Published != 0 || e.Served != 0 || e.Graded != 0 {
			t.Errorf("%s/%s: unavailable source must not claim counts: %+v", tc.service, tc.transport, e)
		}
	}
}

// TestValidateCoverageGate is the "absence cannot read as completeness" gate:
// every cell must have a coverage record, and a record claiming a real source
// must have a non-zero published list.
func TestValidateCoverageGate(t *testing.T) {
	docs := loadDocsForTest(t)
	cells := cellsForCoverageTest(t)
	cov := BuildCoverage(cells, docs)

	if err := ValidateCoverage(cells, cov); err != nil {
		t.Fatalf("ValidateCoverage(real matrix) = %v, want nil", err)
	}

	// Drop one record: a cell would then have no coverage statement.
	trimmed := cov[:0]
	for _, e := range cov {
		if e.Service == "container" && e.Transport == "rest" {
			continue
		}
		trimmed = append(trimmed, e)
	}
	if err := ValidateCoverage(cells, trimmed); err == nil {
		t.Fatal("ValidateCoverage accepted a matrix with a cell but no coverage record")
	}

	// A derivable source that produced nothing is a broken derivation.
	empty := append([]CoverageEntry(nil), cov...)
	for i := range empty {
		if empty[i].Service == "container" && empty[i].Transport == "rest" {
			empty[i].Published = 0
		}
	}
	if err := ValidateCoverage(cells, empty); err == nil {
		t.Fatal("ValidateCoverage accepted a discovery source with 0 published methods")
	}
}

// TestCoverageDoesNotChangeGrading: coverage is reporting. The states and the
// cell set must be exactly what the classifier produced (no absent/not-served
// pseudo-state, no re-grading).
func TestCoverageDoesNotChangeGrading(t *testing.T) {
	docs := loadDocsForTest(t)
	cells := cellsForCoverageTest(t)
	before := make(map[string]string, len(cells))
	for _, c := range cells {
		before[c.Service+"/"+c.Operation+"/"+c.Transport] = c.State
	}

	_ = BuildCoverage(cells, docs)

	for _, c := range cells {
		key := c.Service + "/" + c.Operation + "/" + c.Transport
		if before[key] != c.State {
			t.Errorf("%s: state changed from %q to %q during coverage derivation", key, before[key], c.State)
		}
		if !ValidState(c.State) {
			t.Errorf("%s: coverage emitted a non-grade state %q", key, c.State)
		}
	}
}

// TestCoverageCountsAreConsistent checks the arithmetic every entry promises:
// served + unserved == published, and graded <= published.
func TestCoverageCountsAreConsistent(t *testing.T) {
	docs := loadDocsForTest(t)
	cov := BuildCoverage(cellsForCoverageTest(t), docs)
	for _, e := range cov {
		if e.Source == CoverageUnavailable {
			continue
		}
		if e.Served+len(e.Unserved) != e.Published {
			t.Errorf("%s/%s: served(%d)+unserved(%d) != published(%d)",
				e.Service, e.Transport, e.Served, len(e.Unserved), e.Published)
		}
		if e.Graded > e.Published {
			t.Errorf("%s/%s: graded(%d) > published(%d)", e.Service, e.Transport, e.Graded, e.Published)
		}
		if e.Unimplemented > e.Ungraded {
			t.Errorf("%s/%s: unimplemented(%d) > ungraded(%d)", e.Service, e.Transport, e.Unimplemented, e.Ungraded)
		}
	}
}
