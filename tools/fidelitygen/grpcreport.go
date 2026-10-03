//go:build gcp_conformance

package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// grpcReport is the subset of the gRPC conformance report
// (tests/gcpconformance/grpc/testdata/report/report.json) that fidelitygen
// consumes. The report is produced by the separate module
// tests/gcpconformance/grpc, so the generator cannot import its types; these
// structs mirror the wire shape instead.
//
// The generator keys fidelity cells by the exact proto RPC method, while the
// report's "rpc" field is a human label (e.g. "Doc.Set (Commit)",
// "Lookup (missing)"). The report's "method" field carries the proto method
// name; grpcResult.methodName falls back to "rpc" for checks whose label
// already is the proto method name (Storage, KMS, Secret Manager, Pub/Sub).
type grpcReport struct {
	Results []grpcResult `json:"results"`
}

// grpcResult is one check outcome.
type grpcResult struct {
	Service string `json:"service"`
	Method  string `json:"method"`
	RPC     string `json:"rpc"`
	Status  string `json:"status"`
}

// methodName is the exact proto method under test, falling back to the human
// label when the harness did not record an explicit method.
func (r grpcResult) methodName() string {
	if r.Method != "" {
		return r.Method
	}
	return r.RPC
}

// ReadGRPCReport loads the gRPC conformance report. A missing file yields
// (nil, nil): the matrix then falls back to the default limited classification
// for every gRPC method (no verification), so a fresh clone without the report
// still generates.
func ReadGRPCReport(path string) (*grpcReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read gRPC conformance report %s: %w", path, err)
	}
	var rep grpcReport
	if err := json.Unmarshal(raw, &rep); err != nil {
		return nil, fmt.Errorf("parse gRPC conformance report %s: %w", path, err)
	}
	return &rep, nil
}

// grpcCoverage aggregates every report result for one (service, method).
type grpcCoverage struct {
	passed int
	failed int
	total  int
	label  string // human RPC label of the last matching result, for findings
}

// coverage returns the aggregated conformance results for (service, method).
// A nil report (absent file) covers nothing, so every method stays unverified.
func (r *grpcReport) coverage(service, method string) grpcCoverage {
	var cov grpcCoverage
	if r == nil {
		return cov
	}
	for _, res := range r.Results {
		if res.Service != service || res.methodName() != method {
			continue
		}
		cov.total++
		cov.label = res.RPC
		if res.Status == "pass" {
			cov.passed++
		} else {
			cov.failed++
		}
	}
	return cov
}
