//go:build gcp_conformance

package main

import (
	"sort"

	conf "jaiscloud/tests/gcpconformance"
)

// grpcDefaultReason is the reason attached to every gRPC cell that has no
// curated override. Per the plan (T4), gRPC starts at "limited": the emulator
// implements the RPCs, but there is no proto-driven conformance harness yet, so
// the cells are verified against proto descriptors only.
const grpcDefaultReason = "gRPC verified against proto descriptors only (pending proto-conformance)"

// grpcDefaultOverride is the classification injected when the overrides file
// says nothing about a gRPC cell. It is a downgrade (limited <= ga), so
// Classify accepts it without allow_upgrade.
func grpcDefaultOverride() *Override {
	return &Override{State: StateLimited, Reason: grpcDefaultReason}
}

// GRPCFacts builds one Facts per gRPC method from the enumerated service
// surface (conf.EnumerateGRPC), the recorded gRPC conformance report, and the
// curated overrides (ov).
//
// Every method yields exactly one fact with Transport "grpc" and Implemented
// true. DiscoveryMethod is always empty: Discovery documents describe REST, not
// gRPC. PersistentBackend and Mutating reuse the REST derivation
// (persistentBackends and isMutating); a PascalCase RPC method name splits into
// the same CamelCase words, so "GetObject" is not mistaken for a "Set".
//
// Conformance resolution for (service, method):
//
//   - No report coverage: attach the default limited override (unverified, the
//     pre-conformance-suite behaviour).
//   - >=1 result and all pass: inject no override, so deriveState yields "ga";
//     record the pass/total evidence on the fact.
//   - any fail/unimplemented: attach the default limited override and a
//     non-allowlisted high-severity finding, so deriveState downgrades with a
//     wire-divergence reason.
//
// Report (service, method) pairs that don't correspond to an enumerated method
// are never consulted (coverage is only queried per enumerated method).
//
// Override resolution: if the overrides file has an entry for (service, method)
// it wins verbatim (including allow_upgrade), overriding both the default and
// the verified no-override case.
//
// nil ov and nil report are tolerated (no overrides / report absent), yielding
// the default limited cells, which keeps the function testable without I/O.
func GRPCFacts(services []conf.GRPCService, ov *Overrides, report *grpcReport) []Facts {
	var facts []Facts
	// Two wire services may belong to the same fidelity service and share RPC
	// method names (Cloud Functions v1 CloudFunctionsService and v2
	// FunctionService both declare GetFunction/CreateFunction/...). The matrix
	// keys a cell by (service, operation, transport), so emit one fact per
	// unique (fidelity service, method) and let the conformance report coverage
	// aggregate across the wire services behind it.
	seen := map[string]bool{}
	for _, svc := range services {
		for _, method := range svc.Methods {
			key := svc.Service + "/" + method
			if seen[key] {
				continue
			}
			seen[key] = true
			cov := report.coverage(svc.Service, method)

			var override *Override
			var findings []Finding
			var passed, total int
			switch {
			case cov.total > 0 && cov.failed == 0:
				// Verified against the official client: leave override nil so the
				// derived state is ga (mutating cells still need a backend).
				passed, total = cov.passed, cov.total
			case cov.total > 0:
				findings = append(findings, Finding{
					Severity: "high",
					Kind:     "grpc_conformance",
					Path:     cov.label,
				})
				total = cov.total
				override = grpcDefaultOverride()
			default:
				override = grpcDefaultOverride()
			}

			if ov != nil {
				if o := ov.For(svc.Service, method); o != nil {
					override = o
				}
			}
			facts = append(facts, Facts{
				Service:           svc.Service,
				Operation:         method,
				Transport:         "grpc",
				Implemented:       true,
				DiscoveryMethod:   "",
				PersistentBackend: persistentBackends[svc.Service],
				Mutating:          isMutating(method),
				Findings:          findings,
				Override:          override,
				GRPCChecksPassed:  passed,
				GRPCChecksTotal:   total,
			})
		}
	}
	return facts
}

// GRPCOnlyServices returns the sorted set of services that appear in the gRPC
// facts but have no operation in the REST registry — i.e. they can only be
// reached over gRPC. T5 renders those services' REST transport as
// "unsupported". No fake REST operations are synthesized here; the caller
// generates the unsupported cells from this list.
func GRPCOnlyServices(ops []conf.Operation, grpcFacts []Facts) []string {
	rest := make(map[string]bool, len(ops))
	for _, op := range ops {
		if op.Service != "" {
			rest[op.Service] = true
		}
	}

	seen := map[string]bool{}
	var only []string
	for _, f := range grpcFacts {
		if f.Service == "" || rest[f.Service] || seen[f.Service] {
			continue
		}
		seen[f.Service] = true
		only = append(only, f.Service)
	}
	sort.Strings(only)
	return only
}
