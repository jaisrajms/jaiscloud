package grpcconformance

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Status is the conformance verdict for a single RPC.
type Status string

const (
	// StatusPass means the RPC returned OK and its key field was present.
	StatusPass Status = "pass"
	// StatusFail means the RPC errored for a reason other than Unimplemented,
	// or returned OK but with the expected key field missing.
	StatusFail Status = "fail"
	// StatusUnimplemented means the RPC returned codes.Unimplemented: a real
	// fidelity gap in the emulator, distinct from a generic failure.
	StatusUnimplemented Status = "unimplemented"
)

// Check is one curated RPC conformance probe against the live emulator.
//
// Checks for a service are ordered create -> get -> list -> delete and share
// state through the emulator (the create leaves the resource in place for the
// following get/list/delete probes). Config.Suffix keeps every name unique per
// run, so the sequence is self-contained and idempotent.
type Check struct {
	// Service is the fidelity service name (storage, kms, secretmanager,
	// pubsub, firestore).
	Service string
	// RPC is the human label for the RPC under test (e.g. CreateBucket,
	// Doc.Set (Commit), Lookup (missing)). It is what the report displays.
	RPC string
	// Method is the exact proto RPC method name (e.g. Commit, Lookup) that the
	// check exercises. It is empty when RPC already IS the proto method name, in
	// which case runOne falls back to RPC. fidelitygen keys cells by proto
	// method, so every check whose RPC label is more descriptive than the wire
	// method must set this.
	Method string
	// KeyField documents the field/predicate the probe asserts beyond a nil
	// error, for the report.
	KeyField string
	// Run performs the RPC and returns nil on success or a non-nil error. A
	// status.Unimplemented error is classified separately by RunAll.
	Run func(ctx context.Context, cfg Config) error
}

// Result is the outcome of one Check.
type Result struct {
	Service  string `json:"service"`
	RPC      string `json:"rpc"`
	Method   string `json:"method,omitempty"`
	Status   Status `json:"status"`
	KeyField string `json:"key_field,omitempty"`
	Error    string `json:"error,omitempty"`
}

// Registry returns the full curated check list in execution order.
func Registry() []Check {
	var checks []Check
	checks = append(checks, storageChecks()...)
	checks = append(checks, kmsChecks()...)
	checks = append(checks, secretManagerChecks()...)
	checks = append(checks, secretManagerExtraChecks()...)
	checks = append(checks, pubSubChecks()...)
	checks = append(checks, pubSubExtraChecks()...)
	checks = append(checks, firestoreChecks()...)
	checks = append(checks, firestoreExtraChecks()...)
	checks = append(checks, firestoreAdminChecks()...)
	checks = append(checks, datastoreChecks()...)
	checks = append(checks, loggingChecks()...)
	checks = append(checks, loggingConfigChecks()...)
	checks = append(checks, loggingMetricsChecks()...)
	checks = append(checks, operationsChecks()...)
	checks = append(checks, monitoringChecks()...)
	checks = append(checks, workflowExecutionsChecks()...)
	checks = append(checks, workflowsChecks()...)
	checks = append(checks, serviceUsageChecks()...)
	checks = append(checks, schedulerChecks()...)
	checks = append(checks, tasksChecks()...)
	checks = append(checks, resourceManagerChecks()...)
	checks = append(checks, managedKafkaChecks()...)
	checks = append(checks, metastoreChecks()...)
	checks = append(checks, eventarcChecks()...)
	checks = append(checks, dataprocChecks()...)
	checks = append(checks, functionsChecks()...)
	checks = append(checks, iamChecks()...)
	checks = append(checks, iamCredentialsChecks()...)
	return checks
}

// RunAll executes every check in order and classifies the outcome. It never
// aborts early: a failing probe does not stop later probes from running.
func RunAll(ctx context.Context, cfg Config, checks []Check) []Result {
	results := make([]Result, 0, len(checks))
	for _, c := range checks {
		results = append(results, runOne(ctx, cfg, c))
	}
	return results
}

func runOne(ctx context.Context, cfg Config, c Check) Result {
	method := c.Method
	if method == "" {
		method = c.RPC
	}
	res := Result{Service: c.Service, RPC: c.RPC, Method: method, KeyField: c.KeyField}
	err := c.Run(ctx, cfg)
	switch {
	case err == nil:
		res.Status = StatusPass
	case status.Code(err) == codes.Unimplemented:
		res.Status = StatusUnimplemented
		res.Error = err.Error()
	default:
		res.Status = StatusFail
		res.Error = err.Error()
	}
	return res
}
