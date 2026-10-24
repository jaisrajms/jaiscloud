//go:build gcp_conformance

package gcpconformance

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// OpStatuses records the HTTP statuses the committed transcript observes for a
// single registry operation's resolved Discovery method, together with that
// method id ("which check(s) cover it" for REST is "which response(s)").
type OpStatuses struct {
	Method   string
	Statuses map[int]int
	// OperationStubs counts the 501 responses that name the operation itself as
	// unimplemented (an operation-level stub) rather than rejecting the request
	// input. Only these are stub evidence — see isRequestRejection.
	OperationStubs int
}

// Stub reports whether the evidence is a hard, wire-confirmed **operation-level**
// failure: the method named the operation unimplemented at least once and never
// returned 2xx.
//
// Two things are deliberately NOT a stub:
//
//   - a method that also answered 2xx — the operation works and only a subset of
//     inputs is unserved (a conditional rejection);
//   - a 501 that rejects the request input (an unsupported update mask or an
//     undeclared route) — the operation is implemented, the probe simply sent
//     something the emulator does not accept. Those are the coverage ledger's
//     business, not a fidelity over-claim.
func (s OpStatuses) Stub() bool {
	if s.OperationStubs == 0 {
		return false
	}
	return !s.Working()
}

// Working reports whether at least one 2xx response backs the operation.
func (s OpStatuses) Working() bool {
	for code := range s.Statuses {
		if code >= 200 && code < 300 {
			return true
		}
	}
	return false
}

// requestRejectionPrefixes are the phrasings the emulator uses for a 501 that
// rejects the *request* rather than reporting an unimplemented operation: the
// generic JSON codec's unmatched-route error ("unsupported operation") and the
// update-mask merge handlers' unsupported-path error ("unsupported update_mask
// path: <field>"). An explicit operation stub instead names the operation
// ("DiagnoseCluster is not supported by the emulator"). Keeping this list
// explicit (rather than matching a bare "unsupported " prefix, which would also
// swallow real unimplemented-input errors such as BigQuery's "unsupported
// sourceFormat") keeps the gate precise.
var requestRejectionPrefixes = []string{
	"unsupported operation",
	"unsupported update_mask path",
}

// isRequestRejection reports whether a 501 error message is a request-input
// rejection rather than an operation-level unimplemented stub.
func isRequestRejection(message string) bool {
	for _, p := range requestRejectionPrefixes {
		if strings.HasPrefix(message, p) {
			return true
		}
	}
	return false
}

// errorMessage extracts the `error.message` from a recorded 501 body. A body
// that cannot be parsed yields "" (and is treated as an operation stub by the
// caller — the conservative direction for a gate).
func errorMessage(body json.RawMessage) string {
	if len(body) == 0 {
		return ""
	}
	var env struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return ""
	}
	return env.Error.Message
}

// TranscriptOpStatuses joins the emulator's REST operation registry to the
// committed conformance transcript: for every operation that resolves to a
// Discovery method it returns the HTTP statuses observed against that method.
// The result is keyed by Operation.Key(); an operation with no matching
// transcript entry is absent (no evidence either way).
//
// Attribution mirrors TranscriptCoverage: a transcript entry is keyed by its
// (wire service, matched Discovery method id) and then mapped onto every
// registry operation that resolves to the same method. Unlike TranscriptCoverage
// this keeps non-2xx entries, because a recorded 501 is exactly the stub
// evidence this ledger exists to expose. Every entry is matched regardless of
// status — MatchMethodInService is status-independent — and an entry that
// matches no method is dropped (unmatched requests are reported separately by
// the harness).
func TranscriptOpStatuses(ops []Operation, docs map[string]*DiscoveryDoc, tr Transcript) map[string]OpStatuses {
	ix := BuildMethodIndex(docs)
	resolver := NewActionResolver(docs)

	type methodEvidence struct {
		statuses map[int]int
		stubs    int
	}
	byMethod := map[string]*methodEvidence{}
	for _, e := range tr.Entries {
		_, m, ok := ix.MatchMethodInService(e.Service, e.Method, e.Path)
		if !ok {
			continue // unmatched: reported by the transcript-conformance reporter
		}
		k := methodEvidenceKey(e.Service, m.ID)
		ev := byMethod[k]
		if ev == nil {
			ev = &methodEvidence{statuses: map[int]int{}}
			byMethod[k] = ev
		}
		ev.statuses[e.Status]++
		if e.Status == 501 && !isRequestRejection(errorMessage(e.Body)) {
			ev.stubs++
		}
	}

	out := map[string]OpStatuses{}
	for _, op := range ops {
		method, ok := resolver.Resolve(op)
		if !ok {
			continue
		}
		ev := byMethod[methodEvidenceKey(op.Service, method)]
		if ev == nil {
			continue
		}
		out[op.Key()] = OpStatuses{Method: method, Statuses: ev.statuses, OperationStubs: ev.stubs}
	}
	return out
}

// MatrixCellKey is the stable key shared by the fidelity matrix's REST cells and
// the reconciliation's evidence lookup: transport + service + operation.
func MatrixCellKey(transport, service, operation string) string {
	return transport + "|" + service + "|" + operation
}

// ReconcileStubs cross-checks the wire evidence against the matrix's REST
// grades. It returns two sorted finding lists:
//
//   - undeclared: a wire-confirmed operation-level stub the matrix still grades
//     `ga`/`preview` (an over-claim);
//   - stale: a cell graded `unsupported` whose resolved method recorded a 2xx
//     (a declared-unsupported method that actually works).
//
// Matrix cells whose operation has no single Discovery method (the catch-all
// `Unimplemented` actions) and gRPC cells carry no REST transcript evidence, so
// they are out of scope here. It is pure: the same inputs always yield the same
// findings, which is what makes it unit-testable.
func ReconcileStubs(ops []Operation, statuses map[string]OpStatuses, matrix map[string]string) (undeclared, stale []string) {
	opByCell := map[string]Operation{}
	for _, op := range ops {
		opByCell[op.Service+"|"+op.Key()] = op
	}

	for _, op := range ops {
		ev, ok := statuses[op.Key()]
		if !ok {
			continue // no transcript entry for this op: no evidence either way
		}
		state, ok := matrix[MatrixCellKey("rest", op.Service, op.Key())]
		if !ok {
			continue // op resolves to a matrix cell we do not grade (defensive)
		}
		if ev.Stub() && state != "unsupported" && state != "limited" {
			undeclared = append(undeclared, fmt.Sprintf(
				"%s/%s [%s] state=%s statuses=%s",
				op.Service, op.Key(), ev.Method, state, formatStatuses(ev.Statuses)))
		}
	}

	for key, state := range matrix {
		if state != "unsupported" {
			continue
		}
		parts := strings.SplitN(key, "|", 3)
		if len(parts) != 3 || parts[0] != "rest" {
			continue
		}
		op, ok := opByCell[parts[1]+"|"+parts[2]]
		if !ok {
			continue // catch-all action: no single Discovery method (documented)
		}
		ev, ok := statuses[op.Key()]
		if !ok {
			continue // no transcript evidence (e.g. an unmapped stub action)
		}
		if ev.Working() {
			stale = append(stale, fmt.Sprintf(
				"%s/%s [%s] graded unsupported but recorded 2xx statuses=%s",
				op.Service, op.Key(), ev.Method, formatStatuses(ev.Statuses)))
		}
	}

	sort.Strings(undeclared)
	sort.Strings(stale)
	return undeclared, stale
}

func formatStatuses(st map[int]int) string {
	codes := make([]int, 0, len(st))
	for code := range st {
		codes = append(codes, code)
	}
	sort.Ints(codes)
	parts := make([]string, 0, len(codes))
	for _, code := range codes {
		parts = append(parts, fmt.Sprintf("%d×%d", st[code], code))
	}
	return strings.Join(parts, ",")
}
