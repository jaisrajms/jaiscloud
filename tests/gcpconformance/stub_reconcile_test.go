//go:build gcp_conformance

package gcpconformance

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// fidelityMatrixPath is the committed matrix the emulator's docs publish. The
// reconciliation reads it (rather than the overrides file) because it is the
// artifact users and the ledger actually see: any drift between the wire
// evidence and the graded cells must be visible there.
const fidelityMatrixPath = "../../docs/fidelity/fidelity-matrix.json"

type reconcileMatrixCell struct {
	Service   string `json:"service"`
	Operation string `json:"operation"`
	Transport string `json:"transport"`
	State     string `json:"state"`
}

type reconcileMatrix struct {
	Cells []reconcileMatrixCell `json:"cells"`
}

func loadReconcileMatrix(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(fidelityMatrixPath)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("no fidelity matrix at %s (run `make gen-gcp-fidelity-matrix`)", fidelityMatrixPath)
		}
		t.Fatalf("read %s: %v", fidelityMatrixPath, err)
	}
	var m reconcileMatrix
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse %s: %v", fidelityMatrixPath, err)
	}
	out := make(map[string]string, len(m.Cells))
	for _, c := range m.Cells {
		out[MatrixCellKey(c.Transport, c.Service, c.Operation)] = c.State
	}
	return out
}

// TestUnsupportedStubReconciliation is the AUD5 gate: the matrix's REST cells
// must agree with the wire-confirmed stubs.
//
//   - A registry operation the committed transcript shows returning 501 with no
//     2xx response is a stub/GAP. Its matrix cell must be `unsupported` or
//     `limited` — never `ga`/`preview` (an undeclared stub is a fidelity
//     over-claim).
//   - A matrix `unsupported` REST cell must be backed by 501-only evidence: if
//     the same resolved method ever answered 2xx, the "unsupported" grade is
//     stale (a declared-unsupported method that actually works).
//
// Catch-all `Unimplemented` actions (CloudSQL/CloudDNS/Compute/BigQuery) and
// gRPC methods are out of scope here: they resolve to no single Discovery
// method / carry no REST transcript, so the join has no evidence to compare.
// The gRPC unsupported set is asserted against the official-client report by
// the fidelity generator.
func TestUnsupportedStubReconciliation(t *testing.T) {
	docs := loadDocs(t)

	tr, err := ReadTranscript(transcriptPath)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("no transcript at %s yet (run `make record-gcp-wire-conformance`)", transcriptPath)
		}
		t.Fatalf("ReadTranscript: %v", err)
	}
	if len(tr.Entries) == 0 {
		t.Skip("transcript has no entries")
	}

	ops := Enumerate()
	statuses := TranscriptOpStatuses(ops, docs, tr)
	matrix := loadReconcileMatrix(t)

	undeclared, stale := ReconcileStubs(ops, statuses, matrix)
	if len(undeclared) == 0 && len(stale) == 0 {
		stubs := 0
		for _, ev := range statuses {
			if ev.Stub() {
				stubs++
			}
		}
		t.Logf("stub reconciliation: %d operation-level stub(s) in the transcript are graded unsupported; %d unsupported REST cell(s) checked",
			stubs, countUnsupportedREST(matrix))
		return
	}

	var msg string
	if len(undeclared) > 0 {
		msg += "\nundeclared stub(s) — an operation-level 501-only op graded ga/preview:\n  " +
			strings.Join(undeclared, "\n  ") + "\n"
	}
	if len(stale) > 0 {
		msg += "\nstale unsupported grade(s) — the method serves 2xx:\n  " +
			strings.Join(stale, "\n  ") + "\n"
	}
	t.Fatalf("stub reconciliation failed:%s\n"+
		"(re-grade the cell in docs/fidelity-overrides.yaml, run `make gen-gcp-fidelity-matrix`, and commit docs/fidelity)", msg)
}

func countUnsupportedREST(matrix map[string]string) int {
	n := 0
	for k, s := range matrix {
		if s == "unsupported" && strings.HasPrefix(k, "rest|") {
			n++
		}
	}
	return n
}

// TestOpStatusesStub pins the Stub/Working semantics: an operation-level 501
// that never saw a 2xx is a stub; a method that served any 2xx works (a
// conditional rejection is not a stub); and a 501 with no operation-level stub
// marker is not a stub.
func TestOpStatusesStub(t *testing.T) {
	cases := []struct {
		name    string
		status  map[int]int
		stubs   int
		stub    bool
		working bool
	}{
		{"operation stub only", map[int]int{501: 1}, 1, true, false},
		{"operation stub repeated", map[int]int{501: 3}, 3, true, false},
		{"operation stub then 200", map[int]int{501: 1, 200: 1}, 1, false, true},
		{"200 already seen", map[int]int{200: 2, 501: 1}, 1, false, true},
		{"200 only", map[int]int{200: 1}, 0, false, true},
		{"204 only", map[int]int{204: 9}, 0, false, true},
		{"404 only", map[int]int{404: 1}, 0, false, false},
		{"stub plus 404", map[int]int{501: 1, 404: 1}, 1, true, false},
		{"501 with no stub marker", map[int]int{501: 1}, 0, false, false},
		{"empty", nil, 0, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := OpStatuses{Method: "svc.method", Statuses: tc.status, OperationStubs: tc.stubs}
			if got := s.Stub(); got != tc.stub {
				t.Errorf("Stub() = %v, want %v", got, tc.stub)
			}
			if got := s.Working(); got != tc.working {
				t.Errorf("Working() = %v, want %v", got, tc.working)
			}
		})
	}
}

// TestIsRequestRejection pins the request-rejection classifier: the emulator's
// unmatched-route and unsupported-update-mask 501s are request rejections (not
// stubs), while an operation-level Unimplemented message is not.
func TestIsRequestRejection(t *testing.T) {
	rejected := []string{
		"unsupported operation",
		"unsupported update_mask path: labels",
	}
	for _, m := range rejected {
		if !isRequestRejection(m) {
			t.Errorf("isRequestRejection(%q) = false, want true", m)
		}
	}
	notRejected := []string{
		"DiagnoseCluster is not supported by the emulator",
		"ExportMetadata is not supported by the emulator",
		"operation is not supported by the Cloud SQL emulator",
		"", // empty/unparseable is treated as a stub (conservative)
	}
	for _, m := range notRejected {
		if isRequestRejection(m) {
			t.Errorf("isRequestRejection(%q) = true, want false", m)
		}
	}
}

// TestTranscriptOpStatusesHermetic exercises the join over the synthetic
// Discovery doc (testDocs): statuses accumulate per resolved method, an
// operation-level 501 counts as a stub while a request-rejection 501 does not,
// and an entry matching no method is dropped.
func TestTranscriptOpStatusesHermetic(t *testing.T) {
	ops := []Operation{
		{ProviderPrefix: "Example", Action: "WidgetsGet", Service: "example"},
		{ProviderPrefix: "Example", Action: "WidgetsList", Service: "example"},
	}
	tr := Transcript{Entries: []Entry{
		{Service: "example", Method: "GET", Path: "/v1/widgets/w1", Status: 200},
		{Service: "example", Method: "GET", Path: "/v1/widgets/w1", Status: 501,
			Body: json.RawMessage(`{"error":{"message":"WidgetsGet is not supported by the emulator"}}`)},
		{Service: "example", Method: "GET", Path: "/v1/widgets", Status: 501,
			Body: json.RawMessage(`{"error":{"message":"unsupported update_mask path: labels"}}`)},
		{Service: "example", Method: "GET", Path: "/v1/nope", Status: 200}, // matches no method
	}}

	got := TranscriptOpStatuses(ops, testDocs(), tr)

	get := got["Example.WidgetsGet"]
	if !get.Working() || get.Stub() || get.OperationStubs != 1 {
		t.Errorf("WidgetsGet: Working=%v Stub=%v stubs=%d, want true/false/1",
			get.Working(), get.Stub(), get.OperationStubs)
	}
	list := got["Example.WidgetsList"]
	if list.Working() || list.Stub() || list.OperationStubs != 0 {
		t.Errorf("WidgetsList: Working=%v Stub=%v stubs=%d, want false/false/0 (request rejection only)",
			list.Working(), list.Stub(), list.OperationStubs)
	}
}

// TestTranscriptOpStatusesEvidence exercises the registry↔transcript join over
// the committed data: a declared stub reads as 501-only, and a working
// operation is never flagged.
func TestTranscriptOpStatusesEvidence(t *testing.T) {
	docs := loadDocs(t)
	tr, err := ReadTranscript(transcriptPath)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("no transcript at %s yet (run `make record-gcp-wire-conformance`)", transcriptPath)
		}
		t.Fatalf("ReadTranscript: %v", err)
	}
	if len(tr.Entries) == 0 {
		t.Skip("transcript has no entries")
	}

	statuses := TranscriptOpStatuses(Enumerate(), docs, tr)

	// Dataproc DiagnoseCluster is a declared explicit stub: the transcript shows
	// it answering 501 and never 2xx.
	ev, ok := statuses["Dataproc.DiagnoseCluster"]
	if !ok {
		t.Fatalf("no evidence for Dataproc.DiagnoseCluster (join missed the stub)")
	}
	if !ev.Stub() {
		t.Errorf("Dataproc.DiagnoseCluster: Stub() = false, statuses=%v (want 501-only)", ev.Statuses)
	}

	// A working operation must never be reported as a stub.
	for key, ev := range statuses {
		if !ev.Working() {
			continue
		}
		if ev.Stub() {
			t.Errorf("%s: both Working and Stub (statuses=%v)", key, ev.Statuses)
		}
	}
}

// TestReconcileStubsSeededDrift proves the gate fails on both drift directions
// with hand-built inputs, so the check is trusted without a live emulator.
func TestReconcileStubsSeededDrift(t *testing.T) {
	ops := []Operation{
		{ProviderPrefix: "Dataproc", Action: "DiagnoseCluster", Service: "dataproc"},
		{ProviderPrefix: "PubSub", Action: "TopicsGet", Service: "pubsub"},
	}
	statuses := map[string]OpStatuses{
		"Dataproc.DiagnoseCluster": {Method: "dataproc.projects.regions.clusters.diagnose",
			Statuses: map[int]int{501: 1}, OperationStubs: 1},
		"PubSub.TopicsGet": {Method: "pubsub.projects.topics.get",
			Statuses: map[int]int{200: 1}},
	}

	// A 501-only op graded ga is an undeclared stub.
	undeclared, stale := ReconcileStubs(ops, statuses, map[string]string{
		MatrixCellKey("rest", "dataproc", "Dataproc.DiagnoseCluster"): "ga",
	})
	if len(undeclared) != 1 || len(stale) != 0 {
		t.Fatalf("ga stub: undeclared=%v stale=%v, want 1 undeclared", undeclared, stale)
	}

	// Grading it unsupported or limited clears the finding.
	for _, good := range []string{"unsupported", "limited"} {
		u, s := ReconcileStubs(ops, statuses, map[string]string{
			MatrixCellKey("rest", "dataproc", "Dataproc.DiagnoseCluster"): good,
		})
		if len(u) != 0 || len(s) != 0 {
			t.Errorf("state=%s: undeclared=%v stale=%v, want none", good, u, s)
		}
	}

	// An unsupported cell whose method serves 2xx is stale.
	undeclared, stale = ReconcileStubs(ops, statuses, map[string]string{
		MatrixCellKey("rest", "pubsub", "PubSub.TopicsGet"): "unsupported",
	})
	if len(undeclared) != 0 || len(stale) != 1 {
		t.Fatalf("stale unsupported: undeclared=%v stale=%v, want 1 stale", undeclared, stale)
	}
}
