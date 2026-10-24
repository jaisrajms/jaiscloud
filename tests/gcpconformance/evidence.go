//go:build gcp_conformance

package gcpconformance

// methodEvidenceKey keys a (wire service, Discovery method id) pair. The NUL
// separator cannot occur in either component.
func methodEvidenceKey(service, method string) string { return service + "\x00" + method }

// TranscriptCoverage joins the emulator's REST operation registry to the
// committed conformance transcript: for every registry operation it counts the
// recorded 2xx responses whose matched Discovery method equals the operation's
// resolved method. The result is keyed by Operation.Key() and omits every
// operation with zero coverage.
//
// A mapped operation with no matching 2xx entry is therefore absent from the
// map — that is the "ga without a validated response" gap this ledger exists to
// expose. An operation that resolves to no Discovery method can never be
// covered: there is no official schema to validate against (the matrix already
// records that as a distinct `limited` reason).
//
// The join is offline and deterministic: it reads only the registry, the
// vendored Discovery snapshots and the transcript, and performs no schema
// validation itself. Whether a covered response actually validated clean is a
// separate signal (the conformance report's non-allowlisted divergences); the
// fidelity generator combines the two.
func TranscriptCoverage(ops []Operation, docs map[string]*DiscoveryDoc, tr Transcript) map[string]int {
	ix := BuildMethodIndex(docs)
	resolver := NewActionResolver(docs)

	covered := map[string]int{}
	for _, e := range tr.Entries {
		if e.Status < 200 || e.Status >= 300 {
			continue // only a successful response carries a validatable shape
		}
		_, m, ok := ix.MatchMethodInService(e.Service, e.Method, e.Path)
		if !ok {
			continue // unmatched requests are reported separately by the harness
		}
		covered[methodEvidenceKey(e.Service, m.ID)]++
	}

	out := make(map[string]int, len(ops))
	for _, op := range ops {
		method, ok := resolver.Resolve(op)
		if !ok {
			continue
		}
		if n := covered[methodEvidenceKey(op.Service, method)]; n > 0 {
			out[op.Key()] = n
		}
	}
	return out
}
