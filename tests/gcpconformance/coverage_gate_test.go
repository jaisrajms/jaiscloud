//go:build gcp_conformance

package gcpconformance

import (
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"testing"
)

// coverageExemption records a Discovery-mapped operation that intentionally has
// no schema-validated response, with the reason. Every exemption must carry a
// written reason: the coverage gate fails on any mapped op that is neither
// covered nor explained.
type coverageExemption struct {
	Operation string `json:"operation"`
	Reason    string `json:"reason"`
}

type coverageExemptionsFile struct {
	Exemptions []coverageExemption `json:"exemptions"`
}

const coverageExemptionsPath = "testdata/coverage-exemptions.json"

func loadCoverageExemptions(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(coverageExemptionsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}
		}
		t.Fatalf("read %s: %v", coverageExemptionsPath, err)
	}
	var f coverageExemptionsFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse %s: %v", coverageExemptionsPath, err)
	}
	out := map[string]string{}
	for _, e := range f.Exemptions {
		if e.Operation == "" || e.Reason == "" {
			t.Fatalf("%s: an exemption is missing operation or reason: %+v", coverageExemptionsPath, e)
		}
		out[e.Operation] = e.Reason
	}
	return out
}

// TestMappedOpCoverage is the AUD2 gate: every emulator operation that maps to a
// Discovery method must have at least one schema-validated transcript response,
// or an explicit, reasoned exemption. Unmapped operations are out of scope here
// (they have no official schema to validate against; the fidelity matrix records
// that as a distinct reason). It runs offline over the committed transcript and
// in CI over the freshly recorded one.
func TestMappedOpCoverage(t *testing.T) {
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
	resolver := NewActionResolver(docs)
	coverage := TranscriptCoverage(ops, docs, tr)
	exempt := loadCoverageExemptions(t)

	byService := map[string][]string{}
	total := 0
	for _, op := range ops {
		method, ok := resolver.Resolve(op)
		if !ok {
			continue // unmapped: out of scope
		}
		if coverage[op.Key()] > 0 {
			continue
		}
		if _, ok := exempt[op.Key()]; ok {
			continue
		}
		byService[op.Service] = append(byService[op.Service], op.Key()+" ["+method+"]")
		total++
	}
	if total == 0 {
		t.Logf("coverage gate: all Discovery-mapped operations have a validated response or an exemption")
		return
	}

	svcs := make([]string, 0, len(byService))
	for s := range byService {
		svcs = append(svcs, s)
	}
	sort.Strings(svcs)
	var msg string
	for _, s := range svcs {
		ops := byService[s]
		sort.Strings(ops)
		msg += "\n  " + s + " (" + strconv.Itoa(len(ops)) + ")"
		for _, o := range ops {
			msg += "\n    " + o
		}
	}
	t.Fatalf("coverage gate: %d Discovery-mapped operation(s) have no validated response and no exemption:%s\n"+
		"(add a probe that records a 2xx response, or an entry to %s with a reason)",
		total, msg, coverageExemptionsPath)
}
