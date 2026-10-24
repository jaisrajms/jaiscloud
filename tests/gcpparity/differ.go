//go:build gcp_parity

package gcpparity

import (
	"encoding/json"
	"os"
	"sort"
	"strings"

	"jaiscloud/tests/gcpdifferential"
)

// Finding is one cross-transport parity divergence, tagged with whether it is a
// gate failure or an informational note and whether an allowance accepted it.
type Finding struct {
	Service  string `json:"service"`
	Op       string `json:"op"`
	Kind     string `json:"kind"`
	Severity string `json:"severity"`
	Location string `json:"location"`
	Expected string `json:"expected,omitempty"` // REST side
	Actual   string `json:"actual,omitempty"`   // gRPC side
	Allowed  bool   `json:"allowed"`
	Reason   string `json:"reason,omitempty"`
}

// Allowance accepts a known, real-GCP-grounded REST/gRPC representation
// difference. Every allowance must carry a written reason: the gate fails on
// any divergence that is neither a clean comparison nor an explained exemption.
type Allowance struct {
	Service string `json:"service"`
	Op      string `json:"op"`
	// Path is a JSON-path prefix into the normalized response (e.g.
	// "response.lifecycleState"). Empty matches any location within the op.
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type allowanceFile struct {
	Allowances []Allowance `json:"allowances"`
}

const allowancePath = "testdata/parity-allowlist.json"

// loadAllowances reads the committed allowance file. A missing file is not an
// error (a first cut may have nothing to allow yet); a malformed entry is.
func loadAllowances() ([]Allowance, error) {
	raw, err := os.ReadFile(allowancePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var f allowanceFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, err
	}
	for _, a := range f.Allowances {
		if a.Service == "" || a.Op == "" || a.Reason == "" {
			return nil, errAllowanceMissingField
		}
	}
	return f.Allowances, nil
}

var errAllowanceMissingField = &allowanceError{"parity-allowlist.json: every allowance needs service, op and reason"}

type allowanceError struct{ msg string }

func (e *allowanceError) Error() string { return e.msg }

// compareNormalized diffs the normalized REST body (golden) against the
// normalized gRPC body (actual) using the differential engine, then applies the
// allowance list. The comparison is symmetric: a field only one transport has
// (missing on the other) and a different value/type both gate, unless an
// allowance documents the difference as a real-GCP-grounded representation fact.
func compareNormalized(service, op string, rest, grpc json.RawMessage, allowances []Allowance) []Finding {
	golden := gcpdifferential.Exchange{
		Service: service, Op: op, Method: "PARITY", Path: op, Status: 200, Response: rest,
	}
	actual := gcpdifferential.Exchange{
		Service: service, Op: op, Method: "PARITY", Path: op, Status: 200, Response: grpc,
	}
	divs := gcpdifferential.DiffExchanges(golden, actual)

	findings := make([]Finding, 0, len(divs))
	for _, d := range divs {
		f := Finding{
			Service:  service,
			Op:       op,
			Kind:     d.Kind,
			Severity: d.Severity,
			Location: d.Location,
			Expected: d.Expected,
			Actual:   d.Actual,
		}
		if reason, ok := allowed(allowances, service, op, d.Location); ok {
			f.Allowed = true
			f.Reason = reason
		}
		findings = append(findings, f)
	}
	return findings
}

// allowed reports whether an allowance covers a divergence location.
func allowed(allowances []Allowance, service, op, location string) (string, bool) {
	for _, a := range allowances {
		if a.Service != service || a.Op != op {
			continue
		}
		if a.Path == "" || location == a.Path || strings.HasPrefix(location, a.Path+".") ||
			strings.HasPrefix(location, a.Path+"[") {
			return a.Reason, true
		}
	}
	return "", false
}

// failing reports whether a finding must fail the gate. Every divergence kind
// gates (the comparison is symmetric: a field only the REST body has and one
// only the gRPC body has are both failures) unless an allowance accepted it.
// extra_field/value differences that are a legitimate transport-specific
// representation must be recorded as an allowance with a written reason.
func (f Finding) failing() bool {
	return !f.Allowed
}

// summarize counts findings by kind for a report.
func summarize(findings []Finding) map[string]int {
	out := map[string]int{}
	for _, f := range findings {
		out[f.Kind]++
	}
	return out
}

func sortedFindingKinds(m map[string]int) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
