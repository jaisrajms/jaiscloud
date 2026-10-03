//go:build gcp_conformance

package gcpconformance

import (
	"encoding/json"
	"fmt"
	"os"
)

// ReadReport loads the machine-readable wire-conformance report produced by
// WriteReport (testdata/report/report.json by default).
//
// This helper lives in an additive file so the fidelity-matrix generator can
// consume the same recorded evidence as the conformance suite without
// re-running the harness or reaching into test-only code.
func ReadReport(path string) (*Report, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read conformance report %s: %w", path, err)
	}
	var rep Report
	if err := json.Unmarshal(raw, &rep); err != nil {
		return nil, fmt.Errorf("parse conformance report %s: %w", path, err)
	}
	return &rep, nil
}
