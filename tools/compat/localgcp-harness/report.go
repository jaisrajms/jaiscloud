package main

import (
	"encoding/json"
	"os"
)

// reportPath is where the JSON results are written; override with HARNESS_REPORT.
func reportPath() string { return envOr("HARNESS_REPORT", "results.json") }

func writeReport() error {
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(reportPath(), b, 0644)
}
