//go:build gcp_conformance

package gcpconformance

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

const (
	discoveryDir     = "discovery"
	transcriptPath   = "testdata/transcripts.json"
	defaultReportDir = "testdata/report"
)

func loadDocs(t *testing.T) map[string]*DiscoveryDoc {
	t.Helper()
	docs, err := LoadSnapshots(discoveryDir)
	if err != nil {
		t.Fatalf("LoadSnapshots: %v", err)
	}
	return docs
}

// TestDiscoverySchemas is a sanity check over the vendored snapshots.
func TestDiscoverySchemas(t *testing.T) {
	docs := loadDocs(t)
	if len(docs) == 0 {
		t.Fatal("no discovery documents loaded")
	}
	services := make([]string, 0, len(docs))
	for svc := range docs {
		services = append(services, svc)
	}
	sort.Strings(services)
	for _, svc := range services {
		doc := docs[svc]
		if doc.Name == "" {
			t.Errorf("%s: missing name", svc)
		}
		if doc.Version == "" {
			t.Errorf("%s: missing version", svc)
		}
		methods := 0
		doc.WalkMethods(func(*Method) { methods++ })
		if methods == 0 {
			t.Errorf("%s: no methods", svc)
		}
		t.Logf("%-18s name=%-10s version=%-8s methods=%d schemas=%d",
			svc, doc.Name, doc.Version, methods, len(doc.Schemas))
	}
}

// TestRegistryCoverage maps every enumerated emulator operation to a Discovery
// method id and reports the coverage percentage. Uncovered actions do not fail
// the test — they are reported so gaps stay visible.
func TestRegistryCoverage(t *testing.T) {
	docs := loadDocs(t)
	resolver := NewActionResolver(docs)

	ops := Enumerate()
	if len(ops) == 0 {
		t.Fatal("no operations enumerated")
	}

	var uncovered []string
	byPrefixTotal := map[string]int{}
	byPrefixCovered := map[string]int{}
	for _, op := range ops {
		byPrefixTotal[op.ProviderPrefix]++
		if _, ok := resolver.Resolve(op); ok {
			byPrefixCovered[op.ProviderPrefix]++
			continue
		}
		uncovered = append(uncovered, op.Key())
	}
	sort.Strings(uncovered)

	covered := len(ops) - len(uncovered)
	pct := float64(covered) / float64(len(ops)) * 100
	t.Logf("registry coverage: %d/%d (%.1f%%) discovery-mapped", covered, len(ops), pct)
	t.Logf("uncovered actions: %d", len(uncovered))
	for i, key := range uncovered {
		if i >= 60 {
			t.Logf("  ... %d more", len(uncovered)-i)
			break
		}
		t.Logf("  %s", key)
	}

	prefixes := make([]string, 0, len(byPrefixTotal))
	for p := range byPrefixTotal {
		prefixes = append(prefixes, p)
	}
	sort.Strings(prefixes)
	for _, p := range prefixes {
		t.Logf("  %-18s %d/%d", p, byPrefixCovered[p], byPrefixTotal[p])
	}
}

// TestTranscriptsConform validates committed transcripts against Discovery and
// writes a report. Divergences are reported, never fatal: this is a reporter.
func TestTranscriptsConform(t *testing.T) {
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

	all := ValidateTranscripts(docs, tr)
	divs, suppressed := ApplyAllowlist(all)

	reportDir := defaultReportDir
	if d := os.Getenv("GCP_CONFORMANCE_REPORT_DIR"); d != "" {
		reportDir = d
	}
	if err := WriteReport(reportDir, divs); err != nil {
		t.Errorf("WriteReport: %v", err)
	}

	bySeverity := map[string]int{}
	byKind := map[string]int{}
	for _, d := range divs {
		bySeverity[d.Severity]++
		byKind[d.Kind]++
	}
	t.Logf("validated %d transcript entries -> %d divergences (%d suppressed by allowlist)",
		len(tr.Entries), len(divs), len(suppressed))
	t.Logf("by severity: high=%d medium=%d low=%d info=%d",
		bySeverity["high"], bySeverity["medium"], bySeverity["low"], bySeverity["info"])
	for _, kind := range sortedKeys(byKind, nil) {
		t.Logf("by kind: %-20s %d", kind, byKind[kind])
	}
	t.Logf("report written to %s/report.{json,md}", reportDir)

	// Gate: a high-severity divergence (wrong_type / missing_required) is a
	// real wire-contract violation and fails the build. The allowlist above has
	// dropped the known API-shape false positives, so anything left at "high"
	// is a genuine divergence.
	var high []Divergence
	for _, d := range divs {
		if d.Severity == "high" {
			high = append(high, d)
		}
	}
	for _, d := range high {
		t.Errorf("[high/%s] %s %s: expected %s, got %s", d.Kind, d.Service, d.Path, d.Expected, d.Actual)
	}
	if len(high) > 0 {
		t.Fatalf("conformance gate failed: %d high-severity divergence(s)", len(high))
	}
}

// TestRecord is the -record workflow: it is skipped unless
// GCP_CONFORMANCE_RECORD=1 is set and a live emulator is reachable.
func TestRecord(t *testing.T) {
	if os.Getenv("GCP_CONFORMANCE_RECORD") != "1" {
		t.Skip("set GCP_CONFORMANCE_RECORD=1 to record against a live emulator")
	}
	host := os.Getenv("GCP_CONFORMANCE_ENDPOINT")
	if host == "" {
		host = os.Getenv("JAISCLOUD_GCP_ENDPOINT")
	}
	if host == "" {
		host = "http://localhost:8080"
	}

	tr, err := Record(host)
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(transcriptPath), 0o755); err != nil {
		t.Fatalf("mkdir testdata: %v", err)
	}
	if err := WriteTranscript(transcriptPath, tr); err != nil {
		t.Fatalf("WriteTranscript: %v", err)
	}
	t.Logf("recorded %d entries against %s -> %s", len(tr.Entries), host, transcriptPath)
}
