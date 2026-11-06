//go:build gcp_parity && gcp_conformance

package gcpparity

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestGeneratedFieldRoundTrip drives the generated dataset against a live
// emulator: every pilot resource, both transports, every roundtrip field set to
// a non-default value and read back, plus the per-field update, the negative
// write, the absence create and the delete. A finding not recorded in
// testdata/fieldcoverage-baseline.json fails; a baseline entry that stops
// failing also fails (the baseline is a ratchet).
func TestGeneratedFieldRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping generated field round-trip in -short mode")
	}
	if os.Getenv("GCP_PARITY_SKIP") == "1" {
		t.Skip("GCP_PARITY_SKIP=1")
	}
	cfg := ConfigFromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := probeREST(ctx, cfg.REST); err != nil {
		t.Skipf("emulator REST endpoint %s not reachable: %v", cfg.REST, err)
	}
	if err := probeGRPC(ctx, cfg.GRPCAddr()); err != nil {
		t.Skipf("emulator gRPC endpoint %s not reachable: %v", cfg.GRPC, err)
	}
	env, err := NewEnv(ctx, cfg)
	if err != nil {
		t.Fatalf("new env: %v", err)
	}
	defer env.Close()

	baseline, err := loadFieldBaseline(fieldBaselinePath())
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}

	var findings []fieldFinding
	for _, r := range fieldResources() {
		refs, err := r.prepare(ctx, env)
		if err != nil {
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: "", band: bandLifecycle, field: "", detail: "prepare: " + err.Error()})
			continue
		}
		for _, transport := range r.transports() {
			findings = append(findings, runFieldCoverage(ctx, t, env, r, transport, refs)...)
		}
	}

	gate, accepted, stale := applyFieldBaseline(findings, baseline)
	t.Logf("fieldcoverage: %d assertion finding(s) (%d accepted as recorded gaps, %d gating)", len(findings), accepted, len(gate))
	for _, f := range gate {
		t.Errorf("field coverage [%s] %s", f.key(), f.String())
	}
	for _, k := range stale {
		t.Errorf("stale baseline entry %s: the gap no longer reproduces, remove it from %s", k, fieldBaselinePath())
	}
}

// TestGeneratedFieldCoverageComplete is the catalog↔scenario contract, offline:
// every classified row that needs a scenario has one, every generated value is
// non-default (a zero value would be invisible to the parity normalizer), and
// every generated resource has a wire adapter.
func TestGeneratedFieldCoverageComplete(t *testing.T) {
	rows := readFieldCoverageCSV(t)
	generated := generatedFieldKeys()
	attempts := map[string]bool{}
	for _, a := range generatedFieldWriteAttempts {
		attempts[a.Service+"/"+a.Transport+"/"+a.Resource+"/"+a.Field] = true
	}
	drivers := map[string]fieldResource{}
	for _, r := range fieldResources() {
		drivers[r.resourceID()] = r
	}

	var missing, unadapted, zero []string
	resources := map[string]bool{}
	for _, r := range rows {
		switch r[5] {
		case "roundtrip":
			resources[r[0]+"/"+r[2]] = true
			key := strings.Join([]string{r[0], r[1], r[2], r[3]}, "/")
			if !generated[key] {
				missing = append(missing, key)
			}
		case "output-only", "immutable":
			key := strings.Join([]string{r[0], r[1], r[2], r[3]}, "/")
			if !attempts[key] {
				missing = append(missing, key)
			}
		}
	}
	for _, f := range generatedFieldValues {
		if _, ok := drivers[f.Service+"/"+f.Resource]; !ok {
			unadapted = append(unadapted, f.Service+"/"+f.Resource)
		}
		if f.Kind == "message" || f.Ref != "" {
			continue
		}
		if isZeroValueLiteral(f.Value) {
			zero = append(zero, f.Service+"/"+f.Transport+"/"+f.Resource+"/"+f.Field+" = "+f.Value)
		}
	}
	for id := range resources {
		if _, ok := drivers[id]; !ok {
			unadapted = append(unadapted, id)
		}
	}
	if len(missing) > 0 {
		t.Errorf("classified rows with no generated scenario: %v", missing)
	}
	if len(unadapted) > 0 {
		t.Errorf("catalog resources with no wire adapter in fieldResources(): %v", dedupeStrings(unadapted))
	}
	if len(zero) > 0 {
		t.Errorf("generated create values must be non-default (a zero is dropped by the normalizer): %v", zero)
	}
	// Every pilot resource must be driven on every transport the catalog
	// classifies it for.
	classifiedTransports := map[string]map[string]bool{}
	for _, r := range rows {
		if r[5] == "" {
			continue
		}
		id := r[0] + "/" + r[2]
		if classifiedTransports[id] == nil {
			classifiedTransports[id] = map[string]bool{}
		}
		classifiedTransports[id][r[1]] = true
	}
	for id, want := range classifiedTransports {
		r, ok := drivers[id]
		if !ok {
			continue
		}
		got := map[string]bool{}
		for _, tr := range r.transports() {
			got[tr] = true
		}
		for tr := range want {
			if !got[tr] {
				t.Errorf("%s is classified on %s but the driver does not drive that transport", id, tr)
			}
		}
	}
}

// TestGeneratedFieldCoverageFailsOnPreFixMapping proves the assertions catch the
// TGM1-3 defect: the storage.v2 mapper served bucket CRUD but ignored website,
// logging, autoclass and ip_filter, and every gate stayed green. The fixture
// pins that omission; running the generated assertions over the pinned body must
// report exactly the fields those four blocks carry, and over the complete body
// report nothing.
//
// Refs: TGM1-3, commit 165c9927.
func TestGeneratedFieldCoverageFailsOnPreFixMapping(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "fieldcoverage", "tgm1-3-prefix-omissions.json"))
	if err != nil {
		t.Fatalf("read pre-fix fixture: %v", err)
	}
	var fixture struct {
		Comment string   `json:"_comment"`
		Blocks  []string `json:"omitted_blocks"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse pre-fix fixture: %v", err)
	}
	if len(fixture.Blocks) == 0 {
		t.Fatal("pre-fix fixture names no omitted blocks")
	}

	b := newFieldBody("storage", "Bucket", "grpc", map[string]string{
		"kms": "projects/fc-project/locations/us/keyRings/fc-ring/cryptoKeys/fc-key",
	})
	if err := b.placeAll(); err != nil {
		t.Fatalf("place generated bucket fields: %v", err)
	}
	complete := deepCopyMap(b.body)

	preFix := deepCopyMap(b.body)
	for _, block := range fixture.Blocks {
		delete(preFix, block)
	}

	if findings := assertServed("storage/Bucket", "grpc", b, complete); len(findings) != 0 {
		t.Fatalf("the complete body must satisfy every generated assertion, got %d: %v", len(findings), findings)
	}
	findings := assertServed("storage/Bucket", "grpc", b, preFix)
	if len(findings) == 0 {
		t.Fatal("the pre-fix TGM1-3 body passed the generated assertions: the suite would not have caught the defect")
	}
	// Exactly the fields under the omitted blocks must be reported.
	wantMissing := 0
	for _, f := range b.fields {
		if f.Kind == "message" {
			continue
		}
		for _, block := range fixture.Blocks {
			if strings.HasPrefix(f.Path, block+".") || f.Path == block {
				wantMissing++
				break
			}
		}
	}
	if len(findings) != wantMissing {
		t.Fatalf("pre-fix assertions reported %d finding(s), want %d (one per field under the omitted blocks): %v", len(findings), wantMissing, findings)
	}
	for _, f := range findings {
		if f.band != bandServe {
			t.Fatalf("pre-fix finding has band %q, want %q", f.band, bandServe)
		}
	}
	t.Logf("pre-fix TGM1-3 mapping fails %d generated field assertion(s): %s", len(findings), fixture.Comment)
}

// TestFieldBaselineIsWellFormed keeps the recorded gaps reviewable offline: a
// loopback entry needs an ID, a reason and a future review date.
func TestFieldBaselineIsWellFormed(t *testing.T) {
	baseline, err := loadFieldBaseline(fieldBaselinePath())
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}
	today := time.Now().UTC().Truncate(24 * time.Hour)
	for _, e := range baseline {
		if !strings.HasPrefix(e.ID, "FC3-") {
			t.Errorf("baseline entry %s has id %q; use an FC3-n deferral id", e.key(), e.ID)
		}
		d, err := time.Parse("2006-01-02", e.ReviewBy)
		if err != nil {
			t.Errorf("baseline entry %s has review_by %q (want YYYY-MM-DD)", e.key(), e.ReviewBy)
			continue
		}
		if d.Before(today) {
			t.Errorf("baseline entry %s is overdue for review (review_by %s)", e.key(), e.ReviewBy)
		}
	}
}

func fieldBaselinePath() string {
	return filepath.Join("testdata", "fieldcoverage-baseline.json")
}

// readFieldCoverageCSV reads the committed catalog (service,transport,resource,
// field,behavior,coverage,evidence,reason,review_by).
func readFieldCoverageCSV(t *testing.T) [][]string {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "..", "docs", "fidelity", "field-coverage.csv"))
	if err != nil {
		t.Fatalf("read field-coverage catalog: %v", err)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("parse field-coverage catalog: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("field-coverage catalog is empty")
	}
	return records[1:]
}

func isZeroValueLiteral(raw string) bool {
	switch strings.TrimSpace(raw) {
	case "", "false", "0", `""`, "[]", "{}", "null":
		return true
	}
	return false
}

func deepCopyMap(in map[string]any) map[string]any {
	raw, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		panic(err)
	}
	return out
}

func dedupeStrings(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}
