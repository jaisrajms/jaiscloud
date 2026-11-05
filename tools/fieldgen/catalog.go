//go:build gcp_conformance

package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Coverage classes. Each definition field on each transport must carry exactly
// one:
//
//	roundtrip    the value is accepted, stored and served back on that transport
//	derived      the value is computed by the emulator from other state
//	immutable    accepted at create, rejected/ignored on update (negative test)
//	output-only  server-populated, ignored when supplied by the caller
//	unsupported  not modelled; a written reason and a review date are required
const (
	CoverageRoundtrip   = "roundtrip"
	CoverageDerived     = "derived"
	CoverageImmutable   = "immutable"
	CoverageOutputOnly  = "output-only"
	CoverageUnsupported = "unsupported"
)

var coverageClasses = map[string]bool{
	CoverageRoundtrip:   true,
	CoverageDerived:     true,
	CoverageImmutable:   true,
	CoverageOutputOnly:  true,
	CoverageUnsupported: true,
}

// csvHeader is the committed column order. Do not reorder: reviewers diff it.
var csvHeader = []string{"service", "transport", "resource", "field", "behavior", "coverage", "evidence", "reason", "review_by"}

// Row is one line of docs/fidelity/field-coverage.csv: a definition field on
// one transport plus the emulator's classification of it.
type Row struct {
	Service   string
	Transport string
	Resource  string
	Field     string
	Behavior  string // from the API definition (proto field_behavior / Discovery readOnly)
	Coverage  string
	Evidence  string // repo path#TestName pointers, ';'-separated
	Reason    string // derived formula, unsupported explanation, ...
	ReviewBy  string // YYYY-MM-DD, required for unsupported
}

// RowKey is the stable identity of an inventory row.
func RowKey(service, transport, resource, field string) string {
	return strings.Join([]string{service, transport, resource, field}, "/")
}

func (r Row) Key() string { return RowKey(r.Service, r.Transport, r.Resource, r.Field) }

// ReadCSV loads the committed inventory. Order is irrelevant to the gate;
// generation writes it sorted.
func ReadCSV(path string) ([]Row, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%s does not exist: run `make gen-gcp-field-coverage` to scaffold it", path)
		}
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%s is empty", path)
	}
	if got := records[0]; strings.Join(got, ",") != strings.Join(csvHeader, ",") {
		return nil, fmt.Errorf("%s: unexpected header %q (want %q)", path, strings.Join(got, ","), strings.Join(csvHeader, ","))
	}
	rows := make([]Row, 0, len(records)-1)
	for i, rec := range records[1:] {
		if len(rec) != len(csvHeader) {
			return nil, fmt.Errorf("%s:%d: %d columns (want %d)", path, i+2, len(rec), len(csvHeader))
		}
		rows = append(rows, Row{
			Service: rec[0], Transport: rec[1], Resource: rec[2], Field: rec[3],
			Behavior: rec[4], Coverage: rec[5], Evidence: rec[6], Reason: rec[7], ReviewBy: rec[8],
		})
	}
	return rows, nil
}

func writeCSV(path string, rows []Row) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".field-coverage-*.csv")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	w := csv.NewWriter(tmp)
	if err := w.Write(csvHeader); err != nil {
		tmp.Close()
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{r.Service, r.Transport, r.Resource, r.Field, r.Behavior, r.Coverage, r.Evidence, r.Reason, r.ReviewBy}); err != nil {
			tmp.Close()
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// Generate refreshes the CSV from the definition universe. It preserves every
// classification already recorded for a field that still exists, refreshes the
// definition-derived behavior, scaffolds new fields with no coverage (so the
// check fails until they are classified), and keeps rows for fields that no
// longer exist so their classification is not silently lost — the check reports
// them as stale.
func Generate(path string, universe *Universe) error {
	var existing []Row
	if _, statErr := os.Stat(path); statErr == nil {
		var err error
		existing, err = ReadCSV(path)
		if err != nil {
			return err
		}
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("stat %s: %w", path, statErr)
	}
	byKey := make(map[string]Row, len(existing))
	for _, r := range existing {
		byKey[r.Key()] = r
	}

	inUniverse := make(map[string]bool, len(universe.Fields))
	out := make([]Row, 0, len(universe.Fields)+len(existing))
	added := 0
	for _, f := range universe.Fields {
		inUniverse[f.Key()] = true
		row, ok := byKey[f.Key()]
		if !ok {
			added++
		}
		row.Service, row.Transport, row.Resource, row.Field = f.Service, f.Transport, f.Resource, f.Name
		row.Behavior = f.Behavior
		out = append(out, row)
	}

	stale := 0
	for _, r := range existing {
		if inUniverse[r.Key()] {
			continue
		}
		stale++
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })

	if err := writeCSV(path, out); err != nil {
		return err
	}
	fmt.Printf("field-coverage: wrote %s — %d field(s), %d newly scaffolded, %d stale row(s) preserved\n",
		path, len(universe.Fields), added, stale)
	if added > 0 {
		fmt.Println("field-coverage: scaffolded rows have no coverage yet; classify them (the check fails until then)")
	}
	if stale > 0 {
		fmt.Println("field-coverage: stale rows are for fields the definitions no longer declare; re-map or delete them")
	}
	return nil
}

// Exemptions waive only the evidence requirement: a field whose behaviour is
// known but whose field-level assertion is scheduled for a later wave (FC3).
// A reason is mandatory, and an exemption for a nonexistent row is itself an
// error. Unsupported rows can never be exempted from reason/review_by.
type Exemptions map[string]string

func LoadExemptions(path string) (Exemptions, error) {
	out := Exemptions{}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var f struct {
		Exemptions []struct {
			Field  string `json:"field"`
			Reason string `json:"reason"`
		} `json:"exemptions"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	for i, e := range f.Exemptions {
		if e.Field == "" || e.Reason == "" {
			return nil, fmt.Errorf("%s: exemption %d is missing field or reason", path, i+1)
		}
		if _, dup := out[e.Field]; dup {
			return nil, fmt.Errorf("%s: duplicate exemption for %s", path, e.Field)
		}
		out[e.Field] = e.Reason
	}
	return out, nil
}

// Report summarises a passing check.
type Report struct {
	Total      int
	Resources  int
	Exempt     int
	ByCoverage map[string]int
	ByService  map[string]int
}

// Check validates the committed rows against the freshly derived universe and
// each row's classification contract:
//
//   - every definition field has exactly one row                      (missing)
//   - every row belongs to a still-defined field                      (stale)
//   - the recorded behavior matches the definition                    (drift)
//   - coverage is one of the five classes                             (invalid)
//   - roundtrip/immutable rows carry an evidence pointer at an
//     existing repo file, unless exempted                             (evidence)
//   - output-only rows carry an evidence pointer or a reason why the
//     emulator never populates the member                             (reason)
//   - derived rows name their formula in reason                       (reason)
//   - unsupported rows carry a reason and a review_by date            (reason)
func Check(universe *Universe, rows []Row, exemptions Exemptions) (Report, error) {
	return CheckIn(".", universe, rows, exemptions)
}

// CheckIn is Check with an explicit repository root for evidence-pointer
// resolution (tests point it at a temp dir).
func CheckIn(root string, universe *Universe, rows []Row, exemptions Exemptions) (Report, error) {
	rep := Report{ByCoverage: map[string]int{}, ByService: map[string]int{}}
	byKey := make(map[string]Row, len(rows))
	var problems []string

	for _, r := range rows {
		k := r.Key()
		if prev, dup := byKey[k]; dup {
			problems = append(problems, fmt.Sprintf("duplicate row %s (coverage %q and %q)", k, prev.Coverage, r.Coverage))
			continue
		}
		byKey[k] = r
	}

	universeKeys := make(map[string]bool, len(universe.Fields))
	for _, f := range universe.Fields {
		k := f.Key()
		universeKeys[k] = true
		row, ok := byKey[k]
		if !ok {
			problems = append(problems, fmt.Sprintf("missing row %s (in the API definition, absent from the CSV — run `make gen-gcp-field-coverage`)", k))
			continue
		}
		if row.Coverage == "" {
			problems = append(problems, fmt.Sprintf("unclassified row %s (definition field has no coverage class yet)", k))
			continue
		}
		if row.Behavior != f.Behavior {
			problems = append(problems, fmt.Sprintf("behavior drift %s: CSV says %q, definition says %q (run `make gen-gcp-field-coverage`)", k, row.Behavior, f.Behavior))
		}
		if p := validateRow(root, row, exemptions); p != "" {
			problems = append(problems, p)
			continue
		}
		rep.ByCoverage[row.Coverage]++
		rep.ByService[row.Service]++
		if _, ok := exemptions[k]; ok {
			rep.Exempt++
		}
	}

	for _, r := range rows {
		if !universeKeys[r.Key()] {
			problems = append(problems, fmt.Sprintf("stale row %s: the API definition has no such field (re-map or delete it)", r.Key()))
		}
	}
	for k := range exemptions {
		if _, ok := byKey[k]; !ok {
			problems = append(problems, fmt.Sprintf("stale exemption %s: no such row in %s", k, "docs/fidelity/field-coverage.csv"))
		}
	}

	resources := map[string]bool{}
	for _, f := range universe.Fields {
		resources[f.Service+"/"+f.Resource] = true
	}
	rep.Total = len(universe.Fields)
	rep.Resources = len(resources)

	if len(problems) > 0 {
		sort.Strings(problems)
		const limit = 40
		shown := problems
		if len(shown) > limit {
			shown = shown[:limit]
		}
		return rep, fmt.Errorf("field-coverage gate: %d problem(s):\n  %s",
			len(problems), strings.Join(shown, "\n  "))
	}
	return rep, nil
}

// validateRow enforces the per-classification contract. It returns "" when the
// row is valid.
func validateRow(root string, r Row, exemptions Exemptions) string {
	key := r.Key()
	if !coverageClasses[r.Coverage] {
		return fmt.Sprintf("invalid coverage %q on %s (want roundtrip|derived|immutable|output-only|unsupported)", r.Coverage, key)
	}
	_, exempt := exemptions[key]
	switch r.Coverage {
	case CoverageRoundtrip:
		if !exempt {
			if p := checkEvidence(root, key, r.Coverage, r.Evidence); p != "" {
				return p
			}
		}
	case CoverageImmutable:
		if !exempt {
			if p := checkEvidence(root, key, r.Coverage, r.Evidence); p != "" {
				return p
			}
		}
	case CoverageOutputOnly:
		// Server-populated fields either point at a test that asserts the
		// populated value, or record why the emulator never populates the
		// member (a legal state for an output-only field).
		if !exempt && strings.TrimSpace(r.Evidence) == "" && strings.TrimSpace(r.Reason) == "" {
			return fmt.Sprintf("output-only row %s has neither an evidence pointer nor a reason for never populating it", key)
		}
	case CoverageDerived:
		if strings.TrimSpace(r.Reason) == "" {
			return fmt.Sprintf("derived row %s has no formula in reason (name how the emulator computes it)", key)
		}
	case CoverageUnsupported:
		if strings.TrimSpace(r.Reason) == "" {
			return fmt.Sprintf("unsupported row %s has no reason", key)
		}
		if r.ReviewBy == "" {
			return fmt.Sprintf("unsupported row %s has no review_by date", key)
		}
		if _, err := time.Parse("2006-01-02", r.ReviewBy); err != nil {
			return fmt.Sprintf("unsupported row %s has invalid review_by %q (want YYYY-MM-DD)", key, r.ReviewBy)
		}
	}
	return ""
}

// checkEvidence requires at least one repo path#TestName pointer, and every
// referenced file must exist — a renamed or deleted test must not keep a
// roundtrip row green.
func checkEvidence(root, key, class, evidence string) string {
	if strings.TrimSpace(evidence) == "" {
		return fmt.Sprintf("%s row %s has no evidence pointer (path/to/file_test.go#TestName)", class, key)
	}
	for _, ref := range strings.Split(evidence, ";") {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		file := ref
		if i := strings.IndexAny(file, "#:"); i >= 0 {
			file = file[:i]
		}
		if !strings.Contains(file, "/") {
			return fmt.Sprintf("row %s: evidence %q is not a repo path pointer (want path/to/file_test.go#TestName)", key, ref)
		}
		if _, err := os.Stat(filepath.Join(root, file)); err != nil {
			return fmt.Sprintf("row %s: evidence file %q does not exist (%v)", key, file, err)
		}
	}
	return ""
}
