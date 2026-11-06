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
			return nil, fmt.Errorf("%s:%d: %d column(s), want %d", path, i+2, len(rec), len(csvHeader))
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
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return os.Chmod(path, 0o644)
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
	added, reclassified := 0, 0
	for _, f := range universe.Fields {
		inUniverse[f.Key()] = true
		row, ok := byKey[f.Key()]
		if !ok {
			added++
		} else if row.Behavior != f.Behavior {
			// The definition changed since the row was classified (a field
			// became OUTPUT_ONLY/IMMUTABLE, a reference moved, ...). Drop the
			// classification so the new behavior has to be re-reviewed instead
			// of silently inheriting the old verdict.
			reclassified++
			row.Coverage, row.Evidence, row.Reason, row.ReviewBy = "", "", "", ""
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
	fmt.Printf("field-coverage: wrote %s — %d field(s), %d newly scaffolded, %d reclassified after a definition change, %d stale row(s) preserved\n",
		path, len(universe.Fields), added, reclassified, stale)
	if added > 0 {
		fmt.Println("field-coverage: scaffolded rows have no coverage yet; classify them (the check fails until then)")
	}
	if reclassified > 0 {
		fmt.Println("field-coverage: rows whose definition behavior changed lost their classification; re-review and reclassify them")
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

// CheckIn validates the committed rows against the freshly derived universe and
// each row's classification contract. root is the repository root that evidence
// pointers are resolved against.
//
//   - every definition field has exactly one row                      (missing)
//   - every row belongs to a still-defined field                      (stale)
//   - the recorded behavior matches the definition                    (drift)
//   - coverage is one of the five classes                             (invalid)
//   - roundtrip rows the definition declares non-writable carry a
//     written reason                                                  (deviation)
//   - roundtrip/immutable rows carry an evidence pointer naming an
//     existing test function, unless exempted                         (evidence)
//   - output-only rows carry an evidence pointer or a reason why the
//     emulator never populates the member                             (reason)
//   - derived rows name their formula in reason                       (reason)
//   - unsupported rows carry a reason and a future review_by date     (reason)
//   - exemptions are used (evidence waived) and never misapplied      (exemption)
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
		if _, ok := exemptions[k]; ok {
			switch row.Coverage {
			case CoverageRoundtrip, CoverageImmutable, CoverageOutputOnly:
				if strings.TrimSpace(row.Evidence) == "" {
					// The exemption is doing its job: waiving a missing
					// evidence pointer for a field whose negative/round-trip
					// assertion is scheduled for a later wave.
					rep.Exempt++
				} else {
					problems = append(problems, fmt.Sprintf("unused exemption %s: the row already has an evidence pointer (remove the exemption)", k))
				}
			default:
				problems = append(problems, fmt.Sprintf("misapplied exemption %s: coverage %q has no evidence requirement (remove the exemption)", k, row.Coverage))
			}
		}
		rep.ByCoverage[row.Coverage]++
		rep.ByService[row.Service]++
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
		note := ""
		if len(shown) > limit {
			shown = shown[:limit]
			note = fmt.Sprintf("\n  ... showing the first %d of %d problems", limit, len(problems))
		}
		return rep, fmt.Errorf("field-coverage gate: %d problem(s):\n  %s%s",
			len(problems), strings.Join(shown, "\n  "), note)
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
		// A field the definition declares OUTPUT_ONLY or IMMUTABLE but the
		// emulator round-trips is a documented deviation (the TGM6/TGM11-1
		// class). It is allowed, but the reason must say why.
		if declaresNonWritable(r.Behavior) && strings.TrimSpace(r.Reason) == "" {
			return fmt.Sprintf("roundtrip row %s contradicts its definition behavior %q: record why the emulator accepts/returns a field the API declares %s (or reclassify it)", key, r.Behavior, r.Behavior)
		}
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
		// member (a legal state for an output-only field). A pointer that is
		// present must still resolve.
		if strings.TrimSpace(r.Evidence) != "" && !exempt {
			if p := checkEvidence(root, key, r.Coverage, r.Evidence); p != "" {
				return p
			}
		}
		if !exempt && strings.TrimSpace(r.Evidence) == "" && strings.TrimSpace(r.Reason) == "" {
			return fmt.Sprintf("output-only row %s has neither an evidence pointer nor a reason for never populating it", key)
		}
	case CoverageDerived:
		if strings.TrimSpace(r.Reason) == "" {
			return fmt.Sprintf("derived row %s has no formula in reason (name how the emulator computes it)", key)
		}
		// A derived row may cite the test that asserts the computed value.
		if strings.TrimSpace(r.Evidence) != "" && !exempt {
			if p := checkEvidence(root, key, r.Coverage, r.Evidence); p != "" {
				return p
			}
		}
	case CoverageUnsupported:
		if strings.TrimSpace(r.Reason) == "" {
			return fmt.Sprintf("unsupported row %s has no reason", key)
		}
		if r.ReviewBy == "" {
			return fmt.Sprintf("unsupported row %s has no review_by date", key)
		}
		reviewBy, err := time.Parse("2006-01-02", r.ReviewBy)
		if err != nil {
			return fmt.Sprintf("unsupported row %s has invalid review_by %q (want YYYY-MM-DD)", key, r.ReviewBy)
		}
		// A review date that has passed means the deferral was never revisited.
		if reviewBy.Before(today()) {
			return fmt.Sprintf("unsupported row %s is overdue for review (review_by %s has passed): re-review it, fix it, or move the date with a note", key, r.ReviewBy)
		}
	}
	return ""
}

// declaresNonWritable reports whether a definition behavior token set declares
// the field server-owned or create-only.
func declaresNonWritable(behavior string) bool {
	for _, token := range strings.Split(behavior, "|") {
		switch token {
		case "output_only", "input_only", "immutable", "read_only":
			return true
		}
	}
	return false
}

func today() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}

// checkEvidence requires at least one `path/to/file_test.go#TestName` pointer.
// The file must exist and the named test function must be defined in it: a
// renamed or deleted test must not keep a roundtrip row green. Multiple pointers
// are ';'-separated and every one is resolved.
func checkEvidence(root, key, class, evidence string) string {
	if strings.TrimSpace(evidence) == "" {
		return fmt.Sprintf("%s row %s has no evidence pointer (path/to/file_test.go#TestName)", class, key)
	}
	for _, ref := range strings.Split(evidence, ";") {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		file, symbol, hasSymbol := strings.Cut(ref, "#")
		if !strings.Contains(file, "/") {
			return fmt.Sprintf("row %s: evidence %q is not a repo path pointer (want path/to/file_test.go#TestName)", key, ref)
		}
		source, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			return fmt.Sprintf("row %s: evidence file %q does not exist (%v)", key, file, err)
		}
		if !hasSymbol || symbol == "" {
			return fmt.Sprintf("row %s: evidence %q names no test function (want path/to/file_test.go#TestName)", key, ref)
		}
		if !strings.Contains(string(source), "func "+symbol+"(") {
			return fmt.Sprintf("row %s: evidence %q names no such test: %s defines no func %s", key, ref, file, symbol)
		}
	}
	return ""
}
