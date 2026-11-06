//go:build gcp_conformance

package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Reference coverage (FC4).
//
// Every field the API declares as a google.api.resource_reference names another
// service's resource. Before FC4 those values were stored verbatim (the TGM11-1
// class: Firestore accepted a CMEK key name without asking KMS whether the key
// existed). The universe here is derived from the definitions — the annotated
// fields of the scoped resources plus the reference messages named in
// tools/fieldgen/scope.yaml — and joined with a committed classification:
//
//	validated    existence checked against the referenced store; check names the
//	             accept and reject test
//	format-only  only the name shape is validated; check names the test
//	unvalidated  stored verbatim by design; a written reason and a review date
//	             are required (a scheduled gap, not a silent one)
//	not-modelled the emulator does not parse or serve the field; reason + date
//
// A reference with no row, a stale row, or a row whose check/test is missing
// fails the gate, so a new annotation cannot enter the surface unclassified.

// Reference is one definition field that names another resource.
type Reference struct {
	Service  string
	Resource string // the owning resource label (the catalog's resource column)
	Field    string // dotted path inside the owning message
	Target   string // the referenced resource type (google.api.resource_reference)
}

// ReferenceStatus values.
const (
	RefValidated   = "validated"
	RefFormatOnly  = "format-only"
	RefUnvalidated = "unvalidated"
	RefNotModelled = "not-modelled"
)

var referenceStatuses = map[string]bool{
	RefValidated: true, RefFormatOnly: true, RefUnvalidated: true, RefNotModelled: true,
}

// ReferenceCSVHeader is the committed reference-coverage.csv column order.
var ReferenceCSVHeader = []string{"service", "resource", "field", "target_type", "status", "check", "reason", "review_by"}

// ReferenceRow is one line of reference-coverage.csv.
type ReferenceRow struct {
	Service  string
	Resource string
	Field    string
	Target   string
	Status   string
	Check    string
	Reason   string
	ReviewBy string
}

func (r ReferenceRow) Key() string { return referenceKey(r.Service, r.Resource, r.Field) }

func referenceKey(service, resource, field string) string {
	return service + "/" + resource + "/" + normalizeRefField(field)
}

// normalizeRefField folds the two transports' spellings of one field.
func normalizeRefField(field string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(field) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// BuildReferences derives the reference universe from the definitions.
func BuildReferences(scope *Scope, universe *Universe) ([]Reference, error) {
	byKey := map[string]Reference{}
	add := func(r Reference) {
		if r.Field == "" || r.Target == "" {
			return
		}
		key := referenceKey(r.Service, r.Resource, r.Field)
		if _, ok := byKey[key]; ok {
			return
		}
		byKey[key] = r
	}
	// (a) every annotated field of a scoped resource.
	for _, f := range universe.Fields {
		target := refTarget(f.Behavior)
		if target == "" {
			continue
		}
		add(Reference{Service: f.Service, Resource: f.Resource, Field: f.Name, Target: target})
	}
	// (b) the reference messages named in the scope (services whose own
	// resource fields are not in the field-coverage pilot).
	for _, src := range scope.References {
		fields, err := WalkProto(src.Service, "grpc", src.Resource, src.Message, src.Depth)
		if err != nil {
			return nil, fmt.Errorf("reference source %s/%s: %w", src.Service, src.Resource, err)
		}
		for _, f := range fields {
			target := refTarget(f.Behavior)
			if target == "" {
				continue
			}
			add(Reference{Service: src.Service, Resource: src.Resource, Field: f.Name, Target: target})
		}
	}
	// (c) references a Discovery document documents but cannot annotate.
	for _, d := range scope.DocumentedReferences {
		add(Reference{Service: d.Service, Resource: d.Resource, Field: d.Field, Target: d.Target})
	}
	out := make([]Reference, 0, len(byKey))
	for _, r := range byKey {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		return referenceKey(out[i].Service, out[i].Resource, out[i].Field) < referenceKey(out[j].Service, out[j].Resource, out[j].Field)
	})
	return out, nil
}

// refTarget extracts the resource_reference target from a behavior token list.
func refTarget(behavior string) string {
	for _, token := range strings.Split(behavior, "|") {
		if strings.HasPrefix(token, "ref:") {
			return strings.TrimPrefix(token, "ref:")
		}
	}
	return ""
}

// ReadReferenceCSV loads the committed reference classification.
func ReadReferenceCSV(path string) ([]ReferenceRow, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%s does not exist: run `make gen-gcp-reference-coverage` to scaffold it", path)
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
	if got := strings.Join(records[0], ","); got != strings.Join(ReferenceCSVHeader, ",") {
		return nil, fmt.Errorf("%s: unexpected header %q", path, got)
	}
	rows := make([]ReferenceRow, 0, len(records)-1)
	for i, rec := range records[1:] {
		if len(rec) != len(ReferenceCSVHeader) {
			return nil, fmt.Errorf("%s:%d: %d column(s), want %d", path, i+2, len(rec), len(ReferenceCSVHeader))
		}
		rows = append(rows, ReferenceRow{Service: rec[0], Resource: rec[1], Field: rec[2], Target: rec[3], Status: rec[4], Check: rec[5], Reason: rec[6], ReviewBy: rec[7]})
	}
	return rows, nil
}

// GenerateReferences refreshes the CSV from the definitions, preserving the
// classification of a field that still exists and scaffolding new fields with
// no status (so the check fails until they are classified).
func GenerateReferences(path string, refs []Reference) error {
	var existing []ReferenceRow
	if _, err := os.Stat(path); err == nil {
		var rerr error
		existing, rerr = ReadReferenceCSV(path)
		if rerr != nil {
			return rerr
		}
	}
	byKey := map[string]ReferenceRow{}
	for _, r := range existing {
		byKey[r.Key()] = r
	}
	out := make([]ReferenceRow, 0, len(refs))
	added := 0
	for _, ref := range refs {
		row, ok := byKey[referenceKey(ref.Service, ref.Resource, ref.Field)]
		if !ok {
			added++
		}
		row.Service, row.Resource, row.Field, row.Target = ref.Service, ref.Resource, ref.Field, ref.Target
		out = append(out, row)
	}
	stale := 0
	inUniverse := map[string]bool{}
	for _, r := range out {
		inUniverse[r.Key()] = true
	}
	for _, r := range existing {
		if !inUniverse[r.Key()] {
			stale++
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	if err := writeReferenceCSV(path, out); err != nil {
		return err
	}
	fmt.Printf("reference-coverage: wrote %s — %d reference field(s), %d newly scaffolded, %d stale row(s) preserved\n", path, len(refs), added, stale)
	if added > 0 {
		fmt.Println("reference-coverage: scaffolded rows have no status yet; classify them (the check fails until then)")
	}
	return nil
}

func writeReferenceCSV(path string, rows []ReferenceRow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".reference-coverage-*.csv")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	w := csv.NewWriter(tmp)
	if err := w.Write(ReferenceCSVHeader); err != nil {
		tmp.Close()
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{r.Service, r.Resource, r.Field, r.Target, r.Status, r.Check, r.Reason, r.ReviewBy}); err != nil {
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

// CheckReferences validates the committed classification against the derived
// universe.
func CheckReferences(root string, refs []Reference, rows []ReferenceRow) (int, error) {
	byKey := map[string]ReferenceRow{}
	var problems []string
	for _, r := range rows {
		if prev, dup := byKey[r.Key()]; dup {
			problems = append(problems, fmt.Sprintf("duplicate row %s (status %q and %q)", r.Key(), prev.Status, r.Status))
			continue
		}
		byKey[r.Key()] = r
	}
	today := time.Now().UTC().Truncate(24 * time.Hour)
	universe := map[string]bool{}
	validated := 0
	for _, ref := range refs {
		key := referenceKey(ref.Service, ref.Resource, ref.Field)
		universe[key] = true
		row, ok := byKey[key]
		if !ok {
			problems = append(problems, fmt.Sprintf("missing row %s (annotated in the definition, absent from reference-coverage.csv — run `make gen-gcp-reference-coverage`)", key))
			continue
		}
		if row.Target != ref.Target {
			problems = append(problems, fmt.Sprintf("target drift %s: CSV says %q, definition says %q", key, row.Target, ref.Target))
		}
		if !referenceStatuses[row.Status] {
			problems = append(problems, fmt.Sprintf("invalid status %q on %s (want validated|format-only|unvalidated|not-modelled)", row.Status, key))
			continue
		}
		if row.Status == RefValidated {
			validated++
		}
		if strings.TrimSpace(row.Check) != "" {
			// A check pointer must resolve whatever the status: an unvalidated
			// row may cite the test that pins the current behaviour.
			if p := checkEvidence(root, key, row.Status, row.Check); p != "" {
				problems = append(problems, p)
			}
		}
		switch row.Status {
		case RefValidated, RefFormatOnly:
			if strings.TrimSpace(row.Check) == "" {
				problems = append(problems, fmt.Sprintf("%s row %s has no check (path/to/file_test.go#TestName)", row.Status, key))
			}
		case RefUnvalidated:
			if strings.TrimSpace(row.Reason) == "" {
				problems = append(problems, fmt.Sprintf("unvalidated row %s has no reason", key))
			}
			reviewBy, err := time.Parse("2006-01-02", row.ReviewBy)
			if err != nil {
				problems = append(problems, fmt.Sprintf("unvalidated row %s needs a review_by date (YYYY-MM-DD): a scheduled gap, not a permanent one", key))
			} else if reviewBy.Before(today) {
				problems = append(problems, fmt.Sprintf("unvalidated row %s is overdue for review (review_by %s)", key, row.ReviewBy))
			}
		case RefNotModelled:
			if strings.TrimSpace(row.Reason) == "" {
				problems = append(problems, fmt.Sprintf("not-modelled row %s has no reason", key))
			}
		}
	}
	for _, r := range rows {
		if !universe[r.Key()] {
			problems = append(problems, fmt.Sprintf("stale row %s: no annotated definition field has that key", r.Key()))
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		const limit = 30
		shown := problems
		if len(shown) > limit {
			shown = shown[:limit]
		}
		return validated, fmt.Errorf("reference-coverage gate: %d problem(s):\n  %s", len(problems), strings.Join(shown, "\n  "))
	}
	return validated, nil
}
