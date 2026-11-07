package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	providerschema "jaiscloud/tools/providerschema"
)

// CSVHeader is the committed inventory's column order.
var CSVHeader = []string{
	"provider_resource",
	"service",
	"emulator_state",
	"exercised_by",
	"classification",
	"reason",
	"review_after",
}

// Row is one generated inventory row.
type Row struct {
	ProviderResource string
	Service          string
	EmulatorState    string
	ExercisedBy      string
	Classification   string
	Reason           string
	ReviewAfter      string
}

// Inventory is the computed inventory plus the scope usage it implies.
type Inventory struct {
	Rows        []Row
	OutOfScope  int
	Unmatched   []string
	StaleRules  []string
	ServiceRows map[string][]Row
}

// Compute turns the provider schema snapshot, the scope and the fixture/module
// evidence into the expected inventory. It does not touch the committed CSV;
// Check compares the two.
func Compute(schema *providerschema.ProviderSchema, scope *Scope, exercise Exercise) (*Inventory, error) {
	types := make([]string, 0, len(schema.Resources))
	for t := range schema.Resources {
		types = append(types, t)
	}
	sort.Strings(types)

	u := scope.usage(types)
	inv := &Inventory{Unmatched: u.Unmatched, ServiceRows: map[string][]Row{}}
	for i, n := range u.Rules {
		if n == 0 {
			inv.StaleRules = append(inv.StaleRules, fmt.Sprintf("rule %q matches no provider resource type", scope.Rules[i].Match))
		}
	}
	for i, n := range u.OutOfScope {
		if n == 0 {
			inv.StaleRules = append(inv.StaleRules, fmt.Sprintf("out_of_scope %q matches no provider resource type", scope.OutOfScope[i].Match))
		}
		inv.OutOfScope += n
	}

	// Every HCL resource type must be a provider resource type; a typo in the
	// fixture or a case file would otherwise silently vanish from the
	// inventory (its row would stay `none`).
	for _, typ := range sortedKeys(exercise) {
		if _, ok := schema.Resources[typ]; !ok {
			return nil, fmt.Errorf("exercised resource type %q is not declared by provider %s (typo in the fixture or a module case?)", typ, scope.Provider)
		}
	}

	for _, typ := range types {
		if _, exempt := scope.MatchOutOfScope(typ); exempt {
			continue
		}
		rule, ok := scope.MatchRule(typ)
		if !ok {
			continue
		}
		row := Row{
			ProviderResource: typ,
			Service:          rule.Service,
			EmulatorState:    rule.State,
			ExercisedBy:      exercise.ExercisedBy(typ),
			Classification:   classify(rule.State, exercise.ExercisedBy(typ)),
			Reason:           reason(rule, exercise.ExercisedBy(typ)),
			ReviewAfter:      rule.ReviewAfter,
		}
		if row.ReviewAfter == "" {
			row.ReviewAfter = scope.DefaultReviewAfter
		}
		inv.Rows = append(inv.Rows, row)
		inv.ServiceRows[rule.Service] = append(inv.ServiceRows[rule.Service], row)
	}
	return inv, nil
}

// classify maps the emulator state plus the exercise evidence onto the row's
// classification. A resource exercised only by plan-only cases is plan-only;
// one also applied by the fixture or a module case is served.
func classify(state, exercisedBy string) string {
	switch state {
	case StateUnsupported:
		return ClassUnsupported
	case StateClientSide:
		return ClassClientSide
	}
	if exercisedBy == "none" {
		return ClassServed
	}
	anyApply := false
	for _, e := range strings.Split(exercisedBy, ",") {
		if !strings.HasPrefix(e, "plan-only:") {
			anyApply = true
			break
		}
	}
	if !anyApply {
		return ClassPlanOnly
	}
	return ClassServed
}

func reason(rule *Rule, exercisedBy string) string {
	if rule.Reason != "" {
		return rule.Reason
	}
	if exercisedBy != "none" {
		return ""
	}
	switch rule.State {
	case StateLimited:
		return fmt.Sprintf("%s is served with declared caveats; no fixture or module case declares this resource type yet", rule.Service)
	case StateServed:
		return fmt.Sprintf("%s is served; no fixture or module case declares this resource type yet", rule.Service)
	}
	return ""
}

// Validate applies the inventory rules that depend on the row and on today's
// date. It is run by both -init and -check.
func (inv *Inventory) Validate(scope *Scope, today time.Time, matrix map[string]string) error {
	var errs []string
	if len(inv.Unmatched) > 0 {
		errs = append(errs, fmt.Sprintf("%d provider resource type(s) are neither classified nor out of scope: %s",
			len(inv.Unmatched), strings.Join(limit(inv.Unmatched, 12), ", ")))
	}
	errs = append(errs, inv.StaleRules...)

	// Service-level checks run once, not per row.
	for name, svc := range scope.Services {
		matrixName := svc.MatrixService
		if matrixName == "" {
			matrixName = name
		}
		live, ok := matrix[matrixName]
		if !ok {
			errs = append(errs, fmt.Sprintf("service %s: %q is not in the fidelity matrix", name, matrixName))
			continue
		}
		// The gate's reclassification trigger: a service the rules were
		// written against as limited/unsupported (or one the emulator did not
		// serve) now has ga cells. Re-review instead of keeping a stale "not
		// served" reason.
		if live == "ga" && svc.MatrixState != "ga" {
			errs = append(errs, fmt.Sprintf("service %s was recorded as %q and is now ga; re-review its rules and matrix_state", name, svc.MatrixState))
		}
	}

	for _, row := range inv.Rows {
		needsReason := row.ExercisedBy == "none" || row.EmulatorState == StateUnsupported || row.EmulatorState == StateClientSide
		if needsReason && strings.TrimSpace(row.Reason) == "" {
			errs = append(errs, fmt.Sprintf("%s: exercised_by=%s / state=%s requires a reason", row.ProviderResource, row.ExercisedBy, row.EmulatorState))
		}
		if row.ReviewAfter != "" {
			d, err := parseDate(row.ReviewAfter)
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s: review_after: %v", row.ProviderResource, err))
			} else if needsReason && !d.After(today) {
				errs = append(errs, fmt.Sprintf("%s: review_after %s is not in the future (today %s)", row.ProviderResource, row.ReviewAfter, today.Format(dateLayout)))
			}
		}
	}
	return joinErrs(errs)
}

// WriteCSV writes the inventory deterministically.
func (inv *Inventory) WriteCSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(CSVHeader); err != nil {
		return err
	}
	for _, r := range inv.Rows {
		if err := cw.Write([]string{
			r.ProviderResource, r.Service, r.EmulatorState, r.ExercisedBy,
			r.Classification, r.Reason, r.ReviewAfter,
		}); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// ReadCSV loads a committed inventory.
func ReadCSV(r io.Reader) ([]Row, error) {
	cr := csv.NewReader(r)
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if strings.Join(header, ",") != strings.Join(CSVHeader, ",") {
		return nil, fmt.Errorf("header is %v, want %v (regenerate with make gen-gcp-terraform-coverage)", header, CSVHeader)
	}
	var rows []Row
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) != len(CSVHeader) {
			return nil, fmt.Errorf("row %d has %d columns, want %d", len(rows)+2, len(rec), len(CSVHeader))
		}
		rows = append(rows, Row{
			ProviderResource: rec[0], Service: rec[1], EmulatorState: rec[2],
			ExercisedBy: rec[3], Classification: rec[4], Reason: rec[5], ReviewAfter: rec[6],
		})
	}
	return rows, nil
}

// DiffRows compares the committed rows with the expected ones. It reports both
// stale rows (no longer in the schema or the scope) and drifted fields.
func DiffRows(got, want []Row) []string {
	gotBy := map[string]Row{}
	wantBy := map[string]Row{}
	for _, r := range got {
		gotBy[r.ProviderResource] = r
	}
	for _, r := range want {
		wantBy[r.ProviderResource] = r
	}
	var out []string
	for name := range gotBy {
		if _, ok := wantBy[name]; !ok {
			out = append(out, fmt.Sprintf("%s: stale row (no longer in the provider schema or the scope); remove it with -init", name))
		}
	}
	for name, want := range wantBy {
		got, ok := gotBy[name]
		if !ok {
			out = append(out, fmt.Sprintf("%s: missing row; add it with -init", name))
			continue
		}
		for _, f := range diffFields(got, want) {
			out = append(out, fmt.Sprintf("%s: %s is %q, want %q", name, f.name, f.got, f.want))
		}
	}
	sort.Strings(out)
	return out
}

type fieldDiff struct{ name, got, want string }

func diffFields(got, want Row) []fieldDiff {
	var out []fieldDiff
	add := func(name, g, w string) {
		if g != w {
			out = append(out, fieldDiff{name, g, w})
		}
	}
	add("service", got.Service, want.Service)
	add("emulator_state", got.EmulatorState, want.EmulatorState)
	add("exercised_by", got.ExercisedBy, want.ExercisedBy)
	add("classification", got.Classification, want.Classification)
	add("reason", got.Reason, want.Reason)
	add("review_after", got.ReviewAfter, want.ReviewAfter)
	return out
}

// LoadMatrix reads the fidelity matrix and rolls its cells up to a service
// state using the ledger's rule: ga when a service has at least one ga cell and
// no preview cell, limited when it has cells but no ga, and absent otherwise.
func LoadMatrix(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read fidelity matrix (%s): %w", path, err)
	}
	var f struct {
		Cells []struct {
			Service string `json:"service"`
			State   string `json:"state"`
		} `json:"cells"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse fidelity matrix (%s): %w", path, err)
	}
	type facts struct {
		ga      int
		total   int
		preview bool
	}
	byService := map[string]*facts{}
	for _, c := range f.Cells {
		fc := byService[c.Service]
		if fc == nil {
			fc = &facts{}
			byService[c.Service] = fc
		}
		fc.total++
		switch strings.ToLower(c.State) {
		case "ga":
			fc.ga++
		case "preview":
			fc.preview = true
		}
	}
	out := map[string]string{}
	for name, fc := range byService {
		switch {
		case fc.ga > 0 && !fc.preview:
			out[name] = "ga"
		case fc.total > 0:
			out[name] = "limited"
		}
	}
	return out, nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func limit(in []string, n int) []string {
	if len(in) <= n {
		return in
	}
	return append(append([]string{}, in[:n]...), fmt.Sprintf("… (%d more)", len(in)-n))
}

func joinErrs(errs []string) error {
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(errs, "\n  "))
}
