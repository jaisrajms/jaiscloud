// Command tfcapture is the FC6 provider-capture gate.
//
// The Terraform module gates (tests/integration/gcp/terraform/module-cases)
// capture the provider's outgoing HTTP requests with TF_LOG=DEBUG. This tool
// reads that log, extracts `terraform providers schema -json`, and joins the two
// through docs/fidelity/provider-attributes.csv:
//
//   - a provider attribute the module SENT is checked against the field catalog
//     (docs/fidelity/field-coverage.csv): a sent-but-unsupported attribute, or
//     one mapped to a catalog field with no row, fails the gate;
//   - an attribute the module did NOT send must be classified (client-side
//     provider quirk, output-only, unsupported, or simply not configured by the
//     pinned module), so a provider upgrade that adds an attribute fails until
//     it is classified;
//   - a stale mapping row (an attribute the provider no longer declares) fails.
//
// The empty-label case (TGM15) is the calibration example: the module configures
// labels.team = "" and the provider never sends it, so the classification is
// client-side — see tools/tfcapture/testdata/tgm15-empty-label.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	providerschema "jaiscloud/tools/providerschema"
)

func main() {
	var (
		logPath     = flag.String("log", "", "TF_LOG=DEBUG output captured by the module gate")
		schemaPath  = flag.String("schema", "", "`terraform providers schema -json` dump")
		mappingPath = flag.String("mapping", "docs/fidelity/provider-attributes.csv", "provider attribute classification")
		catalogPath = flag.String("catalog", "docs/fidelity/field-coverage.csv", "field-coverage catalog")
		caseName    = flag.String("case", "", "module case name (report only)")
		initMapping = flag.Bool("init-mapping", false, "write a fresh mapping file from the schema (defaults, then classify)")
		snapshot    = flag.String("snapshot", "", "write a compacted provider-schema snapshot of -resources and exit")
		resources   = flag.String("resources", "", "comma-separated provider resource types for -init-mapping/-snapshot")
		requireRes  = flag.Bool("require-resources", false, "fail when no captured request maps to a provider resource (the case must exercise the join)")
		force       = flag.Bool("force", false, "with -init-mapping: overwrite an existing mapping file")
	)
	flag.Parse()

	if *snapshot != "" {
		if *schemaPath == "" || *resources == "" {
			fatalf("-snapshot requires -schema and -resources")
		}
		raw, err := os.ReadFile(*schemaPath)
		if err != nil {
			fatalf("%v", err)
		}
		compact, err := providerschema.Compact(raw, strings.Split(*resources, ","))
		if err != nil {
			fatalf("%v", err)
		}
		if err := os.WriteFile(*snapshot, append(compact, '\n'), 0o644); err != nil {
			fatalf("%v", err)
		}
		fmt.Printf("tfcapture: wrote %s\n", *snapshot)
		return
	}

	if *initMapping {
		if *schemaPath == "" || *resources == "" {
			fatalf("-init-mapping requires -schema and -resources")
		}
		schema, err := providerschema.Load(*schemaPath)
		if err != nil {
			fatalf("%v", err)
		}
		if _, err := os.Stat(*mappingPath); err == nil && !*force {
			fatalf("%s already exists; refusing to truncate the hand-maintained classifications (pass -force to regenerate)", *mappingPath)
		}
		if err := writeInitialMapping(*mappingPath, schema, strings.Split(*resources, ",")); err != nil {
			fatalf("%v", err)
		}
		fmt.Printf("tfcapture: wrote %s\n", *mappingPath)
		return
	}

	if *logPath == "" || *schemaPath == "" {
		fatalf("usage: tfcapture -log <tf-debug.log> -schema <schema.json> [-mapping ...] [-catalog ...] [-case ...] [-strict]")
	}
	schema, err := providerschema.Load(*schemaPath)
	if err != nil {
		fatalf("%v", err)
	}
	mapping, err := loadMapping(*mappingPath)
	if err != nil {
		fatalf("%v", err)
	}
	catalog, err := loadCatalog(*catalogPath)
	if err != nil {
		fatalf("%v", err)
	}
	logText, err := os.ReadFile(*logPath)
	if err != nil {
		fatalf("read debug log: %v", err)
	}
	requests := ParseDebugLog(string(logText))
	if len(requests) == 0 {
		fatalf("no provider request blocks in %s (was TF_LOG=DEBUG set?)", *logPath)
	}

	report := Check(*caseName, requests, schema, mapping, catalog, *requireRes)
	fmt.Print(report.Render())
	if report.Failed() {
		os.Exit(1)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "tfcapture: "+format+"\n", args...)
	os.Exit(2)
}

// ─── mapping file ────────────────────────────────────────────────────────────

// Classification is how a provider attribute relates to the API surface.
type Classification string

const (
	// Served means the provider sends it and the emulator serves it; api_field
	// names the catalog field it joins to.
	Served Classification = "served"
	// NoCatalog means the provider sends it and the emulator serves it, but the
	// resource is outside the FC1 field catalog.
	NoCatalog Classification = "no-catalog"
	// ClientSide means the provider never sends the attribute (TGM15 class): the
	// value is dropped client-side, so the emulator cannot be blamed.
	ClientSide Classification = "client-side"
	// OutputOnly means the attribute is computed by the provider/server and is
	// never part of a request body.
	OutputOnly Classification = "output-only"
	// Unsupported means real clients can send it but the emulator does not serve
	// it. Sending it is a gate failure.
	Unsupported Classification = "unsupported"
	// NotExercised means this pinned module never sets the attribute.
	NotExercised Classification = "not-exercised"
)

type mappingKey struct {
	resource string
	attr     string
}

type mappingRow struct {
	providerResource string
	catalogService   string
	catalogResource  string
	attribute        string
	apiField         string
	class            Classification
	reason           string
}

func loadMapping(path string) (map[mappingKey]mappingRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read mapping (%s): %w", path, err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse mapping (%s): %w", path, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("mapping (%s) is empty", path)
	}
	header := rows[0]
	idx := map[string]int{}
	for i, h := range header {
		idx[strings.TrimSpace(h)] = i
	}
	for _, col := range []string{"provider_resource", "catalog_service", "catalog_resource", "attribute", "api_field", "classification", "reason"} {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("mapping (%s) missing column %q", path, col)
		}
	}
	out := map[mappingKey]mappingRow{}
	for n, row := range rows[1:] {
		line := n + 2
		get := func(col string) string {
			i := idx[col]
			if i >= len(row) {
				return ""
			}
			return strings.TrimSpace(row[i])
		}
		m := mappingRow{
			providerResource: get("provider_resource"),
			catalogService:   get("catalog_service"),
			catalogResource:  get("catalog_resource"),
			attribute:        get("attribute"),
			apiField:         get("api_field"),
			class:            Classification(get("classification")),
			reason:           get("reason"),
		}
		if m.providerResource == "" || m.attribute == "" {
			return nil, fmt.Errorf("mapping (%s) line %d: empty provider_resource/attribute", path, line)
		}
		switch m.class {
		case Served, NoCatalog, ClientSide, OutputOnly, Unsupported, NotExercised:
		default:
			return nil, fmt.Errorf("mapping (%s) line %d: unknown classification %q", path, line, m.class)
		}
		if m.class == Served && (m.catalogService == "" || m.catalogResource == "") {
			return nil, fmt.Errorf("mapping (%s) line %d: served requires catalog_service/catalog_resource", path, line)
		}
		if m.class != Served && m.class != NoCatalog && m.reason == "" {
			return nil, fmt.Errorf("mapping (%s) line %d: %s requires a reason", path, line, m.class)
		}
		out[mappingKey{m.providerResource, m.attribute}] = m
	}
	return out, nil
}

func writeInitialMapping(path string, schema *providerschema.ProviderSchema, resourceTypes []string) error {
	// Write to a sibling temp file and rename, so an interrupted regeneration
	// cannot leave a truncated mapping behind.
	tmp, err := os.CreateTemp(filepath.Dir(path), ".provider-attributes-*.csv")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	f := tmp
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"provider_resource", "catalog_service", "catalog_resource", "attribute", "api_field", "classification", "reason"}); err != nil {
		return err
	}
	for _, rt := range resourceTypes {
		rt = strings.TrimSpace(rt)
		if rt == "" {
			continue
		}
		for _, a := range schema.AttributeSpecs(rt) {
			class := NotExercised
			reason := "the pinned module cases do not configure this attribute"
			if a.Computed && !a.Optional && !a.Required {
				class = OutputOnly
				reason = "computed-only attribute; never part of a request body"
			}
			row := []string{rt, "", "", a.Path, "", string(class), reason}
			if err := w.Write(row); err != nil {
				return err
			}
		}
	}
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// ─── catalog ─────────────────────────────────────────────────────────────────

type catalogKey struct {
	service  string
	resource string
	field    string
}

func loadCatalog(path string) (map[catalogKey]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read catalog (%s): %w", path, err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse catalog (%s): %w", path, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("catalog (%s) is empty", path)
	}
	idx := map[string]int{}
	for i, h := range rows[0] {
		idx[strings.TrimSpace(h)] = i
	}
	for _, col := range []string{"service", "resource", "field", "coverage"} {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("catalog (%s) missing column %q", path, col)
		}
	}
	out := map[catalogKey]string{}
	for _, row := range rows[1:] {
		get := func(col string) string {
			i, ok := idx[col]
			if !ok || i >= len(row) {
				return ""
			}
			return strings.TrimSpace(row[i])
		}
		key := catalogKey{get("service"), get("resource"), get("field")}
		if key.field == "" {
			continue
		}
		coverage := get("coverage")
		// A field classified unsupported on either transport is unsupported.
		if prev, ok := out[key]; ok && prev == "unsupported" {
			continue
		}
		out[key] = coverage
	}
	return out, nil
}

// ─── report ──────────────────────────────────────────────────────────────────

// Violation is one gate failure.
type Violation struct {
	Resource string
	Attr     string
	Detail   string
}

type Report struct {
	Case         string
	Requests     int
	WriteBodies  int
	Resources    []string
	Sent         map[string]int
	NeverSent    map[string]int
	Violations   []Violation
	Stale        []string
	Notes        []string
	Unclassified []string
	DroppedNote  bool
}

func (r *Report) Failed() bool { return len(r.Violations) > 0 || len(r.Stale) > 0 }

func (r *Report) Render() string {
	var b strings.Builder
	fmt.Fprintf(&b, "tfcapture: case=%s requests=%d write-bodies=%d\n", r.Case, r.Requests, r.WriteBodies)
	if len(r.Resources) == 0 {
		b.WriteString("  no mapped provider resources in the capture (capture recorded, nothing to join)\n")
	}
	for _, res := range r.Resources {
		fmt.Fprintf(&b, "  %s: sent=%d never-sent=%d\n", res, r.Sent[res], r.NeverSent[res])
	}
	for _, n := range r.Notes {
		fmt.Fprintf(&b, "  note: %s\n", n)
	}
	for _, u := range r.Unclassified {
		fmt.Fprintf(&b, "  unclassified: %s\n", u)
	}
	for _, s := range r.Stale {
		fmt.Fprintf(&b, "  STALE MAPPING: %s\n", s)
	}
	for _, v := range r.Violations {
		fmt.Fprintf(&b, "  VIOLATION %s.%s: %s\n", v.Resource, v.Attr, v.Detail)
	}
	if r.Failed() {
		b.WriteString("tfcapture: FAIL\n")
	} else {
		b.WriteString("tfcapture: PASS\n")
	}
	return b.String()
}

// Check joins the captured requests to the provider schema and the catalog.
func Check(caseName string, requests []Request, schema *providerschema.ProviderSchema, mapping map[mappingKey]mappingRow, catalog map[catalogKey]string, requireResources bool) *Report {
	report := &Report{
		Case:      caseName,
		Requests:  len(requests),
		Sent:      map[string]int{},
		NeverSent: map[string]int{},
	}
	// Attribute paths actually present in a request body, per provider resource.
	sent := map[mappingKey]bool{}
	for _, req := range requests {
		res := ResourceForPath(req.Method, req.Path)
		if res == "" || req.Body == nil {
			continue
		}
		report.WriteBodies++
		for _, row := range mapping {
			if row.providerResource != res {
				continue
			}
			if row.apiField != "" && hasJSONPath(req.Body, row.apiField) {
				sent[mappingKey{res, row.attribute}] = true
			}
		}
	}

	// Which resource types the mapping covers and the capture touched.
	touched := map[string]bool{}
	for _, req := range requests {
		if res := ResourceForPath(req.Method, req.Path); res != "" {
			touched[res] = true
		}
	}
	for res := range touched {
		report.Resources = append(report.Resources, res)
	}
	sort.Strings(report.Resources)

	if requireResources && len(report.Resources) == 0 {
		report.Violations = append(report.Violations, Violation{Detail: "no captured request mapped to a provider resource; the case must exercise the join (or JC_TF_CAPTURE=0 for local debugging)"})
	}

	// A mapping row the provider no longer declares is stale, whether or not
	// this capture touched its resource. Resources absent from the extracted
	// schema cannot be judged (the fixture schema is trimmed) and are skipped.
	declaredByResource := map[string]map[string]bool{}
	for res := range schema.Resources {
		set := map[string]bool{}
		for _, a := range schema.AttributeSpecs(res) {
			set[a.Path] = true
		}
		declaredByResource[res] = set
	}
	for key := range mapping {
		declared, ok := declaredByResource[key.resource]
		if !ok || declared[key.attr] {
			continue
		}
		report.Stale = append(report.Stale, fmt.Sprintf("%s.%s", key.resource, key.attr))
	}

	// Check every mapping-covered resource the schema declares, not only the
	// ones this capture touched: a provider release that adds an attribute must
	// be classified even when the running module case does not set it.
	mapped := map[string]bool{}
	for key := range mapping {
		mapped[key.resource] = true
	}
	var checkResources []string
	for res := range mapped {
		if _, ok := schema.Resources[res]; ok {
			checkResources = append(checkResources, res)
		}
	}
	sort.Strings(checkResources)

	for _, res := range checkResources {
		specs := schema.AttributeSpecs(res)
		touchedResource := false
		for _, r := range report.Resources {
			if r == res {
				touchedResource = true
			}
		}
		if len(specs) == 0 {
			report.Violations = append(report.Violations, Violation{Resource: res, Detail: "provider resource is not in the extracted schema (wrong provider version?)"})
			continue
		}
		declared := map[string]bool{}
		for _, spec := range specs {
			declared[spec.Path] = true
			key := mappingKey{res, spec.Path}
			row, ok := mapping[key]
			isSent := sent[key]
			if touchedResource {
				if isSent {
					report.Sent[res]++
				} else {
					report.NeverSent[res]++
				}
			}
			if !ok {
				if spec.Computed && !spec.Optional && !spec.Required {
					continue // computed-only: auto output-only
				}
				msg := fmt.Sprintf("%s.%s", res, spec.Path)
				report.Unclassified = append(report.Unclassified, msg)
				report.Violations = append(report.Violations, Violation{Resource: res, Attr: spec.Path, Detail: "unclassified attribute (add a mapping row)"})
				continue
			}
			switch row.class {
			case Unsupported:
				if isSent {
					report.Violations = append(report.Violations, Violation{Resource: res, Attr: spec.Path, Detail: "sent but the emulator does not serve it: " + row.reason})
				}
			case ClientSide:
				if isSent {
					report.Violations = append(report.Violations, Violation{Resource: res, Attr: spec.Path, Detail: "classified client-side but a request body carries it; reclassify"})
				}
			case OutputOnly:
				if isSent {
					report.Violations = append(report.Violations, Violation{Resource: res, Attr: spec.Path, Detail: "classified output-only but a request body carries it; reclassify"})
				}
			case NotExercised:
				if isSent {
					report.Violations = append(report.Violations, Violation{Resource: res, Attr: spec.Path, Detail: "classified not-exercised but this capture sends it; classify it (served / no-catalog / unsupported)"})
				}
			case Served:
				if !isSent {
					continue // the module does not exercise it; not a failure
				}
				coverage, ok := catalog[catalogKey{row.catalogService, row.catalogResource, row.apiField}]
				if !ok {
					report.Violations = append(report.Violations, Violation{Resource: res, Attr: spec.Path, Detail: fmt.Sprintf("sent as %q but %s/%s has no catalog row", row.apiField, row.catalogService, row.catalogResource)})
					continue
				}
				if coverage == "unsupported" {
					report.Violations = append(report.Violations, Violation{Resource: res, Attr: spec.Path, Detail: fmt.Sprintf("sent as %q but the catalog classifies it unsupported", row.apiField)})
				}
			}
		}
	}

	// TGM15 calibration note: a labels map carrying fewer keys than the module
	// configured is the provider dropping empty values. The capture cannot see
	// the config, so surface the empty-value rule whenever a labels body is sent.
	for _, req := range requests {
		if req.Body == nil {
			continue
		}
		if labels, ok := req.Body["labels"].(map[string]any); ok {
			empty := 0
			for _, v := range labels {
				if s, _ := v.(string); s == "" {
					empty++
				}
			}
			if empty > 0 {
				note := "labels body carries empty-valued entries; the empty-label fixture is in tools/tfcapture/testdata/tgm15-empty-label"
				if !slices.Contains(report.Notes, note) {
					report.Notes = append(report.Notes, note)
				}
			}
		}
	}
	sort.Strings(report.Unclassified)
	sort.Strings(report.Stale)
	return report
}

// hasJSONPath reports whether a decoded JSON body carries the dotted path.
func hasJSONPath(body map[string]any, path string) bool {
	return hasPath(any(body), strings.Split(path, "."))
}

func hasPath(cur any, segs []string) bool {
	if len(segs) == 0 {
		return true
	}
	switch v := cur.(type) {
	case map[string]any:
		next, ok := v[segs[0]]
		if !ok {
			return false
		}
		return hasPath(next, segs[1:])
	case []any:
		// A JSON array stands in for the provider's block list; any element
		// carrying the rest of the path proves it was sent.
		for _, elem := range v {
			if hasPath(elem, segs) {
				return true
			}
		}
		return false
	}
	return false
}
