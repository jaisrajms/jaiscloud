// Per-service behavioral test-coverage inventory (GTC1).
//
// The inventory is *derived*, never hand-maintained, by joining three
// independent sources:
//
//   - the emulator service registry — internal/gcp/adapter's known service
//     names (gcpServices plus the gRPC-only services);
//   - the fidelity matrix — docs/fidelity/fidelity-matrix.json (per-service
//     transports and cell states);
//   - the GCP integration test tree — tests/integration/gcp/** (which suites
//     drive an official client and how many test funcs they carry).
//
// The contract it measures is the behavioral-suite Definition of Done: every
// `ga` service has at least one official-client behavioral suite (or a reasoned
// exemption). GTC9 (W4.1) promotes it to a hard gate (`-coverage-gate`, run by
// `make gcp-status-behavioral-gate` and CI).
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
	gcpadapter "jaiscloud/internal/gcp/adapter"
)

// gcpTestRoot is the integration test tree the inventory scans, relative to the
// repo root (where `make gcp-status` runs).
const gcpTestRoot = "tests/integration/gcp"

// defaultMatrixPath mirrors the -matrix flag default so the inventory can be
// rendered from writeMarkdown without threading the flag through.
const defaultMatrixPath = "docs/fidelity/fidelity-matrix.json"

// gcpServiceAlias normalizes the names that differ across the registry, the
// fidelity matrix and the test tree. Only names that actually differ need an
// entry; identical names pass through unchanged. This is a name-normalization
// join between independently-derived vocabularies — not a hand-maintained
// service list (the set of services always comes from the registry + matrix).
var gcpServiceAlias = map[string]string{
	// Registry wire-service name -> canonical (matrix / test-tree) name.
	"redis":    "memorystore",
	"sqladmin": "cloudsql",
	"dns":      "clouddns",
	// Official-client first-token fallback -> canonical service name.
	"cloudkms":             "kms",
	"cloudfunctions":       "functions",
	"cloudresourcemanager": "resourcemanager",
}

// importServiceNone is the exact-import set of official-client packages that
// are NOT a per-service client: the shared option/handle packages, the
// googleapi/iterator helpers, and the IAM policy *handle* (whose admin clients
// are google.golang.org/api/iam/v1 and cloud.google.com/go/iam/credentials/...).
var importServiceNone = map[string]bool{
	"google.golang.org/api/option":    true,
	"google.golang.org/api/googleapi": true,
	"google.golang.org/api/iterator":  true,
	"cloud.google.com/go/iam":         true,
}

// importServicePrefixes maps an official-client import path (or path prefix) to
// the canonical service it exercises. It is consulted before the first-token
// fallback so a nested or renamed client is credited to the right service
// instead of being truncated: cloudtasks -> tasks, iam/credentials ->
// iamcredentials, firestore/apiv1/admin -> firestoreadmin, workflows/executions
// -> workflowexecutions, longrunning -> operations. The longest matching prefix
// wins, so `.../iam/credentials/apiv1` beats `.../iam`.
var importServicePrefixes = map[string]string{
	"cloud.google.com/go/longrunning":                "operations",
	"cloud.google.com/go/cloudtasks":                 "tasks",
	"cloud.google.com/go/iam/credentials/apiv1":      "iamcredentials",
	"cloud.google.com/go/firestore/apiv1/admin":      "firestoreadmin",
	"cloud.google.com/go/workflows/executions/apiv1": "workflowexecutions",
	"cloud.google.com/go/scheduler":                  "scheduler",
}

// canonicalService lower-cases a name and strips separators, then applies the
// alias map. It maps both a registry service name ("redis") and an official
// client package token ("cloudkms") to the canonical fidelity-matrix name.
func canonicalService(name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	n = strings.NewReplacer("-", "", "_", "", ".", "").Replace(n)
	if c, ok := gcpServiceAlias[n]; ok {
		return c
	}
	return n
}

// matrixServiceFacts is the per-service rollup of the fidelity matrix.
type matrixServiceFacts struct {
	transports map[string]bool // "rest" and/or "grpc"
	ga         int             // cells in state `ga`
	total      int             // all cells
	preview    bool            // any cell in state `preview`
}

// serviceIsGA reports whether a service counts as a `ga` service for the DoD.
// The rule is deliberately mechanical and raw: a service is `ga` when the
// fidelity matrix has at least one `ga` cell and no `preview` cell — derived
// only from the per-operation/per-transport states, matching docs/gcp/wire-contract.md's
// per-operation model. It is NOT docs/gcp/testability.md §2's curated *tier*
// (which applies hand-picked overrides and a stale snapshot); it intentionally
// has no overrides. The rule is stricter than "has any ga cell" (a preview
// service is not ga) and looser than "every cell is ga" (a service may carry a
// few explicit `unsupported` stubs, e.g. container/dataproc/metastore, and stay
// ga). A `limited`-only service (0 `ga` cells) is not ga and is exempt.
func serviceIsGA(f matrixServiceFacts) bool {
	return f.ga > 0 && !f.preview
}

// gaCoverageTotals returns (covered, total) across the `ga` rows.
func gaCoverageTotals(rows []coverageRow) (covered, total int) {
	for _, r := range rows {
		if r.GA {
			total++
			if r.Covered {
				covered++
			}
		}
	}
	return covered, total
}

// matrixServiceFacts rolls the fidelity matrix up per service.
func matrixServiceFactsByService(mf matrixFile) map[string]matrixServiceFacts {
	out := make(map[string]matrixServiceFacts)
	for _, c := range mf.Cells {
		f := out[c.Service]
		if f.transports == nil {
			f.transports = map[string]bool{}
		}
		f.transports[c.Transport] = true
		f.total++
		switch {
		case strings.EqualFold(c.State, "ga"):
			f.ga++
		case strings.EqualFold(c.State, "preview"):
			f.preview = true
		}
		out[c.Service] = f
	}
	return out
}

// coverageTestFile is one *_test.go file and the canonical services it
// exercises, plus its top-level test-func count.
type coverageTestFile struct {
	Suite    string          // dir relative to gcpTestRoot ("." for the root package)
	Services map[string]bool // canonical services exercised
	Funcs    int             // func Test... count
}

// coverageRow is one service's derived behavioral-suite status.
type coverageRow struct {
	Service    string   // registry wire-service name (or matrix-only service name)
	Canonical  string   // fidelity-matrix / test-tree name
	Transports []string // rest and/or grpc
	GACells    int
	TotalCells int
	GA         bool     // qualifies as a `ga` service for the DoD
	Suites     []string // behavioral suites exercising it
	Funcs      int      // summed test funcs of those suites' files
	Covered    bool     // at least one behavioral suite
}

// uncoveredGAServices returns the services that are `ga` but have no official-client
// behavioral suite — the DoD violations ignored by the gate only when a
// reasoned exemption names them.
func uncoveredGAServices(rows []coverageRow) []string {
	var out []string
	for _, r := range rows {
		if r.GA && !r.Covered {
			out = append(out, r.Service)
		}
	}
	return out
}

// defaultCoverageExemptionsPath is the committed reasoned-exemption list: the
// `ga` services that deliberately ship without a tests/integration/gcp
// official-client suite. Same shape (a service -> reason overlay) as the ledger's
// own docs/gcpstatus-resolved.yaml; the conformance-owned
// tests/gcpconformance/testdata/coverage-exemptions.json is a different,
// operation-level list.
const defaultCoverageExemptionsPath = "docs/gcpstatus-coverage-exemptions.yaml"

// coverageExemption is one reasoned exemption from the behavioral-suite DoD.
type coverageExemption struct {
	Service string `yaml:"service"`
	Reason  string `yaml:"reason"`
}

// coverageExemptionsFile is the committed exemption list.
type coverageExemptionsFile struct {
	Exemptions []coverageExemption `yaml:"exemptions"`
}

// loadCoverageExemptions reads the reasoned-exemption list, keyed by canonical
// service name. An empty path (or a missing file) yields no exemptions. Every
// entry must carry both a service and a reason, the service must resolve to a
// known inventory row, and it must currently be an **uncovered `ga`** service —
// so a typo, a duplicate, or a stale exemption (its suite has landed) fails
// loud instead of silently suppressing or inflating the record.
func loadCoverageExemptions(path string, rows []coverageRow) (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(path) == "" {
		return out, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	var f coverageExemptionsFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("coverage exemptions %s: %w", path, err)
	}
	known := map[string]bool{}
	gaps := map[string]bool{}
	for _, r := range rows {
		canon := canonicalService(r.Service)
		known[canon] = true
		if r.GA && !r.Covered {
			gaps[canon] = true
		}
	}
	for i, e := range f.Exemptions {
		if strings.TrimSpace(e.Service) == "" || strings.TrimSpace(e.Reason) == "" {
			return nil, fmt.Errorf("coverage exemptions %s: entry %d needs a non-empty service and reason", path, i+1)
		}
		svc := canonicalService(e.Service)
		if !known[svc] {
			return nil, fmt.Errorf("coverage exemptions %s: entry %d names unknown service %q", path, i+1, e.Service)
		}
		if !gaps[svc] {
			return nil, fmt.Errorf("coverage exemptions %s: entry %d exempts %q, which is not an uncovered `ga` service — remove the stale exemption", path, i+1, e.Service)
		}
		if _, dup := out[svc]; dup {
			return nil, fmt.Errorf("coverage exemptions %s: entry %d duplicates service %q", path, i+1, e.Service)
		}
		out[svc] = e.Reason
	}
	return out, nil
}

// coverageGateViolations returns the `ga` services that have no official-client
// behavioral suite and no reasoned exemption — the DoD gate failures. It is
// pure so the gate is unit-testable with synthetic rows, and deterministic.
func coverageGateViolations(rows []coverageRow, exemptions map[string]string) []string {
	var out []string
	for _, r := range rows {
		if r.GA && !r.Covered && exemptions[canonicalService(r.Service)] == "" {
			out = append(out, r.Service)
		}
	}
	return out
}

// buildCoverage is the pure inventory join: registry services + matrix facts +
// behavioral test files -> one row per service. It is deterministic and has no
// I/O, so the DoD rules are unit-testable without the real tree.
func buildCoverage(services []string, facts map[string]matrixServiceFacts, files []coverageTestFile) []coverageRow {
	suitesBySvc := map[string]map[string]bool{}
	funcsBySvc := map[string]int{}
	for _, f := range files {
		for svc := range f.Services {
			if suitesBySvc[svc] == nil {
				suitesBySvc[svc] = map[string]bool{}
			}
			suitesBySvc[svc][f.Suite] = true
			funcsBySvc[svc] += f.Funcs
		}
	}

	rows := make([]coverageRow, 0, len(services))
	for _, svc := range services {
		canon := canonicalService(svc)
		f := facts[canon]
		var transports []string
		for t := range f.transports {
			transports = append(transports, t)
		}
		sort.Strings(transports)
		var suites []string
		for s := range suitesBySvc[canon] {
			suites = append(suites, s)
		}
		sort.Strings(suites)
		rows = append(rows, coverageRow{
			Service:    svc,
			Canonical:  canon,
			Transports: transports,
			GACells:    f.ga,
			TotalCells: f.total,
			GA:         serviceIsGA(f),
			Suites:     suites,
			Funcs:      funcsBySvc[canon],
			Covered:    len(suites) > 0,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Service < rows[j].Service })
	return rows
}

// dedicatedSuiteService returns the canonical service a `sdk-<service>`
// directory is dedicated to ("" for the generic suites sdk / sdk-rest /
// sdk-gcs-grpc and for non-sdk directories).
func dedicatedSuiteService(dir string) string {
	if !strings.HasPrefix(dir, "sdk-") {
		return ""
	}
	switch tok := canonicalService(strings.TrimPrefix(dir, "sdk-")); tok {
	case "", "rest", "gcsgrpc": // generic suites, not a single service
		return ""
	default:
		return tok
	}
}

// importService maps an official Google client import path to the canonical
// service it exercises ("" when it is not a per-service client package).
// Explicit prefix mappings (importServicePrefixes, longest match wins) are
// checked first so nested/renamed clients resolve correctly; exact non-service
// packages (importServiceNone) return ""; anything else under a known client
// root falls back to its first path token, canonicalized.
func importService(importPath string) string {
	if importServiceNone[importPath] {
		return ""
	}
	best, bestLen := "", -1
	for prefix, svc := range importServicePrefixes {
		if importPath != prefix && !strings.HasPrefix(importPath, prefix+"/") {
			continue
		}
		if len(prefix) > bestLen {
			best, bestLen = svc, len(prefix)
		}
	}
	if bestLen >= 0 {
		return best
	}
	for _, root := range []string{"cloud.google.com/go/", "google.golang.org/api/"} {
		if rest, ok := strings.CutPrefix(importPath, root); ok {
			tok, _, _ := strings.Cut(rest, "/")
			return canonicalService(tok)
		}
	}
	return ""
}

// parseGoTestFile returns a test file's import paths and its top-level
// func Test... count. Import parsing (not text matching) is used so an aliased
// import (e.g. `dns "google.golang.org/api/dns/v1"`) is attributed correctly.
func parseGoTestFile(path string) ([]string, int, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, 0, err
	}
	var imports []string
	for _, imp := range f.Imports {
		if p, err := strconv.Unquote(imp.Path.Value); err == nil {
			imports = append(imports, p)
		}
	}
	funcs := 0
	for _, decl := range f.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Test") {
			funcs++
		}
	}
	return imports, funcs, nil
}

// scanGCPTestFiles walks root and returns one coverageTestFile per *_test.go
// file. A file is attributed to: (a) the service its `sdk-<service>` directory
// is dedicated to, and (b) every service named by its official-client imports.
func scanGCPTestFiles(root string) ([]coverageTestFile, error) {
	var out []coverageTestFile
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // a missing/unreadable path just yields no test files
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		dir := filepath.Dir(path)
		relDir, relErr := filepath.Rel(root, dir)
		if relErr != nil {
			relDir = "."
		}
		suite := filepath.ToSlash(relDir)
		svcs := map[string]bool{}
		if s := dedicatedSuiteService(filepath.Base(dir)); s != "" {
			svcs[s] = true
		}
		imports, funcs, perr := parseGoTestFile(path)
		if perr != nil {
			return nil
		}
		for _, imp := range imports {
			if s := importService(imp); s != "" {
				svcs[s] = true
			}
		}
		out = append(out, coverageTestFile{Suite: suite, Services: svcs, Funcs: funcs})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Suite != out[j].Suite {
			return out[i].Suite < out[j].Suite
		}
		return out[i].Funcs < out[j].Funcs
	})
	return out, nil
}

// coverageServiceNames is the inventory universe: every registry service plus
// every fidelity-matrix service. The union guarantees no matrix service is
// silently dropped — e.g. the shared `operations` (google.longrunning) LRO
// service has no registry descriptor but is a ga matrix service. A matrix name
// already covered by a registry alias is not duplicated (redis -> memorystore).
func coverageServiceNames(registry []string, facts map[string]matrixServiceFacts) []string {
	seenSvc := map[string]bool{}
	seenCanon := map[string]bool{}
	var out []string
	add := func(name string) {
		if name == "" || seenSvc[name] {
			return
		}
		seenSvc[name] = true
		seenCanon[canonicalService(name)] = true
		out = append(out, name)
	}
	for _, svc := range registry {
		add(svc)
	}
	var extra []string
	for svc := range facts {
		if !seenCanon[canonicalService(svc)] {
			extra = append(extra, svc)
		}
	}
	sort.Strings(extra)
	for _, svc := range extra {
		add(svc)
	}
	sort.Strings(out)
	return out
}

// gatherCoverage derives the inventory from the registry + matrix + test tree.
func gatherCoverage(matrixPath, testRoot string) ([]coverageRow, error) {
	raw, err := os.ReadFile(matrixPath)
	if err != nil {
		return nil, err
	}
	var mf matrixFile
	if err := json.Unmarshal(raw, &mf); err != nil {
		return nil, err
	}
	files, err := scanGCPTestFiles(testRoot)
	if err != nil {
		return nil, err
	}
	facts := matrixServiceFactsByService(mf)
	services := coverageServiceNames(gcpadapter.KnownServiceNames(), facts)
	return buildCoverage(services, facts, files), nil
}

// renderCoverageSection writes the derived behavioral-coverage section (used by
// the STATUS.md ledger output and the -coverage-inventory report). The p1–p5
// columns are the behavioural-depth matrix: `✓` = present, `exempt` = a
// reasoned/dated exemption, `**no**` = an unaccounted-for cell, `n/a` = non-`ga`.
func renderCoverageSection(b *strings.Builder, rows []coverageRow, exemptions map[string]string, depth map[string]depthRecord) {
	if len(rows) == 0 {
		return
	}
	gaCovered, gaTotal := gaCoverageTotals(rows)
	violations := coverageGateViolations(rows, exemptions)
	depthViolations := depthGateViolations(rows, depth)

	fmt.Fprintf(b, "## Behavioral coverage — registry service → suite (%d)\n\n", len(rows))
	fmt.Fprintf(b, "> Derived from the service registry (`internal/gcp/adapter`) + the fidelity matrix\n")
	fmt.Fprintf(b, "> (`%s`) + the `tests/integration/gcp/**` test tree. DoD: every `ga` service\n", defaultMatrixPath)
	fmt.Fprintf(b, "> needs ≥1 **official-client** behavioral suite or a reasoned exemption. **Gated** by\n")
	fmt.Fprintf(b, "> `make gcp-status-behavioral-gate` (CI); a service with neither fails.\n")
	fmt.Fprintf(b, "> Raw-REST probes that drive no official client (e.g. `serviceusage` in\n")
	fmt.Fprintf(b, "> `lro_async_test.go`) do not satisfy the DoD.\n")
	fmt.Fprintf(b, ">\n> `ga` = the raw matrix rule: ≥1 `ga` cell and no `preview` cell (a deliberately mechanical,\n")
	fmt.Fprintf(b, "> override-free reading of the per-operation states in `docs/gcp/wire-contract.md`, not the curated\n")
	fmt.Fprintf(b, "> `docs/gcp/testability.md` §2 tier). `limited`-only services (`bigquery`, `compute`, `clouddns`,\n")
	fmt.Fprintf(b, "> `cloudsql`, `memorystore`) and `preview` services (`iceberg`) are exempt.\n")
	fmt.Fprintf(b, "> `%d/%d` `ga` services have a suite", gaCovered, gaTotal)
	if len(exemptions) > 0 {
		fmt.Fprintf(b, "; %d carry a reasoned exemption", len(exemptions))
	}
	fmt.Fprintf(b, ".\n")
	if len(violations) > 0 {
		fmt.Fprintf(b, ">\n> **Gate violations — `ga` services with no suite and no exemption (%d): %s**\n",
			len(violations), strings.Join(violations, ", "))
	}
	fmt.Fprintf(b, ">\n> **Behavioural depth** (`p1`–`p5`, docs/gcp/testability.md §10): `p1` = the\n")
	fmt.Fprintf(b, "> lifecycle baseline; `p2` concurrency/OCC, `p3` errors/negatives, `p4` listing/paging,\n")
	fmt.Fprintf(b, "> `p5` scale/boundary. Each `ga` service records every class as `✓` (asserted by an\n")
	fmt.Fprintf(b, "> official-client suite) or `exempt` (reasoned + dated), gated by\n")
	fmt.Fprintf(b, "> `make gcp-status-depth-gate`. A `**no**` cell is unaccounted for.\n")
	if len(depthViolations) > 0 {
		fmt.Fprintf(b, ">\n> **Depth gate violations — unaccounted-for `ga` cells (%d): %s**\n",
			len(depthViolations), strings.Join(depthViolations, ", "))
	}
	b.WriteString("\n")
	fmt.Fprintf(b, "| service | canonical | transports | ga? | ga/total | suite(s) | funcs | covered | p1 | p2 | p3 | p4 | p5 |\n")
	fmt.Fprintf(b, "|---|---|---|---|---|---|---:|---|:-:|:-:|:-:|:-:|:-:|\n")
	for _, r := range rows {
		ga := "—"
		if r.GA {
			ga = "ga"
		}
		ratio := "—"
		if r.TotalCells > 0 {
			ratio = fmt.Sprintf("%d/%d", r.GACells, r.TotalCells)
		}
		suites := strings.Join(r.Suites, ", ")
		if suites == "" {
			suites = "—"
		}
		cov := "yes"
		if !r.Covered {
			switch {
			case r.GA && exemptions[canonicalService(r.Service)] != "":
				cov = "exempt"
			case r.GA:
				cov = "**no**"
			default:
				cov = "n/a"
			}
		}
		// p1 mirrors `covered`; p2–p5 come from the depth record. Non-ga
		// services are `n/a` (the depth contract only governs `ga`).
		p1 := "n/a"
		if r.GA {
			switch {
			case r.Covered:
				p1 = "✓"
			case exemptions[canonicalService(r.Service)] != "":
				p1 = "exempt"
			default:
				p1 = "**no**"
			}
		}
		cells := make([]string, 0, len(depthClasses))
		for _, class := range depthClasses {
			cells = append(cells, depthCellMark(r, class, depth))
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %d | %s | %s | %s |\n",
			esc(r.Service), esc(r.Canonical), esc(strings.Join(r.Transports, ", ")), ga, ratio, esc(suites), r.Funcs, cov,
			p1, strings.Join(cells, " | "))
	}
	b.WriteString("\n")
}

// depthCellMark renders one p2–p5 cell for the depth matrix: `✓` present,
// `exempt` reasoned gap, `**no**` an unaccounted-for `ga` cell, `n/a` non-`ga`.
func depthCellMark(r coverageRow, class string, depth map[string]depthRecord) string {
	if !r.GA {
		return "n/a"
	}
	rec, ok := depth[canonicalService(r.Service)]
	if !ok {
		return "**no**"
	}
	state, ok := rec.classState(class)
	if !ok {
		return "**no**"
	}
	switch state {
	case "present":
		return "✓"
	case "exempt":
		return "exempt"
	default:
		return "**no**"
	}
}
