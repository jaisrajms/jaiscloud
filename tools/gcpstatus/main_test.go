package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// ─── parsing ──────────────────────────────────────────────────────────────────

func TestParseTables(t *testing.T) {
	md := "# Section\n\n| A | B |\n|---|---|\n| 1 | 2 |\n| 3 | 4 |\n\ntext"
	ts := parseTables(md)
	if len(ts) != 1 {
		t.Fatalf("tables = %d, want 1", len(ts))
	}
	if !reflect.DeepEqual(ts[0].headers, []string{"A", "B"}) {
		t.Fatalf("headers = %v", ts[0].headers)
	}
	if len(ts[0].rows) != 2 || ts[0].rows[1][0] != "3" {
		t.Fatalf("rows = %v", ts[0].rows)
	}
	if ts[0].section != "Section" {
		t.Fatalf("section = %q, want Section", ts[0].section)
	}
}

func TestParseStatusCell(t *testing.T) {
	cases := []struct {
		name               string
		status             string
		row                []string
		wantState, wantDsp string
	}{
		{"done pr", "✅ **DONE (#131)**", nil, "done", "fix"},
		{"done lower", "done (#47)", nil, "done", "fix"},
		{"still open with pr", "still open (deferred surfaces; #62 fixed the routing 404 → explicit 501)", nil, "open", "fix"},
		{"not debt", "**not debt — matches real GCP**", []string{"", "delete from plan"}, "deferred", "no-fix"},
		{"implemented", "implemented", nil, "done", "fix"},
		{"unimplemented", "unimplemented", nil, "open", "fix"},
		{"empty", "", nil, "", "fix"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, disp := parseStatusCell(tc.status, tc.row)
			if state != tc.wantState || disp != tc.wantDsp {
				t.Fatalf("parseStatusCell(%q) = (%q,%q), want (%q,%q)", tc.status, state, disp, tc.wantState, tc.wantDsp)
			}
		})
	}
}

func TestParseDocState(t *testing.T) {
	cases := []struct {
		name    string
		row     []string
		section string
		want    string
	}{
		{"non-compliant section", []string{"check the thing"}, "TEST-NON-COMPLIANT — do NOT change", "deferred"},
		{"out of scope section", []string{"cloud run"}, "Not planned (out of scope per GA.md §7)", "deferred"},
		{"unimplemented", []string{"Unimplemented"}, "", "open"},
		{"still open", []string{"still open"}, "", "open"},
		{"done", []string{"DONE (#127)"}, "", "done"},
		{"deferred word", []string{"deferred"}, "", "deferred"},
		{"nothing", []string{"foo"}, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseDocState(tc.row, tc.section); got != tc.want {
				t.Fatalf("parseDocState(%v,%q) = %q, want %q", tc.row, tc.section, got, tc.want)
			}
		})
	}
}

func TestDispositionFrom(t *testing.T) {
	cases := []struct {
		verdict, section, want string
	}{
		{"fix", "", "fix"},
		{"fix (follow-up)", "", "follow-up"},
		{"**no fix** — accept + document", "", "no-fix"},
		{"optional", "", "optional"},
		{"", "TEST-NON-COMPLIANT (real GCP wins)", "no-fix"},
		{"", "Deferred items", "unknown"},
	}
	for _, tc := range cases {
		if got := dispositionFrom(tc.verdict, tc.section); got != tc.want {
			t.Errorf("dispositionFrom(%q,%q) = %q, want %q", tc.verdict, tc.section, got, tc.want)
		}
	}
}

// ─── ids / aliases ────────────────────────────────────────────────────────────

func TestExpandIDs(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"B2-10", []string{"B2-10"}},                             // dashed single id, not a range
		{"J12-J16", []string{"J12", "J13", "J14", "J15", "J16"}}, // hyphen range
		{"R14–R18", []string{"R14", "R15", "R16", "R17", "R18"}}, // en-dash range
		{"J22", []string{"J22"}},
	}
	for _, tc := range cases {
		if got := expandIDs(tc.in); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("expandIDs(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseAliasGroups(t *testing.T) {
	cases := []struct {
		in   string
		want [][]string
	}{
		{"J22/B2-10/R22", [][]string{{"J22", "B2-10", "R22"}}},
		{"J2/R2, J11/R10", [][]string{{"J2", "R2"}, {"J11", "R10"}}},
		{"J12-J16/R14-R18", [][]string{{"J12", "R14"}, {"J13", "R15"}, {"J14", "R16"}, {"J15", "R17"}, {"J16", "R18"}}},
	}
	for _, tc := range cases {
		if got := parseAliasGroups(tc.in); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parseAliasGroups(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestExtractID(t *testing.T) {
	cases := map[string]string{
		"R11 Datastore REST protobuf bodies": "R11",
		"**P0 infra**":                       "P0",
		"B2-10 iamcredentials":               "B2-10",
		"no id here":                         "",
	}
	for in, want := range cases {
		if got := extractID(in); got != want {
			t.Errorf("extractID(%q) = %q, want %q", in, got, want)
		}
	}
}

// ─── ordering ─────────────────────────────────────────────────────────────────

func TestWaveOrder(t *testing.T) {
	cases := map[string]int{"W2.3": 203, "W10.1": 1001, "P1": 0, "": 0}
	for in, want := range cases {
		if got := waveOrder(in); got != want {
			t.Errorf("waveOrder(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestAssignOrders(t *testing.T) {
	items := []*Item{
		{ID: "W1.1", Kind: "wave", Series: "java-compat", Wave: "W1.1"},
		{ID: "W1.1", Kind: "wave", Series: "bigquery-ga", Wave: "W1.1"},
		{ID: "B1", Kind: "backlog", Pri: 5}, // no wave -> Order untouched
	}
	assignOrders(items, []string{"java-compat"})
	if items[0].Order != 101 {
		t.Errorf("java-compat order = %d, want 101", items[0].Order)
	}
	if items[1].Order != 100101 {
		t.Errorf("bigquery-ga order = %d, want 100101", items[1].Order)
	}
	if items[2].Order != 0 {
		t.Errorf("non-wave order = %d, want 0", items[2].Order)
	}
}

func TestAnnotateWavesCopiesService(t *testing.T) {
	items := []*Item{
		// Empty inferred service -> filled from the aliased detail row.
		{ID: "W1.1", Kind: "wave", Aliases: []string{"FP1"}},
		{ID: "FP1", Kind: "backlog", Service: "functions"},
		// Wrong inferred service (e.g. "storage" from "GCS") -> overridden by
		// the authoritative detail service.
		{ID: "W4.2", Kind: "wave", Service: "storage", Aliases: []string{"FD3"}},
		{ID: "FD3", Kind: "backlog", Service: "functions"},
		// No aliased item with a service -> the inferred value stands.
		{ID: "W9.9", Kind: "wave", Service: "compute", Aliases: []string{"ZZ9"}},
		// Multiple aliases: the first one with a service wins (deterministic).
		{ID: "W3.7", Kind: "wave", Aliases: []string{"J20", "J21"}},
		{ID: "J20", Kind: "backlog", Service: "iam"},
		{ID: "J21", Kind: "backlog", Service: "auth"},
		// Impact is still copied when the wave has none.
		{ID: "W2.2", Kind: "wave", Aliases: []string{"FD5"}},
		{ID: "FD5", Kind: "backlog", Service: "functions", Impact: "medium"},
	}
	annotateWaves(items)

	byID := map[string]*Item{}
	for _, it := range items {
		byID[it.ID] = it
	}
	if got := byID["W1.1"].Service; got != "functions" {
		t.Errorf("W1.1 service = %q, want functions (filled from alias)", got)
	}
	if got := byID["W4.2"].Service; got != "functions" {
		t.Errorf("W4.2 service = %q, want functions (authoritative override)", got)
	}
	if got := byID["W9.9"].Service; got != "compute" {
		t.Errorf("W9.9 service = %q, want compute (no alias service)", got)
	}
	if got := byID["W3.7"].Service; got != "iam" {
		t.Errorf("W3.7 service = %q, want iam (first non-empty alias)", got)
	}
	if got := byID["W2.2"].Impact; got != "medium" {
		t.Errorf("W2.2 impact = %q, want medium (copied from alias)", got)
	}
}

// ─── classification ───────────────────────────────────────────────────────────

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		it   *Item
		want string
	}{
		{"stale-doc", &Item{State: "merged", DocState: "open"}, "stale-doc"},
		{"merged wave-done", &Item{State: "merged", DocState: "open", WaveDone: true}, "done"},
		{"merged", &Item{State: "merged"}, "done"},
		{"intentional", &Item{State: "todo", Disposition: "no-fix"}, "intentional"},
		{"optional intentional", &Item{State: "todo", Disposition: "optional"}, "intentional"},
		{"claimed-done", &Item{State: "done?"}, "claimed-done"},
		{"abandoned branch", &Item{State: "branch"}, "abandoned"},
		{"scheduled", &Item{State: "todo", Disposition: "fix", Branch: "feat/x"}, "scheduled"},
		{"oversight", &Item{State: "todo", Disposition: "fix", WaveDone: true}, "oversight?"},
		{"unowned debt", &Item{State: "todo", Disposition: "fix", Kind: "debt"}, "unowned"},
		{"unscheduled backlog", &Item{State: "todo", Disposition: "fix", Kind: "backlog"}, "unscheduled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classify(tc.it); got != tc.want {
				t.Fatalf("classify(%+v) = %q, want %q", tc.it, got, tc.want)
			}
		})
	}
}

// ─── matrix ───────────────────────────────────────────────────────────────────

func TestMatrixItemsFrom(t *testing.T) {
	mf := matrixFile{Cells: []matrixCell{
		{Service: "datastore", Operation: "Datastore.Lookup", Transport: "grpc", State: "ga"},
		{Service: "bigquery", Operation: "BigQuery.Query", Transport: "rest", State: "preview", Reason: "no SQL engine"},
		{Service: "iceberg", Operation: "Iceberg.List", Transport: "rest", State: "unsupported"},
	}}
	got := matrixItemsFrom(mf)
	if len(got) != 2 {
		t.Fatalf("matrix items = %d, want 2 (ga skipped)", len(got))
	}
	for _, it := range got {
		if it.Kind != "matrix" {
			t.Errorf("%s kind = %q, want matrix", it.ID, it.Kind)
		}
		if it.State == "ga" {
			t.Errorf("%s is ga, should have been skipped", it.ID)
		}
	}
	if got[0].Disposition != "fix" || got[1].Disposition != "unknown" {
		t.Errorf("dispositions = %q,%q", got[0].Disposition, got[1].Disposition)
	}
}

func TestStateRank(t *testing.T) {
	cases := map[string]int{"ga": 3, "limited": 2, "preview": 1, "unsupported": 0, "bogus": 0}
	for in, want := range cases {
		if got := stateRank(in); got != want {
			t.Errorf("stateRank(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestDiffMatrices(t *testing.T) {
	old := matrixFile{Cells: []matrixCell{
		{Service: "s", Operation: "o", Transport: "rest", State: "ga"},
	}}
	cur := matrixFile{Cells: []matrixCell{
		{Service: "s", Operation: "o", Transport: "rest", State: "limited"},     // regression
		{Service: "s", Operation: "n", Transport: "grpc", State: "unsupported"}, // new gap
		{Service: "s", Operation: "m", Transport: "grpc", State: "ga"},          // new ga -> ignored
	}}
	regress, added := diffMatrices(old, cur)
	if len(regress) != 1 || !strings.Contains(regress[0], "ga -> limited") {
		t.Fatalf("regress = %v", regress)
	}
	if len(added) != 1 || !strings.Contains(added[0], "unsupported") {
		t.Fatalf("added = %v", added)
	}
}

// ─── PR backfill ──────────────────────────────────────────────────────────────

func TestBackfillPRs(t *testing.T) {
	items := []*Item{
		{ID: "J1", Branch: "fix/x"},
		{ID: "J2", PRs: []int{100}},
	}
	prs := []ghPR{
		{Number: 100, Title: "landed (J2)", HeadRefName: "other", State: "MERGED"},
		{Number: 200, Title: "unrelated change", HeadRefName: "feat/y", State: "MERGED"},
	}
	out := backfillPRs(items, prs, "", nil)
	if len(out) != 1 {
		t.Fatalf("backfilled rows = %d, want 1", len(out))
	}
	if out[0].ID != "PR200" || out[0].State != "merged" {
		t.Fatalf("row = %s/%s, want PR200/merged", out[0].ID, out[0].State)
	}
}

// TestFindPlanDocIDCollision guards the J40 cross-service collision: an ID
// reused by two services (firestore and monitoring) must not let either row
// inherit the other service's `<ID>-<slug>.md` doc. The ID fallback only binds
// a doc whose declared service matches, and otherwise falls back to the row's
// source doc.
func TestFindPlanDocIDCollision(t *testing.T) {
	fsDoc := "plan_docs/final/J40-firestore-delete-precondition.md"
	monDoc := "plan_docs/final/J12-monitoring-java-gaps.md"
	newDoc := "plan_docs/gcp-monitoring-alignment-grid-wave-plan.md"
	docPaths := []string{fsDoc, monDoc, newDoc}
	docService := serviceIndexByDoc([]*Item{
		{ID: "J40", Service: "firestore", Source: fsDoc},
		{ID: "J40", Service: "monitoring", Source: monDoc},
	})

	mon := &Item{ID: "J40", Service: "monitoring", Source: monDoc}
	if got := findPlanDoc(mon, docPaths, docService); got != monDoc {
		t.Fatalf("monitoring J40 plan doc = %q, want %q", got, monDoc)
	}
	fs := &Item{ID: "J40", Service: "firestore", Source: fsDoc}
	if got := findPlanDoc(fs, docPaths, docService); got != fsDoc {
		t.Fatalf("firestore J40 plan doc = %q, want %q", got, fsDoc)
	}
	// The branch slug still wins over the ID heuristic.
	mon.Branch = "fix/gcp-monitoring-alignment-grid"
	if got := findPlanDoc(mon, docPaths, docService); got != newDoc {
		t.Fatalf("branch-slug plan doc = %q, want %q", got, newDoc)
	}
}

// ─── table predicates ─────────────────────────────────────────────────────────
func TestTablePredicates(t *testing.T) {
	if !isWaveTable([]string{"Wave", "Session", "IDs", "Service(s)", "Branch", "Depends on"}) {
		t.Error("isWaveTable should be true for the template headers")
	}
	if isWaveTable([]string{"ID", "service", "gap"}) {
		t.Error("isWaveTable should be false without session/ids/branch")
	}
	if !isStatusTable([]string{"Item", "Status", "Evidence", "Needed", "Effort"}) {
		t.Error("isStatusTable should be true for the debt table")
	}
	if isStatusTable([]string{"ID", "Status", "Evidence"}) {
		t.Error("isStatusTable should be false when an id column exists")
	}
	for _, p := range []string{"gcp-x-wave-plan.md", "gcp-bigquery-ga-plan.md", "a-plan-b.md"} {
		if !isPlanShaped(p) {
			t.Errorf("isPlanShaped(%q) = false, want true", p)
		}
	}
	if isPlanShaped("gcp-notes.md") {
		t.Error("isPlanShaped(gcp-notes.md) = true, want false")
	}
}

// ─── helpers + end-to-end ─────────────────────────────────────────────────────

func TestSlugAndHash(t *testing.T) {
	if got := slug("BigQuery.CreateDataset"); got != "bigquery-createdataset" {
		t.Errorf("slug = %q", got)
	}
	h1, h2 := shortHash("x"), shortHash("x")
	if h1 != h2 || len(h1) != 8 {
		t.Errorf("shortHash not stable/8: %q %q", h1, h2)
	}
	if shortHash("x") == shortHash("y") {
		t.Error("shortHash collision for x/y")
	}
}

func TestCollectEndToEnd(t *testing.T) {
	dir := t.TempDir()
	md := `# X wave plan

| Wave | Session | IDs | Service(s) | Branch | Depends on |
|---|---|---|---|---|---|
| 1 | W1.1 | T1 | svc thing | ` + "`feat/gcp-thing`" + ` | — |

| ID | service | gap | impact | effort | verdict | prompt |
|---|---|---|---|---|---|---|
| T1 | svc | do the thing | high | S | fix | ` + "`feat/gcp-thing`" + ` |
`
	if err := os.WriteFile(filepath.Join(dir, "gcp-x-wave-plan.md"), []byte(md), 0o644); err != nil {
		t.Fatal(err)
	}
	items, docPaths, waves := collect(dir, false, false)
	if len(docPaths) != 1 {
		t.Fatalf("docPaths = %v", docPaths)
	}
	annotateWaves(items)
	byID := map[string]*Item{}
	for _, it := range items {
		byID[it.ID] = it
	}
	w, ok := byID["W1.1"]
	if !ok || w.Kind != "wave" || w.Series != "x" || w.Branch != "feat/gcp-thing" {
		t.Fatalf("wave item = %+v", w)
	}
	// The wave row's Service(s) text ("svc thing") yields no inferService match,
	// so the service is backfilled from the aliased detail row (T1 -> svc).
	if w.Service != "svc" {
		t.Fatalf("wave service = %q, want svc (from aliased detail row)", w.Service)
	}
	b, ok := byID["T1"]
	if !ok || b.Kind != "backlog" || b.Disposition != "fix" {
		t.Fatalf("backlog item = %+v", b)
	}
	if len(waves.groups) == 0 || waves.waveByAlias["T1"] != "W1.1" {
		t.Fatalf("wave metadata missing: %+v", waves)
	}
}

func TestFinalizePlan(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "plan_docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(wd) }()

	plan := "# plan\n\n| Wave | Session | IDs | Service(s) | Branch | Depends on |\n" +
		"|---|---|---|---|---|---|\n| 1 | W1.1 | J10 | storage csek | `feat/gcp-storage-csek` | — |\n"
	src := filepath.Join(root, "plan_docs", "J10-storage-csek-plan.md")
	if err := os.WriteFile(src, []byte(plan), 0o644); err != nil {
		t.Fatal(err)
	}

	// Invalid ID is rejected before any move.
	if err := finalizePlan("plan_docs/J10-storage-csek-plan.md", "notanid", "", 0, true); err == nil {
		t.Fatal("expected invalid-ID error")
	}
	// Dry run prints but does not move (and reports the row close).
	if err := finalizePlan("plan_docs/J10-storage-csek-plan.md", "J10", "", 10, true); err != nil {
		t.Fatalf("dry finalize: %v", err)
	}
	if _, err := os.Stat(src); err != nil {
		t.Fatalf("dry run moved the file: %v", err)
	}
	// Real move; the leading ID prefix is stripped, then re-prefixed, and the
	// index row is closed with the merged PR.
	if err := finalizePlan("plan_docs/J10-storage-csek-plan.md", "J10", "", 10, false); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	dest := filepath.Join(root, "plan_docs", "final", "J10-storage-csek-plan.md")
	b, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("expected %s: %v", dest, err)
	}
	if !strings.Contains(string(b), "DONE #10") {
		t.Fatalf("finalize did not close the index row:\n%s", b)
	}
	// A -finalize-pr with no matching index row is an error.
	if err := os.WriteFile(src, []byte(plan), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finalizePlan("plan_docs/J10-storage-csek-plan.md", "J99", "", 10, false); err == nil {
		t.Fatal("expected no-matching-row error")
	}
	// A second finalize of the same ID collides.
	if err := os.WriteFile(src, []byte(plan), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finalizePlan("plan_docs/J10-storage-csek-plan.md", "J10", "", 0, false); err == nil {
		t.Fatal("expected destination-exists error")
	}
	// Explicit slug override is honored.
	src2 := filepath.Join(root, "plan_docs", "notes.md")
	if err := os.WriteFile(src2, []byte("# plan\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finalizePlan("plan_docs/notes.md", "R99", "custom-slug", 0, false); err != nil {
		t.Fatalf("slug override: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "plan_docs", "final", "R99-custom-slug.md")); err != nil {
		t.Fatalf("expected R99-custom-slug.md: %v", err)
	}
}

// TestPrMatchesItem pins the merge-link rules: the declared Branch is the
// authoritative anchor, aliases never link a merge, and the canonical ID in the
// title only applies to branch-less rows.
func TestPrMatchesItem(t *testing.T) {
	// The #180 regression: a tooling PR whose title quotes an alias (B2-10) must
	// not satisfy the iamcredentials item.
	iamcred := &Item{ID: "J22", Branch: "feat/gcp-iamcredentials", Aliases: []string{"J22", "B2-10", "R22"}}
	toolingPR := &ghPR{
		Number:      180,
		Title:       "fix(gcp): don't expand a dashed id (B2-10) as a range (#180)",
		HeadRefName: "fix/gcp-status-id-expand",
		State:       "MERGED",
	}
	if prMatchesItem(toolingPR, iamcred) {
		t.Error("alias quoted in a meta PR title must not link a merge")
	}
	// The real branch is the anchor.
	branchPR := &ghPR{Number: 999, Title: "feat(gcp): iamcredentials impersonation", HeadRefName: "feat/gcp-iamcredentials", State: "MERGED"}
	if !prMatchesItem(branchPR, iamcred) {
		t.Error("declared branch must link the merge")
	}
	// An explicit PR number still links.
	if !prMatchesItem(&ghPR{Number: 180, HeadRefName: "unrelated"}, &Item{ID: "J22", PRs: []int{180}}) {
		t.Error("explicit PRs must link")
	}
	// Branch-less rows fall back to the canonical ID, not aliases.
	branchless := &Item{ID: "FP4", Aliases: []string{"FP4", "J2"}}
	if !prMatchesItem(&ghPR{Number: 1, Title: "docs(gcp): note (FP4)"}, branchless) {
		t.Error("branch-less canonical ID in title must link")
	}
	if prMatchesItem(&ghPR{Number: 2, Title: "docs(gcp): note (J2)"}, branchless) {
		t.Error("branch-less alias in title must not link")
	}
}

// TestCloseFinalizedRow checks the finalize-close rewrite of wave index rows.
func TestCloseFinalizedRow(t *testing.T) {
	md := "# plan\n\n| Wave | Session | IDs | Service(s) | Branch | Depends on |\n" +
		"|---|---|---|---|---|---|\n" +
		"| 1 | W1.1 | J10 | storage csek | `feat/gcp-storage-csek` | — |\n" +
		"| 1 | W1.2 | J12-J13/R14-R15 | monitoring | `fix/gcp-monitoring` | W1.1 |\n" +
		"| 1 | W1.3 | R16 | monitoring ops | `fix/gcp-monitoring` | W1.1 |\n"
	got, ok := closeFinalizedRow(md, "R14", 194)
	if !ok {
		t.Fatal("expected the R14 row to match via its alias group")
	}
	// Every row on the finalized branch (W1.2 and W1.3) is closed.
	if strings.Count(got, "DONE #194") != 2 {
		t.Fatalf("branch-sharing rows not closed together:\n%s", got)
	}
	// The unrelated branch row keeps its — cell.
	if !strings.Contains(got, "storage csek | `feat/gcp-storage-csek` | — |") {
		t.Fatalf("unrelated row was modified:\n%s", got)
	}
	if _, ok := closeFinalizedRow(md, "ZZ9", 1); ok {
		t.Error("absent ID must not match")
	}
}

// TestResolvedOverlay checks that a row is closed only with merged-PR evidence.
func TestResolvedOverlay(t *testing.T) {
	entries := []resolvedEntry{
		{Source: "gcp-terraform", Match: "project-level-iam", PR: 151, Note: "done in #151"},
	}
	prs := []ghPR{{Number: 151, State: "MERGED"}, {Number: 200, State: "OPEN"}}

	// Not yet merged -> untouched.
	open := &Item{ID: "prose:project-level-iam-x", State: "todo", DocState: "open", Disposition: "fix", Source: "plan_docs/gcp-terraform-compat.md"}
	applyResolved([]*Item{open}, []resolvedEntry{{Match: "project-level-iam", PR: 200, Note: "x"}}, prs)
	if open.State != "todo" || open.DocState != "open" {
		t.Fatalf("unmerged PR must not close a row: %+v", open)
	}
	// Merged + matching -> closed.
	closed := &Item{ID: "prose:project-level-iam-x", State: "todo", DocState: "open", Disposition: "fix", Source: "plan_docs/gcp-terraform-compat-2026-09-22.md"}
	applyResolved([]*Item{closed}, entries, prs)
	if closed.State != "done" || closed.DocState != "done" || closed.Note == "" {
		t.Fatalf("merged overlay entry must close the row: %+v", closed)
	}
	// Source filter excludes non-matching docs.
	other := &Item{ID: "prose:project-level-iam-y", State: "todo", DocState: "open", Source: "plan_docs/other.md"}
	applyResolved([]*Item{other}, entries, prs)
	if other.State != "todo" {
		t.Fatalf("source filter must exclude non-matching docs: %+v", other)
	}
}

// TestRunQueryCheckPrefersIdentityMatch: a merged item that only mentions the
// query word in its gap must not make an open identity/service match look done.
func TestRunQueryCheckPrefersIdentityMatch(t *testing.T) {
	items := []*Item{
		{ID: "LG1", Service: "gcpstatus", State: "merged", Kind: "backlog", Gap: "falsely satisfies `iamcredentials`"},
		{ID: "W3.2", Service: "iamcredentials", State: "todo", Kind: "backlog", Gap: "iamcredentials"},
	}
	if code := runQuery(items, "iamcredentials", "", true); code != 0 {
		t.Fatalf("check = %d, want 0 (open identity match masked by a done gap-only hit)", code)
	}
	only := []*Item{{ID: "LG1", Service: "gcpstatus", State: "merged", Kind: "backlog", Gap: "mentions iamcredentials"}}
	if code := runQuery(only, "iamcredentials", "", true); code != 2 {
		t.Fatalf("check = %d, want 2 (fallback to all matches when no identity match)", code)
	}
}

// TestLoadResolved validates overlay parsing + required fields.
func TestLoadResolved(t *testing.T) {
	if got, err := loadResolved(""); err != nil || got != nil {
		t.Fatalf("empty path = %v, %v", got, err)
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "resolved.yaml")
	if err := os.WriteFile(p, []byte("entries:\n  - match: foo\n    pr: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := loadResolved(p)
	if err != nil || len(got) != 1 || got[0].Match != "foo" || got[0].PR != 1 {
		t.Fatalf("loadResolved = %+v, %v", got, err)
	}
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("entries:\n  - pr: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadResolved(bad); err == nil {
		t.Fatal("expected error for entry without a match")
	}
}

// TestWaveDoneScopedPerPlan pins the per-plan keying of wave done/order/series:
// a "W1.1" marked DONE in one family must not leak WaveDone into another
// family's W1.1 detail row.
func TestWaveDoneScopedPerPlan(t *testing.T) {
	dir := t.TempDir()
	planA := "# a wave plan\n\n| Wave | Session | IDs | Service(s) | Branch | Depends on |\n" +
		"|---|---|---|---|---|---|\n| 1 | W1.1 | AA1 | a | `feat/a` | DONE #1 |\n\n" +
		"| ID | service | gap | impact | effort | verdict | prompt |\n" +
		"|---|---|---|---|---|---|---|\n| AA1 | a | do a | low | S | fix | `feat/a` |\n"
	planB := "# b wave plan\n\n| Wave | Session | IDs | Service(s) | Branch | Depends on |\n" +
		"|---|---|---|---|---|---|\n| 1 | W1.1 | BB1 | b | `feat/b` | — |\n\n" +
		"| ID | service | gap | impact | effort | verdict | prompt |\n" +
		"|---|---|---|---|---|---|---|\n| BB1 | b | do b | low | S | fix | `feat/b` |\n"
	if err := os.WriteFile(filepath.Join(dir, "gcp-a-wave-plan.md"), []byte(planA), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "gcp-b-wave-plan.md"), []byte(planB), 0o644); err != nil {
		t.Fatal(err)
	}
	items, _, waves := collect(dir, false, false)
	items = applyWaveAliases(items, waves)
	byID := map[string]*Item{}
	for _, it := range items {
		byID[it.ID] = it
	}
	if byID["AA1"] == nil || !byID["AA1"].WaveDone {
		t.Fatalf("AA1 should be wave-done: %+v", byID["AA1"])
	}
	if byID["BB1"] == nil || byID["BB1"].WaveDone {
		t.Fatalf("BB1 inherited another plan's wave-done: %+v", byID["BB1"])
	}
}

// TestSupersededMarker verifies a marked doc contributes a record row, not
// live backlog, and stays accounted for by coverage.
func TestSupersededMarker(t *testing.T) {
	dir := t.TempDir()
	doc := "# superseded report\n\n" + supersededMarker + "\n\n| ID | service | gap | impact | effort | verdict | prompt |\n" +
		"|---|---|---|---|---|---|---|\n| X1 | svc | still open thing | low | S | fix | `feat/x` |\n"
	if err := os.WriteFile(filepath.Join(dir, "gcp-old-results.md"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	items, _, _ := collect(dir, false, false)
	if len(items) != 1 || items[0].Kind != "superseded" {
		t.Fatalf("superseded doc items = %+v, want one superseded record", items)
	}
	classifyAll(items)
	if items[0].Class != "superseded" {
		t.Fatalf("class = %q, want superseded", items[0].Class)
	}
}
