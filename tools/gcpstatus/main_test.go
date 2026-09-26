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

// ─── classification ───────────────────────────────────────────────────────────

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		it   *Item
		want string
	}{
		{"stale-doc", &Item{State: "merged", DocState: "open"}, "stale-doc"},
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
	byID := map[string]*Item{}
	for _, it := range items {
		byID[it.ID] = it
	}
	w, ok := byID["W1.1"]
	if !ok || w.Kind != "wave" || w.Series != "x" || w.Branch != "feat/gcp-thing" {
		t.Fatalf("wave item = %+v", w)
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

	src := filepath.Join(root, "plan_docs", "J10-storage-csek-plan.md")
	if err := os.WriteFile(src, []byte("# plan\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Invalid ID is rejected before any move.
	if err := finalizePlan("plan_docs/J10-storage-csek-plan.md", "notanid", "", true); err == nil {
		t.Fatal("expected invalid-ID error")
	}
	// Dry run prints but does not move.
	if err := finalizePlan("plan_docs/J10-storage-csek-plan.md", "J10", "", true); err != nil {
		t.Fatalf("dry finalize: %v", err)
	}
	if _, err := os.Stat(src); err != nil {
		t.Fatalf("dry run moved the file: %v", err)
	}
	// Real move; the leading ID prefix is stripped, then re-prefixed.
	if err := finalizePlan("plan_docs/J10-storage-csek-plan.md", "J10", "", false); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	dest := filepath.Join(root, "plan_docs", "final", "J10-storage-csek-plan.md")
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("expected %s: %v", dest, err)
	}
	// A second finalize of the same ID collides.
	if err := os.WriteFile(src, []byte("# plan\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finalizePlan("plan_docs/J10-storage-csek-plan.md", "J10", "", false); err == nil {
		t.Fatal("expected destination-exists error")
	}
	// Explicit slug override is honored.
	src2 := filepath.Join(root, "plan_docs", "notes.md")
	if err := os.WriteFile(src2, []byte("# plan\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finalizePlan("plan_docs/notes.md", "R99", "custom-slug", false); err != nil {
		t.Fatalf("slug override: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "plan_docs", "final", "R99-custom-slug.md")); err != nil {
		t.Fatalf("expected R99-custom-slug.md: %v", err)
	}
}
