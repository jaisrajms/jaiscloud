// Command gcpstatus builds a status ledger for the GCP parity backlog.
//
// It parses every markdown table under plan_docs/ (the backlog/J/R/T/B/G/P
// items, the dual-protocol phase tracker, and the deferred-debt tables), derives
// what the docs say, and joins that with authoritative git/GitHub state (branch
// existence, PR open/merged). The result is written as plan_docs/STATUS.md (for
// humans) and plan_docs/status.json (machine-readable).
//
// Query mode lets a new session assess a proposed change against known state
// before starting:
//
//	go run ./tools/gcpstatus -query "datastore protobuf"
//	go run ./tools/gcpstatus -service monitoring -check
//
// With -check the exit code is 2 when a match is already merged/done and 3 when
// a matching branch/PR is in flight (0 otherwise), so a session or script can
// branch on it.
//
// plan_docs/ is gitignored: this tool and AGENTS.md are committed, the ledger is
// local scratch.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Item is one backlog entry with its derived state.
type Item struct {
	ID       string `json:"id"`
	Service  string `json:"service,omitempty"`
	Gap      string `json:"gap,omitempty"`
	Verdict  string `json:"verdict,omitempty"`
	Effort   string `json:"effort,omitempty"`
	Branch   string `json:"branch,omitempty"`
	Source   string `json:"source"`
	Archived bool   `json:"archived,omitempty"`
	DocState string `json:"docState,omitempty"`
	PRs      []int  `json:"prs,omitempty"`

	State    string `json:"state"`
	PR       int    `json:"pr,omitempty"`
	MergeSHA string `json:"mergeSha,omitempty"`
	PlanDoc  string `json:"planDoc,omitempty"`
	Note     string `json:"note,omitempty"`
}

// ghPR is the subset of `gh pr list --json` we use.
type ghPR struct {
	Number      int    `json:"number"`
	Title       string `json:"title"`
	HeadRefName string `json:"headRefName"`
	State       string `json:"state"` // OPEN | MERGED | CLOSED
	MergedAt    string `json:"mergedAt"`
	URL         string `json:"url"`
	MergeCommit *struct {
		OID string `json:"oid"`
	} `json:"mergeCommit"`
}

type table struct {
	headers []string
	rows    [][]string
	section string
}

var idRe = regexp.MustCompile(`^([A-Z]{1,3}[0-9]+(?:-[0-9]+)?)\b`)
var prWordRe = regexp.MustCompile(`(?i)\bPR\s*#?(\d+)`)
var prCellRe = regexp.MustCompile(`(\d+)`)
var sepRe = regexp.MustCompile(`^:?-{2,}:?$`)

func main() {
	docs := flag.String("docs", "plan_docs", "directory to scan for markdown plan docs")
	out := flag.String("out", "plan_docs/STATUS.md", "markdown ledger output ('' to skip)")
	jsonOut := flag.String("json", "plan_docs/status.json", "json ledger output ('' to skip)")
	prRepo := flag.String("pr-repo", "jaisrajms/jaiscloud", "GitHub repo for PR state ('' to use the current checkout)")
	query := flag.String("query", "", "assess a proposed change: free-text match over id/service/gap/verdict")
	service := flag.String("service", "", "filter query by service")
	check := flag.Bool("check", false, "with -query/-service, exit 2 if already done, 3 if in flight")
	includeArchive := flag.Bool("include-archive", false, "also parse plan_docs/archive/**")
	verbose := flag.Bool("v", false, "log parsing/PR diagnostics to stderr")
	flag.Parse()

	items, docPaths, err := collect(*docs, *includeArchive, *verbose)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gcpstatus: %v\n", err)
		os.Exit(1)
	}
	prs, branchSet := loadGitState(*prRepo, *verbose)
	enrich(items, prs, branchSet, docPaths)

	if *query != "" || *service != "" {
		os.Exit(runQuery(items, *query, *service, *check))
	}

	// The -check/query path prints its own result; the default path writes the
	// ledger and prints a summary.
	if *jsonOut != "" {
		if err := writeJSON(*jsonOut, items); err != nil {
			fmt.Fprintf(os.Stderr, "gcpstatus: %v\n", err)
			os.Exit(1)
		}
	}
	if *out != "" {
		if err := writeMarkdown(*out, items); err != nil {
			fmt.Fprintf(os.Stderr, "gcpstatus: %v\n", err)
			os.Exit(1)
		}
	}
	printSummary(items, *out, *jsonOut)
}

// ─── document parsing ─────────────────────────────────────────────────────────

func collect(root string, includeArchive, verbose bool) ([]*Item, []string, error) {
	var itemList []*Item
	var docPaths []string
	byID := map[string]*Item{} // dedupe within a source file

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(strings.ToLower(path), ".md") {
			return nil
		}
		// Never parse our own generated ledger (feedback loop).
		if strings.EqualFold(filepath.Base(path), "STATUS.md") {
			return nil
		}
		rel := filepath.ToSlash(path)
		archived := strings.Contains(rel, "/archive/")
		if archived && !includeArchive {
			return nil
		}
		docPaths = append(docPaths, rel)
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, t := range parseTables(string(content)) {
			for _, it := range itemsFromTable(t, rel, archived) {
				key := rel + "\x00" + it.ID
				if prev, ok := byID[key]; ok {
					mergeItem(prev, it)
				} else {
					byID[key] = it
					itemList = append(itemList, it)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if verbose {
		fmt.Fprintf(os.Stderr, "parsed %d docs, %d items\n", len(docPaths), len(itemList))
	}
	return itemList, docPaths, nil
}

// parseTables extracts markdown pipe tables (header + separator + rows) and
// records the nearest preceding heading as the table's section.
func parseTables(content string) []table {
	lines := strings.Split(content, "\n")
	var tables []table
	section := ""
	for i := 0; i+1 < len(lines); i++ {
		if h := heading(lines[i]); h != "" {
			section = h
			continue
		}
		if !isTableRow(lines[i]) {
			continue
		}
		if !isSeparatorRow(lines[i+1]) {
			continue
		}
		t := table{headers: splitRow(lines[i]), section: section}
		j := i + 2
		for ; j < len(lines) && isTableRow(lines[j]); j++ {
			if isSeparatorRow(lines[j]) {
				continue
			}
			cells := splitRow(lines[j])
			if len(cells) < len(t.headers) {
				cells = append(cells, make([]string, len(t.headers)-len(cells))...)
			}
			t.rows = append(t.rows, cells[:len(t.headers)])
		}
		tables = append(tables, t)
		i = j - 1
	}
	return tables
}

func heading(line string) string {
	s := strings.TrimSpace(line)
	n := 0
	for n < len(s) && s[n] == '#' {
		n++
	}
	if n == 0 || n > 6 || n >= len(s) || s[n] != ' ' {
		return ""
	}
	return stripFormatting(strings.TrimSpace(s[n:]))
}

func isTableRow(line string) bool {
	s := strings.TrimSpace(line)
	return strings.HasPrefix(s, "|") && strings.Count(s, "|") >= 2
}

func isSeparatorRow(line string) bool {
	cells := splitRow(line)
	if len(cells) == 0 {
		return false
	}
	for _, c := range cells {
		if !sepRe.MatchString(strings.TrimSpace(c)) {
			return false
		}
	}
	return true
}

func splitRow(line string) []string {
	parts := strings.Split(strings.TrimSpace(line), "|")
	if len(parts) > 0 && strings.TrimSpace(parts[0]) == "" {
		parts = parts[1:]
	}
	if len(parts) > 0 && strings.TrimSpace(parts[len(parts)-1]) == "" {
		parts = parts[:len(parts)-1]
	}
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func itemsFromTable(t table, source string, archived bool) []*Item {
	idx := map[string]int{}
	for i, h := range t.headers {
		idx[headerKey(h)] = i
	}
	// Only tables that expose item identity are backlog material.
	if _, ok := firstHeader(idx, "id", "#", "pri", "phase"); !ok {
		return nil
	}
	var out []*Item
	for _, row := range t.rows {
		id := idFromRow(idx, row)
		if id == "" {
			continue
		}
		it := &Item{ID: id, Source: source, Archived: archived}
		it.Service = firstNonEmpty(rowAt(idx, row, "service", "serviceop", "service(s)", "sevice"))
		it.Gap = firstNonEmpty(rowAt(idx, row, "gap", "operation", "method(s)", "method", "assertion", "scope", "what", "item"))
		it.Verdict = firstNonEmpty(rowAt(idx, row, "verdict", "decision"))
		it.Effort = firstNonEmpty(rowAt(idx, row, "effort", "estimate"))
		it.Branch = cleanBranch(firstNonEmpty(rowAt(idx, row, "prompt", "branch")))
		it.DocState = parseDocState(row, t.section)
		if it.Service == "" {
			it.Service = inferService(it.Gap + " " + it.Verdict)
		}
		for _, c := range row {
			for _, m := range prWordRe.FindAllStringSubmatch(c, -1) {
				if n := atoi(m[1]); n > 0 && !containsInt(it.PRs, n) {
					it.PRs = append(it.PRs, n)
				}
			}
		}
		// A dedicated PR column may hold a bare number.
		if i, ok := firstHeader(idx, "pr", "prs", "pullrequest"); ok && i < len(row) {
			if m := prCellRe.FindStringSubmatch(row[i]); m != nil {
				if n := atoi(m[1]); n > 0 && !containsInt(it.PRs, n) {
					it.PRs = append(it.PRs, n)
				}
			}
		}
		out = append(out, it)
	}
	return out
}

func idFromRow(idx map[string]int, row []string) string {
	// Prefer an explicit id column.
	if i, ok := firstHeader(idx, "id", "#"); ok && i < len(row) {
		if id := extractID(row[i]); id != "" {
			return id
		}
	}
	// A priority table's "pri" column is an index, not the item id; use an id
	// token inside the gap item ("R11 Datastore ...") when present.
	if pi, ok := idx["pri"]; ok {
		if gi, ok := idx["item"]; ok && gi < len(row) {
			if id := extractID(row[gi]); id != "" {
				return id
			}
		}
		if pi < len(row) {
			if id := extractID(row[pi]); id != "" {
				return id
			}
		}
		return ""
	}
	// Phase tracker rows carry the id in the "phase" cell ("**P0 infra**").
	if i, ok := idx["phase"]; ok && i < len(row) {
		return extractID(row[i])
	}
	return ""
}

func extractID(s string) string {
	s = stripFormatting(s)
	m := idRe.FindStringSubmatch(s)
	if m == nil {
		return ""
	}
	return m[1]
}

func stripFormatting(s string) string {
	r := strings.NewReplacer("*", "", "`", "", "__", "")
	return strings.TrimSpace(r.Replace(s))
}

// headerKey normalises a header for alias matching.
func headerKey(h string) string {
	h = stripFormatting(h)
	h = strings.ToLower(h)
	h = strings.NewReplacer(" ", "", "/", "", "_", "", "-", "", ".", "").Replace(h)
	return h
}

func firstHeader(idx map[string]int, keys ...string) (int, bool) {
	for _, k := range keys {
		if i, ok := idx[headerKey(k)]; ok {
			return i, true
		}
	}
	return 0, false
}

func rowAt(idx map[string]int, row []string, keys ...string) string {
	for _, k := range keys {
		if i, ok := idx[headerKey(k)]; ok && i < len(row) {
			if v := strings.TrimSpace(row[i]); v != "" && v != "—" && v != "-" {
				return v
			}
		}
	}
	return ""
}

func parseDocState(row []string, section string) string {
	s := strings.ToLower(strings.Join(row, " "))
	sec := strings.ToLower(section)
	// Sections that declare non-actionable work.
	if hasAny(sec, "non-compliant", "noncompliant", "out of scope", "not emulated", "not implemented at all", "explicitly non-ga", "deferred") {
		return "deferred"
	}
	switch {
	case hasAny(s, "wontfix", "won't fix", "no fix", "out of scope", "not planned", "not debt", "delete from plan", "do not change"):
		return "deferred"
	case hasAny(s, "deferred", "backlog"):
		return "deferred"
	// Negatives first: "unimplemented"/"not implemented" contain "implemented".
	case hasAny(s, "unimplemented", "not implemented", "not done", "still open", "not started", "todo", "pending", "open"):
		return "open"
	case hasAny(s, "merged", "landed", "shipped", "done", "implemented", "completed", "✅", "resolved"):
		return "done"
	}
	return ""
}

// inferService derives a service name from free text when the table has no
// service column. Order matters: longer/more specific keys first.
func inferService(s string) string {
	s = strings.ToLower(s)
	for _, kv := range []struct{ key, svc string }{
		{"iamcredentials", "iam"}, {"serviceaccount", "iam"}, {"service account", "iam"},
		{"datastore", "datastore"}, {"firestore", "firestore"}, {"gcs", "storage"}, {"storage", "storage"}, {"bucket", "storage"}, {"object", "storage"},
		{"pub/sub", "pubsub"}, {"pubsub", "pubsub"}, {"kms", "kms"}, {"secret", "secretmanager"},
		{"monitoring", "monitoring"}, {"logging", "logging"}, {"log", "logging"},
		{"eventarc", "eventarc"}, {"trigger", "eventarc"}, {"managed kafka", "managedkafka"}, {"kafka", "managedkafka"},
		{"metastore", "metastore"}, {"dataproc", "dataproc"}, {"workflow", "workflows"},
		{"resource manager", "resourcemanager"}, {"service usage", "serviceusage"},
		{"bigquery", "bigquery"}, {"cloud sql", "cloudsql"}, {"cloudsql", "cloudsql"},
		{"cloud dns", "clouddns"}, {"dns", "clouddns"}, {"compute", "compute"}, {"memorystore", "memorystore"}, {"redis", "memorystore"},
		{"gke", "container"}, {"container", "container"}, {"cloud run", "run"}, {"cloud tasks", "tasks"},
		{"scheduler", "scheduler"}, {"firebase", "firebaseauth"}, {"sts", "sts"}, {"oauth", "auth"},
		{"goreleaser", "release"}, {"dockerfile", "release"}, {"container image", "release"}, {"release notes", "release"},
	} {
		if strings.Contains(s, kv.key) {
			return kv.svc
		}
	}
	return ""
}

func hasAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func allTermsIn(s string, terms []string) bool {
	for _, t := range terms {
		if !strings.Contains(s, t) {
			return false
		}
	}
	return true
}

func mergeItem(dst, src *Item) {
	if dst.Service == "" {
		dst.Service = src.Service
	}
	if dst.Gap == "" {
		dst.Gap = src.Gap
	}
	if dst.Verdict == "" {
		dst.Verdict = src.Verdict
	}
	if dst.Effort == "" {
		dst.Effort = src.Effort
	}
	if dst.Branch == "" {
		dst.Branch = src.Branch
	}
	if dst.DocState == "" {
		dst.DocState = src.DocState
	}
	for _, n := range src.PRs {
		if !containsInt(dst.PRs, n) {
			dst.PRs = append(dst.PRs, n)
		}
	}
}

// ─── git / GitHub state ───────────────────────────────────────────────────────

func loadGitState(prRepo string, verbose bool) ([]ghPR, map[string]bool) {
	branches := map[string]bool{}
	if out, err := run("git", "branch", "-a", "--format=%(refname:short)"); err == nil {
		for _, b := range strings.Split(out, "\n") {
			b = strings.TrimSpace(b)
			if b == "" {
				continue
			}
			b = strings.TrimPrefix(b, "remotes/origin/")
			b = strings.TrimPrefix(b, "remotes/upstream/")
			branches[b] = true
		}
	} else if verbose {
		fmt.Fprintf(os.Stderr, "git branch: %v\n", err)
	}

	var prs []ghPR
	args := []string{"pr", "list", "--state", "all", "--limit", "1000",
		"--json", "number,title,headRefName,state,mergedAt,url,mergeCommit"}
	if prRepo != "" {
		args = append(args, "--repo", prRepo)
	}
	if out, err := run("gh", args...); err == nil {
		if err := json.Unmarshal([]byte(out), &prs); err != nil && verbose {
			fmt.Fprintf(os.Stderr, "gh pr list decode: %v\n", err)
		}
	} else if verbose {
		fmt.Fprintf(os.Stderr, "gh pr list: %v\n", err)
	}
	return prs, branches
}

func enrich(items []*Item, prs []ghPR, branches map[string]bool, docPaths []string) {
	for _, it := range items {
		var matched *ghPR
		merged := false
		open := false
		closed := false
		for i := range prs {
			pr := &prs[i]
			if !prMatchesItem(pr, it) {
				continue
			}
			switch strings.ToUpper(pr.State) {
			case "MERGED":
				merged = true
			case "OPEN":
				open = true
			case "CLOSED":
				closed = true
			}
			if matched == nil {
				matched = pr
			}
		}
		if merged {
			it.State = "merged"
			if matched != nil {
				it.PR = matched.Number
				if matched.MergeCommit != nil {
					it.MergeSHA = shortSHA(matched.MergeCommit.OID)
				}
			}
		} else if open {
			it.State = "pr"
			it.PR = matched.Number
		} else if branchExists(it.Branch, branches) {
			it.State = "branch"
		} else if closed {
			it.State = "closed"
		} else {
			it.State = deriveFromDocs(it)
		}
		it.PlanDoc = findPlanDoc(it, docPaths)
	}
}

func prMatchesItem(pr *ghPR, it *Item) bool {
	if containsInt(it.PRs, pr.Number) {
		return true
	}
	if it.Branch != "" {
		if pr.HeadRefName == it.Branch || strings.HasSuffix(pr.HeadRefName, "/"+it.Branch) ||
			strings.HasSuffix(it.Branch, "/"+pr.HeadRefName) {
			return true
		}
	}
	return containsIDToken(pr.Title, it.ID)
}

func deriveFromDocs(it *Item) string {
	switch it.DocState {
	case "done":
		return "done?"
	case "deferred":
		return "deferred"
	case "open":
		return "todo"
	default:
		return "todo"
	}
}

func branchExists(branch string, branches map[string]bool) bool {
	if branch == "" {
		return false
	}
	if branches[branch] {
		return true
	}
	for b := range branches {
		if strings.HasSuffix(b, "/"+branch) {
			return true
		}
	}
	return false
}

func findPlanDoc(it *Item, docPaths []string) string {
	slug := it.Branch
	slug = strings.TrimPrefix(slug, "fix/")
	slug = strings.TrimPrefix(slug, "feat/")
	best := ""
	for _, p := range docPaths {
		base := strings.ToLower(filepath.Base(p))
		if strings.Contains(base, "archive") {
			continue
		}
		if slug != "" && strings.Contains(base, strings.ToLower(slug)) {
			return p
		}
		if strings.Contains(base, strings.ToLower(it.ID)+"-") {
			best = p
		}
	}
	return best
}

// ─── output ───────────────────────────────────────────────────────────────────

func writeJSON(path string, items []*Item) error {
	sortItems(items)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func writeMarkdown(path string, items []*Item) error {
	sortItems(items)
	counts := stateCounts(items)
	var b strings.Builder
	fmt.Fprintf(&b, "# GCP parity status\n\n")
	fmt.Fprintf(&b, "Generated: %s · %d items\n\n", time.Now().Format("2006-01-02 15:04 MST"), len(items))
	fmt.Fprintf(&b, "Counts: %s\n\n", countsLine(counts))
	fmt.Fprintf(&b, "> Regenerate with `make gcp-status`. Sources: every `plan_docs/**/*.md` table plus git/GitHub state.\n")
	fmt.Fprintf(&b, "> `done?` = a doc claims done but no merged PR/branch was found; verify before trusting it.\n\n")

	sections := []struct {
		title string
		rank  map[string]bool
	}{
		{"Open / in flight", map[string]bool{"pr": true, "branch": true, "todo": true, "closed": true, "done?": true, "unknown": true}},
		{"Deferred / no fix", map[string]bool{"deferred": true}},
		{"Merged / done", map[string]bool{"merged": true, "done": true}},
	}
	written := map[string]bool{}
	for _, s := range sections {
		var rows []*Item
		for _, it := range items {
			if s.rank[it.State] {
				rows = append(rows, it)
				written[it.ID+"\x00"+it.Source] = true
			}
		}
		if len(rows) == 0 {
			continue
		}
		fmt.Fprintf(&b, "## %s (%d)\n\n", s.title, len(rows))
		writeTable(&b, rows)
		b.WriteString("\n")
	}
	var rest []*Item
	for _, it := range items {
		if !written[it.ID+"\x00"+it.Source] {
			rest = append(rest, it)
		}
	}
	if len(rest) > 0 {
		fmt.Fprintf(&b, "## Other (%d)\n\n", len(rest))
		writeTable(&b, rest)
		b.WriteString("\n")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func writeTable(b *strings.Builder, rows []*Item) {
	fmt.Fprintf(b, "| ID | service | state | PR | branch | gap | plan doc | source |\n")
	fmt.Fprintf(b, "|---|---|---|---|---|---|---|---|\n")
	for _, it := range rows {
		pr := ""
		if it.PR > 0 {
			pr = fmt.Sprintf("#%d", it.PR)
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s | %s |\n",
			esc(it.ID), esc(truncate(it.Service, 24)), esc(it.State), esc(pr),
			esc(it.Branch), esc(truncate(it.Gap, 70)), esc(it.PlanDoc), esc(shortSource(it.Source)))
	}
}

func printSummary(items []*Item, out, jsonOut string) {
	counts := stateCounts(items)
	fmt.Printf("gcp-status: %d items — %s\n", len(items), countsLine(counts))
	if out != "" {
		fmt.Printf("  wrote %s\n", out)
	}
	if jsonOut != "" {
		fmt.Printf("  wrote %s\n", jsonOut)
	}
}

func stateCounts(items []*Item) map[string]int {
	c := map[string]int{}
	for _, it := range items {
		c[it.State]++
	}
	return c
}

func countsLine(c map[string]int) string {
	order := []string{"todo", "pr", "branch", "done?", "closed", "merged", "done", "deferred", "unknown"}
	var parts []string
	for _, k := range order {
		if c[k] > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", k, c[k]))
		}
	}
	return strings.Join(parts, " ")
}

func sortItems(items []*Item) {
	rank := map[string]int{"pr": 0, "branch": 1, "todo": 2, "closed": 3, "done?": 4, "deferred": 5, "merged": 6, "done": 7, "unknown": 8}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if rank[a.State] != rank[b.State] {
			return rank[a.State] < rank[b.State]
		}
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return a.Source < b.Source
	})
}

// ─── query mode ───────────────────────────────────────────────────────────────

func runQuery(items []*Item, query, service string, check bool) int {
	terms := strings.Fields(strings.ToLower(strings.TrimSpace(query)))
	svc := strings.ToLower(strings.TrimSpace(service))
	var matches []*Item
	for _, it := range items {
		if svc != "" && !strings.Contains(strings.ToLower(it.Service), svc) {
			continue
		}
		if len(terms) > 0 {
			hay := strings.ToLower(strings.Join([]string{it.ID, it.Service, it.Gap, it.Verdict, it.Branch}, " "))
			if !allTermsIn(hay, terms) {
				continue
			}
		}
		matches = append(matches, it)
	}
	sortItems(matches)

	if len(matches) == 0 {
		fmt.Println("status: no matching tracked item (likely new work)")
		return 0
	}
	done, inflight := false, false
	for _, it := range matches {
		pr := ""
		if it.PR > 0 {
			pr = fmt.Sprintf(" PR #%d", it.PR)
		}
		fmt.Printf("%-8s %-14s %-9s%s  %s\n", it.ID, truncate(it.Service, 14), it.State, pr, truncate(it.Gap, 80))
		fmt.Printf("         source: %s\n", shortSource(it.Source))
		if it.PlanDoc != "" {
			fmt.Printf("         plan doc: %s\n", it.PlanDoc)
		}
		switch it.State {
		case "merged", "done":
			done = true
		case "pr", "branch", "done?":
			inflight = true
		}
	}
	if check {
		switch {
		case done:
			fmt.Println("=> ASSESSMENT: already done/merged — do not re-implement; reuse or verify.")
			return 2
		case inflight:
			fmt.Println("=> ASSESSMENT: in flight — coordinate with the existing branch/PR.")
			return 3
		}
	}
	return 0
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func run(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

func cleanBranch(s string) string {
	s = stripFormatting(s)
	s = strings.Trim(s, "`")
	if s == "" || s == "—" || s == "-" {
		return ""
	}
	if !strings.ContainsAny(s, "/") && !strings.HasPrefix(s, "gcp-") {
		return ""
	}
	if strings.ContainsAny(s, " ") {
		// e.g. a prose "prompt" cell; take the first token that looks like a branch.
		for _, f := range strings.Fields(s) {
			if strings.Contains(f, "/") {
				return strings.Trim(f, "`,")
			}
		}
		return ""
	}
	return s
}

func containsIDToken(s, id string) bool {
	if id == "" {
		return false
	}
	re := regexp.MustCompile(`\b` + regexp.QuoteMeta(id) + `\b`)
	return re.MatchString(s)
}

func containsInt(xs []int, n int) bool {
	for _, x := range xs {
		if x == n {
			return true
		}
	}
	return false
}

func atoi(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		n = n*10 + int(r-'0')
	}
	return n
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func shortSHA(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

func shortSource(p string) string {
	p = strings.TrimPrefix(p, "plan_docs/")
	return strings.TrimSuffix(filepath.Base(p), ".md")
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "|", "/")
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

func esc(s string) string { return s }
