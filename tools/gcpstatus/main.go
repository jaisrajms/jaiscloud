// Command gcpstatus builds a status ledger for the GCP parity backlog.
//
// It parses every markdown table under plan_docs/ (backlog J/R/T/B/G/P items,
// the dual-protocol phase tracker, the Java wave plan, deferred-debt status
// tables, and the authoritative "what's actually left" bullet list), derives
// what the docs say, joins it with authoritative git/GitHub state, and adds a
// row for every base-gcp PR not already represented, so nothing merged to gcp
// is invisible.
//
// Outputs: plan_docs/STATUS.md (human) + plan_docs/status.json (machine).
//
// Modes:
//
//	make gcp-status                                  rebuild the ledger
//	make gcp-status-check Q="datastore protobuf"     assess a change (2=done,3=in flight)
//	make gcp-status-audit                            classify not-done items
//
// plan_docs/ is gitignored: this tool and AGENTS.md are committed, the ledger
// is local scratch.
package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
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

// Item is one tracked entry with its derived state.
type Item struct {
	ID          string   `json:"id"`
	Aliases     []string `json:"aliases,omitempty"`
	Service     string   `json:"service,omitempty"`
	Gap         string   `json:"gap,omitempty"`
	Verdict     string   `json:"verdict,omitempty"`
	Effort      string   `json:"effort,omitempty"`
	Branch      string   `json:"branch,omitempty"`
	Source      string   `json:"source"`
	Kind        string   `json:"kind,omitempty"` // backlog | debt | remainder | pr
	Archived    bool     `json:"archived,omitempty"`
	DocState    string   `json:"docState,omitempty"`
	Disposition string   `json:"disposition,omitempty"` // fix | follow-up | no-fix | optional | unknown
	Wave        string   `json:"wave,omitempty"`
	WaveDone    bool     `json:"waveDone,omitempty"`
	PRs         []int    `json:"prs,omitempty"`

	State    string `json:"state"`
	PR       int    `json:"pr,omitempty"`
	MergeSHA string `json:"mergeSha,omitempty"`
	PlanDoc  string `json:"planDoc,omitempty"`
	Note     string `json:"note,omitempty"`
	Class    string `json:"class,omitempty"`
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

// waveMeta captures the Java wave-plan index: alias→branch, alias→wave, and
// which waves are already done, plus the alias groups used to merge J/R pairs.
type waveMeta struct {
	groups        [][]string
	branchByAlias map[string]string
	waveByAlias   map[string]string
	doneWaves     map[string]bool
}

func newWaveMeta() *waveMeta {
	return &waveMeta{
		branchByAlias: map[string]string{},
		waveByAlias:   map[string]string{},
		doneWaves:     map[string]bool{},
	}
}

var idRe = regexp.MustCompile(`^([A-Z]{1,3}[0-9]+(?:-[0-9]+)?)\b`)
var idTokenRe = regexp.MustCompile(`[A-Z]{1,3}[0-9]+(?:-[0-9]+)?`)
var rangeIDRe = regexp.MustCompile(`([A-Z]{1,3})([0-9]+)\s*[-\x{2013}]\s*([A-Z]{1,3})?([0-9]+)`)
var prWordRe = regexp.MustCompile(`(?i)\bPR\s*#?(\d+)`)
var barePRRe = regexp.MustCompile(`#(\d+)`)
var sectionRefRe = regexp.MustCompile(`§[0-9]+(?:\.[0-9]+)?`)
var sepRe = regexp.MustCompile(`^:?-{2,}:?$`)

func main() {
	docs := flag.String("docs", "plan_docs", "directory to scan for markdown plan docs")
	out := flag.String("out", "plan_docs/STATUS.md", "markdown ledger output ('' to skip)")
	jsonOut := flag.String("json", "plan_docs/status.json", "json ledger output ('' to skip)")
	prRepo := flag.String("pr-repo", "jaisrajms/jaiscloud", "GitHub repo for PR state ('' = current checkout)")
	includePRs := flag.Bool("pr", true, "backfill every base-gcp PR not already represented as a row")
	prPrefixes := flag.String("pr-prefixes", "", "comma-separated branch prefixes to keep for PR-only rows (default all)")
	query := flag.String("query", "", "assess a proposed change: free-text match over id/service/gap/verdict")
	service := flag.String("service", "", "filter query by service")
	check := flag.Bool("check", false, "with -query/-service, exit 2 if already done, 3 if in flight")
	audit := flag.Bool("audit", false, "print the audit classification of not-done items")
	includeArchive := flag.Bool("include-archive", false, "also parse plan_docs/archive/**")
	verbose := flag.Bool("v", false, "log parsing/PR diagnostics to stderr")
	flag.Parse()

	items, docPaths, waves := collect(*docs, *includeArchive, *verbose)
	items = applyWaveAliases(items, waves)

	prs, branchSet := loadGitState(*prRepo, *verbose)
	enrich(items, prs, branchSet, docPaths)
	if *includePRs {
		items = append(items, backfillPRs(items, prs, *prPrefixes, docPaths)...)
	}
	classifyAll(items)
	sortItems(items)

	if *audit {
		runAudit(items)
		return
	}
	if *query != "" || *service != "" {
		os.Exit(runQuery(items, *query, *service, *check))
	}
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

func collect(root string, includeArchive, verbose bool) ([]*Item, []string, *waveMeta) {
	var itemList []*Item
	var docPaths []string
	waves := newWaveMeta()
	byKey := map[string]*Item{} // dedupe within a source file

	add := func(it *Item) {
		key := it.Source + "\x00" + it.ID
		if prev, ok := byKey[key]; ok {
			mergeItem(prev, it)
			return
		}
		byKey[key] = it
		itemList = append(itemList, it)
	}

	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(strings.ToLower(path), ".md") {
			return nil
		}
		if strings.EqualFold(filepath.Base(path), "STATUS.md") {
			return nil // never parse our own output
		}
		rel := filepath.ToSlash(path)
		archived := strings.Contains(rel, "/archive/")
		if archived && !includeArchive {
			return nil
		}
		docPaths = append(docPaths, rel)
		content, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		for _, t := range parseTables(string(content)) {
			switch {
			case isWaveTable(t.headers):
				waveFromTable(t, waves)
			case isStatusTable(t.headers):
				for _, it := range debtItemsFromTable(t, rel) {
					add(it)
				}
			default:
				for _, it := range itemsFromTable(t, rel, archived) {
					add(it)
				}
			}
		}
		for _, it := range parseRemainder(string(content), rel) {
			add(it)
		}
		return nil
	})
	if verbose {
		fmt.Fprintf(os.Stderr, "parsed %d docs, %d items\n", len(docPaths), len(itemList))
	}
	return itemList, docPaths, waves
}

func parseTables(content string) []table {
	lines := strings.Split(content, "\n")
	var tables []table
	section := ""
	for i := 0; i+1 < len(lines); i++ {
		if h := heading(lines[i]); h != "" {
			section = h
			continue
		}
		if !isTableRow(lines[i]) || !isSeparatorRow(lines[i+1]) {
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

func headerIndex(headers []string) map[string]int {
	idx := map[string]int{}
	for i, h := range headers {
		idx[headerKey(h)] = i
	}
	return idx
}

func isWaveTable(headers []string) bool {
	idx := headerIndex(headers)
	_, s := idx["session"]
	_, i := idx["ids"]
	_, b := idx["branch"]
	return s && i && b
}

func isStatusTable(headers []string) bool {
	idx := headerIndex(headers)
	if _, ok := firstHeader(idx, "id", "#", "pri", "phase", "session", "ids"); ok {
		return false
	}
	_, status := idx["status"]
	_, item := idx["item"]
	_, ev := idx["evidence"]
	_, needed := idx["needed"]
	return status && (item || ev || needed)
}

func waveFromTable(t table, w *waveMeta) {
	idx := headerIndex(t.headers)
	si := idx["session"]
	ii := idx["ids"]
	for _, row := range t.rows {
		session := cell(row, si)
		if session == "" {
			continue
		}
		branch := cleanBranch(rowAt(idx, row, "branch"))
		dep := rowAt(idx, row, "dependson", "depends", "status")
		done := strings.Contains(strings.ToUpper(dep), "DONE")
		for _, g := range parseAliasGroups(cell(row, ii)) {
			w.groups = append(w.groups, g)
			for _, id := range g {
				if branch != "" {
					w.branchByAlias[id] = branch
				}
				w.waveByAlias[id] = session
			}
			if done {
				w.doneWaves[session] = true
			}
		}
	}
}

func debtItemsFromTable(t table, source string) []*Item {
	idx := headerIndex(t.headers)
	ii := idx["item"]
	var out []*Item
	for _, row := range t.rows {
		itemCell := cell(row, ii)
		if itemCell == "" {
			continue
		}
		statusCell := ""
		if si, ok := idx["status"]; ok {
			statusCell = cell(row, si)
		}
		state, disp := parseStatusCell(statusCell, row)
		gap := stripFormatting(itemCell)
		it := &Item{
			ID:          "debt:" + slug(gap) + "-" + shortHash(source+"|"+gap),
			Kind:        "debt",
			Source:      source,
			Gap:         gap,
			Verdict:     stripFormatting(statusCell),
			DocState:    state,
			Disposition: disp,
		}
		it.Service = inferService(itemCell + " " + statusCell)
		it.Aliases = sectionRefs(itemCell + " " + strings.Join(row, " "))
		for _, m := range barePRRe.FindAllStringSubmatch(statusCell, -1) {
			if n := atoi(m[1]); n > 0 && !containsInt(it.PRs, n) {
				it.PRs = append(it.PRs, n)
			}
		}
		out = append(out, it)
	}
	return out
}

// parseStatusCell returns state + disposition from a debt Status column. A
// no-fix marker (from the row's Needed text) forces "deferred"; otherwise the
// Status cell decides (negatives before positives, so "unimplemented" is open).
func parseStatusCell(status string, row []string) (state, disp string) {
	status = stripFormatting(status)
	all := strings.ToLower(status + " " + strings.Join(row, " "))
	disp = "fix"
	switch {
	case hasAny(all, "not debt", "delete from plan", "no fix", "won't fix", "wontfix", "out of scope", "by design", "by-design", "gold-plating", "gold plating", "not planned"):
		disp = "no-fix"
	case hasAny(all, "optional"):
		disp = "optional"
	}
	ls := strings.ToLower(status)
	switch {
	case disp == "no-fix":
		return "deferred", disp
	case hasAny(ls, "unimplemented", "not implemented", "still open", "not done", "pending", "open"):
		return "open", disp
	case hasAny(ls, "done", "implemented", "merged", "✅", "resolved", "complete"):
		return "done", disp
	case strings.Contains(ls, "#"):
		return "done", disp
	default:
		return "", disp
	}
}

// parseRemainder extracts the debt-plan "What's actually left (authoritative)"
// bullet list (strikethrough + DONE #N = closed; out-of-scope scope = deferred).
func parseRemainder(content, source string) []*Item {
	lines := strings.Split(content, "\n")
	start := -1
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "#") && strings.Contains(strings.ToLower(l), "what's actually left") {
			start = i + 1
			break
		}
	}
	if start < 0 {
		return nil
	}
	scopeDeferred := false
	var out []*Item
	for _, l := range lines[start:] {
		t := strings.TrimSpace(l)
		if strings.HasPrefix(t, "## ") {
			break
		}
		if t == "" {
			continue
		}
		low := strings.ToLower(t)
		if strings.HasPrefix(t, "**") && strings.Contains(t, "**:") || (strings.HasPrefix(t, "**") && strings.HasSuffix(t, "**")) {
			scopeDeferred = hasAny(low, "out of scope", "large", "not emulated")
			continue
		}
		if !strings.HasPrefix(t, "- ") && !strings.HasPrefix(t, "* ") {
			continue
		}
		text := strings.TrimSpace(t[2:])
		done := strings.Contains(text, "~~") || containsIDToken(low, "done") || strings.Contains(low, "**done")
		gap := stripFormatting(strings.ReplaceAll(text, "~~", ""))
		it := &Item{
			ID:       "left:" + slug(gap) + "-" + shortHash(source+"|"+gap),
			Kind:     "remainder",
			Source:   source,
			Gap:      gap,
			DocState: "open",
		}
		switch {
		case done:
			it.DocState, it.Disposition = "done", "unknown"
		case scopeDeferred:
			it.DocState, it.Disposition = "deferred", "no-fix"
		default:
			it.DocState, it.Disposition = "open", "fix"
		}
		it.Service = inferService(gap)
		it.Aliases = sectionRefs(text)
		for _, m := range barePRRe.FindAllStringSubmatch(text, -1) {
			if n := atoi(m[1]); n > 0 && !containsInt(it.PRs, n) {
				it.PRs = append(it.PRs, n)
			}
		}
		out = append(out, it)
	}
	return out
}

func itemsFromTable(t table, source string, archived bool) []*Item {
	idx := headerIndex(t.headers)
	if _, ok := firstHeader(idx, "id", "#", "pri", "phase"); !ok {
		return nil
	}
	var out []*Item
	for _, row := range t.rows {
		id := idFromRow(idx, row)
		if id == "" {
			continue
		}
		it := &Item{ID: id, Source: source, Kind: "backlog", Archived: archived}
		it.Service = firstNonEmpty(rowAt(idx, row, "service", "serviceop", "service(s)", "sevice", "whereservice"))
		it.Gap = firstNonEmpty(rowAt(idx, row, "gap", "operation", "assertion", "scope", "what", "class", "method(s)", "method", "item"))
		it.Verdict = firstNonEmpty(rowAt(idx, row, "verdict", "decision"))
		it.Effort = firstNonEmpty(rowAt(idx, row, "effort", "estimate"))
		it.Branch = cleanBranch(firstNonEmpty(rowAt(idx, row, "prompt", "branch")))
		it.DocState = parseDocState(row, t.section)
		it.Disposition = dispositionFrom(it.Verdict, t.section)
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
		if i, ok := firstHeader(idx, "pr", "prs", "pullrequest"); ok {
			if m := barePRRe.FindStringSubmatch(cell(row, i)); m != nil {
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
	if i, ok := firstHeader(idx, "id", "#"); ok {
		if id := extractID(cell(row, i)); id != "" {
			return id
		}
	}
	if pi, ok := idx["pri"]; ok {
		if gi, ok := idx["item"]; ok {
			if id := extractID(cell(row, gi)); id != "" {
				return id
			}
		}
		return extractID(cell(row, pi))
	}
	if i, ok := idx["phase"]; ok {
		return extractID(cell(row, i))
	}
	return ""
}

func extractID(s string) string {
	m := idRe.FindStringSubmatch(stripFormatting(s))
	if m == nil {
		return ""
	}
	return m[1]
}

func stripFormatting(s string) string {
	return strings.TrimSpace(strings.NewReplacer("*", "", "`", "", "__", "").Replace(s))
}

func headerKey(h string) string {
	h = strings.ToLower(stripFormatting(h))
	return strings.NewReplacer(" ", "", "/", "", "_", "", "-", "", ".", "", "(", "", ")", "").Replace(h)
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
		if i, ok := idx[headerKey(k)]; ok {
			if v := strings.TrimSpace(cell(row, i)); v != "" && v != "—" && v != "-" {
				return v
			}
		}
	}
	return ""
}

func cell(row []string, i int) string {
	if i < 0 || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

func parseDocState(row []string, section string) string {
	s := strings.ToLower(strings.Join(row, " "))
	sec := strings.ToLower(section)
	if hasAny(sec, "non-compliant", "noncompliant", "out of scope", "not emulated", "not implemented at all", "explicitly non-ga", "deferred items") {
		return "deferred"
	}
	switch {
	case hasAny(s, "wontfix", "won't fix", "no fix", "out of scope", "not planned", "not debt", "delete from plan", "do not change"):
		return "deferred"
	case hasAny(s, "deferred", "backlog"):
		return "deferred"
	case hasAny(s, "unimplemented", "not implemented", "not done", "still open", "not started", "todo", "pending", "open"):
		return "open"
	case hasAny(s, "merged", "landed", "shipped", "done", "implemented", "completed", "✅", "resolved"):
		return "done"
	}
	return ""
}

func dispositionFrom(verdict, section string) string {
	sec := strings.ToLower(section)
	if hasAny(sec, "non-compliant", "noncompliant", "out of scope", "not emulated", "not implemented at all", "explicitly non-ga") {
		return "no-fix"
	}
	v := strings.ToLower(verdict)
	switch {
	case hasAny(v, "no fix", "won't fix", "wontfix", "out of scope", "not planned"):
		return "no-fix"
	case hasAny(v, "optional"):
		return "optional"
	case hasAny(v, "follow-up", "follow up"):
		return "follow-up"
	case hasAny(v, "fix"):
		return "fix"
	case hasAny(strings.ToLower(section), "deferred"):
		return "unknown"
	default:
		return "unknown"
	}
}

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
		{"scheduler", "scheduler"}, {"firebase", "firebaseauth"}, {"sts", "sts"}, {"oauth", "oauth"},
		{"goreleaser", "release"}, {"dockerfile", "release"}, {"container image", "release"}, {"release notes", "release"},
	} {
		if strings.Contains(s, kv.key) {
			return kv.svc
		}
	}
	return ""
}

// ─── aliases / merging ────────────────────────────────────────────────────────

func parseAliasGroups(cellValue string) [][]string {
	var groups [][]string
	for _, part := range strings.Split(cellValue, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		sides := strings.Split(part, "/")
		expanded := make([][]string, 0, len(sides))
		for _, s := range sides {
			expanded = append(expanded, expandIDs(s))
		}
		equal := true
		for _, e := range expanded {
			if len(e) != len(expanded[0]) {
				equal = false
				break
			}
		}
		if len(expanded) > 1 && equal {
			for i := range expanded[0] {
				g := make([]string, 0, len(expanded))
				for _, e := range expanded {
					g = append(g, e[i])
				}
				groups = append(groups, g)
			}
		} else {
			var g []string
			for _, e := range expanded {
				g = append(g, e...)
			}
			if len(g) > 0 {
				groups = append(groups, g)
			}
		}
	}
	return groups
}

func expandIDs(s string) []string {
	s = strings.TrimSpace(s)
	if m := rangeIDRe.FindStringSubmatch(s); m != nil {
		pre := m[1]
		start, end := atoi(m[2]), atoi(m[4])
		if end >= start && m[3] == "" || m[3] == m[1] {
			var out []string
			for i := start; i <= end; i++ {
				out = append(out, fmt.Sprintf("%s%d", pre, i))
			}
			return out
		}
	}
	return idTokenRe.FindAllString(s, -1)
}

func applyWaveAliases(items []*Item, w *waveMeta) []*Item {
	uf := newUF()
	inWave := map[string]bool{}
	for _, g := range w.groups {
		for _, id := range g {
			uf.find(id)
			inWave[id] = true
		}
		for i := 1; i < len(g); i++ {
			uf.union(g[0], g[i])
		}
	}
	buckets := map[string][]*Item{}
	var out []*Item
	for _, it := range items {
		if inWave[it.ID] {
			r := uf.find(it.ID)
			buckets[r] = append(buckets[r], it)
		} else {
			out = append(out, it)
		}
	}
	for _, group := range buckets {
		best := pickCanonical(group)
		for _, it := range group {
			if it != best {
				mergeItem(best, it)
			}
		}
		var members []string
		for _, it := range group {
			members = append(members, it.ID)
			members = append(members, it.Aliases...)
		}
		for _, m := range members {
			if m != best.ID {
				best.Aliases = appendUnique(best.Aliases, m)
			}
		}
		if b, ok := anyAlias(w.branchByAlias, members); ok && b != "" {
			if best.Branch != "" && best.Branch != b {
				best.Note = appendNote(best.Note, "branch: "+b+" (wave plan)")
			}
			best.Branch = b
		}
		if wv, ok := anyAlias(w.waveByAlias, members); ok {
			best.Wave = wv
			best.WaveDone = w.doneWaves[wv]
		}
		out = append(out, best)
	}
	return out
}

func pickCanonical(group []*Item) *Item {
	best := group[0]
	score := func(it *Item) int {
		s := 0
		if it.Branch != "" {
			s += 4
		}
		if it.Kind == "backlog" {
			s += 2
		}
		if !strings.HasPrefix(it.ID, "R") && !strings.HasPrefix(it.ID, "B2-") {
			s += 1
		}
		return s
	}
	for _, it := range group[1:] {
		if score(it) > score(best) || (score(it) == score(best) && it.ID < best.ID) {
			best = it
		}
	}
	return best
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
	if dst.Disposition == "" || dst.Disposition == "unknown" {
		dst.Disposition = src.Disposition
	}
	if dst.DocState == "" {
		dst.DocState = src.DocState
	}
	for _, n := range src.PRs {
		if !containsInt(dst.PRs, n) {
			dst.PRs = append(dst.PRs, n)
		}
	}
	for _, a := range src.Aliases {
		dst.Aliases = appendUnique(dst.Aliases, a)
	}
	if src.Kind == "backlog" && dst.Kind != "backlog" {
		dst.Kind = src.Kind
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
		merged, open, closed := false, false, false
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
		switch {
		case merged:
			it.State = "merged"
			if matched != nil {
				it.PR = matched.Number
				if matched.MergeCommit != nil {
					it.MergeSHA = shortSHA(matched.MergeCommit.OID)
				}
			}
		case open:
			it.State = "pr"
			it.PR = matched.Number
		case branchExists(it.Branch, branches):
			it.State = "branch"
		case closed:
			it.State = "closed"
		default:
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
	if containsIDToken(pr.Title, it.ID) {
		return true
	}
	for _, a := range it.Aliases {
		if containsIDToken(pr.Title, a) {
			return true
		}
	}
	return false
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
		if slug != "" && strings.Contains(base, strings.ToLower(slug)) {
			return p
		}
		if strings.Contains(base, strings.ToLower(it.ID)+"-") {
			best = p
		}
	}
	return best
}

// backfillPRs adds a row for every base-gcp PR not already matched to an item.
func backfillPRs(items []*Item, prs []ghPR, prefixes string, docPaths []string) []*Item {
	matched := map[int]bool{}
	for _, it := range items {
		for i := range prs {
			if prMatchesItem(&prs[i], it) {
				matched[prs[i].Number] = true
			}
		}
	}
	keep := map[string]bool{}
	for _, p := range strings.Split(prefixes, ",") {
		if p = strings.TrimSpace(p); p != "" {
			keep[p] = true
		}
	}
	var out []*Item
	for i := range prs {
		pr := &prs[i]
		if matched[pr.Number] {
			continue
		}
		if len(keep) > 0 && !keep[strings.SplitN(pr.HeadRefName, "/", 2)[0]] {
			continue
		}
		state := "open"
		switch strings.ToUpper(pr.State) {
		case "MERGED":
			state = "merged"
		case "CLOSED":
			state = "closed"
		}
		it := &Item{
			ID:       fmt.Sprintf("PR%d", pr.Number),
			Kind:     "pr",
			Source:   "github:pr",
			Gap:      pr.Title,
			Branch:   pr.HeadRefName,
			PR:       pr.Number,
			State:    state,
			Service:  inferService(pr.Title),
			DocState: "done",
		}
		if pr.MergeCommit != nil {
			it.MergeSHA = shortSHA(pr.MergeCommit.OID)
		}
		it.PlanDoc = findPlanDoc(it, docPaths)
		out = append(out, it)
	}
	return out
}

// ─── audit ────────────────────────────────────────────────────────────────────

func classifyAll(items []*Item) {
	for _, it := range items {
		if it.Kind == "pr" {
			it.Class = "pr"
			continue
		}
		it.Class = classify(it)
	}
}

func classify(it *Item) string {
	if it.State == "merged" || it.State == "done" {
		return "done"
	}
	switch it.Disposition {
	case "no-fix", "optional":
		return "intentional"
	}
	switch it.State {
	case "done?":
		return "claimed-done"
	case "branch", "closed":
		return "abandoned"
	}
	if it.Branch != "" {
		return "scheduled"
	}
	if it.WaveDone {
		return "oversight?"
	}
	switch it.Kind {
	case "debt":
		return "unowned"
	default:
		return "unscheduled"
	}
}

func runAudit(items []*Item) {
	order := []string{"oversight?", "unowned", "abandoned", "claimed-done", "unscheduled", "scheduled", "intentional", "done"}
	counts := map[string]int{}
	byClass := map[string][]*Item{}
	for _, it := range items {
		counts[it.Class]++
		byClass[it.Class] = append(byClass[it.Class], it)
	}
	fmt.Printf("gcp-status audit: %d items\n", len(items))
	for _, c := range order {
		if counts[c] > 0 {
			fmt.Printf("  %-14s %d\n", c, counts[c])
		}
	}
	// Detail for the actionable classes.
	for _, c := range []string{"oversight?", "unowned", "abandoned", "claimed-done", "unscheduled"} {
		rows := byClass[c]
		if len(rows) == 0 {
			continue
		}
		fmt.Printf("\n[%s] (%d)\n", c, len(rows))
		for _, it := range rows {
			disp := it.Disposition
			if disp == "" {
				disp = "unknown"
			}
			fmt.Printf("  %-16s %-12s disp=%-9s %-9s %s\n", it.ID, truncate(it.Service, 12), disp, it.State, truncate(it.Gap, 64))
		}
	}
}

// ─── output ───────────────────────────────────────────────────────────────────

func writeJSON(path string, items []*Item) error {
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
	counts := classCounts(items)
	var backlog, prs []*Item
	for _, it := range items {
		if it.Kind == "pr" {
			prs = append(prs, it)
		} else {
			backlog = append(backlog, it)
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# GCP parity status\n\n")
	fmt.Fprintf(&b, "Generated: %s · %d items (%d backlog + %d PR history)\n\n", time.Now().Format("2006-01-02 15:04 MST"), len(items), len(backlog), len(prs))
	fmt.Fprintf(&b, "Audit: %s\n\n", countsLine(counts))
	fmt.Fprintf(&b, "> Regenerate with `make gcp-status`; audit with `make gcp-status-audit`. Sources: every\n")
	fmt.Fprintf(&b, "> `plan_docs/**/*.md` table + the debt-plan remainder list + base-gcp PRs, joined with git.\n")
	fmt.Fprintf(&b, "> `state` is evidence-derived (merged/branch/pr from git/gh); `disp` is the declared intent\n")
	fmt.Fprintf(&b, "> (fix/no-fix/optional); `done?` = a doc claims done but no merged PR/branch was found.\n\n")

	sections := []struct {
		title string
		rank  map[string]bool
	}{
		{"Open / in flight", map[string]bool{"pr": true, "branch": true, "todo": true, "closed": true, "done?": true, "unknown": true}},
		{"Deferred / no fix", map[string]bool{"deferred": true}},
		{"Merged / done", map[string]bool{"merged": true, "done": true}},
	}
	written := map[*Item]bool{}
	for _, s := range sections {
		var rows []*Item
		for _, it := range backlog {
			if s.rank[it.State] {
				rows = append(rows, it)
			}
		}
		if len(rows) == 0 {
			continue
		}
		fmt.Fprintf(&b, "## %s (%d)\n\n", s.title, len(rows))
		writeTable(&b, rows)
		b.WriteString("\n")
		for _, it := range rows {
			written[it] = true
		}
	}
	var rest []*Item
	for _, it := range backlog {
		if !written[it] {
			rest = append(rest, it)
		}
	}
	if len(rest) > 0 {
		fmt.Fprintf(&b, "## Other backlog (%d)\n\n", len(rest))
		writeTable(&b, rest)
		b.WriteString("\n")
	}
	if len(prs) > 0 {
		fmt.Fprintf(&b, "## PR history — no backlog row (%d)\n\n", len(prs))
		fmt.Fprintf(&b, "| ID | state | PR | branch | title | plan doc |\n|---|---|---|---|---|---|\n")
		for _, it := range prs {
			fmt.Fprintf(&b, "| %s | %s | #%d | %s | %s | %s |\n",
				it.ID, it.State, it.PR, esc(it.Branch), esc(truncate(it.Gap, 64)), esc(it.PlanDoc))
		}
		b.WriteString("\n")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func writeTable(b *strings.Builder, rows []*Item) {
	fmt.Fprintf(b, "| ID | service | state | disp | PR | branch | gap | plan doc | source |\n")
	fmt.Fprintf(b, "|---|---|---|---|---|---|---|---|---|\n")
	for _, it := range rows {
		pr := ""
		if it.PR > 0 {
			pr = fmt.Sprintf("#%d", it.PR)
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			esc(it.ID), esc(truncate(it.Service, 22)), esc(it.State), esc(it.Disposition), esc(pr),
			esc(it.Branch), esc(truncate(it.Gap, 66)), esc(it.PlanDoc), esc(shortSource(it.Source)))
	}
}

func printSummary(items []*Item, out, jsonOut string) {
	backlog, prs := 0, 0
	for _, it := range items {
		if it.Kind == "pr" {
			prs++
		} else {
			backlog++
		}
	}
	fmt.Printf("gcp-status: %d items (%d backlog + %d PR) — %s\n", len(items), backlog, prs, countsLine(classCounts(items)))
	if out != "" {
		fmt.Printf("  wrote %s\n", out)
	}
	if jsonOut != "" {
		fmt.Printf("  wrote %s\n", jsonOut)
	}
}

func classCounts(items []*Item) map[string]int {
	c := map[string]int{}
	for _, it := range items {
		c[it.Class]++
	}
	return c
}

func countsLine(c map[string]int) string {
	order := []string{"oversight?", "unowned", "abandoned", "claimed-done", "unscheduled", "scheduled", "intentional", "done"}
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
			hay := strings.ToLower(strings.Join([]string{it.ID, strings.Join(it.Aliases, " "), it.Service, it.Gap, it.Verdict, it.Branch}, " "))
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
	done, inflight, backlogHit := false, false, false
	for _, it := range matches {
		pr := ""
		if it.PR > 0 {
			pr = fmt.Sprintf(" PR #%d", it.PR)
		}
		fmt.Printf("%-16s %-14s %-9s%s  %s\n", it.ID, truncate(it.Service, 14), it.State, pr, truncate(it.Gap, 80))
		fmt.Printf("         source: %s\n", shortSource(it.Source))
		if it.PlanDoc != "" {
			fmt.Printf("         plan doc: %s\n", it.PlanDoc)
		}
		if it.Kind == "pr" {
			continue // PR history is context, not a commitment
		}
		backlogHit = true
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
		case !backlogHit:
			fmt.Println("=> ASSESSMENT: no matching backlog item (PR history only) — likely new work.")
		}
	}
	return 0
}

// ─── helpers ──────────────────────────────────────────────────────────────────

type unionFind struct{ p map[string]string }

func newUF() *unionFind { return &unionFind{p: map[string]string{}} }
func (u *unionFind) find(x string) string {
	if _, ok := u.p[x]; !ok {
		u.p[x] = x
	}
	for u.p[x] != x {
		u.p[x] = u.p[u.p[x]]
		x = u.p[x]
	}
	return x
}
func (u *unionFind) union(a, b string) {
	ra, rb := u.find(a), u.find(b)
	if ra != rb {
		u.p[ra] = rb
	}
}

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
	return regexp.MustCompile(`\b` + regexp.QuoteMeta(id) + `\b`).MatchString(s)
}

func containsInt(xs []int, n int) bool {
	for _, x := range xs {
		if x == n {
			return true
		}
	}
	return false
}

func appendUnique(xs []string, vs ...string) []string {
	for _, v := range vs {
		if v == "" {
			continue
		}
		found := false
		for _, x := range xs {
			if x == v {
				found = true
				break
			}
		}
		if !found {
			xs = append(xs, v)
		}
	}
	return xs
}

func appendNote(note, add string) string {
	if note == "" {
		return add
	}
	return note + "; " + add
}

func anyAlias(m map[string]string, keys []string) (string, bool) {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v, true
		}
	}
	return "", false
}

func sectionRefs(s string) []string {
	var out []string
	for _, m := range sectionRefRe.FindAllString(s, -1) {
		out = appendUnique(out, m)
	}
	return out
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
		if strings.TrimSpace(v) != "" && v != "—" && v != "-" {
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

func shortHash(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:4])
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

func slug(s string) string {
	s = strings.ToLower(stripFormatting(s))
	s = slugRe.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 40 {
		s = strings.Trim(s[:40], "-")
	}
	if s == "" {
		s = "item"
	}
	return s
}

func shortSource(p string) string {
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
