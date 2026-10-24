package resourcemanager

import (
	"fmt"
	"strings"
)

// projectFilter is a compiled project filter expression, shared by the v1
// ListProjects filter (cloudresourcemanager.projects.list.filter) and the v3
// SearchProjects query (SearchProjectsRequest.query): a disjunction of
// field:value clauses. The Discovery/proto docs state that "if multiple fields
// are included in a filter query, the query will return results that match any
// of the fields", so any matching clause admits the project. An empty filter
// matches every project.
//
// The one exception is the v1 by-parent query, which the Discovery document
// specifies as a conjunction: it "must contain both a parent.type and a
// parent.id restriction (example: "parent.type:folder parent.id:123")" and is
// served from an alternate index. When both clauses are present they are
// required together, in addition to the usual OR over the remaining clauses.
type projectFilter struct {
	clauses []projectClause
	// parentTypeIdx / parentIDIdx are the indexes into clauses of a parent.type
	// / parent.id clause (-1 when absent), so a by-parent query can AND the two
	// together while excluding them from the ordinary OR.
	parentTypeIdx int
	parentIDIdx   int
}

// newProjectFilter returns a filter with the parent-clause indexes unset (-1),
// which is also the correct zero-state for an empty expression.
func newProjectFilter() projectFilter {
	return projectFilter{parentTypeIdx: -1, parentIDIdx: -1}
}

// match reports whether p satisfies the filter.
func (f projectFilter) match(p Project) bool {
	if f.parentTypeIdx >= 0 && f.parentIDIdx >= 0 {
		// By-parent query: the two parent clauses are required together, and
		// any other clauses OR over the remaining fields.
		if !f.clauses[f.parentTypeIdx].match(p) || !f.clauses[f.parentIDIdx].match(p) {
			return false
		}
		remaining := 0
		for i, c := range f.clauses {
			if i == f.parentTypeIdx || i == f.parentIDIdx {
				continue
			}
			remaining++
			if c.match(p) {
				return true
			}
		}
		// No other clauses: the parent conjunction is the whole filter.
		return remaining == 0
	}
	if len(f.clauses) == 0 {
		return true
	}
	for _, c := range f.clauses {
		if c.match(p) {
			return true
		}
	}
	return false
}

// projectClause is one compiled filter clause.
type projectClause interface {
	match(Project) bool
}

// projectAnyClause matches every project (a bare `*` value).
type projectAnyClause struct{}

func (projectAnyClause) match(Project) bool { return true }

// projectValueClause matches a string-valued project attribute. Matching is
// case-insensitive; prefix selects a prefix match (the trailing-`*` form)
// rather than an exact match.
type projectValueClause struct {
	get    func(Project) string
	value  string
	prefix bool
}

func (c projectValueClause) match(p Project) bool {
	got := c.get(p)
	if c.prefix {
		return strings.HasPrefix(strings.ToLower(got), c.value)
	}
	return strings.EqualFold(got, c.value)
}

// projectLabelExistsClause matches projects carrying a label key (the
// `labels.<key>:*` form).
type projectLabelExistsClause struct {
	key string
}

func (c projectLabelExistsClause) match(p Project) bool {
	_, ok := p.Labels[c.key]
	return ok
}

// projectLabelsClause matches a bare `labels:value` clause, which the v3
// SearchProjects documentation defines as matching "by label name or value":
// any label key or value equal to (or, with a trailing `*`, prefixed by) the
// search value admits the project. A value of exactly `*` means the project
// carries at least one label.
type projectLabelsClause struct {
	value  string
	prefix bool
	any    bool
}

func (c projectLabelsClause) match(p Project) bool {
	for k, v := range p.Labels {
		if c.any {
			return true
		}
		if c.prefix {
			if strings.HasPrefix(strings.ToLower(k), c.value) || strings.HasPrefix(strings.ToLower(v), c.value) {
				return true
			}
			continue
		}
		if strings.EqualFold(k, c.value) || strings.EqualFold(v, c.value) {
			return true
		}
	}
	return false
}

// parentTypeOf derives the singular parent type ("organization"/"folder") from
// a canonical parent reference ("organizations/{id}"/"folders/{id}"), or "" when
// no parent is set.
func parentTypeOf(parent string) string {
	typ := parent
	if i := strings.IndexByte(parent, '/'); i >= 0 {
		typ = parent[:i]
	}
	switch typ {
	case "organizations":
		return "organization"
	case "folders":
		return "folder"
	}
	return strings.TrimSuffix(typ, "s")
}

// parentIDOf returns the numeric id of a canonical parent reference, or "".
func parentIDOf(parent string) string {
	if i := strings.IndexByte(parent, '/'); i >= 0 {
		return parent[i+1:]
	}
	return ""
}

// compileProjectFilter parses the bounded project filter grammar shared by the
// v1 ListProjects filter and the v3 SearchProjects query:
//
//	clause := field ":" value
//	filter := clause ( <whitespace> clause )*
//	field  := "name" | "displayName" | "id" | "projectId" | "labels" |
//	          "labels." <key> | "lifecycleState" | "state" |
//	          "parent" | "parent.type" | "parent.id"
//	value  := <token> | <double-quoted string>
//
// Field names and values are matched case-insensitively, clauses are OR-ed, a
// trailing `*` in a value is a prefix match, and a value of exactly `*` matches
// any value (for `labels.<key>` it means the key is present, for a bare
// `labels` any label admits the project). Anything outside the grammar is
// InvalidArgument rather than a silently wrong (unfiltered) page.
//
// `parent`/`parent.type`/`parent.id` match the stored parent reference
// ("organizations/{id}"/"folders/{id}"); a by-parent query carrying both
// parent.type and parent.id ANDs the two (see projectFilter).
func compileProjectFilter(filter string) (projectFilter, error) {
	if strings.TrimSpace(filter) == "" {
		return newProjectFilter(), nil
	}
	toks, err := tokenizeProjectFilter(filter)
	if err != nil {
		return projectFilter{}, invalidArgument("invalid project list filter: " + err.Error())
	}
	p := &projectFilterParser{toks: toks}
	f, err := p.parse()
	if err != nil {
		return projectFilter{}, invalidArgument("invalid project list filter: " + err.Error())
	}
	return f, nil
}

// ─── tokenizer ────────────────────────────────────────────────────────────────

type projectFilterTokenKind int

const (
	pjEOF projectFilterTokenKind = iota
	pjIdent
	pjString
	pjColon
)

type projectFilterToken struct {
	kind projectFilterTokenKind
	text string
}

// tokenizeProjectFilter splits an expression into identifiers, quoted strings
// and the `:` separator. `*` is an ordinary identifier character so wildcard
// values (`how*`, `*`) stay one token; whitespace separates clauses, and an
// unterminated string or unexpected character is an error.
func tokenizeProjectFilter(s string) ([]projectFilterToken, error) {
	var toks []projectFilterToken
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == ':':
			toks = append(toks, projectFilterToken{kind: pjColon, text: ":"})
			i++
		case c == '"':
			j := i + 1
			var b strings.Builder
			closed := false
			for j < len(s) {
				if s[j] == '\\' && j+1 < len(s) {
					b.WriteByte(s[j+1])
					j += 2
					continue
				}
				if s[j] == '"' {
					closed = true
					j++
					break
				}
				b.WriteByte(s[j])
				j++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated quoted string")
			}
			toks = append(toks, projectFilterToken{kind: pjString, text: b.String()})
			i = j
		case isProjectFilterIdentPart(c):
			j := i + 1
			for j < len(s) && isProjectFilterIdentPart(s[j]) {
				j++
			}
			toks = append(toks, projectFilterToken{kind: pjIdent, text: s[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q", string(c))
		}
	}
	toks = append(toks, projectFilterToken{kind: pjEOF})
	return toks, nil
}

func isProjectFilterIdentPart(c byte) bool {
	// '/' is included so parent references ("folders/123") and the
	// "organizations/*" prefix form stay a single value token.
	return c == '_' || c == '-' || c == '.' || c == '*' || c == '/' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// ─── parser ───────────────────────────────────────────────────────────────────

type projectFilterParser struct {
	toks []projectFilterToken
	i    int
}

func (p *projectFilterParser) peek() projectFilterToken { return p.toks[p.i] }

func (p *projectFilterParser) next() projectFilterToken {
	t := p.toks[p.i]
	if t.kind != pjEOF {
		p.i++
	}
	return t
}

// parse consumes `field:value` clauses until end of input.
func (p *projectFilterParser) parse() (projectFilter, error) {
	f := newProjectFilter()
	for {
		field := p.next()
		if field.kind != pjIdent {
			return projectFilter{}, fmt.Errorf("expected filter field, got %s", projectTokenText(field))
		}
		op := p.next()
		if op.kind != pjColon {
			return projectFilter{}, fmt.Errorf("expected \":\" after %q, got %s", field.text, projectTokenText(op))
		}
		value := p.next()
		if value.kind != pjIdent && value.kind != pjString {
			return projectFilter{}, fmt.Errorf("expected value after %q, got %s", field.text, projectTokenText(value))
		}
		clause, err := newProjectClause(field.text, value.text)
		if err != nil {
			return projectFilter{}, err
		}
		switch strings.ToLower(field.text) {
		case "parent.type":
			f.parentTypeIdx = len(f.clauses)
		case "parent.id":
			f.parentIDIdx = len(f.clauses)
		}
		f.clauses = append(f.clauses, clause)

		switch p.peek().kind {
		case pjEOF:
			return f, nil
		case pjIdent:
			// Next clause.
		default:
			return projectFilter{}, fmt.Errorf("unexpected token %s", projectTokenText(p.peek()))
		}
	}
}

// newProjectClause builds the predicate for a single field:value pair.
func newProjectClause(field, value string) (projectClause, error) {
	if value == "" {
		return nil, fmt.Errorf("empty value for %q", field)
	}
	switch strings.ToLower(field) {
	case "name", "displayname":
		return newProjectValueClause(value, func(p Project) string { return p.DisplayName }), nil
	case "id", "projectid":
		return newProjectValueClause(value, func(p Project) string { return p.ProjectID }), nil
	case "lifecyclestate", "state":
		return newProjectValueClause(value, func(p Project) string { return p.State }), nil
	case "parent":
		return newProjectValueClause(value, func(p Project) string { return p.Parent }), nil
	case "parent.type":
		return newProjectValueClause(value, func(p Project) string { return parentTypeOf(p.Parent) }), nil
	case "parent.id":
		return newProjectValueClause(value, func(p Project) string { return parentIDOf(p.Parent) }), nil
	case "labels":
		return newProjectLabelsClause(value), nil
	}
	// The "labels." prefix is matched case-insensitively (filter rules are), but
	// the label key that follows is data and keeps its case.
	const labelPrefix = "labels."
	if len(field) > len(labelPrefix) && strings.EqualFold(field[:len(labelPrefix)], labelPrefix) {
		key := field[len(labelPrefix):]
		if value == "*" {
			return projectLabelExistsClause{key: key}, nil
		}
		return newProjectValueClause(value, func(p Project) string { return p.Labels[key] }), nil
	}
	return nil, fmt.Errorf("unsupported filter field %q (only name, id, labels.<key> and lifecycleState are supported)", field)
}

// newProjectLabelsClause builds a bare-`labels` clause, applying the same
// wildcard rules as a value clause but against both label keys and values.
func newProjectLabelsClause(value string) projectClause {
	if value == "*" {
		return projectLabelsClause{any: true}
	}
	if prefix, ok := strings.CutSuffix(value, "*"); ok {
		return projectLabelsClause{value: strings.ToLower(prefix), prefix: true}
	}
	return projectLabelsClause{value: value}
}

// newProjectValueClause builds a value clause, applying the shared wildcard
// rules: a bare `*` matches any value, a trailing `*` is a case-insensitive
// prefix match, and anything else is a case-insensitive exact match.
func newProjectValueClause(value string, get func(Project) string) projectClause {
	if value == "*" {
		return projectAnyClause{}
	}
	if prefix, ok := strings.CutSuffix(value, "*"); ok {
		return projectValueClause{get: get, value: strings.ToLower(prefix), prefix: true}
	}
	return projectValueClause{get: get, value: value}
}

// projectTokenText renders a token for an error message.
func projectTokenText(t projectFilterToken) string {
	if t.kind == pjEOF {
		return "end of filter"
	}
	return fmt.Sprintf("%q", t.text)
}
