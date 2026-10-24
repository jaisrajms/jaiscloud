package workflows

import (
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/gcp/aip160"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// This file implements the two documented workflows.list request parameters the
// transport layer used to drop: `filter` (AIP-160 over name, state, revisionId,
// labels.<key> and the timestamps) and `orderBy` (a comma-separated field list,
// optionally `<field> desc`). Real Cloud Workflows documents both on
// workflows.projects.locations.workflows.list, so a client that filters or sorts
// must not be handed an unfiltered, id-ascending list: anything outside the
// supported grammar is INVALID_ARGUMENT rather than silently ignored.

// ─── filter ───────────────────────────────────────────────────────────────────

// workflowPredicate is one compiled workflows.list filter clause.
type workflowPredicate func(workflowsstore.Workflow) bool

// workflowFilter is the compiled subset of the workflows.list filter grammar: a
// conjunction of clauses over name, state, revisionId, labels.<key> and
// createTime/updateTime. An empty filter matches every workflow.
type workflowFilter struct {
	preds []workflowPredicate
}

// match reports whether w satisfies every clause.
func (f workflowFilter) match(w workflowsstore.Workflow) bool {
	for _, p := range f.preds {
		if !p(w) {
			return false
		}
	}
	return true
}

// workflowTimeFields maps a timestamp filter key onto the workflow field it
// reads.
var workflowTimeFields = map[string]func(workflowsstore.Workflow) time.Time{
	"createtime": func(w workflowsstore.Workflow) time.Time { return w.CreateTime },
	"updatetime": func(w workflowsstore.Workflow) time.Time { return w.UpdateTime },
}

// compileWorkflowFilter parses the bounded workflows.list filter grammar:
//
//	clause := field op value
//	filter := clause ( ("AND" | implicit whitespace) clause )*
//	field  := "name" | "state" | "revisionId" | "labels." <key>
//	        | "createTime" | "updateTime"
//	op     := "=" | "!=" | ":"            (":" only on the identity/label fields)
//	value  := <ident> | <double-quoted string> | "*"
//
// nameOf renders a workflow's canonical resource name, so `name="…/workflows/w"`
// filters on the full name (nameOf is resolved by the caller, which knows the
// project). The clause set mirrors the identity/label/timestamp fields an
// AIP-160 list exposes; anything outside it — including the
// OR/NOT/parenthesis forms — is INVALID_ARGUMENT, so a filter is never silently
// ignored and never returns an unfiltered list to a client that asked for a
// subset.
func compileWorkflowFilter(s string, nameOf func(workflowsstore.Workflow) string) (workflowFilter, error) {
	if strings.TrimSpace(s) == "" {
		return workflowFilter{}, nil
	}
	toks, err := aip160.Tokenize(s)
	if err != nil {
		return workflowFilter{}, invalidArgument("workflows ListWorkflows filter: " + err.Error())
	}
	clause := func(p *aip160.Parser) (any, error) { return parseWorkflowClause(p, nameOf) }
	preds, err := aip160.NewParser(toks).ParseClauses(clause)
	if err != nil {
		return workflowFilter{}, invalidArgument("workflows ListWorkflows filter: " + err.Error())
	}
	f := workflowFilter{preds: make([]workflowPredicate, 0, len(preds))}
	for _, p := range preds {
		pred, ok := p.(workflowPredicate)
		if !ok {
			return workflowFilter{}, invalidArgument("workflows ListWorkflows filter: internal clause type")
		}
		f.preds = append(f.preds, pred)
	}
	return f, nil
}

// parseWorkflowClause parses one workflows.list filter clause (`field op value`)
// into the matching workflowPredicate, returned as an `any` for the shared
// clause sequencer.
func parseWorkflowClause(p *aip160.Parser, nameOf func(workflowsstore.Workflow) string) (any, error) {
	keyTok := p.Next()
	if keyTok.Kind != aip160.KindIdent {
		return nil, fmt.Errorf("expected field, got %s", aip160.TokenText(keyTok))
	}
	key := keyTok.Text
	opTok := p.Next()

	folded := strings.ToLower(key)
	// Quoted-key label form: `labels."env":"prod"`. The tokenizer splits that
	// into the ident ``labels.`` (or ``labels``) followed by the quoted key, so
	// the key is the next token rather than part of the field name.
	if (folded == "labels" || folded == "labels.") && opTok.Kind == aip160.KindString {
		lk := opTok.Text
		if lk == "" {
			return nil, fmt.Errorf("empty label key")
		}
		labelOp := p.Next()
		if labelOp.Kind != aip160.KindOp {
			return nil, fmt.Errorf("expected operator after label key %q, got %s", lk, aip160.TokenText(labelOp))
		}
		return parseWorkflowLabelClause(p, labelOp.Text, lk)
	}

	if opTok.Kind != aip160.KindOp {
		return nil, fmt.Errorf("expected operator after %q, got %s", key, aip160.TokenText(opTok))
	}
	op := opTok.Text

	if get, ok := workflowTimeFields[folded]; ok {
		return parseWorkflowTimeClause(p, key, op, get)
	}
	if lk, ok := strings.CutPrefix(folded, "labels."); ok {
		if lk == "" {
			return nil, fmt.Errorf("empty label key in %q", key)
		}
		labelKey := key[len("labels."):]
		return parseWorkflowLabelClause(p, op, labelKey)
	}
	switch folded {
	case "name":
		return parseWorkflowStringClause(p, key, op, nameOf)
	case "state":
		return parseWorkflowStringClause(p, key, op, func(w workflowsstore.Workflow) string { return w.State })
	case "revisionid":
		return parseWorkflowStringClause(p, key, op, func(w workflowsstore.Workflow) string { return w.RevisionID })
	case "labels":
		if op != ":" && op != "=" {
			return nil, fmt.Errorf("unsupported operator %q on labels (only \":\" and \"=\" are supported)", op)
		}
		if v := p.Next(); v.Kind != aip160.KindStar {
			return nil, fmt.Errorf("labels takes only \"*\" (any label), got %s", aip160.TokenText(v))
		}
		return workflowPredicate(func(w workflowsstore.Workflow) bool { return len(w.Labels) > 0 }), nil
	default:
		return nil, fmt.Errorf("unsupported filter key %q "+
			"(only name, state, revisionId, labels.<key>, labels, createTime and updateTime are supported)", key)
	}
}

// parseWorkflowStringClause compiles an identity-field clause. "=" and "!=" are
// exact matches; ":" is AIP-160's has/contains operator (a substring match).
func parseWorkflowStringClause(p *aip160.Parser, key, op string, get func(workflowsstore.Workflow) string) (any, error) {
	if op != "=" && op != "!=" && op != ":" {
		return nil, fmt.Errorf("unsupported operator %q on %s (only \"=\", \"!=\" and \":\" are supported)", op, key)
	}
	valTok := p.Next()
	if valTok.Kind == aip160.KindStar {
		return nil, fmt.Errorf("%s does not accept \"*\"", key)
	}
	val, ok := aip160.TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("%s value must be a value or a quoted string, got %s", key, aip160.TokenText(valTok))
	}
	switch op {
	case "=":
		return workflowPredicate(func(w workflowsstore.Workflow) bool { return get(w) == val }), nil
	case "!=":
		return workflowPredicate(func(w workflowsstore.Workflow) bool { return get(w) != val }), nil
	default:
		return workflowPredicate(func(w workflowsstore.Workflow) bool { return strings.Contains(get(w), val) }), nil
	}
}

// parseWorkflowLabelClause compiles a labels.<key> clause. "=" and "!=" are
// exact matches; ":" (or "*" as the value) tests that the key is present with
// any value, and "!= *" tests that it is absent.
func parseWorkflowLabelClause(p *aip160.Parser, op, key string) (any, error) {
	if op != "=" && op != "!=" && op != ":" {
		return nil, fmt.Errorf("unsupported operator %q on labels.%s (only \"=\", \"!=\" and \":\" are supported)", op, key)
	}
	valTok := p.Next()
	if valTok.Kind == aip160.KindStar {
		if op == "!=" {
			return workflowPredicate(func(w workflowsstore.Workflow) bool { _, ok := w.Labels[key]; return !ok }), nil
		}
		return workflowPredicate(func(w workflowsstore.Workflow) bool { _, ok := w.Labels[key]; return ok }), nil
	}
	val, ok := aip160.TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("label value must be a value, a quoted string or \"*\", got %s", aip160.TokenText(valTok))
	}
	switch op {
	case "=":
		return workflowPredicate(func(w workflowsstore.Workflow) bool { return w.Labels[key] == val }), nil
	case "!=":
		return workflowPredicate(func(w workflowsstore.Workflow) bool { return w.Labels[key] != val }), nil
	default:
		return workflowPredicate(func(w workflowsstore.Workflow) bool { return strings.Contains(w.Labels[key], val) }), nil
	}
}

// parseWorkflowTimeClause compiles a timestamp comparison. The value must be an
// RFC3339 timestamp or a YYYY-MM-DD date (quoted, per AIP-160); the six
// comparison operators are supported, ":" is not.
func parseWorkflowTimeClause(p *aip160.Parser, key, op string, get func(workflowsstore.Workflow) time.Time) (any, error) {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
	default:
		return nil, fmt.Errorf("unsupported operator %q on %s (only =, !=, <, <=, > and >= are supported)", op, key)
	}
	valTok := p.Next()
	val, ok := aip160.TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("%s value must be an RFC3339 timestamp or YYYY-MM-DD date, got %s", key, aip160.TokenText(valTok))
	}
	bound, err := aip160.ParseTime(val)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", key, err)
	}
	want := bound.UTC().Format(aip160.TimeLayout)
	return workflowPredicate(func(w workflowsstore.Workflow) bool {
		got := get(w).UTC().Format(aip160.TimeLayout)
		switch op {
		case "=":
			return got == want
		case "!=":
			return got != want
		case "<":
			return got < want
		case "<=":
			return got <= want
		case ">":
			return got > want
		default: // ">="
			return got >= want
		}
	}), nil
}

// ─── orderBy ──────────────────────────────────────────────────────────────────

// workflowOrderSpec is the documented workflows.list orderBy grammar: name,
// state, revisionId, createTime and updateTime. name orders by the workflow ID,
// which within a location sorts identically to the full resource name.
var workflowOrderSpec = aip160.NewOrderSpec(func(w workflowsstore.Workflow) string { return w.ID },
	aip160.OrderField[workflowsstore.Workflow]{Name: "name", Value: func(w workflowsstore.Workflow) string { return w.ID }},
	aip160.OrderField[workflowsstore.Workflow]{Name: "state", Value: func(w workflowsstore.Workflow) string { return w.State }},
	aip160.OrderField[workflowsstore.Workflow]{Name: "revisionId", Value: func(w workflowsstore.Workflow) string { return w.RevisionID }},
	aip160.OrderField[workflowsstore.Workflow]{Name: "createTime", Value: func(w workflowsstore.Workflow) string {
		return w.CreateTime.UTC().Format(aip160.TimeLayout)
	}},
	aip160.OrderField[workflowsstore.Workflow]{Name: "updateTime", Value: func(w workflowsstore.Workflow) string {
		return w.UpdateTime.UTC().Format(aip160.TimeLayout)
	}},
)
