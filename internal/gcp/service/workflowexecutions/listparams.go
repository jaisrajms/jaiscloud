package workflowexecutions

import (
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/gcp/aip160"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// This file implements the two documented executions.list request parameters the
// transport layer used to drop: `filter` (AIP-160 over executionId, state,
// workflowRevisionId, labels.<key> and the timestamps) and `orderBy` (a
// comma-separated field list, optionally `<field> desc`). Real Workflow
// Executions documents both on
// workflowexecutions.projects.locations.workflows.executions.list, so a client
// that filters or sorts must not be handed an unfiltered, id-ascending list:
// anything outside the supported grammar is INVALID_ARGUMENT rather than
// silently ignored.

// ─── filter ───────────────────────────────────────────────────────────────────

// executionPredicate is one compiled executions.list filter clause.
type executionPredicate func(workflowsstore.Execution) bool

// executionFilter is the compiled subset of the executions.list filter grammar:
// a conjunction of clauses over executionId, state, workflowRevisionId,
// labels.<key> and createTime/startTime/endTime. An empty filter matches every
// execution.
type executionFilter struct {
	preds []executionPredicate
}

// match reports whether e satisfies every clause.
func (f executionFilter) match(e workflowsstore.Execution) bool {
	for _, p := range f.preds {
		if !p(e) {
			return false
		}
	}
	return true
}

// executionFilterTimeFields maps a timestamp filter key onto the execution field
// it reads. createTime is an alias of startTime: the emulator runs an execution
// synchronously at create time and stores a single start timestamp, so the two
// coincide.
var executionFilterTimeFields = map[string]func(workflowsstore.Execution) time.Time{
	"createtime": func(e workflowsstore.Execution) time.Time { return e.StartTime },
	"starttime":  func(e workflowsstore.Execution) time.Time { return e.StartTime },
	"endtime":    func(e workflowsstore.Execution) time.Time { return e.EndTime },
}

// compileExecutionFilter parses the bounded executions.list filter grammar:
//
//	clause := field op value
//	filter := clause ( ("AND" | implicit whitespace) clause )*
//	field  := "executionId" | "state" | "workflowRevisionId" | "labels." <key>
//	        | "createTime" | "startTime" | "endTime"
//	op     := "=" | "!=" | ":"            (":" only on the identity/label fields)
//	value  := <ident> | <double-quoted string> | "*"
//
// The clause set mirrors the API's own examples ("state=\"SUCCEEDED\"",
// "startTime>\"2023-08-01\" AND state=\"FAILED\"") plus the identity and label
// fields every AIP-160 list exposes. Fields outside this set — including the
// documented duration, stepName, label and
// disableConcurrencyQuotaOverflowBuffering — are INVALID_ARGUMENT, as are the
// OR/NOT/parenthesis forms: a filter is never silently ignored, which would
// return an unfiltered list to a client that asked for a subset.
func compileExecutionFilter(s string) (executionFilter, error) {
	if strings.TrimSpace(s) == "" {
		return executionFilter{}, nil
	}
	toks, err := aip160.Tokenize(s)
	if err != nil {
		return executionFilter{}, invalidArgument("workflowexecutions ListExecutions filter: " + err.Error())
	}
	preds, err := aip160.NewParser(toks).ParseClauses(parseExecutionClause)
	if err != nil {
		return executionFilter{}, invalidArgument("workflowexecutions ListExecutions filter: " + err.Error())
	}
	f := executionFilter{preds: make([]executionPredicate, 0, len(preds))}
	for _, p := range preds {
		pred, ok := p.(executionPredicate)
		if !ok {
			return executionFilter{}, invalidArgument("workflowexecutions ListExecutions filter: internal clause type")
		}
		f.preds = append(f.preds, pred)
	}
	return f, nil
}

// parseExecutionClause parses one executions.list filter clause (`field op
// value`) into the matching executionPredicate, returned as an `any` for the
// shared clause sequencer.
func parseExecutionClause(p *aip160.Parser) (any, error) {
	keyTok := p.Next()
	if keyTok.Kind != aip160.KindIdent {
		return nil, fmt.Errorf("expected field, got %s", aip160.TokenText(keyTok))
	}
	key := keyTok.Text
	opTok := p.Next()

	folded := strings.ToLower(key)
	// Quoted-key label form: `labels."env":"prod"` — the tokenizer splits it
	// into the ident ``labels.`` (or ``labels``) followed by the quoted key.
	if (folded == "labels" || folded == "labels.") && opTok.Kind == aip160.KindString {
		lk := opTok.Text
		if lk == "" {
			return nil, fmt.Errorf("empty label key")
		}
		labelOp := p.Next()
		if labelOp.Kind != aip160.KindOp {
			return nil, fmt.Errorf("expected operator after label key %q, got %s", lk, aip160.TokenText(labelOp))
		}
		return parseExecutionLabelClause(p, labelOp.Text, lk)
	}

	if opTok.Kind != aip160.KindOp {
		return nil, fmt.Errorf("expected operator after %q, got %s", key, aip160.TokenText(opTok))
	}
	op := opTok.Text

	if get, ok := executionFilterTimeFields[folded]; ok {
		return parseExecutionTimeClause(p, key, op, get)
	}
	if lk, ok := strings.CutPrefix(folded, "labels."); ok {
		if lk == "" {
			return nil, fmt.Errorf("empty label key in %q", key)
		}
		// Preserve the caller's label-key casing: map keys are case-sensitive.
		labelKey := key[len("labels."):]
		return parseExecutionLabelClause(p, op, labelKey)
	}
	switch folded {
	case "executionid":
		return parseExecutionStringClause(p, key, op, func(e workflowsstore.Execution) string { return e.ID })
	case "state":
		return parseExecutionStringClause(p, key, op, func(e workflowsstore.Execution) string { return e.State })
	case "workflowrevisionid":
		return parseExecutionStringClause(p, key, op, func(e workflowsstore.Execution) string { return e.WorkflowRevisionID })
	case "labels":
		if op != ":" && op != "=" {
			return nil, fmt.Errorf("unsupported operator %q on labels (only \":\" and \"=\" are supported)", op)
		}
		if v := p.Next(); v.Kind != aip160.KindStar {
			return nil, fmt.Errorf("labels takes only \"*\" (any label), got %s", aip160.TokenText(v))
		}
		return executionPredicate(func(e workflowsstore.Execution) bool { return len(e.Labels) > 0 }), nil
	default:
		return nil, fmt.Errorf("unsupported filter key %q "+
			"(only executionId, state, workflowRevisionId, labels.<key>, labels, createTime, startTime and endTime are supported)", key)
	}
}

// parseExecutionStringClause compiles an identity-field clause. "=" and "!=" are
// exact matches; ":" is AIP-160's has/contains operator (a substring match).
func parseExecutionStringClause(p *aip160.Parser, key, op string, get func(workflowsstore.Execution) string) (any, error) {
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
		return executionPredicate(func(e workflowsstore.Execution) bool { return get(e) == val }), nil
	case "!=":
		return executionPredicate(func(e workflowsstore.Execution) bool { return get(e) != val }), nil
	default:
		return executionPredicate(func(e workflowsstore.Execution) bool { return strings.Contains(get(e), val) }), nil
	}
}

// parseExecutionLabelClause compiles a labels.<key> clause. "=" and "!=" are
// exact matches; ":" (or "*" as the value) tests that the key is present with
// any value, and "!= *" tests that it is absent.
func parseExecutionLabelClause(p *aip160.Parser, op, key string) (any, error) {
	if op != "=" && op != "!=" && op != ":" {
		return nil, fmt.Errorf("unsupported operator %q on labels.%s (only \"=\", \"!=\" and \":\" are supported)", op, key)
	}
	valTok := p.Next()
	if valTok.Kind == aip160.KindStar {
		if op == "!=" {
			return executionPredicate(func(e workflowsstore.Execution) bool { _, ok := e.Labels[key]; return !ok }), nil
		}
		return executionPredicate(func(e workflowsstore.Execution) bool { _, ok := e.Labels[key]; return ok }), nil
	}
	val, ok := aip160.TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("label value must be a value, a quoted string or \"*\", got %s", aip160.TokenText(valTok))
	}
	switch op {
	case "=":
		return executionPredicate(func(e workflowsstore.Execution) bool { return e.Labels[key] == val }), nil
	case "!=":
		return executionPredicate(func(e workflowsstore.Execution) bool { return e.Labels[key] != val }), nil
	default:
		return executionPredicate(func(e workflowsstore.Execution) bool { return strings.Contains(e.Labels[key], val) }), nil
	}
}

// parseExecutionTimeClause compiles a timestamp comparison. The value must be an
// RFC3339 timestamp or a YYYY-MM-DD date (quoted, per the API's own example);
// the six comparison operators are supported, ":" is not.
func parseExecutionTimeClause(p *aip160.Parser, key, op string, get func(workflowsstore.Execution) time.Time) (any, error) {
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
	return executionPredicate(func(e workflowsstore.Execution) bool {
		got := get(e).UTC().Format(aip160.TimeLayout)
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

// executionPageSize applies the documented executions.list page-size contract:
// an unspecified value defaults to 100 (regardless of view) and a value above
// the view's maximum is coerced down to it — 1000 for BASIC, 100 for FULL.
func executionPageSize(n int, view View) int {
	maxSize := 1000
	if view == ViewFull {
		maxSize = 100
	}
	switch {
	case n == 0:
		return 100
	case n > maxSize:
		return maxSize
	default:
		return n
	}
}

// ─── orderBy ──────────────────────────────────────────────────────────────────

// executionOrderSpec is the documented executions.list orderBy grammar:
// executionId, state, createTime, startTime, endTime and workflowRevisionId
// (duration is not orderable here). createTime reads startTime: the emulator
// stores a single start timestamp for a synchronously run execution.
var executionOrderSpec = aip160.NewOrderSpec(func(e workflowsstore.Execution) string { return e.ID },
	aip160.OrderField[workflowsstore.Execution]{Name: "executionId", Value: func(e workflowsstore.Execution) string { return e.ID }},
	aip160.OrderField[workflowsstore.Execution]{Name: "state", Value: func(e workflowsstore.Execution) string { return e.State }},
	aip160.OrderField[workflowsstore.Execution]{Name: "createTime", Value: func(e workflowsstore.Execution) string {
		return e.StartTime.UTC().Format(aip160.TimeLayout)
	}},
	aip160.OrderField[workflowsstore.Execution]{Name: "startTime", Value: func(e workflowsstore.Execution) string {
		return e.StartTime.UTC().Format(aip160.TimeLayout)
	}},
	aip160.OrderField[workflowsstore.Execution]{Name: "endTime", Value: func(e workflowsstore.Execution) string {
		return e.EndTime.UTC().Format(aip160.TimeLayout)
	}},
	aip160.OrderField[workflowsstore.Execution]{Name: "workflowRevisionId", Value: func(e workflowsstore.Execution) string { return e.WorkflowRevisionID }},
)
