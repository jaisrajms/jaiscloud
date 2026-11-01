// Package operationfilter is the shared AIP-160 evaluator for the standard
// google.longrunning.Operations.List `filter` parameter. Every service that
// persists long-running operations renders its record to a common view
// (operation name, done state, and optional string metadata fields), then uses
// this package so all shared Operations surfaces accept the same grammar:
// comparisons, AND/OR/NOT, implicit AND, and parentheses.
package operationfilter

import (
	"fmt"
	"strconv"
	"strings"

	"jaiscloud/internal/gcp/aip160"
)

// Record is one operation as seen by the filter evaluator. Fields holds any
// extra string-valued fields the caller exposes (for example verb, target and
// state); keys are matched case-insensitively.
type Record struct {
	Name   string
	Done   bool
	Fields map[string]string
}

// predicate is one compiled comparison. The parser boxes it in any so the
// shared aip160 expression tree can carry surface predicates.
type predicate func(Record) bool

// Filter is a compiled operations filter. The zero value matches everything.
type Filter struct {
	expr *aip160.Expr
}

// Match reports whether r satisfies the filter.
func (f Filter) Match(r Record) bool {
	if f.expr == nil {
		return true
	}
	return f.expr.Match(func(leaf any) bool { return leaf.(predicate)(r) })
}

// Compile parses an AIP-160 operations filter. `name` and `done` are always
// accepted; extraFields names additional string-valued metadata fields the
// caller exposes. An empty expression matches every operation. The returned
// error is descriptive and should be wrapped in the caller's InvalidArgument.
func Compile(expr string, extraFields ...string) (Filter, error) {
	if strings.TrimSpace(expr) == "" {
		return Filter{}, nil
	}
	allowed := make(map[string]bool, len(extraFields)+2)
	allowed["name"] = true
	allowed["done"] = true
	for _, f := range extraFields {
		allowed[strings.ToLower(f)] = true
	}
	toks, err := aip160.Tokenize(expr)
	if err != nil {
		return Filter{}, err
	}
	root, err := aip160.NewParser(toks).ParseExpression(func(p *aip160.Parser) (any, error) {
		return parseClause(p, allowed)
	})
	if err != nil {
		return Filter{}, err
	}
	return Filter{expr: root}, nil
}

func parseClause(p *aip160.Parser, allowed map[string]bool) (any, error) {
	fieldTok := p.Next()
	if fieldTok.Kind != aip160.KindIdent {
		return nil, fmt.Errorf("expected operations filter field, got %s", aip160.TokenText(fieldTok))
	}
	field := strings.ToLower(fieldTok.Text)
	if !allowed[field] {
		return nil, fmt.Errorf("unsupported operations filter field %q", fieldTok.Text)
	}

	opTok := p.Next()
	if opTok.Kind != aip160.KindOp {
		return nil, fmt.Errorf("expected comparison operator after %q, got %s", fieldTok.Text, aip160.TokenText(opTok))
	}
	op := opTok.Text

	valTok := p.Next()
	star := valTok.Kind == aip160.KindStar
	value := ""
	if !star {
		v, ok := aip160.TokenValue(valTok)
		if !ok {
			return nil, fmt.Errorf("expected value after %q, got %s", op, aip160.TokenText(valTok))
		}
		value = v
	}

	if field == "done" {
		if op != "=" && op != "!=" {
			return nil, fmt.Errorf("unsupported operator %q for done (only \"=\" and \"!=\" are supported)", op)
		}
		if star {
			return nil, fmt.Errorf("done does not accept \"*\"")
		}
		if !strings.EqualFold(value, "true") && !strings.EqualFold(value, "false") {
			return nil, fmt.Errorf("done filter value must be true or false, got %q", value)
		}
		want := strings.EqualFold(value, "true")
		if op == "!=" {
			return predicate(func(r Record) bool { return r.Done != want }), nil
		}
		return predicate(func(r Record) bool { return r.Done == want }), nil
	}

	switch op {
	case "=", "!=", ":", "<", "<=", ">", ">=":
	default:
		return nil, fmt.Errorf("unsupported operator %q for %s", op, fieldTok.Text)
	}
	if star {
		if op != "=" && op != "!=" && op != ":" {
			return nil, fmt.Errorf("%s does not accept \"*\" with %q", fieldTok.Text, op)
		}
		// `field:*`/`field=*` tests presence; `field!=*` tests absence/empty.
		wantPresent := op != "!="
		return predicate(func(r Record) bool {
			return (recordField(r, field) != "") == wantPresent
		}), nil
	}
	return predicate(func(r Record) bool {
		actual := recordField(r, field)
		switch op {
		case "=":
			return actual == value
		case "!=":
			return actual != value
		case ":":
			return strings.Contains(strings.ToLower(actual), strings.ToLower(value))
		case "<":
			return actual < value
		case "<=":
			return actual <= value
		case ">":
			return actual > value
		default: // ">="
			return actual >= value
		}
	}), nil
}

func recordField(r Record, field string) string {
	switch field {
	case "name":
		return r.Name
	case "done":
		return strconv.FormatBool(r.Done)
	default:
		return r.Fields[field]
	}
}
