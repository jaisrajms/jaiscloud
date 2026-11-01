package aip160

import (
	"strings"
	"testing"
)

// exprLeaf is the surface predicate the expression tests box in any.
type exprLeaf struct {
	key, val string
}

func exprClause(p *Parser) (any, error) {
	key := p.Next()
	if key.Kind != KindIdent {
		return nil, errString("expected field")
	}
	op := p.Next()
	if op.Kind != KindOp {
		return nil, errString("expected operator")
	}
	val := p.Next()
	if val.Kind != KindIdent {
		return nil, errString("expected value")
	}
	return exprLeaf{key: key.Text, val: val.Text}, nil
}

func evalExprLeaf(v any) bool { return v.(exprLeaf).val == "t" }

func parseExpr(t *testing.T, s string) *Expr {
	t.Helper()
	toks, err := Tokenize(s)
	if err != nil {
		t.Fatalf("Tokenize(%q): %v", s, err)
	}
	e, err := NewParser(toks).ParseExpression(exprClause)
	if err != nil {
		t.Fatalf("ParseExpression(%q): %v", s, err)
	}
	return e
}

// TestParseExpressionMatch covers the full AIP-160 logical grammar: AND/OR,
// implicit AND, NOT and "-" negation, parentheses, nesting and operator
// precedence (OR binds tighter than AND).
func TestParseExpressionMatch(t *testing.T) {
	cases := []struct {
		expr string
		want bool
	}{
		{"a=t", true},
		{"a=f", false},
		{"a=t AND b=t", true},
		{"a=t AND b=f", false},
		{"a=f OR b=t", true},
		{"a=t b=t", true},     // implicit AND
		{"a=t (b=t)", true},   // implicit AND with a group
		{"NOT a=f", true},     //
		{"-a=f", true},        //
		{"NOT NOT a=t", true}, //
		{"(a=t)", true},       //
		{"((a=f OR b=t) AND c=t)", true},
		{"a=f OR b=t AND c=f", false},  // OR binds tighter: (a OR b) AND c
		{"a=t AND b=f OR c=t", true},   // a AND (b OR c)
		{"a=t AND (b=f OR c=t)", true}, //
		{"NOT (a=t AND b=t)", false},   //
	}
	for _, tc := range cases {
		if got := parseExpr(t, tc.expr).Match(evalExprLeaf); got != tc.want {
			t.Errorf("Match(%q) = %v, want %v", tc.expr, got, tc.want)
		}
	}

	// A nil expression matches everything (the empty filter).
	var nilExpr *Expr
	if !nilExpr.Match(evalExprLeaf) {
		t.Error("nil expression must match everything")
	}
}

func TestParseExpressionRejects(t *testing.T) {
	cases := []string{
		"",                   // empty stream
		"a=t AND",            // dangling AND
		"a=t OR",             // dangling OR
		"NOT",                // dangling NOT
		"-",                  // dangling negation
		"(a=t",               // missing closing parenthesis
		"a=t)",               // trailing token
		"()",                 // empty group
		"a=t AND OR b=t",     // OR where a clause is expected
		"(a=t AND b=t))",     // trailing parenthesis
		"a=t AND (b=t OR )",  // dangling OR inside a group
		"a=t AND (b=t) c=f)", // trailing token after a valid expression
		"a=t AND (b=t",       // unclosed nested group
		"a=t AND NOT",        // dangling NOT after AND
	}
	for _, expr := range cases {
		toks, err := Tokenize(expr)
		if err != nil {
			continue // lexical rejection is fine
		}
		if _, err := NewParser(toks).ParseExpression(exprClause); err == nil {
			t.Errorf("ParseExpression(%q) succeeded, want an error", expr)
		} else if !strings.Contains(err.Error(), "expected") &&
			!strings.Contains(err.Error(), "missing") &&
			!strings.Contains(err.Error(), "unexpected") {
			t.Errorf("ParseExpression(%q) error = %v", expr, err)
		}
	}
}
