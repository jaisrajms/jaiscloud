package aip160

import (
	"strings"
	"testing"
)

// TestTokenize covers the lexical forms the schema surfaces rely on: bare
// identifiers (field names and unquoted values), double-quoted strings with
// escapes, the comparison operators, and the `*` wildcard.
func TestTokenize(t *testing.T) {
	toks, err := Tokenize(`status.state = "RUNNING" AND labels.a:b = c*`)
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	want := []Token{
		{Kind: KindIdent, Text: "status.state"},
		{Kind: KindOp, Text: "="},
		{Kind: KindString, Text: "RUNNING"},
		{Kind: KindIdent, Text: "AND"},
		{Kind: KindIdent, Text: "labels.a"},
		{Kind: KindOp, Text: ":"},
		{Kind: KindIdent, Text: "b"},
		{Kind: KindOp, Text: "="},
		{Kind: KindIdent, Text: "c"},
		{Kind: KindStar, Text: "*"},
		{Kind: KindEOF},
	}
	if len(toks) != len(want) {
		t.Fatalf("tokens = %+v, want %+v", toks, want)
	}
	for i := range want {
		if toks[i] != want[i] {
			t.Errorf("token %d = %+v, want %+v", i, toks[i], want[i])
		}
	}

	// Every comparison operator, including the two-character forms.
	ops, err := Tokenize("a!=b c<=d e>=f g<h i>j")
	if err != nil {
		t.Fatalf("operators: %v", err)
	}
	var got []string
	for _, tok := range ops {
		if tok.Kind == KindOp {
			got = append(got, tok.Text)
		}
	}
	if strings.Join(got, ",") != "!=,<=,>=,<,>" {
		t.Errorf("operators = %v", got)
	}

	// A quoted value keeps spaces and honours backslash escapes.
	q, err := Tokenize(`name = "a \"b\" c"`)
	if err != nil {
		t.Fatalf("quoted: %v", err)
	}
	if v, ok := TokenValue(q[2]); !ok || v != `a "b" c` {
		t.Errorf("quoted value = %q (ok=%v)", v, ok)
	}

	// Errors: an unterminated string, a bare "!" and any other character.
	for _, bad := range []string{`x = "unterminated`, "x ! y", "x = y@z", "x = y/"} {
		if _, err := Tokenize(bad); err == nil {
			t.Errorf("Tokenize(%q) = nil error, want a lexical error", bad)
		}
	}
}

// TestTokenHelpers covers TokenValue/TokenText, including the EOF rendering an
// error message depends on.
func TestTokenHelpers(t *testing.T) {
	if v, ok := TokenValue(Token{Kind: KindIdent, Text: "x"}); !ok || v != "x" {
		t.Errorf("ident TokenValue = %q, ok=%v", v, ok)
	}
	if v, ok := TokenValue(Token{Kind: KindString, Text: "x"}); !ok || v != "x" {
		t.Errorf("string TokenValue = %q, ok=%v", v, ok)
	}
	if _, ok := TokenValue(Token{Kind: KindStar, Text: "*"}); ok {
		t.Error("a star token must carry no value")
	}
	if _, ok := TokenValue(Token{Kind: KindOp, Text: "="}); ok {
		t.Error("an operator token must carry no value")
	}
	if got := TokenText(Token{Kind: KindEOF}); got != "end of filter" {
		t.Errorf("EOF TokenText = %q", got)
	}
	if got := TokenText(Token{Kind: KindIdent, Text: "x"}); got != `"x"` {
		t.Errorf("ident TokenText = %q", got)
	}
}

// TestParseClauses covers the shared clause sequencer: explicit AND, implicit
// AND, the per-clause predicate order, and the two malformed forms (a dangling
// AND and a trailing operator).
func TestParseClauses(t *testing.T) {
	// clause consumes `key = value` and returns the key as its predicate.
	clause := func(p *Parser) (any, error) {
		key := p.Next()
		if key.Kind != KindIdent {
			return nil, errString("expected field")
		}
		if op := p.Next(); op.Kind != KindOp {
			return nil, errString("expected operator")
		}
		if v := p.Next(); v.Kind != KindIdent && v.Kind != KindString {
			return nil, errString("expected value")
		}
		return key.Text, nil
	}

	for _, expr := range []string{"a=1", "a=1 b=2", "a=1 AND b=2", "a=1 AND b=2 AND c=3"} {
		toks, err := Tokenize(expr)
		if err != nil {
			t.Fatalf("Tokenize(%q): %v", expr, err)
		}
		preds, err := NewParser(toks).ParseClauses(clause)
		if err != nil {
			t.Fatalf("ParseClauses(%q): %v", expr, err)
		}
		if len(preds) == 0 {
			t.Fatalf("ParseClauses(%q) returned no predicates", expr)
		}
	}

	for _, bad := range []string{"a=1 AND", "a=1 AND AND b=2", "a=", "=1"} {
		toks, err := Tokenize(bad)
		if err != nil {
			continue // a lexical error is also a rejection
		}
		if _, err := NewParser(toks).ParseClauses(clause); err == nil {
			t.Errorf("ParseClauses(%q) = nil error, want a rejection", bad)
		}
	}
}

// errString is a minimal error for the clause functions above.
type errString string

func (e errString) Error() string { return string(e) }
