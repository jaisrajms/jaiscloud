package datastore

import "testing"

// lexTokens lexes s and returns the tokens, failing on a lex error. The token
// stream always ends with an EOF token.
func lexTokens(t *testing.T, s string) []gqlToken {
	t.Helper()
	toks, err := lexGQL(s)
	if err != nil {
		t.Fatalf("lexGQL(%q): %v", s, err)
	}
	return toks
}

type wantToken struct {
	kind tokKind
	text string
}

// TestLexGQLTokens covers every punctuation operator, identifiers, bindings,
// string literals, number literals and whitespace skipping.
func TestLexGQLTokens(t *testing.T) {
	cases := []struct {
		in   string
		want []wantToken
	}{
		{"()", []wantToken{{tokLParen, "("}, {tokRParen, ")"}, {tokEOF, ""}}},
		{", *", []wantToken{{tokComma, ","}, {tokStar, "*"}, {tokEOF, ""}}},
		{"=", []wantToken{{tokEq, "="}, {tokEOF, ""}}},
		{"!=", []wantToken{{tokNeq, "!="}, {tokEOF, ""}}},
		{"<", []wantToken{{tokLt, "<"}, {tokEOF, ""}}},
		{"<=", []wantToken{{tokLe, "<="}, {tokEOF, ""}}},
		{">", []wantToken{{tokGt, ">"}, {tokEOF, ""}}},
		{">=", []wantToken{{tokGe, ">="}, {tokEOF, ""}}},
		{"name _x y.z", []wantToken{{tokIdent, "name"}, {tokIdent, "_x"}, {tokIdent, "y.z"}, {tokEOF, ""}}},
		{"@p", []wantToken{{tokBinding, "p"}, {tokEOF, ""}}},
		{"@a$b1", []wantToken{{tokBinding, "a$b1"}, {tokEOF, ""}}},
		{"'sq'", []wantToken{{tokString, "sq"}, {tokEOF, ""}}},
		{`"dq"`, []wantToken{{tokString, "dq"}, {tokEOF, ""}}},
		{"1", []wantToken{{tokInt, "1"}, {tokEOF, ""}}},
		{"-2", []wantToken{{tokInt, "-2"}, {tokEOF, ""}}},
		{"3.5", []wantToken{{tokFloat, "3.5"}, {tokEOF, ""}}},
		{"1e10", []wantToken{{tokFloat, "1e10"}, {tokEOF, ""}}},
		{"1E-3", []wantToken{{tokFloat, "1E-3"}, {tokEOF, ""}}},
		{"1e+2", []wantToken{{tokFloat, "1e+2"}, {tokEOF, ""}}},
		{" \t\r\n x", []wantToken{{tokIdent, "x"}, {tokEOF, ""}}},
	}
	for _, tc := range cases {
		got := lexTokens(t, tc.in)
		if len(got) != len(tc.want) {
			t.Errorf("lexGQL(%q) = %d tokens, want %d (%v)", tc.in, len(got), len(tc.want), got)
			continue
		}
		for i := range got {
			if got[i].kind != tc.want[i].kind || got[i].text != tc.want[i].text {
				t.Errorf("lexGQL(%q) token %d = {%d,%q}, want {%d,%q}", tc.in, i, got[i].kind, got[i].text, tc.want[i].kind, tc.want[i].text)
			}
		}
	}
}

// TestLexGQLErrors covers the fail-closed lexer inputs, including an operator
// character at the very end of the input (a bounds edge) and an empty binding.
func TestLexGQLErrors(t *testing.T) {
	for _, s := range []string{
		"!",   // '!' with no '='
		"a!",  // '!' at end of input
		"#",   // unsupported character
		"@",   // empty binding
		"x @", // empty binding after an identifier
		"'unterminated",
		`"unterminated`,
	} {
		if _, err := lexGQL(s); err == nil {
			t.Errorf("lexGQL(%q) should fail", s)
		}
	}
}

// TestLexString covers the escape handling and the unterminated cases.
func TestLexString(t *testing.T) {
	if v, n, err := lexString(`'a\'b'`, 0); err != nil || v != "a'b" || n != 6 {
		t.Fatalf("lexString(escaped quote) = %q,%d,%v; want a'b,6,nil", v, n, err)
	}
	if v, _, err := lexString(`"a\\b"`, 0); err != nil || v != `a\b` {
		t.Fatalf("lexString(escaped backslash) = %q,%v; want a\\b", v, err)
	}
	if v, _, err := lexString(`'a\"b'`, 0); err != nil || v != `a"b` {
		t.Fatalf("lexString(escaped dquote) = %q,%v", v, err)
	}
	if _, _, err := lexString(`'abc`, 0); err == nil {
		t.Fatal("an unterminated string literal must fail")
	}
	if _, _, err := lexString(`'\`, 0); err == nil {
		t.Fatal("a trailing escape must fail")
	}
}

// TestLexNumber covers integer, float and exponent literals and the point at
// which a malformed exponent stops the number.
func TestLexNumber(t *testing.T) {
	cases := []struct {
		in   string
		text string
		kind tokKind
	}{
		{"123", "123", tokInt},
		{"-4", "-4", tokInt},
		{"3.14", "3.14", tokFloat},
		{"1e10", "1e10", tokFloat},
		{"1E+2", "1E+2", tokFloat},
		{"1e-2", "1e-2", tokFloat},
		{"12x", "12", tokInt},
		{"1e", "1", tokInt},
	}
	for _, tc := range cases {
		text, kind := lexNumber(tc.in)
		if text != tc.text || kind != tc.kind {
			t.Errorf("lexNumber(%q) = %q,%d; want %q,%d", tc.in, text, kind, tc.text, tc.kind)
		}
	}
}

// TestCharClasses covers the character-class predicates' boundary characters.
func TestCharClasses(t *testing.T) {
	for _, c := range []byte{'0', '9'} {
		if !isDigit(c) {
			t.Errorf("isDigit(%q) = false", c)
		}
	}
	for _, c := range []byte{'/', ':', 'a'} {
		if isDigit(c) {
			t.Errorf("isDigit(%q) = true", c)
		}
	}
	for _, c := range []byte{'_', 'a', 'z', 'A', 'Z'} {
		if !isIdentStart(c) {
			t.Errorf("isIdentStart(%q) = false", c)
		}
	}
	for _, c := range []byte{'0', '.', '$'} {
		if isIdentStart(c) {
			t.Errorf("isIdentStart(%q) = true", c)
		}
	}
	if !isIdentChar('.') || !isIdentChar('0') || isIdentChar('$') {
		t.Fatal("isIdentChar membership is wrong")
	}
	if !isBindingChar('$') || !isBindingChar('0') || isBindingChar('.') {
		t.Fatal("isBindingChar membership is wrong")
	}
}
