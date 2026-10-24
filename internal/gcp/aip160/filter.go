// Package aip160 provides the shared tokenizer and clause sequencer for the
// AIP-160 list-filter grammars the emulator implements.
//
// Several Google list surfaces document their `filter` parameter as AIP-160
// (dataproc jobs.list and clusters.list, eventarc triggers.list, ...). They
// share the same lexical form — `field op value` clauses joined by the logical
// AND, written explicitly (" AND ") or implicitly (whitespace), with values a
// bare identifier, a double-quoted string, or the `*` wildcard — and differ
// only in which fields they accept and how a clause evaluates. This package
// owns the shared part: each surface supplies its own field set and predicates
// through Parser.ParseClauses, and rejects anything outside its grammar so a
// filter is never silently ignored (which would return an unfiltered list to a
// client that asked for a subset).
package aip160

import (
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/model"
)

// Kind classifies a filter token.
type Kind int

const (
	// KindEOF is the synthetic end-of-input token.
	KindEOF Kind = iota
	// KindIdent is a bare identifier (a field name or an unquoted value).
	KindIdent
	// KindString is a double-quoted string value.
	KindString
	// KindOp is a comparison operator (`=`, `!=`, `<`, `<=`, `>`, `>=`, `:`).
	KindOp
	// KindStar is the `*` wildcard.
	KindStar
)

// Token is one lexical unit of a filter expression.
type Token struct {
	Kind Kind
	Text string
}

// Tokenize splits a filter expression into identifiers, quoted strings,
// comparison operators and the `*` wildcard. Whitespace separates tokens; an
// unterminated string or an unexpected character is an error.
func Tokenize(s string) ([]Token, error) {
	var toks []Token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '*':
			toks = append(toks, Token{Kind: KindStar, Text: "*"})
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
			toks = append(toks, Token{Kind: KindString, Text: b.String()})
			i = j
		case c == '=' || c == '!' || c == '<' || c == '>' || c == ':':
			if i+1 < len(s) {
				switch s[i : i+2] {
				case "!=", "<=", ">=":
					toks = append(toks, Token{Kind: KindOp, Text: s[i : i+2]})
					i += 2
					continue
				}
			}
			if c == '!' {
				return nil, fmt.Errorf("unexpected character %q", string(c))
			}
			toks = append(toks, Token{Kind: KindOp, Text: string(c)})
			i++
		case isIdentStart(c):
			j := i + 1
			for j < len(s) && isIdentPart(s[j]) {
				j++
			}
			toks = append(toks, Token{Kind: KindIdent, Text: s[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q", string(c))
		}
	}
	toks = append(toks, Token{Kind: KindEOF})
	return toks, nil
}

func isIdentStart(c byte) bool {
	return c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || c == '-' || c == '.'
}

// TokenValue returns an identifier or quoted-string token's text.
func TokenValue(t Token) (string, bool) {
	switch t.Kind {
	case KindIdent, KindString:
		return t.Text, true
	}
	return "", false
}

// TokenText renders a token for an error message.
func TokenText(t Token) string {
	if t.Kind == KindEOF {
		return "end of filter"
	}
	return fmt.Sprintf("%q", t.Text)
}

// Parser is the shared clause-sequencing parser over a token stream. It
// consumes a conjunction of clauses separated by an explicit "AND" keyword or
// by whitespace (implicit AND), delegating each clause to a surface-specific
// clause function that returns that surface's predicate type as an `any`. A
// trailing "AND" with no following clause is an error.
type Parser struct {
	toks []Token
	i    int
}

// NewParser returns a Parser positioned at the start of toks.
func NewParser(toks []Token) *Parser { return &Parser{toks: toks} }

// Peek returns the current token without consuming it.
func (p *Parser) Peek() Token { return p.toks[p.i] }

// Next consumes and returns the current token; at end of input it returns the
// EOF token without advancing.
func (p *Parser) Next() Token {
	t := p.toks[p.i]
	if t.Kind != KindEOF {
		p.i++
	}
	return t
}

// ParseClauses parses a conjunction of clauses, returning one predicate per
// clause in source order. An empty token stream (already consumed to EOF) is
// rejected by clause.
func (p *Parser) ParseClauses(clause func(*Parser) (any, error)) ([]any, error) {
	var preds []any
	for {
		pred, err := clause(p)
		if err != nil {
			return nil, err
		}
		preds = append(preds, pred)

		t := p.Peek()
		switch {
		case t.Kind == KindEOF:
			return preds, nil
		case t.Kind == KindIdent && strings.EqualFold(t.Text, "AND"):
			p.Next()
			if p.Peek().Kind == KindEOF {
				return nil, fmt.Errorf("dangling AND")
			}
			// Loop and parse the next clause.
		case t.Kind == KindIdent:
			// Implicit AND: the next token starts another field.
		default:
			return nil, fmt.Errorf("unexpected token %s", TokenText(t))
		}
	}
}

// ─── Generic clause compiler ─────────────────────────────────────────────────

// Fields describes one list surface's filterable field set for Compile. A nil
// accessor means the surface does not accept that field group: a surface with
// no Labels rejects every labels.<key> clause, and a nil Name rejects `name`.
// Desc names the accepted keys for the error a rejected clause returns.
type Fields[T any] struct {
	// Desc lists the accepted keys, e.g. "name, state, labels.<key> and createTime".
	Desc string
	// Name is the resource's canonical (full) name, filtered by the identity
	// key `name`. Nil when the surface documents no name filter.
	Name func(T) string
	// Labels is the resource's label map, consulted by `labels.<key>` and the
	// bare `labels` existence clause. Nil when the resource has no labels.
	Labels func(T) map[string]string
	// Strings are the remaining string-valued fields, keyed by folded name.
	Strings map[string]func(T) string
	// Times are the timestamp fields, keyed by folded name.
	Times map[string]func(T) time.Time
}

// Predicate is one compiled filter clause over an item type T.
type Predicate[T any] func(T) bool

// Filter is a conjunction of compiled clauses. An empty filter matches every
// item.
type Filter[T any] struct {
	preds []Predicate[T]
}

// Match reports whether v satisfies every clause.
func (f Filter[T]) Match(v T) bool {
	for _, p := range f.preds {
		if !p(v) {
			return false
		}
	}
	return true
}

// Compile parses one surface's bounded AIP-160 filter grammar:
//
//	clause := field op value
//	filter := clause ( ("AND" | implicit whitespace) clause )*
//	field  := "name" | "labels." <key> | "labels" | <a surface string/time field>
//	op     := "=" | "!=" | ":" | "<" | "<=" | ">" | ">="
//	value  := <ident> | <double-quoted string> | "*"
//
// name and the string fields accept "=", "!=" and ":" (":" is AIP-160's
// has/contains operator, a case-insensitive substring match, as the Cloud KMS
// and Secret Manager filtering docs specify); "*" tests that the field is
// present (non-empty). Timestamp fields accept the six comparisons (not ":")
// against an RFC3339 timestamp or YYYY-MM-DD date, and "*" tests that the time
// is set. labels.<key> accepts "=", "!=" and ":" against a value, "*" (the key
// is present with any value) or "!= *" (the key is absent); the bare `labels`
// takes only "*" and matches a resource carrying any label.
//
// Anything outside the grammar — OR/NOT, parenthesis groups, an unknown field,
// a comparison on the wrong field kind — is INVALID_ARGUMENT, so a filter is
// never silently ignored and never returns an unfiltered list to a client that
// asked for a subset. label prefixes the error so a client can tell which list
// method rejected the clause.
func Compile[T any](label, expr string, fields Fields[T]) (Filter[T], error) {
	if strings.TrimSpace(expr) == "" {
		return Filter[T]{}, nil
	}
	toks, err := Tokenize(expr)
	if err != nil {
		return Filter[T]{}, invalidArgument(label + " filter: " + err.Error())
	}
	clause := func(p *Parser) (any, error) { return parseClause(p, fields) }
	preds, err := NewParser(toks).ParseClauses(clause)
	if err != nil {
		return Filter[T]{}, invalidArgument(label + " filter: " + err.Error())
	}
	f := Filter[T]{preds: make([]Predicate[T], 0, len(preds))}
	for _, p := range preds {
		pred, ok := p.(Predicate[T])
		if !ok {
			return Filter[T]{}, invalidArgument(label + " filter: internal clause type")
		}
		f.preds = append(f.preds, pred)
	}
	return f, nil
}

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// containsFold reports whether haystack contains needle as a case-insensitive
// substring — AIP-160's ":" containment, which the Cloud KMS and Secret Manager
// filtering docs define as case-insensitive.
func containsFold(haystack, needle string) bool {
	return strings.Contains(strings.ToLower(haystack), strings.ToLower(needle))
}

// parseClause parses one filter clause (`field op value`) into the matching
// Predicate, returned as an `any` for the shared clause sequencer.
func parseClause[T any](p *Parser, fields Fields[T]) (any, error) {
	keyTok := p.Next()
	if keyTok.Kind != KindIdent {
		return nil, fmt.Errorf("expected field, got %s", TokenText(keyTok))
	}
	key := keyTok.Text
	opTok := p.Next()
	if opTok.Kind != KindOp {
		return nil, fmt.Errorf("expected operator after %q, got %s", key, TokenText(opTok))
	}
	op := opTok.Text
	folded := foldOrderName(key)

	switch {
	case folded == "name":
		if fields.Name == nil {
			return nil, fmt.Errorf("this list has no name filter")
		}
		return parseStringClause(p, key, op, fields.Name)
	case strings.HasPrefix(folded, "labels.") && fields.Labels != nil:
		lk := labelKeyOf(key)
		if lk == "" {
			return nil, fmt.Errorf("empty label key in %q", key)
		}
		return parseLabelClause(p, op, lk, fields.Labels)
	case folded == "labels" && fields.Labels != nil:
		return parseLabelsExistsClause(p, op, fields.Labels)
	}
	if get, ok := fields.Strings[folded]; ok {
		return parseStringClause(p, key, op, get)
	}
	if get, ok := fields.Times[folded]; ok {
		return parseTimeClause(p, key, op, get)
	}
	desc := fields.Desc
	if desc == "" {
		desc = "the documented fields"
	}
	return nil, fmt.Errorf("unsupported filter key %q (only %s are supported)", key, desc)
}

// labelKeyOf returns the label key of a `labels.<key>` field name, preserving
// the caller's spelling/case.
func labelKeyOf(key string) string {
	i := strings.IndexByte(key, '.')
	if i < 0 || !strings.EqualFold(key[:i], "labels") {
		return ""
	}
	return key[i+1:]
}

// parseStringClause compiles an identity/string-field clause. "=" and "!=" are
// exact matches; ":" is AIP-160's has/contains operator (a substring match) and
// "*" tests that the field is non-empty.
func parseStringClause[T any](p *Parser, key, op string, get func(T) string) (any, error) {
	if op != "=" && op != "!=" && op != ":" {
		return nil, fmt.Errorf("unsupported operator %q on %s (only \"=\", \"!=\" and \":\" are supported)", op, key)
	}
	valTok := p.Next()
	if valTok.Kind == KindStar {
		if op == "=" || op == ":" {
			return Predicate[T](func(v T) bool { return get(v) != "" }), nil
		}
		return nil, fmt.Errorf("%s does not accept \"*\" with %q", key, op)
	}
	val, ok := TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("%s value must be a value or a quoted string, got %s", key, TokenText(valTok))
	}
	switch op {
	case "=":
		return Predicate[T](func(v T) bool { return get(v) == val }), nil
	case "!=":
		return Predicate[T](func(v T) bool { return get(v) != val }), nil
	default:
		return Predicate[T](func(v T) bool { return containsFold(get(v), val) }), nil
	}
}

// parseLabelClause compiles a labels.<key> clause. "=" and "!=" are exact
// matches; ":" is a (case-insensitive) substring match; "*" tests that the key
// is present with any value, and "!= *" tests that it is absent.
func parseLabelClause[T any](p *Parser, op, key string, labels func(T) map[string]string) (any, error) {
	if op != "=" && op != "!=" && op != ":" {
		return nil, fmt.Errorf("unsupported operator %q on labels.%s (only \"=\", \"!=\" and \":\" are supported)", op, key)
	}
	valTok := p.Next()
	if valTok.Kind == KindStar {
		if op == "!=" {
			return Predicate[T](func(v T) bool { _, ok := labels(v)[key]; return !ok }), nil
		}
		if op == ":" || op == "=" {
			return Predicate[T](func(v T) bool { _, ok := labels(v)[key]; return ok }), nil
		}
	}
	val, ok := TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("label value must be a value, a quoted string or \"*\", got %s", TokenText(valTok))
	}
	switch op {
	case "=":
		return Predicate[T](func(v T) bool { return labels(v)[key] == val }), nil
	case "!=":
		return Predicate[T](func(v T) bool { return labels(v)[key] != val }), nil
	default:
		return Predicate[T](func(v T) bool { return containsFold(labels(v)[key], val) }), nil
	}
}

// parseLabelsExistsClause compiles the bare `labels` clause, which takes only
// "*" and matches a resource carrying at least one label.
func parseLabelsExistsClause[T any](p *Parser, op string, labels func(T) map[string]string) (any, error) {
	if op != ":" && op != "=" {
		return nil, fmt.Errorf("unsupported operator %q on labels (only \":\" and \"=\" are supported)", op)
	}
	if v := p.Next(); v.Kind != KindStar {
		return nil, fmt.Errorf("labels takes only \"*\" (any label), got %s", TokenText(v))
	}
	return Predicate[T](func(v T) bool { return len(labels(v)) > 0 }), nil
}

// parseTimeClause compiles a timestamp comparison. The value must be an RFC3339
// timestamp or a YYYY-MM-DD date; the six comparison operators are supported,
// ":" is not, and "*" tests that the timestamp is set.
func parseTimeClause[T any](p *Parser, key, op string, get func(T) time.Time) (any, error) {
	valTok := p.Next()
	if valTok.Kind == KindStar {
		if op == "=" || op == ":" {
			return Predicate[T](func(v T) bool { return !get(v).IsZero() }), nil
		}
		return nil, fmt.Errorf("%s does not accept \"*\" with %q", key, op)
	}
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
	default:
		return nil, fmt.Errorf("unsupported operator %q on %s (only =, !=, <, <=, > and >= are supported)", op, key)
	}
	val, ok := TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("%s value must be an RFC3339 timestamp or YYYY-MM-DD date, got %s", key, TokenText(valTok))
	}
	bound, err := ParseTime(val)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", key, err)
	}
	want := bound.UTC().Format(TimeLayout)
	return Predicate[T](func(v T) bool {
		got := get(v).UTC().Format(TimeLayout)
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
