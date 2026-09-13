// Package pubsubfilter parses and evaluates Cloud Pub/Sub subscription filters.
//
// A filter selects which messages a subscription receives, based on message
// attributes. The supported grammar mirrors the commonly used subset of the
// real Pub/Sub filter language:
//
//	expr        := term ( "OR" term )*
//	term        := factor ( "AND" factor )*
//	factor      := "NOT" factor | "(" expr ")" | predicate
//	predicate   := attrKey ( "=" | "!=" ) string
//	             | "hasPrefix" "(" attrKey "," string ")"
//	attrKey     := "attributes" ( "." | ":" ) ident
//	string      := '"' escaped-char* '"'
//
// Anything outside this grammar is rejected, so an unparseable filter makes
// SubscriptionCreate fail with InvalidArgument (matching real Pub/Sub, where a
// filter is validated at creation and is immutable thereafter).
package pubsubfilter

import (
	"fmt"
	"strings"
)

// Filter is a compiled subscription filter.
type Filter struct {
	expr node
	src  string
}

// Match reports whether the message attributes satisfy the filter.
func (f *Filter) Match(attrs map[string]string) bool {
	if f == nil || f.expr == nil {
		return true
	}
	return f.expr.eval(attrs)
}

// String returns the original filter expression.
func (f *Filter) String() string { return f.src }

// Compile parses expr into a Filter. An empty expression compiles to a nil
// filter (match everything).
func Compile(expr string) (*Filter, error) {
	if strings.TrimSpace(expr) == "" {
		return nil, nil
	}
	p := &parser{tokens: tokenize(expr)}
	n, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokEOF {
		return nil, fmt.Errorf("unexpected token %q", p.peek().text)
	}
	return &Filter{expr: n, src: expr}, nil
}

// ─── AST ──────────────────────────────────────────────────────────────────────

type node interface {
	eval(attrs map[string]string) bool
}

type orNode struct{ left, right node }

func (n orNode) eval(a map[string]string) bool { return n.left.eval(a) || n.right.eval(a) }

type andNode struct{ left, right node }

func (n andNode) eval(a map[string]string) bool { return n.left.eval(a) && n.right.eval(a) }

type notNode struct{ inner node }

func (n notNode) eval(a map[string]string) bool { return !n.inner.eval(a) }

type cmpNode struct {
	key   string
	value string
	neq   bool
}

func (n cmpNode) eval(a map[string]string) bool {
	v, ok := a[n.key]
	if n.neq {
		return !ok || v != n.value
	}
	return ok && v == n.value
}

type prefixNode struct {
	key    string
	prefix string
}

func (n prefixNode) eval(a map[string]string) bool {
	v, ok := a[n.key]
	return ok && strings.HasPrefix(v, n.prefix)
}

// ─── Lexer ────────────────────────────────────────────────────────────────────

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokString
	tokLParen
	tokRParen
	tokDot
	tokColon
	tokComma
	tokEq
	tokNeq
)

type token struct {
	kind tokKind
	text string
}

func tokenize(s string) []token {
	var toks []token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, token{tokLParen, "("})
			i++
		case c == ')':
			toks = append(toks, token{tokRParen, ")"})
			i++
		case c == ',':
			toks = append(toks, token{tokComma, ","})
			i++
		case c == '.':
			toks = append(toks, token{tokDot, "."})
			i++
		case c == ':':
			toks = append(toks, token{tokColon, ":"})
			i++
		case c == '=':
			toks = append(toks, token{tokEq, "="})
			i++
		case c == '!':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, token{tokNeq, "!="})
				i += 2
			} else {
				toks = append(toks, token{tokIdent, "!"})
				i++
			}
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
				toks = append(toks, token{tokIdent, s[i:]})
				i = len(s)
				continue
			}
			toks = append(toks, token{tokString, b.String()})
			i = j
		case isIdentStart(c):
			j := i + 1
			for j < len(s) && isIdentPart(s[j]) {
				j++
			}
			toks = append(toks, token{tokIdent, s[i:j]})
			i = j
		default:
			// Unknown character: emit as an ident so the parser rejects it.
			toks = append(toks, token{tokIdent, string(c)})
			i++
		}
	}
	toks = append(toks, token{tokEOF, ""})
	return toks
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '-'
}

// ─── Parser ───────────────────────────────────────────────────────────────────

type parser struct {
	tokens []token
	pos    int
}

func (p *parser) peek() token { return p.tokens[p.pos] }

func (p *parser) next() token {
	t := p.tokens[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}
	return t
}

func (p *parser) parseExpr() (node, error) { return p.parseOr() }

func (p *parser) parseOr() (node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("or") {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = orNode{left, right}
	}
	return left, nil
}

func (p *parser) parseAnd() (node, error) {
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("and") {
		p.next()
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		left = andNode{left, right}
	}
	return left, nil
}

func (p *parser) parseFactor() (node, error) {
	if p.isKeyword("not") {
		p.next()
		inner, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		return notNode{inner}, nil
	}
	if p.peek().kind == tokLParen {
		p.next()
		n, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.next().kind != tokRParen {
			return nil, fmt.Errorf("expected ')'")
		}
		return n, nil
	}
	return p.parsePredicate()
}

func (p *parser) parsePredicate() (node, error) {
	if p.isKeyword("hasprefix") {
		p.next()
		if p.next().kind != tokLParen {
			return nil, fmt.Errorf("expected '(' after hasPrefix")
		}
		key, err := p.parseAttrKey()
		if err != nil {
			return nil, err
		}
		if p.next().kind != tokComma {
			return nil, fmt.Errorf("expected ',' in hasPrefix")
		}
		if p.peek().kind != tokString {
			return nil, fmt.Errorf("expected quoted string in hasPrefix")
		}
		val := p.next().text
		if p.next().kind != tokRParen {
			return nil, fmt.Errorf("expected ')' after hasPrefix")
		}
		return prefixNode{key: key, prefix: val}, nil
	}

	key, err := p.parseAttrKey()
	if err != nil {
		return nil, err
	}
	switch p.next().kind {
	case tokEq:
		if p.peek().kind != tokString {
			return nil, fmt.Errorf("expected quoted string after '='")
		}
		return cmpNode{key: key, value: p.next().text}, nil
	case tokNeq:
		if p.peek().kind != tokString {
			return nil, fmt.Errorf("expected quoted string after '!='")
		}
		return cmpNode{key: key, value: p.next().text, neq: true}, nil
	default:
		return nil, fmt.Errorf("expected '=' or '!=' in filter")
	}
}

// parseAttrKey parses "attributes" ("."|":") ident.
func (p *parser) parseAttrKey() (string, error) {
	if !p.isKeyword("attributes") {
		return "", fmt.Errorf("expected 'attributes'")
	}
	p.next()
	if k := p.next().kind; k != tokDot && k != tokColon {
		return "", fmt.Errorf("expected '.' or ':' after attributes")
	}
	id := p.next()
	if id.kind != tokIdent || !isIdentStart(id.text[0]) {
		return "", fmt.Errorf("expected attribute name")
	}
	return id.text, nil
}

func (p *parser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tokIdent && strings.EqualFold(t.text, kw)
}
