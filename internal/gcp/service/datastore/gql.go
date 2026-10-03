package datastore

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// GQLBinding is one resolved GQL query parameter: either an inline value or an
// opaque cursor. The core does not model cursors, so a cursor binding is
// rejected when it is referenced.
type GQLBinding struct {
	Value  *dsstore.Value
	Cursor []byte
}

// GQLQuery is the transport-neutral GQL query: the query string plus its
// bindings. Both transports transcode their wire form (datastorepb.GqlQuery and
// the Discovery-shaped JSON) into this type and hand it to the core, so the
// parser is shared and the two transports cannot drift.
type GQLQuery struct {
	QueryString        string
	AllowLiterals      bool
	NamedBindings      map[string]GQLBinding
	PositionalBindings []GQLBinding
}

// ParseGQL parses a GQL SELECT into the transport-neutral structured Query the
// core executes.
//
// The emulator's query engine models only kind, filter, offset, and limit, so
// projection, ORDER BY, and cursors are parsed (or ignored) for wire
// compatibility but are not applied — the same documented limitation the
// structured-query path already has. Anything the engine cannot express fails
// closed with InvalidArgument rather than silently matching everything.
func ParseGQL(q GQLQuery) (*Query, error) {
	p, err := newGQLParser(q)
	if err != nil {
		return nil, err
	}
	out, err := p.parseQuery()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokEOF {
		return nil, p.errorf("unexpected token %q", p.peek().text)
	}
	return out, nil
}

// ParseGQLAggregation parses a GQL aggregation query
// ("AGGREGATE COUNT(*) OVER (SELECT ...)") into the transport-neutral
// AggregationQuery the core executes.
func ParseGQLAggregation(q GQLQuery) (*AggregationQuery, error) {
	p, err := newGQLParser(q)
	if err != nil {
		return nil, err
	}
	out, err := p.parseAggregation()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tokEOF {
		return nil, p.errorf("unexpected token %q", p.peek().text)
	}
	return out, nil
}

// ─── lexer ────────────────────────────────────────────────────────────────────

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokBinding
	tokString
	tokInt
	tokFloat
	tokLParen
	tokRParen
	tokComma
	tokStar
	tokEq
	tokNeq
	tokLt
	tokLe
	tokGt
	tokGe
)

type gqlToken struct {
	kind tokKind
	text string
	pos  int
}

func lexGQL(s string) ([]gqlToken, error) {
	var toks []gqlToken
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, gqlToken{kind: tokLParen, text: "(", pos: i})
			i++
		case c == ')':
			toks = append(toks, gqlToken{kind: tokRParen, text: ")", pos: i})
			i++
		case c == ',':
			toks = append(toks, gqlToken{kind: tokComma, text: ",", pos: i})
			i++
		case c == '*':
			toks = append(toks, gqlToken{kind: tokStar, text: "*", pos: i})
			i++
		case c == '=':
			toks = append(toks, gqlToken{kind: tokEq, text: "=", pos: i})
			i++
		case c == '!':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, gqlToken{kind: tokNeq, text: "!=", pos: i})
				i += 2
				continue
			}
			return nil, badGQLChar(c, i)
		case c == '<':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, gqlToken{kind: tokLe, text: "<=", pos: i})
				i += 2
				continue
			}
			toks = append(toks, gqlToken{kind: tokLt, text: "<", pos: i})
			i++
		case c == '>':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, gqlToken{kind: tokGe, text: ">=", pos: i})
				i += 2
				continue
			}
			toks = append(toks, gqlToken{kind: tokGt, text: ">", pos: i})
			i++
		case c == '@':
			j := i + 1
			for j < len(s) && isBindingChar(s[j]) {
				j++
			}
			if j == i+1 {
				return nil, invalidArgument(fmt.Sprintf("invalid GQL query: empty binding at position %d", i))
			}
			toks = append(toks, gqlToken{kind: tokBinding, text: s[i+1 : j], pos: i})
			i = j
		case c == '\'' || c == '"':
			val, n, err := lexString(s, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, gqlToken{kind: tokString, text: val, pos: i})
			i += n
		case isDigit(c) || (c == '-' && i+1 < len(s) && isDigit(s[i+1])):
			text, kind := lexNumber(s[i:])
			toks = append(toks, gqlToken{kind: kind, text: text, pos: i})
			i += len(text)
		case isIdentStart(c):
			j := i + 1
			for j < len(s) && isIdentChar(s[j]) {
				j++
			}
			toks = append(toks, gqlToken{kind: tokIdent, text: s[i:j], pos: i})
			i = j
		default:
			return nil, badGQLChar(c, i)
		}
	}
	toks = append(toks, gqlToken{kind: tokEOF, pos: len(s)})
	return toks, nil
}

func badGQLChar(c byte, pos int) error {
	return invalidArgument(fmt.Sprintf("invalid GQL query: unexpected character %q at position %d", c, pos))
}

// lexString reads a single- or double-quoted GQL string literal starting at i,
// returning its unescaped value, the consumed length, and any error. The
// escapes \\, \' and \" are recognized; an unterminated literal is an error.
func lexString(s string, i int) (string, int, error) {
	quote := s[i]
	var b strings.Builder
	j := i + 1
	for j < len(s) {
		c := s[j]
		switch c {
		case '\\':
			if j+1 >= len(s) {
				return "", 0, invalidArgument("invalid GQL query: unterminated escape sequence")
			}
			switch s[j+1] {
			case '\\', '\'', '"':
				b.WriteByte(s[j+1])
			default:
				b.WriteByte(s[j+1])
			}
			j += 2
		case quote:
			return b.String(), j - i + 1, nil
		default:
			b.WriteByte(c)
			j++
		}
	}
	return "", 0, invalidArgument("invalid GQL query: unterminated string literal")
}

// lexNumber reads an integer or floating-point literal from the front of s and
// returns its text and token kind.
func lexNumber(s string) (string, tokKind) {
	i := 0
	if s[i] == '-' {
		i++
	}
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	kind := tokInt
	if i < len(s) && s[i] == '.' {
		kind = tokFloat
		i++
		for i < len(s) && isDigit(s[i]) {
			i++
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		if j < len(s) && isDigit(s[j]) {
			kind = tokFloat
			i = j
			for i < len(s) && isDigit(s[i]) {
				i++
			}
		}
	}
	return s[:i], kind
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || isDigit(c) || c == '.'
}

func isBindingChar(c byte) bool {
	return isIdentStart(c) || isDigit(c) || c == '$'
}

// ─── parser ───────────────────────────────────────────────────────────────────

type gqlParser struct {
	toks []gqlToken
	pos  int
	gql  GQLQuery
}

func newGQLParser(q GQLQuery) (*gqlParser, error) {
	toks, err := lexGQL(q.QueryString)
	if err != nil {
		return nil, err
	}
	return &gqlParser{toks: toks, gql: q}, nil
}

func (p *gqlParser) peek() gqlToken {
	if p.pos >= len(p.toks) {
		return gqlToken{kind: tokEOF}
	}
	return p.toks[p.pos]
}

func (p *gqlParser) next() gqlToken {
	t := p.peek()
	p.pos++
	return t
}

func (p *gqlParser) errorf(format string, args ...any) error {
	return invalidArgument("invalid GQL query: " + fmt.Sprintf(format, args...))
}

func (p *gqlParser) atKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tokIdent && strings.EqualFold(t.text, kw)
}

func (p *gqlParser) accept(tk tokKind) bool {
	if p.peek().kind == tk {
		p.pos++
		return true
	}
	return false
}

func (p *gqlParser) acceptKeyword(kw string) bool {
	if p.atKeyword(kw) {
		p.pos++
		return true
	}
	return false
}

func (p *gqlParser) expect(tk tokKind, what string) (gqlToken, error) {
	t := p.peek()
	if t.kind != tk {
		return t, p.errorf("expected %s, got %q", what, t.text)
	}
	p.pos++
	return t, nil
}

func (p *gqlParser) expectKeyword(kw string) error {
	if !p.acceptKeyword(kw) {
		return p.errorf("expected %s, got %q", kw, p.peek().text)
	}
	return nil
}

func (p *gqlParser) expectIdent(what string) (string, error) {
	t, err := p.expect(tokIdent, what)
	if err != nil {
		return "", err
	}
	return t.text, nil
}

func (p *gqlParser) expectNonNegInt() (int, error) {
	t, err := p.expect(tokInt, "integer")
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(t.text)
	if err != nil || n < 0 {
		return 0, p.errorf("expected a non-negative integer, got %q", t.text)
	}
	return n, nil
}

// parseQuery parses a full SELECT statement.
func (p *gqlParser) parseQuery() (*Query, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	if !p.accept(tokStar) {
		if _, err := p.expectIdent("a projection property or *"); err != nil {
			return nil, err
		}
		for p.accept(tokComma) {
			if _, err := p.expectIdent("a projection property"); err != nil {
				return nil, err
			}
		}
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	kind, err := p.expectIdent("kind")
	if err != nil {
		return nil, err
	}
	q := &Query{Kind: kind}

	if p.acceptKeyword("WHERE") {
		f, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		q.Filter = f
	}
	if p.acceptKeyword("ORDER") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		if err := p.parseOrderList(); err != nil {
			return nil, err
		}
	}
	// ORDER BY is parsed but not applied (the engine has no ordering model).
	// LIMIT/OFFSET may appear in either order.
	for {
		switch {
		case p.acceptKeyword("LIMIT"):
			n, err := p.expectNonNegInt()
			if err != nil {
				return nil, err
			}
			q.Limit = &n
		case p.acceptKeyword("OFFSET"):
			n, err := p.expectNonNegInt()
			if err != nil {
				return nil, err
			}
			q.Offset = n
		default:
			return q, nil
		}
	}
}

// parseOrderList consumes "prop [ASC|DESC], ..." and discards it.
func (p *gqlParser) parseOrderList() error {
	for {
		if _, err := p.expectIdent("an order property"); err != nil {
			return err
		}
		if p.atKeyword("ASC") || p.atKeyword("DESC") {
			p.next()
		}
		if !p.accept(tokComma) {
			return nil
		}
	}
}

func (p *gqlParser) parseExpr() (*Filter, error) { return p.parseOr() }

func (p *gqlParser) parseOr() (*Filter, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("OR") {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &Filter{Composite: &CompositeFilter{Op: CompositeOr, Filters: []*Filter{left, right}}}
	}
	return left, nil
}

func (p *gqlParser) parseAnd() (*Filter, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("AND") {
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		left = &Filter{Composite: &CompositeFilter{Op: CompositeAnd, Filters: []*Filter{left, right}}}
	}
	return left, nil
}

func (p *gqlParser) parsePrimary() (*Filter, error) {
	if p.accept(tokLParen) {
		f, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen, "')'"); err != nil {
			return nil, err
		}
		return f, nil
	}
	if p.atKeyword("NOT") {
		return nil, p.errorf("NOT is not supported")
	}
	return p.parseCondition()
}

func (p *gqlParser) parseCondition() (*Filter, error) {
	prop, err := p.expectIdent("a property")
	if err != nil {
		return nil, err
	}

	switch {
	case p.accept(tokEq):
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return propertyFilter(prop, PropertyEqual, v), nil
	case p.accept(tokNeq):
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return propertyFilter(prop, PropertyNotEqual, v), nil
	case p.accept(tokLt):
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return propertyFilter(prop, PropertyLessThan, v), nil
	case p.accept(tokLe):
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return propertyFilter(prop, PropertyLessThanOrEqual, v), nil
	case p.accept(tokGt):
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return propertyFilter(prop, PropertyGreaterThan, v), nil
	case p.accept(tokGe):
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		return propertyFilter(prop, PropertyGreaterThanOrEqual, v), nil
	case p.acceptKeyword("IN"):
		return p.parseInFilter(prop)
	case p.acceptKeyword("IS"):
		negate := p.acceptKeyword("NOT")
		if err := p.expectKeyword("NULL"); err != nil {
			return nil, err
		}
		if negate {
			return propertyFilter(prop, PropertyNotEqual, nullValue()), nil
		}
		return propertyFilter(prop, PropertyEqual, nullValue()), nil
	case p.atKeyword("HAS"):
		return p.parseHasFilter(prop)
	case p.atKeyword("CONTAINS"):
		return nil, p.errorf("CONTAINS is not supported")
	default:
		return nil, p.errorf("expected an operator after property %q, got %q", prop, p.peek().text)
	}
}

// parseHasFilter parses "HAS ANCESTOR <key>" (the only supported HAS form),
// producing the special __key__ HAS_ANCESTOR operator. HAS DESCENDANT remains
// unsupported.
func (p *gqlParser) parseHasFilter(prop string) (*Filter, error) {
	p.next() // HAS
	if p.atKeyword("DESCENDANT") {
		return nil, p.errorf("HAS DESCENDANT is not supported")
	}
	if err := p.expectKeyword("ANCESTOR"); err != nil {
		return nil, err
	}
	v, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	return propertyFilter(prop, PropertyHasAncestor, v), nil
}

// parseInFilter parses "IN @binding" (an array value) or "IN (v1, v2, ...)".
func (p *gqlParser) parseInFilter(prop string) (*Filter, error) {
	if p.peek().kind == tokBinding {
		t := p.next()
		v, err := p.resolveBinding(t)
		if err != nil {
			return nil, err
		}
		if v.ArrayValue == nil {
			return nil, p.errorf("IN binding @%s must be an array", t.text)
		}
		return propertyFilter(prop, PropertyIn, v), nil
	}
	if _, err := p.expect(tokLParen, "'('"); err != nil {
		return nil, err
	}
	v, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	vals := []dsstore.Value{v}
	for p.accept(tokComma) {
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	if _, err := p.expect(tokRParen, "')'"); err != nil {
		return nil, err
	}
	return propertyFilter(prop, PropertyIn, dsstore.Value{ArrayValue: &dsstore.ArrayValue{Values: vals}}), nil
}

// parseValue parses a value position: a binding or an (allow-literals-gated)
// literal.
func (p *gqlParser) parseValue() (dsstore.Value, error) {
	t := p.peek()
	switch t.kind {
	case tokBinding:
		p.next()
		return p.resolveBinding(t)
	case tokString:
		p.next()
		if err := p.requireLiterals(); err != nil {
			return dsstore.Value{}, err
		}
		s := t.text
		return dsstore.Value{StringValue: &s}, nil
	case tokInt:
		p.next()
		if err := p.requireLiterals(); err != nil {
			return dsstore.Value{}, err
		}
		n, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return dsstore.Value{}, p.errorf("invalid integer %q", t.text)
		}
		return dsstore.Value{IntegerValue: &n}, nil
	case tokFloat:
		p.next()
		if err := p.requireLiterals(); err != nil {
			return dsstore.Value{}, err
		}
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return dsstore.Value{}, p.errorf("invalid number %q", t.text)
		}
		return dsstore.Value{DoubleValue: &f}, nil
	case tokIdent:
		switch {
		case strings.EqualFold(t.text, "TRUE"), strings.EqualFold(t.text, "FALSE"):
			p.next()
			if err := p.requireLiterals(); err != nil {
				return dsstore.Value{}, err
			}
			b := strings.EqualFold(t.text, "TRUE")
			return dsstore.Value{BooleanValue: &b}, nil
		case strings.EqualFold(t.text, "NULL"):
			p.next()
			if err := p.requireLiterals(); err != nil {
				return dsstore.Value{}, err
			}
			return nullValue(), nil
		case strings.EqualFold(t.text, "DATETIME"):
			return p.parseDatetimeLiteral()
		case strings.EqualFold(t.text, "KEY"):
			return p.parseKeyLiteral()
		case strings.EqualFold(t.text, "BLOB"):
			return p.parseBlobLiteral()
		}
	}
	return dsstore.Value{}, p.errorf("expected a value, got %q", t.text)
}

func (p *gqlParser) requireLiterals() error {
	if !p.gql.AllowLiterals {
		return p.errorf("literals are not allowed (allow_literals is false); bind all values")
	}
	return nil
}

func (p *gqlParser) parseDatetimeLiteral() (dsstore.Value, error) {
	if err := p.requireLiterals(); err != nil {
		return dsstore.Value{}, err
	}
	p.next() // DATETIME
	if _, err := p.expect(tokLParen, "'('"); err != nil {
		return dsstore.Value{}, err
	}
	t, err := p.expect(tokString, "a datetime string")
	if err != nil {
		return dsstore.Value{}, err
	}
	if _, err := p.expect(tokRParen, "')'"); err != nil {
		return dsstore.Value{}, err
	}
	ts, err := parseGQLDatetime(t.text)
	if err != nil {
		return dsstore.Value{}, p.errorf("invalid DATETIME %q: %v", t.text, err)
	}
	return dsstore.Value{TimestampValue: &ts}, nil
}

// parseGQLDatetime accepts the timestamp forms GQL DATETIME literals use and
// normalizes them to RFC 3339 (the store's canonical timestamp form).
func parseGQLDatetime(s string) (string, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().Format(time.RFC3339Nano), nil
		}
	}
	return "", fmt.Errorf("expected an RFC 3339 timestamp")
}

func (p *gqlParser) parseKeyLiteral() (dsstore.Value, error) {
	if err := p.requireLiterals(); err != nil {
		return dsstore.Value{}, err
	}
	p.next() // KEY
	if _, err := p.expect(tokLParen, "'('"); err != nil {
		return dsstore.Value{}, err
	}
	kind, err := p.expect(tokString, "a key kind")
	if err != nil {
		return dsstore.Value{}, err
	}
	if _, err := p.expect(tokComma, "','"); err != nil {
		return dsstore.Value{}, err
	}
	idTok := p.peek()
	var key string
	switch idTok.kind {
	case tokInt:
		p.next()
		id, err := strconv.ParseInt(idTok.text, 10, 64)
		if err != nil {
			return dsstore.Value{}, p.errorf("invalid key id %q", idTok.text)
		}
		key = dsstore.KeyOfID(kind.text, id)
	case tokString:
		p.next()
		key = dsstore.KeyOfName(kind.text, idTok.text)
	default:
		return dsstore.Value{}, p.errorf("KEY id must be an integer or a string name, got %q", idTok.text)
	}
	if _, err := p.expect(tokRParen, "')'"); err != nil {
		return dsstore.Value{}, err
	}
	return dsstore.Value{KeyValue: &key}, nil
}

func (p *gqlParser) parseBlobLiteral() (dsstore.Value, error) {
	if err := p.requireLiterals(); err != nil {
		return dsstore.Value{}, err
	}
	p.next() // BLOB
	if _, err := p.expect(tokLParen, "'('"); err != nil {
		return dsstore.Value{}, err
	}
	t, err := p.expect(tokString, "a base64 blob")
	if err != nil {
		return dsstore.Value{}, err
	}
	if _, err := p.expect(tokRParen, "')'"); err != nil {
		return dsstore.Value{}, err
	}
	raw, err := base64.StdEncoding.DecodeString(t.text)
	if err != nil {
		return dsstore.Value{}, p.errorf("invalid BLOB base64: %v", err)
	}
	if raw == nil {
		raw = []byte{}
	}
	return dsstore.Value{BlobValue: raw}, nil
}

// parseAggregation parses "AGGREGATE <agg>[, <agg>] OVER (<select>)".
func (p *gqlParser) parseAggregation() (*AggregationQuery, error) {
	if err := p.expectKeyword("AGGREGATE"); err != nil {
		return nil, err
	}
	aggs := make([]Aggregation, 0, 1)
	for {
		agg, err := p.parseAgg()
		if err != nil {
			return nil, err
		}
		aggs = append(aggs, agg)
		if !p.accept(tokComma) {
			break
		}
	}
	if err := p.expectKeyword("OVER"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tokLParen, "'('"); err != nil {
		return nil, err
	}
	nested, err := p.parseQuery()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRParen, "')'"); err != nil {
		return nil, err
	}
	return &AggregationQuery{Nested: *nested, Aggregations: aggs}, nil
}

func (p *gqlParser) parseAgg() (Aggregation, error) {
	if p.peek().kind != tokIdent {
		return Aggregation{}, p.errorf("expected an aggregation function, got %q", p.peek().text)
	}
	fn := strings.ToUpper(p.next().text)
	if _, err := p.expect(tokLParen, "'('"); err != nil {
		return Aggregation{}, err
	}
	var agg Aggregation
	switch fn {
	case "COUNT":
		if !p.accept(tokStar) {
			return Aggregation{}, p.errorf("COUNT requires '*'")
		}
		agg = Aggregation{Op: AggCount}
	case "COUNT_UP_TO":
		n, err := p.expectNonNegInt()
		if err != nil {
			return Aggregation{}, err
		}
		up := int64(n)
		agg = Aggregation{Op: AggCount, UpTo: &up}
	case "SUM":
		prop, err := p.expectIdent("a sum property")
		if err != nil {
			return Aggregation{}, err
		}
		agg = Aggregation{Op: AggSum, Property: prop}
	case "AVG":
		prop, err := p.expectIdent("an avg property")
		if err != nil {
			return Aggregation{}, err
		}
		agg = Aggregation{Op: AggAvg, Property: prop}
	default:
		return Aggregation{}, p.errorf("unsupported aggregation function %q", fn)
	}
	if _, err := p.expect(tokRParen, "')'"); err != nil {
		return Aggregation{}, err
	}
	if p.acceptKeyword("AS") {
		alias, err := p.expectIdent("an aggregation alias")
		if err != nil {
			return Aggregation{}, err
		}
		agg.Alias = alias
	}
	return agg, nil
}

// ─── bindings ─────────────────────────────────────────────────────────────────

func (p *gqlParser) resolveBinding(t gqlToken) (dsstore.Value, error) {
	if isAllDigits(t.text) {
		idx, err := strconv.Atoi(t.text)
		if err != nil || idx < 1 || idx > len(p.gql.PositionalBindings) {
			return dsstore.Value{}, p.errorf("positional binding @%s is out of range", t.text)
		}
		return p.bindingValue(p.gql.PositionalBindings[idx-1], "@"+t.text)
	}
	b, ok := p.gql.NamedBindings[t.text]
	if !ok {
		return dsstore.Value{}, p.errorf("named binding @%s is not provided", t.text)
	}
	return p.bindingValue(b, "@"+t.text)
}

func (p *gqlParser) bindingValue(b GQLBinding, label string) (dsstore.Value, error) {
	if b.Cursor != nil {
		return dsstore.Value{}, p.errorf("cursor binding %s is not supported", label)
	}
	if b.Value == nil {
		return dsstore.Value{}, p.errorf("binding %s has no value", label)
	}
	return *b.Value, nil
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

func propertyFilter(name string, op PropertyOp, v dsstore.Value) *Filter {
	return &Filter{Property: &PropertyFilter{Property: name, Op: op, Value: v}}
}

func nullValue() dsstore.Value {
	s := "NULL_VALUE"
	return dsstore.Value{NullValue: &s}
}

// ─── core entry points ────────────────────────────────────────────────────────

// RunQueryGQL parses a GQL SELECT and runs it through the same engine as
// RunQuery (in the request's partition), so GQL and structured queries share
// one implementation.
func (s *Service) RunQueryGQL(ctx context.Context, project string, q GQLQuery, txn []byte, namespace, database string) (*QueryResult, error) {
	parsed, err := ParseGQL(q)
	if err != nil {
		return nil, err
	}
	parsed.Namespace, parsed.Database = namespace, database
	return s.RunQuery(ctx, project, parsed, txn)
}

// RunAggregationQueryGQL parses a GQL aggregation query and runs it through the
// same engine as RunAggregationQuery (in the request's partition).
func (s *Service) RunAggregationQueryGQL(ctx context.Context, project string, q GQLQuery, txn []byte, namespace, database string) (*AggregationResult, error) {
	parsed, err := ParseGQLAggregation(q)
	if err != nil {
		return nil, err
	}
	parsed.Nested.Namespace, parsed.Nested.Database = namespace, database
	return s.RunAggregationQuery(ctx, project, parsed, txn)
}
