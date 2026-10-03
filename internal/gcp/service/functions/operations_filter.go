package functions

import (
	"fmt"
	"strconv"
	"strings"
)

// operationFilterFields is the set of filterable
// google.longrunning.Operations fields the emulator evaluates. The real
// Operations.List `filter` is AIP-160; only the fields a persisted function
// operation exposes are supported (its done state and its resource name).
// metadata.*/response.* are deliberately not accepted: they are not first-class
// columns of the persisted record, and silently ignoring an unsupported field
// would return a wrong (unfiltered) page.
var operationFilterFields = map[string]bool{
	"done": true,
	"name": true,
}

// operationPredicate is a compiled filter expression evaluated against an
// operation. The version and project needed to render the operation's resource
// name are captured at compile time.
type operationPredicate interface {
	match(op Operation) bool
}

// matchAllOperations matches every operation (the empty filter).
type matchAllOperations struct{}

func (matchAllOperations) match(Operation) bool { return true }

type andOperation struct{ l, r operationPredicate }

func (p andOperation) match(op Operation) bool { return p.l.match(op) && p.r.match(op) }

type orOperation struct{ l, r operationPredicate }

func (p orOperation) match(op Operation) bool { return p.l.match(op) || p.r.match(op) }

type notOperation struct{ inner operationPredicate }

func (p notOperation) match(op Operation) bool { return !p.inner.match(op) }

// cmpOperation is one `field OP value` predicate. `done` is a bool rendered as
// "true"/"false"; `name` is the version-appropriate operation resource name.
type cmpOperation struct {
	v       Version
	project string
	field   string
	op      string
	value   string
}

func (p cmpOperation) match(op Operation) bool {
	var actual string
	switch p.field {
	case "done":
		actual = strconv.FormatBool(op.Done)
	case "name":
		actual = OperationName(p.v, p.project, op)
	default:
		return false
	}
	switch p.op {
	case "=":
		return actual == p.value
	case "!=":
		return actual != p.value
	case ":":
		return strings.Contains(actual, p.value)
	}
	return false
}

// compileOperationFilter parses an AIP-160 expression over the operation
// fields:
//
//	expr       := and
//	and        := or ( "AND" or )*
//	or         := unary ( "OR" unary )*
//	unary      := ("NOT" | "-") unary | primary
//	primary    := "(" expr ")" | comparison
//	comparison := field ( "=" | "!=" | ":" ) value
//
// Per AIP-160, OR binds tighter than AND (`a AND b OR c` is `a AND (b OR c)`),
// matching the runtimes and logging evaluators. `done` accepts only the boolean
// literals true/false and the `=`/`!=` operators; `name` accepts `=`/`!=`/`:`
// against the rendered resource name. An empty filter matches everything.
// Anything outside this grammar (an unknown field, an AIP-160 function call, a
// dangling operator, ...) is InvalidArgument — a filter the emulator cannot
// evaluate is rejected rather than silently returning a wrong page.
func compileOperationFilter(filter string, v Version, project string) (operationPredicate, error) {
	if strings.TrimSpace(filter) == "" {
		return matchAllOperations{}, nil
	}
	toks, err := tokenizeOperationFilter(filter)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	p := &operationFilterParser{toks: toks, v: v, project: project}
	pred, err := p.parseAnd()
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	if p.peek().kind != opTokEOF {
		return nil, invalidArgument("unsupported operations filter " + filter)
	}
	return pred, nil
}

// ─── tokenizer ────────────────────────────────────────────────────────────────

type operationFilterTokenKind int

const (
	opTokEOF operationFilterTokenKind = iota
	opTokIdent
	opTokString
	opTokOp
	opTokLParen
	opTokRParen
)

type operationFilterToken struct {
	kind operationFilterTokenKind
	text string
}

// tokenizeOperationFilter splits an AIP-160 expression into identifiers,
// quoted strings, comparison operators, and parentheses. An unterminated string
// or an unexpected character is an error.
func tokenizeOperationFilter(s string) ([]operationFilterToken, error) {
	var toks []operationFilterToken
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, operationFilterToken{kind: opTokLParen, text: "("})
			i++
		case c == ')':
			toks = append(toks, operationFilterToken{kind: opTokRParen, text: ")"})
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
				return nil, fmt.Errorf("unterminated string literal in operations filter")
			}
			toks = append(toks, operationFilterToken{kind: opTokString, text: b.String()})
			i = j
		case c == '-':
			// AIP-160 negation: `-a` is equivalent to `NOT a`. (A `-` inside an
			// identifier such as `us-central1` is consumed by the ident scanner.)
			toks = append(toks, operationFilterToken{kind: opTokOp, text: "-"})
			i++
		case c == '=' || c == '!' || c == '<' || c == '>' || c == ':':
			if i+1 < len(s) {
				switch s[i : i+2] {
				case "!=", "<=", ">=":
					toks = append(toks, operationFilterToken{kind: opTokOp, text: s[i : i+2]})
					i += 2
					continue
				}
			}
			if c == '!' {
				return nil, fmt.Errorf("unexpected character %q in operations filter", string(c))
			}
			toks = append(toks, operationFilterToken{kind: opTokOp, text: string(c)})
			i++
		case isOperationIdentStart(c):
			j := i + 1
			for j < len(s) && isOperationIdentPart(s[j]) {
				j++
			}
			toks = append(toks, operationFilterToken{kind: opTokIdent, text: s[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q in operations filter", string(c))
		}
	}
	toks = append(toks, operationFilterToken{kind: opTokEOF})
	return toks, nil
}

func isOperationIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isOperationIdentPart(c byte) bool {
	return isOperationIdentStart(c) || (c >= '0' && c <= '9') || c == '-' || c == '.'
}

// ─── parser ───────────────────────────────────────────────────────────────────

type operationFilterParser struct {
	toks    []operationFilterToken
	pos     int
	v       Version
	project string
}

func (p *operationFilterParser) peek() operationFilterToken { return p.toks[p.pos] }

func (p *operationFilterParser) next() operationFilterToken {
	t := p.toks[p.pos]
	if t.kind != opTokEOF {
		p.pos++
	}
	return t
}

func (p *operationFilterParser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == opTokIdent && strings.EqualFold(t.text, kw)
}

func (p *operationFilterParser) parseAnd() (operationPredicate, error) {
	l, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("and") {
		p.next()
		r, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		l = andOperation{l, r}
	}
	return l, nil
}

func (p *operationFilterParser) parseOr() (operationPredicate, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("or") {
		p.next()
		r, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		l = orOperation{l, r}
	}
	return l, nil
}

func (p *operationFilterParser) parseUnary() (operationPredicate, error) {
	if p.isKeyword("not") || (p.peek().kind == opTokOp && p.peek().text == "-") {
		p.next()
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return notOperation{inner}, nil
	}
	return p.parsePrimary()
}

func (p *operationFilterParser) parsePrimary() (operationPredicate, error) {
	if p.peek().kind == opTokLParen {
		p.next()
		inner, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		if p.next().kind != opTokRParen {
			return nil, fmt.Errorf("missing closing parenthesis in operations filter")
		}
		return inner, nil
	}
	return p.parseComparison()
}

func (p *operationFilterParser) parseComparison() (operationPredicate, error) {
	field := p.next()
	if field.kind != opTokIdent {
		return nil, fmt.Errorf("expected operations filter field, got %q", field.text)
	}
	if !operationFilterFields[field.text] {
		return nil, fmt.Errorf("unsupported operations filter field %q", field.text)
	}
	op := p.next()
	if op.kind != opTokOp {
		return nil, fmt.Errorf("expected comparison operator after %q", field.text)
	}
	value := p.next()
	if value.kind != opTokString && value.kind != opTokIdent {
		return nil, fmt.Errorf("expected value after %q", op.text)
	}
	if field.text == "done" {
		if !strings.EqualFold(value.text, "true") && !strings.EqualFold(value.text, "false") {
			return nil, fmt.Errorf("done filter value must be true or false, got %q", value.text)
		}
		if op.text != "=" && op.text != "!=" {
			return nil, fmt.Errorf("unsupported operator %q for done", op.text)
		}
		value.text = strings.ToLower(value.text)
	} else if op.text != "=" && op.text != "!=" && op.text != ":" {
		return nil, fmt.Errorf("unsupported operator %q for %s", op.text, field.text)
	}
	return cmpOperation{v: p.v, project: p.project, field: field.text, op: op.text, value: value.text}, nil
}
