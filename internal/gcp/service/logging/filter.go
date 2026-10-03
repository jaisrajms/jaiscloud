package logging

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	loggingstore "jaiscloud/internal/gcp/store/logging"
)

// severityNames maps the google.logging.type.LogSeverity name to its numeric
// value (the wire enum numbers: DEFAULT=0, DEBUG=100, ..., EMERGENCY=800).
var severityNames = map[string]int{
	"DEFAULT":   0,
	"DEBUG":     100,
	"INFO":      200,
	"NOTICE":    300,
	"WARNING":   400,
	"ERROR":     500,
	"CRITICAL":  600,
	"ALERT":     700,
	"EMERGENCY": 800,
}

// SeverityName returns the google.logging.type.LogSeverity enum name for a
// numeric severity value, or "DEFAULT" for an unknown/zero value. The REST
// transport uses it to render the severity enum as protojson does.
func SeverityName(v int) string {
	for n, nv := range severityNames {
		if nv == v {
			return n
		}
	}
	return "DEFAULT"
}

// SeverityValue resolves a severity operand to its numeric value. It accepts a
// severity name (WARNING) or a bare integer.
func SeverityValue(s string) (int, bool) {
	if v, ok := severityNames[strings.ToUpper(s)]; ok {
		return v, true
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n, true
	}
	return 0, false
}

// Predicate matches a stored log entry against a compiled filter.
type Predicate interface {
	Match(e loggingstore.LogEntry) bool
}

type matchAll struct{}

func (matchAll) Match(loggingstore.LogEntry) bool { return true }

type andExpr struct{ l, r Predicate }

func (a *andExpr) Match(e loggingstore.LogEntry) bool { return a.l.Match(e) && a.r.Match(e) }

type orExpr struct{ l, r Predicate }

func (o *orExpr) Match(e loggingstore.LogEntry) bool { return o.l.Match(e) || o.r.Match(e) }

type notExpr struct{ inner Predicate }

func (n *notExpr) Match(e loggingstore.LogEntry) bool { return !n.inner.Match(e) }

// cmpExpr is a comparison between a supported field and a literal value. The
// value is kept as a raw token (string/number/ident) and coerced per-field at
// match time, mirroring the advanced-log filter's type inference.
type cmpExpr struct {
	field     string
	op        string
	value     string
	valueKind string // "string", "number", "ident"
}

func (c *cmpExpr) Match(e loggingstore.LogEntry) bool {
	switch c.field {
	case "logName", "log_name":
		switch c.op {
		case "=":
			return e.LogName == c.value
		case "!=":
			return e.LogName != c.value
		}
		return false
	case "severity":
		rhs, ok := SeverityValue(c.value)
		if !ok {
			return false
		}
		return cmpInt(int64(e.Severity), int64(rhs), c.op)
	case "timestamp":
		rhs, err := time.Parse(time.RFC3339Nano, c.value)
		if err != nil {
			return false
		}
		return cmpTime(e.Timestamp, rhs, c.op)
	case "resource.type", "resource_type":
		switch c.op {
		case "=":
			return e.ResourceType == c.value
		case "!=":
			return e.ResourceType != c.value
		}
		return false
	case "textPayload", "text_payload":
		switch c.op {
		case ":":
			return strings.Contains(e.TextPayload, c.value)
		case "=":
			return e.TextPayload == c.value
		case "!=":
			return e.TextPayload != c.value
		}
		return false
	}
	return false
}

func cmpInt(a, b int64, op string) bool {
	switch op {
	case "=":
		return a == b
	case "!=":
		return a != b
	case ">":
		return a > b
	case ">=":
		return a >= b
	case "<":
		return a < b
	case "<=":
		return a <= b
	}
	return false
}

func cmpTime(a, b time.Time, op string) bool {
	switch op {
	case "=":
		return a.Equal(b)
	case "!=":
		return !a.Equal(b)
	case ">":
		return a.After(b)
	case ">=":
		return !a.Before(b)
	case "<":
		return a.Before(b)
	case "<=":
		return !a.After(b)
	}
	return false
}

// --- tokenizer ---

type filterToken struct {
	kind string // "ident", "string", "number", "op", "(", ")", "eof"
	text string
}

func isDelimiter(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '(', ')', '=', '>', '<', '!', ':', '"', '\'':
		return true
	}
	return false
}

func isNumber(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func tokenize(s string) ([]filterToken, error) {
	var toks []filterToken
	i := 0
	n := len(s)
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, filterToken{kind: "(", text: "("})
			i++
		case c == ')':
			toks = append(toks, filterToken{kind: ")", text: ")"})
			i++
		case c == '"' || c == '\'':
			quote := c
			j := i + 1
			var sb strings.Builder
			closed := false
			for j < n {
				if s[j] == '\\' && j+1 < n {
					sb.WriteByte(s[j+1])
					j += 2
					continue
				}
				if s[j] == quote {
					closed = true
					j++
					break
				}
				sb.WriteByte(s[j])
				j++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated string literal in filter")
			}
			toks = append(toks, filterToken{kind: "string", text: sb.String()})
			i = j
		case c == '=' || c == '>' || c == '<' || c == '!' || c == ':':
			if c != ':' && i+1 < n && (s[i:i+2] == ">=" || s[i:i+2] == "<=" || s[i:i+2] == "!=") {
				toks = append(toks, filterToken{kind: "op", text: s[i : i+2]})
				i += 2
				continue
			}
			if c == '=' || c == '>' || c == '<' || c == ':' {
				toks = append(toks, filterToken{kind: "op", text: string(c)})
				i++
				continue
			}
			return nil, fmt.Errorf("unsupported operator %q", string(c))
		default:
			j := i
			for j < n && !isDelimiter(s[j]) {
				j++
			}
			word := s[i:j]
			if word == "" {
				return nil, fmt.Errorf("unexpected character %q", string(c))
			}
			if isNumber(word) {
				toks = append(toks, filterToken{kind: "number", text: word})
			} else {
				toks = append(toks, filterToken{kind: "ident", text: word})
			}
			i = j
		}
	}
	toks = append(toks, filterToken{kind: "eof"})
	return toks, nil
}

// --- parser ---

type filterParser struct {
	toks []filterToken
	pos  int
}

func (p *filterParser) peek() filterToken { return p.toks[p.pos] }

func (p *filterParser) next() filterToken {
	t := p.toks[p.pos]
	p.pos++
	return t
}

func (p *filterParser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == "ident" && strings.EqualFold(t.text, kw)
}

func (p *filterParser) parse() (Predicate, error) {
	e, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != "eof" {
		return nil, fmt.Errorf("unexpected token %q", p.peek().text)
	}
	return e, nil
}

func (p *filterParser) parseAnd() (Predicate, error) {
	l, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("AND") {
		p.next()
		r, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		l = &andExpr{l, r}
	}
	return l, nil
}

func (p *filterParser) parseOr() (Predicate, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("OR") {
		p.next()
		r, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		l = &orExpr{l, r}
	}
	return l, nil
}

func (p *filterParser) parseUnary() (Predicate, error) {
	if p.isKeyword("NOT") {
		p.next()
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &notExpr{inner: inner}, nil
	}
	return p.parsePrimary()
}

func (p *filterParser) parsePrimary() (Predicate, error) {
	if p.peek().kind == "(" {
		p.next()
		e, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		if p.next().kind != ")" {
			return nil, fmt.Errorf("missing closing parenthesis in filter")
		}
		return e, nil
	}
	return p.parseComparison()
}

// supportedFilterField reports whether the filter field is one this subset
// understands (logName, severity, timestamp, resource.type, textPayload).
// Anything else is rejected as InvalidArgument rather than silently matching
// nothing.
func supportedFilterField(field string) bool {
	switch field {
	case "logName", "log_name", "severity", "timestamp", "resource.type", "resource_type", "textPayload", "text_payload":
		return true
	}
	return false
}

// supportedFilterOp reports whether op is implemented for field by
// cmpExpr.Match. The tokenizer accepts >, <, >=, <=, and : for any field, but
// only a subset of (field, op) combinations are actually evaluated — without
// this check, e.g. "logName>\"x\"" or "severity:5" would compile successfully
// and then silently match zero entries at evaluation time instead of being
// rejected as InvalidArgument.
func supportedFilterOp(field, op string) bool {
	switch field {
	case "logName", "log_name", "resource.type", "resource_type":
		switch op {
		case "=", "!=":
			return true
		}
	case "severity", "timestamp":
		switch op {
		case "=", "!=", ">", ">=", "<", "<=":
			return true
		}
	case "textPayload", "text_payload":
		switch op {
		case ":", "=", "!=":
			return true
		}
	}
	return false
}

func (p *filterParser) parseComparison() (Predicate, error) {
	field := p.next()
	if field.kind != "ident" {
		return nil, fmt.Errorf("expected field name, got %q", field.text)
	}
	if !supportedFilterField(field.text) {
		return nil, fmt.Errorf("unsupported filter field %q", field.text)
	}
	op := p.next()
	if op.kind != "op" {
		return nil, fmt.Errorf("expected comparison operator after %q", field.text)
	}
	if !supportedFilterOp(field.text, op.text) {
		return nil, fmt.Errorf("unsupported operator %q for filter field %q", op.text, field.text)
	}
	value := p.next()
	switch value.kind {
	case "string", "number", "ident":
	default:
		return nil, fmt.Errorf("expected value after operator %q", op.text)
	}
	return &cmpExpr{field: field.text, op: op.text, value: value.text, valueKind: value.kind}, nil
}

// CompileFilter parses a ListLogEntriesRequest filter into a predicate. An
// empty/blank filter matches everything. Unsupported or malformed constructs
// return an error (faithful InvalidArgument), never a silent match-all.
func CompileFilter(filter string) (Predicate, error) {
	if strings.TrimSpace(filter) == "" {
		return matchAll{}, nil
	}
	toks, err := tokenize(filter)
	if err != nil {
		return nil, err
	}
	return (&filterParser{toks: toks}).parse()
}
