package expr

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// toJSON renders v as compact JSON (used for stringification of non-strings).
func toJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

// EvalString parses and evaluates a single expression (the content of ${...}).
func EvalString(s string, vars map[string]any, env Env) (any, error) {
	p := &parser{toks: lex(s)}
	node, err := p.parse()
	if err != nil {
		return nil, err
	}
	if p.cur().kind != tokEOF {
		return nil, parseErrorf("unexpected %q", p.cur().text)
	}
	return eval(node, vars, env)
}

// --- lexer ---

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokInt
	tokFloat
	tokString
	tokOp
)

type token struct {
	kind tokKind
	text string
}

func lex(s string) []token {
	var toks []token
	i := 0
	n := len(s)
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '\'' || c == '"':
			quote := c
			j := i + 1
			var b strings.Builder
			for j < n && s[j] != quote {
				if s[j] == '\\' && j+1 < n {
					j++
				}
				b.WriteByte(s[j])
				j++
			}
			if j >= n {
				toks = append(toks, token{tokString, b.String()})
				i = j
				continue
			}
			toks = append(toks, token{tokString, b.String()})
			i = j + 1
		case isDigit(c):
			j := i
			hasDot := false
			for j < n && (isDigit(s[j]) || s[j] == '.') {
				if s[j] == '.' {
					hasDot = true
				}
				j++
			}
			if hasDot {
				toks = append(toks, token{tokFloat, s[i:j]})
			} else {
				toks = append(toks, token{tokInt, s[i:j]})
			}
			i = j
		case isIdentStart(c):
			j := i
			for j < n && isIdentPart(s[j]) {
				j++
			}
			toks = append(toks, token{tokIdent, s[i:j]})
			i = j
		default:
			// two-char operators first
			two := ""
			if i+1 < n {
				two = s[i : i+2]
			}
			switch two {
			case "==", "!=", "<=", ">=", "&&", "||", "//":
				toks = append(toks, token{tokOp, two})
				i += 2
				continue
			}
			if strings.ContainsRune("+-*/%<>()[]{},:.!", rune(c)) {
				toks = append(toks, token{tokOp, string(c)})
				i++
				continue
			}
			toks = append(toks, token{tokOp, string(c)})
			i++
		}
	}
	toks = append(toks, token{kind: tokEOF})
	return toks
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
func isIdentPart(c byte) bool { return isIdentStart(c) || isDigit(c) }

// --- AST ---

type node interface{ node() }

type litNode struct{ v any }
type identNode struct{ name string }
type listNode struct{ items []node }
type mapNode struct {
	keys []string
	vals []node
}
type binaryNode struct {
	op   string
	l, r node
}
type unaryNode struct {
	op string
	x  node
}
type memberNode struct {
	obj node
	key string
}
type indexNode struct {
	obj node
	idx node
}
type callNode struct {
	fn   string
	args []node
}

func (litNode) node()    {}
func (identNode) node()  {}
func (listNode) node()   {}
func (mapNode) node()    {}
func (binaryNode) node() {}
func (unaryNode) node()  {}
func (memberNode) node() {}
func (indexNode) node()  {}
func (callNode) node()   {}

// --- parser (recursive descent, precedence climbing) ---

type parser struct {
	toks []token
	pos  int
}

func (p *parser) cur() token { return p.toks[p.pos] }
func (p *parser) next() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}
func (p *parser) peek() token {
	if p.pos+1 < len(p.toks) {
		return p.toks[p.pos+1]
	}
	return p.toks[len(p.toks)-1]
}

func (p *parser) parse() (node, error) { return p.parseOr() }

func (p *parser) parseOr() (node, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur().kind == tokIdent && (p.cur().text == "or" || p.cur().text == "||") {
		p.next()
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l = binaryNode{op: "or", l: l, r: r}
	}
	return l, nil
}

func (p *parser) parseAnd() (node, error) {
	l, err := p.parseComparison()
	if err != nil {
		return nil, err
	}
	for p.cur().kind == tokIdent && (p.cur().text == "and" || p.cur().text == "&&") {
		p.next()
		r, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		l = binaryNode{op: "and", l: l, r: r}
	}
	return l, nil
}

// parseNot handles the boolean NOT operator. Per the Workflows expressions
// reference it binds tighter than unary minus and comparison: `not a == b`
// parses as `(not a) == b`, and `not -x` requires parentheses (`not (-x)`).
func (p *parser) parseNot() (node, error) {
	if p.cur().kind == tokIdent && p.cur().text == "not" {
		p.next()
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return unaryNode{op: "not", x: x}, nil
	}
	if p.cur().kind == tokOp && p.cur().text == "!" {
		p.next()
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return unaryNode{op: "not", x: x}, nil
	}
	return p.parsePostfix()
}

func (p *parser) parseComparison() (node, error) {
	l, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	for {
		op := ""
		if p.cur().kind == tokOp && isCompOp(p.cur().text) {
			op = p.cur().text
		} else if p.cur().kind == tokIdent && p.cur().text == "in" {
			op = "in"
		} else {
			break
		}
		p.next()
		r, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		l = binaryNode{op: op, l: l, r: r}
	}
	return l, nil
}

func isCompOp(s string) bool {
	switch s {
	case "==", "!=", "<", ">", "<=", ">=":
		return true
	}
	return false
}

func (p *parser) parseAdditive() (node, error) {
	l, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}
	for p.cur().kind == tokOp && (p.cur().text == "+" || p.cur().text == "-") {
		op := p.cur().text
		p.next()
		r, err := p.parseMultiplicative()
		if err != nil {
			return nil, err
		}
		l = binaryNode{op: op, l: l, r: r}
	}
	return l, nil
}

func (p *parser) parseMultiplicative() (node, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.cur().kind == tokOp && (p.cur().text == "*" || p.cur().text == "/" || p.cur().text == "//" || p.cur().text == "%") {
		op := p.cur().text
		p.next()
		r, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		l = binaryNode{op: op, l: l, r: r}
	}
	return l, nil
}

func (p *parser) parseUnary() (node, error) {
	if p.cur().kind == tokOp && (p.cur().text == "-" || p.cur().text == "+") {
		op := p.cur().text
		p.next()
		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		if op == "-" {
			return unaryNode{op: "-", x: x}, nil
		}
		return x, nil
	}
	return p.parseNot()
}

func (p *parser) parsePostfix() (node, error) {
	n, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for {
		switch {
		case p.cur().kind == tokOp && p.cur().text == ".":
			p.next()
			if p.cur().kind != tokIdent {
				return nil, parseErrorf("expected field name after '.'")
			}
			key := p.next().text
			n = memberNode{obj: n, key: key}
		case p.cur().kind == tokOp && p.cur().text == "[":
			p.next()
			idx, err := p.parseOr()
			if err != nil {
				return nil, err
			}
			if p.cur().kind != tokOp || p.cur().text != "]" {
				return nil, parseErrorf("expected ']'")
			}
			p.next()
			n = indexNode{obj: n, idx: idx}
		default:
			return n, nil
		}
	}
}

func (p *parser) parsePrimary() (node, error) {
	t := p.cur()
	switch t.kind {
	case tokInt:
		p.next()
		v, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return nil, parseErrorf("invalid integer %q", t.text)
		}
		return litNode{v: v}, nil
	case tokFloat:
		p.next()
		v, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, parseErrorf("invalid number %q", t.text)
		}
		return litNode{v: v}, nil
	case tokString:
		p.next()
		return litNode{v: t.text}, nil
	case tokIdent:
		switch t.text {
		case "true":
			p.next()
			return litNode{v: true}, nil
		case "false":
			p.next()
			return litNode{v: false}, nil
		case "null":
			p.next()
			return litNode{v: nil}, nil
		}
		// A dotted path rooted at "sys" is a stdlib function call.
		if t.text == "sys" {
			return p.parseSysCall()
		}
		p.next()
		return identNode{name: t.text}, nil
	case tokOp:
		if t.text == "(" {
			p.next()
			n, err := p.parseOr()
			if err != nil {
				return nil, err
			}
			if p.cur().kind != tokOp || p.cur().text != ")" {
				return nil, parseErrorf("expected ')'")
			}
			p.next()
			return n, nil
		}
		if t.text == "[" {
			return p.parseList()
		}
		if t.text == "{" {
			return p.parseMap()
		}
	}
	return nil, parseErrorf("unexpected token %q", t.text)
}

func (p *parser) parseSysCall() (node, error) {
	fn := "sys"
	p.next() // consume "sys"
	for p.cur().kind == tokOp && p.cur().text == "." && p.peek().kind == tokIdent {
		p.next() // '.'
		fn += "." + p.next().text
	}
	if p.cur().kind != tokOp || p.cur().text != "(" {
		return nil, parseErrorf("%s must be called with parentheses", fn)
	}
	p.next() // '('
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}
	return callNode{fn: fn, args: args}, nil
}

func (p *parser) parseArgs() ([]node, error) {
	var args []node
	if p.cur().kind == tokOp && p.cur().text == ")" {
		p.next()
		return args, nil
	}
	for {
		a, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		args = append(args, a)
		if p.cur().kind == tokOp && p.cur().text == "," {
			p.next()
			continue
		}
		if p.cur().kind == tokOp && p.cur().text == ")" {
			p.next()
			return args, nil
		}
		return nil, parseErrorf("expected ',' or ')' in argument list")
	}
}

func (p *parser) parseList() (node, error) {
	p.next() // '['
	var items []node
	if p.cur().kind == tokOp && p.cur().text == "]" {
		p.next()
		return listNode{items: items}, nil
	}
	for {
		it, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		items = append(items, it)
		if p.cur().kind == tokOp && p.cur().text == "," {
			p.next()
			continue
		}
		if p.cur().kind == tokOp && p.cur().text == "]" {
			p.next()
			return listNode{items: items}, nil
		}
		return nil, parseErrorf("expected ',' or ']' in list")
	}
}

func (p *parser) parseMap() (node, error) {
	p.next() // '{'
	m := mapNode{}
	if p.cur().kind == tokOp && p.cur().text == "}" {
		p.next()
		return m, nil
	}
	for {
		var key string
		switch p.cur().kind {
		case tokString, tokIdent:
			key = p.next().text
		default:
			return nil, parseErrorf("expected map key")
		}
		if p.cur().kind != tokOp || p.cur().text != ":" {
			return nil, parseErrorf("expected ':' after map key")
		}
		p.next()
		v, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		m.keys = append(m.keys, key)
		m.vals = append(m.vals, v)
		if p.cur().kind == tokOp && p.cur().text == "," {
			p.next()
			continue
		}
		if p.cur().kind == tokOp && p.cur().text == "}" {
			p.next()
			return m, nil
		}
		return nil, parseErrorf("expected ',' or '}' in map")
	}
}

// --- evaluator ---

func eval(n node, vars map[string]any, env Env) (any, error) {
	switch t := n.(type) {
	case litNode:
		return t.v, nil
	case identNode:
		if v, ok := vars[t.name]; ok {
			return v, nil
		}
		return nil, evalErrorf("unknown variable %q", t.name)
	case listNode:
		out := make([]any, 0, len(t.items))
		for _, it := range t.items {
			v, err := eval(it, vars, env)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case mapNode:
		out := make(map[string]any, len(t.keys))
		for i, k := range t.keys {
			v, err := eval(t.vals[i], vars, env)
			if err != nil {
				return nil, err
			}
			out[k] = v
		}
		return out, nil
	case unaryNode:
		v, err := eval(t.x, vars, env)
		if err != nil {
			return nil, err
		}
		switch t.op {
		case "-":
			switch n := v.(type) {
			case int64:
				return -n, nil
			case float64:
				return -n, nil
			default:
				return nil, evalErrorf("cannot negate %T", v)
			}
		case "not":
			return !truthy(v), nil
		}
		return nil, evalErrorf("unsupported unary operator %q", t.op)
	case memberNode:
		obj, err := eval(t.obj, vars, env)
		if err != nil {
			return nil, err
		}
		m, ok := obj.(map[string]any)
		if !ok {
			return nil, evalErrorf("cannot access field %q on non-map value", t.key)
		}
		v, ok := m[t.key]
		if !ok {
			return nil, evalErrorf("unknown field %q", t.key)
		}
		return v, nil
	case indexNode:
		obj, err := eval(t.obj, vars, env)
		if err != nil {
			return nil, err
		}
		idx, err := eval(t.idx, vars, env)
		if err != nil {
			return nil, err
		}
		switch c := obj.(type) {
		case []any:
			i, ok := toInt(idx)
			if !ok {
				return nil, evalErrorf("list index must be an integer")
			}
			if i < 0 || i >= int64(len(c)) {
				return nil, evalErrorf("list index %d out of range", i)
			}
			return c[i], nil
		case map[string]any:
			k, ok := idx.(string)
			if !ok {
				return nil, evalErrorf("map key must be a string")
			}
			v, ok := c[k]
			if !ok {
				return nil, evalErrorf("unknown map key %q", k)
			}
			return v, nil
		default:
			return nil, evalErrorf("cannot index %T", obj)
		}
	case callNode:
		return callStdlib(t.fn, t.args, vars, env)
	case binaryNode:
		return evalBinary(t, vars, env)
	}
	return nil, evalErrorf("unknown expression node %T", n)
}

func evalBinary(b binaryNode, vars map[string]any, env Env) (any, error) {
	// Short-circuit for and/or.
	if b.op == "and" {
		l, err := eval(b.l, vars, env)
		if err != nil {
			return nil, err
		}
		if !truthy(l) {
			return false, nil
		}
		r, err := eval(b.r, vars, env)
		if err != nil {
			return nil, err
		}
		return truthy(r), nil
	}
	if b.op == "or" {
		l, err := eval(b.l, vars, env)
		if err != nil {
			return nil, err
		}
		if truthy(l) {
			return true, nil
		}
		r, err := eval(b.r, vars, env)
		if err != nil {
			return nil, err
		}
		return truthy(r), nil
	}

	l, err := eval(b.l, vars, env)
	if err != nil {
		return nil, err
	}
	r, err := eval(b.r, vars, env)
	if err != nil {
		return nil, err
	}
	switch b.op {
	case "+":
		return add(l, r)
	case "-", "*", "/", "//", "%":
		return arithmetic(b.op, l, r)
	case "==":
		return equal(l, r), nil
	case "!=":
		return !equal(l, r), nil
	case "<", ">", "<=", ">=":
		return compare(b.op, l, r)
	case "in":
		return inOp(l, r)
	}
	return nil, evalErrorf("unsupported binary operator %q", b.op)
}

func add(l, r any) (any, error) {
	switch a := l.(type) {
	case string:
		// String concatenation: string + anything → string (JSON for non-strings).
		if _, ok := r.(string); !ok {
			return a + stringify(r), nil
		}
		return a + r.(string), nil
	case int64:
		switch b := r.(type) {
		case int64:
			return a + b, nil
		case float64:
			return float64(a) + b, nil
		}
	case float64:
		switch b := r.(type) {
		case float64:
			return a + b, nil
		case int64:
			return a + float64(b), nil
		}
	}
	return nil, evalErrorf("cannot add %T and %T", l, r)
}

func arithmetic(op string, l, r any) (any, error) {
	lf, lok := toFloat(l)
	rf, rok := toFloat(r)
	if !lok || !rok {
		return nil, evalErrorf("operator %q requires numeric operands (%T, %T)", op, l, r)
	}
	bothInt := isInt(l) && isInt(r)
	switch op {
	case "-":
		if bothInt {
			return asInt(l) - asInt(r), nil
		}
		return lf - rf, nil
	case "*":
		if bothInt {
			return asInt(l) * asInt(r), nil
		}
		return lf * rf, nil
	case "/":
		if rf == 0 {
			return nil, evalErrorf("division by zero")
		}
		return lf / rf, nil
	case "//":
		// Floor division: the largest integer <= the true quotient. Integer
		// operands yield an integer; any float operand yields a float.
		if rf == 0 {
			return nil, evalErrorf("division by zero")
		}
		if bothInt {
			return int64(math.Floor(lf / rf)), nil
		}
		return math.Floor(lf / rf), nil
	case "%":
		// Remainder division works on general numbers (integers and floats).
		if rf == 0 {
			return nil, evalErrorf("modulo by zero")
		}
		if bothInt {
			return asInt(l) % asInt(r), nil
		}
		return math.Mod(lf, rf), nil
	}
	return nil, evalErrorf("unsupported operator %q", op)
}

func equal(l, r any) bool {
	switch a := l.(type) {
	case int64:
		if b, ok := r.(int64); ok {
			return a == b
		}
		if b, ok := r.(float64); ok {
			return float64(a) == b
		}
	case float64:
		if b, ok := r.(float64); ok {
			return a == b
		}
		if b, ok := r.(int64); ok {
			return a == float64(b)
		}
	}
	return jsonEqual(l, r)
}

func compare(op string, l, r any) (any, error) {
	lf, lok := toFloat(l)
	rf, rok := toFloat(r)
	if !lok || !rok {
		return nil, evalErrorf("comparison %q requires numeric operands", op)
	}
	switch op {
	case "<":
		return lf < rf, nil
	case ">":
		return lf > rf, nil
	case "<=":
		return lf <= rf, nil
	case ">=":
		return lf >= rf, nil
	}
	return nil, evalErrorf("unsupported comparison %q", op)
}

func inOp(l, r any) (any, error) {
	switch c := r.(type) {
	case []any:
		for _, e := range c {
			if equal(l, e) {
				return true, nil
			}
		}
		return false, nil
	case map[string]any:
		k, ok := l.(string)
		if !ok {
			return nil, evalErrorf("map membership requires a string key")
		}
		_, ok = c[k]
		return ok, nil
	default:
		return nil, evalErrorf("'in' requires a list or map on the right, got %T", r)
	}
}

func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case int64:
		return t != 0
	case float64:
		return t != 0
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return true
	}
}

func isInt(v any) bool { _, ok := v.(int64); return ok }
func asInt(v any) int64 {
	if i, ok := v.(int64); ok {
		return i
	}
	return 0
}
func toInt(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case float64:
		return int64(t), t == float64(int64(t))
	}
	return 0, false
}
func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case int64:
		return float64(t), true
	case float64:
		return t, true
	}
	return 0, false
}

// jsonEqual compares two values for deep equality via JSON serialization
// (works for nil, bool, string, and nested lists/maps).
func jsonEqual(l, r any) bool {
	lb, err := json.Marshal(l)
	if err != nil {
		return false
	}
	rb, err := json.Marshal(r)
	if err != nil {
		return false
	}
	return string(lb) == string(rb)
}

// callStdlib dispatches sys.* expression functions. Unsupported functions
// return a descriptive error (fail-loud).
func callStdlib(fn string, args []node, vars map[string]any, env Env) (any, error) {
	argvals := make([]any, len(args))
	for i, a := range args {
		v, err := eval(a, vars, env)
		if err != nil {
			return nil, err
		}
		argvals[i] = v
	}
	switch fn {
	case "sys.get_env":
		if len(argvals) < 1 || len(argvals) > 2 {
			return nil, evalErrorf("sys.get_env expects 1 or 2 arguments")
		}
		name, ok := argvals[0].(string)
		if !ok {
			return nil, evalErrorf("sys.get_env name must be a string")
		}
		if len(argvals) == 2 {
			if _, ok := argvals[1].(string); !ok {
				return nil, evalErrorf("sys.get_env default must be a string")
			}
		}
		// GCP built-in environment variables (GOOGLE_CLOUD_*) resolve from the
		// execution context, then the process environment, then the supplied
		// default, then null (an unset variable is null, not an error).
		if v, ok := env.Builtins[name]; ok {
			return v, nil
		}
		if v, ok := env.Getenv(name); ok {
			return v, nil
		}
		if len(argvals) == 2 {
			return argvals[1], nil
		}
		return nil, nil
	case "sys.now":
		if len(argvals) != 0 {
			return nil, evalErrorf("sys.now expects no arguments")
		}
		return env.Now().Format(time.RFC3339Nano), nil
	default:
		return nil, evalErrorf("unsupported function %q", fn)
	}
}
