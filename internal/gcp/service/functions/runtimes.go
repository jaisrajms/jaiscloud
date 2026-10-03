package functions

import (
	"fmt"
	"strings"
)

// Runtime is one Cloud Functions v2 runtime descriptor
// (google.cloud.functions.v2.ListRuntimesResponse.Runtime). It is the
// transport-neutral shape shared by the REST and gRPC adapters: the REST render
// emits RuntimeJSON directly, and the gRPC transcode maps it onto the generated
// proto.
//
// The catalog is a curated static snapshot, not a live query: real Cloud
// Functions derives it from its build images, which an emulator does not have.
// The field names/enums are exactly the Discovery `Runtime` schema
// (name/displayName/stage/environment/warnings/deprecationDate/
// decommissionDate), so a client that validates against the official schema
// (gcloud `functions deploy --gen2` resolves a runtime from this list, filtered
// to environment GEN_2) accepts it unchanged.
type Runtime struct {
	// Name is the runtime id, e.g. "nodejs20".
	Name string
	// DisplayName is the user-facing name, e.g. "Node.js 20".
	DisplayName string
	// Stage is a ListRuntimesResponse_RuntimeStage enum value
	// (DEVELOPMENT/ALPHA/BETA/GA/DEPRECATED/DECOMMISSIONED).
	Stage string
	// Environment is an Environment enum value (GEN_1/GEN_2).
	Environment string
	// Warnings are deprecation/advisory messages for the runtime.
	Warnings []string
	// DeprecationDate and DecommissionDate are optional google.type.Date
	// objects ({year, month, day}); nil omits the field.
	DeprecationDate  map[string]any
	DecommissionDate map[string]any
}

// runtimeCatalog is the stable set of runtimes the emulator advertises. It
// covers the GEN_2 runtimes gcloud/Cloud Functions clients validate against
// (gcloud filters ListRuntimes to environment GEN_2 when deploying a gen2
// function). Deprecated entries carry a warning and a deprecation date, matching
// the shape of real GCP's response.
var runtimeCatalog = []Runtime{
	// Node.js.
	{Name: "nodejs18", DisplayName: "Node.js 18", Stage: "DEPRECATED", Environment: "GEN_2",
		Warnings:        []string{"This runtime is deprecated. See https://cloud.google.com/functions/docs/runtime-support for more information."},
		DeprecationDate: date(2025, 4, 30)},
	{Name: "nodejs20", DisplayName: "Node.js 20", Stage: "GA", Environment: "GEN_2"},
	{Name: "nodejs22", DisplayName: "Node.js 22", Stage: "GA", Environment: "GEN_2"},
	{Name: "nodejs24", DisplayName: "Node.js 24", Stage: "GA", Environment: "GEN_2"},

	// Python.
	{Name: "python310", DisplayName: "Python 3.10", Stage: "GA", Environment: "GEN_2"},
	{Name: "python311", DisplayName: "Python 3.11", Stage: "GA", Environment: "GEN_2"},
	{Name: "python312", DisplayName: "Python 3.12", Stage: "GA", Environment: "GEN_2"},
	{Name: "python313", DisplayName: "Python 3.13", Stage: "GA", Environment: "GEN_2"},

	// Go.
	{Name: "go121", DisplayName: "Go 1.21", Stage: "GA", Environment: "GEN_2"},
	{Name: "go122", DisplayName: "Go 1.22", Stage: "GA", Environment: "GEN_2"},
	{Name: "go123", DisplayName: "Go 1.23", Stage: "GA", Environment: "GEN_2"},
	{Name: "go124", DisplayName: "Go 1.24", Stage: "GA", Environment: "GEN_2"},

	// Java.
	{Name: "java17", DisplayName: "Java 17", Stage: "GA", Environment: "GEN_2"},
	{Name: "java21", DisplayName: "Java 21", Stage: "GA", Environment: "GEN_2"},

	// Ruby.
	{Name: "ruby32", DisplayName: "Ruby 3.2", Stage: "GA", Environment: "GEN_2"},
	{Name: "ruby33", DisplayName: "Ruby 3.3", Stage: "GA", Environment: "GEN_2"},

	// PHP.
	{Name: "php82", DisplayName: "PHP 8.2", Stage: "GA", Environment: "GEN_2"},
	{Name: "php83", DisplayName: "PHP 8.3", Stage: "GA", Environment: "GEN_2"},
	{Name: "php84", DisplayName: "PHP 8.4", Stage: "GA", Environment: "GEN_2"},

	// .NET.
	{Name: "dotnet6", DisplayName: ".NET 6", Stage: "DEPRECATED", Environment: "GEN_2",
		Warnings:        []string{"This runtime is deprecated. See https://cloud.google.com/functions/docs/runtime-support for more information."},
		DeprecationDate: date(2025, 2, 28)},
	{Name: "dotnet8", DisplayName: ".NET 8", Stage: "GA", Environment: "GEN_2"},
}

// date builds a google.type.Date object.
func date(year, month, day int) map[string]any {
	return map[string]any{"year": year, "month": month, "day": day}
}

// ListRuntimes returns the runtimes available in a project/location, honoring an
// optional AIP-160 filter over name/displayName/environment/stage. The catalog
// is project- and location-independent (as in the emulator's other synthesized
// catalogs), so the parent only validates the request.
//
// Unlike the emulator's other list surfaces, ListRuntimes has no pagination:
// the real v2 Discovery method declares only `parent` and `filter` (no
// pageSize/pageToken, no nextPageToken in the response).
func (s *Service) ListRuntimes(project, location, filter string) ([]Runtime, error) {
	if location == "" {
		return nil, invalidArgument("missing location")
	}
	return filterRuntimes(runtimeCatalog, filter)
}

// RuntimeJSON renders a Runtime as the Discovery-shaped map both transports
// agree on.
func RuntimeJSON(rt Runtime) map[string]any {
	out := map[string]any{
		"name":        rt.Name,
		"displayName": rt.DisplayName,
		"stage":       rt.Stage,
		"environment": rt.Environment,
	}
	if len(rt.Warnings) > 0 {
		out["warnings"] = rt.Warnings
	}
	if rt.DeprecationDate != nil {
		out["deprecationDate"] = rt.DeprecationDate
	}
	if rt.DecommissionDate != nil {
		out["decommissionDate"] = rt.DecommissionDate
	}
	return out
}

// filterRuntimes applies an AIP-160 filter over the catalog. Only the fields
// the Runtime schema exposes are supported (name/displayName/stage/environment);
// an unknown field, an unsupported operator (e.g. an AIP-160 function call), or
// a malformed expression is InvalidArgument, matching real Cloud Functions — a
// filter the emulator cannot evaluate is rejected rather than silently returning
// a wrong (e.g. empty) result.
func filterRuntimes(catalog []Runtime, filter string) ([]Runtime, error) {
	pred, err := compileRuntimeFilter(filter)
	if err != nil {
		return nil, err
	}
	// Always return a fresh slice: callers must not be able to mutate the
	// package-level catalog (or its Warnings sub-slices) through the result.
	out := make([]Runtime, 0, len(catalog))
	for _, rt := range catalog {
		if pred.match(rt) {
			out = append(out, rt)
		}
	}
	return out, nil
}

// runtimeFilterFields is the set of filterable Runtime fields.
var runtimeFilterFields = map[string]bool{
	"name":        true,
	"displayName": true,
	"stage":       true,
	"environment": true,
}

// runtimePredicate is a compiled filter expression evaluated against a runtime.
type runtimePredicate interface {
	match(rt Runtime) bool
}

// matchAllRuntime matches every runtime (the empty filter).
type matchAllRuntime struct{}

func (matchAllRuntime) match(Runtime) bool { return true }

type andRuntime struct{ l, r runtimePredicate }

func (p andRuntime) match(rt Runtime) bool { return p.l.match(rt) && p.r.match(rt) }

type orRuntime struct{ l, r runtimePredicate }

func (p orRuntime) match(rt Runtime) bool { return p.l.match(rt) || p.r.match(rt) }

type notRuntime struct{ inner runtimePredicate }

func (p notRuntime) match(rt Runtime) bool { return !p.inner.match(rt) }

// cmpRuntime is one `field OP value` predicate. Every Runtime field is a string,
// so the ordering operators compare lexicographically (AIP-160 string order)
// and `:` performs the AIP-160 "has" (substring) match.
type cmpRuntime struct {
	field string
	op    string
	value string
}

func (p cmpRuntime) match(rt Runtime) bool {
	var actual string
	switch p.field {
	case "name":
		actual = rt.Name
	case "displayName":
		actual = rt.DisplayName
	case "stage":
		actual = rt.Stage
	case "environment":
		actual = rt.Environment
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
	case "<":
		return actual < p.value
	case "<=":
		return actual <= p.value
	case ">":
		return actual > p.value
	case ">=":
		return actual >= p.value
	}
	return false
}

// compileRuntimeFilter parses an AIP-160 expression over the Runtime fields:
//
//	expr       := and
//	and        := or ( "AND" or )*
//	or         := unary ( "OR" unary )*
//	unary      := "NOT" unary | primary
//	primary    := "(" expr ")" | comparison
//	comparison := field ( "=" | "!=" | ":" | "<" | "<=" | ">" | ">=" ) value
//
// Per AIP-160, OR binds tighter than AND (`a AND b OR c` is `a AND (b OR c)`),
// matching the logging filter evaluator. field is one of
// name/displayName/stage/environment (case-sensitive) and value is a
// double-quoted string or a bare token. Keywords are case-insensitive. An
// empty filter matches everything. Anything outside this grammar (an unknown
// field, an AIP-160 function call, a dangling operator, ...) is InvalidArgument.
func compileRuntimeFilter(filter string) (runtimePredicate, error) {
	if strings.TrimSpace(filter) == "" {
		return matchAllRuntime{}, nil
	}
	toks, err := tokenizeRuntimeFilter(filter)
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	p := &runtimeFilterParser{toks: toks}
	pred, err := p.parseAnd()
	if err != nil {
		return nil, invalidArgument(err.Error())
	}
	if p.peek().kind != rtTokEOF {
		return nil, invalidArgument("unsupported runtimes filter " + filter)
	}
	return pred, nil
}

// ─── tokenizer ────────────────────────────────────────────────────────────────

type runtimeFilterTokenKind int

const (
	rtTokEOF runtimeFilterTokenKind = iota
	rtTokIdent
	rtTokString
	rtTokOp
	rtTokLParen
	rtTokRParen
)

type runtimeFilterToken struct {
	kind runtimeFilterTokenKind
	text string
}

// tokenizeRuntimeFilter splits an AIP-160 expression into identifiers, quoted
// strings, comparison operators, and parentheses. An unterminated string or an
// unexpected character is an error.
func tokenizeRuntimeFilter(s string) ([]runtimeFilterToken, error) {
	var toks []runtimeFilterToken
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, runtimeFilterToken{kind: rtTokLParen, text: "("})
			i++
		case c == ')':
			toks = append(toks, runtimeFilterToken{kind: rtTokRParen, text: ")"})
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
				return nil, fmt.Errorf("unterminated string literal in runtimes filter")
			}
			toks = append(toks, runtimeFilterToken{kind: rtTokString, text: b.String()})
			i = j
		case c == '=' || c == '!' || c == '<' || c == '>' || c == ':':
			if i+1 < len(s) {
				switch s[i : i+2] {
				case "!=", "<=", ">=":
					toks = append(toks, runtimeFilterToken{kind: rtTokOp, text: s[i : i+2]})
					i += 2
					continue
				}
			}
			if c == '!' {
				return nil, fmt.Errorf("unexpected character %q in runtimes filter", string(c))
			}
			toks = append(toks, runtimeFilterToken{kind: rtTokOp, text: string(c)})
			i++
		case isRuntimeIdentStart(c):
			j := i + 1
			for j < len(s) && isRuntimeIdentPart(s[j]) {
				j++
			}
			toks = append(toks, runtimeFilterToken{kind: rtTokIdent, text: s[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q in runtimes filter", string(c))
		}
	}
	toks = append(toks, runtimeFilterToken{kind: rtTokEOF})
	return toks, nil
}

func isRuntimeIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isRuntimeIdentPart(c byte) bool {
	return isRuntimeIdentStart(c) || (c >= '0' && c <= '9') || c == '-' || c == '.'
}

// ─── parser ───────────────────────────────────────────────────────────────────

type runtimeFilterParser struct {
	toks []runtimeFilterToken
	pos  int
}

func (p *runtimeFilterParser) peek() runtimeFilterToken { return p.toks[p.pos] }

func (p *runtimeFilterParser) next() runtimeFilterToken {
	t := p.toks[p.pos]
	if t.kind != rtTokEOF {
		p.pos++
	}
	return t
}

func (p *runtimeFilterParser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == rtTokIdent && strings.EqualFold(t.text, kw)
}

func (p *runtimeFilterParser) parseAnd() (runtimePredicate, error) {
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
		l = andRuntime{l, r}
	}
	return l, nil
}

func (p *runtimeFilterParser) parseOr() (runtimePredicate, error) {
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
		l = orRuntime{l, r}
	}
	return l, nil
}

func (p *runtimeFilterParser) parseUnary() (runtimePredicate, error) {
	if p.isKeyword("not") {
		p.next()
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return notRuntime{inner}, nil
	}
	return p.parsePrimary()
}

func (p *runtimeFilterParser) parsePrimary() (runtimePredicate, error) {
	if p.peek().kind == rtTokLParen {
		p.next()
		inner, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		if p.next().kind != rtTokRParen {
			return nil, fmt.Errorf("missing closing parenthesis in runtimes filter")
		}
		return inner, nil
	}
	return p.parseComparison()
}

func (p *runtimeFilterParser) parseComparison() (runtimePredicate, error) {
	field := p.next()
	if field.kind != rtTokIdent {
		return nil, fmt.Errorf("expected runtimes filter field, got %q", field.text)
	}
	if !runtimeFilterFields[field.text] {
		return nil, fmt.Errorf("unsupported runtimes filter field %q", field.text)
	}
	op := p.next()
	if op.kind != rtTokOp {
		return nil, fmt.Errorf("expected comparison operator after %q", field.text)
	}
	value := p.next()
	if value.kind != rtTokString && value.kind != rtTokIdent {
		return nil, fmt.Errorf("expected value after %q", op.text)
	}
	return cmpRuntime{field: field.text, op: op.text, value: value.text}, nil
}
