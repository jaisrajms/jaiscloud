package compute

import (
	"fmt"
	"strconv"
	"strings"

	"jaiscloud/internal/model"
)

// filter.go implements the documented subset of Compute Engine's list `filter`
// query parameter that the emulator supports. The real API (Discovery
// `instances.list.filter`) accepts two mutually exclusive syntaxes: AIP-160
// expressions (`field OP value`, OP ∈ {=, !=, >, <, <=, >=, :}) and RE2
// regular expressions (`field eq|ne regex`). The emulator implements a single
// AIP-160 equality/existence expression over a dotted field path:
//
//	name = vm-1            name != vm-1            status = RUNNING
//	labels.env = test      labels.env:*            name = "vm 1"
//
// Anything outside that grammar fails loud with 400 rather than silently
// returning a wrong page, so a client never gets an unfiltered (or empty)
// result for a filter the emulator does not understand. In particular a
// compound expression (`a = 1 AND b = 2`, `(a = 1) OR (b = 2)`, a second
// operator, comparison operators, or the RE2 eq/ne form) is rejected, never
// swallowed into the operand. The unsupported parts are recorded in the plan
// doc (TD4-a).
//
// Other services keep their own filter parsers with different field models
// (internal/gcp/service/functions/operations_filter.go, internal/gcp/pubsubfilter);
// this is the Compute subset and is intentionally narrower.

// filterExpr is one parsed `field OP value` list filter.
type filterExpr struct {
	path  string // dotted field path, e.g. "labels.env"
	op    string // "=", "!=" or ":" (key-defined)
	value string // literal operand (unquoted); empty for ":"
}

// compileFilter reads the request's `filter` parameter. It returns (nil, nil)
// when no filter was supplied and a 400 ProviderError when the expression is
// outside the supported grammar.
func compileFilter(nr *model.NormalizedRequest) (*filterExpr, error) {
	raw, _ := nr.Params["filter"].(string)
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	f, err := parseFilter(raw)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument",
			fmt.Sprintf("Invalid value for field 'filter': %q. %v", raw, err), 400)
	}
	return f, nil
}

// parseFilter parses one AIP-160 equality/existence expression.
func parseFilter(raw string) (*filterExpr, error) {
	s := strings.TrimSpace(raw)
	// Locate the first operator. The field path may not contain '=', '!' or
	// ':', so the first one found terminates it.
	op, i, valStart := "", -1, -1
	for j := 0; j < len(s); j++ {
		switch {
		case s[j] == '!' && j+1 < len(s) && s[j+1] == '=':
			op, i, valStart = "!=", j, j+2
		case s[j] == '=':
			op, i, valStart = "=", j, j+1
		case s[j] == ':':
			op, i, valStart = ":", j, j+1
		}
		if op != "" {
			break
		}
	}
	if op == "" {
		if strings.ContainsAny(s, "<>()") || strings.Contains(s, " eq ") || strings.Contains(s, " ne ") {
			return nil, fmt.Errorf("unsupported filter operator; compute list filters support only `field = value`, `field != value` and `field:*`")
		}
		return nil, fmt.Errorf("expression must be of the form \"field operator value\"")
	}
	field := strings.TrimSpace(s[:i])
	if field == "" || !validFieldPath(field) {
		return nil, fmt.Errorf("unsupported field %q", field)
	}
	rest := strings.TrimSpace(s[valStart:])
	if op == ":" {
		// Real GCP uses the `field:*` form for a key-defined test.
		if rest != "*" {
			return nil, fmt.Errorf("operator \":\" supports only the key-defined form \"field:*\"")
		}
		return &filterExpr{path: field, op: op}, nil
	}
	val, err := parseLiteral(rest)
	if err != nil {
		return nil, err
	}
	return &filterExpr{path: field, op: op, value: val}, nil
}

// parseLiteral extracts one operand. A quoted literal may contain spaces and
// punctuation, but not the quote itself (which would signal trailing
// expression text); an unquoted literal must be a single token. A second
// operator, parentheses or a boolean keyword is therefore rejected instead of
// being absorbed into the operand — absorbing it would return a wrong page.
func parseLiteral(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("operator requires a value")
	}
	if s[0] == '"' || s[0] == '\'' {
		q := s[0]
		if len(s) < 2 || s[len(s)-1] != q {
			return "", errCompoundFilter
		}
		inner := s[1 : len(s)-1]
		if strings.ContainsAny(inner, `"'`) {
			return "", errCompoundFilter
		}
		return inner, nil
	}
	if strings.ContainsAny(s, " \t\"'=!:()<>") {
		return "", errCompoundFilter
	}
	return s, nil
}

// errCompoundFilter rejects expressions beyond a single `field OP value`: the
// emulator supports one expression, not AIP-160 AND/OR or a second constraint.
var errCompoundFilter = fmt.Errorf("only a single \"field operator value\" expression is supported (no AND/OR, parentheses or a second constraint)")

// validFieldPath accepts dotted lowercase/uppercase/digit/underscore segments.
func validFieldPath(p string) bool {
	for _, seg := range strings.Split(p, ".") {
		if seg == "" {
			return false
		}
		for _, r := range seg {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
				continue
			}
			return false
		}
	}
	return true
}

// match reports whether the resource map satisfies the expression. A nil
// expression matches everything.
func (f *filterExpr) match(m map[string]any) bool {
	if f == nil {
		return true
	}
	val, ok := lookupPath(m, f.path)
	if f.op == ":" {
		// `field:*` is the key-defined test: presence, regardless of value.
		return ok
	}
	equal := ok && renderValue(val) == f.value
	if f.op == "!=" {
		return !equal
	}
	return equal
}

// lookupPath resolves a dotted field path through nested JSON objects.
func lookupPath(m map[string]any, path string) (any, bool) {
	var cur any = m
	for _, seg := range strings.Split(path, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = obj[seg]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// renderValue renders a JSON scalar for comparison. Objects and arrays render
// to their Go string form, which is enough to compare against a literal.
func renderValue(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

// applyFilterMaps returns the items satisfying the request's filter (all items
// when none was supplied).
func applyFilterMaps(nr *model.NormalizedRequest, items []map[string]any) ([]map[string]any, error) {
	f, err := compileFilter(nr)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return items, nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, m := range items {
		if f.match(m) {
			out = append(out, m)
		}
	}
	return out, nil
}
