// Package expr implements the Google Workflows expression language: the
// ${...} interpolation syntax plus the operators and sys.* functions used
// inside those expressions. Values are the JSON-compatible Go types nil, bool,
// int64, float64, string, []any, and map[string]any.
package expr

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// ParseError describes a syntax error in an expression.
type ParseError struct{ msg string }

func (e *ParseError) Error() string { return "expression: " + e.msg }

// EvalError describes a runtime error while evaluating an expression.
type EvalError struct{ msg string }

func (e *EvalError) Error() string { return "expression: " + e.msg }

func parseErrorf(format string, a ...any) error { return &ParseError{fmt.Sprintf(format, a...)} }
func evalErrorf(format string, a ...any) error  { return &EvalError{fmt.Sprintf(format, a...)} }

// Env carries the environment an expression evaluates against. Getenv resolves
// sys.get_env, and Now resolves sys.now (the engine injects the global clock so
// deterministic time-freeze applies). Builtins carries the GCP built-in
// environment variables (GOOGLE_CLOUD_*) for the current execution; sys.get_env
// resolves those before falling back to Getenv.
type Env struct {
	Getenv   func(string) (string, bool)
	Now      func() time.Time
	Builtins map[string]string
}

// DefaultEnv returns a real-environment Env (process env + wall-clock UTC).
func DefaultEnv() Env {
	return Env{Getenv: os.LookupEnv, Now: func() time.Time { return time.Now().UTC() }}
}

// Interpolate evaluates a template string. A string that is exactly a single
// ${...} expression yields the expression's typed value; otherwise each ${...}
// is substituted (non-string values are stringified) into the surrounding
// literal text. A $$ escapes a literal dollar sign. A string with no ${...} is
// returned unchanged.
func Interpolate(s string, vars map[string]any, env Env) (any, error) {
	if !strings.Contains(s, "$") {
		return s, nil
	}
	if isSingleExpr(s) {
		return EvalString(s[2:len(s)-1], vars, env)
	}
	var b strings.Builder
	i := 0
	for i < len(s) {
		c := s[i]
		if c == '$' && i+1 < len(s) && s[i+1] == '$' {
			b.WriteByte('$')
			i += 2
			continue
		}
		if c == '$' && i+1 < len(s) && s[i+1] == '{' {
			end := findCloseBrace(s, i+2)
			if end < 0 {
				return nil, parseErrorf("unterminated ${...} in %q", s)
			}
			v, err := EvalString(s[i+2:end], vars, env)
			if err != nil {
				return nil, err
			}
			b.WriteString(stringify(v))
			i = end + 1
			continue
		}
		b.WriteByte(c)
		i++
	}
	return b.String(), nil
}

// isSingleExpr reports whether s is exactly "${...}" with no surrounding text.
func isSingleExpr(s string) bool {
	if len(s) < 3 || s[0] != '$' || s[1] != '{' {
		return false
	}
	return findCloseBrace(s, 2) == len(s)-1
}

// findCloseBrace returns the index of the '}' matching the '{' implied at
// position open (the index just past the '{'), honoring nested braces and
// quoted strings. Returns -1 when unmatched.
func findCloseBrace(s string, open int) int {
	depth := 1
	var quote byte
	for i := open; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == '\\' {
				i++
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// stringify renders a value as its string form for interpolation (JSON-encoded
// for non-strings so lists/maps embed deterministically).
func stringify(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return toJSON(v)
	}
}
