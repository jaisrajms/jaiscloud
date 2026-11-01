package queryengine

import (
	"database/sql/driver"
	"fmt"
	"strings"

	sqlite "modernc.org/sqlite"
)

// BigQuery's operators are strictly typed. SQLite silently coerces values
// between storage classes, so the translator/runtime add the missing BigQuery
// checks here rather than letting the scratch engine's affinity rules leak:
//
//   - '/' gets a runtime denominator guard (SQLite returns NULL on division by
//     zero; BigQuery raises an error unless SAFE_DIVIDE is used).
//   - comparisons and arithmetic operators reject operands whose statically
//     known BigQuery types have no common signature.

const divisionGuardFunction = "bq_div_guard"

func init() {
	// The guard is injected by guardDivisionDenominators after user functions
	// have been validated, so a client cannot call it directly.
	sqlite.MustRegisterDeterministicScalarFunction(divisionGuardFunction, 1,
		func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			if len(args) != 1 || args[0] == nil {
				return nil, nil
			}
			switch v := args[0].(type) {
			case int64:
				if v == 0 {
					return nil, fmt.Errorf("division by zero")
				}
			case float64:
				if v == 0 {
					return nil, fmt.Errorf("division by zero")
				}
			}
			return args[0], nil
		})
}

// guardDivisionDenominators wraps the right operand of every binary '/' in the
// scratch SQL with the division guard. The rewrite is recursive, so a division
// nested inside the denominator is guarded too, while left-associative chains
// (a / b / c) guard each denominator independently.
func guardDivisionDenominators(toks []token) []token {
	out := make([]token, 0, len(toks))
	for i := 0; i < len(toks); i++ {
		tk := toks[i]
		out = append(out, tk)
		if tk.kind != tokOp || tk.raw != "/" {
			continue
		}
		start, end, ok := divisionDenominator(toks, i)
		if !ok {
			continue
		}
		out = append(out,
			token{kind: tokIdent, raw: divisionGuardFunction, up: strings.ToUpper(divisionGuardFunction)},
			token{kind: tokOp, raw: "("})
		out = append(out, guardDivisionDenominators(toks[start:end+1])...)
		out = append(out, token{kind: tokOp, raw: ")"})
		i = end
	}
	return out
}

// divisionDenominator returns the token span of the primary expression that is
// the right operand of the '/' at op: a literal, qualified column, function
// call, CASE expression, parenthesized group, or a unary-signed primary.
func divisionDenominator(toks []token, op int) (start, end int, ok bool) {
	start = op + 1
	j := start
	for j < len(toks) && toks[j].kind == tokOp && (toks[j].raw == "+" || toks[j].raw == "-") {
		j++
	}
	if j >= len(toks) {
		return 0, 0, false
	}
	switch toks[j].kind {
	case tokNumber, tokString, tokQuoted:
		j++
	case tokIdent:
		if toks[j].up == "CASE" {
			depth := 1
			for j++; j < len(toks) && depth > 0; j++ {
				if toks[j].kind != tokIdent {
					continue
				}
				switch toks[j].up {
				case "CASE":
					depth++
				case "END":
					depth--
				}
			}
			if depth != 0 {
				return 0, 0, false
			}
		} else {
			j++
			for j+1 < len(toks) && toks[j].kind == tokOp && toks[j].raw == "." &&
				(toks[j+1].kind == tokIdent || toks[j+1].kind == tokQuoted) {
				j += 2
			}
			if j < len(toks) && toks[j].kind == tokOp && toks[j].raw == "(" {
				close := matchParen(toks, j)
				if close < 0 {
					return 0, 0, false
				}
				j = close + 1
			}
		}
	case tokOp:
		if toks[j].raw != "(" {
			return 0, 0, false
		}
		close := matchParen(toks, j)
		if close < 0 {
			return 0, 0, false
		}
		j = close + 1
	default:
		return 0, 0, false
	}
	return start, j - 1, true
}

var comparisonOperators = map[string]bool{
	"=": true, "<>": true, "!=": true, "<": true, "<=": true, ">": true, ">=": true,
}

var arithmeticOperators = map[string]bool{
	"+": true, "-": true, "*": true, "/": true,
}

// comparisonBoundaryKeywords end an operand at the same nesting level.
var comparisonBoundaryKeywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "GROUP": true, "BY": true,
	"HAVING": true, "ORDER": true, "LIMIT": true, "OFFSET": true, "ON": true,
	"JOIN": true, "LEFT": true, "INNER": true, "OUTER": true, "UNION": true,
	"AND": true, "OR": true, "NOT": true, "WHEN": true, "THEN": true,
	"ELSE": true, "END": true, "CASE": true, "AS": true, "ASC": true,
	"DESC": true, "INTO": true, "SET": true, "VALUES": true, "BETWEEN": true,
	"LIKE": true, "IN": true, "IS": true,
}

// validateCoercions rejects the operator/operand combinations BigQuery refuses
// but SQLite would silently coerce. It runs after table hydration, so column
// types are known; operands whose type cannot be inferred are left to SQLite
// rather than guessed.
func validateCoercions(toks []token, fields map[string]Field) error {
	for i, tk := range toks {
		if tk.kind != tokOp {
			continue
		}
		switch {
		case comparisonOperators[tk.raw]:
			if err := validateOperatorOperands(toks, i, fields, false); err != nil {
				return err
			}
		case arithmeticOperators[tk.raw]:
			// A '*' in SELECT * / COUNT(*) has no left or right arithmetic
			// operand; validateOperatorOperands skips an empty side.
			if err := validateOperatorOperands(toks, i, fields, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateOperatorOperands(toks []token, op int, fields map[string]Field, arithmetic bool) error {
	leftStart, leftEnd := operandLeft(toks, op, arithmetic)
	rightStart, rightEnd := operandRight(toks, op, arithmetic)
	if leftStart >= leftEnd || rightStart >= rightEnd {
		return nil
	}
	lt := inferExprType(toks[leftStart:leftEnd], fields)
	rt := inferExprType(toks[rightStart:rightEnd], fields)
	if lt == "" || rt == "" {
		return nil
	}
	if operatorTypesCompatible(lt, rt, arithmetic) {
		return nil
	}
	if !arithmetic && literalCoercionAllowed(toks[leftStart:leftEnd], lt, toks[rightStart:rightEnd], rt) {
		return nil
	}
	return unsupported(fmt.Sprintf("No matching signature for operator %s for argument types: %s, %s",
		toks[op].raw, bigQueryTypeName(lt), bigQueryTypeName(rt)))
}

// literalCoercionAllowed models GoogleSQL's literal coercion for temporal
// comparisons: a bare string literal may compare with a DATE/DATETIME/TIME/
// TIMESTAMP column, but a STRING column may not. Numeric/string literal pairs
// remain errors.
func literalCoercionAllowed(left []token, lt string, right []token, rt string) bool {
	if isTemporalType(lt) && comparisonFamily(rt) == "STRING" && isStringLiteral(right) {
		return true
	}
	if isTemporalType(rt) && comparisonFamily(lt) == "STRING" && isStringLiteral(left) {
		return true
	}
	return false
}

func isStringLiteral(expr []token) bool {
	expr = stripParens(expr)
	return len(expr) == 1 && expr[0].kind == tokString
}

func isTemporalType(t string) bool {
	switch normalizeTypeName(t) {
	case "DATE", "DATETIME", "TIME", "TIMESTAMP":
		return true
	}
	return false
}

func operandLeft(toks []token, op int, arithmetic bool) (start, end int) {
	end = op
	start = op
	depth := 0
	for j := op - 1; j >= 0; j-- {
		tk := toks[j]
		if tk.kind == tokOp {
			switch tk.raw {
			case ")":
				depth++
				start = j
				continue
			case "(":
				if depth == 0 {
					return j + 1, end
				}
				depth--
				start = j
				continue
			}
		}
		if depth == 0 && operandBoundary(tk, arithmetic) {
			return j + 1, end
		}
		start = j
	}
	return start, end
}

func operandRight(toks []token, op int, arithmetic bool) (start, end int) {
	start = op + 1
	end = start
	depth := 0
	// A unary sign belongs to the right operand, not to the operator being
	// validated.
	j := start
	for j < len(toks) && toks[j].kind == tokOp && (toks[j].raw == "+" || toks[j].raw == "-") {
		j++
	}
	for ; j < len(toks); j++ {
		tk := toks[j]
		if tk.kind == tokOp {
			switch tk.raw {
			case "(":
				depth++
			case ")":
				if depth == 0 {
					return start, j
				}
				depth--
			}
		}
		if depth == 0 && operandBoundary(tk, arithmetic) {
			return start, j
		}
		end = j + 1
	}
	return start, end
}

func operandBoundary(tk token, arithmetic bool) bool {
	switch tk.kind {
	case tokOp:
		if tk.raw == "," || tk.raw == ";" || tk.raw == "||" {
			return true
		}
		if comparisonOperators[tk.raw] {
			return true
		}
		return arithmetic && arithmeticOperators[tk.raw]
	case tokIdent:
		return comparisonBoundaryKeywords[tk.up]
	}
	return false
}

// operatorTypesCompatible applies BigQuery's implicit numeric coercions:
// INT64/FLOAT64/NUMERIC/BIGNUMERIC compare and combine freely, every other
// type needs an exact match. Arithmetic is numeric-only.
func operatorTypesCompatible(a, b string, arithmetic bool) bool {
	fa, fb := comparisonFamily(a), comparisonFamily(b)
	if arithmetic {
		return fa == "NUMERIC" && fb == "NUMERIC"
	}
	return fa == fb
}

func comparisonFamily(t string) string {
	switch n := normalizeTypeName(t); n {
	case "INT64", "FLOAT64", "NUMERIC":
		return "NUMERIC"
	default:
		return n
	}
}

func bigQueryTypeName(t string) string {
	if n := normalizeTypeName(t); n != "" {
		return n
	}
	return strings.ToUpper(t)
}
