package queryengine

import "strings"

// inferOutputFields derives the output column types of a SELECT from the
// statement's structure (its "query plan") instead of the runtime SQLite
// values. That matters for two cases the runtime cannot answer:
//
//   - a boolean expression (SELECT a > 1) is INT64/1 at runtime but BOOL by
//     plan, and
//   - an empty result set carries no values at all, so its columns would
//     otherwise default to STRING.
//
// It returns nil when the shape is not a plain single SELECT with an explicit
// list (SELECT *, set operations, ...), in which case the engine falls back to
// declared-column / runtime inference.
func inferOutputFields(toks []token, fields map[string]Field) []Field {
	selIdx := -1
	depth := 0
	for i, tk := range toks {
		if tk.kind == tokOp {
			switch tk.raw {
			case "(":
				depth++
			case ")":
				depth--
			}
			continue
		}
		if depth == 0 && tk.kind == tokIdent && tk.up == "SELECT" {
			selIdx = i
			break
		}
	}
	if selIdx < 0 {
		return nil
	}

	end := len(toks)
	depth = 0
	for i := selIdx + 1; i < len(toks); i++ {
		tk := toks[i]
		if tk.kind == tokOp {
			switch tk.raw {
			case "(":
				depth++
			case ")":
				depth--
			case "*":
				// A depth-0 '*' is a wildcard only in select-list position:
				// right after SELECT or a comma, or qualified (t.*). A
				// multiplication operand (a * b, count(*) * 2) is not.
				if depth == 0 && (i == selIdx+1 || (i > 0 && (toks[i-1].raw == "," || toks[i-1].raw == "."))) {
					return nil
				}
			}
			continue
		}
		if depth == 0 && tk.kind == tokIdent {
			switch tk.up {
			case "FROM":
				end = i
				i = len(toks)
			case "UNION", "EXCEPT", "INTERSECT":
				return nil // set operations: fall back
			}
		}
	}

	items := splitTopLevel(toks[selIdx+1:end], ",")
	out := make([]Field, 0, len(items))
	for _, item := range items {
		expr := item
		if as := lastTopLevelAs(item); as >= 0 {
			expr = item[:as]
		}
		f := Field{Type: inferExprType(expr, fields)}
		// A passthrough column keeps its declared mode (REQUIRED/REPEATED);
		// an expression is NULLABLE.
		if col, ok := simpleColumnRef(expr); ok {
			if sf, ok := fields[strings.ToLower(col)]; ok {
				f.Mode = sf.Mode
			}
		}
		out = append(out, f)
	}
	return out
}

// simpleColumnRef reports whether expr is a single (possibly table-qualified)
// column reference and returns the bare column name.
func simpleColumnRef(expr []token) (string, bool) {
	expr = stripParens(expr)
	switch len(expr) {
	case 1:
		switch expr[0].kind {
		case tokIdent:
			switch expr[0].up {
			case "TRUE", "FALSE", "NULL":
				return "", false
			}
			return expr[0].raw, true
		case tokQuoted:
			return expr[0].val, true
		}
	case 3:
		if expr[1].kind == tokOp && expr[1].raw == "." {
			last := expr[2]
			if last.kind == tokIdent {
				return last.raw, true
			}
			if last.kind == tokQuoted {
				return last.val, true
			}
		}
	}
	return "", false
}

// lastTopLevelAs returns the index of the last top-level AS in item, or -1.
func lastTopLevelAs(item []token) int {
	found := -1
	depth := 0
	for i, tk := range item {
		if tk.kind == tokOp {
			switch tk.raw {
			case "(":
				depth++
			case ")":
				depth--
			}
			continue
		}
		if depth == 0 && tk.kind == tokIdent && tk.up == "AS" {
			found = i
		}
	}
	return found
}

// inferExprType returns the BigQuery type of a select-list expression, or ""
// when it cannot be inferred statically.
func inferExprType(expr []token, fields map[string]Field) string {
	expr = stripParens(expr)
	if len(expr) == 0 {
		return ""
	}

	// Operators at the top level determine the result type.
	if hasTopLevelBoolOp(expr) {
		return "BOOL"
	}
	if hasTopLevelOp(expr, "/") {
		return "FLOAT64"
	}
	if hasTopLevelOp(expr, "||") {
		return "STRING"
	}
	if hasTopLevelOp(expr, "+") || hasTopLevelOp(expr, "-") || hasTopLevelOp(expr, "*") {
		return numericExprType(expr, fields)
	}

	// Function call.
	if expr[0].kind == tokIdent && len(expr) > 1 && expr[1].kind == tokOp && expr[1].raw == "(" {
		return inferFunctionType(expr, fields)
	}

	// Column reference (possibly table-qualified).
	if col, ok := simpleColumnRef(expr); ok {
		return columnType(col, fields)
	}

	// Literal.
	switch expr[0].kind {
	case tokNumber:
		if strings.ContainsAny(expr[0].raw, ".eE") {
			return "FLOAT64"
		}
		return "INT64"
	case tokString:
		return "STRING"
	case tokIdent:
		switch expr[0].up {
		case "TRUE", "FALSE":
			return "BOOL"
		}
	}
	return ""
}

// inferFunctionType maps a supported function call to its result type.
func inferFunctionType(expr []token, fields map[string]Field) string {
	name := expr[0].up
	close := matchParen(expr, 1)
	if close < 0 {
		return ""
	}
	args := splitTopLevel(expr[2:close], ",")
	argType := func(i int) string {
		if i < len(args) {
			return inferExprType(args[i], fields)
		}
		return ""
	}
	switch name {
	case "COUNT", "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "LENGTH", "INSTR":
		return "INT64"
	case "PERCENT_RANK", "CUME_DIST", "AVG":
		return "FLOAT64"
	case "SUM":
		if isFloatType(argType(0)) {
			return "FLOAT64"
		}
		return "INT64"
	case "MIN", "MAX", "ABS", "ROUND", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE":
		return argType(0)
	case "UPPER", "LOWER", "SUBSTR", "TRIM", "LTRIM", "RTRIM", "REPLACE":
		return "STRING"
	case "COALESCE", "NULLIF", "IFNULL":
		for i := range args {
			if t := argType(i); t != "" {
				return t
			}
		}
	case "CAST":
		if len(args) > 0 {
			inner := args[0]
			for i := 0; i+1 < len(inner); i++ {
				if inner[i].kind == tokIdent && inner[i].up == "AS" && inner[i+1].kind == tokIdent {
					return normalizeTypeName(inner[i+1].raw)
				}
			}
		}
	}
	return ""
}

// numericExprType returns FLOAT64 when any operand is floating, else INT64.
func numericExprType(expr []token, fields map[string]Field) string {
	depth := 0
	for i := 0; i < len(expr); i++ {
		tk := expr[i]
		if tk.kind == tokOp {
			switch tk.raw {
			case "(":
				depth++
			case ")":
				depth--
			}
			continue
		}
		var t string
		switch tk.kind {
		case tokNumber:
			if strings.ContainsAny(tk.raw, ".eE") {
				t = "FLOAT64"
			} else {
				t = "INT64"
			}
		case tokQuoted:
			t = columnType(tk.val, fields)
		case tokIdent:
			if i+1 < len(expr) && expr[i+1].kind == tokOp && expr[i+1].raw == "(" {
				t = inferFunctionType(expr[i:], fields)
			} else {
				t = columnType(tk.raw, fields)
			}
		}
		if isFloatType(t) {
			return "FLOAT64"
		}
	}
	return "INT64"
}

// isFloatType reports whether a BigQuery type name is floating point. Field
// types may arrive in either the engine-canonical form (FLOAT64) or the
// Discovery/TableFieldSchema form (FLOAT) when a table schema is read back from
// the store, so both must be recognized when promoting an arithmetic
// expression.
func isFloatType(t string) bool {
	switch strings.ToUpper(t) {
	case "FLOAT64", "FLOAT", "REAL", "DOUBLE",
		"NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL":
		return true
	}
	return false
}

// columnType resolves a (possibly qualified) column name against the hydrated
// declared fields.
func columnType(name string, fields map[string]Field) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	if f, ok := fields[strings.ToLower(name)]; ok {
		return f.Type
	}
	return ""
}

func hasTopLevelOp(expr []token, op string) bool {
	depth := 0
	for _, tk := range expr {
		if tk.kind != tokOp {
			continue
		}
		switch tk.raw {
		case "(":
			depth++
		case ")":
			depth--
		default:
			if depth == 0 && tk.raw == op {
				return true
			}
		}
	}
	return false
}

func hasTopLevelBoolOp(expr []token) bool {
	boolOps := map[string]bool{
		"=": true, "<": true, ">": true, "<=": true, ">=": true, "<>": true, "!=": true,
	}
	boolWords := map[string]bool{
		"AND": true, "OR": true, "NOT": true, "LIKE": true, "IN": true, "BETWEEN": true, "IS": true,
	}
	depth := 0
	for _, tk := range expr {
		switch tk.kind {
		case tokOp:
			switch tk.raw {
			case "(":
				depth++
			case ")":
				depth--
			default:
				if depth == 0 && boolOps[tk.raw] {
					return true
				}
			}
		case tokIdent:
			if depth == 0 && boolWords[tk.up] {
				return true
			}
		}
	}
	return false
}

// stripParens removes redundant outer parentheses.
func stripParens(expr []token) []token {
	for len(expr) >= 2 && expr[0].kind == tokOp && expr[0].raw == "(" &&
		matchParen(expr, 0) == len(expr)-1 {
		expr = expr[1 : len(expr)-1]
	}
	return expr
}
