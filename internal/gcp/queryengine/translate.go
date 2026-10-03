package queryengine

import (
	"fmt"
	"strconv"
	"strings"
)

// --- Tokenizer ---
//
// The translator works on a small token stream rather than raw text so that
// '/'-to-float rewriting never touches string literals or comments, and table
// references can be detected positionally (after FROM/JOIN) instead of by a
// fragile regexp.

type tokKind uint8

const (
	tokIdent  tokKind = iota // unquoted identifier / keyword
	tokQuoted                // `backtick` quoted identifier (or table reference)
	tokString                // '...' or "..." string literal
	tokNumber                // numeric literal
	tokOp                    // operator / punctuation
)

type token struct {
	kind tokKind
	raw  string // exact source text (identifiers, strings, numbers, operators)
	val  string // decoded value for a quoted identifier
	up   string // upper-cased raw for identifiers
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool { return isIdentStart(c) || (c >= '0' && c <= '9') }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func tokenize(q string) ([]token, error) {
	var toks []token
	i, n := 0, len(q)
	for i < n {
		c := q[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < n && q[i+1] == '-':
			for i < n && q[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && q[i+1] == '*':
			j := strings.Index(q[i+2:], "*/")
			if j < 0 {
				return nil, unsupported("unterminated block comment")
			}
			i += 2 + j + 2
		case c == '\'' || c == '"':
			quote := c
			// Triple-quoted literals are a BigQuery extension v1 does not
			// support; reject rather than mis-tokenize.
			if i+2 < n && q[i+1] == quote && q[i+2] == quote {
				return nil, unsupported("triple-quoted string literals are not supported")
			}
			start := i
			i++
			var inner strings.Builder
			closed := false
			for i < n {
				if q[i] == '\\' {
					dec, adv, err := decodeEscape(q[i:])
					if err != nil {
						return nil, err
					}
					inner.WriteString(dec)
					i += adv
					continue
				}
				if q[i] == quote {
					if i+1 < n && q[i+1] == quote { // doubled quote escape
						inner.WriteByte(quote)
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				inner.WriteByte(q[i])
				i++
			}
			if !closed {
				return nil, unsupported("unterminated string literal")
			}
			// Keep the raw text for diagnostics; val holds the decoded bytes.
			toks = append(toks, token{kind: tokString, raw: q[start:i], val: inner.String()})
		case c == '`':
			j := i + 1
			for j < n && q[j] != '`' {
				j++
			}
			if j >= n {
				return nil, unsupported("unterminated backquoted identifier")
			}
			toks = append(toks, token{kind: tokQuoted, raw: q[i : j+1], val: q[i+1 : j]})
			i = j + 1
		case isIdentStart(c):
			j := i + 1
			for j < n && isIdentPart(q[j]) {
				j++
			}
			w := q[i:j]
			toks = append(toks, token{kind: tokIdent, raw: w, up: strings.ToUpper(w)})
			i = j
		case isDigit(c) || (c == '.' && i+1 < n && isDigit(q[i+1])):
			j := i
			for j < n && (isDigit(q[j]) || q[j] == '.') {
				j++
			}
			if j < n && (q[j] == 'e' || q[j] == 'E') {
				j++
				if j < n && (q[j] == '+' || q[j] == '-') {
					j++
				}
				for j < n && isDigit(q[j]) {
					j++
				}
			}
			toks = append(toks, token{kind: tokNumber, raw: q[i:j]})
			i = j
		default:
			if i+1 < n {
				switch q[i : i+2] {
				case "<=", ">=", "<>", "!=", "||", "<<", ">>":
					toks = append(toks, token{kind: tokOp, raw: q[i : i+2]})
					i += 2
					continue
				}
			}
			toks = append(toks, token{kind: tokOp, raw: string(c)})
			i++
		}
	}
	return toks, nil
}

// --- Validation ---

// rejectedKeywords are reserved words whose presence means the query uses a
// feature outside the v1 subset. Rejecting the whole statement is the safe
// behavior: these constructs (arrays/structs, scripting, DDL/DML, wildcard
// tables, QUALIFY, unsupported joins/set operators) cannot be translated
// faithfully.
var rejectedKeywords = map[string]bool{
	// nested / semi-structured types
	"UNNEST": true, "ARRAY": true, "STRUCT": true, "GEOGRAPHY": true,
	"GEOG": true, "JSON": true,
	// advanced clauses
	"QUALIFY": true, "PIVOT": true, "UNPIVOT": true, "TABLESAMPLE": true,
	"WINDOW": true, "OVERWRITE": true, "VALUES": true, "LATERAL": true,
	// DDL/DML
	"CREATE": true, "DROP": true, "ALTER": true, "INSERT": true, "UPDATE": true,
	"DELETE": true, "MERGE": true, "TRUNCATE": true, "GRANT": true, "REVOKE": true,
	// scripting / session
	"CALL": true, "EXECUTE": true, "DECLARE": true, "BEGIN": true,
	"COMMIT": true, "ROLLBACK": true, "ASSERT": true, "RAISE": true, "SET": true,
	"IF": true, "WHILE": true, "FOR": true, "LOOP": true, "LOAD": true,
	"EXPORT": true,
	// metadata / wildcard tables
	"INFORMATION_SCHEMA": true, "TABLE_SUFFIX": true,
	// joins + set operators outside the subset
	"RIGHT": true, "FULL": true, "CROSS": true, "EXCEPT": true, "INTERSECT": true,
	"DIV": true, "SAFE_OFFSET": true, "SAFE_ORDINAL": true,
	"RECURSIVE": true,
}

// dmlKeywords are reserved words the DML translator accepts (they are still
// rejected for a SELECT, where they cannot appear). They must also be skipped
// by the function allow-list check (for example "VALUES (").
var dmlKeywords = map[string]bool{
	"INSERT": true, "INTO": true, "UPDATE": true, "DELETE": true,
	"SET": true, "VALUES": true,
}

// keywords are reserved words that are never function calls, so the function
// allow-list check must skip them (for example "IN (...)").
var keywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "GROUP": true, "BY": true,
	"HAVING": true, "ORDER": true, "LIMIT": true, "OFFSET": true, "AND": true,
	"OR": true, "NOT": true, "AS": true, "ON": true, "JOIN": true, "INNER": true,
	"LEFT": true, "OUTER": true, "UNION": true, "ALL": true, "DISTINCT": true,
	"IN": true, "IS": true, "NULL": true, "TRUE": true, "FALSE": true,
	"CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
	"BETWEEN": true, "LIKE": true, "EXISTS": true, "CAST": true, "OVER": true,
	"PARTITION": true, "ROWS": true, "RANGE": true, "UNBOUNDED": true,
	"PRECEDING": true, "FOLLOWING": true, "CURRENT": true, "ROW": true,
	"WITH": true, "ASC": true, "DESC": true, "NULLS": true, "FIRST": true,
	"LAST": true, "USING": true, "INTERVAL": true, "ESCAPE": true,
}

// allowedFunctions are the scalar/aggregate/window functions that map 1:1 (or
// near enough) onto SQLite. Any other function call fails loud so the engine
// never silently diverges from BigQuery semantics.
var allowedFunctions = map[string]bool{
	// aggregates
	"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
	// window functions
	"ROW_NUMBER": true, "RANK": true, "DENSE_RANK": true, "PERCENT_RANK": true,
	"CUME_DIST": true, "NTILE": true, "LAG": true, "LEAD": true,
	"FIRST_VALUE": true, "LAST_VALUE": true, "NTH_VALUE": true,
	// string
	"LENGTH": true, "UPPER": true, "LOWER": true, "SUBSTR": true, "TRIM": true,
	"LTRIM": true, "RTRIM": true, "REPLACE": true, "INSTR": true,
	// math / null
	"ABS": true, "ROUND": true, "COALESCE": true, "NULLIF": true, "IFNULL": true,
}

// typeKeywords are BigQuery type names that introduce a typed literal when
// followed by a string ("DATE '2020-01-01'"), which SQLite cannot parse.
var typeKeywords = map[string]bool{
	"DATE": true, "DATETIME": true, "TIME": true, "TIMESTAMP": true,
	"NUMERIC": true, "BIGNUMERIC": true, "BYTES": true,
}

func validate(toks []token, dml bool) error {
	for i, tk := range toks {
		if tk.kind == tokOp && (tk.raw == "@" || tk.raw == "?") {
			return unsupported("query parameters are not supported")
		}
		if tk.kind != tokIdent {
			continue
		}
		if rejectedKeywords[tk.up] && !(dml && dmlKeywords[tk.up]) {
			return unsupported(fmt.Sprintf("%s is not supported", tk.up))
		}
		// Raw/bytes string prefixes (r'…', b'…', rb'…') are not supported.
		if (tk.up == "R" || tk.up == "B" || tk.up == "RB" || tk.up == "BR") &&
			i+1 < len(toks) && toks[i+1].kind == tokString {
			return unsupported(fmt.Sprintf("%s string literals are not supported", tk.up))
		}
		if typeKeywords[tk.up] && i+1 < len(toks) && toks[i+1].kind == tokString {
			return unsupported(fmt.Sprintf("%s literals are not supported", tk.up))
		}
		if i+1 < len(toks) && toks[i+1].kind == tokOp && toks[i+1].raw == "(" {
			if !keywords[tk.up] && !allowedFunctions[tk.up] && !(dml && dmlKeywords[tk.up]) {
				return unsupported(fmt.Sprintf("function %s is not supported", tk.raw))
			}
		}
	}
	return nil
}

// --- CAST type rewriting ---

// rewriteCastTypes rewrites the type name inside every CAST(expr AS type) to a
// SQLite affinity and strips any type parameters (NUMERIC(10,2) -> REAL). It
// runs before the main rewrite so CAST can be treated as a normal keyword.
func rewriteCastTypes(toks []token) ([]token, error) {
	out := make([]token, len(toks))
	copy(out, toks)
	for i := 0; i < len(out); i++ {
		if out[i].kind != tokIdent || out[i].up != "CAST" {
			continue
		}
		if i+1 >= len(out) || out[i+1].kind != tokOp || out[i+1].raw != "(" {
			continue
		}
		depth, asIdx, closeIdx := 0, -1, -1
		for j := i + 1; j < len(out); j++ {
			if out[j].kind == tokOp {
				switch out[j].raw {
				case "(":
					depth++
				case ")":
					depth--
					if depth == 0 {
						closeIdx = j
					}
				}
			} else if out[j].kind == tokIdent && out[j].up == "AS" && depth == 1 {
				asIdx = j
			}
			if closeIdx >= 0 {
				break
			}
		}
		if closeIdx < 0 {
			return nil, unsupported("unbalanced CAST expression")
		}
		if asIdx < 0 || asIdx+1 >= closeIdx {
			return nil, unsupported("CAST without a type")
		}
		typTok := out[asIdx+1]
		if typTok.kind != tokIdent {
			return nil, unsupported("unsupported CAST type")
		}
		mapped, err := sqliteCastType(typTok.up)
		if err != nil {
			return nil, err
		}
		out[asIdx+1] = token{kind: tokIdent, raw: mapped, up: strings.ToUpper(mapped)}
		// Strip type parameters: CAST(x AS NUMERIC(10,2)).
		if asIdx+2 < len(out) && out[asIdx+2].kind == tokOp && out[asIdx+2].raw == "(" {
			k := asIdx + 2
			pd := 0
			for ; k < closeIdx; k++ {
				if out[k].kind == tokOp && out[k].raw == "(" {
					pd++
				} else if out[k].kind == tokOp && out[k].raw == ")" {
					pd--
					if pd == 0 {
						break
					}
				}
			}
			// Blank out the parameter tokens in [asIdx+2, k].
			for m := asIdx + 2; m <= k && m < len(out); m++ {
				out[m] = token{kind: tokOp, raw: ""}
			}
		}
		i = closeIdx - 1
	}
	return out, nil
}

func sqliteCastType(bqType string) (string, error) {
	switch bqType {
	case "INT64", "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "BYTEINT":
		return "INTEGER", nil
	case "FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL",
		"REAL", "DOUBLE":
		return "REAL", nil
	case "STRING", "DATE", "DATETIME", "TIME", "TIMESTAMP":
		return "TEXT", nil
	case "BYTES":
		return "BLOB", nil
	case "BOOL", "BOOLEAN":
		return "INTEGER", nil
	}
	return "", unsupported(fmt.Sprintf("cast type %s is not supported", bqType))
}

// --- Translation ---

// resolve maps a table reference (1-3 dot-separated parts, unquoted or from a
// backquoted identifier) to the internal SQLite table name the engine hydrated.
type resolveFunc func(ref string) (string, error)

// translate rewrites a BigQuery-subset SELECT to SQLite SQL:
//   - table references become the hydrated internal table names via resolve;
//   - the binary '/' operator becomes '* 1.0 /' so integer operands divide as
//     FLOAT64 (BigQuery '/' is float division; SQLite's '/' is integer division);
//   - backquoted identifiers that are not table references are emitted as
//     quoted SQLite identifiers.
//
// It never rewrites string literals or comments (the tokenizer drops those).
// translate rewrites a BigQuery-subset SELECT to SQLite SQL. It is the
// SELECT-only entry point; DML statements use translateTokens with dml=true.
func translate(query string, resolve resolveFunc) (string, error) {
	toks, err := tokenize(query)
	if err != nil {
		return "", err
	}
	return translateTokens(toks, resolve, false)
}

// translateTokens rewrites a tokenized BigQuery-subset statement to SQLite SQL.
// With dml=true the INSERT/UPDATE target positions (INTO/UPDATE) are resolved as
// table references as well, and the DML keywords are accepted.
func translateTokens(toks []token, resolve resolveFunc, dml bool) (string, error) {
	var err error
	// Rewrite CAST types before validation so a BigQuery cast type with
	// parameters (NUMERIC(10,2)) is not mistaken for a function call.
	toks, err = rewriteCastTypes(toks)
	if err != nil {
		return "", err
	}
	if err := validate(toks, dml); err != nil {
		return "", err
	}

	// CTE names look like table references in the main query but are not
	// catalog tables; resolve() must leave them alone.
	var ctes map[string]bool
	if !dml {
		ctes = collectCTENames(toks)
	}
	resolveIfTable := func(ref string) (string, bool, error) {
		if !strings.Contains(ref, ".") && ctes[strings.ToLower(ref)] {
			return ref, true, nil
		}
		name, err := resolve(ref)
		return name, false, err
	}

	var b strings.Builder
	parenDepth, fromDepth := 0, 0
	inFrom, expectTable := false, false

	for i := 0; i < len(toks); i++ {
		tk := toks[i]
		if tk.kind == tokIdent {
			stateKeyword := false
			switch tk.up {
			case "FROM":
				inFrom, fromDepth, expectTable, stateKeyword = true, parenDepth, true, true
			case "JOIN":
				expectTable, stateKeyword = true, true
			case "INTO", "UPDATE":
				if dml {
					expectTable, stateKeyword = true, true
				}
			case "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "SELECT", "ON":
				inFrom, expectTable, stateKeyword = false, false, true
			}
			if expectTable && !stateKeyword {
				if i+1 < len(toks) && toks[i+1].kind == tokOp && toks[i+1].raw == "(" {
					return "", unsupported("table-valued functions and subqueries are not supported in this position")
				}
				ref, last, ok := collectTableRef(toks, i)
				if ok {
					name, isCTE, err := resolveIfTable(ref)
					if err != nil {
						return "", err
					}
					if isCTE {
						writeToken(&b, tk)
					} else {
						writeToken(&b, token{kind: tokIdent, raw: name})
					}
					i = last
					expectTable = false
					continue
				}
			}
			writeToken(&b, tk)
			continue
		}
		if expectTable && tk.kind == tokQuoted {
			ref, last, ok := collectTableRef(toks, i)
			if !ok {
				writeToken(&b, tk)
				continue
			}
			name, isCTE, err := resolveIfTable(ref)
			if err != nil {
				return "", err
			}
			if isCTE {
				writeToken(&b, tk)
			} else {
				writeToken(&b, token{kind: tokIdent, raw: name})
			}
			i = last
			expectTable = false
			continue
		}
		if tk.kind == tokOp {
			switch tk.raw {
			case "(":
				parenDepth++
			case ")":
				if parenDepth > 0 {
					parenDepth--
				}
				if inFrom && parenDepth < fromDepth {
					inFrom, expectTable = false, false
				}
			case ",":
				if inFrom && parenDepth == fromDepth {
					expectTable = true
				}
			case "/":
				// BigQuery '/' is FLOAT64 division; '* 1.0' forces float in
				// SQLite without changing operator precedence or associativity.
				if b.Len() > 0 {
					b.WriteByte(' ')
				}
				b.WriteString("* 1.0 /")
				continue
			case "":
				continue // blanked CAST type parameter
			}
			writeToken(&b, tk)
			continue
		}
		writeToken(&b, tk)
	}
	return b.String(), nil
}

// collectCTENames returns the lower-cased names declared by a leading WITH
// clause (WITH a AS (...), b AS (...) SELECT ...). The engine must not try to
// resolve those names through the catalog.
func collectCTENames(toks []token) map[string]bool {
	names := map[string]bool{}
	depth := 0
	withIdx := -1
	for i := 0; i < len(toks); i++ {
		if toks[i].kind == tokOp {
			switch toks[i].raw {
			case "(":
				depth++
			case ")":
				depth--
			}
		}
		if depth == 0 && toks[i].kind == tokIdent && toks[i].up == "WITH" {
			withIdx = i
			break
		}
	}
	if withIdx < 0 {
		return names
	}
	j := withIdx + 1
	for j < len(toks) {
		nameTok := toks[j]
		if nameTok.kind != tokIdent && nameTok.kind != tokQuoted {
			break
		}
		if j+1 >= len(toks) || toks[j+1].kind != tokIdent || toks[j+1].up != "AS" {
			break
		}
		name := nameTok.raw
		if nameTok.kind == tokQuoted {
			name = nameTok.val
		}
		names[strings.ToLower(name)] = true
		k := j + 2
		if k >= len(toks) || toks[k].kind != tokOp || toks[k].raw != "(" {
			break
		}
		d := 0
		for ; k < len(toks); k++ {
			if toks[k].kind != tokOp {
				continue
			}
			if toks[k].raw == "(" {
				d++
			} else if toks[k].raw == ")" {
				d--
				if d == 0 {
					break
				}
			}
		}
		// k is the CTE body's closing paren; a following comma starts another.
		if k+1 < len(toks) && toks[k+1].kind == tokOp && toks[k+1].raw == "," {
			j = k + 2
			continue
		}
		break
	}
	return names
}

// collectTableRef reads a table reference starting at token i (an identifier or
// a backquoted identifier) and returns the dotted reference plus the index of
// its last token. A backquoted identifier is a reference only in table position
// (the caller already checked); its value may still contain dots (a
// fully-qualified `project.dataset.table`) or be a plain table name.
func collectTableRef(toks []token, i int) (string, int, bool) {
	first := toks[i]
	if first.kind != tokIdent && first.kind != tokQuoted {
		return "", i, false
	}
	part := func(t token) string {
		if t.kind == tokQuoted {
			return t.val
		}
		return t.raw
	}
	parts := []string{part(first)}
	last := i
	for last+2 < len(toks) && toks[last+1].kind == tokOp && toks[last+1].raw == "." {
		nxt := toks[last+2]
		if nxt.kind == tokIdent || nxt.kind == tokQuoted {
			parts = append(parts, part(nxt))
			last += 2
			continue
		}
		break
	}
	return strings.Join(parts, "."), last, true
}

func writeToken(b *strings.Builder, tk token) {
	if tk.raw == "" {
		return
	}
	if b.Len() > 0 {
		b.WriteByte(' ')
	}
	switch tk.kind {
	case tokQuoted:
		b.WriteString(quoteSQLiteIdent(tk.val))
	case tokString:
		// Re-emit as a SQLite single-quoted literal built from the decoded
		// value, so BigQuery backslash escapes can never terminate the SQLite
		// string early (and a double-quoted BigQuery string is not mistaken for
		// a SQLite identifier).
		b.WriteString(sqliteStringLiteral(tk.val))
	default:
		b.WriteString(tk.raw)
	}
}

// decodeEscape decodes one BigQuery string escape sequence starting at the
// backslash. It returns the decoded text and the number of source bytes
// consumed. Unknown escapes fail loud.
func decodeEscape(s string) (string, int, error) {
	if len(s) < 2 {
		return "", 0, unsupported("unterminated string escape")
	}
	switch s[1] {
	case 'a':
		return "\a", 2, nil
	case 'b':
		return "\b", 2, nil
	case 'f':
		return "\f", 2, nil
	case 'n':
		return "\n", 2, nil
	case 'r':
		return "\r", 2, nil
	case 't':
		return "\t", 2, nil
	case 'v':
		return "\v", 2, nil
	case '\\', '?', '"', '\'', '`':
		return s[1:2], 2, nil
	case '\n':
		return "", 2, nil // line continuation
	case 'x', 'X':
		j := 2
		for j < len(s) && j < 4 && isHex(s[j]) {
			j++
		}
		if j == 2 {
			return "", 0, unsupported("invalid \\x escape")
		}
		n, _ := strconv.ParseUint(s[2:j], 16, 32)
		return string(rune(n)), j, nil
	case 'u', 'U':
		width := 4
		if s[1] == 'U' {
			width = 8
		}
		if len(s) < 2+width {
			return "", 0, unsupported("invalid unicode escape")
		}
		n, err := strconv.ParseUint(s[2:2+width], 16, 32)
		if err != nil {
			return "", 0, unsupported("invalid unicode escape")
		}
		return string(rune(n)), 2 + width, nil
	default:
		if s[1] >= '0' && s[1] <= '7' {
			j := 1
			for j < len(s) && j < 4 && s[j] >= '0' && s[j] <= '7' {
				j++
			}
			n, _ := strconv.ParseUint(s[1:j], 8, 32)
			return string(rune(n)), j, nil
		}
		return "", 0, unsupported(fmt.Sprintf("unsupported string escape \\%c", s[1]))
	}
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// sqliteStringLiteral renders a decoded value as a SQLite single-quoted string
// literal ('...' with embedded quotes doubled).
func sqliteStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// quoteSQLiteIdent quotes a (possibly dotted) identifier for SQLite, splitting
// on dots so `t.col` becomes "t"."col".
func quoteSQLiteIdent(s string) string {
	parts := strings.Split(s, ".")
	for i, p := range parts {
		parts[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}
