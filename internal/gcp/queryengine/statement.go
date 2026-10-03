package queryengine

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// classifyStatement returns the Discovery statementType of a tokenized
// statement, or "" when the engine does not execute it (MERGE, ALTER,
// scripting, CREATE VIEW/MODEL/FUNCTION, ...). CREATE TABLE is reported as
// CREATE_TABLE here; executeCreateTable upgrades it to
// CREATE_TABLE_AS_SELECT when the statement carries an AS query.
func classifyStatement(toks []token) string {
	if len(toks) == 0 || toks[0].kind != tokIdent {
		return ""
	}
	switch toks[0].up {
	case "SELECT", "WITH":
		return StatementSelect
	case "CREATE":
		i := 1
		if i+1 < len(toks) && toks[i].kind == tokIdent && toks[i].up == "OR" &&
			toks[i+1].kind == tokIdent && toks[i+1].up == "REPLACE" {
			i += 2
		}
		if i < len(toks) && toks[i].kind == tokIdent {
			switch toks[i].up {
			case "TABLE":
				return StatementCreateTable
			case "SCHEMA":
				return StatementCreateSchema
			}
		}
		return ""
	case "DROP":
		i := 1
		if i < len(toks) && toks[i].kind == tokIdent {
			switch toks[i].up {
			case "TABLE":
				return StatementDropTable
			case "SCHEMA":
				return StatementDropSchema
			}
		}
		return ""
	case "INSERT":
		return StatementInsert
	case "UPDATE":
		return StatementUpdate
	case "DELETE":
		return StatementDelete
	case "TRUNCATE":
		return StatementTruncateTable
	}
	return ""
}

// --- SELECT ---

func (h *hydrator) executeSelect(toks []token) (Result, error) {
	translated, err := translateTokens(toks, h.resolve, false)
	if err != nil {
		return Result{}, err
	}
	inferred := inferOutputFields(toks, h.fields)
	if h.dryRun {
		return Result{StatementType: StatementSelect, Fields: inferred}, nil
	}
	res, err := h.run(translated, inferred)
	if err != nil {
		return Result{}, err
	}
	res.StatementType = StatementSelect
	return res, nil
}

// --- DDL ---

func (h *hydrator) executeCreateTable(toks []token) (Result, error) {
	i := 1 // CREATE
	orReplace := false
	if i+1 < len(toks) && toks[i].kind == tokIdent && toks[i].up == "OR" &&
		toks[i+1].kind == tokIdent && toks[i+1].up == "REPLACE" {
		orReplace = true
		i += 2
	}
	if i >= len(toks) || toks[i].kind != tokIdent || toks[i].up != "TABLE" {
		return Result{}, unsupported("expected CREATE TABLE")
	}
	i++
	ifNotExists, i := parseIfNotExists(toks, i)

	ref, last, ok := collectTableRef(toks, i)
	if !ok {
		return Result{}, unsupported("CREATE TABLE requires a table name")
	}
	project, dataset, table, err := h.qualify(ref)
	if err != nil {
		return Result{}, err
	}
	i = last + 1

	var fields []Field
	if i < len(toks) && toks[i].kind == tokOp && toks[i].raw == "(" {
		end := matchParen(toks, i)
		if end < 0 {
			return Result{}, unsupported("unbalanced column list")
		}
		fields, err = parseColumnDefs(toks[i+1 : end])
		if err != nil {
			return Result{}, err
		}
		i = end + 1
	}

	asIdx := findTopLevelAsQuery(toks, i)
	// Reject any clause the engine does not model (PARTITION BY, CLUSTER BY,
	// OPTIONS, ...) rather than silently creating a plain table.
	if end := asIdx; i < end || (end < 0 && i < len(toks)) {
		return Result{}, unsupported("unsupported CREATE TABLE clause")
	}
	stmtType := StatementCreateTable
	var rows []map[string]any
	if asIdx >= 0 {
		stmtType = StatementCreateTableAsSelect
		selRes, err := h.executeSelect(toks[asIdx+1:])
		if err != nil {
			return Result{}, err
		}
		if len(fields) == 0 {
			fields = selRes.Fields
			rows = rowsFromResult(selRes)
		} else {
			// An explicit column list plus AS query maps by position, as
			// BigQuery does.
			if len(selRes.Fields) != len(fields) {
				return Result{}, unsupported("column list does not match the AS query result")
			}
			rows = rowsFromResultPositional(selRes, fields)
		}
	} else if len(fields) == 0 {
		return Result{}, unsupported("CREATE TABLE requires a column list or an AS query")
	}
	if h.dryRun {
		if _, err := h.cat.Table(h.ctx, project, dataset, table); err == nil && !orReplace && !ifNotExists {
			return Result{}, ErrTableExists
		}
		return Result{StatementType: stmtType, Fields: fields}, nil
	}

	if orReplace {
		// Ignore a missing target: CREATE OR REPLACE replaces it if present.
		_ = h.w.DropTable(h.ctx, project, dataset, table)
	}
	if err := h.w.CreateTable(h.ctx, Table{
		Project: project, Dataset: dataset, Table: table, Fields: fields, Rows: rows,
	}); err != nil {
		if ifNotExists && errors.Is(err, ErrTableExists) {
			return Result{StatementType: stmtType}, nil
		}
		return Result{}, ddlError(err)
	}
	return Result{StatementType: stmtType}, nil
}

func (h *hydrator) executeCreateSchema(toks []token) (Result, error) {
	i := 1 // CREATE
	if i >= len(toks) || toks[i].kind != tokIdent || toks[i].up != "SCHEMA" {
		return Result{}, unsupported("expected CREATE SCHEMA")
	}
	i++
	ifNotExists, i := parseIfNotExists(toks, i)
	ref, last, ok := collectTableRef(toks, i)
	if !ok {
		return Result{}, unsupported("CREATE SCHEMA requires a dataset name")
	}
	project, dataset, err := h.qualifyDataset(ref)
	if err != nil {
		return Result{}, err
	}
	// OPTIONS(...) and any other trailing clause are not modeled; fail loud
	// rather than silently ignoring a requested location/labels.
	if last+1 < len(toks) {
		return Result{}, unsupported("unsupported CREATE SCHEMA clause")
	}
	if h.dryRun {
		return Result{StatementType: StatementCreateSchema}, nil
	}
	if err := h.w.CreateDataset(h.ctx, project, dataset); err != nil {
		if ifNotExists && errors.Is(err, ErrDatasetExists) {
			return Result{StatementType: StatementCreateSchema}, nil
		}
		return Result{}, ddlError(err)
	}
	return Result{StatementType: StatementCreateSchema}, nil
}

func (h *hydrator) executeDropTable(toks []token) (Result, error) {
	i := 1 // DROP
	if i >= len(toks) || toks[i].kind != tokIdent || toks[i].up != "TABLE" {
		return Result{}, unsupported("expected DROP TABLE")
	}
	i++
	ifExists, i := parseIfExists(toks, i)
	ref, last, ok := collectTableRef(toks, i)
	if !ok {
		return Result{}, unsupported("DROP TABLE requires a table name")
	}
	project, dataset, table, err := h.qualify(ref)
	if err != nil {
		return Result{}, err
	}
	if last+1 < len(toks) {
		return Result{}, unsupported("unsupported DROP TABLE clause")
	}
	if h.dryRun {
		if _, err := h.cat.Table(h.ctx, project, dataset, table); err != nil && !ifExists {
			return Result{}, &TableNotFoundError{Project: project, Dataset: dataset, Table: table}
		}
		return Result{StatementType: StatementDropTable}, nil
	}
	if err := h.w.DropTable(h.ctx, project, dataset, table); err != nil {
		if errors.Is(err, ErrTableNotFound) {
			if ifExists {
				return Result{StatementType: StatementDropTable}, nil
			}
			return Result{}, &TableNotFoundError{Project: project, Dataset: dataset, Table: table}
		}
		return Result{}, ddlError(err)
	}
	return Result{StatementType: StatementDropTable}, nil
}

func (h *hydrator) executeDropSchema(toks []token) (Result, error) {
	i := 1 // DROP
	if i >= len(toks) || toks[i].kind != tokIdent || toks[i].up != "SCHEMA" {
		return Result{}, unsupported("expected DROP SCHEMA")
	}
	i++
	ifExists, i := parseIfExists(toks, i)
	ref, last, ok := collectTableRef(toks, i)
	if !ok {
		return Result{}, unsupported("DROP SCHEMA requires a dataset name")
	}
	project, dataset, err := h.qualifyDataset(ref)
	if err != nil {
		return Result{}, err
	}
	cascade := false
	for _, tk := range toks[last+1:] {
		if tk.kind != tokIdent {
			return Result{}, unsupported("unsupported DROP SCHEMA clause")
		}
		switch tk.up {
		case "CASCADE":
			cascade = true
		case "RESTRICT":
			// The default; the RESTRICT check below enforces it.
		default:
			return Result{}, unsupported("unsupported DROP SCHEMA clause")
		}
	}
	if h.dryRun {
		return Result{StatementType: StatementDropSchema}, nil
	}
	if !cascade {
		tables, err := h.w.ListTables(h.ctx, project, dataset)
		if err != nil && !errors.Is(err, ErrDatasetNotFound) {
			return Result{}, err
		}
		if len(tables) > 0 {
			return Result{}, unsupported("dataset " + dataset + " is not empty; use DROP SCHEMA ... CASCADE")
		}
	}
	if err := h.w.DropDataset(h.ctx, project, dataset); err != nil {
		if errors.Is(err, ErrDatasetNotFound) {
			if ifExists {
				return Result{StatementType: StatementDropSchema}, nil
			}
			return Result{}, &DatasetNotFoundError{Project: project, Dataset: dataset}
		}
		return Result{}, ddlError(err)
	}
	return Result{StatementType: StatementDropSchema}, nil
}

// --- DML ---

func (h *hydrator) executeDML(typ string, toks []token) (Result, error) {
	project, dataset, table, internal, err := h.dmlTarget(typ, toks)
	if err != nil {
		return Result{}, err
	}
	var translated string
	if typ == StatementTruncateTable {
		translated = "DELETE FROM " + quoteSQLiteIdent(internal)
	} else {
		translated, err = translateTokens(toks, h.resolve, true)
		if err != nil {
			return Result{}, err
		}
	}
	// The DML runs against the disposable scratch even for a dry run, so an
	// unknown column or bad expression fails loud exactly as a real run would;
	// only the store write is skipped.
	res, err := h.db.ExecContext(h.ctx, translated)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Result{}, err
		}
		return Result{}, unsupported(err.Error())
	}
	affected, _ := res.RowsAffected()
	if h.dryRun {
		return Result{StatementType: typ, NumDMLAffectedRows: affected}, nil
	}
	rows, err := h.readInternalTable(internal)
	if err != nil {
		return Result{}, err
	}
	if err := h.w.ReplaceRows(h.ctx, project, dataset, table, rows); err != nil {
		return Result{}, ddlError(err)
	}
	return Result{StatementType: typ, NumDMLAffectedRows: affected}, nil
}

// dmlTarget resolves the row-mutated table of a DML statement and returns its
// fully-qualified name plus its internal scratch table name.
func (h *hydrator) dmlTarget(typ string, toks []token) (project, dataset, table, internal string, err error) {
	want := func(i int, kw string) bool {
		return i < len(toks) && toks[i].kind == tokIdent && toks[i].up == kw
	}
	var at int
	switch typ {
	case StatementInsert:
		if !want(1, "INTO") {
			return "", "", "", "", unsupported("INSERT requires INTO")
		}
		at = 2 // INSERT INTO <ref>
	case StatementUpdate:
		at = 1 // UPDATE <ref>
	case StatementDelete:
		if !want(1, "FROM") {
			return "", "", "", "", unsupported("DELETE requires FROM")
		}
		at = 2 // DELETE FROM <ref>
	case StatementTruncateTable:
		if !want(1, "TABLE") {
			return "", "", "", "", unsupported("TRUNCATE requires TABLE")
		}
		at = 2 // TRUNCATE TABLE <ref>
	default:
		return "", "", "", "", unsupported("unsupported DML statement")
	}
	ref, _, ok := collectTableRef(toks, at)
	if !ok {
		return "", "", "", "", unsupported(typ + " requires a table name")
	}
	p, d, t, err := h.qualify(ref)
	if err != nil {
		return "", "", "", "", err
	}
	internal, err = h.resolve(ref)
	if err != nil {
		return "", "", "", "", err
	}
	return p, d, t, internal, nil
}

// readInternalTable reads every row of an internal scratch table back into a
// JSON-shaped row map (values normalized to their BigQuery representation) so
// the mutated table can be written back to the store.
func (h *hydrator) readInternalTable(name string) ([]map[string]any, error) {
	rows, err := h.db.QueryContext(h.ctx, "SELECT * FROM "+quoteSQLiteIdent(name))
	if err != nil {
		return nil, unsupported(err.Error())
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	fields := h.tableFields[name]
	out := make([]map[string]any, 0)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			var f Field
			if i < len(fields) {
				f = fields[i]
			}
			m[c] = normalizeValue(f, vals[i])
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// --- parsing helpers ---

// parseIfNotExists consumes an optional IF NOT EXISTS sequence and returns
// whether it was present plus the next token index.
func parseIfNotExists(toks []token, i int) (bool, int) {
	if i+2 < len(toks) && toks[i].kind == tokIdent && toks[i].up == "IF" &&
		toks[i+1].kind == tokIdent && toks[i+1].up == "NOT" &&
		toks[i+2].kind == tokIdent && toks[i+2].up == "EXISTS" {
		return true, i + 3
	}
	return false, i
}

// parseIfExists consumes an optional IF EXISTS sequence.
func parseIfExists(toks []token, i int) (bool, int) {
	if i+1 < len(toks) && toks[i].kind == tokIdent && toks[i].up == "IF" &&
		toks[i+1].kind == tokIdent && toks[i+1].up == "EXISTS" {
		return true, i + 2
	}
	return false, i
}

// matchParen returns the index of the ')' matching the '(' at open, or -1.
func matchParen(toks []token, open int) int {
	depth := 0
	for i := open; i < len(toks); i++ {
		if toks[i].kind != tokOp {
			continue
		}
		switch toks[i].raw {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// parseColumnDefs parses a CREATE TABLE column list (name type [NOT NULL], ...).
// Nested/REPEATED types are rejected (v1 subset).
func parseColumnDefs(inner []token) ([]Field, error) {
	items := splitTopLevel(inner, ",")
	fields := make([]Field, 0, len(items))
	for _, item := range items {
		if len(item) == 0 {
			continue
		}
		if item[0].kind != tokIdent && item[0].kind != tokQuoted {
			return nil, unsupported("invalid column definition")
		}
		name := item[0].raw
		if item[0].kind == tokQuoted {
			name = item[0].val
		}
		if len(item) < 2 || item[1].kind != tokIdent {
			return nil, unsupported("column " + name + " is missing a type")
		}
		typ := normalizeTypeName(item[1].up)
		if isNestedType(typ) {
			return nil, unsupported(typ + " columns are not supported")
		}
		mode := "NULLABLE"
		for j := 2; j+1 < len(item); j++ {
			if item[j].kind == tokIdent && item[j].up == "NOT" &&
				item[j+1].kind == tokIdent && item[j+1].up == "NULL" {
				mode = "REQUIRED"
			}
		}
		fields = append(fields, Field{Name: name, Type: typ, Mode: mode})
	}
	if len(fields) == 0 {
		return nil, unsupported("empty column list")
	}
	return fields, nil
}

// findTopLevelAsQuery returns the index of a top-level AS followed by a query
// (SELECT/WITH), or -1. It marks the AS query of CREATE TABLE ... AS SELECT.
func findTopLevelAsQuery(toks []token, from int) int {
	depth := 0
	for i := from; i < len(toks); i++ {
		if toks[i].kind == tokOp {
			switch toks[i].raw {
			case "(":
				depth++
			case ")":
				depth--
			}
			continue
		}
		if depth == 0 && toks[i].kind == tokIdent && toks[i].up == "AS" &&
			i+1 < len(toks) && toks[i+1].kind == tokIdent &&
			(toks[i+1].up == "SELECT" || toks[i+1].up == "WITH") {
			return i
		}
	}
	return -1
}

// splitTopLevel splits toks on the given separator operator at paren depth 0.
func splitTopLevel(toks []token, sep string) [][]token {
	var out [][]token
	depth := 0
	start := 0
	for i, tk := range toks {
		if tk.kind == tokOp {
			switch tk.raw {
			case "(":
				depth++
			case ")":
				depth--
			case sep:
				if depth == 0 {
					out = append(out, toks[start:i])
					start = i + 1
				}
			}
		}
	}
	out = append(out, toks[start:])
	return out
}

// rowsFromResult converts an executed SELECT result into JSON-shaped row maps
// keyed by output field name.
func rowsFromResult(res Result) []map[string]any {
	return rowsFromResultPositional(res, res.Fields)
}

// rowsFromResultPositional maps an AS query result onto a target field list by
// position (used by CTAS `CREATE TABLE t (a INT64) AS SELECT x`).
func rowsFromResultPositional(res Result, fields []Field) []map[string]any {
	out := make([]map[string]any, 0, len(res.Rows))
	for _, r := range res.Rows {
		m := make(map[string]any, len(fields))
		for i, f := range fields {
			if i < len(r) {
				m[f.Name] = r[i]
			}
		}
		out = append(out, m)
	}
	return out
}

// ddlError maps a mutator error for a DDL statement: the sentinel errors flow
// through so the provider can map them to 404/409; anything else is an
// unsupported construct, never a silent success.
func ddlError(err error) error {
	if errors.Is(err, ErrTableExists) || errors.Is(err, ErrDatasetExists) ||
		errors.Is(err, ErrTableNotFound) || errors.Is(err, ErrDatasetNotFound) {
		return err
	}
	return unsupported(err.Error())
}

// normalizeTypeName maps a BigQuery type name to the engine's canonical name.
func normalizeTypeName(t string) string {
	switch strings.ToUpper(t) {
	case "INT64", "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "BYTEINT":
		return "INT64"
	case "FLOAT64", "FLOAT", "DOUBLE", "REAL":
		return "FLOAT64"
	case "NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL":
		return "NUMERIC"
	case "BOOL", "BOOLEAN":
		return "BOOL"
	}
	return strings.ToUpper(t)
}

// qualifyDataset resolves a schema reference (dataset, or project.dataset).
func (h *hydrator) qualifyDataset(ref string) (project, dataset string, err error) {
	parts := strings.Split(ref, ".")
	switch len(parts) {
	case 1:
		project, dataset = h.defaultProject(), parts[0]
	case 2:
		project, dataset = parts[0], parts[1]
	default:
		return "", "", unsupported(fmt.Sprintf("dataset reference %q is not supported", ref))
	}
	if project == "" || dataset == "" {
		return "", "", unsupported(fmt.Sprintf("dataset reference %q is not fully qualified", ref))
	}
	for _, p := range parts {
		if strings.EqualFold(p, "INFORMATION_SCHEMA") {
			return "", "", unsupported("INFORMATION_SCHEMA is not supported")
		}
	}
	return project, dataset, nil
}
