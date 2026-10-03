package hms

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"

	"jaiscloud/internal/clock"
	hmsstore "jaiscloud/internal/gcp/store/hms"
)

// Partition methods for the Hive Metastore serving plane. The handler layer
// mirrors the database/table handlers: a Partition is decoded generically and
// its full canonical JSON is stored (get_partition -> alter_partition is a
// lossless full-struct round-trip), while the ordered value tuple is the store
// key. Exception result-struct positions are declared per method — the
// hive_metastore.thrift `throws` clauses order exceptions differently across
// methods, so they are mapped here rather than shared.

// --- exception mapping (per hive_metastore.thrift throws clauses) ---

func isMissingPartitionErr(err error) bool {
	return errors.Is(err, hmsstore.ErrPartitionNotFound) ||
		errors.Is(err, hmsstore.ErrTableNotFound) ||
		errors.Is(err, hmsstore.ErrDatabaseNotFound)
}

// addPartitionErr maps to the add/append/pspec throws clause:
// 1:InvalidObjectException 2:AlreadyExistsException 3:MetaException.
func (s *Server) addPartitionErr(err error) error {
	switch {
	case errors.Is(err, hmsstore.ErrPartitionExists):
		return declared(2, excAlreadyExistsException, "partition already exists")
	case isMissingPartitionErr(err):
		return declared(1, excInvalidObjectException, "table or database not found")
	default:
		return declared(3, excMetaException, err.Error())
	}
}

// getPartitionErrA maps to throws(1:MetaException, 2:NoSuchObjectException):
// get_partition, get_partition_with_auth, get_partition_by_name,
// get_partitions_ps(_with_auth), get_partition_names_ps, get_partitions_by_filter,
// get_partitions_by_names, get_num_partitions_by_filter, get_part_specs_by_filter.
func (s *Server) getPartitionErrA(err error) error {
	if isMissingPartitionErr(err) {
		return declared(2, excNoSuchObjectException, "table or partition not found")
	}
	return declared(1, excMetaException, err.Error())
}

// getPartitionErrB maps to throws(1:NoSuchObjectException, 2:MetaException):
// get_partitions, get_partitions_with_auth, get_partitions_pspec.
func (s *Server) getPartitionErrB(err error) error {
	if isMissingPartitionErr(err) {
		return declared(1, excNoSuchObjectException, "table or partition not found")
	}
	return declared(2, excMetaException, err.Error())
}

// alterPartitionErr maps to throws(1:InvalidOperationException, 2:MetaException):
// alter_partition(_with_environment_context), alter_partitions(_...), rename_partition.
func (s *Server) alterPartitionErr(err error) error {
	if isMissingPartitionErr(err) {
		return declared(1, excInvalidOperationException, "table or partition not found")
	}
	return declared(2, excMetaException, err.Error())
}

// metaOnlyErr maps to a method whose only declared exception is
// MetaException at field 1 (get_partition_names, partition_name_to_*).
func metaOnlyErr(err error) error {
	return declared(1, excMetaException, err.Error())
}

// --- value helpers ---

func valueStrings(vs []Value) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		if v.T == thrift.STRING {
			out = append(out, v.Str)
		}
	}
	return out
}

func stringValues(vs []string) []Value {
	out := make([]Value, 0, len(vs))
	for _, v := range vs {
		out = append(out, StringV(v))
	}
	return out
}

// partitionFromStruct decodes a Partition struct into its (db, table, values)
// identity plus its canonical JSON. A nil partition or an unencodable struct is
// reported as an invalid object; callers choose the exception field.
func partitionFromStruct(p *Struct) (db, tbl string, values []string, raw []byte, err error) {
	if p == nil {
		return "", "", nil, nil, errors.New("partition is null")
	}
	values = valueStrings(p.List(partValues))
	db = p.String(partDBName)
	tbl = p.String(partTableName)
	raw, err = MarshalStruct(p)
	return db, tbl, values, raw, err
}

// partitionColumn is one partition-key column: its name and Hive type.
type partitionColumn struct {
	name string
	typ  string
}

// tablePartitionColumns returns the partition-key columns (Table.partitionKeys,
// field 8) in order.
func tablePartitionColumns(t hmsstore.Table) ([]partitionColumn, error) {
	st, err := UnmarshalStruct(t.TableJSON)
	if err != nil {
		return nil, err
	}
	var cols []partitionColumn
	for _, v := range st.List(tblPartitionKeys) {
		if v.T == thrift.STRUCT && v.S != nil {
			cols = append(cols, partitionColumn{name: v.S.String(fsName), typ: v.S.String(fsType)})
		}
	}
	return cols, nil
}

func columnNames(cols []partitionColumn) []string {
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		names = append(names, c.name)
	}
	return names
}

func columnTypes(cols []partitionColumn) map[string]string {
	types := make(map[string]string, len(cols))
	for _, c := range cols {
		types[c.name] = c.typ
	}
	return types
}

// tableSDLocation returns Table.sd.location (Table field 7 -> StorageDescriptor
// field 2), the warehouse root used for pspec relative paths.
func tableSDLocation(t hmsstore.Table) string {
	st, err := UnmarshalStruct(t.TableJSON)
	if err != nil {
		return ""
	}
	if sd := st.Struct(tblSD); sd != nil {
		return sd.String(sdLocation)
	}
	return ""
}

// --- partition name <-> values (Warehouse.makeSpecFromName / makePartName) ---

func partitionNameToVals(partName string) []string {
	if partName == "" {
		return []string{}
	}
	vals := []string{}
	for _, kv := range strings.Split(partName, "/") {
		i := strings.Index(kv, "=")
		if i < 0 {
			continue
		}
		vals = append(vals, unescapePathName(kv[i+1:]))
	}
	return vals
}

func partitionNameToSpec(partName string) map[string]string {
	m := map[string]string{}
	if partName == "" {
		return m
	}
	for _, kv := range strings.Split(partName, "/") {
		i := strings.Index(kv, "=")
		if i < 0 {
			continue
		}
		m[kv[:i]] = unescapePathName(kv[i+1:])
	}
	return m
}

func partitionName(keys, vals []string) string {
	n := len(keys)
	if len(vals) < n {
		n = len(vals)
	}
	if n == 0 {
		return ""
	}
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte('/')
		}
		// Hive's Warehouse.makePartName lowercases the column name but not the
		// value, so names round-trip with the canonical key casing.
		b.WriteString(escapePathName(strings.ToLower(keys[i])))
		b.WriteByte('=')
		b.WriteString(escapePathName(vals[i]))
	}
	return b.String()
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// unescapePathName percent-decodes %XX escapes, matching Hive's
// FileUtils.unescapePathName (a literal '%' not followed by two hex digits
// passes through unchanged).
func unescapePathName(s string) string {
	if !strings.ContainsRune(s, '%') {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] == '%' && i+2 < len(s) {
			hi, ok1 := hexVal(s[i+1])
			lo, ok2 := hexVal(s[i+2])
			if ok1 && ok2 {
				b.WriteByte(hi<<4 | lo)
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// escapePathName percent-encodes non-printable bytes, matching Hive's
// FileUtils.escapePathName.
func escapePathName(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c >= 0x7F {
			fmt.Fprintf(&b, "%%%02X", c)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// --- partition filter (get_partitions_by_filter / *_by_expr subset) ---

type filterNode interface {
	eval(values, types map[string]string) bool
}

type filterOr struct{ l, r filterNode }

func (n filterOr) eval(values, types map[string]string) bool {
	return n.l.eval(values, types) || n.r.eval(values, types)
}

type filterAnd struct{ l, r filterNode }

func (n filterAnd) eval(values, types map[string]string) bool {
	return n.l.eval(values, types) && n.r.eval(values, types)
}

type filterCmp struct{ key, op, val string }

func (n filterCmp) eval(values, types map[string]string) bool {
	v, ok := values[n.key]
	if !ok {
		return false
	}
	switch n.op {
	case "=", "==":
		return v == n.val
	case "!=", "<>":
		return v != n.val
	case "<":
		return comparePartitionValue(v, n.val, types[n.key]) < 0
	case "<=":
		return comparePartitionValue(v, n.val, types[n.key]) <= 0
	case ">":
		return comparePartitionValue(v, n.val, types[n.key]) > 0
	case ">=":
		return comparePartitionValue(v, n.val, types[n.key]) >= 0
	case "like":
		return likeMatch(v, n.val)
	}
	return false
}

// isNumericType reports whether a Hive column type compares numerically.
func isNumericType(t string) bool {
	t = strings.ToLower(strings.TrimSpace(t))
	for _, p := range []string{"tinyint", "smallint", "int", "bigint", "float", "double", "decimal", "numeric"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

// comparePartitionValue compares two partition values as Hive would: numeric
// for numeric column types, lexicographic otherwise (so "10" < "9" for a
// string-typed partition column).
func comparePartitionValue(a, b, typ string) int {
	if isNumericType(typ) {
		af, aerr := strconv.ParseFloat(a, 64)
		bf, berr := strconv.ParseFloat(b, 64)
		if aerr == nil && berr == nil {
			switch {
			case af < bf:
				return -1
			case af > bf:
				return 1
			default:
				return 0
			}
		}
	}
	return strings.Compare(a, b)
}

func likeMatch(v, pattern string) bool {
	var b strings.Builder
	b.WriteString("(?s)^")
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		default:
			b.WriteString(regexp.QuoteMeta(string(pattern[i])))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(v)
}

// tokenizeFilter splits a Hive partition filter into tokens. Quoted strings are
// prefixed "STR:"; operators are kept whole (`<=`, `>=`, `<>`, `!=`).
func tokenizeFilter(s string) ([]string, error) {
	var toks []string
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(' || c == ')':
			toks = append(toks, string(c))
			i++
		case c == '\'':
			j := i + 1
			var sb strings.Builder
			closed := false
			for j < len(s) {
				if s[j] == '\\' && j+1 < len(s) {
					sb.WriteByte(s[j+1])
					j += 2
					continue
				}
				if s[j] == '\'' {
					closed = true
					j++
					break
				}
				sb.WriteByte(s[j])
				j++
			}
			if !closed {
				return nil, errors.New("unterminated string literal in filter")
			}
			toks = append(toks, "STR:"+sb.String())
			i = j
		case c == '=' || c == '!' || c == '<' || c == '>':
			if i+1 < len(s) && (s[i+1] == '=' || (c == '<' && s[i+1] == '>')) {
				toks = append(toks, s[i:i+2])
				i += 2
			} else {
				toks = append(toks, string(c))
				i++
			}
		default:
			j := i
			for j < len(s) && !strings.ContainsRune(" \t\n\r()=!<>'", rune(s[j])) {
				j++
			}
			toks = append(toks, s[i:j])
			i = j
		}
	}
	return toks, nil
}

type filterParser struct {
	toks []string
	pos  int
}

func (p *filterParser) peek() string {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}
	return ""
}

func (p *filterParser) next() string {
	if p.pos >= len(p.toks) {
		return ""
	}
	t := p.toks[p.pos]
	p.pos++
	return t
}

func (p *filterParser) parseOr() (filterNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for strings.EqualFold(p.peek(), "or") {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = filterOr{l: left, r: right}
	}
	return left, nil
}

func (p *filterParser) parseAnd() (filterNode, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for strings.EqualFold(p.peek(), "and") {
		p.next()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		left = filterAnd{l: left, r: right}
	}
	return left, nil
}

func (p *filterParser) parsePrimary() (filterNode, error) {
	if p.peek() == "(" {
		p.next()
		n, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.next() != ")" {
			return nil, errors.New("expected ')' in filter")
		}
		return n, nil
	}
	return p.parseComparison()
}

func (p *filterParser) parseComparison() (filterNode, error) {
	key := strings.Trim(p.next(), "`")
	if key == "" {
		return nil, errors.New("unexpected end of filter")
	}
	op := strings.ToLower(p.next())
	switch op {
	case "=", "==", "!=", "<>", "<", "<=", ">", ">=", "like":
	default:
		return nil, fmt.Errorf("unsupported filter operator %q", op)
	}
	// A quoted empty string tokenizes as "STR:" (non-empty); a genuinely
	// missing token is "" — reject the latter so a trailing operator cannot
	// index past the token slice.
	tok := p.next()
	if tok == "" {
		return nil, errors.New("missing filter value")
	}
	val := strings.TrimPrefix(tok, "STR:")
	return filterCmp{key: key, op: op, val: val}, nil
}

// parsePartitionFilter parses a Hive filter into a predicate. An empty filter
// returns (nil, nil) meaning "match everything".
func parsePartitionFilter(s string) (filterNode, error) {
	toks, err := tokenizeFilter(s)
	if err != nil {
		return nil, err
	}
	if len(toks) == 0 {
		return nil, nil
	}
	p := &filterParser{toks: toks}
	n, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.pos < len(p.toks) {
		return nil, fmt.Errorf("unexpected token %q in filter", p.toks[p.pos])
	}
	return n, nil
}

// partitionSpecMap pairs partition-key names with a partition's values; nil when
// the tuple does not line up (an unpartitioned table, or a malformed record).
func partitionSpecMap(keys, values []string) map[string]string {
	if len(keys) != len(values) {
		return nil
	}
	m := make(map[string]string, len(keys))
	for i := range keys {
		m[keys[i]] = values[i]
	}
	return m
}

// --- partition specs (get_partitions_pspec / get_part_specs_by_filter / add_partitions_pspec) ---

// sdGroupKey canonicalizes a StorageDescriptor for grouping, ignoring the
// location field (partitions sharing an SD differ only in their relative path).
func sdGroupKey(sd *Struct) string {
	if sd == nil {
		return "null"
	}
	clone := &Struct{Fields: make([]Field, 0, len(sd.Fields))}
	for _, f := range sd.Fields {
		if f.ID == sdLocation {
			continue
		}
		clone.Fields = append(clone.Fields, f)
	}
	b, err := MarshalStruct(clone)
	if err != nil {
		return "null"
	}
	return string(b)
}

// cloneWithLocation returns a copy of sd whose location is loc, preserving
// ascending field order. A blank loc leaves the original location.
func cloneWithLocation(sd *Struct, loc string) *Struct {
	if sd == nil {
		sd = &Struct{}
	}
	if loc == "" {
		return &Struct{Fields: append([]Field(nil), sd.Fields...)}
	}
	fields := make([]Field, 0, len(sd.Fields)+1)
	added := false
	for _, f := range sd.Fields {
		if f.ID == sdLocation {
			fields = append(fields, Field{ID: sdLocation, V: StringV(loc)})
			added = true
			continue
		}
		if !added && f.ID > sdLocation {
			fields = append(fields, Field{ID: sdLocation, V: StringV(loc)})
			added = true
		}
		fields = append(fields, f)
	}
	if !added {
		fields = append(fields, Field{ID: sdLocation, V: StringV(loc)})
	}
	return &Struct{Fields: fields}
}

// isAbsolutePath reports whether a path carries its own scheme/root (so it must
// not be re-parented under the table root).
func isAbsolutePath(p string) bool {
	return strings.Contains(p, "://") || strings.HasPrefix(p, "/")
}

func relativePath(root, loc string) string {
	if root == "" || loc == "" {
		return loc
	}
	root = strings.TrimRight(root, "/")
	if loc == root {
		return ""
	}
	if strings.HasPrefix(loc, root+"/") {
		return loc[len(root)+1:]
	}
	return loc
}

// buildPartitionSpecs groups partitions by shared StorageDescriptor and renders
// list<PartitionSpec> as real Hive does: one PartitionSpecWithSharedSD per SD
// group, with each partition reduced to values/times/parameters/relativePath.
func buildPartitionSpecs(db, tbl, rootPath string, parts []*Struct) []*Struct {
	type group struct {
		sd    *Struct
		items []*Struct
	}
	var order []*group
	groups := map[string]*group{}
	for _, p := range parts {
		sd := p.Struct(partSD)
		k := sdGroupKey(sd)
		g := groups[k]
		if g == nil {
			g = &group{sd: sd}
			groups[k] = g
			order = append(order, g)
		}
		g.items = append(g.items, p)
	}

	specs := make([]*Struct, 0, len(order))
	for _, g := range order {
		items := make([]Value, 0, len(g.items))
		for _, p := range g.items {
			vals := valueStrings(p.List(partValues))
			var loc string
			if psd := p.Struct(partSD); psd != nil {
				loc = psd.String(sdLocation)
			}
			b := NewBuilder().Add(partValues, ListV(thrift.STRING, stringValues(vals)))
			b.I32(2, p.I32(partCreateTime)).I32(3, p.I32(partLastAccessTime))
			// PartitionWithoutSD.relativePath is a required field: emit it even
			// when empty (root-level partition).
			b.Str(4, relativePath(rootPath, loc))
			if params := p.MapStrStr(partParameters); len(params) > 0 {
				b.MapStrStr(5, params)
			}
			if priv := p.Struct(partPrivileges); priv != nil {
				b.Struct(6, priv)
			}
			items = append(items, StructV(b.Build()))
		}
		shared := NewBuilder().
			Add(1, ListV(thrift.STRUCT, items)).
			Struct(2, cloneWithLocation(g.sd, rootPath)).
			Build()
		spec := NewBuilder().
			Str(1, db).
			Str(2, tbl).
			Str(3, rootPath).
			Add(4, StructV(shared)).
			Build()
		specs = append(specs, spec)
	}
	return specs
}

// expandPartitionSpec turns a PartitionSpec into concrete partitions (shared-SD
// or composing form), reconstructing each partition's absolute location.
func expandPartitionSpec(spec *Struct) ([]hmsstore.Partition, error) {
	db := spec.String(1)
	tbl := spec.String(2)
	root := spec.String(3)
	out := []hmsstore.Partition{}

	if shared := spec.Struct(4); shared != nil {
		sharedSD := shared.Struct(2)
		for _, pv := range shared.List(1) {
			if pv.T != thrift.STRUCT || pv.S == nil {
				continue
			}
			pws := pv.S
			values := valueStrings(pws.List(1))
			// Reconstruct the absolute location: root + relativePath, unless
			// relativePath is itself absolute (a partition outside the root).
			sd := cloneWithLocation(sharedSD, "")
			switch rel := pws.String(4); {
			case rel == "":
				if root != "" {
					sd = cloneWithLocation(sharedSD, root)
				}
			case isAbsolutePath(rel):
				sd = cloneWithLocation(sharedSD, rel)
			default:
				sd = cloneWithLocation(sharedSD, strings.TrimRight(root, "/")+"/"+strings.TrimLeft(rel, "/"))
			}
			b := NewBuilder().
				Add(partValues, ListV(thrift.STRING, stringValues(values))).
				Str(partDBName, db).
				Str(partTableName, tbl).
				I32(partCreateTime, pws.I32(2)).
				I32(partLastAccessTime, pws.I32(3)).
				Struct(partSD, sd)
			if params := pws.MapStrStr(5); len(params) > 0 {
				b.MapStrStr(partParameters, params)
			}
			if priv := pws.Struct(6); priv != nil {
				b.Struct(partPrivileges, priv)
			}
			raw, err := MarshalStruct(b.Build())
			if err != nil {
				return nil, err
			}
			out = append(out, hmsstore.Partition{DBName: db, TableName: tbl, Values: values, PartJSON: raw})
		}
	}
	if plist := spec.Struct(5); plist != nil {
		for _, pv := range plist.List(1) {
			if pv.T != thrift.STRUCT || pv.S == nil {
				continue
			}
			pdb, ptbl, values, raw, err := partitionFromStruct(pv.S)
			if err != nil {
				return nil, err
			}
			if pdb == "" {
				pdb = db
			}
			if ptbl == "" {
				ptbl = tbl
			}
			out = append(out, hmsstore.Partition{DBName: pdb, TableName: ptbl, Values: values, PartJSON: raw})
		}
	}
	return out, nil
}

// --- add ---

func (s *Server) addPartition(ctx context.Context, args *Struct) (*Struct, error) {
	db, tbl, values, raw, err := partitionFromStruct(args.Struct(1))
	if err != nil {
		return nil, declared(1, excInvalidObjectException, err.Error())
	}
	if db == "" {
		db = "default"
	}
	if tbl == "" {
		return nil, declared(1, excInvalidObjectException, "partition table name cannot be empty")
	}
	if cerr := s.store.CreatePartition(ctx, db, tbl, hmsstore.Partition{DBName: db, TableName: tbl, Values: values, PartJSON: raw}); cerr != nil {
		return nil, s.addPartitionErr(cerr)
	}
	st, err := UnmarshalStruct(raw)
	if err != nil {
		return nil, declared(3, excMetaException, err.Error())
	}
	return result0(StructV(st)), nil
}

func (s *Server) addPartitions(ctx context.Context, args *Struct) (*Struct, error) {
	parts := args.List(1)
	entries := make([]hmsstore.Partition, 0, len(parts))
	for _, v := range parts {
		if v.T != thrift.STRUCT || v.S == nil {
			continue
		}
		db, tbl, values, raw, err := partitionFromStruct(v.S)
		if err != nil {
			return nil, declared(1, excInvalidObjectException, err.Error())
		}
		if db == "" {
			db = "default"
		}
		if tbl == "" {
			return nil, declared(1, excInvalidObjectException, "partition table name cannot be empty")
		}
		entries = append(entries, hmsstore.Partition{DBName: db, TableName: tbl, Values: values, PartJSON: raw})
	}
	// Old add_partitions API assumes one table and ifNotExists=false: if any
	// partition already exists the whole call fails (Hive add_partitions_core).
	for _, e := range entries {
		if _, err := s.store.GetPartition(ctx, e.DBName, e.TableName, e.Values); err == nil {
			return nil, declared(2, excAlreadyExistsException, "partition already exists")
		} else if !errors.Is(err, hmsstore.ErrPartitionNotFound) {
			return nil, s.addPartitionErr(err)
		}
	}
	for _, e := range entries {
		if err := s.store.CreatePartition(ctx, e.DBName, e.TableName, e); err != nil {
			return nil, s.addPartitionErr(err)
		}
	}
	return result0(I32V(int32(len(entries)))), nil
}

func (s *Server) addPartitionsReq(ctx context.Context, args *Struct) (*Struct, error) {
	req := args.Struct(1)
	if req == nil {
		return nil, declared(1, excInvalidObjectException, "AddPartitionsRequest is required")
	}
	db := req.String(1)
	tbl := req.String(2)
	ifNotExists := req.Bool(4)
	needResult := true
	if v, ok := req.Get(5); ok && v.T == thrift.BOOL {
		needResult = v.B
	}
	type pending struct {
		db, tbl string
		values  []string
		raw     []byte
	}
	var pend []pending
	for _, v := range req.List(3) {
		if v.T != thrift.STRUCT || v.S == nil {
			continue
		}
		pdb, ptbl, values, raw, err := partitionFromStruct(v.S)
		if err != nil {
			return nil, declared(1, excInvalidObjectException, err.Error())
		}
		if pdb == "" {
			pdb = db
		}
		if ptbl == "" {
			ptbl = tbl
		}
		pend = append(pend, pending{db: pdb, tbl: ptbl, values: values, raw: raw})
	}
	// add_partitions_req is transactional in Hive: with ifNotExists=false a
	// pre-existing partition fails the whole call before anything is written.
	if !ifNotExists {
		for _, p := range pend {
			if _, err := s.store.GetPartition(ctx, p.db, p.tbl, p.values); err == nil {
				return nil, declared(2, excAlreadyExistsException, "partition already exists")
			} else if !errors.Is(err, hmsstore.ErrPartitionNotFound) {
				return nil, s.addPartitionErr(err)
			}
		}
	}
	var added []Value
	for _, p := range pend {
		cerr := s.store.CreatePartition(ctx, p.db, p.tbl, hmsstore.Partition{DBName: p.db, TableName: p.tbl, Values: p.values, PartJSON: p.raw})
		if cerr != nil {
			if errors.Is(cerr, hmsstore.ErrPartitionExists) && ifNotExists {
				continue
			}
			return nil, s.addPartitionErr(cerr)
		}
		if needResult {
			if st, err := UnmarshalStruct(p.raw); err == nil {
				added = append(added, StructV(st))
			}
		}
	}
	res := NewBuilder()
	if needResult {
		res.Add(1, ListV(thrift.STRUCT, added))
	}
	return result0(StructV(res.Build())), nil
}

func (s *Server) addPartitionsPspec(ctx context.Context, args *Struct) (*Struct, error) {
	count := 0
	for _, v := range args.List(1) {
		if v.T != thrift.STRUCT || v.S == nil {
			continue
		}
		parts, err := expandPartitionSpec(v.S)
		if err != nil {
			return nil, declared(1, excInvalidObjectException, err.Error())
		}
		for _, p := range parts {
			if cerr := s.store.CreatePartition(ctx, p.DBName, p.TableName, p); cerr != nil {
				return nil, s.addPartitionErr(cerr)
			}
			count++
		}
	}
	return result0(I32V(int32(count))), nil
}

// append_partition builds a partition from an explicit (db, tbl, values) tuple,
// as its method signature carries no Partition struct.
func (s *Server) appendPartition(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	values := valueStrings(args.List(3))
	if db == "" {
		db = "default"
	}
	// Hive derives the partition location from the table root + partition name;
	// do the same so the returned/stored partition has a usable sd.location.
	sd := NewBuilder()
	if tblStore, terr := s.store.GetTable(ctx, db, tbl); terr == nil {
		cols, _ := tablePartitionColumns(tblStore)
		if root := tableSDLocation(tblStore); root != "" {
			sd.Str(sdLocation, strings.TrimRight(root, "/")+"/"+partitionName(columnNames(cols), values))
		}
	} else {
		return nil, s.addPartitionErr(terr)
	}
	st := NewBuilder().
		Add(partValues, ListV(thrift.STRING, stringValues(values))).
		Str(partDBName, db).
		Str(partTableName, tbl).
		I32(partCreateTime, int32(clock.Now().Unix())).
		I32(partLastAccessTime, 0).
		Struct(partSD, sd.Build()).
		Build()
	raw, err := MarshalStruct(st)
	if err != nil {
		return nil, declared(3, excMetaException, err.Error())
	}
	if cerr := s.store.CreatePartition(ctx, db, tbl, hmsstore.Partition{DBName: db, TableName: tbl, Values: values, PartJSON: raw}); cerr != nil {
		return nil, s.addPartitionErr(cerr)
	}
	return result0(StructV(st)), nil
}

func (s *Server) appendPartitionByName(ctx context.Context, args *Struct) (*Struct, error) {
	name := args.String(3)
	values := partitionNameToVals(name)
	built := NewBuilder().
		Str(1, args.String(1)).
		Str(2, args.String(2)).
		Add(3, ListV(thrift.STRING, stringValues(values))).
		Build()
	return s.appendPartition(ctx, built)
}

// --- get ---

func (s *Server) getPartition(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	values := valueStrings(args.List(3))
	p, err := s.store.GetPartition(ctx, db, tbl, values)
	if err != nil {
		return nil, s.getPartitionErrA(err)
	}
	st, err := UnmarshalStruct(p.PartJSON)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	return result0(StructV(st)), nil
}

func (s *Server) getPartitionByName(ctx context.Context, args *Struct) (*Struct, error) {
	name := args.String(3)
	values := partitionNameToVals(name)
	p, err := s.store.GetPartition(ctx, args.String(1), args.String(2), values)
	if err != nil {
		return nil, s.getPartitionErrA(err)
	}
	st, err := UnmarshalStruct(p.PartJSON)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	return result0(StructV(st)), nil
}

// listAllPartitions returns every partition struct of a table.
func (s *Server) listAllPartitions(ctx context.Context, db, tbl string) ([]*Struct, error) {
	parts, err := s.store.ListPartitions(ctx, db, tbl)
	if err != nil {
		return nil, err
	}
	out := make([]*Struct, 0, len(parts))
	for _, p := range parts {
		st, err := UnmarshalStruct(p.PartJSON)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

func capPartsMax(parts []*Struct, maxParts int32) []*Struct {
	if maxParts > 0 && int(maxParts) < len(parts) {
		return parts[:maxParts]
	}
	return parts
}

func (s *Server) getPartitions(ctx context.Context, args *Struct) (*Struct, error) {
	parts, err := s.listAllPartitions(ctx, args.String(1), args.String(2))
	if err != nil {
		return nil, s.getPartitionErrB(err)
	}
	parts = capPartsMax(parts, int32(args.I16(3)))
	return NewBuilder().ListStruct(0, parts).Build(), nil
}

func (s *Server) getPartitionsPs(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	prefix := valueStrings(args.List(3))
	parts, err := s.listAllPartitions(ctx, db, tbl)
	if err != nil {
		return nil, s.getPartitionErrA(err)
	}
	filtered := make([]*Struct, 0, len(parts))
	for _, p := range parts {
		if hasValuePrefix(valueStrings(p.List(partValues)), prefix) {
			filtered = append(filtered, p)
		}
	}
	filtered = capPartsMax(filtered, int32(args.I16(4)))
	return NewBuilder().ListStruct(0, filtered).Build(), nil
}

func hasValuePrefix(values, prefix []string) bool {
	if len(prefix) > len(values) {
		return false
	}
	for i := range prefix {
		if values[i] != prefix[i] {
			return false
		}
	}
	return true
}

// filterPartitions evaluates a Hive filter against each partition's value tuple.
func (s *Server) filterPartitions(ctx context.Context, db, tbl, filter string) ([]*Struct, error) {
	tblStore, err := s.store.GetTable(ctx, db, tbl)
	if err != nil {
		return nil, s.getPartitionErrA(err)
	}
	cols, err := tablePartitionColumns(tblStore)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	node, perr := parsePartitionFilter(filter)
	if perr != nil {
		return nil, s.getPartitionErrA(perr)
	}
	names := columnNames(cols)
	types := columnTypes(cols)
	parts, err := s.listAllPartitions(ctx, db, tbl)
	if err != nil {
		return nil, s.getPartitionErrA(err)
	}
	out := make([]*Struct, 0, len(parts))
	for _, p := range parts {
		if node == nil {
			out = append(out, p)
			continue
		}
		m := partitionSpecMap(names, valueStrings(p.List(partValues)))
		if m != nil && node.eval(m, types) {
			out = append(out, p)
		}
	}
	return out, nil
}

func (s *Server) getPartitionsByFilter(ctx context.Context, args *Struct) (*Struct, error) {
	parts, err := s.filterPartitions(ctx, args.String(1), args.String(2), args.String(3))
	if err != nil {
		return nil, err
	}
	parts = capPartsMax(parts, int32(args.I16(4)))
	return NewBuilder().ListStruct(0, parts).Build(), nil
}

func (s *Server) getNumPartitionsByFilter(ctx context.Context, args *Struct) (*Struct, error) {
	parts, err := s.filterPartitions(ctx, args.String(1), args.String(2), args.String(3))
	if err != nil {
		return nil, err
	}
	return result0(I32V(int32(len(parts)))), nil
}

func (s *Server) getPartitionsByNames(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	names := valueStrings(args.List(3))
	out := make([]*Struct, 0, len(names))
	for _, name := range names {
		values := partitionNameToVals(name)
		p, err := s.store.GetPartition(ctx, db, tbl, values)
		if err != nil {
			if errors.Is(err, hmsstore.ErrPartitionNotFound) {
				continue // get_partitions_by_names returns found partitions only
			}
			return nil, s.getPartitionErrA(err)
		}
		st, err := UnmarshalStruct(p.PartJSON)
		if err != nil {
			return nil, declared(1, excMetaException, err.Error())
		}
		out = append(out, st)
	}
	return NewBuilder().ListStruct(0, out).Build(), nil
}

func (s *Server) getPartitionNames(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	tblStore, err := s.store.GetTable(ctx, db, tbl)
	if err != nil {
		return nil, metaOnlyErr(err)
	}
	cols, err := tablePartitionColumns(tblStore)
	if err != nil {
		return nil, metaOnlyErr(err)
	}
	keys := columnNames(cols)
	parts, err := s.store.ListPartitions(ctx, db, tbl)
	if err != nil {
		return nil, metaOnlyErr(err)
	}
	names := make([]string, 0, len(parts))
	for _, p := range capParts(parts, int32(args.I16(3))) {
		names = append(names, partitionName(keys, p.Values))
	}
	return NewBuilder().ListStr(0, names).Build(), nil
}

func (s *Server) getPartitionNamesPs(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	prefix := valueStrings(args.List(3))
	tblStore, err := s.store.GetTable(ctx, db, tbl)
	if err != nil {
		return nil, s.getPartitionErrA(err)
	}
	cols, err := tablePartitionColumns(tblStore)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	keys := columnNames(cols)
	parts, err := s.store.ListPartitions(ctx, db, tbl)
	if err != nil {
		return nil, s.getPartitionErrA(err)
	}
	// Filter by the partial spec first, THEN apply max_parts (Hive semantics).
	filtered := make([]hmsstore.Partition, 0, len(parts))
	for _, p := range parts {
		if hasValuePrefix(p.Values, prefix) {
			filtered = append(filtered, p)
		}
	}
	names := make([]string, 0, len(filtered))
	for _, p := range capParts(filtered, int32(args.I16(4))) {
		names = append(names, partitionName(keys, p.Values))
	}
	return NewBuilder().ListStr(0, names).Build(), nil
}

func capParts(parts []hmsstore.Partition, maxParts int32) []hmsstore.Partition {
	if maxParts > 0 && int(maxParts) < len(parts) {
		return parts[:maxParts]
	}
	return parts
}

func (s *Server) getPartitionsPspec(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	tblStore, err := s.store.GetTable(ctx, db, tbl)
	if err != nil {
		return nil, s.getPartitionErrB(err)
	}
	parts, err := s.listAllPartitions(ctx, db, tbl)
	if err != nil {
		return nil, s.getPartitionErrB(err)
	}
	specs := buildPartitionSpecs(db, tbl, tableSDLocation(tblStore), capPartsMax(parts, args.I32(3)))
	return NewBuilder().ListStruct(0, specs).Build(), nil
}

func (s *Server) getPartSpecsByFilter(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	tblStore, err := s.store.GetTable(ctx, db, tbl)
	if err != nil {
		return nil, s.getPartitionErrA(err)
	}
	parts, err := s.filterPartitions(ctx, db, tbl, args.String(3))
	if err != nil {
		return nil, err
	}
	specs := buildPartitionSpecs(db, tbl, tableSDLocation(tblStore), capPartsMax(parts, args.I32(4)))
	return NewBuilder().ListStruct(0, specs).Build(), nil
}

func (s *Server) partitionNameToValsHandler(_ context.Context, args *Struct) (*Struct, error) {
	return NewBuilder().ListStr(0, partitionNameToVals(args.String(1))).Build(), nil
}

func (s *Server) partitionNameToSpecHandler(_ context.Context, args *Struct) (*Struct, error) {
	m := partitionNameToSpec(args.String(1))
	entries := make([]MapEntry, 0, len(m))
	// Deterministic order for a stable response.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		entries = append(entries, MapEntry{K: StringV(k), V: StringV(m[k])})
	}
	return NewBuilder().Add(0, MapV(thrift.STRING, thrift.STRING, entries)).Build(), nil
}

// --- alter / rename ---

// alterPartition overwrites the partition identified by new_part.values with the
// full new Partition (Hive alter_partition is a full replacement, not a patch).
func (s *Server) alterPartition(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	_, _, values, raw, err := partitionFromStruct(args.Struct(3))
	if err != nil {
		return nil, declared(1, excInvalidOperationException, err.Error())
	}
	_, aerr := s.store.AlterPartition(ctx, db, tbl, values, func(current hmsstore.Partition) (hmsstore.Partition, error) {
		return hmsstore.Partition{DBName: db, TableName: tbl, Values: current.Values, PartJSON: raw}, nil
	})
	if aerr != nil {
		return nil, s.alterPartitionErr(aerr)
	}
	return voidResult(), nil
}

func (s *Server) alterPartitions(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	for _, v := range args.List(3) {
		if v.T != thrift.STRUCT || v.S == nil {
			continue
		}
		_, _, values, raw, err := partitionFromStruct(v.S)
		if err != nil {
			return nil, declared(1, excInvalidOperationException, err.Error())
		}
		_, aerr := s.store.AlterPartition(ctx, db, tbl, values, func(current hmsstore.Partition) (hmsstore.Partition, error) {
			return hmsstore.Partition{DBName: db, TableName: tbl, Values: current.Values, PartJSON: raw}, nil
		})
		if aerr != nil {
			return nil, s.alterPartitionErr(aerr)
		}
	}
	return voidResult(), nil
}

func (s *Server) renamePartition(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	oldValues := valueStrings(args.List(3))
	_, _, newValues, raw, err := partitionFromStruct(args.Struct(4))
	if err != nil {
		return nil, declared(1, excInvalidOperationException, err.Error())
	}
	_, rerr := s.store.RenamePartition(ctx, db, tbl, oldValues, newValues, hmsstore.Partition{DBName: db, TableName: tbl, Values: newValues, PartJSON: raw})
	if rerr != nil {
		return nil, s.alterPartitionErr(rerr)
	}
	return voidResult(), nil
}

// --- drop ---

// dropPartition removes partition metadata. The deleteData flag (field 4) is a
// documented no-op: the emulator owns no table data files, and deleting GCS
// objects would cross into the storage plane (recorded in the plan).
func (s *Server) dropPartition(ctx context.Context, args *Struct) (*Struct, error) {
	values := valueStrings(args.List(3))
	if err := s.store.DropPartition(ctx, args.String(1), args.String(2), values); err != nil {
		return nil, s.getPartitionErrB(err)
	}
	return result0(BoolV(true)), nil
}

func (s *Server) dropPartitionByName(ctx context.Context, args *Struct) (*Struct, error) {
	values := partitionNameToVals(args.String(3))
	if err := s.store.DropPartition(ctx, args.String(1), args.String(2), values); err != nil {
		return nil, s.getPartitionErrB(err)
	}
	return result0(BoolV(true)), nil
}
