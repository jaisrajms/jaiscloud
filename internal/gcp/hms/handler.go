package hms

import (
	"context"
	"regexp"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"

	hmsstore "jaiscloud/internal/gcp/store/hms"
)

// methodHandler decodes a method's args struct and returns its result struct.
// A nil result with a nil error is a void success (encoded as an empty result
// struct). A non-nil *DeclaredError is encoded as an exception field in the
// result struct; a non-nil *AppError (or any other error) becomes a
// TApplicationException.
type methodHandler func(ctx context.Context, args *Struct) (*Struct, error)

func result0(v Value) *Struct { return &Struct{Fields: []Field{{ID: 0, V: v}}} }
func voidResult() *Struct     { return &Struct{} }

// declared builds a *DeclaredError at the given result-struct position.
func declared(fieldID int16, name, msg string) error {
	return &DeclaredError{FieldID: fieldID, Name: name, Message: msg}
}

// matchName reports whether name matches pattern, which Hive's
// get_databases/get_tables treat as a regular expression. Empty and "*"
// patterns match everything; a pattern that fails to compile falls back to a
// literal substring match.
func matchName(pattern, name string) bool {
	if pattern == "" || pattern == "*" || pattern == ".*" {
		return true
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return strings.Contains(name, pattern)
	}
	return re.MatchString(name)
}

// --- Identity ---

// setUgi implements set_ugi(user, group_names) -> list<string>: the caller
// identity announcement Hive's client issues once per new binary-NOSASL
// connection (hive.metastore.execute.setugi). The emulator enforces no authz, so
// it echoes the requested groups (matching a metastore with no group
// resolution) rather than returning a server-resolved set. The declared
// MetaException belongs at result field 1 but is never raised.
func (s *Server) setUgi(_ context.Context, args *Struct) (*Struct, error) {
	return NewBuilder().ListStr(0, stringElems(args.List(2))).Build(), nil
}

// --- Databases ---

func (s *Server) createDatabase(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.Struct(1)
	name := db.String(dbName)
	if name == "" {
		return nil, declared(2, excInvalidObjectException, "database name cannot be empty")
	}
	if err := s.store.CreateDatabase(ctx, hmsstore.Database{
		Name:        name,
		Description: db.String(dbDescription),
		LocationURI: db.String(dbLocationURI),
		Parameters:  db.MapStrStr(dbParameters),
		Owner:       db.String(dbOwnerName),
	}); err != nil {
		switch {
		case err == hmsstore.ErrDatabaseExists:
			return nil, declared(1, excAlreadyExistsException, "database "+name+" already exists")
		default:
			return nil, declared(3, excMetaException, err.Error())
		}
	}
	return voidResult(), nil
}

func (s *Server) getDatabase(ctx context.Context, args *Struct) (*Struct, error) {
	name := args.String(1)
	db, err := s.store.GetDatabase(ctx, name)
	if err != nil {
		switch {
		case err == hmsstore.ErrDatabaseNotFound:
			return nil, declared(1, excNoSuchObjectException, "database "+name+" not found")
		default:
			return nil, declared(2, excMetaException, err.Error())
		}
	}
	return result0(StructV(buildDatabaseStruct(db))), nil
}

func (s *Server) dropDatabase(ctx context.Context, args *Struct) (*Struct, error) {
	name := args.String(1)
	cascade := args.Bool(3)
	if err := s.store.DropDatabase(ctx, name, cascade); err != nil {
		switch {
		case err == hmsstore.ErrDatabaseNotFound:
			return nil, declared(1, excNoSuchObjectException, "database "+name+" not found")
		case err == hmsstore.ErrDatabaseNotEmpty:
			return nil, declared(2, excInvalidOperationException, "database "+name+" is not empty")
		default:
			return nil, declared(3, excMetaException, err.Error())
		}
	}
	return voidResult(), nil
}

func (s *Server) listDatabases(ctx context.Context, args *Struct) (*Struct, error) {
	pattern := args.String(1)
	names, err := s.store.ListDatabases(ctx)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	filtered := make([]string, 0, len(names))
	for _, n := range names {
		if matchName(pattern, n) {
			filtered = append(filtered, n)
		}
	}
	b := NewBuilder()
	b.ListStr(0, filtered)
	return b.Build(), nil
}

func (s *Server) alterDatabase(ctx context.Context, args *Struct) (*Struct, error) {
	name := args.String(1)
	db := args.Struct(2)
	if err := s.store.AlterDatabase(ctx, name, hmsstore.Database{
		Name:        name,
		Description: db.String(dbDescription),
		LocationURI: db.String(dbLocationURI),
		Parameters:  db.MapStrStr(dbParameters),
		Owner:       db.String(dbOwnerName),
	}); err != nil {
		switch {
		case err == hmsstore.ErrDatabaseNotFound:
			return nil, declared(2, excNoSuchObjectException, "database "+name+" not found")
		default:
			return nil, declared(1, excMetaException, err.Error())
		}
	}
	return voidResult(), nil
}

// --- Tables ---

// tableJSON encodes a decoded Table struct as canonical JSON and returns its
// (dbName, tableName, json) plus an error for an invalid object.
func tableJSON(t *Struct) (db, tbl string, raw []byte, err error) {
	if t == nil {
		return "", "", nil, declared(2, excInvalidObjectException, "table is null")
	}
	db = t.String(tblDBName)
	tbl = t.String(tblTableName)
	raw, err = MarshalStruct(t)
	if err != nil {
		return "", "", nil, declared(3, excMetaException, err.Error())
	}
	return db, tbl, raw, nil
}

func (s *Server) createTable(ctx context.Context, args *Struct) (*Struct, error) {
	db, tbl, raw, err := tableJSON(args.Struct(1))
	if err != nil {
		if de, ok := err.(*DeclaredError); ok {
			return nil, de
		}
		return nil, declared(3, excMetaException, err.Error())
	}
	if tbl == "" {
		return nil, declared(2, excInvalidObjectException, "table name cannot be empty")
	}
	if db == "" {
		db = "default"
	}
	if serr := s.store.CreateTable(ctx, db, tbl, hmsstore.Table{DBName: db, TableName: tbl, TableJSON: raw}); serr != nil {
		switch {
		case serr == hmsstore.ErrTableExists:
			return nil, declared(1, excAlreadyExistsException, "table "+db+"."+tbl+" already exists")
		case serr == hmsstore.ErrDatabaseNotFound:
			return nil, declared(4, excNoSuchObjectException, "database "+db+" not found")
		default:
			return nil, declared(3, excMetaException, serr.Error())
		}
	}
	return voidResult(), nil
}

// tableStruct loads and decodes the stored Table for (db, tbl) as a wire Table
// struct. The declared exceptions (MetaException at result field 1,
// NoSuchObjectException at field 2) are shared by get_table and get_table_req,
// whose throws clauses match.
func (s *Server) tableStruct(ctx context.Context, db, tbl string) (*Struct, error) {
	t, err := s.store.GetTable(ctx, db, tbl)
	if err != nil {
		switch {
		case err == hmsstore.ErrTableNotFound:
			return nil, declared(2, excNoSuchObjectException, "table "+db+"."+tbl+" not found")
		default:
			return nil, declared(1, excMetaException, err.Error())
		}
	}
	st, err := UnmarshalStruct(t.TableJSON)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	return st, nil
}

func (s *Server) getTable(ctx context.Context, args *Struct) (*Struct, error) {
	st, err := s.tableStruct(ctx, args.String(1), args.String(2))
	if err != nil {
		return nil, err
	}
	return result0(StructV(st)), nil
}

// getTableReq implements get_table_req(GetTableRequest) -> GetTableResult: the
// request-struct form of get_table that Hive 2.3+ clients (including the Go
// hms-client-go client) use. dbName/tblName come from the nested request's
// fields 1/2; catName (field 4) is ignored because the serving plane is a single
// global catalog. The reply wraps the Table in GetTableResult.table, i.e. result
// field 0 holds a struct whose field 1 is the table.
func (s *Server) getTableReq(ctx context.Context, args *Struct) (*Struct, error) {
	req := args.Struct(1)
	st, err := s.tableStruct(ctx, req.String(reqDBName), req.String(reqTblName))
	if err != nil {
		return nil, err
	}
	return result0(StructV(NewBuilder().Struct(reqResultTable, st).Build())), nil
}

func (s *Server) listTables(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	pattern := args.String(2)
	names, err := s.store.ListTables(ctx, db)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	filtered := make([]string, 0, len(names))
	for _, n := range names {
		if matchName(pattern, n) {
			filtered = append(filtered, n)
		}
	}
	b := NewBuilder()
	b.ListStr(0, filtered)
	return b.Build(), nil
}

// tableStructsByName decodes the stored Table structs for names, skipping names
// that do not exist (the client filters missing tables itself). A store failure
// surfaces as MetaException (result field 1), the declared exception shared by
// the positional and request-struct forms.
func (s *Server) tableStructsByName(ctx context.Context, db string, names []string) ([]*Struct, error) {
	var tables []*Struct
	for _, name := range names {
		t, err := s.store.GetTable(ctx, db, name)
		if err != nil {
			if err == hmsstore.ErrTableNotFound {
				continue // tolerate missing tables (the client filters them)
			}
			return nil, declared(1, excMetaException, err.Error())
		}
		st, err := UnmarshalStruct(t.TableJSON)
		if err != nil {
			return nil, declared(1, excMetaException, err.Error())
		}
		tables = append(tables, st)
	}
	return tables, nil
}

// getTableObjectsByName implements get_table_objects_by_name(dbname,
// list<string>) -> list<Table>. It is not in D3's enumerated subset but is on
// the real Iceberg HiveCatalog.listTables critical path (verified Iceberg
// 1.5.2 HiveCatalog.java:133), so it is implemented rather than stubbed.
func (s *Server) getTableObjectsByName(ctx context.Context, args *Struct) (*Struct, error) {
	tables, err := s.tableStructsByName(ctx, args.String(1), stringElems(args.List(2)))
	if err != nil {
		return nil, err
	}
	return NewBuilder().ListStruct(0, tables).Build(), nil
}

// getTableObjectsByNameReq implements get_table_objects_by_name_req(
// GetTablesRequest) -> GetTablesResult: the request-struct form of
// get_table_objects_by_name (tblNames at field 2; optional catName at field 4,
// ignored). The reply wraps the table list in GetTablesResult.tables, i.e.
// result field 0 holds a struct whose field 1 is the list.
func (s *Server) getTableObjectsByNameReq(ctx context.Context, args *Struct) (*Struct, error) {
	req := args.Struct(1)
	tables, err := s.tableStructsByName(ctx, req.String(reqDBName), stringElems(req.List(reqTblNames)))
	if err != nil {
		return nil, err
	}
	return result0(StructV(NewBuilder().ListStruct(reqResultTables, tables).Build())), nil
}

// stringElems returns the string values of a decoded list, ignoring elements of
// any other type (the emulator is lenient about malformed lists).
func stringElems(v []Value) []string {
	out := make([]string, 0, len(v))
	for _, e := range v {
		if e.T == thrift.STRING {
			out = append(out, e.Str)
		}
	}
	return out
}

// alterTable handles alter_table/alter_table_with_environment_context: a plain
// overwrite of the stored Table (D5). When the incoming Table's dbName or
// tableName differs from the addressed (db, tbl), it is a rename (the real Hive
// rename path — Iceberg's renameTable drives alter_table with a renamed Table).
func (s *Server) alterTable(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	newDB, newTbl, raw, err := tableJSON(args.Struct(3))
	if err != nil {
		if de, ok := err.(*DeclaredError); ok {
			return nil, de
		}
		return nil, declared(2, excMetaException, err.Error())
	}
	if newTbl == "" {
		newTbl = tbl
	}
	if newDB == "" {
		newDB = db
	}

	if newDB != db || newTbl != tbl {
		_, rerr := s.store.RenameTable(ctx, db, tbl, newDB, newTbl, hmsstore.Table{DBName: newDB, TableName: newTbl, TableJSON: raw})
		if rerr != nil {
			switch {
			case rerr == hmsstore.ErrTableNotFound:
				return nil, declared(1, excInvalidOperationException, "table "+db+"."+tbl+" not found")
			case rerr == hmsstore.ErrDatabaseNotFound:
				return nil, declared(1, excInvalidOperationException, "database "+newDB+" not found")
			case rerr == hmsstore.ErrTableExists:
				return nil, declared(1, excInvalidOperationException, "new table "+newDB+"."+newTbl+" already exists")
			default:
				return nil, declared(2, excMetaException, rerr.Error())
			}
		}
		return voidResult(), nil
	}

	_, aerr := s.store.AlterTable(ctx, db, tbl, func(current hmsstore.Table) (hmsstore.Table, error) {
		// Plain unconditional overwrite (D5): the lock is the only concurrency
		// control, matching real Hive alter_table.
		return hmsstore.Table{DBName: db, TableName: tbl, TableJSON: raw}, nil
	})
	if aerr != nil {
		switch {
		case aerr == hmsstore.ErrTableNotFound:
			return nil, declared(1, excInvalidOperationException, "table "+db+"."+tbl+" not found")
		default:
			return nil, declared(2, excMetaException, aerr.Error())
		}
	}
	return voidResult(), nil
}

// getTableMeta implements get_table_meta(db_patterns, tbl_patterns, tbl_types)
// -> list<TableMeta>: the pattern-based bulk metadata lookup Hive 3.x clients
// (HiveMetaStoreClient.getTableMeta) use. It reuses the get_databases/get_tables
// pattern semantics (matchName) and derives each TableMeta from the stored Table
// struct (tableType field 12; comments from parameters field 9). tbl_types, when
// supplied, filters on the exact table type. A missing database/table is skipped;
// only store failures surface as MetaException (result field 1).
func (s *Server) getTableMeta(ctx context.Context, args *Struct) (*Struct, error) {
	dbPattern := args.String(1)
	tblPattern := args.String(2)
	var types map[string]bool
	if l := args.List(3); len(l) > 0 {
		types = make(map[string]bool, len(l))
		for _, v := range l {
			if v.T == thrift.STRING {
				types[v.Str] = true
			}
		}
	}

	dbs, err := s.store.ListDatabases(ctx)
	if err != nil {
		return nil, declared(1, excMetaException, err.Error())
	}
	metas := make([]*Struct, 0)
	for _, db := range dbs {
		if !matchName(dbPattern, db) {
			continue
		}
		names, err := s.store.ListTables(ctx, db)
		if err != nil {
			return nil, declared(1, excMetaException, err.Error())
		}
		for _, name := range names {
			if !matchName(tblPattern, name) {
				continue
			}
			t, err := s.store.GetTable(ctx, db, name)
			if err != nil {
				if err == hmsstore.ErrTableNotFound {
					continue // raced with a concurrent drop
				}
				return nil, declared(1, excMetaException, err.Error())
			}
			st, err := UnmarshalStruct(t.TableJSON)
			if err != nil {
				return nil, declared(1, excMetaException, err.Error())
			}
			tableType := st.String(tblTableType)
			if types != nil && !types[tableType] {
				continue
			}
			metas = append(metas, buildTableMeta(db, name, tableType, st))
		}
	}
	b := NewBuilder()
	b.ListStruct(0, metas)
	return b.Build(), nil
}

func (s *Server) dropTable(ctx context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	tbl := args.String(2)
	if err := s.store.DropTable(ctx, db, tbl); err != nil {
		switch {
		case err == hmsstore.ErrTableNotFound:
			return nil, declared(1, excNoSuchObjectException, "table "+db+"."+tbl+" not found")
		default:
			return nil, declared(2, excMetaException, err.Error())
		}
	}
	return voidResult(), nil
}

// --- Functions (stubbed: no function store) ---

func (s *Server) createFunction(_ context.Context, _ *Struct) (*Struct, error) {
	return voidResult(), nil
}

func (s *Server) dropFunction(_ context.Context, _ *Struct) (*Struct, error) {
	return voidResult(), nil
}

func (s *Server) getFunctions(_ context.Context, _ *Struct) (*Struct, error) {
	return result0(ListV(thrift.STRING, nil)), nil
}

func (s *Server) getFunction(_ context.Context, args *Struct) (*Struct, error) {
	db := args.String(1)
	fn := args.String(2)
	return nil, declared(2, excNoSuchObjectException, "function "+db+"."+fn+" not found")
}

func (s *Server) getAllFunctions(_ context.Context, _ *Struct) (*Struct, error) {
	// GetAllFunctionsResponse {1: optional list<Function> functions} — omit the
	// field so the client observes an empty registry.
	return result0(StructV(&Struct{})), nil
}

// --- Locks ---

func (s *Server) lock(ctx context.Context, args *Struct) (*Struct, error) {
	req := args.Struct(1)
	l := hmsstore.Lock{User: req.String(lrUser), Hostname: req.String(lrHostname)}
	if comps := req.List(lrComponent); len(comps) > 0 {
		if c := comps[0].S; c != nil {
			l.DBName = c.String(lcDBName)
			l.TableName = c.String(lcTableName)
		}
	}
	id, err := s.store.Lock(ctx, l)
	if err != nil {
		return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: err.Error()}
	}
	return result0(StructV(buildLockResponse(id, hmsstore.LockStateAcquired))), nil
}

func (s *Server) checkLock(ctx context.Context, args *Struct) (*Struct, error) {
	req := args.Struct(1)
	id := req.I64(1)
	state, err := s.store.CheckLock(ctx, id)
	if err != nil {
		return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: err.Error()}
	}
	return result0(StructV(buildLockResponse(id, state))), nil
}

func (s *Server) unlock(ctx context.Context, args *Struct) (*Struct, error) {
	req := args.Struct(1)
	id := req.I64(1)
	if err := s.store.Unlock(ctx, id); err != nil {
		return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: err.Error()}
	}
	return voidResult(), nil
}

// --- Notifications (stubbed: valid empty, never an error — F10) ---

func (s *Server) getCurrentNotificationEventID(_ context.Context, _ *Struct) (*Struct, error) {
	// CurrentNotificationEventId {1: required i64 eventId}.
	return result0(StructV(NewBuilder().I64(notifEventID, 0).Build())), nil
}

func (s *Server) getNextNotification(_ context.Context, _ *Struct) (*Struct, error) {
	// NotificationEventResponse {1: required list<NotificationEvent> events}.
	return result0(StructV(NewBuilder().Add(notifEvents, ListV(thrift.STRUCT, nil)).Build())), nil
}

// --- Partition helpers ---

// stubPartitionBoolTrue is the valid-return stub for
// partition_name_has_valid_characters: the emulator accepts any value the
// client sends, so validating them as "valid" is correct (not a gap).
func (s *Server) stubPartitionBoolTrue(_ context.Context, _ *Struct) (*Struct, error) {
	return result0(BoolV(true)), nil
}

// --- Unsupported (honest failure, not a silent ACK — F10) ---

// unsupportedMethod returns a TApplicationException(INTERNAL_ERROR) for methods
// that are out of scope (ACID txn, heartbeat, niche partition methods, ...). A
// TApplicationException (msg type EXCEPTION) is used rather than a declared
// exception so the failure is unambiguous even for methods with no throws
// clause (get_open_txns, etc.).
func (s *Server) unsupportedMethod(_ context.Context, _ *Struct) (*Struct, error) {
	return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: "method not supported by the emulator"}
}

func (s *Server) heartbeat(_ context.Context, _ *Struct) (*Struct, error) {
	return nil, &AppError{Type: thrift.INTERNAL_ERROR, Message: "heartbeat (ACID transaction locks) is not supported"}
}
