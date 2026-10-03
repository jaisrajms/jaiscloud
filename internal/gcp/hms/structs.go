package hms

import hmsstore "jaiscloud/internal/gcp/store/hms"

// Field IDs and enum values derived from hive_metastore.thrift (Apache Hive
// branch-2.3, metastore/if/hive_metastore.thrift). Only the fields the serving
// plane actually reads/builds are named here; the codec itself is schema-free,
// so every other field round-trips generically.

// Database fields.
const (
	dbName        int16 = 1
	dbDescription int16 = 2
	dbLocationURI int16 = 3
	dbParameters  int16 = 4
	dbOwnerName   int16 = 6
	dbOwnerType   int16 = 7
)

// Table fields.
const (
	tblTableName     int16 = 1
	tblDBName        int16 = 2
	tblSD            int16 = 7
	tblPartitionKeys int16 = 8
	tblParameters    int16 = 9
	tblTableType     int16 = 12
	tblCatName       int16 = 16
)

// TableMeta fields (hive_metastore.thrift). 1-4 are the branch-2.3 set; 5
// (catName) is Hive 3.x, and 6-7 (ownerName/ownerType) are Hive 4. The serving
// plane emits 1-4 plus catName when the stored Table carries one; older clients
// skip the unknown optional field.
const (
	tmDBName    int16 = 1
	tmTableName int16 = 2
	tmTableType int16 = 3
	tmComments  int16 = 4
	tmCatName   int16 = 5
)

// tableCommentKey is the Table.parameters key Hive uses to store the table
// comment; get_table_meta surfaces it as TableMeta.comments.
const tableCommentKey = "comment"

// get_table_req / get_table_objects_by_name_req request and result fields
// (hive_metastore.thrift, Hive 2.3+). Both requests carry dbName (1) and either
// tblName (2) or tblNames (2), plus an optional catName at field 4 (the older
// get_table positional form had no catName; the request structs do). The reply
// wraps the table(s) inside GetTableResult.table / GetTablesResult.tables at
// field 1, which is itself the result-struct's field 0.
const (
	reqDBName       int16 = 1
	reqTblName      int16 = 2 // get_table_req: string
	reqTblNames     int16 = 2 // get_table_objects_by_name_req: list<string>
	reqCatName      int16 = 4
	reqResultTable  int16 = 1 // GetTableResult.table
	reqResultTables int16 = 1 // GetTablesResult.tables
)

// Partition fields (hive_metastore.thrift Partition struct).
const (
	partValues         int16 = 1
	partDBName         int16 = 2
	partTableName      int16 = 3
	partCreateTime     int16 = 4
	partLastAccessTime int16 = 5
	partSD             int16 = 6
	partParameters     int16 = 7
	partPrivileges     int16 = 8
)

// StorageDescriptor fields (only the ones read here).
const (
	sdLocation int16 = 2
)

// FieldSchema fields.
const (
	fsName int16 = 1
	fsType int16 = 2
)

// LockComponent fields.
const (
	lcDBName    int16 = 3
	lcTableName int16 = 4
)

// LockRequest fields.
const (
	lrComponent int16 = 1
	lrUser      int16 = 3
	lrHostname  int16 = 4
)

// LockResponse fields.
const (
	lockRespLockID int16 = 1
	lockRespState  int16 = 2
)

// Notification structs.
const (
	notifEventID int16 = 1
	notifEvents  int16 = 1
)

// PrincipalType enum.
const (
	principalTypeUser  int32 = 1
	principalTypeRole  int32 = 2
	principalTypeGroup int32 = 3
)

// buildDatabaseStruct renders a store Database as a wire Database struct,
// emitting fields in ascending field-ID order (1,2,3,4,6,7). ownerType defaults
// to USER; privileges and catalogName are not persisted and are omitted.
func buildDatabaseStruct(db hmsstore.Database) *Struct {
	b := NewBuilder().Str(dbName, db.Name)
	if db.Description != "" {
		b.Str(dbDescription, db.Description)
	}
	if db.LocationURI != "" {
		b.Str(dbLocationURI, db.LocationURI)
	}
	b.MapStrStr(dbParameters, db.Parameters)
	if db.Owner != "" {
		b.Str(dbOwnerName, db.Owner)
		b.I32(dbOwnerType, principalTypeUser)
	}
	return b.Build()
}

// buildTableMeta renders a stored Table as the wire TableMeta struct
// (hive_metastore.thrift): the required dbName/tableName/tableType, plus the
// optional comments (the Table's "comment" parameter) and catName when present.
// Fields are emitted in ascending ID order (1,2,3,4[,5]) so hand-encoded golden
// fixtures round-trip byte-for-byte.
func buildTableMeta(dbName, tableName, tableType string, t *Struct) *Struct {
	b := NewBuilder().
		Str(tmDBName, dbName).
		Str(tmTableName, tableName).
		Str(tmTableType, tableType)
	if comment := t.MapStrStr(tblParameters)[tableCommentKey]; comment != "" {
		b.Str(tmComments, comment)
	}
	if cat := t.String(tblCatName); cat != "" {
		b.Str(tmCatName, cat)
	}
	return b.Build()
}

// buildLockResponse renders (lockid, state) as a LockResponse struct.
func buildLockResponse(lockID int64, state hmsstore.LockState) *Struct {
	return NewBuilder().
		I64(lockRespLockID, lockID).
		I32(lockRespState, int32(state)).
		Build()
}
