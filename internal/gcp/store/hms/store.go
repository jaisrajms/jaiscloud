// Package hms provides the DPMS Hive Metastore serving-plane store. It is the
// GCP analogue of the AWS Glue table-metadata store, but for the Hive
// Metastore protocol (hive_metastore.thrift) rather than Glue's REST surface.
//
// The serving plane is a single global catalog: databases and tables are keyed
// by name only, not by projects/{p}/locations/{l}/services/{s}; the per-Service
// endpoint_uri emitted by the control plane is cosmetic. Databases are
// decomposed columns; tables are stored as the full, round-trippable Hive
// Table JSON blob. Locks are the minimal jc_hms_locks state machine that backs
// Iceberg's lock/check_lock/unlock flow.
package hms

import (
	"context"
	"encoding/json"
	"errors"
)

var (
	// ErrDatabaseExists is returned when creating a database that already
	// exists.
	ErrDatabaseExists = errors.New("DatabaseExists")
	// ErrDatabaseNotFound is returned when addressing a missing database.
	ErrDatabaseNotFound = errors.New("DatabaseNotFound")
	// ErrDatabaseNotEmpty is returned when dropping a database that still
	// holds tables (and cascade was not requested).
	ErrDatabaseNotEmpty = errors.New("DatabaseNotEmpty")
	// ErrTableExists is returned when creating a table that already exists or
	// renaming onto an existing destination.
	ErrTableExists = errors.New("TableExists")
	// ErrTableNotFound is returned when addressing a missing table.
	ErrTableNotFound = errors.New("TableNotFound")
	// ErrPartitionExists is returned when creating a partition whose values
	// already exist, or renaming onto an existing partition.
	ErrPartitionExists = errors.New("PartitionExists")
	// ErrPartitionNotFound is returned when addressing a missing partition.
	ErrPartitionNotFound = errors.New("PartitionNotFound")
)

// LockState is the on-wire Hive LockState enum value (hive_metastore.thrift).
// ACQUIRED=1, WAITING=2, ABORT=3, NOT_ACQUIRED=4.
type LockState int32

const (
	LockStateAcquired    LockState = 1
	LockStateWaiting     LockState = 2
	LockStateAbort       LockState = 3
	LockStateNotAcquired LockState = 4
)

// Database is a Hive database record. Parameters and owner map directly to the
// wire Database fields name(1)/description(2)/locationUri(3)/parameters(4)/
// ownerName(6); ownerType/catalogName/privileges are not persisted (the
// Iceberg-on-Hive critical path only reads locationUri and parameters).
type Database struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	LocationURI string            `json:"locationUri"`
	Parameters  map[string]string `json:"parameters"`
	Owner       string            `json:"owner"`
}

// Table is a Hive table record: the full Hive Table struct encoded as the
// codec's canonical (type-tagged, lossless) JSON. The codec owns the JSON
// shape; the store persists and returns it verbatim so get_table -> alter_table
// round-trips every field the client sent (F5).
type Table struct {
	DBName    string          `json:"dbName"`
	TableName string          `json:"tableName"`
	TableJSON json.RawMessage `json:"table"`
}

// Partition is a Hive partition record: the full Hive Partition struct encoded
// as the codec's canonical (type-tagged, lossless) JSON, keyed by
// (dbName, tableName, values). Like Table, the JSON is persisted and returned
// verbatim so get_partition -> alter_partition round-trips every field the
// client sent (F5). Values is the ordered partition-key tuple
// (Partition.values, field 1); it is the natural key and is also persisted
// separately so both backends can address a partition without decoding the
// codec-owned JSON.
type Partition struct {
	DBName    string          `json:"dbName"`
	TableName string          `json:"tableName"`
	Values    []string        `json:"values"`
	PartJSON  json.RawMessage `json:"partition"`
}

// PartitionKey returns the canonical store key for a partition value tuple.
// Both backends derive it identically so memory and Postgres keys agree; the
// JSON encoding keeps arbitrary value strings (including "/" and "=") from
// colliding across different tuples. A nil slice is normalized to an empty one.
func PartitionKey(values []string) string {
	if values == nil {
		values = []string{}
	}
	b, _ := json.Marshal(values)
	return string(b)
}

// Lock is the minimal record a lock() call captures: the first LockComponent's
// db/table plus the request's user/hostname.
type Lock struct {
	DBName    string
	TableName string
	User      string
	Hostname  string
}

// Store is the Hive Metastore serving-plane store. Both the memory and postgres
// backends implement it, plus Reset/Snapshot/Restore/IsEmpty.
type Store interface {
	CreateDatabase(ctx context.Context, db Database) error
	GetDatabase(ctx context.Context, name string) (Database, error)
	// ListDatabases returns every database name in sorted order.
	ListDatabases(ctx context.Context) ([]string, error)
	// DropDatabase removes a database. With cascade it also removes the
	// database's tables; without cascade a non-empty database returns
	// ErrDatabaseNotEmpty.
	DropDatabase(ctx context.Context, name string, cascade bool) error
	// AlterDatabase unconditionally overwrites a database (missing database ->
	// ErrDatabaseNotFound).
	AlterDatabase(ctx context.Context, name string, db Database) error

	CreateTable(ctx context.Context, dbName, tableName string, t Table) error
	GetTable(ctx context.Context, dbName, tableName string) (Table, error)
	// ListTables returns every table name in a database in sorted order.
	ListTables(ctx context.Context, dbName string) ([]string, error)
	DropTable(ctx context.Context, dbName, tableName string) error
	// AlterTable atomically applies mutate to the table identified by
	// (dbName, tableName), returning the committed Table. The mutate closure is
	// evaluated against the current table under the store's lock (or within a
	// Serializable transaction), and an error returned from mutate aborts
	// without writing. Missing table -> ErrTableNotFound. This is a plain
	// overwrite inside the closure (D5) — the lock is the only concurrency
	// control, matching real Hive alter_table.
	AlterTable(ctx context.Context, dbName, tableName string, mutate func(Table) (Table, error)) (Table, error)
	// RenameTable atomically moves a table to a new (dbName, tableName).
	// Missing source -> ErrTableNotFound; existing destination ->
	// ErrTableExists; missing destination database -> ErrDatabaseNotFound.
	RenameTable(ctx context.Context, srcDB, srcName, dstDB, dstName string, t Table) (Table, error)

	// CreatePartition adds a partition to an existing table. Missing table ->
	// ErrTableNotFound; duplicate (dbName, tableName, values) ->
	// ErrPartitionExists.
	CreatePartition(ctx context.Context, dbName, tableName string, p Partition) error
	// GetPartition returns the partition identified by its value tuple.
	// Missing table or partition -> ErrTableNotFound / ErrPartitionNotFound.
	GetPartition(ctx context.Context, dbName, tableName string, values []string) (Partition, error)
	// ListPartitions returns every partition of a table, ordered by value
	// tuple for deterministic responses.
	ListPartitions(ctx context.Context, dbName, tableName string) ([]Partition, error)
	// AlterPartition atomically applies mutate to the partition identified by
	// (dbName, tableName, values), returning the committed Partition. The
	// mutate closure runs under the store's lock (or within a Serializable
	// transaction) and an error aborts without writing. Missing table or
	// partition -> ErrTableNotFound / ErrPartitionNotFound. The address tuple
	// does not change (use RenamePartition to re-key).
	AlterPartition(ctx context.Context, dbName, tableName string, values []string, mutate func(Partition) (Partition, error)) (Partition, error)
	// DropPartition removes the partition identified by its value tuple.
	// Missing -> ErrPartitionNotFound.
	DropPartition(ctx context.Context, dbName, tableName string, values []string) error
	// RenamePartition atomically re-keys a partition from oldValues to the
	// values in p. Missing source -> ErrPartitionNotFound; existing
	// destination -> ErrPartitionExists.
	RenamePartition(ctx context.Context, dbName, tableName string, oldValues, newValues []string, p Partition) (Partition, error)

	// Lock persists a row with state=ACQUIRED and returns its monotonically
	// increasing lock id.
	Lock(ctx context.Context, l Lock) (int64, error)
	// CheckLock returns the stored lock state, or LockStateNotAcquired if the
	// lock is absent (D2 — the client's polling check_lock must receive a sane
	// value, not an error).
	CheckLock(ctx context.Context, id int64) (LockState, error)
	// Unlock deletes the lock idempotently.
	Unlock(ctx context.Context, id int64) error

	Reset(ctx context.Context)
}
