// Package iceberg provides the BigLake Metastore Iceberg REST Catalog store:
// namespaces (multi-level, levels joined with "/") and tables (a full
// TableMetadata JSON blob plus a metadata-location pointer, the table UUID,
// and a monotonically increasing version). BigLake Metastore is GCP's
// managed-Iceberg product, and its catalog is the standard Apache Iceberg REST
// catalog surface, mounted at /iceberg/v1/... rather than under
// projects/{project}/locations/{location}.
//
// The commit path is the crux of the catalog: CommitTable applies a mutate
// closure to a table under one lock (memory) or a Serializable SELECT ... FOR
// UPDATE transaction (postgres), so concurrent commits to the same table
// cannot lose updates and a failing requirement leaves the stored state
// untouched.
package iceberg

import (
	"context"
	"encoding/json"
	"errors"
)

var (
	// ErrNamespaceExists is returned when creating a namespace that already
	// exists.
	ErrNamespaceExists = errors.New("NamespaceExists")
	// ErrNamespaceNotFound is returned when addressing a missing namespace.
	ErrNamespaceNotFound = errors.New("NamespaceNotFound")
	// ErrNamespaceNotEmpty is returned when dropping a namespace that still
	// holds tables.
	ErrNamespaceNotEmpty = errors.New("NamespaceNotEmpty")
	// ErrTableExists is returned when creating a table that already exists or
	// renaming onto an existing destination.
	ErrTableExists = errors.New("TableExists")
	// ErrTableNotFound is returned when addressing a missing table.
	ErrTableNotFound = errors.New("TableNotFound")
)

// Namespace is a named set of table-grouping properties. Namespace holds the
// namespace string with levels joined by "/" (the emulator's wire convention,
// matching the store's addressing scheme).
type Namespace struct {
	Namespace  string            `json:"namespace"`
	Properties map[string]string `json:"properties"`
}

// Table is a table record: the full Iceberg TableMetadata JSON (Metadata),
// plus the derived metadata-location pointer, the table UUID, and the current
// monotonically increasing version.
type Table struct {
	Namespace        string          `json:"namespace"`
	Name             string          `json:"name"`
	Metadata         json.RawMessage `json:"metadata"`
	MetadataLocation string          `json:"metadataLocation"`
	UUID             string          `json:"uuid"`
	Version          int             `json:"version"`
}

// NamespacePropertiesUpdate is the atomic result of UpdateNamespaceProperties:
// the property map before the update (used by the provider to report which
// removals actually removed a key vs. referred to a missing one) and the map
// after. Both snapshots come from the same locked read, so the removed/missing
// split cannot race a concurrent update.
type NamespacePropertiesUpdate struct {
	Before map[string]string
	After  map[string]string
}

// Store is the Iceberg catalog store. Both the memory and postgres backends
// implement it, plus Reset/Snapshot/Restore/IsEmpty.
type Store interface {
	CreateNamespace(ctx context.Context, namespace string, properties map[string]string) error
	GetNamespace(ctx context.Context, namespace string) (map[string]string, error)
	ListNamespaces(ctx context.Context) ([]Namespace, error)
	NamespaceExists(ctx context.Context, namespace string) (bool, error)
	// UpdateNamespaceProperties applies removals then updates to a namespace's
	// properties atomically and returns the before/after property maps (so the
	// provider can report removed/missing without a separate GetNamespace read
	// that would race a concurrent writer). Missing namespace ->
	// ErrNamespaceNotFound.
	UpdateNamespaceProperties(ctx context.Context, namespace string, removals []string, updates map[string]string) (NamespacePropertiesUpdate, error)
	// DropNamespace removes a namespace, refusing with ErrNamespaceNotEmpty if
	// it still holds tables.
	DropNamespace(ctx context.Context, namespace string) error

	CreateTable(ctx context.Context, namespace, name string, t Table) error
	GetTable(ctx context.Context, namespace, name string) (Table, error)
	ListTables(ctx context.Context, namespace string) ([]Table, error)
	DropTable(ctx context.Context, namespace, name string) error
	// CommitTable atomically applies mutate to the table identified by
	// (namespace, name), returning the committed Table. The mutate closure is
	// evaluated against the current table under the store's lock (or within a
	// Serializable transaction), and an error returned from mutate aborts
	// without writing. Missing table -> ErrTableNotFound.
	CommitTable(ctx context.Context, namespace, name string, mutate func(Table) (Table, error)) (Table, error)
	// RenameTable atomically moves a table to a new (namespace, name). Missing
	// source -> ErrTableNotFound; existing destination -> ErrTableExists.
	RenameTable(ctx context.Context, srcNamespace, srcName, dstNamespace, dstName string) (Table, error)

	Reset(ctx context.Context)
}
