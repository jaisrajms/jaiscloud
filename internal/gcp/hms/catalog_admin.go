package hms

import (
	"context"
	"encoding/json"

	hmsstore "jaiscloud/internal/gcp/store/hms"
)

// AdminStore applies the Dataproc Metastore control plane's catalog-level admin
// mutations to ONE catalog: services.moveTableToDatabase and
// services.alterLocation act on the Hive Metastore catalog, not on the
// control-plane record, so they are implemented here, where the Hive struct
// codec lives. The control-plane core
// (internal/gcp/service/metastore) drives these through its CatalogAdmin seam,
// so it never depends on the serving plane's wire encoding — the same split as
// CatalogHost.
type AdminStore struct{ store hmsstore.Store }

// NewAdmin returns the admin surface of one catalog.
func NewAdmin(store hmsstore.Store) *AdminStore { return &AdminStore{store: store} }

// hiveDBNameField is Table.db_name and Partition.db_name: both structs carry the
// database at field 2 (hive_metastore.thrift), one below the table name at 3.
const hiveDBNameField int16 = 2

// MoveTable moves a table to another database, keeping its name, its stored
// Hive Table struct and its partitions — real Dataproc Metastore moves catalog
// metadata only and copies no data. The stored structs are rewritten so the
// Table and Partition records the Thrift plane returns carry the destination
// database, and the store applies the move and the rewrites as one atomic step.
// A move into the table's own database verifies the table exists and then
// changes nothing (the request is satisfied, not an AlreadyExists collision with
// itself).
func (a *AdminStore) MoveTable(ctx context.Context, db, tableName, destDB string) error {
	if db == destDB {
		_, err := a.store.GetTable(ctx, db, tableName)
		return err
	}
	_, err := a.store.MoveTable(ctx, db, tableName, destDB, func(raw json.RawMessage) (json.RawMessage, error) {
		st, err := UnmarshalStruct(raw)
		if err != nil {
			return nil, err
		}
		st.Set(hiveDBNameField, StringV(destDB))
		return MarshalStruct(st)
	})
	return err
}

// SetDatabaseLocation re-points a database's root directory — Hive's
// ALTER DATABASE … SET LOCATION. The database's tables keep the locations their
// own storage descriptors name, exactly as Hive leaves them.
func (a *AdminStore) SetDatabaseLocation(ctx context.Context, db, locationURI string) error {
	current, err := a.store.GetDatabase(ctx, db)
	if err != nil {
		return err
	}
	current.LocationURI = locationURI
	return a.store.AlterDatabase(ctx, db, current)
}

// SetTableLocation re-points a table's storage descriptor root — Hive's
// ALTER TABLE … SET LOCATION. Stored partitions keep their own locations, as
// Hive leaves them (and as the dataproc-metastore alterLocation contract says:
// only the addressed resource's parent directory changes).
func (a *AdminStore) SetTableLocation(ctx context.Context, db, tableName, locationURI string) error {
	_, err := a.store.AlterTable(ctx, db, tableName, func(current hmsstore.Table) (hmsstore.Table, error) {
		raw, err := rewriteSDLocation(current.TableJSON, tblSD, locationURI)
		if err != nil {
			return hmsstore.Table{}, err
		}
		current.TableJSON = raw
		return current, nil
	})
	return err
}

// SetPartitionLocation re-points one partition's storage descriptor root.
func (a *AdminStore) SetPartitionLocation(ctx context.Context, db, tableName string, values []string, locationURI string) error {
	_, err := a.store.AlterPartition(ctx, db, tableName, values, func(current hmsstore.Partition) (hmsstore.Partition, error) {
		raw, err := rewriteSDLocation(current.PartJSON, partSD, locationURI)
		if err != nil {
			return hmsstore.Partition{}, err
		}
		current.PartJSON = raw
		return current, nil
	})
	return err
}

// rewriteSDLocation sets the storage descriptor's location (field 2 of
// StorageDescriptor) of the struct that carries it at sdField (Table.sd=7,
// Partition.sd=6), leaving every other field of the struct intact. A Table or
// Partition stored without a storage descriptor gains one.
func rewriteSDLocation(raw json.RawMessage, sdField int16, locationURI string) (json.RawMessage, error) {
	st, err := UnmarshalStruct(raw)
	if err != nil {
		return nil, err
	}
	sd := st.Struct(sdField)
	if sd == nil {
		sd = &Struct{}
	}
	sd.Set(sdLocation, StringV(locationURI))
	st.Set(sdField, StructV(sd))
	return MarshalStruct(st)
}
