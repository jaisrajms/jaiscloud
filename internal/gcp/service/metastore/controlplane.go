package metastore

import (
	"context"
	"errors"
	"strings"

	hmsstore "jaiscloud/internal/gcp/store/hms"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
	"jaiscloud/internal/model"
)

// Admin/DR operations of the Dataproc Metastore v1 surface, as documented by the
// Discovery document and the pinned proto:
//
//	POST v1/{service}:moveTableToDatabase  MoveTableToDatabaseRequest  -> Operation
//	POST v1/{service}:alterLocation        AlterMetadataResourceLocationRequest -> Operation
//
// Both mutate the Service's Hive Metastore catalog (not the control-plane
// record), so they run against the Service's own catalog through the
// CatalogAdmin seam and return a done operation carrying the corresponding
// empty *Response message.
//
// The other three — ExportMetadata, RestoreService, QueryMetadata — remain
// explicit Unimplemented failures in both transports. They are not modelled here
// because they would have to invent a dump/restore/query format: ExportMetadata
// must write a MySQL dump or Avro files of the metastore backend's RDBMS schema
// (DBS/TBLS/SDS/...), RestoreService must consume such an artifact (or a backup
// that contains one), and QueryMetadata must run read-only SQL against that same
// RDBMS schema and publish the result manifest to Cloud Storage. The emulator
// stores tables as Hive Thrift JSON in its own store, so serving any of the
// three would report a success it cannot back with real data. See the recorded
// deferrals in plan_docs (GA9 / MP12-MP15).

// MoveTableToDatabase moves a table to another database in the Service's own
// catalog and returns the done operation. Only catalog metadata moves: the table
// keeps its name, its Hive Table struct (including its storage location) and its
// partitions, exactly like real Dataproc Metastore (which copies no data).
func (s *Service) MoveTableToDatabase(ctx context.Context, project, location, serviceID, dbName, tableName, destDBName string) (metastorestore.Operation, error) {
	if location == "" || serviceID == "" {
		return metastorestore.Operation{}, invalidArgument("missing location or serviceId")
	}
	if dbName == "" || tableName == "" || destDBName == "" {
		return metastorestore.Operation{}, invalidArgument("dbName, tableName and destinationDbName are required")
	}
	admin, err := s.catalogAdmin(ctx, project, location, serviceID)
	if err != nil {
		return metastorestore.Operation{}, err
	}
	if err := admin.MoveTable(ctx, dbName, tableName, destDBName); err != nil {
		return metastorestore.Operation{}, catalogErr(err, dbName+"."+tableName)
	}
	target := ServiceName(project, location, serviceID)
	op, err := s.storeOperation(ctx, project, location, "moveTableToDatabase", target,
		operationResponse(moveTableResponseTypeURL, nil))
	if err != nil {
		return metastorestore.Operation{}, err
	}
	return op, nil
}

// AlterMetadataResourceLocation re-points one metadata resource's parent
// directory — a database, a table, or a partition — in the Service's own
// catalog, and returns the done operation. As the API documents, only the
// address of the metadata is updated: no data is moved, and the resource's
// children keep the locations their own descriptors name.
//
// resourceName is the relative metadata resource name the request carries:
//
//	databases/{database_id}
//	databases/{database_id}/tables/{table_id}
//	databases/{database_id}/tables/{table_id}/partitions/{partition_id}
//
// The partition id is the Hive partition name (`dt=2026-01-01`, normally with one
// `key=value` segment per partition key); a segment without `=` is taken as the
// value itself, so a plain value tuple works too.
func (s *Service) AlterMetadataResourceLocation(ctx context.Context, project, location, serviceID, resourceName, locationURI string) (metastorestore.Operation, error) {
	if location == "" || serviceID == "" {
		return metastorestore.Operation{}, invalidArgument("missing location or serviceId")
	}
	if resourceName == "" || locationURI == "" {
		return metastorestore.Operation{}, invalidArgument("resourceName and locationUri are required")
	}
	res, err := parseMetadataResource(resourceName)
	if err != nil {
		return metastorestore.Operation{}, err
	}
	admin, err := s.catalogAdmin(ctx, project, location, serviceID)
	if err != nil {
		return metastorestore.Operation{}, err
	}
	switch {
	case res.table == "":
		err = admin.SetDatabaseLocation(ctx, res.database, locationURI)
	case res.values == nil:
		err = admin.SetTableLocation(ctx, res.database, res.table, locationURI)
	default:
		err = admin.SetPartitionLocation(ctx, res.database, res.table, res.values, locationURI)
	}
	if err != nil {
		return metastorestore.Operation{}, catalogErr(err, resourceName)
	}
	target := ServiceName(project, location, serviceID)
	op, err := s.storeOperation(ctx, project, location, "alterLocation", target,
		operationResponse(alterLocationResponseTypeURL, nil))
	if err != nil {
		return metastorestore.Operation{}, err
	}
	return op, nil
}

// metadataResource is a parsed relative metadata resource name.
type metadataResource struct {
	database string
	table    string
	// values is nil when the resource is not a partition (an empty slice is a
	// partition of a table with no partition keys, which no stored partition
	// matches).
	values []string
}

// parseMetadataResource parses the relative metadata resource name
// databases/{db}[/tables/{t}[/partitions/{partition_name}]]. An unrecognized
// shape is InvalidArgument rather than a guess, because the name decides which
// resource is mutated.
func parseMetadataResource(name string) (metadataResource, error) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) < 2 || segs[0] != "databases" || segs[1] == "" {
		return metadataResource{}, invalidArgument("resourceName must be databases/{database_id}[/tables/{table_id}[/partitions/{partition_id}]]")
	}
	res := metadataResource{database: segs[1]}
	if len(segs) == 2 {
		return res, nil
	}
	if len(segs) < 4 || segs[2] != "tables" || segs[3] == "" {
		return metadataResource{}, invalidArgument("resourceName must be databases/{database_id}[/tables/{table_id}[/partitions/{partition_id}]]")
	}
	res.table = segs[3]
	if len(segs) == 4 {
		return res, nil
	}
	if segs[4] != "partitions" || len(segs) < 6 {
		return metadataResource{}, invalidArgument("resourceName must be databases/{database_id}[/tables/{table_id}[/partitions/{partition_id}]]")
	}
	// The partition name itself is slash-separated (including the trailing
	// empty value Hive tolerates), so it may span several URL segments.
	res.values = partitionNameValues(strings.Join(segs[5:], "/"))
	return res, nil
}

// partitionNameValues splits a Hive partition name into its value tuple: one
// `key=value` segment per key, where a segment without `=` is the value itself
// (Hive's own partition_name_to_vals does the same).
func partitionNameValues(name string) []string {
	segs := strings.Split(name, "/")
	values := make([]string, 0, len(segs))
	for _, seg := range segs {
		if i := strings.IndexByte(seg, '='); i >= 0 {
			seg = seg[i+1:]
		}
		values = append(values, seg)
	}
	return values
}

// catalogAdmin resolves the admin surface of the Service's own Hive Metastore.
// A missing Service is NotFound; a deployment that serves no catalog for it is a
// failed precondition — never a fabricated success.
func (s *Service) catalogAdmin(ctx context.Context, project, location, serviceID string) (CatalogAdmin, error) {
	if _, err := s.store.GetService(ctx, project, location, serviceID); err != nil {
		return nil, mapErr(err)
	}
	if s.catalogHost == nil {
		return nil, noCatalogError()
	}
	admin, err := s.catalogHost.Catalog(ctx, CatalogID(project, location, serviceID))
	if err != nil {
		return nil, noCatalogError()
	}
	if admin == nil {
		return nil, noCatalogError()
	}
	return admin, nil
}

// noCatalogError is the failure for a Service whose Hive Metastore this
// deployment does not serve.
func noCatalogError() error {
	return model.NewProviderError("FailedPrecondition",
		"the emulator serves no Hive Metastore catalog for this service, so no metadata can be mutated", 412)
}

// catalogErr maps a serving-plane store error onto the control plane's wire
// error. resource is the catalog resource the request addressed, for the
// message.
func catalogErr(err error, resource string) error {
	switch {
	case errors.Is(err, hmsstore.ErrDatabaseNotFound):
		return model.NewProviderError("NotFound", "database not found: "+resource, 404)
	case errors.Is(err, hmsstore.ErrTableNotFound):
		return model.NewProviderError("NotFound", "table not found: "+resource, 404)
	case errors.Is(err, hmsstore.ErrPartitionNotFound):
		return model.NewProviderError("NotFound", "partition not found: "+resource, 404)
	case errors.Is(err, hmsstore.ErrTableExists):
		return model.NewProviderError("AlreadyExists", "a table with that name already exists in the destination database: "+resource, 409)
	case errors.Is(err, hmsstore.ErrDatabaseExists):
		return model.NewProviderError("AlreadyExists", "database already exists: "+resource, 409)
	case errors.Is(err, hmsstore.ErrDatabaseNotEmpty):
		return model.NewProviderError("FailedPrecondition", "database is not empty: "+resource, 412)
	case errors.Is(err, hmsstore.ErrPartitionExists):
		return model.NewProviderError("AlreadyExists", "partition already exists: "+resource, 409)
	default:
		return err
	}
}
