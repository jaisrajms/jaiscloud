package metastore

import (
	"context"
	"errors"
	"strings"
	"testing"

	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hms "jaiscloud/internal/gcp/hms"
	core "jaiscloud/internal/gcp/service/metastore"
	hmsstore "jaiscloud/internal/gcp/store/hms"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

// testCatalogHost is the Hive serving plane as the control plane sees it, backed
// by real in-memory catalogs, so the admin/DR RPCs run end-to-end through the
// proto adapter, the shared core and the catalog codec.
type testCatalogHost struct{ catalogs hmsstore.Catalogs }

func (h testCatalogHost) OpenCatalog(context.Context, string, int) error { return nil }

func (h testCatalogHost) CloseCatalog(ctx context.Context, id string) {
	h.catalogs.DropCatalog(ctx, id)
}

func (h testCatalogHost) Catalog(_ context.Context, id string) (core.CatalogAdmin, error) {
	return hms.NewAdmin(h.catalogs.Catalog(id)), nil
}

// newAdminGRPCService returns a gRPC service whose probe Service owns a real
// (in-memory) Hive Metastore catalog with databases db1/db2, plus that catalog.
func newAdminGRPCService(t *testing.T) (*Service, hmsstore.Store) {
	t.Helper()
	catalogs := hmsstore.NewMemoryCatalogs(nil)
	svc := NewService(core.NewService(metastorestore.NewMemoryStore(),
		core.WithCatalogHost(testCatalogHost{catalogs: catalogs}), core.WithHMSPort(9083)), "proj")
	ctx := context.Background()
	if _, err := svc.CreateService(ctx, &metastorepb.CreateServiceRequest{
		Parent: "projects/proj/locations/us-central1", ServiceId: "svc1",
	}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	store := catalogs.Catalog(core.CatalogID("proj", "us-central1", "svc1"))
	for _, db := range []string{"db1", "db2"} {
		if err := store.CreateDatabase(ctx, hmsstore.Database{Name: db, LocationURI: "gs://wh/" + db}); err != nil {
			t.Fatalf("seed database %s: %v", db, err)
		}
	}
	return svc, store
}

// TestMoveTableToDatabaseGRPC covers the RPC end-to-end: the request names the
// Service, the reply is a done operation carrying the typed
// MoveTableToDatabaseResponse, and the table really moves in the catalog.
func TestMoveTableToDatabaseGRPC(t *testing.T) {
	ctx := context.Background()
	svc, store := newAdminGRPCService(t)

	if err := store.CreateTable(ctx, "db1", "t1", hmsstore.Table{DBName: "db1", TableName: "t1", TableJSON: grpcTestTable("db1", "t1", "gs://wh/db1/t1")}); err != nil {
		t.Fatalf("seed table: %v", err)
	}

	op, err := svc.MoveTableToDatabase(ctx, &metastorepb.MoveTableToDatabaseRequest{
		Service:           "projects/proj/locations/us-central1/services/svc1",
		DbName:            "db1",
		TableName:         "t1",
		DestinationDbName: "db2",
	})
	if err != nil {
		t.Fatalf("MoveTableToDatabase: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("operation not done: %+v", op)
	}
	var meta metastorepb.OperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if meta.GetVerb() != "moveTableToDatabase" || meta.GetTarget() != "projects/proj/locations/us-central1/services/svc1" {
		t.Errorf("metadata = %+v", &meta)
	}
	var resp metastorepb.MoveTableToDatabaseResponse
	if err := op.GetResponse().UnmarshalTo(&resp); err != nil {
		t.Fatalf("unmarshal typed response: %v", err)
	}
	if _, err := store.GetTable(ctx, "db1", "t1"); !errors.Is(err, hmsstore.ErrTableNotFound) {
		t.Fatalf("source table still present: %v", err)
	}
	moved, err := store.GetTable(ctx, "db2", "t1")
	if err != nil {
		t.Fatalf("moved table missing: %v", err)
	}
	if !strings.Contains(string(moved.TableJSON), `["s","db2"]`) {
		t.Fatalf("moved table does not carry the destination database: %s", moved.TableJSON)
	}

	// The operation is resolvable by name once settled (the async-mode poll path
	// reconstructs the typed response from the persisted @type).
	polled, handled, err := svc.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation(%q) = %v, %v", op.GetName(), handled, err)
	}
	var polledResp metastorepb.MoveTableToDatabaseResponse
	if err := polled.GetResponse().UnmarshalTo(&polledResp); err != nil {
		t.Fatalf("polled response type: %v", err)
	}
}

// TestAlterMetadataResourceLocationGRPC covers the RPC for all three addressed
// resources, plus the failure shapes (NotFound for a missing catalog resource,
// InvalidArgument for a malformed request).
func TestAlterMetadataResourceLocationGRPC(t *testing.T) {
	ctx := context.Background()
	svc, store := newAdminGRPCService(t)
	if err := store.CreateTable(ctx, "db1", "t1", hmsstore.Table{DBName: "db1", TableName: "t1", TableJSON: grpcTestTable("db1", "t1", "gs://wh/db1/t1")}); err != nil {
		t.Fatalf("seed table: %v", err)
	}
	if err := store.CreatePartition(ctx, "db1", "t1", hmsstore.Partition{
		DBName: "db1", TableName: "t1", Values: []string{"2026-01-01"},
		PartJSON: grpcTestPartition("db1", "t1", "2026-01-01", "gs://wh/db1/t1/dt=2026-01-01"),
	}); err != nil {
		t.Fatalf("seed partition: %v", err)
	}

	alter := func(resource, location string) error {
		op, err := svc.AlterMetadataResourceLocation(ctx, &metastorepb.AlterMetadataResourceLocationRequest{
			Service:      "projects/proj/locations/us-central1/services/svc1",
			ResourceName: resource,
			LocationUri:  location,
		})
		if err != nil {
			return err
		}
		if !op.GetDone() {
			return errors.New("operation not done")
		}
		var resp metastorepb.AlterMetadataResourceLocationResponse
		return op.GetResponse().UnmarshalTo(&resp)
	}

	if err := alter("databases/db1", "gs://wh/db1-moved"); err != nil {
		t.Fatalf("alter database: %v", err)
	}
	if db, err := store.GetDatabase(ctx, "db1"); err != nil || db.LocationURI != "gs://wh/db1-moved" {
		t.Fatalf("database location = %q, %v", db.LocationURI, err)
	}
	if err := alter("databases/db1/tables/t1", "gs://wh/table-moved"); err != nil {
		t.Fatalf("alter table: %v", err)
	}
	if tbl, err := store.GetTable(ctx, "db1", "t1"); err != nil || !strings.Contains(string(tbl.TableJSON), "gs://wh/table-moved") {
		t.Fatalf("table location not altered: %v", err)
	}
	if err := alter("databases/db1/tables/t1/partitions/dt=2026-01-01", "gs://wh/part-moved"); err != nil {
		t.Fatalf("alter partition: %v", err)
	}
	if p, err := store.GetPartition(ctx, "db1", "t1", []string{"2026-01-01"}); err != nil || !strings.Contains(string(p.PartJSON), "gs://wh/part-moved") {
		t.Fatalf("partition location not altered: %v", err)
	}

	if err := alter("databases/nope", "gs://wh/x"); status.Code(err) != codes.NotFound {
		t.Fatalf("missing database: code = %v, want NotFound", status.Code(err))
	}
	if err := alter("db1", "gs://wh/x"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed resourceName: code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := svc.AlterMetadataResourceLocation(ctx, &metastorepb.AlterMetadataResourceLocationRequest{
		Service:      "projects/proj/locations/us-central1/services/svc1",
		ResourceName: "databases/db1",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing locationUri: code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := svc.MoveTableToDatabase(ctx, &metastorepb.MoveTableToDatabaseRequest{
		Service: "projects/proj/locations/us-central1/services/nope", DbName: "db1", TableName: "t1", DestinationDbName: "db2",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown service: code = %v, want NotFound", status.Code(err))
	}
}

// grpcTestTable builds a canonical Hive Table JSON with a storage location.
func grpcTestTable(db, name, location string) []byte {
	st := hms.NewBuilder().
		Str(1, name).
		Str(2, db).
		Struct(7, hms.NewBuilder().Str(2, location).Build()).
		Build()
	raw, _ := hms.MarshalStruct(st)
	return raw
}

// grpcTestPartition builds a canonical Hive Partition JSON with a storage
// location.
func grpcTestPartition(db, name, value, location string) []byte {
	st := hms.NewBuilder().
		ListStr(1, []string{value}).
		Str(2, db).
		Str(3, name).
		Struct(6, hms.NewBuilder().Str(2, location).Build()).
		Build()
	raw, _ := hms.MarshalStruct(st)
	return raw
}
