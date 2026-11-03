package metastore

import (
	"context"
	"errors"
	"strings"
	"testing"

	hms "jaiscloud/internal/gcp/hms"
	metastorecore "jaiscloud/internal/gcp/service/metastore"
	hmsstore "jaiscloud/internal/gcp/store/hms"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
	"jaiscloud/internal/model"
)

// testCatalogHost is the Hive serving plane as the control plane sees it,
// backed by real in-memory catalogs: the two admin/DR verbs therefore run
// end-to-end through the REST adapter, the shared core, and the catalog codec.
type testCatalogHost struct{ catalogs hmsstore.Catalogs }

func (h testCatalogHost) OpenCatalog(context.Context, string, int) error { return nil }

func (h testCatalogHost) CloseCatalog(ctx context.Context, id string) {
	h.catalogs.DropCatalog(ctx, id)
}

func (h testCatalogHost) Catalog(_ context.Context, id string) (metastorecore.CatalogAdmin, error) {
	return hms.NewAdmin(h.catalogs.Catalog(id)), nil
}

// newAdminProvider returns a provider whose Service owns a real (in-memory)
// Hive Metastore catalog, and that catalog.
func newAdminProvider(t *testing.T) (*Provider, hmsstore.Store) {
	t.Helper()
	catalogs := hmsstore.NewMemoryCatalogs(nil)
	core := metastorecore.NewService(metastorestore.NewMemoryStore(),
		metastorecore.WithCatalogHost(testCatalogHost{catalogs: catalogs}), metastorecore.WithHMSPort(9083))
	p := NewProvider(core, "proj")
	ctx := context.Background()
	if _, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc1", nil))); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	store := catalogs.Catalog(metastorecore.CatalogID("proj", "us-central1", "svc1"))
	for _, db := range []string{"db1", "db2"} {
		if err := store.CreateDatabase(ctx, hmsstore.Database{Name: db, LocationURI: "gs://wh/" + db}); err != nil {
			t.Fatalf("seed database %s: %v", db, err)
		}
	}
	return p, store
}

// TestMoveTableToDatabaseREST covers the REST custom method end-to-end: the
// request body carries dbName/tableName/destinationDbName, the response is a
// done operation carrying MoveTableToDatabaseResponse, and the table (with its
// partitions) really moves in the Service's catalog.
func TestMoveTableToDatabaseREST(t *testing.T) {
	ctx := context.Background()
	p, store := newAdminProvider(t)

	if err := store.CreateTable(ctx, "db1", "t1", hmsstore.Table{DBName: "db1", TableName: "t1", TableJSON: restTestTable("db1", "t1", "gs://wh/db1/t1")}); err != nil {
		t.Fatalf("seed table: %v", err)
	}

	resp, err := p.MoveTableToDatabase(ctx, newNR(map[string]any{
		"location": "us-central1", "serviceId": "svc1",
		"body": map[string]any{"dbName": "db1", "tableName": "t1", "destinationDbName": "db2"},
	}))
	if err != nil {
		t.Fatalf("MoveTableToDatabase: %v", err)
	}
	if resp.Data["done"] != true {
		t.Fatalf("operation not done: %v", resp.Data)
	}
	response, _ := resp.Data["response"].(map[string]any)
	if want := "type.googleapis.com/google.cloud.metastore.v1.MoveTableToDatabaseResponse"; response["@type"] != want {
		t.Fatalf("LRO response = %v, want @type %s", response, want)
	}
	metadata, _ := resp.Data["metadata"].(map[string]any)
	if metadata["verb"] != "moveTableToDatabase" {
		t.Fatalf("metadata verb = %v", metadata["verb"])
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
}

// TestAlterMetadataResourceLocationREST covers the REST custom method for all
// three addressed resources, and the failure shapes: a missing resource is a
// 404 envelope, a malformed request a 400, and an unknown Service a 404.
func TestAlterMetadataResourceLocationREST(t *testing.T) {
	ctx := context.Background()
	p, store := newAdminProvider(t)
	if err := store.CreateTable(ctx, "db1", "t1", hmsstore.Table{DBName: "db1", TableName: "t1", TableJSON: restTestTable("db1", "t1", "gs://wh/db1/t1")}); err != nil {
		t.Fatalf("seed table: %v", err)
	}
	if err := store.CreatePartition(ctx, "db1", "t1", hmsstore.Partition{
		DBName: "db1", TableName: "t1", Values: []string{"2026-01-01"},
		PartJSON: restTestPartition("db1", "t1", "2026-01-01", "gs://wh/db1/t1/dt=2026-01-01"),
	}); err != nil {
		t.Fatalf("seed partition: %v", err)
	}

	alter := func(resource, location string) (*model.ProviderResponse, error) {
		return p.AlterMetadataResourceLocation(ctx, newNR(map[string]any{
			"location": "us-central1", "serviceId": "svc1",
			"body": map[string]any{"resourceName": resource, "locationUri": location},
		}))
	}

	resp, err := alter("databases/db1", "gs://wh/db1-moved")
	if err != nil {
		t.Fatalf("alter database: %v", err)
	}
	if response, _ := resp.Data["response"].(map[string]any); response["@type"] != "type.googleapis.com/google.cloud.metastore.v1.AlterMetadataResourceLocationResponse" {
		t.Fatalf("LRO response = %v", response)
	}
	db, err := store.GetDatabase(ctx, "db1")
	if err != nil || db.LocationURI != "gs://wh/db1-moved" {
		t.Fatalf("database location = %q, %v", db.LocationURI, err)
	}

	if _, err := alter("databases/db1/tables/t1", "gs://wh/table-moved"); err != nil {
		t.Fatalf("alter table: %v", err)
	}
	tbl, err := store.GetTable(ctx, "db1", "t1")
	if err != nil || !strings.Contains(string(tbl.TableJSON), "gs://wh/table-moved") {
		t.Fatalf("table location not altered: %s, %v", tbl.TableJSON, err)
	}

	if _, err := alter("databases/db1/tables/t1/partitions/dt=2026-01-01", "gs://wh/part-moved"); err != nil {
		t.Fatalf("alter partition: %v", err)
	}
	part, err := store.GetPartition(ctx, "db1", "t1", []string{"2026-01-01"})
	if err != nil || !strings.Contains(string(part.PartJSON), "gs://wh/part-moved") {
		t.Fatalf("partition location not altered: %s, %v", part.PartJSON, err)
	}

	// Failures: NotFound for a missing catalog resource, InvalidArgument for a
	// malformed request or missing field, NotFound for an unknown Service.
	_, err = alter("databases/nope", "gs://wh/x")
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "NotFound" || perr.HTTPStatus != 404 {
		t.Fatalf("missing database: err = %v, want NotFound/404", err)
	}
	_, err = alter("db1", "gs://wh/x")
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("malformed resourceName: err = %v, want InvalidArgument/400", err)
	}
	_, err = p.AlterMetadataResourceLocation(ctx, newNR(map[string]any{
		"location": "us-central1", "serviceId": "svc1",
		"body": map[string]any{"resourceName": "databases/db1"},
	}))
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("missing locationUri: err = %v, want InvalidArgument", err)
	}
	_, err = p.MoveTableToDatabase(ctx, newNR(map[string]any{
		"location": "us-central1", "serviceId": "nope",
		"body": map[string]any{"dbName": "db1", "tableName": "t1", "destinationDbName": "db2"},
	}))
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "NotFound" {
		t.Fatalf("unknown service: err = %v, want NotFound", err)
	}
}

// TestDeferredCatalogOpsFailLoud pins the three remaining admin/DR operations:
// each is an explicit Unimplemented failure whose message states why the
// emulator cannot serve it (no fabricated success, no bare 501).
func TestDeferredCatalogOpsFailLoud(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider2()
	for _, tc := range []struct {
		action string
		reason string
	}{
		{"Metastore.ExportMetadata", "metadata dump"},
		{"Metastore.RestoreService", "restore"},
		{"Metastore.QueryMetadata", "SQL"},
	} {
		_, err := p.Routes()[tc.action](ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1"}))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "Unimplemented" || perr.HTTPStatus != 501 {
			t.Fatalf("%s: err = %v, want Unimplemented/501", tc.action, err)
		}
		if !strings.Contains(perr.Message, tc.reason) {
			t.Errorf("%s: message %q does not document the reason (%q)", tc.action, perr.Message, tc.reason)
		}
	}
}

// newAdminProvider2 returns a provider with no wired catalog, which is enough
// for the deferred operations (they never touch a catalog).
func newAdminProvider2() *Provider { return newProvider() }

// restTestTable builds a canonical Hive Table JSON with a storage location.
func restTestTable(db, name, location string) []byte {
	st := hms.NewBuilder().
		Str(1, name).
		Str(2, db).
		Struct(7, hms.NewBuilder().Str(2, location).Build()).
		Build()
	raw, _ := hms.MarshalStruct(st)
	return raw
}

// restTestPartition builds a canonical Hive Partition JSON with a storage
// location.
func restTestPartition(db, name, value, location string) []byte {
	st := hms.NewBuilder().
		ListStr(1, []string{value}).
		Str(2, db).
		Str(3, name).
		Struct(6, hms.NewBuilder().Str(2, location).Build()).
		Build()
	raw, _ := hms.MarshalStruct(st)
	return raw
}
