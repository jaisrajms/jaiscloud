package metastore

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/resource"
	metastorecore "jaiscloud/internal/gcp/service/metastore"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
	"jaiscloud/internal/model"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func newProvider() *Provider {
	return NewProvider(metastorecore.NewService(metastorestore.NewMemoryStore()), "proj")
}

func createServiceParams(location, serviceID string, body map[string]any) map[string]any {
	return map[string]any{"location": location, "serviceId": serviceID, "body": body}
}

func TestServiceCRUDAndLROShape(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc1", map[string]any{
		"labels":              map[string]any{"env": "test"},
		"hiveMetastoreConfig": map[string]any{"version": "3.1.2"},
	})))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if resp.Data["done"] != true {
		t.Fatalf("expected done=true, got %v", resp.Data["done"])
	}
	opName, _ := resp.Data["name"].(string)
	if want := "projects/proj/locations/us-central1/operations/"; !hasPrefix(opName, want) {
		t.Errorf("operation name = %q, want prefix %q", opName, want)
	}
	metadata, _ := resp.Data["metadata"].(map[string]any)
	if metadata["@type"] != "type.googleapis.com/google.cloud.metastore.v1.OperationMetadata" {
		t.Errorf("metadata @type = %v", metadata["@type"])
	}
	if metadata["verb"] != "create" {
		t.Errorf("metadata verb = %v, want create", metadata["verb"])
	}
	created, _ := resp.Data["response"].(map[string]any)
	wantName := "projects/proj/locations/us-central1/services/svc1"
	if created["@type"] != "type.googleapis.com/google.cloud.metastore.v1.Service" {
		t.Errorf("LRO response @type = %v, want the metastore Service type URL", created["@type"])
	}
	if created["name"] != wantName {
		t.Errorf("service name = %v, want %v", created["name"], wantName)
	}
	if created["state"] != "ACTIVE" {
		t.Errorf("state = %v, want ACTIVE", created["state"])
	}
	if created["tier"] != "DEVELOPER" {
		t.Errorf("tier = %v, want DEVELOPER", created["tier"])
	}
	if created["endpointUri"] != "thrift://svc1.us-central1.metastore.jaiscloud.local:9083" {
		t.Errorf("endpointUri = %v", created["endpointUri"])
	}
	labels, _ := created["labels"].(map[string]any)
	if labels["env"] != "test" {
		t.Errorf("labels = %v, want env=test", created["labels"])
	}

	// Get.
	getResp, err := p.GetService(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1"}))
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if getResp.Data["name"] != wantName {
		t.Errorf("get name = %v, want %v", getResp.Data["name"], wantName)
	}

	// Update (labels).
	updResp, err := p.UpdateService(ctx, newNR(map[string]any{
		"location": "us-central1", "serviceId": "svc1",
		"updateMask": "labels",
		"body":       map[string]any{"labels": map[string]any{"env": "prod"}},
	}))
	if err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	updated, _ := updResp.Data["response"].(map[string]any)
	if labels, _ := updated["labels"].(map[string]any); labels["env"] != "prod" {
		t.Errorf("updated labels = %v, want env=prod", updated["labels"])
	}

	// List.
	listResp, err := p.ListServices(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	items, _ := listResp.Data["services"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 service, got %d", len(items))
	}

	// Delete (LRO response is google.protobuf.Empty).
	delResp, err := p.DeleteService(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1"}))
	if err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if delResp.Data["done"] != true {
		t.Errorf("delete done = %v, want true", delResp.Data["done"])
	}
	delResponse, _ := delResp.Data["response"].(map[string]any)
	if len(delResponse) != 0 {
		t.Errorf("delete response = %v, want empty", delResponse)
	}

	if _, err := p.GetService(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1"})); err == nil {
		t.Fatal("expected NotFound after delete, got nil error")
	}
}

func TestGetOperationRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc1", nil)))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	opName, _ := resp.Data["name"].(string)
	// opName = projects/proj/locations/us-central1/operations/{id}
	opID := opName[len("projects/proj/locations/us-central1/operations/"):]

	opResp, err := p.GetOperation(ctx, newNR(map[string]any{"location": "us-central1", "operationId": opID}))
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if opResp.Data["name"] != opName {
		t.Errorf("operation name = %v, want %v", opResp.Data["name"], opName)
	}
	if opResp.Data["done"] != true {
		t.Errorf("operation done = %v, want true", opResp.Data["done"])
	}

	// ListOperations includes the create operation.
	listResp, err := p.ListOperations(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	ops, _ := listResp.Data["operations"].([]any)
	if len(ops) != 1 {
		t.Errorf("ListOperations returned %d operations, want 1", len(ops))
	}

	// Missing op → NotFound.
	if _, err := p.GetOperation(ctx, newNR(map[string]any{"location": "us-central1", "operationId": "deadbeef"})); err == nil {
		t.Fatal("expected NotFound for missing operation, got nil")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "NotFound" {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestBackupCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc1", nil))); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	resp, err := p.CreateBackup(ctx, newNR(map[string]any{
		"location": "us-central1", "serviceId": "svc1", "backupId": "bk1",
		"body": map[string]any{"description": "nightly"},
	}))
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	created, _ := resp.Data["response"].(map[string]any)
	wantName := "projects/proj/locations/us-central1/services/svc1/backups/bk1"
	if created["name"] != wantName {
		t.Errorf("backup name = %v, want %v", created["name"], wantName)
	}
	if created["state"] != "ACTIVE" {
		t.Errorf("backup state = %v, want ACTIVE", created["state"])
	}
	if created["description"] != "nightly" {
		t.Errorf("backup description = %v, want nightly", created["description"])
	}

	getResp, err := p.GetBackup(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1", "backupId": "bk1"}))
	if err != nil {
		t.Fatalf("GetBackup: %v", err)
	}
	if getResp.Data["name"] != wantName {
		t.Errorf("get backup name = %v, want %v", getResp.Data["name"], wantName)
	}

	listResp, err := p.ListBackups(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1"}))
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	items, _ := listResp.Data["backups"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 backup, got %d", len(items))
	}

	if _, err := p.DeleteBackup(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1", "backupId": "bk1"})); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}
	if _, err := p.GetBackup(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1", "backupId": "bk1"})); err == nil {
		t.Fatal("expected NotFound after backup delete, got nil error")
	}
}

func TestMetadataImportCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc1", nil))); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	resp, err := p.CreateMetadataImport(ctx, newNR(map[string]any{
		"location": "us-central1", "serviceId": "svc1", "metadataImportId": "imp1",
		"body": map[string]any{"description": "initial", "databaseDump": map[string]any{"gcsUri": "gs://b/dump.sql"}},
	}))
	if err != nil {
		t.Fatalf("CreateMetadataImport: %v", err)
	}
	created, _ := resp.Data["response"].(map[string]any)
	wantName := "projects/proj/locations/us-central1/services/svc1/metadataImports/imp1"
	if created["name"] != wantName {
		t.Errorf("import name = %v, want %v", created["name"], wantName)
	}
	if created["state"] != "SUCCEEDED" {
		t.Errorf("import state = %v, want SUCCEEDED", created["state"])
	}
	if dump, _ := created["databaseDump"].(map[string]any); dump["gcsUri"] != "gs://b/dump.sql" {
		t.Errorf("databaseDump = %v", created["databaseDump"])
	}

	// Update (description only).
	updResp, err := p.UpdateMetadataImport(ctx, newNR(map[string]any{
		"location": "us-central1", "serviceId": "svc1", "metadataImportId": "imp1",
		"updateMask": "description",
		"body":       map[string]any{"description": "updated"},
	}))
	if err != nil {
		t.Fatalf("UpdateMetadataImport: %v", err)
	}
	updated, _ := updResp.Data["response"].(map[string]any)
	if updated["description"] != "updated" {
		t.Errorf("updated description = %v, want updated", updated["description"])
	}

	listResp, err := p.ListMetadataImports(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1"}))
	if err != nil {
		t.Fatalf("ListMetadataImports: %v", err)
	}
	items, _ := listResp.Data["metadataImports"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 import, got %d", len(items))
	}
}

func TestNotFoundAndAlreadyExists(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.GetService(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "missing"})); err == nil {
		t.Fatal("expected NotFound for missing service, got nil")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "NotFound" {
		t.Fatalf("expected NotFound, got %v", err)
	}

	nr := newNR(createServiceParams("us-central1", "svc1", nil))
	if _, err := p.CreateService(ctx, nr); err != nil {
		t.Fatalf("first CreateService: %v", err)
	}
	if _, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc1", nil))); err == nil {
		t.Fatal("expected AlreadyExists, got nil")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "AlreadyExists" {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}

	// CreateBackup/CreateMetadataImport against a missing service → NotFound.
	if _, err := p.CreateBackup(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "missing", "backupId": "b"})); err == nil {
		t.Fatal("expected NotFound creating backup on missing service, got nil")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "NotFound" {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestCreateService_MissingParams(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	cases := []map[string]any{
		{"location": "", "serviceId": "s"},
		{"location": "us-central1", "serviceId": ""},
	}
	for _, params := range cases {
		if _, err := p.CreateService(ctx, newNR(params)); err == nil {
			t.Errorf("params=%v: expected InvalidArgument, got nil", params)
		} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
			t.Errorf("params=%v: expected InvalidArgument, got %v", params, err)
		}
	}
}

func TestDeferredOps_Unimplemented(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	routes := p.Routes()
	for _, name := range []string{
		"Metastore.ExportMetadata", "Metastore.RestoreService", "Metastore.QueryMetadata",
		"Metastore.MoveTableToDatabase", "Metastore.AlterMetadataResourceLocation",
	} {
		_, err := routes[name](ctx, newNR(nil))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "Unimplemented" {
			t.Errorf("%s: expected Unimplemented, got %v", name, err)
		}
	}
}

func TestPagination(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	for i := 0; i < 5; i++ {
		name := string(rune('a' + i))
		if _, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc-"+name, nil))); err != nil {
			t.Fatalf("CreateService %s: %v", name, err)
		}
	}

	page1, err := p.ListServices(ctx, newNR(map[string]any{"location": "us-central1", "pageSize": float64(2)}))
	if err != nil {
		t.Fatalf("ListServices page1: %v", err)
	}
	items1, _ := page1.Data["services"].([]any)
	if len(items1) != 2 {
		t.Fatalf("page1 len = %d, want 2", len(items1))
	}
	token, _ := page1.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken on page1")
	}

	page2, err := p.ListServices(ctx, newNR(map[string]any{"location": "us-central1", "pageSize": float64(2), "pageToken": token}))
	if err != nil {
		t.Fatalf("ListServices page2: %v", err)
	}
	items2, _ := page2.Data["services"].([]any)
	if len(items2) != 2 {
		t.Fatalf("page2 len = %d, want 2", len(items2))
	}
}

func TestRoutes_AllHandlersRegistered(t *testing.T) {
	p := newProvider()
	routes := p.Routes()
	want := []string{
		"Metastore.CreateService", "Metastore.GetService", "Metastore.ListServices",
		"Metastore.UpdateService", "Metastore.DeleteService",
		"Metastore.CreateBackup", "Metastore.GetBackup", "Metastore.ListBackups", "Metastore.DeleteBackup",
		"Metastore.CreateMetadataImport", "Metastore.GetMetadataImport", "Metastore.ListMetadataImports",
		"Metastore.UpdateMetadataImport",
		"Metastore.GetOperation", "Metastore.ListOperations",
		"Metastore.ExportMetadata", "Metastore.RestoreService", "Metastore.QueryMetadata",
		"Metastore.MoveTableToDatabase", "Metastore.AlterMetadataResourceLocation",
	}
	for _, k := range want {
		if routes[k] == nil {
			t.Errorf("missing route %q", k)
		}
	}
	if len(routes) != len(want) {
		t.Errorf("got %d routes, want %d", len(routes), len(want))
	}
}

func TestServiceDefaultsOnReadBack(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc1", map[string]any{
		"tier": "ENTERPRISE",
	}))); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	got, err := p.GetService(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1"}))
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if got.Data["tier"] != "ENTERPRISE" {
		t.Errorf("tier = %v, want ENTERPRISE (client value preserved)", got.Data["tier"])
	}
	if got.Data["port"] != 9083 {
		t.Errorf("port = %v, want 9083", got.Data["port"])
	}
	if got.Data["releaseChannel"] != "STABLE" {
		t.Errorf("releaseChannel = %v, want STABLE", got.Data["releaseChannel"])
	}
	uid, _ := got.Data["uid"].(string)
	if uid == "" {
		t.Error("uid should be non-empty on read-back")
	}
}

func TestUpdateService_NestedMask(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc1", map[string]any{
		"hiveMetastoreConfig": map[string]any{"version": "3.1.2"},
	}))); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	upd, err := p.UpdateService(ctx, newNR(map[string]any{
		"location": "us-central1", "serviceId": "svc1",
		"updateMask": "hive_metastore_config.version",
		"body":       map[string]any{"hiveMetastoreConfig": map[string]any{"version": "3.2.0"}},
	}))
	if err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	updated, _ := upd.Data["response"].(map[string]any)
	if cfg, _ := updated["hiveMetastoreConfig"].(map[string]any); cfg["version"] != "3.2.0" {
		t.Errorf("hiveMetastoreConfig.version = %v, want 3.2.0", cfg["version"])
	}

	// Read-back confirms the nested field actually persisted.
	got, err := p.GetService(ctx, newNR(map[string]any{"location": "us-central1", "serviceId": "svc1"}))
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if cfg, _ := got.Data["hiveMetastoreConfig"].(map[string]any); cfg["version"] != "3.2.0" {
		t.Errorf("read-back hiveMetastoreConfig.version = %v, want 3.2.0", cfg["version"])
	}
}

func TestUpdateService_UnrecognizedNestedMaskFailsLoud(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc1", nil))); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	_, err := p.UpdateService(ctx, newNR(map[string]any{
		"location": "us-central1", "serviceId": "svc1",
		"updateMask": "bogus_config.foo",
		"body":       map[string]any{"bogusConfig": map[string]any{"foo": "bar"}},
	}))
	if err == nil {
		t.Fatal("expected InvalidArgument for unrecognized nested mask, got nil")
	}
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func TestCreateServiceEndpointProtocol(t *testing.T) {
	ctx := context.Background()

	// Absent endpointProtocol -> THRIFT default, accepted.
	p := newProvider()
	if _, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc1", map[string]any{
		"hiveMetastoreConfig": map[string]any{"version": "3.1.2"},
	}))); err != nil {
		t.Fatalf("absent endpointProtocol should default to THRIFT: %v", err)
	}

	// THRIFT (and lowercase) accepted.
	p = newProvider()
	if _, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc2", map[string]any{
		"hiveMetastoreConfig": map[string]any{"version": "3.1.2", "endpointProtocol": "thrift"},
	}))); err != nil {
		t.Fatalf("THRIFT endpointProtocol should be accepted: %v", err)
	}

	// GRPC rejected with InvalidArgument (per-service gRPC serving plane deferred — D4).
	p = newProvider()
	_, err := p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc3", map[string]any{
		"hiveMetastoreConfig": map[string]any{"version": "3.1.2", "endpointProtocol": "GRPC"},
	})))
	if err == nil {
		t.Fatal("expected GRPC endpointProtocol to be rejected, got nil")
	}
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("expected InvalidArgument for GRPC, got %v", err)
	}

	// Unknown value rejected with InvalidArgument.
	p = newProvider()
	_, err = p.CreateService(ctx, newNR(createServiceParams("us-central1", "svc4", map[string]any{
		"hiveMetastoreConfig": map[string]any{"endpointProtocol": "BOGUS"},
	})))
	if err == nil {
		t.Fatal("expected invalid endpointProtocol to be rejected, got nil")
	}
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("expected InvalidArgument for invalid value, got %v", err)
	}
}
