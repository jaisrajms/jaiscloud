package metastore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	metastorestore "jaiscloud/internal/gcp/store/metastore"
	"jaiscloud/internal/model"
)

func newCore() *Service { return NewService(metastorestore.NewMemoryStore()) }

func TestNameFormattersAndParse(t *testing.T) {
	if got := ServiceName("p", "us-central1", "s"); got != "projects/p/locations/us-central1/services/s" {
		t.Errorf("ServiceName = %q", got)
	}
	if got := BackupName("p", "us-central1", "s", "b"); got != "projects/p/locations/us-central1/services/s/backups/b" {
		t.Errorf("BackupName = %q", got)
	}
	if got := MetadataImportName("p", "us-central1", "s", "m"); got != "projects/p/locations/us-central1/services/s/metadataImports/m" {
		t.Errorf("MetadataImportName = %q", got)
	}
	if got := OperationName("p", "us-central1", "op"); got != "projects/p/locations/us-central1/operations/op" {
		t.Errorf("OperationName = %q", got)
	}

	rn := ParseName("projects/p/locations/us-central1/services/s/backups/b")
	if rn.Project != "p" || rn.Location != "us-central1" || rn.Service != "s" || rn.Backup != "b" {
		t.Errorf("ParseName backup = %+v", rn)
	}
	rn = ParseName("projects/p/locations/us-central1/services/s/metadataImports/m")
	if rn.MetadataImport != "m" {
		t.Errorf("ParseName import = %+v", rn)
	}
	rn = ParseName("projects/p/locations/us-central1/operations/op")
	if rn.Operation != "op" {
		t.Errorf("ParseName operation = %+v", rn)
	}
	if ProjectFromName("not-a-name") != "" {
		t.Error("ProjectFromName of a non-project name should be empty")
	}
	if ProjectFromName("projects/p/locations/us/services/s") != "p" {
		t.Error("ProjectFromName should extract p")
	}
}

func TestConfigHelpers(t *testing.T) {
	cfg := json.RawMessage(`{"labels":{"env":"test","n":1},"description":"d","hiveMetastoreConfig":{"endpointProtocol":"thrift"}}`)
	if got := labelsFromConfig(cfg); got["env"] != "test" || len(got) != 1 {
		t.Errorf("labelsFromConfig = %v", got)
	}
	if got := descriptionFromConfig(cfg); got != "d" {
		t.Errorf("descriptionFromConfig = %q", got)
	}
	if got := endpointProtocolFromConfig(cfg); got != "thrift" {
		t.Errorf("endpointProtocolFromConfig = %q", got)
	}
	if labelsFromConfig(nil) != nil {
		t.Error("labelsFromConfig(nil) should be nil")
	}
	if descriptionFromConfig(nil) != "" {
		t.Error("descriptionFromConfig(nil) should be empty")
	}
}

func TestCRUDAndOperationListing(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	svc, op, err := s.CreateService(ctx, "proj", "us-central1", "svc1", json.RawMessage(`{"labels":{"env":"test"}}`))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if svc.Name != "svc1" || svc.State != "ACTIVE" || op.Verb != "create" || !op.Done {
		t.Fatalf("create = %+v / op = %+v", svc, op)
	}

	if _, err := s.GetService(ctx, "proj", "us-central1", "missing"); !isCode(err, "NotFound") {
		t.Errorf("GetService missing = %v, want NotFound", err)
	}
	if _, _, err := s.CreateService(ctx, "proj", "us-central1", "svc1", nil); !isCode(err, "AlreadyExists") {
		t.Errorf("duplicate CreateService = %v, want AlreadyExists", err)
	}

	page, next, err := s.ListServices(ctx, "proj", "us-central1", 0, "")
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	if len(page) != 1 || next != "" {
		t.Fatalf("ListServices = %d items, next %q", len(page), next)
	}

	// Unknown nested mask fails loud.
	if _, _, err := s.UpdateService(ctx, "proj", "us-central1", "svc1", json.RawMessage(`{"bogusConfig":{"x":1}}`), "bogus_config.x"); !isCode(err, "InvalidArgument") {
		t.Errorf("bad mask = %v, want InvalidArgument", err)
	}
	// Known nested mask succeeds.
	if _, _, err := s.UpdateService(ctx, "proj", "us-central1", "svc1", json.RawMessage(`{"hiveMetastoreConfig":{"version":"3.2.0"}}`), "hive_metastore_config.version"); err != nil {
		t.Fatalf("UpdateService: %v", err)
	}

	got, err := s.GetService(ctx, "proj", "us-central1", "svc1")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if cfg := decodeConfig(got.Config); cfg["hiveMetastoreConfig"].(map[string]any)["version"] != "3.2.0" {
		t.Errorf("nested update not persisted: %v", got.Config)
	}

	// GetOperation / ListOperations.
	if _, err := s.GetOperation(ctx, "proj", "us-central1", op.ID); err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "missing"); !isCode(err, "NotFound") {
		t.Errorf("GetOperation missing = %v, want NotFound", err)
	}
	ops, _, err := s.ListOperations(ctx, "proj", "us-central1", 0, "")
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(ops) < 2 {
		t.Errorf("ListOperations = %d ops, want >= 2", len(ops))
	}

	if _, err := s.DeleteService(ctx, "proj", "us-central1", "svc1"); err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if _, err := s.GetService(ctx, "proj", "us-central1", "svc1"); !isCode(err, "NotFound") {
		t.Errorf("GetService after delete = %v, want NotFound", err)
	}
}

func TestBackupAndImportCRUD(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	if _, _, err := s.CreateService(ctx, "proj", "us-central1", "svc1", nil); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	// Backup on a missing service → NotFound.
	if _, _, err := s.CreateBackup(ctx, "proj", "us-central1", "missing", "b", nil); !isCode(err, "NotFound") {
		t.Errorf("CreateBackup on missing service = %v, want NotFound", err)
	}

	b, op, err := s.CreateBackup(ctx, "proj", "us-central1", "svc1", "bk1", json.RawMessage(`{"description":"nightly"}`))
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if b.Description != "nightly" || b.State != "ACTIVE" || op.Verb != "create" {
		t.Fatalf("backup = %+v / op = %+v", b, op)
	}
	if _, err := s.GetBackup(ctx, "proj", "us-central1", "svc1", "missing"); !isCode(err, "NotFound") {
		t.Errorf("GetBackup missing = %v, want NotFound", err)
	}
	if _, err := s.DeleteBackup(ctx, "proj", "us-central1", "svc1", "bk1"); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}

	mi, impOp, err := s.CreateMetadataImport(ctx, "proj", "us-central1", "svc1", "imp1", json.RawMessage(`{"description":"initial","databaseDump":{"gcsUri":"gs://b/dump.sql"}}`))
	if err != nil {
		t.Fatalf("CreateMetadataImport: %v", err)
	}
	if mi.Description != "initial" || mi.State != "SUCCEEDED" || impOp.Verb != "create" {
		t.Fatalf("import = %+v / op = %+v", mi, impOp)
	}
	updated, _, err := s.UpdateMetadataImport(ctx, "proj", "us-central1", "svc1", "imp1", json.RawMessage(`{"description":"updated"}`), "description")
	if err != nil {
		t.Fatalf("UpdateMetadataImport: %v", err)
	}
	if updated.Description != "updated" {
		t.Errorf("updated description = %q", updated.Description)
	}
	if _, _, err := s.UpdateMetadataImport(ctx, "proj", "us-central1", "svc1", "missing", nil, ""); !isCode(err, "NotFound") {
		t.Errorf("UpdateMetadataImport missing = %v, want NotFound", err)
	}
}

func TestListingAndRender(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateService(ctx, "proj", "us-central1", "svc1", nil); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if _, _, err := s.CreateBackup(ctx, "proj", "us-central1", "svc1", "bk1", nil); err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if _, _, err := s.CreateMetadataImport(ctx, "proj", "us-central1", "svc1", "imp1", nil); err != nil {
		t.Fatalf("CreateMetadataImport: %v", err)
	}

	if b, err := s.GetBackup(ctx, "proj", "us-central1", "svc1", "bk1"); err != nil || b.Name != "bk1" {
		t.Fatalf("GetBackup = %+v, %v", b, err)
	}
	backups, _, err := s.ListBackups(ctx, "proj", "us-central1", "svc1", 10, "")
	if err != nil || len(backups) != 1 {
		t.Fatalf("ListBackups = %d, %v", len(backups), err)
	}
	imports, _, err := s.ListMetadataImports(ctx, "proj", "us-central1", "svc1", 10, "")
	if err != nil || len(imports) != 1 {
		t.Fatalf("ListMetadataImports = %d, %v", len(imports), err)
	}
	if mi, err := s.GetMetadataImport(ctx, "proj", "us-central1", "svc1", "imp1"); err != nil || mi.Name != "imp1" {
		t.Fatalf("GetMetadataImport = %+v, %v", mi, err)
	}

	op, err := s.GetOperation(ctx, "proj", "us-central1", "")
	if !isCode(err, "InvalidArgument") {
		t.Fatalf("GetOperation invalid = %v", err)
	}
	_ = op

	ops, _, err := s.ListOperations(ctx, "proj", "us-central1", 1, "")
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("ListOperations page = %d, want 1", len(ops))
	}
	rendered := OperationJSON(ops[0], "proj")
	if rendered["name"] == "" || rendered["done"] != true {
		t.Errorf("OperationJSON = %v", rendered)
	}

	if err := validateEndpointProtocol(json.RawMessage(`{"hiveMetastoreConfig":{"endpointProtocol":"GRPC"}}`)); !isCode(err, "InvalidArgument") {
		t.Errorf("validateEndpointProtocol(GRPC) = %v", err)
	}
	if err := validateEndpointProtocol(json.RawMessage(`{"hiveMetastoreConfig":{"endpointProtocol":"BOGUS"}}`)); !isCode(err, "InvalidArgument") {
		t.Errorf("validateEndpointProtocol(BOGUS) = %v", err)
	}
	if err := validateEndpointProtocol(nil); err != nil {
		t.Errorf("validateEndpointProtocol(nil) = %v", err)
	}
}

func TestStableUIDAndOperationResponseType(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	_, op, err := s.CreateService(ctx, "proj", "us-central1", "svc1", nil)
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	// The stored operation response is an Any JSON object with an @type.
	var response map[string]any
	if err := json.Unmarshal([]byte(op.Response), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response["@type"] != serviceTypeURL {
		t.Errorf("response @type = %v, want %v", response["@type"], serviceTypeURL)
	}

	// uid is stable across renders (Create vs Get) and derived from the name.
	created, err := s.GetService(ctx, "proj", "us-central1", "svc1")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	first := ServiceJSON(created, "proj")["uid"]
	second := ServiceJSON(created, "proj")["uid"]
	if first != second {
		t.Errorf("uid not stable: %v vs %v", first, second)
	}
	if want := serviceUID(ServiceName("proj", "us-central1", "svc1")); first != want {
		t.Errorf("uid = %v, want %v", first, want)
	}
}

func TestListMissingParentNotFound(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	if _, _, err := s.ListBackups(ctx, "proj", "us-central1", "missing", 0, ""); !isCode(err, "NotFound") {
		t.Errorf("ListBackups missing parent = %v, want NotFound", err)
	}
	if _, _, err := s.ListMetadataImports(ctx, "proj", "us-central1", "missing", 0, ""); !isCode(err, "NotFound") {
		t.Errorf("ListMetadataImports missing parent = %v, want NotFound", err)
	}
}

func TestUpdateServiceRejectsGRPCEndpointProtocol(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateService(ctx, "proj", "us-central1", "svc1", nil); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	_, _, err := s.UpdateService(ctx, "proj", "us-central1", "svc1",
		json.RawMessage(`{"hiveMetastoreConfig":{"endpointProtocol":"GRPC"}}`), "hive_metastore_config.endpoint_protocol")
	if !isCode(err, "InvalidArgument") {
		t.Errorf("UpdateService GRPC endpointProtocol = %v, want InvalidArgument", err)
	}
}

func TestUpdateMetadataImportKeepsEndTime(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateService(ctx, "proj", "us-central1", "svc1", nil); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if _, _, err := s.CreateMetadataImport(ctx, "proj", "us-central1", "svc1", "imp1", json.RawMessage(`{"description":"a"}`)); err != nil {
		t.Fatalf("CreateMetadataImport: %v", err)
	}
	before, err := s.GetMetadataImport(ctx, "proj", "us-central1", "svc1", "imp1")
	if err != nil {
		t.Fatalf("GetMetadataImport: %v", err)
	}
	if _, _, err := s.UpdateMetadataImport(ctx, "proj", "us-central1", "svc1", "imp1", json.RawMessage(`{"description":"b"}`), "description"); err != nil {
		t.Fatalf("UpdateMetadataImport: %v", err)
	}
	after, err := s.GetMetadataImport(ctx, "proj", "us-central1", "svc1", "imp1")
	if err != nil {
		t.Fatalf("GetMetadataImport: %v", err)
	}
	if !after.EndTime.Equal(before.EndTime) {
		t.Errorf("endTime moved on update: %v -> %v", before.EndTime, after.EndTime)
	}
	if after.Description != "b" {
		t.Errorf("description = %q, want b", after.Description)
	}
}

func TestInvalidArguments(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	if _, _, err := s.CreateService(ctx, "proj", "", "s", nil); !isCode(err, "InvalidArgument") {
		t.Errorf("CreateService no location = %v", err)
	}
	if _, err := s.GetService(ctx, "proj", "", "s"); !isCode(err, "InvalidArgument") {
		t.Errorf("GetService no location = %v", err)
	}
	if _, _, err := s.ListServices(ctx, "proj", "", 0, ""); !isCode(err, "InvalidArgument") {
		t.Errorf("ListServices no location = %v", err)
	}
	if _, err := s.DeleteService(ctx, "proj", "", "s"); !isCode(err, "InvalidArgument") {
		t.Errorf("DeleteService no location = %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "", "op"); !isCode(err, "InvalidArgument") {
		t.Errorf("GetOperation no location = %v", err)
	}
	if _, _, err := s.CreateMetadataImport(ctx, "proj", "", "s", "m", nil); !isCode(err, "InvalidArgument") {
		t.Errorf("CreateMetadataImport no location = %v", err)
	}
	if _, err := s.GetMetadataImport(ctx, "proj", "l", "s", ""); !isCode(err, "InvalidArgument") {
		t.Errorf("GetMetadataImport no import = %v", err)
	}
	if _, _, err := s.ListMetadataImports(ctx, "proj", "", "s", 0, ""); !isCode(err, "InvalidArgument") {
		t.Errorf("ListMetadataImports no location = %v", err)
	}
	if _, _, err := s.ListBackups(ctx, "proj", "", "s", 0, ""); !isCode(err, "InvalidArgument") {
		t.Errorf("ListBackups no location = %v", err)
	}
}

func TestReset(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateService(ctx, "proj", "us-central1", "svc1", nil); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	s.Reset(ctx)
	page, _, err := s.ListServices(ctx, "proj", "us-central1", 0, "")
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	if len(page) != 0 {
		t.Errorf("after Reset, %d services remain", len(page))
	}
}

func isCode(err error, code string) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.Code == code
}
