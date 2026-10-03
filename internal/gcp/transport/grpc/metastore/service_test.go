package metastore

import (
	"context"
	"testing"

	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	core "jaiscloud/internal/gcp/service/metastore"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

func newGRPCService() *Service {
	return NewService(core.NewService(metastorestore.NewMemoryStore()), "proj")
}

func TestCreateServiceLRO(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	op, err := s.CreateService(ctx, &metastorepb.CreateServiceRequest{
		Parent:    "projects/proj/locations/us-central1",
		ServiceId: "svc1",
		Service: &metastorepb.Service{
			Labels: map[string]string{"env": "test"},
			MetastoreConfig: &metastorepb.Service_HiveMetastoreConfig{
				HiveMetastoreConfig: &metastorepb.HiveMetastoreConfig{Version: "3.1.2"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("operation not done: %+v", op)
	}
	var meta metastorepb.OperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if meta.GetVerb() != "create" || meta.GetTarget() != "projects/proj/locations/us-central1/services/svc1" {
		t.Errorf("metadata = %+v", &meta)
	}
	var svc metastorepb.Service
	if err := op.GetResponse().UnmarshalTo(&svc); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if svc.GetName() != "projects/proj/locations/us-central1/services/svc1" {
		t.Errorf("service name = %q", svc.GetName())
	}
	if svc.GetState() != metastorepb.Service_ACTIVE {
		t.Errorf("service state = %v, want ACTIVE", svc.GetState())
	}
	if svc.GetTier() != metastorepb.Service_DEVELOPER {
		t.Errorf("service tier = %v, want DEVELOPER", svc.GetTier())
	}
	if svc.GetLabels()["env"] != "test" {
		t.Errorf("labels = %v", svc.GetLabels())
	}
	if svc.GetEndpointUri() == "" || svc.GetUid() == "" {
		t.Errorf("output-only fields missing: %+v", &svc)
	}

	got, err := s.GetService(ctx, &metastorepb.GetServiceRequest{Name: svc.GetName()})
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if got.GetName() != svc.GetName() {
		t.Errorf("get name = %q", got.GetName())
	}
	if got.GetUid() != svc.GetUid() || got.GetUid() == "" {
		t.Errorf("uid not stable across create/get: %q vs %q", svc.GetUid(), got.GetUid())
	}

	list, err := s.ListServices(ctx, &metastorepb.ListServicesRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	if len(list.GetServices()) != 1 {
		t.Fatalf("services = %v", list.GetServices())
	}

	delOp, err := s.DeleteService(ctx, &metastorepb.DeleteServiceRequest{Name: svc.GetName()})
	if err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if !delOp.GetDone() {
		t.Error("delete op not done")
	}
	var deleted metastorepb.OperationMetadata
	if err := delOp.GetMetadata().UnmarshalTo(&deleted); err != nil {
		t.Fatalf("unmarshal delete metadata: %v", err)
	}
	if deleted.GetVerb() != "delete" {
		t.Errorf("delete verb = %q", deleted.GetVerb())
	}
	if _, err := s.GetService(ctx, &metastorepb.GetServiceRequest{Name: svc.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetService after delete: code = %v, want NotFound", status.Code(err))
	}
}

func TestUpdateServiceLabels(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	if _, err := s.CreateService(ctx, &metastorepb.CreateServiceRequest{
		Parent:    "projects/proj/locations/us-central1",
		ServiceId: "svc1",
		Service:   &metastorepb.Service{Labels: map[string]string{"env": "test"}},
	}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	op, err := s.UpdateService(ctx, &metastorepb.UpdateServiceRequest{
		Service:    &metastorepb.Service{Name: "projects/proj/locations/us-central1/services/svc1", Labels: map[string]string{"env": "prod"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	var svc metastorepb.Service
	if err := op.GetResponse().UnmarshalTo(&svc); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if svc.GetLabels()["env"] != "prod" {
		t.Errorf("labels = %v, want env=prod", svc.GetLabels())
	}
}

func TestCreateServiceAlreadyExistsAndNotFound(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	if _, err := s.CreateService(ctx, &metastorepb.CreateServiceRequest{Parent: "projects/proj/locations/us-central1", ServiceId: "svc1"}); err != nil {
		t.Fatalf("first CreateService: %v", err)
	}
	_, err := s.CreateService(ctx, &metastorepb.CreateServiceRequest{Parent: "projects/proj/locations/us-central1", ServiceId: "svc1"})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("second CreateService: code = %v, want AlreadyExists", status.Code(err))
	}
	_, err = s.GetService(ctx, &metastorepb.GetServiceRequest{Name: "projects/proj/locations/us-central1/services/missing"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetService missing: code = %v, want NotFound", status.Code(err))
	}
}

func TestCreateServiceRejectsGRPCEndpointProtocol(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	_, err := s.CreateService(ctx, &metastorepb.CreateServiceRequest{
		Parent:    "projects/proj/locations/us-central1",
		ServiceId: "svc1",
		Service: &metastorepb.Service{
			MetastoreConfig: &metastorepb.Service_HiveMetastoreConfig{
				HiveMetastoreConfig: &metastorepb.HiveMetastoreConfig{
					Version:          "3.1.2",
					EndpointProtocol: metastorepb.HiveMetastoreConfig_GRPC,
				},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateService GRPC endpointProtocol: code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestBackupCRUD(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	serviceName := "projects/proj/locations/us-central1/services/svc1"

	if _, err := s.CreateService(ctx, &metastorepb.CreateServiceRequest{Parent: "projects/proj/locations/us-central1", ServiceId: "svc1"}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	op, err := s.CreateBackup(ctx, &metastorepb.CreateBackupRequest{
		Parent:   serviceName,
		BackupId: "bk1",
		Backup:   &metastorepb.Backup{Description: "nightly"},
	})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	var b metastorepb.Backup
	if err := op.GetResponse().UnmarshalTo(&b); err != nil {
		t.Fatalf("unmarshal backup: %v", err)
	}
	want := serviceName + "/backups/bk1"
	if b.GetName() != want || b.GetState() != metastorepb.Backup_ACTIVE || b.GetDescription() != "nightly" {
		t.Errorf("backup = %+v", &b)
	}

	got, err := s.GetBackup(ctx, &metastorepb.GetBackupRequest{Name: want})
	if err != nil {
		t.Fatalf("GetBackup: %v", err)
	}
	if got.GetName() != want {
		t.Errorf("GetBackup name = %q", got.GetName())
	}
	list, err := s.ListBackups(ctx, &metastorepb.ListBackupsRequest{Parent: serviceName})
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(list.GetBackups()) != 1 {
		t.Fatalf("backups = %v", list.GetBackups())
	}
	if _, err := s.DeleteBackup(ctx, &metastorepb.DeleteBackupRequest{Name: want}); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}
	if _, err := s.GetBackup(ctx, &metastorepb.GetBackupRequest{Name: want}); status.Code(err) != codes.NotFound {
		t.Errorf("GetBackup after delete: code = %v, want NotFound", status.Code(err))
	}
}

func TestMetadataImportCRUD(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	serviceName := "projects/proj/locations/us-central1/services/svc1"

	if _, err := s.CreateService(ctx, &metastorepb.CreateServiceRequest{Parent: "projects/proj/locations/us-central1", ServiceId: "svc1"}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	op, err := s.CreateMetadataImport(ctx, &metastorepb.CreateMetadataImportRequest{
		Parent:           serviceName,
		MetadataImportId: "imp1",
		MetadataImport: &metastorepb.MetadataImport{
			Description: "initial",
			Metadata: &metastorepb.MetadataImport_DatabaseDump_{
				DatabaseDump: &metastorepb.MetadataImport_DatabaseDump{
					DatabaseType:   metastorepb.MetadataImport_DatabaseDump_MYSQL,
					GcsUri:         "gs://b/dump.sql",
					SourceDatabase: "db",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateMetadataImport: %v", err)
	}
	var mi metastorepb.MetadataImport
	if err := op.GetResponse().UnmarshalTo(&mi); err != nil {
		t.Fatalf("unmarshal import: %v", err)
	}
	want := serviceName + "/metadataImports/imp1"
	if mi.GetName() != want || mi.GetState() != metastorepb.MetadataImport_SUCCEEDED {
		t.Errorf("import = %+v", &mi)
	}
	if mi.GetDatabaseDump().GetGcsUri() != "gs://b/dump.sql" {
		t.Errorf("databaseDump = %+v", mi.GetDatabaseDump())
	}

	upd, err := s.UpdateMetadataImport(ctx, &metastorepb.UpdateMetadataImportRequest{
		MetadataImport: &metastorepb.MetadataImport{Name: want, Description: "updated"},
		UpdateMask:     &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	if err != nil {
		t.Fatalf("UpdateMetadataImport: %v", err)
	}
	var updated metastorepb.MetadataImport
	if err := upd.GetResponse().UnmarshalTo(&updated); err != nil {
		t.Fatalf("unmarshal updated import: %v", err)
	}
	if updated.GetDescription() != "updated" {
		t.Errorf("description = %q, want updated", updated.GetDescription())
	}

	list, err := s.ListMetadataImports(ctx, &metastorepb.ListMetadataImportsRequest{Parent: serviceName})
	if err != nil {
		t.Fatalf("ListMetadataImports: %v", err)
	}
	if len(list.GetMetadataImports()) != 1 {
		t.Fatalf("imports = %v", list.GetMetadataImports())
	}
}

func TestDeferredRPCsUnimplemented(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	if _, err := s.ExportMetadata(ctx, &metastorepb.ExportMetadataRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("ExportMetadata: code = %v, want Unimplemented", status.Code(err))
	}
	if _, err := s.RestoreService(ctx, &metastorepb.RestoreServiceRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("RestoreService: code = %v, want Unimplemented", status.Code(err))
	}
	if _, err := s.QueryMetadata(ctx, &metastorepb.QueryMetadataRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("QueryMetadata: code = %v, want Unimplemented", status.Code(err))
	}
	if _, err := s.MoveTableToDatabase(ctx, &metastorepb.MoveTableToDatabaseRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("MoveTableToDatabase: code = %v, want Unimplemented", status.Code(err))
	}
	if _, err := s.AlterMetadataResourceLocation(ctx, &metastorepb.AlterMetadataResourceLocationRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("AlterMetadataResourceLocation: code = %v, want Unimplemented", status.Code(err))
	}
}

// TestServiceToProtoMatchesJSONRender locks the REST/gRPC render invariant: the
// gRPC Service is built from the same core ServiceJSON map the REST transport
// emits, so both carry the same derived fields.
func TestServiceToProtoMatchesJSONRender(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	if _, err := s.CreateService(ctx, &metastorepb.CreateServiceRequest{
		Parent:    "projects/proj/locations/us-central1",
		ServiceId: "svc1",
		Service:   &metastorepb.Service{Labels: map[string]string{"env": "test"}},
	}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	stored, err := s.core.GetService(ctx, "proj", "us-central1", "svc1")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	rendered := core.ServiceJSON(stored, "proj")
	proto := serviceToProto(stored, "proj")

	if proto.GetName() != rendered["name"] {
		t.Errorf("name: proto %q, json %v", proto.GetName(), rendered["name"])
	}
	if proto.GetState().String() != rendered["state"] {
		t.Errorf("state: proto %v, json %v", proto.GetState(), rendered["state"])
	}
	if proto.GetTier().String() != rendered["tier"] {
		t.Errorf("tier: proto %v, json %v", proto.GetTier(), rendered["tier"])
	}
}
