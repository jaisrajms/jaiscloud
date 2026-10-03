package grpcconformance

import (
	"context"
	"fmt"

	metastore "cloud.google.com/go/metastore/apiv1"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// metastoreChecks covers the Dataproc Metastore v1 control plane
// (google.cloud.metastore.v1.DataprocMetastore) via the official generated
// cloud.google.com/go/metastore/apiv1 client: service CRUD, backup CRUD, and
// metadata-import read/create/update. Create/update/delete are long-running
// operations whose typed response is packed inline (done=true), so the client's
// Wait observes it without polling.
//
// The five deferred control-plane RPCs (ExportMetadata, RestoreService,
// QueryMetadata, MoveTableToDatabase, AlterMetadataResourceLocation) are
// deliberately not probed: they are explicit Unimplemented stubs.
//
// Every probe is self-contained: it ensures a run-unique service exists and then
// exercises one RPC. Names are run-unique via cfg.ResourceName, so a long-lived
// emulator never sees cross-run collisions.
func metastoreChecks() []Check {
	return []Check{
		{Service: "metastore", RPC: "CreateService", Method: "CreateService", KeyField: "LRO done + ACTIVE service", Run: checkMSCreateService},
		{Service: "metastore", RPC: "GetService", Method: "GetService", KeyField: "name/state round-trip", Run: checkMSGetService},
		{Service: "metastore", RPC: "ListServices", Method: "ListServices", KeyField: "created service present", Run: checkMSListServices},
		{Service: "metastore", RPC: "UpdateService", Method: "UpdateService", KeyField: "labels updated via LRO", Run: checkMSUpdateService},
		{Service: "metastore", RPC: "DeleteService", Method: "DeleteService", KeyField: "LRO done + NotFound after", Run: checkMSDeleteService},
		{Service: "metastore", RPC: "CreateMetadataImport", Method: "CreateMetadataImport", KeyField: "LRO done + SUCCEEDED import", Run: checkMSCreateMetadataImport},
		{Service: "metastore", RPC: "GetMetadataImport", Method: "GetMetadataImport", KeyField: "name/state round-trip", Run: checkMSGetMetadataImport},
		{Service: "metastore", RPC: "ListMetadataImports", Method: "ListMetadataImports", KeyField: "created import present", Run: checkMSListMetadataImports},
		{Service: "metastore", RPC: "UpdateMetadataImport", Method: "UpdateMetadataImport", KeyField: "description updated via LRO", Run: checkMSUpdateMetadataImport},
		{Service: "metastore", RPC: "CreateBackup", Method: "CreateBackup", KeyField: "LRO done + ACTIVE backup", Run: checkMSCreateBackup},
		{Service: "metastore", RPC: "GetBackup", Method: "GetBackup", KeyField: "name/state round-trip", Run: checkMSGetBackup},
		{Service: "metastore", RPC: "ListBackups", Method: "ListBackups", KeyField: "created backup present", Run: checkMSListBackups},
		{Service: "metastore", RPC: "DeleteBackup", Method: "DeleteBackup", KeyField: "LRO done + NotFound after", Run: checkMSDeleteBackup},
	}
}

const metastoreLocation = "us-central1"

// newMetastoreClient dials the emulator and returns the official generated
// Dataproc Metastore client.
func newMetastoreClient(ctx context.Context, cfg Config) (*metastore.DataprocMetastoreClient, error) {
	return metastore.NewDataprocMetastoreClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func metastoreParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, metastoreLocation)
}

func metastoreServiceName(cfg Config, id string) string {
	return metastoreParent(cfg) + "/services/" + id
}

// ensureMetastoreService creates the run-unique probe service, treating
// AlreadyExists as success so repeated probes are idempotent.
func ensureMetastoreService(ctx context.Context, client *metastore.DataprocMetastoreClient, cfg Config) (string, error) {
	id := cfg.ResourceName("gcpc-grpc-ms")
	op, err := client.CreateService(ctx, &metastorepb.CreateServiceRequest{
		Parent:    metastoreParent(cfg),
		ServiceId: id,
		Service:   &metastorepb.Service{Labels: map[string]string{"probe": "gcpc"}},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return metastoreServiceName(cfg, id), nil
		}
		return "", fmt.Errorf("create service: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait service: %w", err)
	}
	return metastoreServiceName(cfg, id), nil
}

// Check 1: CreateService returns a done operation whose response is the ACTIVE
// service.
func checkMSCreateService(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-ms-create")
	wantName := metastoreServiceName(cfg, id)
	op, err := client.CreateService(ctx, &metastorepb.CreateServiceRequest{
		Parent:    metastoreParent(cfg),
		ServiceId: id,
		Service:   &metastorepb.Service{Labels: map[string]string{"probe": "gcpc"}},
	})
	if err != nil {
		return fmt.Errorf("CreateService: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateService operation not done")
	}
	meta, err := op.Metadata()
	if err != nil {
		return fmt.Errorf("operation metadata: %w", err)
	}
	if meta.GetVerb() != "create" {
		return fmt.Errorf("operation verb = %q, want create", meta.GetVerb())
	}
	svc, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if svc.GetState() != metastorepb.Service_ACTIVE {
		return fmt.Errorf("service state = %v, want ACTIVE", svc.GetState())
	}
	if svc.GetName() != wantName {
		return fmt.Errorf("service name = %q, want %q", svc.GetName(), wantName)
	}
	if svc.GetLabels()["probe"] != "gcpc" {
		return fmt.Errorf("service labels = %v", svc.GetLabels())
	}
	return nil
}

// Check 2: GetService round-trips.
func checkMSGetService(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	got, err := client.GetService(ctx, &metastorepb.GetServiceRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetService: %w", err)
	}
	if got.GetName() != name || got.GetState() != metastorepb.Service_ACTIVE {
		return fmt.Errorf("GetService = %+v", got)
	}
	return nil
}

// Check 3: ListServices includes the probe service.
func checkMSListServices(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	it := client.ListServices(ctx, &metastorepb.ListServicesRequest{Parent: metastoreParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListServices did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListServices: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

// Check 4: UpdateService applies labels through an LRO.
func checkMSUpdateService(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	op, err := client.UpdateService(ctx, &metastorepb.UpdateServiceRequest{
		Service:    &metastorepb.Service{Name: name, Labels: map[string]string{"updated": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateService: %w", err)
	}
	svc, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if svc.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("updated labels = %v", svc.GetLabels())
	}
	return nil
}

// Check 5: DeleteService returns a done operation and the service is then gone.
func checkMSDeleteService(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-ms-del")
	name := metastoreServiceName(cfg, id)
	if op, err := client.CreateService(ctx, &metastorepb.CreateServiceRequest{
		Parent:    metastoreParent(cfg),
		ServiceId: id,
		Service:   &metastorepb.Service{},
	}); err != nil {
		return fmt.Errorf("create: %w", err)
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("create wait: %w", err)
	}
	delOp, err := client.DeleteService(ctx, &metastorepb.DeleteServiceRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteService: %w", err)
	}
	if !delOp.Done() {
		return fmt.Errorf("delete operation not done")
	}
	if err := delOp.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetService(ctx, &metastorepb.GetServiceRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetService after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── Metadata imports ─────────────────────────────────────────────────────────

func metastoreImportName(service, id string) string {
	return service + "/metadataImports/" + id
}

func ensureMetastoreImport(ctx context.Context, client *metastore.DataprocMetastoreClient, service, id string) (string, error) {
	op, err := client.CreateMetadataImport(ctx, &metastorepb.CreateMetadataImportRequest{
		Parent:           service,
		MetadataImportId: id,
		MetadataImport: &metastorepb.MetadataImport{
			Description: "probe",
			Metadata: &metastorepb.MetadataImport_DatabaseDump_{
				DatabaseDump: &metastorepb.MetadataImport_DatabaseDump{GcsUri: "gs://bucket/dump.sql"},
			},
		},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return metastoreImportName(service, id), nil
		}
		return "", fmt.Errorf("CreateMetadataImport: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("CreateMetadataImport wait: %w", err)
	}
	return metastoreImportName(service, id), nil
}

// Check 6: CreateMetadataImport returns a done operation whose response is the
// SUCCEEDED import carrying the database dump.
func checkMSCreateMetadataImport(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	service, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	id := cfg.ResourceName("gcpc-grpc-ms-imp")
	op, err := client.CreateMetadataImport(ctx, &metastorepb.CreateMetadataImportRequest{
		Parent:           service,
		MetadataImportId: id,
		MetadataImport: &metastorepb.MetadataImport{
			Metadata: &metastorepb.MetadataImport_DatabaseDump_{
				DatabaseDump: &metastorepb.MetadataImport_DatabaseDump{GcsUri: "gs://bucket/dump.sql"},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateMetadataImport: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateMetadataImport operation not done")
	}
	mi, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if mi.GetName() != metastoreImportName(service, id) {
		return fmt.Errorf("import name = %q", mi.GetName())
	}
	if mi.GetState() != metastorepb.MetadataImport_SUCCEEDED {
		return fmt.Errorf("import state = %v, want SUCCEEDED", mi.GetState())
	}
	if mi.GetDatabaseDump().GetGcsUri() != "gs://bucket/dump.sql" {
		return fmt.Errorf("databaseDump = %+v", mi.GetDatabaseDump())
	}
	return nil
}

// Check 7: GetMetadataImport round-trips.
func checkMSGetMetadataImport(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	service, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	name, err := ensureMetastoreImport(ctx, client, service, cfg.ResourceName("gcpc-grpc-ms-imp-get"))
	if err != nil {
		return err
	}
	got, err := client.GetMetadataImport(ctx, &metastorepb.GetMetadataImportRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetMetadataImport: %w", err)
	}
	if got.GetName() != name || got.GetState() != metastorepb.MetadataImport_SUCCEEDED {
		return fmt.Errorf("GetMetadataImport = %+v", got)
	}
	return nil
}

// Check 8: ListMetadataImports includes the probe import.
func checkMSListMetadataImports(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	service, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	name, err := ensureMetastoreImport(ctx, client, service, cfg.ResourceName("gcpc-grpc-ms-imp-list"))
	if err != nil {
		return err
	}
	it := client.ListMetadataImports(ctx, &metastorepb.ListMetadataImportsRequest{Parent: service})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListMetadataImports did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListMetadataImports: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

// Check 9: UpdateMetadataImport changes the description through an LRO.
func checkMSUpdateMetadataImport(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	service, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	name, err := ensureMetastoreImport(ctx, client, service, cfg.ResourceName("gcpc-grpc-ms-imp-upd"))
	if err != nil {
		return err
	}
	op, err := client.UpdateMetadataImport(ctx, &metastorepb.UpdateMetadataImportRequest{
		MetadataImport: &metastorepb.MetadataImport{Name: name, Description: "updated"},
		UpdateMask:     &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateMetadataImport: %w", err)
	}
	mi, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if mi.GetDescription() != "updated" {
		return fmt.Errorf("description = %q, want updated", mi.GetDescription())
	}
	return nil
}

// ─── Backups ──────────────────────────────────────────────────────────────────

func metastoreBackupName(service, id string) string { return service + "/backups/" + id }

func ensureMetastoreBackup(ctx context.Context, client *metastore.DataprocMetastoreClient, service, id string) (string, error) {
	op, err := client.CreateBackup(ctx, &metastorepb.CreateBackupRequest{
		Parent:   service,
		BackupId: id,
		Backup:   &metastorepb.Backup{Description: "probe"},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return metastoreBackupName(service, id), nil
		}
		return "", fmt.Errorf("CreateBackup: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("CreateBackup wait: %w", err)
	}
	return metastoreBackupName(service, id), nil
}

// Check 10: CreateBackup returns a done operation whose response is the ACTIVE
// backup.
func checkMSCreateBackup(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	service, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	id := cfg.ResourceName("gcpc-grpc-ms-bk")
	op, err := client.CreateBackup(ctx, &metastorepb.CreateBackupRequest{
		Parent:   service,
		BackupId: id,
		Backup:   &metastorepb.Backup{Description: "probe"},
	})
	if err != nil {
		return fmt.Errorf("CreateBackup: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateBackup operation not done")
	}
	b, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if b.GetName() != metastoreBackupName(service, id) {
		return fmt.Errorf("backup name = %q", b.GetName())
	}
	if b.GetState() != metastorepb.Backup_ACTIVE {
		return fmt.Errorf("backup state = %v, want ACTIVE", b.GetState())
	}
	return nil
}

// Check 11: GetBackup round-trips.
func checkMSGetBackup(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	service, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	name, err := ensureMetastoreBackup(ctx, client, service, cfg.ResourceName("gcpc-grpc-ms-bk-get"))
	if err != nil {
		return err
	}
	got, err := client.GetBackup(ctx, &metastorepb.GetBackupRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetBackup: %w", err)
	}
	if got.GetName() != name || got.GetState() != metastorepb.Backup_ACTIVE {
		return fmt.Errorf("GetBackup = %+v", got)
	}
	return nil
}

// Check 12: ListBackups includes the probe backup.
func checkMSListBackups(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	service, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	name, err := ensureMetastoreBackup(ctx, client, service, cfg.ResourceName("gcpc-grpc-ms-bk-list"))
	if err != nil {
		return err
	}
	it := client.ListBackups(ctx, &metastorepb.ListBackupsRequest{Parent: service})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListBackups did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListBackups: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

// Check 13: DeleteBackup returns a done operation and the backup is then gone.
func checkMSDeleteBackup(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	service, err := ensureMetastoreService(ctx, client, cfg)
	if err != nil {
		return err
	}
	name, err := ensureMetastoreBackup(ctx, client, service, cfg.ResourceName("gcpc-grpc-ms-bk-del"))
	if err != nil {
		return err
	}
	delOp, err := client.DeleteBackup(ctx, &metastorepb.DeleteBackupRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteBackup: %w", err)
	}
	if !delOp.Done() {
		return fmt.Errorf("delete operation not done")
	}
	if err := delOp.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetBackup(ctx, &metastorepb.GetBackupRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetBackup after delete = %v, want NotFound", err)
	}
	return nil
}
