// Package metastore is the gRPC transport for the Dataproc Metastore v1
// control plane (google.cloud.metastore.v1.DataprocMetastore). It is a thin
// proto adapter over the transport-neutral core in
// internal/gcp/service/metastore: it transcodes between the generated protobuf
// messages and the core's typed API, and maps core errors to gRPC status codes.
// It owns no business logic and no state beyond its default project.
//
// Create/Update/Delete return a done google.longrunning.Operation with the
// typed response packed inline, so the generated client's Wait observes it
// without polling. In the opt-in async mode the operation is returned in flight
// and the client's poll resolves it through ResolveOperation (registered with
// the shared google.longrunning.Operations service in main.go). The five
// deferred control-plane RPCs (ExportMetadata,
// RestoreService, QueryMetadata, MoveTableToDatabase,
// AlterMetadataResourceLocation) fail loud with codes.Unimplemented. The
// separate google.cloud.metastore.v1.DataprocMetastoreFederation service is not
// registered.
package metastore

import (
	"context"
	"encoding/json"
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	grpcutil "jaiscloud/internal/gcp/grpc"
	grpcoperations "jaiscloud/internal/gcp/grpc/operations"
	core "jaiscloud/internal/gcp/service/metastore"
	"jaiscloud/internal/model"
)

// Service implements metastorepb.DataprocMetastoreServer over the shared core.
type Service struct {
	metastorepb.UnimplementedDataprocMetastoreServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Dataproc Metastore gRPC service wrapping the core.
// defaultProj is the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// unimplemented builds the canonical Unimplemented provider error the deferred
// control-plane RPCs report.
func unimplemented(name string) error {
	return model.NewProviderError("Unimplemented", name+" is not supported by the emulator", 501)
}

// protoConfig marshals a request message to the camelCase JSON body the core
// stores verbatim. A nil message yields a nil body.
func protoConfig(m proto.Message) json.RawMessage {
	if m == nil {
		return nil
	}
	data, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}
	return data
}

// maskString joins a proto field mask's paths into the comma-separated form the
// core's updateMask handling expects.
func maskString(paths []string) string { return strings.Join(paths, ",") }

// ─── Services ─────────────────────────────────────────────────────────────────

func (s *Service) ListServices(ctx context.Context, req *metastorepb.ListServicesRequest) (*metastorepb.ListServicesResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListServices(ctx, project, rn.Location, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &metastorepb.ListServicesResponse{NextPageToken: next}
	for _, svc := range page {
		out.Services = append(out.Services, serviceToProto(svc, project))
	}
	return out, nil
}

func (s *Service) GetService(ctx context.Context, req *metastorepb.GetServiceRequest) (*metastorepb.Service, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	svc, err := s.core.GetService(ctx, project, rn.Location, rn.Service)
	if err != nil {
		return nil, mapError(err)
	}
	return serviceToProto(svc, project), nil
}

func (s *Service) CreateService(ctx context.Context, req *metastorepb.CreateServiceRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	svc, op, err := s.core.CreateService(ctx, project, rn.Location, req.GetServiceId(), protoConfig(req.GetService()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, serviceToProto(svc, project))
}

func (s *Service) UpdateService(ctx context.Context, req *metastorepb.UpdateServiceRequest) (*longrunningpb.Operation, error) {
	name := req.GetService().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	svc, op, err := s.core.UpdateService(ctx, project, rn.Location, rn.Service, protoConfig(req.GetService()), maskString(req.GetUpdateMask().GetPaths()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, serviceToProto(svc, project))
}

func (s *Service) DeleteService(ctx context.Context, req *metastorepb.DeleteServiceRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	op, err := s.core.DeleteService(ctx, project, rn.Location, rn.Service)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, emptyResponse())
}

// ─── Metadata imports ─────────────────────────────────────────────────────────

func (s *Service) ListMetadataImports(ctx context.Context, req *metastorepb.ListMetadataImportsRequest) (*metastorepb.ListMetadataImportsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListMetadataImports(ctx, project, rn.Location, rn.Service, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &metastorepb.ListMetadataImportsResponse{NextPageToken: next}
	for _, mi := range page {
		out.MetadataImports = append(out.MetadataImports, metadataImportToProto(mi, project))
	}
	return out, nil
}

func (s *Service) GetMetadataImport(ctx context.Context, req *metastorepb.GetMetadataImportRequest) (*metastorepb.MetadataImport, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	mi, err := s.core.GetMetadataImport(ctx, project, rn.Location, rn.Service, rn.MetadataImport)
	if err != nil {
		return nil, mapError(err)
	}
	return metadataImportToProto(mi, project), nil
}

func (s *Service) CreateMetadataImport(ctx context.Context, req *metastorepb.CreateMetadataImportRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	mi, op, err := s.core.CreateMetadataImport(ctx, project, rn.Location, rn.Service, req.GetMetadataImportId(), protoConfig(req.GetMetadataImport()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, metadataImportToProto(mi, project))
}

func (s *Service) UpdateMetadataImport(ctx context.Context, req *metastorepb.UpdateMetadataImportRequest) (*longrunningpb.Operation, error) {
	name := req.GetMetadataImport().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	mi, op, err := s.core.UpdateMetadataImport(ctx, project, rn.Location, rn.Service, rn.MetadataImport, protoConfig(req.GetMetadataImport()), maskString(req.GetUpdateMask().GetPaths()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, metadataImportToProto(mi, project))
}

// ─── Backups ──────────────────────────────────────────────────────────────────

func (s *Service) ListBackups(ctx context.Context, req *metastorepb.ListBackupsRequest) (*metastorepb.ListBackupsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListBackups(ctx, project, rn.Location, rn.Service, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &metastorepb.ListBackupsResponse{NextPageToken: next}
	for _, b := range page {
		out.Backups = append(out.Backups, backupToProto(b, project))
	}
	return out, nil
}

func (s *Service) GetBackup(ctx context.Context, req *metastorepb.GetBackupRequest) (*metastorepb.Backup, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	b, err := s.core.GetBackup(ctx, project, rn.Location, rn.Service, rn.Backup)
	if err != nil {
		return nil, mapError(err)
	}
	return backupToProto(b, project), nil
}

func (s *Service) CreateBackup(ctx context.Context, req *metastorepb.CreateBackupRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	b, op, err := s.core.CreateBackup(ctx, project, rn.Location, rn.Service, req.GetBackupId(), protoConfig(req.GetBackup()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, backupToProto(b, project))
}

func (s *Service) DeleteBackup(ctx context.Context, req *metastorepb.DeleteBackupRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	op, err := s.core.DeleteBackup(ctx, project, rn.Location, rn.Service, rn.Backup)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, emptyResponse())
}

// ─── Deferred control-plane RPCs ──────────────────────────────────────────────

func (s *Service) ExportMetadata(_ context.Context, _ *metastorepb.ExportMetadataRequest) (*longrunningpb.Operation, error) {
	return nil, mapError(unimplemented("ExportMetadata"))
}

func (s *Service) RestoreService(_ context.Context, _ *metastorepb.RestoreServiceRequest) (*longrunningpb.Operation, error) {
	return nil, mapError(unimplemented("RestoreService"))
}

func (s *Service) QueryMetadata(_ context.Context, _ *metastorepb.QueryMetadataRequest) (*longrunningpb.Operation, error) {
	return nil, mapError(unimplemented("QueryMetadata"))
}

func (s *Service) MoveTableToDatabase(_ context.Context, _ *metastorepb.MoveTableToDatabaseRequest) (*longrunningpb.Operation, error) {
	return nil, mapError(unimplemented("MoveTableToDatabase"))
}

func (s *Service) AlterMetadataResourceLocation(_ context.Context, _ *metastorepb.AlterMetadataResourceLocationRequest) (*longrunningpb.Operation, error) {
	return nil, mapError(unimplemented("AlterMetadataResourceLocation"))
}

// ResolveOperation implements the google.longrunning.Operations resolver for
// Dataproc Metastore. Names are location-scoped
// (projects/{p}/locations/{l}/operations/{id}); a name outside that shape — or
// an id the metastore store does not know, since a location-scoped name is
// shared with other services — is not ours (handled=false) so the generic
// terminal stub keeps serving it done=true.
func (s *Service) ResolveOperation(ctx context.Context, name string) (*longrunningpb.Operation, bool, error) {
	project, location, id, ok := parseOperationName(name)
	if !ok {
		return nil, false, nil
	}
	op, err := s.core.GetOperation(ctx, project, location, id)
	if err != nil {
		if core.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, true, mapError(err)
	}
	out, err := operationToProto(op, project, operationResponse(op))
	if err != nil {
		return nil, true, err
	}
	return out, true, nil
}

// parseOperationName parses a Dataproc Metastore location operation name
// projects/{project}/locations/{location}/operations/{id}.
func parseOperationName(name string) (project, location, id string, ok bool) {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "locations" || parts[4] != "operations" {
		return "", "", "", false
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

// compile-time assertions that Service implements the generated server and the
// operations resolver.
var (
	_ metastorepb.DataprocMetastoreServer = (*Service)(nil)
	_ grpcoperations.Resolver             = (*Service)(nil)
)
