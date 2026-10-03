// Package firestoreadmin implements the Firestore Admin gRPC surface
// (google.firestore.admin.v1.FirestoreAdmin), covering collection-group
// composite-index CRUD. Every other FirestoreAdmin RPC (databases, backups,
// user creds, schedules, fields, export/import) is inherited from
// adminpb.UnimplementedFirestoreAdminServer and fails loud with Unimplemented.
//
// The transport-agnostic index logic lives in the shared
// provider/firestore.Service, so this gRPC surface and the REST adapter
// (projects.databases.collectionGroups.indexes) share one resource store and
// one set of pagination/sort semantics.
package firestoreadmin

import (
	"context"
	"strings"

	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	grpcutil "jaiscloud/internal/gcp/grpc"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	"jaiscloud/internal/model"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements adminpb.FirestoreAdminServer over the shared
// transport-agnostic provider Service, so the REST adapter and this gRPC
// transport operate on the same composite-index state.
type Service struct {
	adminpb.UnimplementedFirestoreAdminServer
	svc         *firestoreprovider.Service
	defaultProj string
}

// NewService returns a FirestoreAdmin gRPC service wrapping the provider
// Service. defaultProj is the config-default project used when a request
// carries none.
func NewService(svc *firestoreprovider.Service, defaultProj string) *Service {
	return &Service{svc: svc, defaultProj: defaultProj}
}

// Reset clears the shared transaction read-set registry (delegating to the
// provider Service), mirroring the Firestore data-plane gRPC service. Index
// state is reset via the shared resource store's own Resetter.
func (s *Service) Reset(ctx context.Context) { s.svc.Reset(ctx) }

// resolveProject derives the project for an RPC via the shared gRPC metadata
// resolver, falling back to the configured default. The request messages carry
// full resource names (including the project), so those are authoritative.
func (s *Service) resolveProject(ctx context.Context) string {
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// mapError converts a provider/service error into a gRPC status error via the
// shared grpc.GRPCStatus mapping.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// ─── resource-name parsing ────────────────────────────────────────────────────

// splitParent parses a collection-group parent resource name
// "projects/{p}/databases/{db}/collectionGroups/{cg}" into its parts.
func splitParent(parent string) (project, database, cg string, ok bool) {
	parts := strings.Split(parent, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "databases" || parts[4] != "collectionGroups" {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

// splitIndexName parses a full index resource name
// "projects/{p}/databases/{db}/collectionGroups/{cg}/indexes/{id}".
func splitIndexName(name string) (project, database, cg, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 8 || parts[0] != "projects" || parts[2] != "databases" ||
		parts[4] != "collectionGroups" || parts[6] != "indexes" {
		return "", "", "", "", false
	}
	return parts[1], parts[3], parts[5], parts[7], true
}

// ─── index RPCs ───────────────────────────────────────────────────────────────

// CreateIndex creates a composite index and returns a terminal
// google.longrunning.Operation wrapping the created Index.
func (s *Service) CreateIndex(ctx context.Context, req *adminpb.CreateIndexRequest) (*longrunningpb.Operation, error) {
	project, database, cg, ok := splitParent(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid index parent resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	created, opName, err := s.svc.CreateIndexDef(ctx, project, database, cg, indexFromProto(req.GetIndex()))
	if err != nil {
		return nil, mapError(err)
	}
	resp, err := anypb.New(indexToProto(created))
	if err != nil {
		return nil, mapError(model.NewProviderError("Internal", err.Error(), 500))
	}
	return &longrunningpb.Operation{
		Name:   opName,
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: resp},
	}, nil
}

// GetIndex returns a single composite index.
func (s *Service) GetIndex(ctx context.Context, req *adminpb.GetIndexRequest) (*adminpb.Index, error) {
	project, database, cg, id, ok := splitIndexName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid index resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	idx, err := s.svc.GetIndex(ctx, project, database, cg, id)
	if err != nil {
		return nil, mapError(err)
	}
	return indexToProto(idx), nil
}

// ListIndexes returns the composite indexes of a collection group, honoring
// page_size/page_token.
func (s *Service) ListIndexes(ctx context.Context, req *adminpb.ListIndexesRequest) (*adminpb.ListIndexesResponse, error) {
	project, database, cg, ok := splitParent(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid index parent resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	idxs, nextToken, err := s.svc.ListIndexes(ctx, project, database, cg, req.GetFilter(),
		firestoreprovider.NewPageParams(int(req.GetPageSize()), req.GetPageToken()))
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*adminpb.Index, 0, len(idxs))
	for _, idx := range idxs {
		out = append(out, indexToProto(idx))
	}
	return &adminpb.ListIndexesResponse{Indexes: out, NextPageToken: nextToken}, nil
}

// DeleteIndex removes a composite index. The proto result is Empty (the
// emulator applies the delete synchronously).
func (s *Service) DeleteIndex(ctx context.Context, req *adminpb.DeleteIndexRequest) (*emptypb.Empty, error) {
	project, database, cg, id, ok := splitIndexName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid index resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	if err := s.svc.DeleteIndex(ctx, project, database, cg, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── proto ↔ stored-definition conversion ────────────────────────────────────

// indexFromProto converts an adminpb.Index definition into the provider's
// stored shape. Enum names are preserved as strings (COLLECTION, ASCENDING,
// CONTAINS, ...); unspecified enum values are stored empty.
func indexFromProto(p *adminpb.Index) firestoreprovider.IndexDef {
	idx := firestoreprovider.IndexDef{}
	if p.GetQueryScope() != adminpb.Index_QUERY_SCOPE_UNSPECIFIED {
		idx.QueryScope = p.GetQueryScope().String()
	}
	for _, f := range p.GetFields() {
		field := firestoreprovider.IndexField{FieldPath: f.GetFieldPath()}
		switch v := f.GetValueMode().(type) {
		case *adminpb.Index_IndexField_Order_:
			if v.Order != adminpb.Index_IndexField_ORDER_UNSPECIFIED {
				field.Order = v.Order.String()
			}
		case *adminpb.Index_IndexField_ArrayConfig_:
			if v.ArrayConfig != adminpb.Index_IndexField_ARRAY_CONFIG_UNSPECIFIED {
				field.ArrayConfig = v.ArrayConfig.String()
			}
		}
		idx.Fields = append(idx.Fields, field)
	}
	return idx
}

// indexToProto renders a stored index definition to its adminpb.Index form.
// The emulator indexes are always READY.
func indexToProto(idx firestoreprovider.IndexDef) *adminpb.Index {
	out := &adminpb.Index{
		Name:       idx.Name,
		QueryScope: adminpb.Index_QueryScope(adminpb.Index_QueryScope_value[idx.QueryScope]),
		State:      adminpb.Index_READY,
	}
	for _, f := range idx.Fields {
		field := &adminpb.Index_IndexField{FieldPath: f.FieldPath}
		switch {
		case f.Order != "":
			field.ValueMode = &adminpb.Index_IndexField_Order_{
				Order: adminpb.Index_IndexField_Order(adminpb.Index_IndexField_Order_value[f.Order]),
			}
		case f.ArrayConfig != "":
			field.ValueMode = &adminpb.Index_IndexField_ArrayConfig_{
				ArrayConfig: adminpb.Index_IndexField_ArrayConfig(adminpb.Index_IndexField_ArrayConfig_value[f.ArrayConfig]),
			}
		}
		out.Fields = append(out.Fields, field)
	}
	return out
}
