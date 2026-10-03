// Package functions is the gRPC transport for Cloud Functions v1 and v2
// (google.cloud.functions.v1.CloudFunctionsService and
// google.cloud.functions.v2.FunctionService). It is a thin proto adapter over
// the transport-neutral core in internal/gcp/service/functions: it transcodes
// between the generated protobuf messages and the core's typed API, and maps
// core errors to gRPC status codes. It owns no business logic and no state
// beyond its default project.
//
// v1 and v2 are separate Go servers (Service and ServiceV2) because the two
// generated server interfaces declare methods with the same names but different
// request types, which a single Go type cannot satisfy. Both wrap the SAME core
// Service instance, so the two API versions cannot drift.
//
// Create/Update/Delete return a google.longrunning.Operation whose metadata and
// response are packed as typed Any protos (OperationMetadataV1 / OperationMetadata
// for v1 and v2 respectively, and the Function or google.protobuf.Empty as the
// response), so the generated client's Wait observes the result without
// polling. v1 CallFunction (runtime invocation) is served over the SAME core
// executor as the REST transport, and v2 ListRuntimes (the runtime catalog) is
// served from the SAME core catalog as the REST /v2/.../runtimes route.
package functions

import (
	"context"
	"strings"

	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	grpcoperations "jaiscloud/internal/gcp/grpc/operations"
	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/functions"
)

// Service implements functionspb.CloudFunctionsServiceServer (v1) over the
// shared core.
type Service struct {
	functionspb.UnimplementedCloudFunctionsServiceServer

	core        *core.Service
	defaultProj string
	uploadBase  string
}

// ServiceV2 implements apiv2functionspb.FunctionServiceServer (v2) over the
// same shared core.
type ServiceV2 struct {
	apiv2functionspb.UnimplementedFunctionServiceServer

	core        *core.Service
	defaultProj string
	uploadBase  string
}

// NewService returns the Cloud Functions v1 gRPC server wrapping the core.
// defaultProj is the config-default project used when a request carries none.
// uploadBase is the emulator-http origin used to build a source-upload URL
// (empty falls back to the real-GCP-shaped storage.googleapis.com).
func NewService(c *core.Service, defaultProj, uploadBase string) *Service {
	return &Service{core: c, defaultProj: defaultProj, uploadBase: uploadBase}
}

// NewServiceV2 returns the Cloud Functions v2 gRPC server wrapping the core.
func NewServiceV2(c *core.Service, defaultProj, uploadBase string) *ServiceV2 {
	return &ServiceV2{core: c, defaultProj: defaultProj, uploadBase: uploadBase}
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// resolveProject prefers an explicit project (parsed from a resource name),
// falling back to the gRPC routing metadata then the configured default.
func resolveProject(ctx context.Context, project, defaultProj string) string {
	if project != "" {
		return project
	}
	return grpcutil.ProjectFromMetadata(ctx, defaultProj)
}

// ─── v1 CloudFunctionsService ────────────────────────────────────────────────

func (s *Service) CreateFunction(ctx context.Context, req *functionspb.CreateFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, err := core.ParseLocationParent(req.GetLocation())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	in := core.FunctionInputFromMap(protojsonToMap(req.GetFunction()), core.V1)
	_, op, err := s.core.CreateFunction(ctx, project, location, "", in, core.V1)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV1(project, op)
}

func (s *Service) UpdateFunction(ctx context.Context, req *functionspb.UpdateFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, id, err := core.ParseFunctionName(req.GetFunction().GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	in := core.FunctionInputFromMap(protojsonToMap(req.GetFunction()), core.V1)
	_, op, err := s.core.UpdateFunction(ctx, project, location, id, in, req.GetUpdateMask().GetPaths(), core.V1)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV1(project, op)
}

func (s *Service) DeleteFunction(ctx context.Context, req *functionspb.DeleteFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	op, err := s.core.DeleteFunction(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV1(project, op)
}

func (s *Service) GetFunction(ctx context.Context, req *functionspb.GetFunctionRequest) (*functionspb.CloudFunction, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	f, err := s.core.GetFunction(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return functionToProtoV1(project, f), nil
}

func (s *Service) ListFunctions(ctx context.Context, req *functionspb.ListFunctionsRequest) (*functionspb.ListFunctionsResponse, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	page, next, err := s.core.ListFunctions(ctx, project, location, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &functionspb.ListFunctionsResponse{NextPageToken: next}
	for _, f := range page {
		out.Functions = append(out.Functions, functionToProtoV1(project, f))
	}
	return out, nil
}

func (s *Service) GenerateUploadUrl(ctx context.Context, req *functionspb.GenerateUploadUrlRequest) (*functionspb.GenerateUploadUrlResponse, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	up, err := s.core.GenerateUploadURL(ctx, project, location, s.uploadBase)
	if err != nil {
		return nil, mapError(err)
	}
	// v1 GenerateUploadUrlResponse carries only the upload URL (there is no
	// storageSource field in the v1 proto).
	return &functionspb.GenerateUploadUrlResponse{UploadUrl: up.URL}, nil
}

func (s *Service) GenerateDownloadUrl(ctx context.Context, req *functionspb.GenerateDownloadUrlRequest) (*functionspb.GenerateDownloadUrlResponse, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	url, err := s.core.GenerateDownloadURL(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &functionspb.GenerateDownloadUrlResponse{DownloadUrl: url}, nil
}

// CallFunction invokes a function synchronously via the shared core's Lambda
// executor — the same executor the REST Function.CallFunction uses (one store,
// one executor). A function-execution failure is reported in-band on the
// response's error field, matching real Cloud Functions; only control-plane
// failures (e.g. NotFound) become gRPC status errors.
func (s *Service) CallFunction(ctx context.Context, req *functionspb.CallFunctionRequest) (*functionspb.CallFunctionResponse, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	executionID, result, invokeErr, err := s.core.CallFunction(ctx, project, location, id, req.GetData())
	if err != nil {
		return nil, mapError(err)
	}
	return &functionspb.CallFunctionResponse{
		ExecutionId: executionID,
		Result:      result,
		Error:       invokeErr,
	}, nil
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project, location, id, err := core.ParseFunctionName(req.GetResource())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	pol, err := s.core.GetIamPolicy(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project, location, id, err := core.ParseFunctionName(req.GetResource())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	pol, err := s.core.SetIamPolicy(ctx, project, location, id, protojsonToMap(req.GetPolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	project, location, id, err := core.ParseFunctionName(req.GetResource())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	perms, err := s.core.TestIamPermissions(ctx, project, location, id, req.GetPermissions())
	if err != nil {
		return nil, mapError(err)
	}
	return &iampb.TestIamPermissionsResponse{Permissions: perms}, nil
}

// ─── v2 FunctionService ──────────────────────────────────────────────────────

func (s *ServiceV2) CreateFunction(ctx context.Context, req *apiv2functionspb.CreateFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	in := core.FunctionInputFromMap(protojsonToMap(req.GetFunction()), core.V2)
	_, op, err := s.core.CreateFunction(ctx, project, location, req.GetFunctionId(), in, core.V2)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV2(project, op)
}

func (s *ServiceV2) UpdateFunction(ctx context.Context, req *apiv2functionspb.UpdateFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, id, err := core.ParseFunctionName(req.GetFunction().GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	in := core.FunctionInputFromMap(protojsonToMap(req.GetFunction()), core.V2)
	_, op, err := s.core.UpdateFunction(ctx, project, location, id, in, req.GetUpdateMask().GetPaths(), core.V2)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV2(project, op)
}

func (s *ServiceV2) DeleteFunction(ctx context.Context, req *apiv2functionspb.DeleteFunctionRequest) (*longrunningpb.Operation, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	op, err := s.core.DeleteFunction(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProtoV2(project, op)
}

func (s *ServiceV2) GetFunction(ctx context.Context, req *apiv2functionspb.GetFunctionRequest) (*apiv2functionspb.Function, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	f, err := s.core.GetFunction(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return functionToProtoV2(project, f), nil
}

func (s *ServiceV2) ListFunctions(ctx context.Context, req *apiv2functionspb.ListFunctionsRequest) (*apiv2functionspb.ListFunctionsResponse, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	page, next, err := s.core.ListFunctions(ctx, project, location, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &apiv2functionspb.ListFunctionsResponse{NextPageToken: next}
	for _, f := range page {
		out.Functions = append(out.Functions, functionToProtoV2(project, f))
	}
	return out, nil
}

// ListRuntimes returns the v2 runtime catalog over the shared core. It is the
// v2-deploy enabler: a client resolves a function's runtime from this list.
func (s *ServiceV2) ListRuntimes(ctx context.Context, req *apiv2functionspb.ListRuntimesRequest) (*apiv2functionspb.ListRuntimesResponse, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	runtimes, err := s.core.ListRuntimes(project, location, req.GetFilter())
	if err != nil {
		return nil, mapError(err)
	}
	out := &apiv2functionspb.ListRuntimesResponse{}
	for _, rt := range runtimes {
		out.Runtimes = append(out.Runtimes, runtimeToProto(rt))
	}
	return out, nil
}

func (s *ServiceV2) GenerateUploadUrl(ctx context.Context, req *apiv2functionspb.GenerateUploadUrlRequest) (*apiv2functionspb.GenerateUploadUrlResponse, error) {
	project, location, err := core.ParseLocationParent(req.GetParent())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	up, err := s.core.GenerateUploadURL(ctx, project, location, s.uploadBase)
	if err != nil {
		return nil, mapError(err)
	}
	return &apiv2functionspb.GenerateUploadUrlResponse{
		UploadUrl: up.URL,
		StorageSource: &apiv2functionspb.StorageSource{
			Bucket: up.Bucket,
			Object: up.Object,
		},
	}, nil
}

func (s *ServiceV2) GenerateDownloadUrl(ctx context.Context, req *apiv2functionspb.GenerateDownloadUrlRequest) (*apiv2functionspb.GenerateDownloadUrlResponse, error) {
	project, location, id, err := core.ParseFunctionName(req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	project = resolveProject(ctx, project, s.defaultProj)
	url, err := s.core.GenerateDownloadURL(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return &apiv2functionspb.GenerateDownloadUrlResponse{DownloadUrl: url}, nil
}

// policyToProto renders an internal policy.Policy as an iampb.Policy.
func policyToProto(p policy.Policy) *iampb.Policy {
	var out iampb.Policy
	if err := mapToProto(policy.ToMap(p), &out); err != nil {
		return &iampb.Policy{}
	}
	return &out
}

// ─── google.longrunning.Operations resolvers (J58/J60) ───────────────────────
//
// Function mutations return their operation inline, but a gRPC client that
// explicitly polls Operations.GetOperation/:wait must get the typed Operation
// (a function/Empty Any), not the shared stub's empty terminal op. Both the v1
// and v2 function services register a resolver with the shared Operations
// service (see cmd/jaiscloud-gcp/main.go).

// isTopLevelOperationName reports whether name has the v1 functions operation
// shape "operations/{id}".
func isTopLevelOperationName(name string) bool {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	return len(parts) == 2 && parts[0] == "operations" && parts[1] != ""
}

// parseV2OperationName parses a location-scoped operation name
// ("projects/{p}/locations/{l}/operations/{id}") into its parts.
func parseV2OperationName(name string) (project, location, id string, ok bool) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		switch parts[i] {
		case "projects":
			project = parts[i+1]
		case "locations":
			location = parts[i+1]
		case "operations":
			id = parts[i+1]
		}
	}
	return project, location, id, id != "" && location != ""
}

// ResolveOperation resolves a v1 function operation name (operations/{id}). The
// name is unambiguously the v1 functions shape, so an unseen id is a NotFound.
func (s *Service) ResolveOperation(ctx context.Context, name string) (*longrunningpb.Operation, bool, error) {
	if !isTopLevelOperationName(name) {
		return nil, false, nil
	}
	project := resolveProject(ctx, "", s.defaultProj)
	op, err := s.core.LoadOperation(ctx, project, name)
	if err != nil {
		return nil, true, mapError(err)
	}
	out, err := operationToProtoV1(project, op)
	if err != nil {
		return nil, true, err
	}
	return out, true, nil
}

// ResolveOperation resolves a v2 function operation name
// (projects/{p}/locations/{l}/operations/{id}). A location-scoped name is shared
// with other services, so an id absent from the function store is not handled
// here — the shared stub (or another resolver) answers it instead.
func (s *ServiceV2) ResolveOperation(ctx context.Context, name string) (*longrunningpb.Operation, bool, error) {
	project, _, _, ok := parseV2OperationName(name)
	if !ok {
		return nil, false, nil
	}
	project = resolveProject(ctx, project, s.defaultProj)
	op, err := s.core.LoadOperation(ctx, project, name)
	if err != nil {
		if core.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, true, mapError(err)
	}
	out, err := operationToProtoV2(project, op)
	if err != nil {
		return nil, true, err
	}
	return out, true, nil
}

// ListOperations implements the shared google.longrunning.Operations
// ListRegistry surface for v2 function operations. The parent must be a
// location parent (projects/{p}/locations/{l}); any other parent is declined
// (handled=false) so the empty page answers. The `filter` parameter is
// forwarded to the core, which evaluates the AIP-160 subset (or returns
// InvalidArgument for one it cannot parse); returnPartialSuccess is rejected
// once by the shared Operations service. v1 operations are top-level (J60) and
// deliberately do not implement this surface, so a location-scoped parent is
// owned by v2.
//
// A location parent is shared with other location-scoped services (Workflows,
// Metastore, Managed Kafka), so v2 claims it only when the location actually
// holds function operations — otherwise it declines and those services' list
// calls are not shadowed by an unrelated page.
func (s *ServiceV2) ListOperations(ctx context.Context, parent string, pageSize int32, pageToken, filter string) (*longrunningpb.ListOperationsResponse, bool, error) {
	project, location, ok := parseLocationParent(parent)
	if !ok {
		return nil, false, nil
	}
	project = resolveProject(ctx, project, s.defaultProj)
	owned, _, err := s.core.ListOperations(ctx, project, location, core.V2, "", 1, "")
	if err != nil {
		return nil, true, mapError(err)
	}
	if len(owned) == 0 {
		return nil, false, nil
	}
	ops, next, err := s.core.ListOperations(ctx, project, location, core.V2, filter, int(pageSize), pageToken)
	if err != nil {
		return nil, true, mapError(err)
	}
	out := &longrunningpb.ListOperationsResponse{NextPageToken: next}
	for _, op := range ops {
		p, err := operationToProtoV2(project, op)
		if err != nil {
			return nil, true, err
		}
		out.Operations = append(out.Operations, p)
	}
	return out, true, nil
}

// parseLocationParent validates a location parent
// ("projects/{p}/locations/{l}") and returns its project and location. Service
// Usage shares the top-level operations namespace, but a location-scoped
// collection (".../operations") is not a location parent and is declined.
func parseLocationParent(parent string) (project, location string, ok bool) {
	parts := strings.Split(strings.Trim(parent, "/"), "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "locations" || parts[1] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// compile-time assertions that the servers implement the generated interfaces
// and the shared google.longrunning.Operations surfaces they register.
var (
	_ functionspb.CloudFunctionsServiceServer = (*Service)(nil)
	_ apiv2functionspb.FunctionServiceServer  = (*ServiceV2)(nil)
	_ grpcoperations.Resolver                 = (*Service)(nil)
	_ grpcoperations.Resolver                 = (*ServiceV2)(nil)
	_ grpcoperations.ListRegistry             = (*ServiceV2)(nil)
)
