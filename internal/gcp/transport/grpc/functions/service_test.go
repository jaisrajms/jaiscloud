package functions

import (
	"context"
	"errors"
	"strings"
	"testing"

	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"jaiscloud/internal/blobfs"
	lambdaexec "jaiscloud/internal/executor/awslambda"
	core "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

func newTestV1() *Service {
	return NewService(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		core.WithBlobs(blobfs.NewMemoryBlobStore())), "proj", "http://localhost:8080")
}

func newTestV2() *ServiceV2 {
	return NewServiceV2(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		core.WithBlobs(blobfs.NewMemoryBlobStore())), "proj", "http://localhost:8080")
}

func createV1Req(id string) *functionspb.CreateFunctionRequest {
	return &functionspb.CreateFunctionRequest{
		Location: "projects/proj/locations/us-central1",
		Function: &functionspb.CloudFunction{
			Name:       "projects/proj/locations/us-central1/functions/" + id,
			Runtime:    "nodejs20",
			EntryPoint: "handler",
		},
	}
}

func TestCreateFunctionV1_TypedOperation(t *testing.T) {
	s := newTestV1()
	op, err := s.CreateFunction(context.Background(), createV1Req("f1"))
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	if !op.GetDone() {
		t.Fatal("operation not done")
	}
	var meta functionspb.OperationMetadataV1
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("metadata UnmarshalTo: %v", err)
	}
	if meta.GetTarget() != "projects/proj/locations/us-central1/functions/f1" || meta.GetType() != functionspb.OperationType_CREATE_FUNCTION {
		t.Fatalf("metadata = %+v", &meta)
	}
	var fn functionspb.CloudFunction
	if err := op.GetResponse().UnmarshalTo(&fn); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if fn.GetName() != "projects/proj/locations/us-central1/functions/f1" || fn.GetStatus() != functionspb.CloudFunctionStatus_ACTIVE {
		t.Fatalf("function = %+v", &fn)
	}
}

func TestGetListUpdateDeleteV1(t *testing.T) {
	ctx := context.Background()
	s := newTestV1()
	if _, err := s.CreateFunction(ctx, createV1Req("f1")); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	fn, err := s.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: "projects/proj/locations/us-central1/functions/f1"})
	if err != nil {
		t.Fatalf("GetFunction: %v", err)
	}
	if fn.GetRuntime() != "nodejs20" || fn.GetEntryPoint() != "handler" {
		t.Fatalf("function = %+v", fn)
	}
	list, err := s.ListFunctions(ctx, &functionspb.ListFunctionsRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil {
		t.Fatalf("ListFunctions: %v", err)
	}
	if len(list.GetFunctions()) != 1 {
		t.Fatalf("functions = %+v", list.GetFunctions())
	}
	op, err := s.UpdateFunction(ctx, &functionspb.UpdateFunctionRequest{
		Function:   &functionspb.CloudFunction{Name: "projects/proj/locations/us-central1/functions/f1", Runtime: "nodejs22"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"runtime"}},
	})
	if err != nil {
		t.Fatalf("UpdateFunction: %v", err)
	}
	var updated functionspb.CloudFunction
	if err := op.GetResponse().UnmarshalTo(&updated); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if updated.GetRuntime() != "nodejs22" || updated.GetEntryPoint() != "handler" {
		t.Fatalf("updated = %+v", &updated)
	}
	delOp, err := s.DeleteFunction(ctx, &functionspb.DeleteFunctionRequest{Name: "projects/proj/locations/us-central1/functions/f1"})
	if err != nil {
		t.Fatalf("DeleteFunction: %v", err)
	}
	if delOp.GetResponse().GetTypeUrl() != "type.googleapis.com/google.protobuf.Empty" {
		t.Fatalf("delete response type = %q, want Empty", delOp.GetResponse().GetTypeUrl())
	}
	if _, err := s.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: "projects/proj/locations/us-central1/functions/f1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetFunction after delete = %v, want NotFound", err)
	}
}

func TestCreateFunctionV2_TypedOperation(t *testing.T) {
	s := newTestV2()
	op, err := s.CreateFunction(context.Background(), &apiv2functionspb.CreateFunctionRequest{
		Parent:     "projects/proj/locations/us-central1",
		FunctionId: "f2",
		Function: &apiv2functionspb.Function{
			BuildConfig: &apiv2functionspb.BuildConfig{Runtime: "nodejs20", EntryPoint: "handler"},
		},
	})
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	if !op.GetDone() {
		t.Fatal("operation not done")
	}
	var meta apiv2functionspb.OperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("metadata UnmarshalTo: %v", err)
	}
	if meta.GetTarget() != "projects/proj/locations/us-central1/functions/f2" || meta.GetVerb() != "create" {
		t.Fatalf("metadata = %+v", &meta)
	}
	var fn apiv2functionspb.Function
	if err := op.GetResponse().UnmarshalTo(&fn); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if fn.GetName() != "projects/proj/locations/us-central1/functions/f2" || fn.GetState() != apiv2functionspb.Function_ACTIVE {
		t.Fatalf("function = %+v", &fn)
	}
	if fn.GetBuildConfig().GetRuntime() != "nodejs20" {
		t.Fatalf("buildConfig = %+v", fn.GetBuildConfig())
	}
}

// TestInstanceConfigV2_FD6 pins the v2 ServiceConfig instance/concurrency
// settings over gRPC: they survive the protojson round-trip on create and get,
// and an out-of-range value maps to InvalidArgument.
func TestInstanceConfigV2_FD6(t *testing.T) {
	s := newTestV2()
	ctx := context.Background()
	op, err := s.CreateFunction(ctx, &apiv2functionspb.CreateFunctionRequest{
		Parent:     "projects/proj/locations/us-central1",
		FunctionId: "cfg",
		Function: &apiv2functionspb.Function{
			BuildConfig: &apiv2functionspb.BuildConfig{Runtime: "nodejs22"},
			ServiceConfig: &apiv2functionspb.ServiceConfig{
				MinInstanceCount:              2,
				MaxInstanceCount:              10,
				MaxInstanceRequestConcurrency: 80,
				AvailableCpu:                  "1",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	var created apiv2functionspb.Function
	if err := op.GetResponse().UnmarshalTo(&created); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	cs := created.GetServiceConfig()
	if cs.GetMinInstanceCount() != 2 || cs.GetMaxInstanceCount() != 10 ||
		cs.GetMaxInstanceRequestConcurrency() != 80 || cs.GetAvailableCpu() != "1" {
		t.Fatalf("create serviceConfig = %+v", cs)
	}

	got, err := s.GetFunction(ctx, &apiv2functionspb.GetFunctionRequest{
		Name: "projects/proj/locations/us-central1/functions/cfg",
	})
	if err != nil {
		t.Fatalf("GetFunction: %v", err)
	}
	gs := got.GetServiceConfig()
	if gs.GetMinInstanceCount() != 2 || gs.GetMaxInstanceCount() != 10 ||
		gs.GetMaxInstanceRequestConcurrency() != 80 || gs.GetAvailableCpu() != "1" {
		t.Fatalf("get serviceConfig = %+v", gs)
	}

	// An updateMask path applies the new value over gRPC (protojson round-trip).
	upd, err := s.UpdateFunction(ctx, &apiv2functionspb.UpdateFunctionRequest{
		Function: &apiv2functionspb.Function{
			Name:          "projects/proj/locations/us-central1/functions/cfg",
			ServiceConfig: &apiv2functionspb.ServiceConfig{MaxInstanceCount: 20},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"serviceConfig.maxInstanceCount"}},
	})
	if err != nil {
		t.Fatalf("UpdateFunction: %v", err)
	}
	var updated apiv2functionspb.Function
	if err := upd.GetResponse().UnmarshalTo(&updated); err != nil {
		t.Fatalf("update response UnmarshalTo: %v", err)
	}
	if updated.GetServiceConfig().GetMaxInstanceCount() != 20 {
		t.Fatalf("updated serviceConfig = %+v", updated.GetServiceConfig())
	}

	_, err = s.CreateFunction(ctx, &apiv2functionspb.CreateFunctionRequest{
		Parent:     "projects/proj/locations/us-central1",
		FunctionId: "bad",
		Function: &apiv2functionspb.Function{
			BuildConfig:   &apiv2functionspb.BuildConfig{Runtime: "nodejs22"},
			ServiceConfig: &apiv2functionspb.ServiceConfig{MaxInstanceCount: 1001},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateFunction out-of-range = %v, want InvalidArgument", err)
	}
}

func TestGenerateURLs(t *testing.T) {
	ctx := context.Background()
	s := newTestV1()
	if _, err := s.CreateFunction(ctx, createV1Req("f1")); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	up, err := s.GenerateUploadUrl(ctx, &functionspb.GenerateUploadUrlRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil || up.GetUploadUrl() == "" {
		t.Fatalf("GenerateUploadUrl = %v, %v", up, err)
	}
	dl, err := s.GenerateDownloadUrl(ctx, &functionspb.GenerateDownloadUrlRequest{Name: "projects/proj/locations/us-central1/functions/f1"})
	if err != nil || dl.GetDownloadUrl() == "" {
		t.Fatalf("GenerateDownloadUrl = %v, %v", dl, err)
	}
	if _, err := s.GenerateDownloadUrl(ctx, &functionspb.GenerateDownloadUrlRequest{Name: "projects/proj/locations/us-central1/functions/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GenerateDownloadUrl missing = %v, want NotFound", err)
	}
}

func TestIAMV1(t *testing.T) {
	ctx := context.Background()
	s := newTestV1()
	if _, err := s.CreateFunction(ctx, createV1Req("f1")); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	name := "projects/proj/locations/us-central1/functions/f1"
	if _, err := s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: name,
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/cloudfunctions.invoker", Members: []string{"allUsers"}}}},
	}); err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	pol, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if len(pol.GetBindings()) != 1 {
		t.Fatalf("bindings = %+v", pol.GetBindings())
	}
	resp, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: name, Permissions: []string{"cloudfunctions.functions.invoke"}})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(resp.GetPermissions()) != 1 {
		t.Fatalf("permissions = %v", resp.GetPermissions())
	}
}

// errExecutor fails every invocation, to exercise the in-band error path.
type errExecutor struct{}

func (errExecutor) Invoke(context.Context, lambdaexec.InvokeRequest) (lambdaexec.InvokeResult, error) {
	return lambdaexec.InvokeResult{}, errors.New("boom")
}
func (errExecutor) DeleteFunction(context.Context, string) {}
func (errExecutor) Reset(context.Context)                  {}
func (errExecutor) Close() error                           { return nil }

// newTestV1WithExecutor returns a v1 server whose core uses the given executor.
func newTestV1WithExecutor(e lambdaexec.LambdaExecutor) *Service {
	return NewService(core.NewService(
		functionsstore.NewMemoryStore(),
		store.NewMemoryResourceStore(),
		core.WithExecutor(e),
		core.WithBlobs(blobfs.NewMemoryBlobStore()),
	), "proj", "http://localhost:8080")
}

// TestCallFunctionV1 covers runtime invocation over the shared Lambda executor:
// the mock-echo success path, an in-band executor error (returned on the
// response, not as a gRPC status), NotFound for a missing function, and
// InvalidArgument for a malformed resource name.
func TestCallFunctionV1(t *testing.T) {
	ctx := context.Background()
	s := newTestV1()
	if _, err := s.CreateFunction(ctx, createV1Req("f1")); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	resp, err := s.CallFunction(ctx, &functionspb.CallFunctionRequest{
		Name: "projects/proj/locations/us-central1/functions/f1",
		Data: `{"hello":"world"}`,
	})
	if err != nil {
		t.Fatalf("CallFunction: %v", err)
	}
	if resp.GetExecutionId() == "" {
		t.Fatal("empty executionId")
	}
	if resp.GetError() != "" {
		t.Fatalf("unexpected in-band error: %q", resp.GetError())
	}
	if resp.GetResult() != `{"hello":"world"}` {
		t.Fatalf("result = %q, want echoed payload", resp.GetResult())
	}

	failing := newTestV1WithExecutor(errExecutor{})
	if _, err := failing.CreateFunction(ctx, createV1Req("f2")); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	resp, err = failing.CallFunction(ctx, &functionspb.CallFunctionRequest{
		Name: "projects/proj/locations/us-central1/functions/f2",
		Data: "ignored",
	})
	if err != nil {
		t.Fatalf("CallFunction (failing executor): %v", err)
	}
	if resp.GetExecutionId() == "" {
		t.Fatal("empty executionId on error path")
	}
	if resp.GetError() != "boom" || resp.GetResult() != "" {
		t.Fatalf("in-band error = %q, result = %q; want boom/empty", resp.GetError(), resp.GetResult())
	}

	if _, err := s.CallFunction(ctx, &functionspb.CallFunctionRequest{
		Name: "projects/proj/locations/us-central1/functions/missing",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("CallFunction missing = %v, want NotFound", err)
	}
	if _, err := s.CallFunction(ctx, &functionspb.CallFunctionRequest{Name: "not-a-function"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CallFunction malformed name = %v, want InvalidArgument", err)
	}
}

// TestListRuntimesV2 returns the v2 runtime catalog over the shared core: the
// catalog is non-empty, nodejs20 is GEN_2/GA, a filter narrows it to one
// runtime, and a malformed parent is InvalidArgument.
func TestListRuntimesV2(t *testing.T) {
	ctx := context.Background()
	v2 := newTestV2()

	resp, err := v2.ListRuntimes(ctx, &apiv2functionspb.ListRuntimesRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil {
		t.Fatalf("ListRuntimes: %v", err)
	}
	if len(resp.GetRuntimes()) == 0 {
		t.Fatal("empty runtime list")
	}
	var node bool
	for _, rt := range resp.GetRuntimes() {
		if rt.GetName() == "nodejs20" {
			node = true
			if rt.GetEnvironment() != apiv2functionspb.Environment_GEN_2 ||
				rt.GetStage() != apiv2functionspb.ListRuntimesResponse_GA {
				t.Fatalf("nodejs20 = %+v", rt)
			}
		}
	}
	if !node {
		t.Fatal("nodejs20 missing from catalog")
	}

	// A deprecated runtime must transcode its stage, warnings, and
	// deprecationDate (the nested google.type.Date) onto the proto.
	var node18 *apiv2functionspb.ListRuntimesResponse_Runtime
	for _, rt := range resp.GetRuntimes() {
		if rt.GetName() == "nodejs18" {
			node18 = rt
		}
	}
	if node18 == nil {
		t.Fatal("nodejs18 missing from catalog")
	}
	if node18.GetStage() != apiv2functionspb.ListRuntimesResponse_DEPRECATED {
		t.Fatalf("nodejs18 stage = %v, want DEPRECATED", node18.GetStage())
	}
	if len(node18.GetWarnings()) == 0 {
		t.Fatal("nodejs18 missing warnings")
	}
	if d := node18.GetDeprecationDate(); d == nil || d.GetYear() != 2025 {
		t.Fatalf("nodejs18 deprecationDate = %v", d)
	}

	filtered, err := v2.ListRuntimes(ctx, &apiv2functionspb.ListRuntimesRequest{
		Parent: "projects/proj/locations/us-central1",
		Filter: `name="python312"`,
	})
	if err != nil {
		t.Fatalf("ListRuntimes filter: %v", err)
	}
	if len(filtered.GetRuntimes()) != 1 || filtered.GetRuntimes()[0].GetName() != "python312" {
		t.Fatalf("filtered = %+v", filtered.GetRuntimes())
	}

	// The AIP-160 subset (OR + parentheses) is honored over gRPC too.
	ored, err := v2.ListRuntimes(ctx, &apiv2functionspb.ListRuntimesRequest{
		Parent: "projects/proj/locations/us-central1",
		Filter: `(name="python312" OR name="go122") AND environment="GEN_2"`,
	})
	if err != nil {
		t.Fatalf("ListRuntimes OR filter: %v", err)
	}
	if len(ored.GetRuntimes()) != 2 {
		t.Fatalf("OR filter = %+v, want 2 runtimes", ored.GetRuntimes())
	}

	if _, err := v2.ListRuntimes(ctx, &apiv2functionspb.ListRuntimesRequest{Parent: "projects/proj"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed parent = %v, want InvalidArgument", err)
	}
}

// TestResolveOperationV1 covers J58/J60: a v1 function operation is named
// top-level (operations/{id}) and the shared Operations resolver returns the
// typed CloudFunction response — not the empty terminal stub.
func TestResolveOperationV1(t *testing.T) {
	s := newTestV1()
	ctx := context.Background()
	op, err := s.CreateFunction(ctx, createV1Req("f1"))
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	if !strings.HasPrefix(op.GetName(), "operations/") {
		t.Fatalf("v1 operation name = %q, want operations/{id}", op.GetName())
	}

	got, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	var fn functionspb.CloudFunction
	if err := got.GetResponse().UnmarshalTo(&fn); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if fn.GetName() != "projects/proj/locations/us-central1/functions/f1" {
		t.Fatalf("resolved function = %+v", &fn)
	}

	// A top-level name for an unknown id is a NotFound handed back by the
	// functions resolver.
	if _, handled, err := s.ResolveOperation(ctx, "operations/missing"); !handled || status.Code(err) != codes.NotFound {
		t.Fatalf("unknown v1 op: handled=%v err=%v, want handled+NotFound", handled, err)
	}
	// A location-scoped name is not the v1 shape, so v1 declines to handle it.
	if _, handled, _ := s.ResolveOperation(ctx, "projects/proj/locations/us-central1/operations/x"); handled {
		t.Fatal("v1 resolver should not handle a location-scoped name")
	}
}

// TestResolveOperationV2 covers J58: a v2 function operation is resolved by the
// shared Operations service to its typed Function response.
func TestResolveOperationV2(t *testing.T) {
	s := newTestV2()
	ctx := context.Background()
	op, err := s.CreateFunction(ctx, &apiv2functionspb.CreateFunctionRequest{
		Parent:     "projects/proj/locations/us-central1",
		FunctionId: "f2",
		Function: &apiv2functionspb.Function{
			BuildConfig: &apiv2functionspb.BuildConfig{Runtime: "nodejs20", EntryPoint: "handler"},
		},
	})
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	got, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	var fn apiv2functionspb.Function
	if err := got.GetResponse().UnmarshalTo(&fn); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if fn.GetName() != "projects/proj/locations/us-central1/functions/f2" {
		t.Fatalf("resolved function = %+v", &fn)
	}
	// An unknown id under a location-scoped name is not claimed by functions.
	if _, handled, _ := s.ResolveOperation(ctx, "projects/proj/locations/us-central1/operations/missing"); handled {
		t.Fatal("v2 resolver should not claim an unknown operation id")
	}
}
