package grpcconformance

import (
	"context"
	"fmt"

	functionsv1 "cloud.google.com/go/functions/apiv1"
	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	functionsv2 "cloud.google.com/go/functions/apiv2"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// functionsChecks covers both Cloud Functions gRPC services —
// google.cloud.functions.v1.CloudFunctionsService and
// google.cloud.functions.v2.FunctionService — via the official generated
// clients. Create/Update/Delete return a done long-running operation whose
// response is packed as a typed Any, so the client's Wait observes it inline.
// The two API versions share one emulator store, so a v1-created function is
// visible to v2.
//
// CallFunction (v1 runtime invocation) is probed over the shared executor, and
// ListRuntimes (v2) is probed over the shared runtime catalog (the v2-deploy
// enabler: a client resolves a runtime from this list).
func functionsChecks() []Check {
	return []Check{
		// v1 CloudFunctionsService.
		{Service: "functions", RPC: "CreateFunction (v1)", Method: "CreateFunction", KeyField: "LRO done + ACTIVE function", Run: checkFnCreateV1},
		{Service: "functions", RPC: "GetFunction (v1)", Method: "GetFunction", KeyField: "runtime/entryPoint round-trip", Run: checkFnGetV1},
		{Service: "functions", RPC: "ListFunctions (v1)", Method: "ListFunctions", KeyField: "created function present", Run: checkFnListV1},
		{Service: "functions", RPC: "UpdateFunction (v1)", Method: "UpdateFunction", KeyField: "runtime updated via LRO", Run: checkFnUpdateV1},
		{Service: "functions", RPC: "DeleteFunction (v1)", Method: "DeleteFunction", KeyField: "NotFound after delete", Run: checkFnDeleteV1},
		{Service: "functions", RPC: "GenerateUploadUrl (v1)", Method: "GenerateUploadUrl", KeyField: "non-empty uploadUrl", Run: checkFnGenerateUploadURLV1},
		{Service: "functions", RPC: "GenerateDownloadUrl (v1)", Method: "GenerateDownloadUrl", KeyField: "non-empty downloadUrl", Run: checkFnGenerateDownloadURLV1},
		{Service: "functions", RPC: "CallFunction (v1)", Method: "CallFunction", KeyField: "executionId + echoed result", Run: checkFnCallV1},
		{Service: "functions", RPC: "SetIamPolicy (v1)", Method: "SetIamPolicy", KeyField: "binding round-trip", Run: checkFnSetIamV1},
		{Service: "functions", RPC: "GetIamPolicy (v1)", Method: "GetIamPolicy", KeyField: "stored binding returned", Run: checkFnGetIamV1},
		{Service: "functions", RPC: "TestIamPermissions (v1)", Method: "TestIamPermissions", KeyField: "requested permission echoed", Run: checkFnTestIamV1},
		// v2 FunctionService.
		{Service: "functions", RPC: "CreateFunction (v2)", Method: "CreateFunction", KeyField: "LRO done + ACTIVE function", Run: checkFnCreateV2},
		{Service: "functions", RPC: "GetFunction (v2)", Method: "GetFunction", KeyField: "runtime round-trip", Run: checkFnGetV2},
		{Service: "functions", RPC: "ListFunctions (v2)", Method: "ListFunctions", KeyField: "created function present", Run: checkFnListV2},
		{Service: "functions", RPC: "UpdateFunction (v2)", Method: "UpdateFunction", KeyField: "buildConfig.runtime updated via LRO", Run: checkFnUpdateV2},
		{Service: "functions", RPC: "DeleteFunction (v2)", Method: "DeleteFunction", KeyField: "NotFound after delete", Run: checkFnDeleteV2},
		{Service: "functions", RPC: "GenerateUploadUrl (v2)", Method: "GenerateUploadUrl", KeyField: "non-empty uploadUrl + storageSource", Run: checkFnGenerateUploadURLV2},
		{Service: "functions", RPC: "GenerateDownloadUrl (v2)", Method: "GenerateDownloadUrl", KeyField: "non-empty downloadUrl", Run: checkFnGenerateDownloadURLV2},
		{Service: "functions", RPC: "ListRuntimes (v2)", Method: "ListRuntimes", KeyField: "runtime catalog includes nodejs20 (GEN_2)", Run: checkFnListRuntimesV2},
		// Dual-protocol invariant: one store behind both API versions.
		{Service: "functions", RPC: "GetFunction (v1-created, v2-read)", Method: "GetFunction", KeyField: "v1-created function visible to v2", Run: checkFnCrossVersion},
	}
}

const functionsRegion = "us-central1"

func functionsParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, functionsRegion)
}

func functionsClientOptions(cfg Config) []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

func newFunctionsV1Client(ctx context.Context, cfg Config) (*functionsv1.CloudFunctionsClient, error) {
	return functionsv1.NewCloudFunctionsClient(ctx, functionsClientOptions(cfg)...)
}

func newFunctionsV2Client(ctx context.Context, cfg Config) (*functionsv2.FunctionClient, error) {
	return functionsv2.NewFunctionClient(ctx, functionsClientOptions(cfg)...)
}

// functionsV1Name is the full v1/v2 resource name for a probe function id.
func functionsV1Name(cfg Config, id string) string {
	return functionsParent(cfg) + "/functions/" + id
}

// ensureFunctionV1 creates the run-unique probe function via the v1 client,
// treating AlreadyExists as success.
func ensureFunctionV1(ctx context.Context, client *functionsv1.CloudFunctionsClient, cfg Config, id string) error {
	op, err := client.CreateFunction(ctx, &functionspb.CreateFunctionRequest{
		Location: functionsParent(cfg),
		Function: &functionspb.CloudFunction{
			Name:       functionsV1Name(cfg, id),
			Runtime:    "nodejs20",
			EntryPoint: "handler",
		},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		return fmt.Errorf("create v1: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("create v1 wait: %w", err)
	}
	return nil
}

// ensureFunctionV2 creates the run-unique probe function via the v2 client,
// treating AlreadyExists as success.
func ensureFunctionV2(ctx context.Context, client *functionsv2.FunctionClient, cfg Config, id string) error {
	op, err := client.CreateFunction(ctx, &apiv2functionspb.CreateFunctionRequest{
		Parent:     functionsParent(cfg),
		FunctionId: id,
		Function: &apiv2functionspb.Function{
			BuildConfig: &apiv2functionspb.BuildConfig{Runtime: "nodejs20", EntryPoint: "handler"},
		},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		return fmt.Errorf("create v2: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("create v2 wait: %w", err)
	}
	return nil
}

// ─── v1 ──────────────────────────────────────────────────────────────────────

func checkFnCreateV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-fn-v1-create")
	op, err := client.CreateFunction(ctx, &functionspb.CreateFunctionRequest{
		Location: functionsParent(cfg),
		Function: &functionspb.CloudFunction{Name: functionsV1Name(cfg, id), Runtime: "nodejs20", EntryPoint: "handler"},
	})
	if err != nil {
		return fmt.Errorf("CreateFunction: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateFunction operation not done")
	}
	fn, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if fn.GetName() != functionsV1Name(cfg, id) {
		return fmt.Errorf("name = %q, want %q", fn.GetName(), functionsV1Name(cfg, id))
	}
	if fn.GetStatus() != functionspb.CloudFunctionStatus_ACTIVE {
		return fmt.Errorf("status = %v, want ACTIVE", fn.GetStatus())
	}
	return nil
}

func checkFnGetV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v1-get")
	if err := ensureFunctionV1(ctx, client, cfg, id); err != nil {
		return err
	}
	fn, err := client.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: functionsV1Name(cfg, id)})
	if err != nil {
		return fmt.Errorf("GetFunction: %w", err)
	}
	if fn.GetRuntime() != "nodejs20" || fn.GetEntryPoint() != "handler" {
		return fmt.Errorf("function = %+v", fn)
	}
	return nil
}

func checkFnListV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v1-list")
	if err := ensureFunctionV1(ctx, client, cfg, id); err != nil {
		return err
	}
	it := client.ListFunctions(ctx, &functionspb.ListFunctionsRequest{Parent: functionsParent(cfg)})
	for {
		fn, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListFunctions did not include %q", id)
		}
		if err != nil {
			return fmt.Errorf("ListFunctions: %w", err)
		}
		if fn.GetName() == functionsV1Name(cfg, id) {
			return nil
		}
	}
}

func checkFnUpdateV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v1-update")
	if err := ensureFunctionV1(ctx, client, cfg, id); err != nil {
		return err
	}
	op, err := client.UpdateFunction(ctx, &functionspb.UpdateFunctionRequest{
		Function:   &functionspb.CloudFunction{Name: functionsV1Name(cfg, id), Runtime: "nodejs22"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"runtime"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateFunction: %w", err)
	}
	fn, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if fn.GetRuntime() != "nodejs22" {
		return fmt.Errorf("runtime = %q, want nodejs22", fn.GetRuntime())
	}
	return nil
}

func checkFnDeleteV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v1-del")
	if err := ensureFunctionV1(ctx, client, cfg, id); err != nil {
		return err
	}
	op, err := client.DeleteFunction(ctx, &functionspb.DeleteFunctionRequest{Name: functionsV1Name(cfg, id)})
	if err != nil {
		return fmt.Errorf("DeleteFunction: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetFunction(ctx, &functionspb.GetFunctionRequest{Name: functionsV1Name(cfg, id)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetFunction after delete = %v, want NotFound", err)
	}
	return nil
}

func checkFnGenerateUploadURLV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	resp, err := client.GenerateUploadUrl(ctx, &functionspb.GenerateUploadUrlRequest{Parent: functionsParent(cfg)})
	if err != nil {
		return fmt.Errorf("GenerateUploadUrl: %w", err)
	}
	if resp.GetUploadUrl() == "" {
		return fmt.Errorf("empty uploadUrl")
	}
	return nil
}

func checkFnGenerateDownloadURLV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v1-dl")
	if err := ensureFunctionV1(ctx, client, cfg, id); err != nil {
		return err
	}
	resp, err := client.GenerateDownloadUrl(ctx, &functionspb.GenerateDownloadUrlRequest{Name: functionsV1Name(cfg, id)})
	if err != nil {
		return fmt.Errorf("GenerateDownloadUrl: %w", err)
	}
	if resp.GetDownloadUrl() == "" {
		return fmt.Errorf("empty downloadUrl")
	}
	return nil
}

// checkFnCallV1 invokes a function through the v1 runtime-invocation RPC. The
// emulator's default mock executor echoes the request payload, so a successful
// call returns a non-empty executionId and the payload as result, with no
// in-band error.
func checkFnCallV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v1-call")
	if err := ensureFunctionV1(ctx, client, cfg, id); err != nil {
		return err
	}
	const payload = `{"hello":"world"}`
	resp, err := client.CallFunction(ctx, &functionspb.CallFunctionRequest{
		Name: functionsV1Name(cfg, id),
		Data: payload,
	})
	if err != nil {
		return fmt.Errorf("CallFunction: %w", err)
	}
	if resp.GetExecutionId() == "" {
		return fmt.Errorf("empty executionId")
	}
	if resp.GetError() != "" {
		return fmt.Errorf("unexpected in-band error: %q", resp.GetError())
	}
	if resp.GetResult() != payload {
		return fmt.Errorf("result = %q, want %q", resp.GetResult(), payload)
	}
	return nil
}

func checkFnSetIamV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v1-iam")
	if err := ensureFunctionV1(ctx, client, cfg, id); err != nil {
		return err
	}
	pol, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: functionsV1Name(cfg, id),
		Policy: &iampb.Policy{Bindings: []*iampb.Binding{
			{Role: "roles/cloudfunctions.invoker", Members: []string{"allUsers"}},
		}},
	})
	if err != nil {
		return fmt.Errorf("SetIamPolicy: %w", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != "roles/cloudfunctions.invoker" {
		return fmt.Errorf("policy = %+v", pol)
	}
	return nil
}

func checkFnGetIamV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v1-iam-get")
	if err := ensureFunctionV1(ctx, client, cfg, id); err != nil {
		return err
	}
	if _, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: functionsV1Name(cfg, id),
		Policy: &iampb.Policy{Bindings: []*iampb.Binding{
			{Role: "roles/cloudfunctions.invoker", Members: []string{"allUsers"}},
		}},
	}); err != nil {
		return fmt.Errorf("SetIamPolicy: %w", err)
	}
	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: functionsV1Name(cfg, id)})
	if err != nil {
		return fmt.Errorf("GetIamPolicy: %w", err)
	}
	if len(pol.GetBindings()) != 1 {
		return fmt.Errorf("bindings = %+v, want 1", pol.GetBindings())
	}
	return nil
}

func checkFnTestIamV1(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v1-iam-test")
	if err := ensureFunctionV1(ctx, client, cfg, id); err != nil {
		return err
	}
	resp, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    functionsV1Name(cfg, id),
		Permissions: []string{"cloudfunctions.functions.invoke"},
	})
	if err != nil {
		return fmt.Errorf("TestIamPermissions: %w", err)
	}
	if len(resp.GetPermissions()) != 1 || resp.GetPermissions()[0] != "cloudfunctions.functions.invoke" {
		return fmt.Errorf("permissions = %v", resp.GetPermissions())
	}
	return nil
}

// ─── v2 ──────────────────────────────────────────────────────────────────────

func checkFnCreateV2(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV2Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v2-create")
	op, err := client.CreateFunction(ctx, &apiv2functionspb.CreateFunctionRequest{
		Parent:     functionsParent(cfg),
		FunctionId: id,
		Function: &apiv2functionspb.Function{
			BuildConfig: &apiv2functionspb.BuildConfig{Runtime: "nodejs20", EntryPoint: "handler"},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateFunction: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateFunction operation not done")
	}
	fn, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if fn.GetName() != functionsV1Name(cfg, id) {
		return fmt.Errorf("name = %q, want %q", fn.GetName(), functionsV1Name(cfg, id))
	}
	if fn.GetState() != apiv2functionspb.Function_ACTIVE {
		return fmt.Errorf("state = %v, want ACTIVE", fn.GetState())
	}
	return nil
}

func checkFnGetV2(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV2Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v2-get")
	if err := ensureFunctionV2(ctx, client, cfg, id); err != nil {
		return err
	}
	fn, err := client.GetFunction(ctx, &apiv2functionspb.GetFunctionRequest{Name: functionsV1Name(cfg, id)})
	if err != nil {
		return fmt.Errorf("GetFunction: %w", err)
	}
	if fn.GetBuildConfig().GetRuntime() != "nodejs20" {
		return fmt.Errorf("buildConfig.runtime = %q", fn.GetBuildConfig().GetRuntime())
	}
	return nil
}

func checkFnListV2(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV2Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v2-list")
	if err := ensureFunctionV2(ctx, client, cfg, id); err != nil {
		return err
	}
	it := client.ListFunctions(ctx, &apiv2functionspb.ListFunctionsRequest{Parent: functionsParent(cfg)})
	for {
		fn, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListFunctions did not include %q", id)
		}
		if err != nil {
			return fmt.Errorf("ListFunctions: %w", err)
		}
		if fn.GetName() == functionsV1Name(cfg, id) {
			return nil
		}
	}
}

func checkFnUpdateV2(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV2Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v2-update")
	if err := ensureFunctionV2(ctx, client, cfg, id); err != nil {
		return err
	}
	op, err := client.UpdateFunction(ctx, &apiv2functionspb.UpdateFunctionRequest{
		Function: &apiv2functionspb.Function{
			Name:        functionsV1Name(cfg, id),
			BuildConfig: &apiv2functionspb.BuildConfig{Runtime: "nodejs22"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"buildConfig.runtime"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateFunction: %w", err)
	}
	fn, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if fn.GetBuildConfig().GetRuntime() != "nodejs22" {
		return fmt.Errorf("runtime = %q, want nodejs22", fn.GetBuildConfig().GetRuntime())
	}
	return nil
}

func checkFnDeleteV2(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV2Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v2-del")
	if err := ensureFunctionV2(ctx, client, cfg, id); err != nil {
		return err
	}
	op, err := client.DeleteFunction(ctx, &apiv2functionspb.DeleteFunctionRequest{Name: functionsV1Name(cfg, id)})
	if err != nil {
		return fmt.Errorf("DeleteFunction: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetFunction(ctx, &apiv2functionspb.GetFunctionRequest{Name: functionsV1Name(cfg, id)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetFunction after delete = %v, want NotFound", err)
	}
	return nil
}

func checkFnGenerateUploadURLV2(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV2Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	resp, err := client.GenerateUploadUrl(ctx, &apiv2functionspb.GenerateUploadUrlRequest{Parent: functionsParent(cfg)})
	if err != nil {
		return fmt.Errorf("GenerateUploadUrl: %w", err)
	}
	if resp.GetUploadUrl() == "" {
		return fmt.Errorf("empty uploadUrl")
	}
	// v2 also returns the GCS storageSource the client echoes back through
	// buildConfig.source.storageSource after uploading.
	if resp.GetStorageSource().GetBucket() == "" || resp.GetStorageSource().GetObject() == "" {
		return fmt.Errorf("empty storageSource: %v", resp.GetStorageSource())
	}
	return nil
}

func checkFnGenerateDownloadURLV2(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV2Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-fn-v2-dl")
	if err := ensureFunctionV2(ctx, client, cfg, id); err != nil {
		return err
	}
	resp, err := client.GenerateDownloadUrl(ctx, &apiv2functionspb.GenerateDownloadUrlRequest{Name: functionsV1Name(cfg, id)})
	if err != nil {
		return fmt.Errorf("GenerateDownloadUrl: %w", err)
	}
	if resp.GetDownloadUrl() == "" {
		return fmt.Errorf("empty downloadUrl")
	}
	return nil
}

// checkFnListRuntimesV2 lists the v2 runtime catalog and asserts the gen2
// deploy path can resolve a runtime (nodejs20, environment GEN_2).
func checkFnListRuntimesV2(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV2Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	resp, err := client.ListRuntimes(ctx, &apiv2functionspb.ListRuntimesRequest{Parent: functionsParent(cfg)})
	if err != nil {
		return fmt.Errorf("ListRuntimes: %w", err)
	}
	if len(resp.GetRuntimes()) == 0 {
		return fmt.Errorf("ListRuntimes returned no runtimes")
	}
	for _, rt := range resp.GetRuntimes() {
		if rt.GetName() == "nodejs20" {
			if rt.GetEnvironment() != apiv2functionspb.Environment_GEN_2 {
				return fmt.Errorf("nodejs20 environment = %v, want GEN_2", rt.GetEnvironment())
			}
			if rt.GetStage() != apiv2functionspb.ListRuntimesResponse_GA {
				return fmt.Errorf("nodejs20 stage = %v, want GA", rt.GetStage())
			}
			return nil
		}
	}
	return fmt.Errorf("nodejs20 missing from ListRuntimes")
}

// checkFnCrossVersion creates a function via the v1 API and reads it via the v2
// API, proving both transports share one store.
func checkFnCrossVersion(ctx context.Context, cfg Config) error {
	v1, err := newFunctionsV1Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer v1.Close()
	id := cfg.ResourceName("gcpc-fn-cross")
	if err := ensureFunctionV1(ctx, v1, cfg, id); err != nil {
		return err
	}
	v2, err := newFunctionsV2Client(ctx, cfg)
	if err != nil {
		return err
	}
	defer v2.Close()
	fn, err := v2.GetFunction(ctx, &apiv2functionspb.GetFunctionRequest{Name: functionsV1Name(cfg, id)})
	if err != nil {
		return fmt.Errorf("v2 GetFunction of v1-created function: %w", err)
	}
	if fn.GetBuildConfig().GetRuntime() != "nodejs20" {
		return fmt.Errorf("cross-version runtime = %q", fn.GetBuildConfig().GetRuntime())
	}
	return nil
}
