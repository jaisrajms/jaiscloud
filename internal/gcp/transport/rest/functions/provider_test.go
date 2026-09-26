package functions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gcp/resource"
	core "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const (
	operationMetadataTypeV2 = "type.googleapis.com/google.cloud.functions.v2.OperationMetadata"
	functionTypeURLV2       = "type.googleapis.com/google.cloud.functions.v2.Function"
	emptyTypeURL            = "type.googleapis.com/google.protobuf.Empty"
)

// stubExecutor captures the InvokeRequest and optionally returns a fixed error.
type stubExecutor struct {
	req     lambdaexec.InvokeRequest
	invoked bool
	err     error
}

func (e *stubExecutor) Invoke(_ context.Context, req lambdaexec.InvokeRequest) (lambdaexec.InvokeResult, error) {
	e.req = req
	e.invoked = true
	if e.err != nil {
		return lambdaexec.InvokeResult{}, e.err
	}
	return lambdaexec.InvokeResult{Payload: req.Payload}, nil
}

func (e *stubExecutor) DeleteFunction(_ context.Context, _ string) {}
func (e *stubExecutor) Reset(_ context.Context)                    {}
func (e *stubExecutor) Close() error                               { return nil }

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

// newNRv2 builds a v2 request (apiVersion=v2) with the standard project wiring.
func newNRv2(params map[string]any) *model.NormalizedRequest {
	nr := newNR(params)
	nr.Params["apiVersion"] = "v2"
	return nr
}

func newProvider(t *testing.T, resources store.ResourceStore, exec lambdaexec.LambdaExecutor) *Provider {
	t.Helper()
	return NewProvider(core.NewService(functionsstore.NewMemoryStore(), resources, core.WithExecutor(exec)), "proj")
}

// operationResponse asserts resp is a done google.longrunning.Operation and
// returns its response object (the Function for create/update, the
// google.protobuf.Empty Any for delete).
func operationResponse(t *testing.T, resp *model.ProviderResponse) map[string]any {
	t.Helper()
	if done, _ := resp.Data["done"].(bool); !done {
		t.Fatalf("expected done operation, got %v", resp.Data)
	}
	meta, _ := resp.Data["metadata"].(map[string]any)
	if meta == nil || meta["@type"] != "type.googleapis.com/google.cloud.functions.v1.OperationMetadataV1" {
		t.Fatalf("expected functions OperationMetadataV1, got %v", resp.Data["metadata"])
	}
	if _, ok := resp.Data["name"].(string); !ok {
		t.Fatalf("expected operation name, got %v", resp.Data["name"])
	}
	m, ok := resp.Data["response"].(map[string]any)
	if !ok {
		t.Fatalf("expected response object, got %v", resp.Data["response"])
	}
	return m
}

func TestFunctionCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	// Create returns a done Operation wrapping the Function.
	nr := newNR(map[string]any{
		"location":   "us-central1",
		"functionId": "hello",
		"body": map[string]any{
			"runtime":              "nodejs20",
			"entryPoint":           "helloWorld",
			"environmentVariables": map[string]any{"K": "V"},
		},
	})
	resp, err := p.CreateFunction(ctx, nr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	fn := operationResponse(t, resp)
	if fn["@type"] != "type.googleapis.com/google.cloud.functions.v1.CloudFunction" {
		t.Errorf("v1 create response @type = %v", fn["@type"])
	}
	if fn["name"] != "projects/proj/locations/us-central1/functions/hello" {
		t.Errorf("unexpected name: %v", fn["name"])
	}
	if fn["status"] != "ACTIVE" {
		t.Errorf("expected ACTIVE, got %v", fn["status"])
	}
	ht, _ := fn["httpsTrigger"].(map[string]any)
	if ht == nil || ht["url"] == "" {
		t.Errorf("expected httpsTrigger.url on HTTP function")
	}

	// Get.
	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/hello"})
	resp, err = p.GetFunction(ctx, nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Data["entryPoint"] != "helloWorld" {
		t.Errorf("unexpected entryPoint: %v", resp.Data["entryPoint"])
	}

	// Update (PATCH merge) returns a done Operation wrapping the updated Function.
	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/hello",
		"body": map[string]any{"runtime": "nodejs22"}})
	resp, err = p.UpdateFunction(ctx, nr)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	upd := operationResponse(t, resp)
	if upd["runtime"] != "nodejs22" || upd["entryPoint"] != "helloWorld" {
		t.Errorf("unexpected update result: %v", upd)
	}

	// List.
	nr = newNR(map[string]any{"location": "us-central1"})
	resp, err = p.ListFunctions(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	fns, _ := resp.Data["functions"].([]any)
	if len(fns) != 1 {
		t.Errorf("expected 1 function, got %d", len(fns))
	}

	// Delete returns a done Operation whose response is a typed
	// google.protobuf.Empty Any (gax unpacks it as Empty).
	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/hello"})
	resp, err = p.DeleteFunction(ctx, nr)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if del := operationResponse(t, resp); del["@type"] != "type.googleapis.com/google.protobuf.Empty" || len(del) != 1 {
		t.Errorf("expected Empty-typed delete response, got %v", del)
	}
	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/hello"})
	if _, err := p.GetFunction(ctx, nr); err == nil {
		t.Errorf("expected NotFound after delete")
	}
}

// TestFunctionCRUDv2 exercises the Cloud Functions v2 wire shape end to end:
// create (v2 buildConfig body) → get/list emit state/buildConfig/serviceConfig,
// and update merges nested buildConfig fields.
func TestFunctionCRUDv2(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	create := newNRv2(map[string]any{
		"location":   "us-central1",
		"functionId": "hello",
		"body": map[string]any{
			"buildConfig": map[string]any{
				"runtime":              "nodejs20",
				"entryPoint":           "helloWorld",
				"environmentVariables": map[string]any{"K": "V"},
				"source": map[string]any{
					"storageSource": map[string]any{"bucket": "bkt", "object": "src.zip"},
				},
			},
			"serviceConfig": map[string]any{"availableMemory": "512M", "timeoutSeconds": float64(120)},
			"labels":        map[string]any{"env": "test"},
		},
	})
	resp, err := p.CreateFunction(ctx, create)
	if err != nil {
		t.Fatalf("create v2: %v", err)
	}
	if got, _ := resp.Data["metadata"].(map[string]any)["@type"].(string); got != operationMetadataTypeV2 {
		t.Fatalf("create v2 metadata @type = %q, want %q", got, operationMetadataTypeV2)
	}
	fn, _ := resp.Data["response"].(map[string]any)
	if fn["@type"] != functionTypeURLV2 {
		t.Errorf("v2 create response @type = %v, want %v", fn["@type"], functionTypeURLV2)
	}
	if fn["state"] != "ACTIVE" {
		t.Errorf("state = %v, want ACTIVE", fn["state"])
	}
	if fn["environment"] != "GEN_2" {
		t.Errorf("environment = %v, want GEN_2", fn["environment"])
	}
	if _, ok := fn["status"]; ok {
		t.Errorf("v2 response must not carry v1 status: %v", fn["status"])
	}
	bc, _ := fn["buildConfig"].(map[string]any)
	if bc["runtime"] != "nodejs20" || bc["entryPoint"] != "helloWorld" {
		t.Errorf("unexpected buildConfig: %v", bc)
	}
	if src, _ := bc["source"].(map[string]any); src["storageSource"] == nil {
		t.Errorf("expected buildConfig.source.storageSource, got %v", bc["source"])
	}
	sc, _ := fn["serviceConfig"].(map[string]any)
	if sc["uri"] == "" || sc["availableMemory"] != "512M" || sc["timeoutSeconds"] != 120 {
		t.Errorf("unexpected serviceConfig: %v", sc)
	}
	if sc["service"] != "projects/proj/locations/us-central1/services/hello" {
		t.Errorf("serviceConfig.service = %v, want the backing Cloud Run service", sc["service"])
	}

	// Get emits v2.
	resp, err = p.GetFunction(ctx, newNRv2(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/functions/hello",
	}))
	if err != nil {
		t.Fatalf("get v2: %v", err)
	}
	if resp.Data["state"] != "ACTIVE" || resp.Data["buildConfig"] == nil || resp.Data["serviceConfig"] == nil {
		t.Errorf("unexpected v2 get: %v", resp.Data)
	}
	if _, ok := resp.Data["status"]; ok {
		t.Errorf("v2 get must not carry v1 status")
	}

	// List emits v2 and supports the all-locations wildcard.
	resp, err = p.ListFunctions(ctx, newNRv2(map[string]any{"location": "-"}))
	if err != nil {
		t.Fatalf("list v2: %v", err)
	}
	fns, _ := resp.Data["functions"].([]any)
	if len(fns) != 1 {
		t.Fatalf("expected 1 v2 function, got %d", len(fns))
	}
	if m, _ := fns[0].(map[string]any); m["state"] != "ACTIVE" || m["buildConfig"] == nil {
		t.Errorf("unexpected v2 list item: %v", fns[0])
	}

	// Update merges a nested buildConfig path via the v2 updateMask.
	resp, err = p.UpdateFunction(ctx, newNRv2(map[string]any{
		"location":   "us-central1",
		"name":       "locations/us-central1/functions/hello",
		"updateMask": "buildConfig.runtime",
		"body":       map[string]any{"buildConfig": map[string]any{"runtime": "nodejs22"}},
	}))
	if err != nil {
		t.Fatalf("update v2: %v", err)
	}
	upd, _ := resp.Data["response"].(map[string]any)
	if upd["@type"] != functionTypeURLV2 {
		t.Errorf("v2 update response @type = %v, want %v", upd["@type"], functionTypeURLV2)
	}
	if bc, _ := upd["buildConfig"].(map[string]any); bc["runtime"] != "nodejs22" {
		t.Errorf("v2 update runtime = %v, want nodejs22", upd["buildConfig"])
	}

	// Delete returns a v2-done operation whose response is a typed Empty Any.
	resp, err = p.DeleteFunction(ctx, newNRv2(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/functions/hello",
	}))
	if err != nil {
		t.Fatalf("delete v2: %v", err)
	}
	if got, _ := resp.Data["metadata"].(map[string]any)["@type"].(string); got != operationMetadataTypeV2 {
		t.Errorf("delete v2 metadata @type = %q", got)
	}
	if del, _ := resp.Data["response"].(map[string]any); del["@type"] != emptyTypeURL || len(del) != 1 {
		t.Errorf("delete v2 response = %v, want a bare Empty Any", del)
	}
}

// TestOperationsV2 pins the v2 LRO surface: get returns a done v2 Operation,
// list is empty, and cancel/delete return empty objects.
func TestOperationsV2(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	resp, err := p.GetOperation(ctx, newNRv2(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/operations/op1",
	}))
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("expected done operation, got %v", resp.Data)
	}
	if resp.Data["name"] != "projects/proj/locations/us-central1/operations/op1" {
		t.Errorf("unexpected operation name: %v", resp.Data["name"])
	}
	if got, _ := resp.Data["metadata"].(map[string]any)["@type"].(string); got != operationMetadataTypeV2 {
		t.Errorf("operation metadata @type = %q, want v2", got)
	}
	if got, _ := resp.Data["response"].(map[string]any)["@type"].(string); got != emptyTypeURL {
		t.Errorf("operation response @type = %q, want %q", got, emptyTypeURL)
	}

	resp, err = p.ListOperations(ctx, newNRv2(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if ops, _ := resp.Data["operations"].([]any); len(ops) != 0 {
		t.Errorf("expected no operations, got %v", ops)
	}

	if _, err := p.CancelOperation(ctx, newNRv2(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/operations/op1",
	})); err != nil {
		t.Fatalf("cancel operation: %v", err)
	}
	if _, err := p.DeleteOperation(ctx, newNRv2(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/operations/op1",
	})); err != nil {
		t.Fatalf("delete operation: %v", err)
	}

	// A malformed operation name is rejected.
	if _, err := p.GetOperation(ctx, newNRv2(map[string]any{"location": "us-central1", "name": "bogus"})); err == nil {
		t.Errorf("expected InvalidArgument for malformed operation name")
	}
}

// delayedGetStore wraps a functionsstore.Store, delaying every GetFunction
// call to widen a TOCTOU race window in tests.
type delayedGetStore struct {
	functionsstore.Store
	delay time.Duration
}

func (d *delayedGetStore) GetFunction(ctx context.Context, projectID, location, id string) (functionsstore.Function, error) {
	f, err := d.Store.GetFunction(ctx, projectID, location, id)
	time.Sleep(d.delay)
	return f, err
}

// TestUpdateFunctionConcurrentDisjointFieldsNoLostUpdate proves that
// UpdateFunction's get-merge-write cycle is atomic with respect to other
// concurrent PATCH requests.
func TestUpdateFunctionConcurrentDisjointFieldsNoLostUpdate(t *testing.T) {
	ctx := context.Background()
	p := NewProvider(core.NewService(
		&delayedGetStore{Store: functionsstore.NewMemoryStore(), delay: 5 * time.Millisecond},
		store.NewMemoryResourceStore(),
	), "proj")

	createNR := newNR(map[string]any{
		"location":   "us-central1",
		"functionId": "f1",
		"body": map[string]any{
			"runtime":     "nodejs20",
			"entryPoint":  "h",
			"description": "d0",
			"timeout":     "60s",
		},
	})
	if _, err := p.CreateFunction(ctx, createNR); err != nil {
		t.Fatalf("create: %v", err)
	}

	const perField = 25
	var wg sync.WaitGroup
	errs := make([]error, 2*perField)
	for i := 0; i < perField; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			nr := newNR(map[string]any{
				"location": "us-central1", "name": "locations/us-central1/functions/f1",
				"body": map[string]any{"runtime": fmt.Sprintf("vR%d", i)},
			})
			_, errs[i] = p.UpdateFunction(ctx, nr)
		}(i)
		go func(i int) {
			defer wg.Done()
			nr := newNR(map[string]any{
				"location": "us-central1", "name": "locations/us-central1/functions/f1",
				"body": map[string]any{"description": fmt.Sprintf("vD%d", i)},
			})
			_, errs[perField+i] = p.UpdateFunction(ctx, nr)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	got, err := p.GetFunction(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/f1"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	runtime, _ := got.Data["runtime"].(string)
	description, _ := got.Data["description"].(string)
	if runtime == "nodejs20" || !strings.HasPrefix(runtime, "vR") {
		t.Errorf("runtime reverted to a stale value instead of one of the 25 concurrent writers': got %q", runtime)
	}
	if description == "d0" || !strings.HasPrefix(description, "vD") {
		t.Errorf("description reverted to a stale value instead of one of the 25 concurrent writers': got %q", description)
	}
	if got.Data["timeout"] != "60s" {
		t.Errorf("untouched field timeout should be unaffected, got %v", got.Data["timeout"])
	}
}

// TestListFunctions_AllLocationsWildcard verifies that location="-" aggregates
// functions across every region for the project.
func TestListFunctions_AllLocationsWildcard(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	for _, loc := range []string{"us-central1", "europe-west1"} {
		nr := newNR(map[string]any{
			"location":   loc,
			"functionId": "fn-" + loc,
			"body": map[string]any{
				"runtime":    "nodejs20",
				"entryPoint": "helloWorld",
			},
		})
		if _, err := p.CreateFunction(ctx, nr); err != nil {
			t.Fatalf("create in %s: %v", loc, err)
		}
	}

	resp, err := p.ListFunctions(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("list us-central1: %v", err)
	}
	if fns, _ := resp.Data["functions"].([]any); len(fns) != 1 {
		t.Fatalf("expected 1 function in us-central1, got %d", len(fns))
	}

	resp, err = p.ListFunctions(ctx, newNR(map[string]any{"location": "-"}))
	if err != nil {
		t.Fatalf("list -: %v", err)
	}
	fns, _ := resp.Data["functions"].([]any)
	if len(fns) != 2 {
		t.Fatalf("expected 2 functions across all locations, got %d: %+v", len(fns), fns)
	}
}

func TestCallFunctionMockEcho(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	nr := newNR(map[string]any{
		"location":   "us-central1",
		"functionId": "echo",
		"body":       map[string]any{"runtime": "nodejs20", "entryPoint": "handler"},
	})
	if _, err := p.CreateFunction(ctx, nr); err != nil {
		t.Fatalf("create: %v", err)
	}

	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/echo",
		"body": map[string]any{"data": "hello world"}})
	resp, err := p.CallFunction(ctx, nr)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.Data["result"] != "hello world" {
		t.Errorf("expected echo result, got %v", resp.Data["result"])
	}
	if id, _ := resp.Data["executionId"].(string); id == "" {
		t.Errorf("expected non-empty executionId")
	}

	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/missing",
		"body": map[string]any{"data": "x"}})
	if _, err := p.CallFunction(ctx, nr); err == nil {
		t.Errorf("expected NotFound calling missing function")
	}
}

func TestGenerateUploadUrl(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)
	nr := newNR(map[string]any{"location": "us-central1"})
	resp, err := p.GenerateUploadUrl(ctx, nr)
	if err != nil {
		t.Fatalf("generateUploadUrl: %v", err)
	}
	if u, _ := resp.Data["uploadUrl"].(string); u == "" {
		t.Errorf("expected non-empty uploadUrl")
	}
}

func TestIamPolicyLocationScoped(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	for _, loc := range []string{"us-central1", "europe-west1"} {
		nr := newNR(map[string]any{
			"location":   loc,
			"functionId": "foo",
			"body":       map[string]any{"runtime": "nodejs20", "entryPoint": "handler"},
		})
		if _, err := p.CreateFunction(ctx, nr); err != nil {
			t.Fatalf("create %s: %v", loc, err)
		}
	}

	set := newNR(map[string]any{
		"location": "us-central1",
		"name":     "locations/us-central1/functions/foo",
		"body": map[string]any{
			"bindings": []any{
				map[string]any{"role": "roles/cloudfunctions.invoker", "members": []any{"allUsers"}},
			},
		},
	})
	if _, err := p.FunctionSetIamPolicy(ctx, set); err != nil {
		t.Fatalf("set iam: %v", err)
	}

	get := newNR(map[string]any{
		"location": "europe-west1",
		"name":     "locations/europe-west1/functions/foo",
	})
	resp, err := p.FunctionGetIamPolicy(ctx, get)
	if err != nil {
		t.Fatalf("get iam other location: %v", err)
	}
	if b, _ := resp.Data["bindings"].([]any); len(b) != 0 {
		t.Errorf("expected empty bindings in other location, got %v", b)
	}

	get2 := newNR(map[string]any{
		"location": "us-central1",
		"name":     "locations/us-central1/functions/foo",
	})
	resp2, err := p.FunctionGetIamPolicy(ctx, get2)
	if err != nil {
		t.Fatalf("get iam same location: %v", err)
	}
	if b, _ := resp2.Data["bindings"].([]any); len(b) != 1 {
		t.Errorf("expected 1 binding in same location, got %v", b)
	}
}

func TestCallFunctionExecutorErrorReturns200(t *testing.T) {
	ctx := context.Background()
	exec := &stubExecutor{err: errors.New("boom")}
	p := NewProvider(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(), core.WithExecutor(exec)), "proj")

	nr := newNR(map[string]any{
		"location":   "us-central1",
		"functionId": "f",
		"body":       map[string]any{"runtime": "nodejs20", "entryPoint": "handler"},
	})
	if _, err := p.CreateFunction(ctx, nr); err != nil {
		t.Fatalf("create: %v", err)
	}

	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/f",
		"body": map[string]any{"data": "x"}})
	resp, err := p.CallFunction(ctx, nr)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.HTTPStatus != 200 {
		t.Errorf("expected HTTP 200, got %d", resp.HTTPStatus)
	}
	if got, _ := resp.Data["error"].(string); got != "boom" {
		t.Errorf("expected error=boom, got %v", resp.Data["error"])
	}
	if _, ok := resp.Data["result"]; ok {
		t.Errorf("expected no result on failure")
	}
	if id, _ := resp.Data["executionId"].(string); id == "" {
		t.Errorf("expected non-empty executionId")
	}
}

func TestCallFunctionPropagatesMemoryAndTimeout(t *testing.T) {
	ctx := context.Background()
	exec := &stubExecutor{}
	p := NewProvider(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(), core.WithExecutor(exec)), "proj")

	nr := newNR(map[string]any{
		"location":   "us-central1",
		"functionId": "f",
		"body": map[string]any{
			"runtime":           "nodejs20",
			"entryPoint":        "handler",
			"availableMemoryMb": float64(512),
			"timeout":           "120s",
		},
	})
	if _, err := p.CreateFunction(ctx, nr); err != nil {
		t.Fatalf("create: %v", err)
	}

	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/f",
		"body": map[string]any{"data": "hello"}})
	if _, err := p.CallFunction(ctx, nr); err != nil {
		t.Fatalf("call: %v", err)
	}
	if !exec.invoked {
		t.Fatalf("executor not invoked")
	}
	if exec.req.MemoryMB != 512 {
		t.Errorf("expected MemoryMB=512, got %d", exec.req.MemoryMB)
	}
	if exec.req.TimeoutSecs != 120 {
		t.Errorf("expected TimeoutSecs=120, got %d", exec.req.TimeoutSecs)
	}
}

func providerError(t *testing.T, err error) *model.ProviderError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	return perr
}

func TestUpdateFunctionUpdateMask(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	create := newNR(map[string]any{
		"location":   "us-central1",
		"functionId": "f",
		"body": map[string]any{
			"runtime":     "nodejs20",
			"entryPoint":  "h",
			"description": "d0",
		},
	})
	if _, err := p.CreateFunction(ctx, create); err != nil {
		t.Fatalf("create: %v", err)
	}

	masked := newNR(map[string]any{
		"location":   "us-central1",
		"name":       "locations/us-central1/functions/f",
		"updateMask": "function.runtime",
		"body":       map[string]any{"runtime": "nodejs22", "description": "ignored"},
	})
	resp, err := p.UpdateFunction(ctx, masked)
	if err != nil {
		t.Fatalf("masked update: %v", err)
	}
	got := operationResponse(t, resp)
	if got["runtime"] != "nodejs22" {
		t.Errorf("masked runtime = %v, want nodejs22", got["runtime"])
	}
	if got["description"] != "d0" {
		t.Errorf("unmasked description = %v, want d0 (retained)", got["description"])
	}

	full := newNR(map[string]any{
		"location": "us-central1",
		"name":     "locations/us-central1/functions/f",
		"body":     map[string]any{"description": "d1", "entryPoint": "h2"},
	})
	resp, err = p.UpdateFunction(ctx, full)
	if err != nil {
		t.Fatalf("full update: %v", err)
	}
	got = operationResponse(t, resp)
	if got["description"] != "d1" || got["entryPoint"] != "h2" {
		t.Errorf("full update = %v", got)
	}
	if got["runtime"] != "nodejs22" {
		t.Errorf("runtime should be retained on full update, got %v", got["runtime"])
	}

	bad := newNR(map[string]any{
		"location":   "us-central1",
		"name":       "locations/us-central1/functions/f",
		"updateMask": "bogus",
		"body":       map[string]any{"runtime": "nodejs18"},
	})
	_, err = p.UpdateFunction(ctx, bad)
	perr := providerError(t, err)
	if perr.Code != "Unimplemented" || perr.HTTPStatus != 501 {
		t.Errorf("unsupported mask: code=%q status=%d, want Unimplemented/501", perr.Code, perr.HTTPStatus)
	}
	cur, err := p.GetFunction(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/f"}))
	if err != nil {
		t.Fatalf("get after bad mask: %v", err)
	}
	if cur.Data["runtime"] != "nodejs22" {
		t.Errorf("bad mask must not mutate the function, runtime = %v", cur.Data["runtime"])
	}
}

func TestGenerateDownloadUrl(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	create := newNR(map[string]any{
		"location":   "us-central1",
		"functionId": "dl",
		"body":       map[string]any{"runtime": "nodejs20", "entryPoint": "h"},
	})
	if _, err := p.CreateFunction(ctx, create); err != nil {
		t.Fatalf("create: %v", err)
	}

	resp, err := p.GenerateDownloadUrl(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/functions/dl",
	}))
	if err != nil {
		t.Fatalf("generateDownloadUrl: %v", err)
	}
	u, _ := resp.Data["downloadUrl"].(string)
	if u == "" || !strings.Contains(u, "storage.googleapis.com") || !strings.Contains(u, "dl") {
		t.Errorf("unexpected downloadUrl: %q", u)
	}

	_, err = p.GenerateDownloadUrl(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/functions/missing",
	}))
	if perr := providerError(t, err); perr.Code != "NotFound" {
		t.Errorf("missing function: code=%q, want NotFound", perr.Code)
	}
}

func TestListLocationsPagination(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	resp, err := p.ListLocations(ctx, newNR(map[string]any{"pageSize": "2"}))
	if err != nil {
		t.Fatalf("list locations: %v", err)
	}
	page, _ := resp.Data["locations"].([]any)
	if len(page) != 2 {
		t.Fatalf("expected 2 locations, got %d", len(page))
	}
	next, _ := resp.Data["nextPageToken"].(string)
	if next == "" {
		t.Fatalf("expected nextPageToken")
	}

	resp, err = p.ListLocations(ctx, newNR(map[string]any{"pageSize": "2", "pageToken": next}))
	if err != nil {
		t.Fatalf("list locations page 2: %v", err)
	}
	page2, _ := resp.Data["locations"].([]any)
	if len(page2) != 2 {
		t.Fatalf("expected 2 locations on page 2, got %d", len(page2))
	}
	l0, _ := page[0].(map[string]any)
	l1, _ := page2[0].(map[string]any)
	if l0["locationId"] == l1["locationId"] {
		t.Errorf("page 2 repeated page 1 location %v", l0["locationId"])
	}
	if l0["name"] == nil || l0["displayName"] == nil {
		t.Errorf("location record missing fields: %v", l0)
	}

	loc, err := p.GetLocation(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("get location: %v", err)
	}
	if loc.Data["name"] != "projects/proj/locations/us-central1" || loc.Data["locationId"] != "us-central1" {
		t.Errorf("unexpected location: %v", loc.Data)
	}
}

func TestFunctionValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	_, err := p.CreateFunction(ctx, newNR(map[string]any{
		"location": "us-central1", "functionId": "f", "body": map[string]any{"entryPoint": "h"},
	}))
	if perr := providerError(t, err); perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Errorf("missing runtime: code=%q status=%d, want InvalidArgument/400", perr.Code, perr.HTTPStatus)
	}

	_, err = p.CreateFunction(ctx, newNR(map[string]any{
		"location": "us-central1", "body": map[string]any{"runtime": "nodejs20"},
	}))
	if perr := providerError(t, err); perr.Code != "InvalidArgument" {
		t.Errorf("missing functionId: code=%q, want InvalidArgument", perr.Code)
	}

	_, err = p.CreateFunction(ctx, newNR(map[string]any{
		"location": "us-central1", "body": map[string]any{"name": "not-a-resource-name", "runtime": "nodejs20"},
	}))
	if perr := providerError(t, err); perr.Code != "InvalidArgument" {
		t.Errorf("malformed create name: code=%q, want InvalidArgument", perr.Code)
	}

	_, err = p.UpdateFunction(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "badname", "body": map[string]any{"runtime": "nodejs20"},
	}))
	if perr := providerError(t, err); perr.Code != "InvalidArgument" {
		t.Errorf("malformed update name: code=%q, want InvalidArgument", perr.Code)
	}

	_, err = p.DeleteFunction(ctx, newNR(map[string]any{"location": "us-central1", "name": "badname"}))
	if perr := providerError(t, err); perr.Code != "InvalidArgument" {
		t.Errorf("malformed delete name: code=%q, want InvalidArgument", perr.Code)
	}
}
