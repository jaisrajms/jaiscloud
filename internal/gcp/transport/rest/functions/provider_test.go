package functions

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/blobfs"
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
	return NewProvider(core.NewService(functionsstore.NewMemoryStore(), resources,
		core.WithExecutor(exec), core.WithBlobs(blobfs.NewMemoryBlobStore())), "proj")
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

// operationResponseV2 is operationResponse for a v2 operation (its metadata
// @type is google.cloud.functions.v2.OperationMetadata, which operationResponse
// asserts against v1).
func operationResponseV2(t *testing.T, resp *model.ProviderResponse) map[string]any {
	t.Helper()
	if done, _ := resp.Data["done"].(bool); !done {
		t.Fatalf("expected done operation, got %v", resp.Data)
	}
	meta, _ := resp.Data["metadata"].(map[string]any)
	if meta == nil || meta["@type"] != operationMetadataTypeV2 {
		t.Fatalf("expected v2 OperationMetadata, got %v", resp.Data["metadata"])
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

// TestFunctionInstanceConfigV2 exercises the FD6 v2 ServiceConfig
// instance/concurrency configuration over REST: create persists it, get renders
// it, an updateMask path merges it, and an out-of-range value is an
// InvalidArgument.
func TestFunctionInstanceConfigV2(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	resp, err := p.CreateFunction(ctx, newNRv2(map[string]any{
		"location":   "us-central1",
		"functionId": "cfg",
		"body": map[string]any{
			"buildConfig": map[string]any{"runtime": "nodejs22", "entryPoint": "h"},
			"serviceConfig": map[string]any{
				"minInstanceCount":              float64(2),
				"maxInstanceCount":              float64(10),
				"maxInstanceRequestConcurrency": float64(80),
				"availableCpu":                  "1",
			},
		},
	}))
	if err != nil {
		t.Fatalf("create v2: %v", err)
	}
	created := operationResponseV2(t, resp)
	sc, _ := created["serviceConfig"].(map[string]any)
	if sc["minInstanceCount"] != 2 || sc["maxInstanceCount"] != 10 ||
		sc["maxInstanceRequestConcurrency"] != 80 || sc["availableCpu"] != "1" {
		t.Fatalf("create serviceConfig = %+v", sc)
	}

	resp, err = p.GetFunction(ctx, newNRv2(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/functions/cfg",
	}))
	if err != nil {
		t.Fatalf("get v2: %v", err)
	}
	sc, _ = resp.Data["serviceConfig"].(map[string]any)
	if sc["minInstanceCount"] != 2 || sc["availableCpu"] != "1" {
		t.Fatalf("get serviceConfig = %+v", sc)
	}

	// An explicit updateMask path applies the new value.
	resp, err = p.UpdateFunction(ctx, newNRv2(map[string]any{
		"location":   "us-central1",
		"name":       "locations/us-central1/functions/cfg",
		"updateMask": "serviceConfig.maxInstanceCount",
		"body":       map[string]any{"serviceConfig": map[string]any{"maxInstanceCount": float64(20)}},
	}))
	if err != nil {
		t.Fatalf("update v2: %v", err)
	}
	sc, _ = operationResponseV2(t, resp)["serviceConfig"].(map[string]any)
	if sc["maxInstanceCount"] != 20 {
		t.Fatalf("updated serviceConfig = %+v", sc)
	}

	// Out-of-range values are rejected before they are persisted.
	_, err = p.CreateFunction(ctx, newNRv2(map[string]any{
		"location":   "us-central1",
		"functionId": "bad",
		"body": map[string]any{
			"buildConfig":   map[string]any{"runtime": "nodejs22"},
			"serviceConfig": map[string]any{"maxInstanceRequestConcurrency": float64(1001)},
		},
	}))
	if perr := providerError(t, err); perr.Code != "InvalidArgument" {
		t.Fatalf("code = %q, want InvalidArgument", perr.Code)
	}

	// v1 availableMemoryMb accepts only the documented sizes.
	_, err = p.CreateFunction(ctx, newNR(map[string]any{
		"location":   "us-central1",
		"functionId": "badmem",
		"body":       map[string]any{"runtime": "nodejs20", "availableMemoryMb": float64(300)},
	}))
	if perr := providerError(t, err); perr.Code != "InvalidArgument" {
		t.Fatalf("v1 memory code = %q, want InvalidArgument", perr.Code)
	}
}

// TestOperationsV2 pins the v2 LRO surface: mutations persist an operation that
// get/list/wait read back with the typed response, unknown ids are NotFound, and
// cancel/delete operate on the store.
func TestOperationsV2(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	create, err := p.CreateFunction(ctx, newNRv2(map[string]any{
		"location":   "us-central1",
		"functionId": "ops-fn",
		"body":       map[string]any{"buildConfig": map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	name, _ := create.Data["name"].(string)
	if name == "" {
		t.Fatalf("create did not return an operation name: %v", create.Data)
	}

	// Get returns the persisted done operation with a typed v2 Function response.
	resp, err := p.GetOperation(ctx, newNRv2(map[string]any{"location": "us-central1", "name": name}))
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("expected done operation, got %v", resp.Data)
	}
	if resp.Data["name"] != name {
		t.Errorf("operation name = %v, want %v", resp.Data["name"], name)
	}
	if got, _ := resp.Data["metadata"].(map[string]any)["@type"].(string); got != operationMetadataTypeV2 {
		t.Errorf("operation metadata @type = %q, want v2", got)
	}
	if got, _ := resp.Data["response"].(map[string]any)["@type"].(string); got != functionTypeURLV2 {
		t.Errorf("operation response @type = %q, want %q", got, functionTypeURLV2)
	}

	// :wait returns the same persisted done operation.
	resp, err = p.WaitOperation(ctx, newNRv2(map[string]any{"location": "us-central1", "name": name}))
	if err != nil {
		t.Fatalf("wait operation: %v", err)
	}
	if resp.Data["name"] != name || resp.Data["done"] != true {
		t.Errorf("wait operation = %v", resp.Data)
	}

	// list returns the persisted operation.
	resp, err = p.ListOperations(ctx, newNRv2(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	ops, _ := resp.Data["operations"].([]any)
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %v", ops)
	}
	if got, _ := ops[0].(map[string]any)["name"].(string); got != name {
		t.Errorf("listed operation name = %q, want %q", got, name)
	}

	// Unknown operations are NotFound (never synthesized).
	if _, err := p.GetOperation(ctx, newNRv2(map[string]any{"location": "us-central1", "name": "locations/us-central1/operations/missing"})); err == nil {
		t.Errorf("expected NotFound for unknown operation")
	}

	// A delete mutation persists an operation whose response is an Empty Any.
	if _, err := p.CreateFunction(ctx, newNRv2(map[string]any{
		"location":   "us-central1",
		"functionId": "ops-del",
		"body":       map[string]any{"buildConfig": map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}},
	})); err != nil {
		t.Fatalf("create del: %v", err)
	}
	delOp, err := p.DeleteFunction(ctx, newNRv2(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/functions/ops-del",
	}))
	if err != nil {
		t.Fatalf("delete function: %v", err)
	}
	delName, _ := delOp.Data["name"].(string)
	resp, err = p.GetOperation(ctx, newNRv2(map[string]any{"location": "us-central1", "name": delName}))
	if err != nil {
		t.Fatalf("get delete operation: %v", err)
	}
	if got, _ := resp.Data["response"].(map[string]any)["@type"].(string); got != emptyTypeURL {
		t.Errorf("delete operation response @type = %q, want %q", got, emptyTypeURL)
	}

	// cancel and delete operate on the store; a second delete is NotFound.
	if _, err := p.CancelOperation(ctx, newNRv2(map[string]any{"location": "us-central1", "name": name})); err != nil {
		t.Fatalf("cancel operation: %v", err)
	}
	if _, err := p.DeleteOperation(ctx, newNRv2(map[string]any{"location": "us-central1", "name": name})); err != nil {
		t.Fatalf("delete operation: %v", err)
	}
	if _, err := p.DeleteOperation(ctx, newNRv2(map[string]any{"location": "us-central1", "name": name})); err == nil {
		t.Errorf("expected NotFound deleting a deleted operation")
	}

	// A malformed operation name is rejected.
	if _, err := p.GetOperation(ctx, newNRv2(map[string]any{"location": "us-central1", "name": "bogus"})); err == nil {
		t.Errorf("expected InvalidArgument for malformed operation name")
	}
}

// TestListOperationsFilter covers the REST ListOperations filter and
// returnPartialSuccess parameters end to end: the handler threads them to the
// core, which evaluates the AIP-160 subset and rejects a filter it cannot parse.
func TestListOperationsFilter(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	var firstName string
	for _, id := range []string{"f-one", "f-two"} {
		op, err := p.CreateFunction(ctx, newNRv2(map[string]any{
			"location":   "us-central1",
			"functionId": id,
			"body":       map[string]any{"buildConfig": map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}},
		}))
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		if firstName == "" {
			firstName, _ = op.Data["name"].(string)
		}
	}
	id := firstName[strings.LastIndex(firstName, "/")+1:]

	cases := []struct {
		name   string
		params map[string]any
		want   int
	}{
		{"all", map[string]any{"location": "us-central1"}, 2},
		{"done true", map[string]any{"location": "us-central1", "filter": "done=true"}, 2},
		{"done false", map[string]any{"location": "us-central1", "filter": "done=false"}, 0},
		{"name exact", map[string]any{"location": "us-central1", "filter": `name="` + firstName + `"`}, 1},
		{"name contains", map[string]any{"location": "us-central1", "filter": `name:"` + id + `"`}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := p.ListOperations(ctx, newNRv2(tc.params))
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			ops, _ := resp.Data["operations"].([]any)
			if len(ops) != tc.want {
				t.Fatalf("list = %d ops, want %d: %v", len(ops), tc.want, ops)
			}
		})
	}

	// A filter the emulator cannot evaluate is InvalidArgument, never a silent
	// unfiltered (or empty) page.
	if _, err := p.ListOperations(ctx, newNRv2(map[string]any{"location": "us-central1", "filter": "bogus=1"})); err == nil {
		t.Errorf("expected InvalidArgument for an unsupported filter field")
	}

	// returnPartialSuccess is not supported by Cloud Functions operations.list.
	if _, err := p.ListOperations(ctx, newNRv2(map[string]any{"location": "us-central1", "returnPartialSuccess": "true"})); err == nil {
		t.Errorf("expected Unimplemented for returnPartialSuccess")
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

func TestListRuntimes(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	resp, err := p.ListRuntimes(ctx, newNRv2(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("list runtimes: %v", err)
	}
	rts, _ := resp.Data["runtimes"].([]any)
	if len(rts) == 0 {
		t.Fatal("expected a non-empty runtime catalog")
	}
	// The real v2 method declares no pagination, so the response must not
	// invent nextPageToken.
	if _, ok := resp.Data["nextPageToken"]; ok {
		t.Errorf("ListRuntimes must not emit nextPageToken: %v", resp.Data["nextPageToken"])
	}
	var node map[string]any
	for _, v := range rts {
		m, _ := v.(map[string]any)
		if m["name"] == "nodejs20" {
			node = m
		}
	}
	if node == nil {
		t.Fatalf("nodejs20 missing from %v", rts)
	}
	if node["environment"] != "GEN_2" || node["stage"] != "GA" || node["displayName"] != "Node.js 20" {
		t.Errorf("nodejs20 = %v", node)
	}

	// The filter narrows the catalog.
	resp, err = p.ListRuntimes(ctx, newNRv2(map[string]any{
		"location": "us-central1", "filter": `name="python312"`,
	}))
	if err != nil {
		t.Fatalf("filtered list runtimes: %v", err)
	}
	rts, _ = resp.Data["runtimes"].([]any)
	if len(rts) != 1 {
		t.Fatalf("expected 1 filtered runtime, got %d", len(rts))
	}
	if m, _ := rts[0].(map[string]any); m["name"] != "python312" {
		t.Errorf("filtered runtime = %v", rts[0])
	}

	// The AIP-160 subset (substring + OR) is honored over REST too.
	resp, err = p.ListRuntimes(ctx, newNRv2(map[string]any{
		"location": "us-central1", "filter": `name:"nodejs" OR name="go122"`,
	}))
	if err != nil {
		t.Fatalf("OR list runtimes: %v", err)
	}
	rts, _ = resp.Data["runtimes"].([]any)
	if len(rts) != 5 {
		t.Fatalf("expected 5 runtimes (4 nodejs + go122), got %d", len(rts))
	}

	// A missing location is InvalidArgument.
	if _, err := p.ListRuntimes(ctx, newNRv2(map[string]any{})); err == nil {
		t.Errorf("expected InvalidArgument for missing location")
	}
}

func TestGenerateUploadUrl(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	// v1: uploadUrl only (the v1 response has no storageSource field).
	nr := newNR(map[string]any{"location": "us-central1"})
	resp, err := p.GenerateUploadUrl(ctx, nr)
	if err != nil {
		t.Fatalf("generateUploadUrl v1: %v", err)
	}
	if u, _ := resp.Data["uploadUrl"].(string); u == "" {
		t.Errorf("expected non-empty v1 uploadUrl")
	}
	if _, ok := resp.Data["storageSource"]; ok {
		t.Errorf("v1 response must not carry storageSource: %v", resp.Data)
	}

	// v2: uploadUrl + storageSource(bucket/object), URL pointed at the request host.
	nr2 := newNRv2(map[string]any{"location": "us-central1"})
	resp, err = p.GenerateUploadUrl(ctx, nr2)
	if err != nil {
		t.Fatalf("generateUploadUrl v2: %v", err)
	}
	u, _ := resp.Data["uploadUrl"].(string)
	ss, _ := resp.Data["storageSource"].(map[string]any)
	if u == "" || ss == nil {
		t.Fatalf("v2 response = %v", resp.Data)
	}
	if got, want := ss["bucket"], core.SourceBucket("proj", "us-central1"); got != want {
		t.Errorf("storageSource.bucket = %v, want %v", got, want)
	}
	if obj, _ := ss["object"].(string); obj == "" || !strings.HasSuffix(obj, ".zip") {
		t.Errorf("storageSource.object = %v", ss["object"])
	}
	if !strings.Contains(u, "/"+ss["bucket"].(string)+"/") {
		t.Errorf("uploadUrl %q must contain the storage bucket %v", u, ss["bucket"])
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

// blockingExecutor blocks until the invocation context is cancelled, modelling
// a function that runs past its configured timeout.
type blockingExecutor struct{}

func (e *blockingExecutor) Invoke(ctx context.Context, _ lambdaexec.InvokeRequest) (lambdaexec.InvokeResult, error) {
	<-ctx.Done()
	return lambdaexec.InvokeResult{}, ctx.Err()
}
func (e *blockingExecutor) DeleteFunction(_ context.Context, _ string) {}
func (e *blockingExecutor) Reset(_ context.Context)                    {}
func (e *blockingExecutor) Close() error                               { return nil }

// TestInvokeTrigger covers the HTTPS-trigger handler: an HTTP-triggered function
// echoes the request body with HTTP 200, while a missing or event-only function
// is NotFound (event functions have no HTTPS endpoint).
func TestInvokeTrigger(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	if _, err := p.CreateFunction(ctx, newNR(map[string]any{
		"location": "us-central1", "functionId": "httpfn",
		"body": map[string]any{"runtime": "nodejs20", "entryPoint": "handler"},
	})); err != nil {
		t.Fatalf("create http fn: %v", err)
	}
	resp, err := p.InvokeTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "functionId": "httpfn", "payload": "hello trigger",
	}))
	if err != nil {
		t.Fatalf("invoke trigger: %v", err)
	}
	if resp.HTTPStatus != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.HTTPStatus)
	}
	if body, _ := resp.Data["body"].([]byte); string(body) != "hello trigger" {
		t.Errorf("body = %q, want hello trigger", body)
	}

	// Unknown function → NotFound.
	if _, err := p.InvokeTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "functionId": "missing",
	})); err == nil {
		t.Errorf("expected NotFound invoking a missing function")
	}

	// Event-only function has no HTTPS endpoint → NotFound.
	if _, err := p.CreateFunction(ctx, newNR(map[string]any{
		"location": "us-central1", "functionId": "evtfn",
		"body": map[string]any{"runtime": "nodejs20", "entryPoint": "handler",
			"eventTrigger": map[string]any{"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t"}},
	})); err != nil {
		t.Fatalf("create event fn: %v", err)
	}
	if _, err := p.InvokeTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "functionId": "evtfn",
	})); err == nil {
		t.Errorf("expected NotFound invoking an event-only function")
	}
}

// TestInvokeTriggerTimeout verifies the configured function timeout is enforced
// by the trigger path: a function that outruns it returns HTTP 500.
func TestInvokeTriggerTimeout(t *testing.T) {
	ctx := context.Background()
	p := NewProvider(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		core.WithExecutor(&blockingExecutor{})), "proj")

	if _, err := p.CreateFunction(ctx, newNR(map[string]any{
		"location": "us-central1", "functionId": "slow",
		"body": map[string]any{"runtime": "nodejs20", "entryPoint": "handler", "timeout": "1s"},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	resp, err := p.InvokeTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "functionId": "slow", "payload": "x",
	}))
	if err != nil {
		t.Fatalf("invoke trigger: %v", err)
	}
	if resp.HTTPStatus != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 on timeout", resp.HTTPStatus)
	}
	if body, _ := resp.Data["body"].([]byte); len(body) == 0 {
		t.Errorf("expected a timeout error body")
	}
}

// TestInvokeTriggerUnlistedRegion proves a function created in a region outside
// the advertised catalog still serves its synthesized URL: when the host label
// cannot be resolved against the region catalog, the provider resolves it
// against the stored functions.
func TestInvokeTriggerUnlistedRegion(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	if _, err := p.CreateFunction(ctx, newNR(map[string]any{
		"location": "me-west1", "functionId": "regfn",
		"body": map[string]any{"runtime": "nodejs20", "entryPoint": "handler"},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	resp, err := p.InvokeTrigger(ctx, newNR(map[string]any{
		"functionId": "regfn", "triggerLabel": "me-west1-proj", "payload": "hi",
	}))
	if err != nil {
		t.Fatalf("invoke trigger: %v", err)
	}
	if body, _ := resp.Data["body"].([]byte); string(body) != "hi" {
		t.Errorf("body = %q, want hi", body)
	}

	// A label that does not end in the resolved project is NotFound.
	if _, err := p.InvokeTrigger(ctx, newNR(map[string]any{
		"functionId": "regfn", "triggerLabel": "me-west1-other", "payload": "hi",
	})); err == nil {
		t.Errorf("expected NotFound for a label not matching the resolved project")
	}
}

// TestUpgradeTrafficControlPlaneREST covers the seven v2 gen1→gen2 upgrade
// verbs over REST: setup captures overrides, redirect/rollback move traffic,
// commit/abort/detach are terminal, and invalid transitions fail loud.
func TestUpgradeTrafficControlPlaneREST(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)

	if _, err := p.CreateFunction(ctx, newNRv2(map[string]any{
		"location": "us-central1", "functionId": "up",
		"body": map[string]any{"buildConfig": map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	name := map[string]any{"location": "us-central1", "name": "locations/us-central1/functions/up"}

	// setupFunctionUpgradeConfig captures the Gen2 overrides.
	setup := newNRv2(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/functions/up",
		"body": map[string]any{
			"buildConfigOverrides":   map[string]any{"runtime": "nodejs22"},
			"serviceConfigOverrides": map[string]any{"maxInstanceCount": float64(4)},
		},
	})
	resp, err := p.SetupFunctionUpgradeConfig(ctx, setup)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	fn, _ := resp.Data["response"].(map[string]any)
	ui, _ := fn["upgradeInfo"].(map[string]any)
	if ui == nil || ui["upgradeState"] != core.UpgradeStateSetupSuccessful {
		t.Fatalf("setup upgradeInfo = %v", fn["upgradeInfo"])
	}
	if bc, _ := ui["buildConfig"].(map[string]any); bc["runtime"] != "nodejs22" {
		t.Errorf("setup buildConfig.runtime = %v", bc["runtime"])
	}

	// redirectFunctionUpgradeTraffic moves traffic to the Gen2 copy.
	resp, err = p.RedirectFunctionUpgradeTraffic(ctx, newNRv2(name))
	if err != nil {
		t.Fatalf("redirect: %v", err)
	}
	fn, _ = resp.Data["response"].(map[string]any)
	if sc, _ := fn["serviceConfig"].(map[string]any); sc["allTrafficOnLatestRevision"] != false {
		t.Errorf("after redirect allTrafficOnLatestRevision = %v, want false", sc["allTrafficOnLatestRevision"])
	}

	// rollbackFunctionUpgradeTraffic returns traffic to Gen1.
	if _, err := p.RollbackFunctionUpgradeTraffic(ctx, newNRv2(name)); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	resp, err = p.GetFunction(ctx, newNRv2(name))
	if err != nil {
		t.Fatalf("get after rollback: %v", err)
	}
	if sc, _ := resp.Data["serviceConfig"].(map[string]any); sc["allTrafficOnLatestRevision"] != true {
		t.Errorf("after rollback allTrafficOnLatestRevision = %v, want true", sc["allTrafficOnLatestRevision"])
	}

	// commitFunctionUpgradeAsGen2 is terminal.
	if _, err := p.RedirectFunctionUpgradeTraffic(ctx, newNRv2(name)); err != nil {
		t.Fatalf("re-redirect: %v", err)
	}
	resp, err = p.CommitFunctionUpgradeAsGen2(ctx, newNRv2(name))
	if err != nil {
		t.Fatalf("commitAsGen2: %v", err)
	}
	fn, _ = resp.Data["response"].(map[string]any)
	if ui, _ := fn["upgradeInfo"].(map[string]any); ui["upgradeState"] != core.UpgradeStateCommitAsGen2Successful {
		t.Errorf("commitAsGen2 upgradeInfo = %v", fn["upgradeInfo"])
	}

	// detachFunction clears the upgrade state.
	if _, err := p.DetachFunction(ctx, newNRv2(name)); err != nil {
		t.Fatalf("detach: %v", err)
	}
	resp, _ = p.GetFunction(ctx, newNRv2(name))
	if _, ok := resp.Data["upgradeInfo"]; ok {
		t.Errorf("detach should clear upgradeInfo: %v", resp.Data["upgradeInfo"])
	}

	// A missing function is NotFound.
	if _, err := p.SetupFunctionUpgradeConfig(ctx, newNRv2(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/functions/missing",
	})); err == nil {
		t.Errorf("expected NotFound for a missing function")
	}
}

// TestUpgradeInvalidTransitionREST pins the precondition error for redirect
// without a prior setup.
func TestUpgradeInvalidTransitionREST(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t, store.NewMemoryResourceStore(), nil)
	if _, err := p.CreateFunction(ctx, newNRv2(map[string]any{
		"location": "us-central1", "functionId": "f",
		"body": map[string]any{"buildConfig": map[string]any{"runtime": "nodejs20"}},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err := p.RedirectFunctionUpgradeTraffic(ctx, newNRv2(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/functions/f",
	}))
	if perr := providerError(t, err); perr.Code != "FailedPrecondition" || perr.HTTPStatus != 400 {
		t.Errorf("redirect before setup: code=%q status=%d, want FailedPrecondition/400", perr.Code, perr.HTTPStatus)
	}
}

// TestInvokeTriggerExecutorErrorIs500 pins that a non-timeout executor failure
// also returns HTTP 500 with the error text.
func TestInvokeTriggerExecutorErrorIs500(t *testing.T) {
	ctx := context.Background()
	p := NewProvider(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		core.WithExecutor(&stubExecutor{err: errors.New("boom")})), "proj")

	if _, err := p.CreateFunction(ctx, newNR(map[string]any{
		"location": "us-central1", "functionId": "f",
		"body": map[string]any{"runtime": "nodejs20", "entryPoint": "handler"},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}
	resp, err := p.InvokeTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "functionId": "f", "payload": "x",
	}))
	if err != nil {
		t.Fatalf("invoke trigger: %v", err)
	}
	if resp.HTTPStatus != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.HTTPStatus)
	}
	if body, _ := resp.Data["body"].([]byte); string(body) != "boom" {
		t.Errorf("body = %q, want boom", body)
	}
}
