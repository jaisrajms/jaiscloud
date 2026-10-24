package run

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"

	core "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func testProvider() *Provider {
	return NewProvider(core.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore()))
}

func req(action string, params map[string]any) *model.NormalizedRequest {
	params["project"] = "p"
	params["location"] = "l"
	return &model.NormalizedRequest{Service: "run", Action: action, Params: params}
}

func TestProviderServiceLifecycle(t *testing.T) {
	ctx := context.Background()
	p := testProvider()

	body := map[string]any{"template": map[string]any{"containers": []any{
		map[string]any{"image": "nginx:latest", "ports": []any{map[string]any{"containerPort": float64(80)}}},
	}}}
	resp, err := p.CreateService(ctx, req("CreateService", map[string]any{"serviceId": "svc", "body": body}))
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("create resp done = %v", resp.Data["done"])
	}
	if _, ok := resp.Data["response"].(map[string]any); !ok {
		t.Fatalf("create response missing typed Any: %v", resp.Data)
	}

	resp, err = p.ListServices(ctx, req("ListServices", map[string]any{}))
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	if svcs, _ := resp.Data["services"].([]any); len(svcs) != 1 {
		t.Errorf("services = %v", resp.Data["services"])
	}

	resp, err = p.GetService(ctx, req("GetService", map[string]any{"service": "svc"}))
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if resp.Data["name"] != "projects/p/locations/l/services/svc" {
		t.Errorf("name = %v", resp.Data["name"])
	}

	resp, err = p.ListRevisions(ctx, req("ListRevisions", map[string]any{"service": "svc"}))
	if err != nil {
		t.Fatalf("ListRevisions: %v", err)
	}
	if revs, _ := resp.Data["revisions"].([]any); len(revs) != 1 {
		t.Errorf("revisions = %v", resp.Data["revisions"])
	}

	resp, err = p.SetIamPolicy(ctx, req("SetIamPolicy", map[string]any{"service": "svc",
		"body": map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{"role": "roles/run.invoker", "members": []any{"allUsers"}}}}}}))
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if bindings, _ := resp.Data["bindings"].([]any); len(bindings) != 1 {
		t.Errorf("bindings = %v", resp.Data["bindings"])
	}
	resp, err = p.TestIamPermissions(ctx, req("TestIamPermissions", map[string]any{"service": "svc",
		"body": map[string]any{"permissions": []any{"run.services.get"}}}))
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if perms, _ := resp.Data["permissions"].([]string); len(perms) != 1 {
		t.Errorf("permissions = %v", resp.Data["permissions"])
	}

	op, err := p.DeleteService(ctx, req("DeleteService", map[string]any{"service": "svc"}))
	if err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if op.Data["done"] != true {
		t.Errorf("delete done = %v", op.Data["done"])
	}
}

// TestProviderValidateOnly proves the dry-run flags are threaded from the REST
// query parameters into the core: a validateOnly request previews the resource
// but persists nothing, and etag mismatches are Aborted/409.
func TestProviderValidateOnly(t *testing.T) {
	ctx := context.Background()
	p := testProvider()
	body := map[string]any{"template": map[string]any{"containers": []any{
		map[string]any{"image": "nginx:latest"},
	}}}

	// validateOnly create: the operation previews the service, but nothing is
	// stored (a following get is NotFound).
	resp, err := p.CreateService(ctx, req("CreateService", map[string]any{
		"serviceId": "svc", "validateOnly": "true", "body": body}))
	if err != nil {
		t.Fatalf("validateOnly CreateService: %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("validateOnly create done = %v", resp.Data["done"])
	}
	if _, err := p.GetService(ctx, req("GetService", map[string]any{"service": "svc"})); err == nil {
		t.Fatal("validateOnly create persisted the service")
	}

	// Real create; then a validateOnly template update must not change it.
	if _, err := p.CreateService(ctx, req("CreateService", map[string]any{"serviceId": "svc", "body": body})); err != nil {
		t.Fatalf("create: %v", err)
	}
	before, _ := p.GetService(ctx, req("GetService", map[string]any{"service": "svc"}))
	newBody := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "httpd:latest"}}}}
	if _, err := p.UpdateService(ctx, req("UpdateService", map[string]any{
		"service": "svc", "updateMask": "template", "validateOnly": "true", "body": newBody})); err != nil {
		t.Fatalf("validateOnly UpdateService: %v", err)
	}
	after, _ := p.GetService(ctx, req("GetService", map[string]any{"service": "svc"}))
	if after.Data["latestReadyRevision"] != before.Data["latestReadyRevision"] {
		t.Errorf("validateOnly update changed the revision: %v -> %v",
			before.Data["latestReadyRevision"], after.Data["latestReadyRevision"])
	}

	// etag mismatch on delete is Aborted/409 and leaves the service.
	if _, err := p.DeleteService(ctx, req("DeleteService", map[string]any{
		"service": "svc", "etag": "stale-token"})); err == nil {
		t.Fatal("stale-etag DeleteService accepted")
	} else {
		var perr *model.ProviderError
		if !errors.As(err, &perr) || perr.HTTPStatus != 409 {
			t.Fatalf("stale-etag delete err = %v, want 409", err)
		}
	}
	if _, err := p.GetService(ctx, req("GetService", map[string]any{"service": "svc"})); err != nil {
		t.Fatalf("stale-etag delete removed the service: %v", err)
	}

	// validateOnly delete keeps the service.
	if _, err := p.DeleteService(ctx, req("DeleteService", map[string]any{
		"service": "svc", "validateOnly": "true"})); err != nil {
		t.Fatalf("validateOnly DeleteService: %v", err)
	}
	if _, err := p.GetService(ctx, req("GetService", map[string]any{"service": "svc"})); err != nil {
		t.Fatalf("validateOnly delete removed the service: %v", err)
	}

	// allowMissing update upserts a missing service.
	if _, err := p.UpdateService(ctx, req("UpdateService", map[string]any{
		"service": "upsert", "updateMask": "template", "allowMissing": "true", "body": body})); err != nil {
		t.Fatalf("allowMissing UpdateService: %v", err)
	}
	if _, err := p.GetService(ctx, req("GetService", map[string]any{"service": "upsert"})); err != nil {
		t.Fatalf("allowMissing update did not create the service: %v", err)
	}
}

func TestProviderInvokeNoRuntime(t *testing.T) {
	ctx := context.Background()
	p := testProvider()
	u, _ := url.Parse("/?x=1")
	nr := &model.NormalizedRequest{
		Service: "run",
		Action:  "Invoke",
		Params:  map[string]any{},
		Raw:     &http.Request{Host: "svc-abcdef012345.us-central1.run.app", Method: http.MethodGet, URL: u},
	}
	_, err := p.Invoke(ctx, nr)
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.HTTPStatus != 503 {
		t.Fatalf("Invoke err = %v, want 503 ProviderError", err)
	}
}

func TestProviderInvokeLegacyPathForm(t *testing.T) {
	ctx := context.Background()
	p := testProvider()
	if _, err := p.CreateService(ctx, req("CreateService", map[string]any{"serviceId": "svc", "body": map[string]any{}})); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	u, _ := url.Parse("/run/v2/projects/p/locations/l/services/svc/hello")
	nr := &model.NormalizedRequest{
		Service: "run",
		Action:  "Invoke",
		Params:  map[string]any{"project": "p", "location": "l", "service": "svc", "invokePath": "/hello"},
		Raw:     &http.Request{Host: "localhost:8080", Method: http.MethodGet, URL: u},
	}
	// The mock runtime runs no container, so the resolved path still reports 503.
	_, err := p.Invoke(ctx, nr)
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.HTTPStatus != 503 {
		t.Fatalf("Invoke err = %v, want 503 ProviderError", err)
	}
}

func TestInvocationCodecRoundTrip(t *testing.T) {
	u, _ := url.Parse("/hello?x=1")
	r := &http.Request{Method: http.MethodGet, Host: "svc-abcdef012345.us-central1.run.app", URL: u}
	nr, err := NewInvocationCodec().Decode(r, []byte("payload"))
	if err != nil || nr.Action != "Invoke" {
		t.Fatalf("Decode = %+v, %v", nr, err)
	}
	status, _, outBody := NewInvocationCodec().Encode(nr, &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"headers": map[string]string{"X-Test": "1"}, "body": []byte("ok")},
	})
	if status != 200 || string(outBody) != "ok" {
		t.Errorf("Encode = %d %q", status, outBody)
	}
}

// TestListNegativePageSizeRejected pins the shared paging contract on Cloud
// Run's REST list surfaces: a negative pageSize is INVALID_ARGUMENT.
func TestListNegativePageSizeRejected(t *testing.T) {
	ctx := context.Background()
	p := testProvider()
	body := map[string]any{"template": map[string]any{"containers": []any{
		map[string]any{"image": "nginx:latest", "ports": []any{map[string]any{"containerPort": float64(80)}}},
	}}}
	if _, err := p.CreateService(ctx, req("CreateService", map[string]any{"serviceId": "svc", "body": body})); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	cases := []struct {
		name string
		call func() (*model.ProviderResponse, error)
	}{
		{"ListServices", func() (*model.ProviderResponse, error) {
			return p.ListServices(ctx, req("ListServices", map[string]any{"pageSize": "-1"}))
		}},
		{"ListRevisions", func() (*model.ProviderResponse, error) {
			return p.ListRevisions(ctx, req("ListRevisions", map[string]any{"pageSize": "-1", "service": "svc"}))
		}},
	}
	for _, tc := range cases {
		_, err := tc.call()
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "InvalidArgument" {
			t.Errorf("%s(pageSize=-1) = %v, want InvalidArgument", tc.name, err)
		}
	}

	// run.projects.locations.operations.list documents "If not set or set to
	// less than or equal to 0, the default page size is 100" — a documented
	// exception to the reject-negative rule, so a negative size takes the
	// default rather than raising InvalidArgument.
	if _, err := p.ListOperations(ctx, req("ListOperations", map[string]any{"pageSize": "-1"})); err != nil {
		t.Errorf("ListOperations(pageSize=-1) = %v, want nil (documented default)", err)
	}
	// A non-integer pageSize still fails the int32 query binding on every list
	// method — including operations.list, whose non-positive exception must not
	// swallow an unparseable value.
	for name, call := range map[string]func() error{
		"ListServices": func() error {
			_, err := p.ListServices(ctx, req("ListServices", map[string]any{"pageSize": "abc"}))
			return err
		},
		"ListOperations": func() error {
			_, err := p.ListOperations(ctx, req("ListOperations", map[string]any{"pageSize": "-0.5"}))
			return err
		},
	} {
		err := call()
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
			t.Errorf("%s(non-integer pageSize) = %v, want InvalidArgument/400", name, err)
		}
	}
}
