package run

import (
	"context"
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
