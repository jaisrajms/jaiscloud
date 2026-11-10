package serviceusage

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/resource"
	core "jaiscloud/internal/gcp/service/serviceusage"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params}
}

func newProvider() *Provider {
	return NewProvider(core.NewService(store.NewMemoryResourceStore()), "default-proj")
}

func opResponse(t *testing.T, resp *model.ProviderResponse) map[string]any {
	t.Helper()
	if done, _ := resp.Data["done"].(bool); !done {
		t.Fatalf("operation is not done: %v", resp.Data)
	}
	r, ok := resp.Data["response"].(map[string]any)
	if !ok {
		t.Fatalf("operation has no response object: %v", resp.Data)
	}
	return r
}

func TestCodecDecode(t *testing.T) {
	c := NewCodec()
	cases := []struct {
		method, path string
		wantAction   string
	}{
		{http.MethodGet, "/v1/projects/proj/services", "ServicesList"},
		{http.MethodGet, "/v1/projects/proj/services:batchGet", "ServicesBatchGet"},
		{http.MethodPost, "/v1/projects/proj/services:batchEnable", "ServicesBatchEnable"},
		{http.MethodGet, "/v1/projects/proj/services/run.googleapis.com", "ServicesGet"},
		{http.MethodPost, "/v1/projects/proj/services/run.googleapis.com:enable", "ServicesEnable"},
		{http.MethodPost, "/v1/projects/proj/services/run.googleapis.com:disable", "ServicesDisable"},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := c.Decode(r, nil)
		if err != nil {
			t.Fatalf("%s %s: decode: %v", tc.method, tc.path, err)
		}
		if nr.Service != "serviceusage" {
			t.Fatalf("%s: service = %q", tc.path, nr.Service)
		}
		if nr.Action != tc.wantAction {
			t.Fatalf("%s: action = %q, want %q", tc.path, nr.Action, tc.wantAction)
		}
		if nr.Params["project"] != "proj" {
			t.Fatalf("%s: project = %v", tc.path, nr.Params["project"])
		}
	}
}

func TestCodecDecodeUnsupported(t *testing.T) {
	c := NewCodec()
	for _, tc := range []struct{ method, path string }{
		{http.MethodDelete, "/v1/projects/proj/services/run.googleapis.com"},
		{http.MethodGet, "/v1/projects/proj/services/run.googleapis.com:enable"},
		{http.MethodPost, "/v1/projects/proj/services/run.googleapis.com"},
		{http.MethodGet, "/v1/projects/proj"},
		{http.MethodGet, "/v1/projects/proj/services/run.googleapis.com:unsupported"},
		// generateServiceIdentity is v1beta1-only; a v1 path is a 404 on real
		// GCP and must stay unsupported here.
		{http.MethodPost, "/v1/projects/proj/services/run.googleapis.com:generateServiceIdentity"},
		{http.MethodPost, "/v1beta1/projects/proj/services/run.googleapis.com:disable"},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		if _, err := c.Decode(r, nil); err == nil {
			t.Fatalf("%s %s: expected error", tc.method, tc.path)
		}
	}
}

// TestCodecDecodeV1Beta1 covers the v1beta1 paths: the services
// generateServiceIdentity custom method and the top-level operations poll.
func TestCodecDecodeV1Beta1(t *testing.T) {
	c := NewCodec()

	nr, err := c.Decode(httptest.NewRequest(http.MethodPost,
		"/v1beta1/projects/proj/services/secretmanager.googleapis.com:generateServiceIdentity", nil), nil)
	if err != nil {
		t.Fatalf("decode generateServiceIdentity: %v", err)
	}
	if nr.Action != "ServicesGenerateServiceIdentity" {
		t.Fatalf("action = %q", nr.Action)
	}
	if nr.Params["project"] != "proj" || nr.Params["service"] != "secretmanager.googleapis.com" {
		t.Fatalf("params = %v", nr.Params)
	}

	nr, err = c.Decode(httptest.NewRequest(http.MethodGet, "/v1beta1/operations/op-1", nil), nil)
	if err != nil {
		t.Fatalf("decode operations.get: %v", err)
	}
	if nr.Action != "OperationsGet" || nr.Params["name"] != "operations/op-1" {
		t.Fatalf("operations.get = action %q name %v", nr.Action, nr.Params["name"])
	}
}

// TestGenerateServiceIdentityREST drives the raw v1beta1 request through the
// codec, provider and encoder, the way the google-beta provider does: the
// response is a done google.longrunning.Operation whose ServiceIdentity carries
// the service agent email and uniqueId. A second request returns the same
// identity, and the unsupported-service path is a 400 FAILED_PRECONDITION.
func TestGenerateServiceIdentityREST(t *testing.T) {
	resources := store.NewMemoryResourceStore()
	p := NewProvider(core.NewService(resources), "default-proj")

	status, body := serveBody(t, p, http.MethodPost,
		"/v1beta1/projects/proj/services/secretmanager.googleapis.com:generateServiceIdentity", "")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", status, body)
	}
	var op map[string]any
	if err := json.Unmarshal(body, &op); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if done, _ := op["done"].(bool); !done {
		t.Fatalf("operation not done: %s", body)
	}
	// The v1beta1 proto declares google.protobuf.Empty metadata for this method.
	meta, _ := op["metadata"].(map[string]any)
	if meta["@type"] != emptyMetadataType {
		t.Errorf("metadata @type = %v, want %q", meta["@type"], emptyMetadataType)
	}
	resp, _ := op["response"].(map[string]any)
	if resp["@type"] != identityRespType {
		t.Errorf("response @type = %v, want %q", resp["@type"], identityRespType)
	}
	email, _ := resp["email"].(string)
	if want := "service-" + resource.ProjectNumber("proj") + "@gcp-sa-secretmanager.iam.gserviceaccount.com"; email != want {
		t.Errorf("email = %q, want %q", email, want)
	}
	if unique, _ := resp["uniqueId"].(string); !regexp.MustCompile(`^1[0-9]{20}$`).MatchString(unique) {
		t.Errorf("uniqueId = %q", unique)
	}

	// Idempotence over the wire: a second call returns the same identity.
	_, body2 := serveBody(t, p, http.MethodPost,
		"/v1beta1/projects/proj/services/secretmanager.googleapis.com:generateServiceIdentity", "")
	var op2 map[string]any
	if err := json.Unmarshal(body2, &op2); err != nil {
		t.Fatalf("unmarshal 2: %v", err)
	}
	resp2, _ := op2["response"].(map[string]any)
	if resp2["email"] != resp["email"] || resp2["uniqueId"] != resp["uniqueId"] {
		t.Errorf("identity changed: %v -> %v", resp, resp2)
	}

	// The operation is persisted and readable at the v1beta1 poll path.
	name, _ := op["name"].(string)
	status, body = serveBody(t, p, http.MethodGet, "/v1beta1/"+name, "")
	if status != http.StatusOK {
		t.Fatalf("operations.get status = %d, body = %s", status, body)
	}
	var polled map[string]any
	if err := json.Unmarshal(body, &polled); err != nil {
		t.Fatalf("unmarshal poll: %v", err)
	}
	if polled["name"] != name || polled["done"] != true {
		t.Errorf("polled operation = %v", polled)
	}

	// Unsupported service: a name that is not a Google API service has no
	// managed identity (real GCP also refuses to generate one).
	status, body = serveBody(t, p, http.MethodPost,
		"/v1beta1/projects/proj/services/example.com:generateServiceIdentity", "")
	if status != http.StatusBadRequest {
		t.Fatalf("unsupported service status = %d, body = %s", status, body)
	}
	if !strings.Contains(string(body), "FAILED_PRECONDITION") ||
		!strings.Contains(string(body), "No service identity exists for service") {
		t.Errorf("unsupported service body = %s", body)
	}
}

func TestEnableGetListRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.EnableService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"}))
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	svc, _ := opResponse(t, resp)["service"].(map[string]any)
	if svc["state"] != string(core.StateEnabled) {
		t.Fatalf("enable response state = %v, want ENABLED", svc["state"])
	}
	if svc["name"] != "projects/proj/services/run.googleapis.com" {
		t.Fatalf("enable response name = %v", svc["name"])
	}
	if cfg, _ := svc["config"].(map[string]any); cfg["name"] != "run.googleapis.com" {
		t.Fatalf("enable response config = %v", svc["config"])
	}
	// The LRO metadata matches google.api.serviceusage.v1.OperationMetadata:
	// resourceNames only (no non-Discovery "verb" field).
	meta, _ := resp.Data["metadata"].(map[string]any)
	if meta["@type"] != "type.googleapis.com/google.api.serviceusage.v1.OperationMetadata" {
		t.Fatalf("metadata @type = %v", meta["@type"])
	}
	names, _ := meta["resourceNames"].([]string)
	if len(names) != 1 || names[0] != "projects/proj/services/run.googleapis.com" {
		t.Fatalf("metadata resourceNames = %v", meta["resourceNames"])
	}
	if _, ok := meta["verb"]; ok {
		t.Fatalf("metadata must not carry a verb field: %v", meta)
	}

	resp, err = p.GetService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Data["state"] != string(core.StateEnabled) {
		t.Fatalf("get state = %v, want ENABLED", resp.Data["state"])
	}
	if resp.Data["parent"] != "projects/proj" {
		t.Fatalf("get parent = %v", resp.Data["parent"])
	}

	// filter=state:ENABLED must include it; state:DISABLED must not.
	resp, err = p.ListServices(ctx, newNR(map[string]any{"project": "proj", "filter": "state:ENABLED"}))
	if err != nil {
		t.Fatalf("list enabled: %v", err)
	}
	if got := len(resp.Data["services"].([]any)); got != 1 {
		t.Fatalf("list enabled count = %d, want 1", got)
	}
	resp, err = p.ListServices(ctx, newNR(map[string]any{"project": "proj", "filter": "state:DISABLED"}))
	if err != nil {
		t.Fatalf("list disabled: %v", err)
	}
	if got := len(resp.Data["services"].([]any)); got != 0 {
		t.Fatalf("list disabled count = %d, want 0", got)
	}
	// No filter lists everything tracked.
	resp, err = p.ListServices(ctx, newNR(map[string]any{"project": "proj"}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := len(resp.Data["services"].([]any)); got != 1 {
		t.Fatalf("unfiltered list count = %d, want 1", got)
	}
}

// failingStore surfaces a storage outage; embedded ResourceStore satisfies the
// remaining interface methods (nil is never called).
type failingStore struct{ store.ResourceStore }

func (failingStore) Get(context.Context, string, string, string, string) (store.ResourceEntry, error) {
	return store.ResourceEntry{}, store.ErrStorageUnavailable
}

func (failingStore) Upsert(context.Context, string, string, store.ResourceEntry) error {
	return store.ErrStorageUnavailable
}

func TestStorageErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	p := NewProvider(core.NewService(failingStore{}), "default-proj")

	if _, err := p.GetService(ctx, newNR(map[string]any{"project": "proj", "service": "a.googleapis.com"})); !errors.Is(err, store.ErrStorageUnavailable) {
		t.Errorf("GetService error = %v, want ErrStorageUnavailable", err)
	}
	if _, err := p.EnableService(ctx, newNR(map[string]any{"project": "proj", "service": "a.googleapis.com"})); !errors.Is(err, store.ErrStorageUnavailable) {
		t.Errorf("EnableService error = %v, want ErrStorageUnavailable", err)
	}
	if _, err := p.DisableService(ctx, newNR(map[string]any{"project": "proj", "service": "a.googleapis.com"})); !errors.Is(err, store.ErrStorageUnavailable) {
		t.Errorf("DisableService error = %v, want ErrStorageUnavailable", err)
	}
	if _, err := p.GenerateServiceIdentity(ctx, newNR(map[string]any{"project": "proj", "service": "a.googleapis.com"})); !errors.Is(err, store.ErrStorageUnavailable) {
		t.Errorf("GenerateServiceIdentity error = %v, want ErrStorageUnavailable", err)
	}
}

func TestMissingParamsAreInvalidArgument(t *testing.T) {
	ctx := context.Background()
	p := NewProvider(core.NewService(store.NewMemoryResourceStore()), "")
	handlers := map[string]func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error){
		"list":        p.ListServices,
		"get":         p.GetService,
		"enable":      p.EnableService,
		"disable":     p.DisableService,
		"batchGet":    p.BatchGetServices,
		"batchEnable": p.BatchEnableServices,
		"identity":    p.GenerateServiceIdentity,
	}
	// No project in the path, no account scope, and no configured default.
	for name, fn := range handlers {
		_, err := fn(ctx, &model.NormalizedRequest{Params: map[string]any{}})
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Errorf("%s: err = %v, want 400 InvalidArgument", name, err)
		}
	}
}

func TestGetUnknownServiceIsDisabled(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	resp, err := p.GetService(ctx, newNR(map[string]any{"project": "proj", "service": "unknown.googleapis.com"}))
	if err != nil {
		t.Fatalf("get unknown: %v", err)
	}
	if resp.Data["state"] != string(core.StateDisabled) {
		t.Fatalf("unknown service state = %v, want DISABLED", resp.Data["state"])
	}
}

func TestDisableNotEnabledIsFailedPrecondition(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	_, err := p.DisableService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"}))
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T (%v)", err, err)
	}
	if pe.Code != "FailedPrecondition" || pe.HTTPStatus != 400 {
		t.Fatalf("disable not-enabled = %+v, want FailedPrecondition/400", pe)
	}

	// Enable then disable succeeds and flips the stored state.
	if _, err := p.EnableService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"})); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if _, err := p.DisableService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"})); err != nil {
		t.Fatalf("disable: %v", err)
	}
	resp, err := p.GetService(ctx, newNR(map[string]any{"project": "proj", "service": "run.googleapis.com"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Data["state"] != string(core.StateDisabled) {
		t.Fatalf("state after disable = %v, want DISABLED", resp.Data["state"])
	}
}

func TestBatchEnable(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.BatchEnableServices(ctx, newNR(map[string]any{
		"project": "proj",
		"body":    map[string]any{"serviceIds": []any{"a.googleapis.com", "b.googleapis.com"}},
	}))
	if err != nil {
		t.Fatalf("batchEnable: %v", err)
	}
	services := opResponse(t, resp)["services"].([]any)
	if len(services) != 2 {
		t.Fatalf("batchEnable services = %d, want 2", len(services))
	}
	for _, svc := range services {
		if svc.(map[string]any)["state"] != string(core.StateEnabled) {
			t.Fatalf("batchEnable state = %v", svc)
		}
	}

	// Empty and oversized batches are rejected.
	if _, err := p.BatchEnableServices(ctx, newNR(map[string]any{"project": "proj", "body": map[string]any{}})); err == nil {
		t.Error("expected error for empty batch")
	}
	ids := make([]any, 21)
	for i := range ids {
		ids[i] = "s.googleapis.com"
	}
	if _, err := p.BatchEnableServices(ctx, newNR(map[string]any{"project": "proj", "body": map[string]any{"serviceIds": ids}})); err == nil {
		t.Error("expected error for oversized batch")
	}
}

func TestBatchGet(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.EnableService(ctx, newNR(map[string]any{"project": "proj", "service": "a.googleapis.com"})); err != nil {
		t.Fatalf("enable: %v", err)
	}
	resp, err := p.BatchGetServices(ctx, newNR(map[string]any{
		"project": "proj",
		"names":   []string{"projects/proj/services/a.googleapis.com", "projects/proj/services/b.googleapis.com"},
	}))
	if err != nil {
		t.Fatalf("batchGet: %v", err)
	}
	services, _ := resp.Data["services"].([]any)
	if len(services) != 2 {
		t.Fatalf("batchGet services = %d, want 2", len(services))
	}
	first, _ := services[0].(map[string]any)
	if first["name"] != "projects/proj/services/a.googleapis.com" || first["state"] != string(core.StateEnabled) {
		t.Errorf("services[0] = %v, want enabled a.googleapis.com", services[0])
	}
	second, _ := services[1].(map[string]any)
	if second["state"] != string(core.StateDisabled) {
		t.Errorf("services[1] = %v, want DISABLED", services[1])
	}

	// Empty and oversized batches are rejected with a 400.
	for _, tc := range []struct {
		name  string
		names any
	}{
		{"empty", []string{}},
		{"oversized", oversizedNames(31)},
		{"malformed", []string{"a.googleapis.com"}},
	} {
		_, err := p.BatchGetServices(ctx, newNR(map[string]any{"project": "proj", "names": tc.names}))
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Errorf("batchGet(%s) error = %v, want 400 InvalidArgument", tc.name, err)
		}
	}
}

func oversizedNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = "projects/proj/services/s.googleapis.com"
	}
	return names
}

func TestListPaginationAndFilterValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, s := range []string{"a.googleapis.com", "b.googleapis.com", "c.googleapis.com"} {
		if _, err := p.EnableService(ctx, newNR(map[string]any{"project": "proj", "service": s})); err != nil {
			t.Fatalf("enable %s: %v", s, err)
		}
	}

	resp, err := p.ListServices(ctx, newNR(map[string]any{"project": "proj", "pageSize": "2"}))
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if got := len(resp.Data["services"].([]any)); got != 2 {
		t.Fatalf("page 1 size = %d, want 2", got)
	}
	next, _ := resp.Data["nextPageToken"].(string)
	if next == "" {
		t.Fatal("expected nextPageToken on a full page")
	}
	resp, err = p.ListServices(ctx, newNR(map[string]any{"project": "proj", "pageSize": "2", "pageToken": next}))
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if got := len(resp.Data["services"].([]any)); got != 1 {
		t.Fatalf("page 2 size = %d, want 1", got)
	}

	// Invalid filter is a 400 InvalidArgument.
	_, err = p.ListServices(ctx, newNR(map[string]any{"project": "proj", "filter": "state:RUNNING"}))
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
		t.Fatalf("invalid filter error = %v, want 400", err)
	}
}

// TestListNonIntegerPageSize pins the int32 query binding: a non-integer
// pageSize is INVALID_ARGUMENT rather than a silent zero.
func TestListNonIntegerPageSize(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, v := range []any{"abc", "-0.5", "1.5"} {
		_, err := p.ListServices(ctx, newNR(map[string]any{"project": "proj", "pageSize": v}))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("ListServices pageSize=%#v err = %v, want InvalidArgument/400", v, err)
		}
	}
	if _, err := p.ListServices(ctx, newNR(map[string]any{"project": "proj", "pageSize": "5"})); err != nil {
		t.Errorf("ListServices pageSize=5: %v", err)
	}
}
