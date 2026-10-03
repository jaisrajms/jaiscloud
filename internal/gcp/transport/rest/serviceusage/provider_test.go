package serviceusage

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		if _, err := c.Decode(r, nil); err == nil {
			t.Fatalf("%s %s: expected error", tc.method, tc.path)
		}
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
}

func TestMissingParamsAreInvalidArgument(t *testing.T) {
	ctx := context.Background()
	p := NewProvider(core.NewService(store.NewMemoryResourceStore()), "")
	handlers := map[string]func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error){
		"list":        p.ListServices,
		"get":         p.GetService,
		"enable":      p.EnableService,
		"disable":     p.DisableService,
		"batchEnable": p.BatchEnableServices,
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
