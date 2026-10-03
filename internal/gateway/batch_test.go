package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// stubBatchCodec is a codec that always decodes to Storage.ObjectsGet and
// encodes a recognisable marker body.
type stubBatchCodec struct{}

func (stubBatchCodec) ServiceName() string { return "storage" }

func (stubBatchCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	return &model.NormalizedRequest{
		Service: "storage",
		Action:  "ObjectsGet",
		Params:  map[string]any{},
		Raw:     r,
	}, nil
}

func (stubBatchCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	return http.StatusOK, http.Header{}, []byte(`{"hooked":true}`)
}

func (stubBatchCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	return perr.HTTPStatus, nil, []byte(`{"error":true}`)
}

// stubBatchAdapter implements adapter.CloudAdapter plus the gateway's
// BatchHandler extension point so the gateway's batch hook can be exercised
// without any cloud-specific code.
type stubBatchAdapter struct {
	batchCalled bool
}

func (a *stubBatchAdapter) Cloud() model.Cloud { return model.CloudGCP }
func (a *stubBatchAdapter) DetectAndDecode(r *http.Request, body []byte) (*model.NormalizedRequest, adapter.Codec, error) {
	return &model.NormalizedRequest{Service: "storage", Action: "ObjectsGet", Params: map[string]any{}, Raw: r}, stubBatchCodec{}, nil
}
func (a *stubBatchAdapter) ServiceToProvider(service string) string { return "Storage" }
func (a *stubBatchAdapter) EnrichRequest(r *http.Request, _, _ string) (string, string, string) {
	return "global", "proj", ""
}
func (a *stubBatchAdapter) ResourceIDFor(_, _ string) func(string, string) string {
	return func(resourceType, name string) string { return "proj/" + resourceType + "/" + name }
}
func (a *stubBatchAdapter) IsBatchRequest(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, "/batch/")
}
func (a *stubBatchAdapter) ServeBatch(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte, process BatchProcessFunc) {
	a.batchCalled = true
	sub := httptest.NewRequest(http.MethodGet, "/storage/v1/b/bkt/o/x", nil)
	status, headers, respBody := process(ctx, sub, nil)
	for k, vs := range headers {
		for _, v := range vs {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(status)
	_, _ = w.Write(respBody)
}

type stubStorageProvider struct{ calls int }

func (p *stubStorageProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Storage.ObjectsGet": func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			p.calls++
			return &model.ProviderResponse{HTTPStatus: http.StatusOK, Data: map[string]any{"name": "x"}}, nil
		},
	}
}

func TestGatewayBatchHookDispatchesSubRequests(t *testing.T) {
	adp := &stubBatchAdapter{}
	prov := &stubStorageProvider{}
	reg := provider.NewRegistry().Register(prov)
	s := &Server{
		cfg:          &config.Config{Region: "global", AccountID: "proj", LogLevel: "error"},
		registry:     reg,
		cloudAdapter: adp,
	}

	req := httptest.NewRequest(http.MethodPost, "/batch/storage/v1", strings.NewReader("--x--"))
	req.Header.Set("Content-Type", "multipart/mixed; boundary=x")
	rec := httptest.NewRecorder()
	s.handleCloudRequest(rec, req)

	if !adp.batchCalled {
		t.Fatal("batch handler was not invoked for POST /batch/storage/v1")
	}
	if prov.calls != 1 {
		t.Fatalf("provider calls = %d, want 1 (the sub-request should dispatch)", prov.calls)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"hooked":true`) {
		t.Fatalf("response = %d %q, want the encoded sub-response", rec.Code, rec.Body.String())
	}
}

func TestGatewayNonBatchRequestSkipsBatchHook(t *testing.T) {
	adp := &stubBatchAdapter{}
	prov := &stubStorageProvider{}
	reg := provider.NewRegistry().Register(prov)
	s := &Server{
		cfg:          &config.Config{Region: "global", AccountID: "proj", LogLevel: "error"},
		registry:     reg,
		cloudAdapter: adp,
	}

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b/bkt/o/x", nil)
	rec := httptest.NewRecorder()
	s.handleCloudRequest(rec, req)

	if adp.batchCalled {
		t.Fatal("batch handler must not be invoked for a non-batch request")
	}
	if prov.calls != 1 {
		t.Fatalf("provider calls = %d, want 1 (normal path dispatch)", prov.calls)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"hooked":true`) {
		t.Fatalf("response = %d %q, want the encoded response", rec.Code, rec.Body.String())
	}
}
