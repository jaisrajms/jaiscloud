package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// stubFilterCodec decodes to Storage.ObjectsGet and marks its error encoding so
// the test can prove the filter response came through the service codec.
type stubFilterCodec struct{}

func (stubFilterCodec) ServiceName() string { return "storage" }

func (stubFilterCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	return &model.NormalizedRequest{Service: "storage", Action: "ObjectsGet", Params: map[string]any{}, Raw: r}, nil
}

func (stubFilterCodec) Encode(*model.NormalizedRequest, *model.ProviderResponse) (int, http.Header, []byte) {
	return http.StatusOK, http.Header{}, []byte(`{"ok":true}`)
}

func (stubFilterCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	return perr.HTTPStatus, h, []byte(`{"error":true}`)
}

// stubFilterAdapter implements adapter.CloudAdapter plus the gateway's optional
// RequestFilter capability.
type stubFilterAdapter struct {
	fail           bool
	decorateCalled bool
}

func (a *stubFilterAdapter) Cloud() model.Cloud { return model.CloudGCP }
func (a *stubFilterAdapter) DetectAndDecode(r *http.Request, body []byte) (*model.NormalizedRequest, adapter.Codec, error) {
	return &model.NormalizedRequest{Service: "storage", Action: "ObjectsGet", Params: map[string]any{}, Raw: r}, stubFilterCodec{}, nil
}
func (a *stubFilterAdapter) ServiceToProvider(string) string { return "Storage" }
func (a *stubFilterAdapter) EnrichRequest(*http.Request, string, string) (string, string, string) {
	return "global", "proj", ""
}
func (a *stubFilterAdapter) ResourceIDFor(string, string) func(string, string) string {
	return func(resourceType, name string) string { return "proj/" + resourceType + "/" + name }
}
func (a *stubFilterAdapter) FilterRequest(*model.NormalizedRequest) *model.ProviderError {
	if !a.fail {
		return nil
	}
	return &model.ProviderError{Code: "ResourceExhausted", Message: "throttled", HTTPStatus: http.StatusTooManyRequests}
}
func (a *stubFilterAdapter) DecorateError(_ *model.NormalizedRequest, _ *model.ProviderError, status int, headers http.Header, body []byte) (int, http.Header, []byte) {
	a.decorateCalled = true
	if headers == nil {
		headers = http.Header{}
	}
	headers.Set("Retry-After", "7")
	return status, headers, append(body, []byte("-decorated")...)
}

func newFilterServer(adp adapter.CloudAdapter, prov *stubStorageProvider) *Server {
	return &Server{
		cfg:          &config.Config{Region: "global", AccountID: "proj", LogLevel: "error"},
		registry:     provider.NewRegistry().Register(prov),
		cloudAdapter: adp,
	}
}

func TestGatewayRequestFilterEncodesAndDecorates(t *testing.T) {
	adp := &stubFilterAdapter{fail: true}
	prov := &stubStorageProvider{}
	s := newFilterServer(adp, prov)

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b/bkt/o/x", nil)
	rec := httptest.NewRecorder()
	s.handleCloudRequest(rec, req)

	if prov.calls != 0 {
		t.Fatalf("provider must not be dispatched when the request is filtered, calls=%d", prov.calls)
	}
	if !adp.decorateCalled {
		t.Fatal("DecorateError must run for a filtered request")
	}
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "7" {
		t.Fatalf("Retry-After = %q, want 7", got)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `{"error":true}`) || !strings.Contains(body, "-decorated") {
		t.Fatalf("body = %q, want the codec error plus the decorator marker", body)
	}
}

func TestGatewayRequestFilterNilAllowsDispatch(t *testing.T) {
	adp := &stubFilterAdapter{fail: false}
	prov := &stubStorageProvider{}
	s := newFilterServer(adp, prov)

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b/bkt/o/x", nil)
	rec := httptest.NewRecorder()
	s.handleCloudRequest(rec, req)

	if prov.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", prov.calls)
	}
	if adp.decorateCalled {
		t.Fatal("DecorateError must not run when the filter allows the request")
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `{"ok":true}`) {
		t.Fatalf("response = %d %q, want the encoded response", rec.Code, rec.Body.String())
	}
}

// TestGatewayRequestFilterSkippedForAdapterWithoutCapability proves the filter
// is an optional capability: an adapter that does not implement RequestFilter
// dispatches normally (the AWS/Azure case).
func TestGatewayRequestFilterSkippedForAdapterWithoutCapability(t *testing.T) {
	adp := &stubBatchAdapter{} // implements CloudAdapter but not RequestFilter
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

	if prov.calls != 1 {
		t.Fatalf("provider calls = %d, want 1 (adapter without the capability dispatches)", prov.calls)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"hooked":true`) {
		t.Fatalf("response = %d %q, want the encoded response", rec.Code, rec.Body.String())
	}
}
