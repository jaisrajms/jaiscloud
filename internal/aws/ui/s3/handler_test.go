package s3ui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

type mockS3Provider struct {
	listBucketsResp    *model.ProviderResponse
	listBucketsErr     error
	createBucketResp   *model.ProviderResponse
	createBucketErr    error
	deleteBucketErr    error
	listObjectsResp    *model.ProviderResponse
	listObjectsErr     error
	headObjectResp     *model.ProviderResponse
	headObjectErr      error
	getObjectResp      *model.ProviderResponse
	getObjectErr       error
	putObjectErr       error
	deleteObjectErr    error
	deleteObjectsResp  *model.ProviderResponse
	deleteObjectsErr   error
	getBucketTagsResp  *model.ProviderResponse
	getBucketTagsErr   error

	lastNR          *model.NormalizedRequest
	listBucketsNR   *model.NormalizedRequest
	createBucketNR  *model.NormalizedRequest
}

func (m *mockS3Provider) ListBuckets(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.listBucketsNR = nr
	m.lastNR = nr
	if m.listBucketsResp != nil {
		return m.listBucketsResp, m.listBucketsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Buckets": []map[string]any{}}}, m.listBucketsErr
}
func (m *mockS3Provider) CreateBucket(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.createBucketNR = nr
	m.lastNR = nr
	return m.createBucketResp, m.createBucketErr
}
func (m *mockS3Provider) DeleteBucket(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, m.deleteBucketErr
}
func (m *mockS3Provider) ListObjectsV2(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listObjectsResp != nil {
		return m.listObjectsResp, m.listObjectsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Contents": []map[string]any{}}}, m.listObjectsErr
}
func (m *mockS3Provider) HeadObject(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.headObjectResp != nil {
		return m.headObjectResp, m.headObjectErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.headObjectErr
}
func (m *mockS3Provider) GetObject(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getObjectResp != nil {
		return m.getObjectResp, m.getObjectErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"_body": []byte("hello")}}, m.getObjectErr
}
func (m *mockS3Provider) PutObject(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.putObjectErr
}
func (m *mockS3Provider) DeleteObject(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, m.deleteObjectErr
}
func (m *mockS3Provider) DeleteObjects(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.deleteObjectsResp != nil {
		return m.deleteObjectsResp, m.deleteObjectsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteObjectsErr
}
func (m *mockS3Provider) CopyObject(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}
func (m *mockS3Provider) GetBucketVersioning(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Status": "Suspended"}}, nil
}
func (m *mockS3Provider) PutBucketVersioning(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}
func (m *mockS3Provider) ListObjectVersions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Versions": []map[string]any{}, "DeleteMarkers": []map[string]any{}}}, nil
}
func (m *mockS3Provider) ListMultipartUploads(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Uploads": []map[string]any{}}}, nil
}
func (m *mockS3Provider) AbortMultipartUpload(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}
func (m *mockS3Provider) GetBucketTagging(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getBucketTagsResp != nil {
		return m.getBucketTagsResp, m.getBucketTagsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"TagSet": []map[string]any{}}}, m.getBucketTagsErr
}
func (m *mockS3Provider) PutBucketTagging(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}
func (m *mockS3Provider) HeadBucket(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func testCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

// withChiParam sets a chi URL param on the request context.
func withChiParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		rctx = chi.NewRouteContext()
		r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	}
	rctx.URLParams.Add(key, value)
	return r
}

// ── ListBuckets ───────────────────────────────────────────────────────────────

func TestS3ListBuckets_Returns200WithItems(t *testing.T) {
	mock := &mockS3Provider{
		listBucketsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Buckets": []map[string]any{
				{"Name": "my-bucket"},
				{"Name": "other-bucket"},
			},
		}},
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/buckets", nil)
	rr := httptest.NewRecorder()
	h.ListBuckets(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ListBucketsResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("want 2 items, got %d", len(resp.Items))
	}
	if resp.Items[0].Name != "my-bucket" {
		t.Errorf("want name=my-bucket, got %q", resp.Items[0].Name)
	}
}

func TestS3ListBuckets_EmptyReturns200WithEmptyArray(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/buckets", nil)
	rr := httptest.NewRecorder()
	h.ListBuckets(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ListBucketsResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Items == nil {
		t.Error("items should be non-nil empty slice")
	}
}

func TestS3ListBuckets_ProviderErrorReturnsErrorShape(t *testing.T) {
	mock := &mockS3Provider{
		listBucketsErr: model.NewProviderError("AccessDenied", "not allowed", 403),
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/buckets", nil)
	rr := httptest.NewRecorder()
	h.ListBuckets(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("want 403, got %d", rr.Code)
	}
	var body map[string]string
	json.NewDecoder(rr.Body).Decode(&body)
	if body["code"] != "AccessDenied" {
		t.Errorf("want code=AccessDenied, got %q", body["code"])
	}
}

// ── CreateBucket ──────────────────────────────────────────────────────────────

func TestS3CreateBucket_ValidBodyReturns201(t *testing.T) {
	mock := &mockS3Provider{
		createBucketResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"name":"test-bucket"}`
	req := httptest.NewRequest(http.MethodPost, "/buckets", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateBucket(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestS3CreateBucket_MissingNameReturns400(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodPost, "/buckets", bytes.NewBufferString(`{"name":""}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateBucket(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestS3CreateBucket_InvalidBodyReturns400(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodPost, "/buckets", bytes.NewBufferString(`not-json`))
	rr := httptest.NewRecorder()
	h.CreateBucket(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

// ── DeleteBucket ──────────────────────────────────────────────────────────────

func TestS3DeleteBucket_Returns204(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodDelete, "/buckets/my-bucket", nil)
	req = withChiParam(req, "bucket", "my-bucket")
	rr := httptest.NewRecorder()
	h.DeleteBucket(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d", rr.Code)
	}
}

// ── HeadObject ────────────────────────────────────────────────────────────────

func TestS3HeadObject_MissingKeyReturns400(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/buckets/b/objects/head", nil)
	req = withChiParam(req, "bucket", "b")
	rr := httptest.NewRecorder()
	h.HeadObject(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestS3HeadObject_Returns200(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/buckets/b/objects/head?key=foo.txt", nil)
	req = withChiParam(req, "bucket", "b")
	rr := httptest.NewRecorder()
	h.HeadObject(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
}

// ── PutObject ─────────────────────────────────────────────────────────────────

func TestS3PutObject_MissingKeyReturns400(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodPut, "/buckets/b/objects", bytes.NewBufferString("data"))
	req = withChiParam(req, "bucket", "b")
	rr := httptest.NewRecorder()
	h.PutObject(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestS3PutObject_Returns200(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodPut, "/buckets/b/objects?key=foo.txt", bytes.NewBufferString("hello"))
	req = withChiParam(req, "bucket", "b")
	rr := httptest.NewRecorder()
	h.PutObject(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
}

// ── DeleteObject ──────────────────────────────────────────────────────────────

func TestS3DeleteObject_Returns204(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodDelete, "/buckets/b/objects?key=foo.txt", nil)
	req = withChiParam(req, "bucket", "b")
	rr := httptest.NewRecorder()
	h.DeleteObject(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d", rr.Code)
	}
}

func TestS3DeleteObject_MissingKeyReturns400(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodDelete, "/buckets/b/objects", nil)
	req = withChiParam(req, "bucket", "b")
	rr := httptest.NewRecorder()
	h.DeleteObject(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

// ── NR invariant tests ────────────────────────────────────────────────────────

func TestS3NR_PortIsWirePortNotUIPort(t *testing.T) {
	cfg := testCfg()
	mock := &mockS3Provider{}
	h := NewHandler(mock, cfg)
	req := httptest.NewRequest(http.MethodGet, "/buckets", nil)
	h.ListBuckets(httptest.NewRecorder(), req)

	if mock.listBucketsNR == nil {
		t.Fatal("NR was not captured")
	}
	if mock.listBucketsNR.Port != cfg.Port {
		t.Errorf("nr.Port = %d, want wire port %d (not UI port %d)", mock.listBucketsNR.Port, cfg.Port, cfg.UIPort)
	}
}

func TestS3NR_ClockIsNotNil(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/buckets", nil)
	h.ListBuckets(httptest.NewRecorder(), req)

	if mock.listBucketsNR == nil {
		t.Fatal("NR was not captured")
	}
	if mock.listBucketsNR.Clock == nil {
		t.Error("nr.Clock must not be nil")
	}
}

func TestS3CreateBucket_NrParamsBucketKey(t *testing.T) {
	mock := &mockS3Provider{
		createBucketResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"name":"test-bucket"}`
	req := httptest.NewRequest(http.MethodPost, "/buckets", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	h.CreateBucket(httptest.NewRecorder(), req)

	if mock.createBucketNR == nil {
		t.Fatal("createBucketNR was not captured")
	}
	if v, ok := mock.createBucketNR.Params["_bucket"]; !ok || v == "" {
		t.Error("nr.Params must contain key _bucket for S3 provider")
	}
}

func TestS3ListObjects_NrParamsBucketKey(t *testing.T) {
	mock := &mockS3Provider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/buckets/my-bucket/objects", nil)
	req = withChiParam(req, "bucket", "my-bucket")
	h.ListObjects(httptest.NewRecorder(), req)

	if mock.lastNR == nil {
		t.Fatal("NR was not captured")
	}
	if v, ok := mock.lastNR.Params["_bucket"]; !ok || v != "my-bucket" {
		t.Errorf("nr.Params[_bucket] = %v, want my-bucket", mock.lastNR.Params["_bucket"])
	}
}

func TestS3GetBucketTags_Returns200WithTags(t *testing.T) {
	mock := &mockS3Provider{
		getBucketTagsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"TagSet": []map[string]any{
				{"Key": "Env", "Value": "test"},
			},
		}},
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/buckets/b/tags", nil)
	req = withChiParam(req, "bucket", "b")
	rr := httptest.NewRecorder()
	h.GetBucketTags(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp TagsResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Tags["Env"] != "test" {
		t.Errorf("want tags[Env]=test, got %v", resp.Tags)
	}
}
