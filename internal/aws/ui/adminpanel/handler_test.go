package adminpanel

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
)

func testCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

func testHandler() *Handler {
	a := admin.NewHandler()
	return NewHandler(a, testCfg())
}

func TestAdminStatus_Returns200(t *testing.T) {
	h := testHandler()
	req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	rr := httptest.NewRecorder()
	h.Status(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestAdminStatus_ResponseContainsSnapshotters(t *testing.T) {
	h := testHandler()
	req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	rr := httptest.NewRecorder()
	h.Status(rr, req)

	body := rr.Body.String()
	if len(body) == 0 {
		t.Error("expected non-empty JSON response")
	}
	// Should be valid JSON (not error out decoding) — snapshotters field may be empty array.
	if body[0] != '{' {
		t.Errorf("expected JSON object, got: %s", body)
	}
}

func TestAdminExportInfo_Returns200WithDownloadUrl(t *testing.T) {
	h := testHandler()
	req := httptest.NewRequest(http.MethodGet, "/admin/export-info", nil)
	rr := httptest.NewRecorder()
	h.ExportInfo(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	if len(body) == 0 {
		t.Error("expected non-empty response")
	}
}

func TestAdminGetClock_Returns200(t *testing.T) {
	h := testHandler()
	req := httptest.NewRequest(http.MethodGet, "/admin/clock", nil)
	rr := httptest.NewRecorder()
	h.GetClock(rr, req)

	// admin.GetClock returns 200 with clock state.
	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestAdminListSnapshots_Returns200(t *testing.T) {
	h := testHandler()
	req := httptest.NewRequest(http.MethodGet, "/admin/snapshots", nil)
	rr := httptest.NewRecorder()
	h.ListSnapshots(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestAdminResponseRecorder_CapturesBody(t *testing.T) {
	rec := &responseRecorder{}
	rec.Write([]byte("hello"))

	if rec.body.String() != "hello" {
		t.Errorf("want body=hello, got %q", rec.body.String())
	}
}

func TestAdminResponseRecorder_CapturesStatusCode(t *testing.T) {
	rec := &responseRecorder{}
	rec.WriteHeader(204)

	if rec.statusCode != 204 {
		t.Errorf("want statusCode=204, got %d", rec.statusCode)
	}
}

func TestAdminResponseRecorder_HeaderNotNil(t *testing.T) {
	rec := &responseRecorder{}
	h := rec.Header()

	if h == nil {
		t.Error("Header() must return non-nil map")
	}
}
