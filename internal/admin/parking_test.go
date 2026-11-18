package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeParker records the project it was handed and returns a canned result.
type fakeParker struct {
	name    string
	result  map[string]any
	err     error
	project string
	calls   int
}

func (f *fakeParker) Name() string { return f.name }

func (f *fakeParker) Park(_ context.Context, project string) (map[string]any, error) {
	f.calls++
	f.project = project
	return f.result, f.err
}

type fakeResumer struct {
	name    string
	err     error
	project string
	calls   int
}

func (f *fakeResumer) Name() string { return f.name }

func (f *fakeResumer) Resume(_ context.Context, project string) error {
	f.calls++
	f.project = project
	return f.err
}

func decodeHookBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	return body
}

func TestParkRunsEveryHookAndReportsResults(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{ProjectID: "proj"})
	okParker := &fakeParker{name: "dataproc", result: map[string]any{"jobsCancelled": 2}}
	h.RegisterParker(okParker)

	rec := httptest.NewRecorder()
	h.Park(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/park", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := decodeHookBody(t, rec)
	parked, ok := body["parked"].(map[string]any)
	if !ok {
		t.Fatalf("parked = %v, want an object", body["parked"])
	}
	dp, ok := parked["dataproc"].(map[string]any)
	if !ok || dp["jobsCancelled"] != float64(2) {
		t.Fatalf("parked.dataproc = %v, want jobsCancelled=2", parked["dataproc"])
	}
	if _, reported := body["errors"]; reported {
		t.Fatalf("errors reported on success: %v", body["errors"])
	}
	if okParker.project != "proj" || okParker.calls != 1 {
		t.Fatalf("parker calls = %d project = %q, want 1/proj", okParker.calls, okParker.project)
	}
}

func TestParkReportsSuccessesBesideErrors(t *testing.T) {
	h := NewHandler()
	good := &fakeParker{name: "good", result: map[string]any{"n": 1}}
	bad := &fakeParker{name: "bad", err: errors.New("boom")}
	h.RegisterParker(good)
	h.RegisterParker(bad)

	rec := httptest.NewRecorder()
	h.Park(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/park", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	body := decodeHookBody(t, rec)
	parked, _ := body["parked"].(map[string]any)
	if _, ok := parked["good"]; !ok {
		t.Fatalf("successful hook missing from parked: %v", body)
	}
	if _, ok := parked["bad"]; ok {
		t.Fatalf("failed hook reported as parked: %v", body)
	}
	errs, _ := body["errors"].(map[string]any)
	if errs["bad"] != "boom" {
		t.Fatalf("errors = %v, want bad=boom", body["errors"])
	}
}

func TestParkWithoutHooksReturnsEmptyMaps(t *testing.T) {
	h := NewHandler()
	rec := httptest.NewRecorder()
	h.Park(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/park", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := decodeHookBody(t, rec)
	parked, ok := body["parked"].(map[string]any)
	if !ok || len(parked) != 0 {
		t.Fatalf("parked = %v, want an empty object", body["parked"])
	}
}

func TestResumeRunsEveryHookAndReportsOK(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{ProjectID: "proj"})
	fr := &fakeResumer{name: "managedkafka"}
	h.RegisterResumer(fr)

	rec := httptest.NewRecorder()
	h.Resume(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/resume", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := decodeHookBody(t, rec)
	resumed, _ := body["resumed"].(map[string]any)
	if resumed["managedkafka"] != "ok" {
		t.Fatalf("resumed = %v, want managedkafka=ok", body["resumed"])
	}
	if fr.project != "proj" || fr.calls != 1 {
		t.Fatalf("resumer calls = %d project = %q, want 1/proj", fr.calls, fr.project)
	}
}

func TestResumeReportsSuccessesBesideErrors(t *testing.T) {
	h := NewHandler()
	h.RegisterResumer(&fakeResumer{name: "good"})
	h.RegisterResumer(&fakeResumer{name: "bad", err: errors.New("nope")})

	rec := httptest.NewRecorder()
	h.Resume(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/resume", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	body := decodeHookBody(t, rec)
	resumed, _ := body["resumed"].(map[string]any)
	if resumed["good"] != "ok" {
		t.Fatalf("resumed = %v, want good=ok", body["resumed"])
	}
	errs, _ := body["errors"].(map[string]any)
	if errs["bad"] != "nope" {
		t.Fatalf("errors = %v, want bad=nope", body["errors"])
	}
}

func TestResumeWithoutHooksReturnsEmptyMaps(t *testing.T) {
	h := NewHandler()
	rec := httptest.NewRecorder()
	h.Resume(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/resume", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := decodeHookBody(t, rec)
	resumed, ok := body["resumed"].(map[string]any)
	if !ok || len(resumed) != 0 {
		t.Fatalf("resumed = %v, want an empty object", body["resumed"])
	}
}
