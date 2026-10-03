package firestore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
)

func newTestProvider() *Provider {
	return New(firestorestore.NewMemoryStore(), nil)
}

func testNR() *model.NormalizedRequest {
	return &model.NormalizedRequest{
		AccountID: "proj",
		Params:    map[string]any{},
		ResourceID: func(rt, n string) string {
			return "projects/proj/" + n
		},
	}
}

func patchNR() *model.NormalizedRequest {
	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{
			"name": map[string]any{"stringValue": "SF"},
		},
	}
	return nr
}

func assertPreconditionErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected FAILED_PRECONDITION error, got nil")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 400 || pe.Status != "FAILED_PRECONDITION" {
		t.Fatalf("expected FAILED_PRECONDITION@400, got HTTP=%d status=%q", pe.HTTPStatus, pe.Status)
	}
}

func assertNotFoundErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected NOT_FOUND error, got nil")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 404 || pe.Code != "NotFound" {
		t.Fatalf("expected NotFound@404, got HTTP=%d code=%q", pe.HTTPStatus, pe.Code)
	}
	if !strings.Contains(pe.Message, "No document to update") {
		t.Fatalf("expected 'No document to update' message, got %q", pe.Message)
	}
}

func assertAlreadyExistsErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected ALREADY_EXISTS error, got nil")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 409 || pe.Code != "AlreadyExists" {
		t.Fatalf("expected AlreadyExists@409, got HTTP=%d code=%q", pe.HTTPStatus, pe.Code)
	}
	if !strings.Contains(pe.Message, "already exists") {
		t.Fatalf("expected 'already exists' message, got %q", pe.Message)
	}
}

func assertInvalidArgumentErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected INVALID_ARGUMENT error, got nil")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
		t.Fatalf("expected InvalidArgument@400, got HTTP=%d code=%q", pe.HTTPStatus, pe.Code)
	}
}

func TestDocumentsPatchPrecondition(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	// Seed a document.
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: name, Fields: map[string]*firestorestore.Value{"a": intField(1)}, CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// exists=false on a present doc → ALREADY_EXISTS "Document already exists".
	nr := patchNR()
	nr.Params["currentDocument.exists"] = "false"
	_, err := p.DocumentsPatch(ctx, nr)
	assertAlreadyExistsErr(t, err)

	// stale updateTime → FAILED_PRECONDITION.
	nr = patchNR()
	nr.Params["currentDocument.updateTime"] = now.Add(-time.Hour).Format(time.RFC3339Nano)
	_, err = p.DocumentsPatch(ctx, nr)
	assertPreconditionErr(t, err)

	// exists=true on a missing doc → NOT_FOUND "No document to update" (real
	// Firestore rejects the update; it does not report FAILED_PRECONDITION).
	nr = patchNR()
	nr.Params["name"] = "databases/(default)/documents/cities/MISSING"
	nr.Params["currentDocument.exists"] = "true"
	_, err = p.DocumentsPatch(ctx, nr)
	assertNotFoundErr(t, err)

	// matching updateTime → succeeds.
	nr = patchNR()
	nr.Params["currentDocument.updateTime"] = now.Format(time.RFC3339Nano)
	resp, err := p.DocumentsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch with matching updateTime: %v", err)
	}
	if resp == nil {
		t.Fatal("expected a response")
	}
}

// TestCommitUpdateMissingWithExistsPrecondition reproduces the Java SDK's
// DocumentReference.update(): it commits a Write.Update with
// currentDocument.exists=true. On a missing document real Firestore returns
// NOT_FOUND "No document to update: <name>" (FirestoreTest
// #updateFailsWhenDocumentMissing), not FAILED_PRECONDITION.
func TestCommitUpdateMissingWithExistsPrecondition(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/MISSING"

	nr := testNR()
	nr.Params["body"] = map[string]any{
		"writes": []any{
			map[string]any{
				"update": map[string]any{
					"name":   name,
					"fields": map[string]any{"name": map[string]any{"stringValue": "Alice"}},
				},
				"currentDocument": map[string]any{"exists": true},
			},
		},
	}
	_, err := p.Commit(ctx, nr)
	assertNotFoundErr(t, err)

	// The failed precondition must not have created the document.
	if _, err := p.store.GetDocument(ctx, name); !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		t.Fatalf("update-on-missing must not create the document, got err=%v", err)
	}

	// updateTime precondition on a missing doc stays FAILED_PRECONDITION.
	nr = testNR()
	nr.Params["body"] = map[string]any{
		"writes": []any{
			map[string]any{
				"update": map[string]any{
					"name":   name,
					"fields": map[string]any{"name": map[string]any{"stringValue": "Alice"}},
				},
				"currentDocument": map[string]any{"updateTime": "2026-01-01T00:00:00Z"},
			},
		},
	}
	_, err = p.Commit(ctx, nr)
	assertPreconditionErr(t, err)
}

// TestBatchWriteUpdateMissingStatus checks the BatchWrite per-write status for
// the same precondition: google.rpc code 5 (NOT_FOUND), not 3.
func TestBatchWriteUpdateMissingStatus(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/MISSING"

	nr := testNR()
	nr.Params["body"] = map[string]any{
		"writes": []any{
			map[string]any{
				"update": map[string]any{
					"name":   name,
					"fields": map[string]any{"name": map[string]any{"stringValue": "Alice"}},
				},
				"currentDocument": map[string]any{"exists": true},
			},
		},
	}
	resp, err := p.BatchWrite(ctx, nr)
	if err != nil {
		t.Fatalf("batchWrite: %v", err)
	}
	statuses, _ := resp.Data["status"].([]any)
	if len(statuses) != 1 {
		t.Fatalf("expected 1 status, got %d", len(statuses))
	}
	st, _ := statuses[0].(map[string]any)
	if code, _ := st["code"].(int64); code != 5 {
		t.Fatalf("expected NOT_FOUND (5) per-write status, got %+v", st)
	}
	if msg, _ := st["message"].(string); !strings.Contains(msg, "No document to update") {
		t.Fatalf("expected 'No document to update' message, got %q", msg)
	}
}

// TestCommitDeleteMissingWithExistsPrecondition covers the delete-write half of
// the precondition contract: a Write.delete with currentDocument.exists=true on
// a missing document is NOT_FOUND ("No document to update"), and
// currentDocument.exists=false on a present document is ALREADY_EXISTS. Both
// come from the Commit path the SDKs use, not the unary REST methods.
func TestCommitDeleteMissingWithExistsPrecondition(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	missing := "projects/proj/databases/(default)/documents/cities/MISSING"
	present := "projects/proj/databases/(default)/documents/cities/SF"

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: present, Fields: map[string]*firestorestore.Value{"a": intField(1)}, CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// exists=true on a missing doc → NOT_FOUND.
	nr := testNR()
	nr.Params["body"] = map[string]any{
		"writes": []any{
			map[string]any{
				"delete":          missing,
				"currentDocument": map[string]any{"exists": true},
			},
		},
	}
	_, err := p.Commit(ctx, nr)
	assertNotFoundErr(t, err)

	// exists=false on a present doc → ALREADY_EXISTS; the doc survives.
	nr = testNR()
	nr.Params["body"] = map[string]any{
		"writes": []any{
			map[string]any{
				"delete":          present,
				"currentDocument": map[string]any{"exists": false},
			},
		},
	}
	_, err = p.Commit(ctx, nr)
	assertAlreadyExistsErr(t, err)
	if _, err := p.store.GetDocument(ctx, present); err != nil {
		t.Fatalf("failed delete precondition must not delete the document, got err=%v", err)
	}

	// A matching exists=true precondition deletes the document.
	nr = testNR()
	nr.Params["body"] = map[string]any{
		"writes": []any{
			map[string]any{
				"delete":          present,
				"currentDocument": map[string]any{"exists": true},
			},
		},
	}
	if _, err := p.Commit(ctx, nr); err != nil {
		t.Fatalf("delete with matching exists=true: %v", err)
	}
	if _, err := p.store.GetDocument(ctx, present); !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		t.Fatalf("expected document deleted, got err=%v", err)
	}
}

// TestBatchWriteDeleteStatus checks the BatchWrite per-write status for delete
// preconditions: NOT_FOUND (5) for exists=true on a missing doc, ALREADY_EXISTS
// (6) for exists=false on a present doc.
func TestBatchWriteDeleteStatus(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	missing := "projects/proj/databases/(default)/documents/cities/MISSING"
	present := "projects/proj/databases/(default)/documents/cities/SF"

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: present, Fields: map[string]*firestorestore.Value{"a": intField(1)}, CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cases := []struct {
		name     string
		write    map[string]any
		wantCode int64
		wantMsg  string
	}{
		{
			name:     "exists=true missing",
			write:    map[string]any{"delete": missing, "currentDocument": map[string]any{"exists": true}},
			wantCode: 5,
			wantMsg:  "No document to update",
		},
		{
			name:     "exists=false present",
			write:    map[string]any{"delete": present, "currentDocument": map[string]any{"exists": false}},
			wantCode: 6,
			wantMsg:  "already exists",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nr := testNR()
			nr.Params["body"] = map[string]any{"writes": []any{tc.write}}
			resp, err := p.BatchWrite(ctx, nr)
			if err != nil {
				t.Fatalf("batchWrite: %v", err)
			}
			statuses, _ := resp.Data["status"].([]any)
			if len(statuses) != 1 {
				t.Fatalf("expected 1 status, got %d", len(statuses))
			}
			st, _ := statuses[0].(map[string]any)
			if code, _ := st["code"].(int64); code != tc.wantCode {
				t.Fatalf("expected code %d, got %+v", tc.wantCode, st)
			}
			if msg, _ := st["message"].(string); !strings.Contains(msg, tc.wantMsg) {
				t.Fatalf("expected message containing %q, got %q", tc.wantMsg, msg)
			}
		})
	}
}

func TestDocumentsPatchMask(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: name,
		Fields: map[string]*firestorestore.Value{
			"a": intField(1),
			"b": intField(2),
		},
		CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{
			"a": map[string]any{"integerValue": "99"},
		},
	}
	nr.Params["mask.fieldPaths"] = "a"

	resp, err := p.DocumentsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch with mask: %v", err)
	}
	m, _ := resp.Data["fields"].(map[string]any)
	if _, ok := m["a"]; !ok {
		t.Errorf("expected field a to be present, got %+v", m)
	}
	if _, ok := m["b"]; !ok {
		t.Errorf("field b should be untouched by mask, got %+v", m)
	}
}

func TestDocumentsDeletePrecondition(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: name, Fields: map[string]*firestorestore.Value{"a": intField(1)}, CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// exists=false on a present doc → ALREADY_EXISTS "Document already exists".
	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["currentDocument.exists"] = "false"
	_, err := p.DocumentsDelete(ctx, nr)
	assertAlreadyExistsErr(t, err)

	// stale updateTime → FAILED_PRECONDITION.
	nr = testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["currentDocument.updateTime"] = now.Add(-time.Hour).Format(time.RFC3339Nano)
	_, err = p.DocumentsDelete(ctx, nr)
	assertPreconditionErr(t, err)

	// exists=true on a missing doc → NOT_FOUND "No document to update" (the
	// same delete-precondition contract as an update).
	nr = testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/MISSING"
	nr.Params["currentDocument.exists"] = "true"
	_, err = p.DocumentsDelete(ctx, nr)
	assertNotFoundErr(t, err)

	// matching updateTime → succeeds.
	nr = testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["currentDocument.updateTime"] = now.Format(time.RFC3339Nano)
	if _, err := p.DocumentsDelete(ctx, nr); err != nil {
		t.Fatalf("delete with matching updateTime: %v", err)
	}
}

func TestDocumentsGetMask(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: name,
		Fields: map[string]*firestorestore.Value{
			"a": intField(1),
			"b": intField(2),
		},
		CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["mask.fieldPaths"] = "a"

	resp, err := p.DocumentsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get with mask: %v", err)
	}
	m, _ := resp.Data["fields"].(map[string]any)
	if _, ok := m["a"]; !ok {
		t.Errorf("expected field a to be present, got %+v", m)
	}
	if _, ok := m["b"]; ok {
		t.Errorf("field b should be masked out, got %+v", m)
	}
}

func TestStringValueSizeLimit(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Over the per-field string limit → 400 INVALID_ARGUMENT.
	tooBig := strings.Repeat("x", maxStringBytes+1)
	nr := patchNR()
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{"s": map[string]any{"stringValue": tooBig}},
	}
	if _, err := p.DocumentsPatch(ctx, nr); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for oversized stringValue")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// Exactly at the limit → accepted by the field-limit check.
	atLimit := strings.Repeat("x", maxStringBytes)
	if err := checkFieldLimits(map[string]*firestorestore.Value{"s": firestorestore.StringVal(atLimit)}); err != nil {
		t.Fatalf("expected %d-byte stringValue to be accepted, got %v", maxStringBytes, err)
	}
}

func TestFieldNameLimits(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Empty field name → 400 INVALID_ARGUMENT.
	nr := patchNR()
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{"": map[string]any{"integerValue": "1"}},
	}
	if _, err := p.DocumentsPatch(ctx, nr); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for empty field name")
	} else {
		assertInvalidArgumentErr(t, err)
	}

	// 1,501-byte field name → 400 INVALID_ARGUMENT.
	longName := strings.Repeat("f", maxFieldNameBytes+1)
	nr = patchNR()
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{longName: map[string]any{"integerValue": "1"}},
	}
	if _, err := p.DocumentsPatch(ctx, nr); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for oversized field name")
	} else {
		assertInvalidArgumentErr(t, err)
	}
}
