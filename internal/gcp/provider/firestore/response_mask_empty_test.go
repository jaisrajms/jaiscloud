package firestore

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// TestDocumentsGetEmptyResponseMask pins DC60: a present-but-empty
// mask.fieldPaths is an explicit request for zero fields, not the omitted mask
// that returns every field.
func TestDocumentsGetEmptyResponseMask(t *testing.T) {
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
	nr.Raw = httptest.NewRequest("GET", "/v1/projects/proj/databases/(default)/documents/cities/SF?mask.fieldPaths=", nil)

	resp, err := p.DocumentsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get with empty mask: %v", err)
	}
	if fields, ok := resp.Data["fields"]; ok {
		t.Fatalf("empty response mask returned fields: %v", fields)
	}
	if resp.Data["name"] != name {
		t.Errorf("empty response mask dropped the document name: %v", resp.Data["name"])
	}
}

// TestListDocumentsEmptyResponseMask pins DC60 on documents.list: every
// returned document carries its name but no fields.
func TestListDocumentsEmptyResponseMask(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	for _, id := range []string{"a", "b"} {
		if err := p.store.CreateDocument(ctx, firestorestore.Document{
			Name:       "projects/proj/databases/(default)/documents/cities/" + id,
			ParentPath: "projects/proj/databases/(default)/documents/cities",
			Fields:     map[string]*firestorestore.Value{"n": intField(1)},
			CreateTime: now, UpdateTime: now,
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities"
	nr.Raw = httptest.NewRequest("GET", "/v1/projects/proj/databases/(default)/documents/cities?mask.fieldPaths=", nil)

	resp, err := p.ListDocuments(ctx, nr)
	if err != nil {
		t.Fatalf("list with empty mask: %v", err)
	}
	docs, _ := resp.Data["documents"].([]any)
	if len(docs) != 2 {
		t.Fatalf("documents = %v, want 2", resp.Data["documents"])
	}
	for _, raw := range docs {
		doc := raw.(map[string]any)
		if doc["name"] == nil || doc["name"] == "" {
			t.Errorf("empty response mask dropped a document name: %v", doc)
		}
		if fields, ok := doc["fields"]; ok {
			t.Errorf("empty response mask returned fields: %v", fields)
		}
	}
}

// TestDocumentsPatchEmptyResponseMask pins DC60 on the patch reply: the write
// is unaffected, but the response carries no fields.
func TestDocumentsPatchEmptyResponseMask(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	docName, _ := seedPatchDoc(t, p)

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{"a": map[string]any{"integerValue": "99"}},
	}
	nr.Raw = httptest.NewRequest("PATCH", "/v1/projects/proj/databases/(default)/documents/cities/SF?mask.fieldPaths=", nil)

	resp, err := p.DocumentsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch with empty response mask: %v", err)
	}
	if fields, ok := resp.Data["fields"]; ok {
		t.Fatalf("empty response mask returned fields: %v", fields)
	}
	stored, err := p.store.GetDocument(ctx, docName)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.Fields["a"].IntegerValue == nil || *stored.Fields["a"].IntegerValue != 99 {
		t.Errorf("stored a = %+v, want the written 99", stored.Fields["a"])
	}
}
