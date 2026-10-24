package firestore

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
)

// seedPatchDoc stores a two-field document at a fixed time and returns its name
// and updateTime.
func seedPatchDoc(t *testing.T, p *Provider) (string, time.Time) {
	t.Helper()
	name := "projects/proj/databases/(default)/documents/cities/SF"
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(context.Background(), firestorestore.Document{
		Name: name,
		Fields: map[string]*firestorestore.Value{
			"a": intField(1),
			"b": intField(2),
		},
		CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return name, now
}

func patchBody() map[string]any {
	return map[string]any{
		"fields": map[string]any{
			"a": map[string]any{"integerValue": "99"},
		},
	}
}

// TestDocumentsPatchEmptyMaskNoop covers AIP-134's present-but-empty
// update_mask: the request is a no-op — no field is written, update_time does
// not move, and the stored document is returned as-is. The write mask is
// `updateMask.fieldPaths`; both its decoded-Params and raw-URL forms count as
// present when supplied with no value.
func TestDocumentsPatchEmptyMaskNoop(t *testing.T) {
	cases := map[string]func() *model.NormalizedRequest{
		"params": func() *model.NormalizedRequest {
			nr := testNR()
			nr.Params["name"] = "databases/(default)/documents/cities/SF"
			nr.Params["body"] = patchBody()
			nr.Params["updateMask.fieldPaths"] = ""
			return nr
		},
		"raw": func() *model.NormalizedRequest {
			nr := testNR()
			nr.Params["name"] = "databases/(default)/documents/cities/SF"
			nr.Params["body"] = patchBody()
			nr.Raw = httptest.NewRequest("PATCH", "/v1/projects/proj/databases/(default)/documents/cities/SF?updateMask.fieldPaths=", nil)
			return nr
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			p := newTestProvider()
			docName, seeded := seedPatchDoc(t, p)

			resp, err := p.DocumentsPatch(ctx, build())
			if err != nil {
				t.Fatalf("patch with empty mask: %v", err)
			}
			fields, _ := resp.Data["fields"].(map[string]any)
			if len(fields) != 2 {
				t.Errorf("empty mask wrote fields: %+v", fields)
			}
			if got, _ := fields["a"].(map[string]any); got["integerValue"] != "1" {
				t.Errorf("field a = %+v, want the stored 1 (mask ignores the body)", fields["a"])
			}
			if resp.Data["updateTime"] != timestamp.Canonical(seeded) {
				t.Errorf("updateTime rotated to %v, want %v", resp.Data["updateTime"], timestamp.Canonical(seeded))
			}

			stored, err := p.store.GetDocument(ctx, docName)
			if err != nil {
				t.Fatalf("get after no-op: %v", err)
			}
			if len(stored.Fields) != 2 || stored.Fields["a"].IntegerValue == nil || *stored.Fields["a"].IntegerValue != 1 {
				t.Errorf("stored fields changed: %+v", stored.Fields)
			}
			if !stored.UpdateTime.Equal(seeded) {
				t.Errorf("stored updateTime = %v, want %v", stored.UpdateTime, seeded)
			}
			if seq := p.Service.CurrentSeq(); seq != 0 {
				t.Errorf("no-op published change seq %d; listeners must not see it", seq)
			}
		})
	}
}

// TestDocumentsPatchEmptyMaskValidatesPrecondition: the no-op still validates
// the currentDocument precondition — an empty mask is not an escape hatch.
func TestDocumentsPatchEmptyMaskValidatesPrecondition(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	seedPatchDoc(t, p)

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["body"] = patchBody()
	nr.Params["updateMask.fieldPaths"] = ""
	nr.Params["currentDocument.exists"] = "false"
	_, err := p.DocumentsPatch(ctx, nr)
	assertAlreadyExistsErr(t, err)

	nr = testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["body"] = patchBody()
	nr.Params["updateMask.fieldPaths"] = ""
	nr.Params["currentDocument.updateTime"] = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	_, err = p.DocumentsPatch(ctx, nr)
	assertPreconditionErr(t, err)
}

// TestDocumentsPatchEmptyMaskCreatesEmptyDocument: PATCH is an upsert, so an
// empty mask on a missing document still creates it — with no fields, because
// the mask discards the body (the same shape buildUpdate's transform-only path
// produces).
func TestDocumentsPatchEmptyMaskCreatesEmptyDocument(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/NOPE"
	nr.Params["body"] = patchBody()
	nr.Params["updateMask.fieldPaths"] = ""
	resp, err := p.DocumentsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch empty mask on missing doc: %v", err)
	}
	fields, _ := resp.Data["fields"].(map[string]any)
	if len(fields) != 0 {
		t.Errorf("created document carries fields: %+v", fields)
	}
	stored, err := p.store.GetDocument(ctx, "projects/proj/databases/(default)/documents/cities/NOPE")
	if err != nil {
		t.Fatalf("missing document was not created: %v", err)
	}
	if len(stored.Fields) != 0 {
		t.Errorf("stored fields = %+v, want none", stored.Fields)
	}
}

// TestDocumentsPatchOmittedMaskReplaces is the contrast case: with no mask
// query parameter at all the body fields replace the document's fields, and
// update_time rotates.
func TestDocumentsPatchOmittedMaskReplaces(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	docName, seeded := seedPatchDoc(t, p)

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["body"] = patchBody()
	resp, err := p.DocumentsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch without mask: %v", err)
	}
	fields, _ := resp.Data["fields"].(map[string]any)
	if len(fields) != 1 {
		t.Errorf("replace-form fields = %+v, want only a", fields)
	}
	if _, ok := fields["b"]; ok {
		t.Errorf("replace-form kept b: %+v", fields)
	}
	stored, err := p.store.GetDocument(ctx, docName)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.Fields["a"].IntegerValue == nil || *stored.Fields["a"].IntegerValue != 99 {
		t.Errorf("stored a = %+v, want 99", stored.Fields["a"])
	}
	if !stored.UpdateTime.After(seeded) {
		t.Errorf("updateTime did not rotate: %v", stored.UpdateTime)
	}
}

// TestDocumentsPatchResponseMaskIsNotWriteMask: `mask.fieldPaths` selects the
// fields the reply is projected to (Firestore's DocumentMask.mask, "fields to
// return"); it must never restrict — or blank out — the write.
func TestDocumentsPatchResponseMaskIsNotWriteMask(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	docName, seeded := seedPatchDoc(t, p)

	// The response mask asks for `a` only; the write is the omitted-mask form,
	// so both body fields are written and the reply carries just `a`. Had the
	// response mask been (wrongly) used as the write mask, `b` would survive.
	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents/cities/SF"
	nr.Params["body"] = map[string]any{
		"fields": map[string]any{
			"a": map[string]any{"integerValue": "99"},
			"c": map[string]any{"integerValue": "7"},
		},
	}
	nr.Params["mask.fieldPaths"] = "a"
	resp, err := p.DocumentsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch with response mask: %v", err)
	}
	fields, _ := resp.Data["fields"].(map[string]any)
	if len(fields) != 1 {
		t.Errorf("response fields = %+v, want only a", fields)
	}
	if _, ok := fields["a"]; !ok {
		t.Errorf("response mask dropped a: %+v", fields)
	}
	stored, err := p.store.GetDocument(ctx, docName)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.Fields["a"].IntegerValue == nil || *stored.Fields["a"].IntegerValue != 99 {
		t.Errorf("stored a = %+v, want the written 99 (mask is a response projection)", stored.Fields["a"])
	}
	if stored.Fields["c"].IntegerValue == nil || *stored.Fields["c"].IntegerValue != 7 {
		t.Errorf("stored c = %+v, want the written 7", stored.Fields["c"])
	}
	if _, ok := stored.Fields["b"]; ok {
		t.Errorf("stored document kept b = %+v; the omitted write mask replaces", stored.Fields["b"])
	}
	if !stored.UpdateTime.After(seeded) {
		t.Errorf("updateTime did not rotate: %v", stored.UpdateTime)
	}
}

// TestUpdateMaskFromQuery covers the presence matrix for the write mask,
// including a request whose query lives only on Raw (no decoded Params) and the
// response mask, which must not be mistaken for a write mask.
func TestUpdateMaskFromQuery(t *testing.T) {
	base := func() *model.NormalizedRequest {
		return &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{}}
	}
	if m := updateMaskFromQuery(base()); m != nil {
		t.Errorf("omitted mask = %+v, want nil", m)
	}
	nr := base()
	nr.Params["updateMask.fieldPaths"] = "a,b"
	if m := updateMaskFromQuery(nr); m == nil || len(m.Paths) != 2 {
		t.Errorf("updateMask.fieldPaths=a,b -> %+v", m)
	}
	nr = base()
	nr.Params["updateMask.fieldPaths"] = ""
	if m := updateMaskFromQuery(nr); m == nil || len(m.Paths) != 0 {
		t.Errorf("updateMask.fieldPaths= (empty) -> %+v, want present-but-empty", m)
	}
	// The response mask is not a write mask.
	nr = base()
	nr.Params["mask.fieldPaths"] = "a"
	if m := updateMaskFromQuery(nr); m != nil {
		t.Errorf("mask.fieldPaths=a produced a write mask %+v", m)
	}
	nr = base()
	nr.Params["mask.fieldPaths"] = ""
	if m := updateMaskFromQuery(nr); m != nil {
		t.Errorf("mask.fieldPaths= produced a write mask %+v", m)
	}
	nr = &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{"name": "databases/(default)/documents/cities/SF"},
		Raw: httptest.NewRequest("PATCH", "/v1/projects/proj/databases/(default)/documents/cities/SF?updateMask.fieldPaths=", nil)}
	if m := updateMaskFromQuery(nr); m == nil || len(m.Paths) != 0 {
		t.Errorf("raw updateMask.fieldPaths= -> %+v, want present-but-empty", m)
	}
	nr = &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{},
		Raw: httptest.NewRequest("PATCH", "/v1/projects/proj/databases/(default)/documents/cities/SF?updateMask.fieldPaths=a", nil)}
	if m := updateMaskFromQuery(nr); m == nil || len(m.Paths) != 1 || m.Paths[0] != "a" {
		t.Errorf("raw updateMask.fieldPaths=a -> %+v", m)
	}
}
