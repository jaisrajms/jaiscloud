package firestore

import (
	"context"
	"testing"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/gcp/timestamp"
)

// commitEmptyMask issues a documents:commit whose single write is an update
// with `update_mask: {}` (present but empty) and the given transforms.
func commitEmptyMask(t *testing.T, p *Provider, name string, fields map[string]any, transforms []any) map[string]any {
	t.Helper()
	write := map[string]any{
		"update":     map[string]any{"name": name, "fields": fields},
		"updateMask": map[string]any{},
	}
	if len(transforms) > 0 {
		write["updateTransforms"] = transforms
	}
	nr := testNR()
	nr.Params["body"] = map[string]any{"writes": []any{write}}
	resp, err := p.Commit(context.Background(), nr)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	results, _ := resp.Data["writeResults"].([]any)
	if len(results) != 1 {
		t.Fatalf("writeResults = %+v, want 1", resp.Data["writeResults"])
	}
	res, _ := results[0].(map[string]any)
	return res
}

// TestCommitEmptyMaskNoop pins real Firestore's answer to a commit whose only
// write carries `update_mask: {}` and no transforms: the write is AIP-134's
// no-op. The stored document and its update_time are untouched, the write
// result echoes the stored update_time, and listeners see no change. Verified
// against real GCP: `documents:commit` with an empty update mask returns the
// unchanged updateTime, and an open Listen target reports only NO_CHANGE (a
// control write with a real field emits a DocumentChange).
func TestCommitEmptyMaskNoop(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	docName, seeded := seedPatchDoc(t, p)

	res := commitEmptyMask(t, p, docName, map[string]any{
		"a": map[string]any{"integerValue": "99"},
	}, nil)
	if res["updateTime"] != timestamp.Canonical(seeded) {
		t.Errorf("writeResult.updateTime = %v, want the stored %v", res["updateTime"], timestamp.Canonical(seeded))
	}
	if _, ok := res["transformResults"]; ok {
		t.Errorf("no-op writeResult carries transformResults: %+v", res)
	}

	stored, err := p.store.GetDocument(ctx, docName)
	if err != nil {
		t.Fatalf("get after no-op: %v", err)
	}
	if len(stored.Fields) != 2 {
		t.Errorf("no-op wrote fields: %+v", stored.Fields)
	}
	if v, ok := stored.Fields["a"].AsInt64(); !ok || v != 1 {
		t.Errorf("field a = %+v, want the stored 1 (the empty mask ignores the body)", stored.Fields["a"])
	}
	if !stored.UpdateTime.Equal(seeded) {
		t.Errorf("stored updateTime = %v, want %v", stored.UpdateTime, seeded)
	}
	if seq := p.Service.CurrentSeq(); seq != 0 {
		t.Errorf("no-op published change seq %d; listeners must not see it", seq)
	}
}

// TestCommitEmptyMaskWithTransformIsAChange is the contrast case: with a field
// transform the empty mask is the documented transform-only update, so it
// rotates update_time and fans out the change.
func TestCommitEmptyMaskWithTransformIsAChange(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	docName, seeded := seedPatchDoc(t, p)

	res := commitEmptyMask(t, p, docName, nil, []any{
		map[string]any{"fieldPath": "a", "increment": map[string]any{"integerValue": "5"}},
	})
	if res["updateTime"] == timestamp.Canonical(seeded) {
		t.Errorf("transform-only update did not rotate update_time (%v)", res["updateTime"])
	}
	tr, _ := res["transformResults"].([]any)
	if len(tr) != 1 {
		t.Errorf("transformResults = %+v, want one", res["transformResults"])
	}
	stored, err := p.store.GetDocument(ctx, docName)
	if err != nil {
		t.Fatalf("get after transform: %v", err)
	}
	if v, ok := stored.Fields["a"].AsInt64(); !ok || v != 6 {
		t.Errorf("field a = %+v, want 6", stored.Fields["a"])
	}
	if !stored.UpdateTime.After(seeded) {
		t.Errorf("stored updateTime did not rotate: %v", stored.UpdateTime)
	}
	if seq := p.Service.CurrentSeq(); seq != 1 {
		t.Errorf("transform-only update published seq %d, want 1", seq)
	}
}

// TestCommitEmptyMaskCreatesFieldlessOnMissing pins the upsert half: an empty
// mask on a missing document still creates it, with no fields (real GCP returns
// a created document whose createTime is the commit time). That is a real
// create, so it fans out.
func TestCommitEmptyMaskCreatesFieldlessOnMissing(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/NOPE"

	res := commitEmptyMask(t, p, name, map[string]any{
		"a": map[string]any{"integerValue": "99"},
	}, nil)
	stored, err := p.store.GetDocument(ctx, name)
	if err != nil {
		t.Fatalf("missing document was not created: %v", err)
	}
	if len(stored.Fields) != 0 {
		t.Errorf("created document carries fields: %+v", stored.Fields)
	}
	if stored.CreateTime.IsZero() || !stored.CreateTime.Equal(stored.UpdateTime) {
		t.Errorf("createTime/updateTime = %v/%v, want the commit time", stored.CreateTime, stored.UpdateTime)
	}
	if res["updateTime"] != timestamp.Canonical(stored.UpdateTime) {
		t.Errorf("writeResult.updateTime = %v, want %v", res["updateTime"], timestamp.Canonical(stored.UpdateTime))
	}
	if seq := p.Service.CurrentSeq(); seq != 1 {
		t.Errorf("create published seq %d, want 1", seq)
	}
}

// TestBatchWriteFansOutChangeEvents pins the listener half of documents:batchWrite:
// every write real Firestore applies is reported to Listen subscribers
// (verified against real GCP), so a batchWrite update/delete fans out here, and
// the empty-mask no-op stays silent.
func TestBatchWriteFansOutChangeEvents(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	docName, seeded := seedPatchDoc(t, p)

	// A real field update is a change: seq advances and the event carries it.
	statuses, firstResults, err := p.Service.BatchWrite(ctx, []*writeWire{{
		Update: &documentWire{Name: docName, Fields: map[string]*firestorestore.Value{
			"a": firestorestore.IntVal(7),
		}},
	}})
	if err != nil {
		t.Fatalf("batchWrite update: %v", err)
	}
	if status := statuses[0].(map[string]any); status["code"].(int64) != 0 {
		t.Fatalf("batchWrite update status = %+v", status)
	}
	if seq := p.Service.CurrentSeq(); seq != 1 {
		t.Errorf("batchWrite update published seq %d, want 1", seq)
	}
	first, _ := firstResults[0].(map[string]any)
	updateTime, _ := first["updateTime"].(string)
	if updateTime == "" || updateTime == timestamp.Canonical(seeded) {
		t.Fatalf("real batchWrite update did not rotate updateTime: %+v", first)
	}

	// The empty-mask no-op is not a change: no new seq, no update_time move.
	statuses, results, err := p.Service.BatchWrite(ctx, []*writeWire{{
		Update:     &documentWire{Name: docName},
		UpdateMask: &documentMaskWire{},
	}})
	if err != nil {
		t.Fatalf("batchWrite no-op: %v", err)
	}
	if status := statuses[0].(map[string]any); status["code"].(int64) != 0 {
		t.Fatalf("batchWrite no-op status = %+v", status)
	}
	res, _ := results[0].(map[string]any)
	if res["updateTime"] != updateTime {
		t.Errorf("no-op write result updateTime = %v, want the stored %v", res["updateTime"], updateTime)
	}
	if seq := p.Service.CurrentSeq(); seq != 1 {
		t.Errorf("empty-mask batchWrite published seq %d, want the unchanged 1", seq)
	}

	// A delete is a change too.
	statuses, _, err = p.Service.BatchWrite(ctx, []*writeWire{{Delete: docName}})
	if err != nil {
		t.Fatalf("batchWrite delete: %v", err)
	}
	if status := statuses[0].(map[string]any); status["code"].(int64) != 0 {
		t.Fatalf("batchWrite delete status = %+v", status)
	}
	if seq := p.Service.CurrentSeq(); seq != 2 {
		t.Errorf("batchWrite delete published seq %d, want 2", seq)
	}
}

// TestCommitEmptyMaskWithPreconditionStillValidates: the no-op is not an escape
// hatch from currentDocument preconditions.
func TestCommitEmptyMaskWithPreconditionStillValidates(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	docName, _ := seedPatchDoc(t, p)

	nr := testNR()
	nr.Params["body"] = map[string]any{"writes": []any{map[string]any{
		"update":          map[string]any{"name": docName},
		"updateMask":      map[string]any{},
		"currentDocument": map[string]any{"exists": false},
	}}}
	if _, err := p.Commit(ctx, nr); err == nil {
		t.Fatal("empty-mask no-op bypassed currentDocument.exists=false")
	} else {
		assertAlreadyExistsErr(t, err)
	}
}
