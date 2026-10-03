package storage

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/gcp/eventing"
	"jaiscloud/internal/gcp/store/gcs"
)

// recordingStorageDispatcher captures events handed to the functions delivery
// engine.
type recordingStorageDispatcher struct{ events []eventing.Event }

func (r *recordingStorageDispatcher) DispatchEvent(_ context.Context, ev eventing.Event) {
	r.events = append(r.events, ev)
}

func TestPublishObjectEventDispatchesFunctionEvent(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	rec := &recordingStorageDispatcher{}
	p.SetFunctionDispatcher(rec)

	meta := gcs.ObjectMeta{Bucket: "bkt", Name: "obj.txt", Generation: "1"}
	p.publishObjectEvent(ctx, "proj", "bkt", "obj.txt", "OBJECT_FINALIZE", meta, time.Now(), nil)
	p.publishObjectEvent(ctx, "proj", "bkt", "obj.txt", "OBJECT_DELETE", meta, time.Now(), nil)
	// Archive events have no Cloud Functions analogue and must not dispatch.
	p.publishObjectEvent(ctx, "proj", "bkt", "obj.txt", "OBJECT_ARCHIVE", meta, time.Now(), nil)

	if len(rec.events) != 2 {
		t.Fatalf("expected finalize+delete dispatches, got %d: %+v", len(rec.events), rec.events)
	}
	fin := rec.events[0]
	if fin.EventType != eventing.TypeStorageFinalize || fin.Source != eventing.SourceStorage {
		t.Fatalf("finalize event = %+v", fin)
	}
	if fin.Resource != "projects/_/buckets/bkt" {
		t.Errorf("resource = %q", fin.Resource)
	}
	if fin.Attributes["bucketId"] != "bkt" || fin.Attributes["objectId"] != "obj.txt" {
		t.Errorf("attributes = %+v", fin.Attributes)
	}
	if rec.events[1].EventType != eventing.TypeStorageDelete {
		t.Errorf("second event = %+v", rec.events[1])
	}
}
