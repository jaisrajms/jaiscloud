package functions

import (
	"bytes"
	"context"
	"testing"
)

// followupFieldsFunction is a stored Function carrying every field the
// functions field follow-up wave added (FC2-11, FC3-1, FC3-2): the CMEK key, the
// runtime service account, the v2 storageSource upload URL and the event filter
// list. The memory, Postgres and snapshot backends are compared field-for-field.
func followupFieldsFunction() Function {
	return Function{
		ID: "fu", Location: "us-central1", Runtime: "nodejs20", EntryPoint: "hello",
		Status:                 "ACTIVE",
		KmsKeyName:             "projects/p/locations/global/keyRings/r/cryptoKeys/k",
		ServiceAccountEmail:    "sa@p.iam.gserviceaccount.com",
		SourceStorageUploadURL: "https://upload.example/x.zip",
		EventTrigger: &EventTrigger{
			EventType:   "google.cloud.storage.object.v1.finalized",
			Resource:    "projects/_/buckets/b",
			RetryPolicy: "RETRY_POLICY_RETRY",
			EventFilters: []EventFilter{
				{Attribute: "bucket", Value: "b"},
				{Attribute: "type", Value: "v", Operator: "match-path-pattern"},
			},
		},
	}
}

// assertFollowupFields fails when a read-back does not carry every follow-up
// field.
func assertFollowupFields(t *testing.T, f Function) {
	t.Helper()
	if f.KmsKeyName != "projects/p/locations/global/keyRings/r/cryptoKeys/k" {
		t.Fatalf("kmsKeyName = %q", f.KmsKeyName)
	}
	if f.ServiceAccountEmail != "sa@p.iam.gserviceaccount.com" {
		t.Fatalf("serviceAccountEmail = %q", f.ServiceAccountEmail)
	}
	if f.SourceStorageUploadURL != "https://upload.example/x.zip" {
		t.Fatalf("sourceStorageUploadUrl = %q", f.SourceStorageUploadURL)
	}
	if f.EventTrigger == nil || len(f.EventTrigger.EventFilters) != 2 {
		t.Fatalf("eventTrigger = %+v", f.EventTrigger)
	}
	if got := f.EventTrigger.EventFilters[1]; got != (EventFilter{Attribute: "type", Value: "v", Operator: "match-path-pattern"}) {
		t.Fatalf("eventFilters = %+v", f.EventTrigger.EventFilters)
	}
}

func TestMemoryStoreFollowupFields(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateFunction(ctx, "p", "us-central1", "fu", followupFieldsFunction()); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.GetFunction(ctx, "p", "us-central1", "fu")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertFollowupFields(t, got)
}

func TestMemoryStoreFollowupFieldsSnapshot(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateFunction(ctx, "p", "us-central1", "fu", followupFieldsFunction()); err != nil {
		t.Fatalf("create: %v", err)
	}
	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := NewMemoryStore()
	if err := restored.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := restored.GetFunction(ctx, "p", "us-central1", "fu")
	if err != nil {
		t.Fatalf("get restored: %v", err)
	}
	assertFollowupFields(t, got)
}
