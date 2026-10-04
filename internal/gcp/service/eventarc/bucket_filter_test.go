package eventarc

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// eventFilter builds one eventFilters entry.
func eventFilter(attr, value string) map[string]any {
	return map[string]any{"attribute": attr, "value": value}
}

// TestValidateFiltersStorageBucket covers EV7: a trigger whose type filter names
// a Cloud Storage event must also declare a non-empty bucket filter, while
// Pub/Sub and unknown event types are unaffected.
func TestValidateFiltersStorageBucket(t *testing.T) {
	cases := []struct {
		name    string
		filters []any
		wantErr bool
	}{
		{"storage CloudEvent with bucket", []any{eventFilter("type", "google.cloud.storage.object.v1.finalized"), eventFilter("bucket", "b")}, false},
		{"storage v1 delete with bucket", []any{eventFilter("type", "google.storage.object.delete"), eventFilter("bucket", "b")}, false},
		{"storage archived requires bucket", []any{eventFilter("type", "google.cloud.storage.object.v1.archived")}, true},
		{"storage metadataUpdated requires bucket", []any{eventFilter("type", "google.cloud.storage.object.v1.metadataUpdated")}, true},
		{"object.change catch-all with bucket", []any{eventFilter("type", "providers/cloud.storage/eventTypes/object.change"), eventFilter("bucket", "b")}, false},
		{"storage without bucket", []any{eventFilter("type", "google.cloud.storage.object.v1.finalized")}, true},
		{"storage with empty bucket value", []any{eventFilter("type", "google.cloud.storage.object.v1.finalized"), eventFilter("bucket", "")}, true},
		{"storage v1 finalize without bucket", []any{eventFilter("type", "google.storage.object.finalize")}, true},
		{"pubsub without bucket", []any{eventFilter("type", "google.cloud.pubsub.topic.v1.messagePublished")}, false},
		{"unknown type without bucket", []any{eventFilter("type", "com.example.custom")}, false},
		{"missing type", []any{eventFilter("bucket", "b")}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateFilters(map[string]any{"eventFilters": tc.filters})
			if tc.wantErr && err == nil {
				t.Fatal("validateFilters accepted an invalid storage filter set")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validateFilters rejected a valid filter set: %v", err)
			}
		})
	}
}

// TestCreateStorageTriggerRequiresBucket covers the create/validateOnly path:
// a storage trigger without a bucket filter is rejected with InvalidArgument/400
// and never persisted, while the same trigger with a bucket is accepted.
func TestCreateStorageTriggerRequiresBucket(t *testing.T) {
	ctx := context.Background()
	invalid := json.RawMessage(`{
		"destination":{"httpEndpoint":{"uri":"https://example.com/events"}},
		"eventFilters":[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"}]}`)
	valid := json.RawMessage(`{
		"destination":{"httpEndpoint":{"uri":"https://example.com/events"}},
		"eventFilters":[
			{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},
			{"attribute":"bucket","value":"b"}]}`)

	svc := NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())
	var pe *model.ProviderError
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "t1", invalid, false); !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
		t.Fatalf("CreateTrigger error = %v, want InvalidArgument/400", err)
	}
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "t1", invalid, true); err == nil {
		t.Fatal("validateOnly accepted a storage trigger without a bucket filter")
	}
	if _, err := svc.GetTrigger(ctx, "proj", "us-central1", "t1"); err == nil {
		t.Fatal("invalid trigger was persisted")
	}
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "t1", valid, false); err != nil {
		t.Fatalf("CreateTrigger with bucket: %v", err)
	}
	if _, err := svc.GetTrigger(ctx, "proj", "us-central1", "t1"); err != nil {
		t.Fatalf("GetTrigger after create: %v", err)
	}
}

// TestPatchStorageTriggerRequiresBucket covers the update path: a PATCH that
// drops the bucket filter from a Cloud Storage trigger is rejected, while a
// labels-only PATCH that leaves the filters untouched still succeeds.
func TestPatchStorageTriggerRequiresBucket(t *testing.T) {
	ctx := context.Background()
	body := json.RawMessage(`{
		"destination":{"httpEndpoint":{"uri":"https://example.com/events"}},
		"eventFilters":[
			{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},
			{"attribute":"bucket","value":"b"}]}`)
	svc := NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "t1", body, false); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	withoutBucket := json.RawMessage(`{"eventFilters":[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"}]}`)
	var pe *model.ProviderError
	if _, _, err := svc.UpdateTrigger(ctx, "proj", "us-central1", "t1", withoutBucket, "eventFilters", "", false); !errors.As(err, &pe) || pe.Code != "InvalidArgument" {
		t.Fatalf("UpdateTrigger error = %v, want InvalidArgument", err)
	}

	stored, err := svc.GetTrigger(ctx, "proj", "us-central1", "t1")
	if err != nil {
		t.Fatalf("GetTrigger: %v", err)
	}
	if _, _, err := svc.UpdateTrigger(ctx, "proj", "us-central1", "t1", json.RawMessage(`{"labels":{"env":"prod"}}`), "labels", stored.Etag, false); err != nil {
		t.Fatalf("labels-only UpdateTrigger: %v", err)
	}
}
