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

// TestValidateFiltersSource covers EV10: a filter attribute the trigger's source
// does not define is rejected (a bucket/object filter without a Cloud Storage
// type, a topic filter on a Cloud Storage trigger), while attributes valid for
// the source — and attributes of an unknown provider — are accepted.
func TestValidateFiltersSource(t *testing.T) {
	const (
		pubsubType  = "google.cloud.pubsub.topic.v1.messagePublished"
		storageType = "google.cloud.storage.object.v1.finalized"
		customType  = "com.example.custom"
	)
	cases := []struct {
		name    string
		filters []any
		wantErr bool
	}{
		{"pubsub bucket rejected", []any{eventFilter("type", pubsubType), eventFilter("bucket", "b")}, true},
		{"pubsub object rejected", []any{eventFilter("type", pubsubType), eventFilter("object", "o")}, true},
		{"custom bucket rejected", []any{eventFilter("type", customType), eventFilter("bucket", "b")}, true},
		{"custom object rejected", []any{eventFilter("type", customType), eventFilter("object", "o")}, true},
		{"storage topic rejected", []any{eventFilter("type", storageType), eventFilter("bucket", "b"), eventFilter("topic", "t")}, true},
		{"object.change topic rejected", []any{eventFilter("type", "providers/cloud.storage/eventTypes/object.change"), eventFilter("bucket", "b"), eventFilter("topic", "t")}, true},
		{"storage bucket and object accepted", []any{eventFilter("type", storageType), eventFilter("bucket", "b"), eventFilter("object", "o")}, false},
		{"pubsub topic accepted", []any{eventFilter("type", pubsubType), eventFilter("topic", "t")}, false},
		{"custom topic accepted", []any{eventFilter("type", customType), eventFilter("topic", "t")}, false},
		{"empty mismatched values ignored", []any{eventFilter("type", pubsubType), eventFilter("bucket", "")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateFilters(map[string]any{"eventFilters": tc.filters})
			if tc.wantErr && err == nil {
				t.Fatal("validateFilters accepted a cross-source filter set")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validateFilters rejected a valid filter set: %v", err)
			}
		})
	}
}

// TestCreateTriggerRejectsCrossSourceFilters covers the create/validateOnly path:
// a cross-source filter is rejected with InvalidArgument/400 and never persisted.
func TestCreateTriggerRejectsCrossSourceFilters(t *testing.T) {
	ctx := context.Background()
	svc := NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())

	cases := []struct {
		name string
		body json.RawMessage
	}{
		{
			"pubsub bucket",
			json.RawMessage(`{
				"destination":{"httpEndpoint":{"uri":"https://example.com/events"}},
				"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"},
					{"attribute":"bucket","value":"b"}]}`),
		},
		{
			"storage topic",
			json.RawMessage(`{
				"destination":{"httpEndpoint":{"uri":"https://example.com/events"}},
				"eventFilters":[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},
					{"attribute":"bucket","value":"b"},{"attribute":"topic","value":"t"}]}`),
		},
	}
	var pe *model.ProviderError
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "t1", tc.body, false); !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
				t.Fatalf("CreateTrigger error = %v, want InvalidArgument/400", err)
			}
			if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "t1", tc.body, true); err == nil {
				t.Fatal("validateOnly accepted a cross-source filter set")
			}
			if _, err := svc.GetTrigger(ctx, "proj", "us-central1", "t1"); err == nil {
				t.Fatal("invalid trigger was persisted")
			}
		})
	}
}

// TestPatchTriggerRejectsCrossSourceFilters covers the update path: a PATCH that
// introduces a cross-source attribute is rejected, while a labels-only PATCH
// leaves the valid filters untouched.
func TestPatchTriggerRejectsCrossSourceFilters(t *testing.T) {
	ctx := context.Background()
	body := json.RawMessage(`{
		"destination":{"httpEndpoint":{"uri":"https://example.com/events"}},
		"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`)
	svc := NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "t1", body, false); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	withBucket := json.RawMessage(`{"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"},{"attribute":"bucket","value":"b"}]}`)
	var pe *model.ProviderError
	if _, _, err := svc.UpdateTrigger(ctx, "proj", "us-central1", "t1", withBucket, "eventFilters", "", false); !errors.As(err, &pe) || pe.Code != "InvalidArgument" {
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
