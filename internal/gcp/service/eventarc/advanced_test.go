package eventarc

import (
	"context"
	"encoding/json"
	"testing"

	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/store"
)

func newAdvancedService() *Service {
	return NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())
}

// TestAdvancedCRUD round-trips every collection kind: create → get → list →
// masked update → delete, asserting the output-only fields and the etag
// lifecycle.
func TestAdvancedCRUD(t *testing.T) {
	ctx := context.Background()
	for _, k := range AdvancedKinds {
		t.Run(k.Proto, func(t *testing.T) {
			s := newAdvancedService()
			cfg := json.RawMessage(`{"displayName":"demo"}`)
			// Seed the reference each family validates against.
			switch k {
			case EnrollmentKind:
				if _, _, err := s.CreateAdvanced(ctx, "proj", MessageBusKind, "us-central1", "mb", nil, false); err != nil {
					t.Fatalf("seed message bus: %v", err)
				}
				cfg = json.RawMessage(`{"displayName":"demo","messageBus":"projects/proj/locations/us-central1/messageBuses/mb"}`)
			case ChannelConnectionKind:
				if err := s.store.CreateChannel(ctx, "proj", "us-central1", eventarcstore.Channel{Location: "us-central1", Name: "c1"}); err != nil {
					t.Fatalf("seed channel: %v", err)
				}
				cfg = json.RawMessage(`{"channel":"projects/proj/locations/us-central1/channels/c1"}`)
			}
			rec, _, err := s.CreateAdvanced(ctx, "proj", k, "us-central1", "demo-1", cfg, false)
			if err != nil {
				t.Fatalf("CreateAdvanced: %v", err)
			}
			if rec.UID == "" {
				t.Errorf("uid is empty")
			}
			if k.HasActivationToken && rec.ActivationToken == "" {
				t.Errorf("activationToken is empty")
			}
			wantName := k.Name("proj", "us-central1", "demo-1")
			if got := AdvancedJSON("proj", k, rec)["name"]; got != wantName {
				t.Errorf("name = %v, want %v", got, wantName)
			}
			// Duplicate create is AlreadyExists.
			if _, _, err := s.CreateAdvanced(ctx, "proj", k, "us-central1", "demo-1", cfg, false); err == nil {
				t.Errorf("duplicate create: want error")
			}
			got, err := s.GetAdvanced(ctx, "proj", k, "us-central1", "demo-1")
			if err != nil || got.Etag != rec.Etag {
				t.Fatalf("GetAdvanced = %+v, %v", got, err)
			}
			page, _, err := s.ListAdvanced(ctx, "proj", k, "us-central1", "", "", 0, "")
			if err != nil || len(page) != 1 {
				t.Fatalf("ListAdvanced = %d items, %v", len(page), err)
			}
			// A stale etag is rejected.
			if _, _, err := s.UpdateAdvanced(ctx, "proj", k, "us-central1", "demo-1",
				json.RawMessage(`{"labels":{"a":"b"}}`), "labels", "stale", false); err == nil {
				t.Errorf("stale etag update: want error")
			}
			updated, _, err := s.UpdateAdvanced(ctx, "proj", k, "us-central1", "demo-1",
				json.RawMessage(`{"labels":{"a":"b"}}`), "labels", rec.Etag, false)
			if err != nil {
				t.Fatalf("UpdateAdvanced: %v", err)
			}
			if updated.Labels["a"] != "b" {
				t.Errorf("labels = %v, want a=b", updated.Labels)
			}
			if updated.Etag == rec.Etag {
				t.Errorf("etag did not change after update")
			}
			if _, _, err := s.DeleteAdvanced(ctx, "proj", k, "us-central1", "demo-1", "", false); err != nil {
				t.Fatalf("DeleteAdvanced: %v", err)
			}
			if _, err := s.GetAdvanced(ctx, "proj", k, "us-central1", "demo-1"); !isAdvancedNotFound(err) {
				t.Errorf("Get after delete = %v, want NotFound", err)
			}
		})
	}
}

// TestGoogleChannelConfigSingleton verifies the per-location singleton: a fresh
// location serves an empty default, and an update persists under the same name.
func TestGoogleChannelConfigSingleton(t *testing.T) {
	ctx := context.Background()
	s := newAdvancedService()
	rec, err := s.GetAdvanced(ctx, "proj", GoogleChannelConfigKind, "us-central1", "")
	if err != nil {
		t.Fatalf("Get default: %v", err)
	}
	if rec.Config != nil {
		t.Errorf("fresh config body = %s, want empty", rec.Config)
	}
	updated, _, err := s.UpdateAdvanced(ctx, "proj", GoogleChannelConfigKind, "us-central1", "",
		json.RawMessage(`{"cryptoKeyName":"projects/p/locations/us-central1/keyRings/r/cryptoKeys/k"}`),
		"crypto_key_name", "", false)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := AdvancedJSON("proj", GoogleChannelConfigKind, updated)["cryptoKeyName"]; got == nil || got == "" {
		t.Errorf("cryptoKeyName not stored: %v", got)
	}
	again, err := s.GetAdvanced(ctx, "proj", GoogleChannelConfigKind, "us-central1", "")
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if again.Etag == "" {
		t.Errorf("singleton etag is empty after update")
	}
}

// TestAdvancedEnrollmentRequiresMessageBus verifies the enrollment → message bus
// reference contract, and the message-bus listEnrollments custom method.
func TestAdvancedEnrollmentRequiresMessageBus(t *testing.T) {
	ctx := context.Background()
	s := newAdvancedService()
	if _, _, err := s.CreateAdvanced(ctx, "proj", EnrollmentKind, "us-central1", "e1",
		json.RawMessage(`{"messageBus":"projects/proj/locations/us-central1/messageBuses/mb1"}`), false); err == nil {
		t.Fatalf("enrollment without an existing message bus: want NotFound")
	}
	if _, _, err := s.CreateAdvanced(ctx, "proj", EnrollmentKind, "us-central1", "e1", json.RawMessage(`{}`), false); err == nil {
		t.Fatalf("enrollment without messageBus: want InvalidArgument")
	}
	if _, _, err := s.CreateAdvanced(ctx, "proj", MessageBusKind, "us-central1", "mb1", nil, false); err != nil {
		t.Fatalf("create message bus: %v", err)
	}
	if _, _, err := s.CreateAdvanced(ctx, "proj", EnrollmentKind, "us-central1", "e1",
		json.RawMessage(`{"messageBus":"projects/proj/locations/us-central1/messageBuses/mb1"}`), false); err != nil {
		t.Fatalf("create enrollment: %v", err)
	}
	names, _, err := s.ListMessageBusEnrollments(ctx, "proj", "us-central1", "mb1", 0, "")
	if err != nil {
		t.Fatalf("ListMessageBusEnrollments: %v", err)
	}
	if len(names) != 1 || names[0] != EnrollmentName("proj", "us-central1", "e1") {
		t.Errorf("enrollments = %v", names)
	}
}

// TestChannelConnectionRequiresChannel verifies the channel-connection → channel
// reference contract.
func TestChannelConnectionRequiresChannel(t *testing.T) {
	ctx := context.Background()
	s := newAdvancedService()
	if _, _, err := s.CreateAdvanced(ctx, "proj", ChannelConnectionKind, "us-central1", "cc1",
		json.RawMessage(`{"channel":"projects/proj/locations/us-central1/channels/c1"}`), false); err == nil {
		t.Fatalf("channel connection with a missing channel: want NotFound")
	}
	if err := s.store.CreateChannel(ctx, "proj", "us-central1", eventarcstore.Channel{
		Location: "us-central1", Name: "c1", UID: "u", Etag: "e",
	}); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	if _, _, err := s.CreateAdvanced(ctx, "proj", ChannelConnectionKind, "us-central1", "cc1",
		json.RawMessage(`{"channel":"projects/proj/locations/us-central1/channels/c1"}`), false); err != nil {
		t.Fatalf("create channel connection: %v", err)
	}
}

// TestAdvancedMaskPathClassification pins the update_mask error classes for the
// advanced surface: a path that names no field of the kind is a 400
// InvalidArgument (AIP-134/AIP-161); an output-only field the emulator owns is
// a 501 UnsupportedOperation; a writable field still merges.
func TestAdvancedMaskPathClassification(t *testing.T) {
	ctx := context.Background()
	s := newAdvancedService()
	if _, _, err := s.CreateAdvanced(ctx, "proj", MessageBusKind, "us-central1", "mb1",
		json.RawMessage(`{"displayName":"d"}`), false); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, _, err := s.UpdateAdvanced(ctx, "proj", MessageBusKind, "us-central1", "mb1",
		json.RawMessage(`{}`), "bogusField", "", false); maskStatus(err) != 400 {
		t.Fatalf("unknown path err = %v, want 400", err)
	}
	if _, _, err := s.UpdateAdvanced(ctx, "proj", MessageBusKind, "us-central1", "mb1",
		json.RawMessage(`{}`), "createTime", "", false); maskStatus(err) != 501 {
		t.Fatalf("output-only path err = %v, want 501", err)
	}
	if _, _, err := s.UpdateAdvanced(ctx, "proj", MessageBusKind, "us-central1", "mb1",
		json.RawMessage(`{"displayName":"e"}`), "displayName", "", false); err != nil {
		t.Fatalf("writable path: %v", err)
	}
}
