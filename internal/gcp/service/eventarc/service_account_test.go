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

// fakeServiceAccounts is a static ServiceAccountExister.
type fakeServiceAccounts struct{ existing map[string]bool }

func (f fakeServiceAccounts) ServiceAccountExists(_ context.Context, _, email string) (bool, error) {
	return f.existing[email], nil
}

func triggerWithServiceAccount(sa string) json.RawMessage {
	return json.RawMessage(`{"destination":{"httpEndpoint":{"uri":"https://example.com"}},` +
		`"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}],` +
		`"serviceAccount":"` + sa + `"}`)
}

// TestTriggerServiceAccountReference covers FC4-2: a Trigger's serviceAccount
// is validated when the trigger is written. The address must be a service
// account email, and when the IAM store is wired the account must exist
// (NOT_FOUND otherwise).
func TestTriggerServiceAccountReference(t *testing.T) {
	ctx := context.Background()
	svc := NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())
	svc.SetServiceAccountExister(fakeServiceAccounts{existing: map[string]bool{
		"sa@proj.iam.gserviceaccount.com": true,
	}})

	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "ok", triggerWithServiceAccount("sa@proj.iam.gserviceaccount.com"), false); err != nil {
		t.Fatalf("accept: %v", err)
	}

	_, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "missing", triggerWithServiceAccount("missing@proj.iam.gserviceaccount.com"), false)
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 404 {
		t.Fatalf("missing account = %v, want NOT_FOUND", err)
	}

	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "malformed", triggerWithServiceAccount("not-an-email"), false); err == nil {
		t.Fatal("malformed serviceAccount: create succeeded, want INVALID_ARGUMENT")
	} else if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
		t.Fatalf("malformed serviceAccount = %v, want INVALID_ARGUMENT", err)
	}
}
