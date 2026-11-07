package functions

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/eventing"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// rejectingTriggerProvisioner models the Eventarc provisioner resolving a
// function's declared Pub/Sub topic: ValidateFunctionTriggerReference answers
// INVALID_ARGUMENT when the topic does not exist, and EnsureFunctionTrigger
// must never run for such a create/update.
type rejectingTriggerProvisioner struct {
	validated int
	ensured   int
}

func (p *rejectingTriggerProvisioner) ValidateFunctionTriggerReference(_ context.Context, spec eventing.FunctionTriggerSpec) error {
	p.validated++
	return model.NewProviderError("InvalidArgument", "event trigger topic not found: "+spec.Resource, 400)
}

func (p *rejectingTriggerProvisioner) EnsureFunctionTrigger(context.Context, eventing.FunctionTriggerSpec) (string, string, error) {
	p.ensured++
	return "", "", nil
}

func (p *rejectingTriggerProvisioner) DeleteFunctionTrigger(context.Context, string, string, string) error {
	return nil
}

// TestCreateFunctionEventTriggerTopicMustExist covers FC4-3: the function's
// eventTrigger.pubsubTopic is a google.api.resource_reference, so real Cloud
// Functions rejects a create naming a topic that does not exist. The create
// path resolves the reference through the Eventarc provisioner before it
// persists anything and answers INVALID_ARGUMENT.
func TestCreateFunctionEventTriggerTopicMustExist(t *testing.T) {
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	fake := &rejectingTriggerProvisioner{}
	s.SetTriggerProvisioner(fake)

	in := FunctionInputFromMap(map[string]any{
		"buildConfig": map[string]any{"runtime": "nodejs20"},
		"eventTrigger": map[string]any{
			"eventType":   "google.cloud.pubsub.topic.v1.messagePublished",
			"pubsubTopic": "projects/proj/topics/does-not-exist",
		},
	}, V2)

	_, _, err := s.CreateFunction(context.Background(), "proj", "us-central1", "f1", in, V2)
	if err == nil {
		t.Fatal("CreateFunction with a nonexistent topic = nil, want INVALID_ARGUMENT")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
		t.Fatalf("CreateFunction error = %v, want HTTP 400 INVALID_ARGUMENT", err)
	}
	if fake.validated != 1 {
		t.Fatalf("reference validated %d times, want 1", fake.validated)
	}
	if fake.ensured != 0 {
		t.Fatalf("backing trigger provisioned %d times for a rejected reference", fake.ensured)
	}
	if _, gerr := s.GetFunction(context.Background(), "proj", "us-central1", "f1"); gerr == nil {
		t.Fatal("the rejected function was persisted")
	}
}

// TestUpdateFunctionEventTriggerTopicMustExist is the update half of FC4-3: an
// update that introduces a nonexistent pubsubTopic fails before the stored
// function changes, so the reference cannot be persisted by a PATCH.
func TestUpdateFunctionEventTriggerTopicMustExist(t *testing.T) {
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	fake := &rejectingTriggerProvisioner{}
	s.SetTriggerProvisioner(fake)

	base := FunctionInputFromMap(map[string]any{
		"buildConfig": map[string]any{"runtime": "nodejs20"},
	}, V2)
	if _, _, err := s.CreateFunction(context.Background(), "proj", "us-central1", "f1", base, V2); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}

	update := FunctionInputFromMap(map[string]any{
		"eventTrigger": map[string]any{
			"eventType":   "google.cloud.pubsub.topic.v1.messagePublished",
			"pubsubTopic": "projects/proj/topics/does-not-exist",
		},
	}, V2)
	_, _, err := s.UpdateFunction(context.Background(), "proj", "us-central1", "f1", update, nil, V2)
	if err == nil {
		t.Fatal("UpdateFunction with a nonexistent topic = nil, want INVALID_ARGUMENT")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
		t.Fatalf("UpdateFunction error = %v, want HTTP 400 INVALID_ARGUMENT", err)
	}
	stored, gerr := s.GetFunction(context.Background(), "proj", "us-central1", "f1")
	if gerr != nil {
		t.Fatalf("GetFunction: %v", gerr)
	}
	if stored.EventTrigger != nil {
		t.Fatalf("the rejected event trigger was persisted: %+v", stored.EventTrigger)
	}
}
