package functions

import (
	"context"
	"fmt"
	"testing"

	"jaiscloud/internal/gcp/eventing"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

// failingTriggerProvisioner models the Eventarc provisioner refusing a
// reference: EnsureFunctionTrigger resolves the referenced Pub/Sub topic and
// answers NotFound when it does not exist.
type failingTriggerProvisioner struct {
	calls int
}

func (p *failingTriggerProvisioner) EnsureFunctionTrigger(ctx context.Context, spec eventing.FunctionTriggerSpec) (string, string, error) {
	p.calls++
	return "", "", fmt.Errorf("topic %s not found", spec.Resource)
}

func (p *failingTriggerProvisioner) DeleteFunctionTrigger(ctx context.Context, project, location, functionID string) error {
	return nil
}

// TestCreateFunctionEventTriggerReferenceIsNotValidated pins the FC4 reference
// gap for functions/Function.event_trigger.pubsub_topic: the backing trigger
// provisioner resolves the topic (a missing one is NotFound), but the create
// path treats the failure as non-fatal, so a function naming a nonexistent topic
// is created with an empty output-only Trigger. Real GCP rejects the create.
// The reference-coverage inventory records this as `unvalidated` (FC4-n) until
// the create path propagates the provisioner's NotFound as INVALID_ARGUMENT.
func TestCreateFunctionEventTriggerReferenceIsNotValidated(t *testing.T) {
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	fake := &failingTriggerProvisioner{}
	s.SetTriggerProvisioner(fake)

	in := FunctionInputFromMap(map[string]any{
		"buildConfig": map[string]any{"runtime": "nodejs20"},
		"eventTrigger": map[string]any{
			"eventType":   "google.cloud.pubsub.topic.v1.messagePublished",
			"pubsubTopic": "projects/proj/topics/does-not-exist",
		},
	}, V2)

	fn, _, err := s.CreateFunction(context.Background(), "proj", "us-central1", "f1", in, V2)
	if err != nil {
		t.Fatalf("CreateFunction with a nonexistent topic = %v, want the documented non-fatal behaviour", err)
	}
	if fake.calls == 0 {
		t.Fatal("the provisioner was never consulted: the reference is not even resolved")
	}
	if fn.EventTrigger == nil || fn.EventTrigger.Trigger != "" {
		t.Fatalf("event trigger = %+v, want the stored reference with an empty materialized trigger", fn.EventTrigger)
	}
	if got := fn.EventTrigger.Resource; got != "projects/proj/topics/does-not-exist" {
		t.Fatalf("stored reference = %q, want the verbatim topic", got)
	}
}
