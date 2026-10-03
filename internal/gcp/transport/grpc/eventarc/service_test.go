package eventarc

import (
	"context"
	"testing"

	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	core "jaiscloud/internal/gcp/service/eventarc"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/store"
)

func newGRPCService() *Service {
	return NewService(core.NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore()), "proj")
}

func newGRPCServiceWithWorkflow(t *testing.T) *Service {
	t.Helper()
	workflows := workflowsstore.NewMemoryStore()
	if err := workflows.CreateWorkflow(context.Background(), "proj", "us-central1", "w1", workflowsstore.Workflow{}); err != nil {
		t.Fatalf("seed workflow: %v", err)
	}
	return NewService(core.NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflows), "proj")
}

func triggerProto(name string) *eventarcpb.Trigger {
	return &eventarcpb.Trigger{
		Name: name,
		Destination: &eventarcpb.Destination{
			Descriptor_: &eventarcpb.Destination_Workflow{
				Workflow: "projects/proj/locations/us-central1/workflows/w1",
			},
		},
		EventFilters: []*eventarcpb.EventFilter{{Attribute: "type", Value: "google.cloud.workflows.workflow.v1.executed"}},
	}
}

func TestCreateTriggerLRO(t *testing.T) {
	ctx := context.Background()
	s := newGRPCServiceWithWorkflow(t)

	op, err := s.CreateTrigger(ctx, &eventarcpb.CreateTriggerRequest{
		Parent:    "projects/proj/locations/us-central1",
		TriggerId: "t1",
		Trigger:   triggerProto(""),
	})
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("operation not done: %+v", op)
	}
	var meta eventarcpb.OperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if meta.GetVerb() != "create" || meta.GetTarget() != "projects/proj/locations/us-central1/triggers/t1" {
		t.Errorf("metadata = %+v", &meta)
	}
	var trig eventarcpb.Trigger
	if err := op.GetResponse().UnmarshalTo(&trig); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if trig.GetName() != "projects/proj/locations/us-central1/triggers/t1" {
		t.Errorf("trigger name = %q", trig.GetName())
	}
	if trig.GetUid() == "" || trig.GetEtag() == "" {
		t.Errorf("output-only fields missing: %+v", &trig)
	}
	if trig.GetDestination().GetWorkflow() == "" {
		t.Errorf("destination not echoed: %+v", trig.GetDestination())
	}

	got, err := s.GetTrigger(ctx, &eventarcpb.GetTriggerRequest{Name: trig.GetName()})
	if err != nil {
		t.Fatalf("GetTrigger: %v", err)
	}
	if got.GetUid() != trig.GetUid() || got.GetUid() == "" {
		t.Errorf("uid not stable: %q vs %q", trig.GetUid(), got.GetUid())
	}

	list, err := s.ListTriggers(ctx, &eventarcpb.ListTriggersRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	if len(list.GetTriggers()) != 1 {
		t.Fatalf("triggers = %v", list.GetTriggers())
	}

	updOp, err := s.UpdateTrigger(ctx, &eventarcpb.UpdateTriggerRequest{
		Trigger:    &eventarcpb.Trigger{Name: trig.GetName(), Labels: map[string]string{"env": "prod"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateTrigger: %v", err)
	}
	var updated eventarcpb.Trigger
	if err := updOp.GetResponse().UnmarshalTo(&updated); err != nil {
		t.Fatalf("unmarshal update response: %v", err)
	}
	if updated.GetLabels()["env"] != "prod" {
		t.Errorf("labels = %v", updated.GetLabels())
	}

	delOp, err := s.DeleteTrigger(ctx, &eventarcpb.DeleteTriggerRequest{Name: trig.GetName()})
	if err != nil {
		t.Fatalf("DeleteTrigger: %v", err)
	}
	if !delOp.GetDone() {
		t.Error("delete op not done")
	}
	if _, err := s.GetTrigger(ctx, &eventarcpb.GetTriggerRequest{Name: trig.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetTrigger after delete: code = %v, want NotFound", status.Code(err))
	}
}

func TestChannelAndProvider(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()

	op, err := s.CreateChannel(ctx, &eventarcpb.CreateChannelRequest{
		Parent:    "projects/proj/locations/us-central1",
		ChannelId: "c1",
		Channel:   &eventarcpb.Channel{Provider: "projects/proj/locations/us-central1/providers/pubsub.googleapis.com"},
	})
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	var ch eventarcpb.Channel
	if err := op.GetResponse().UnmarshalTo(&ch); err != nil {
		t.Fatalf("unmarshal channel: %v", err)
	}
	if ch.GetName() != "projects/proj/locations/us-central1/channels/c1" {
		t.Errorf("channel name = %q", ch.GetName())
	}
	if ch.GetActivationToken() == "" {
		t.Errorf("activation token missing: %+v", &ch)
	}
	if ch.GetState() != eventarcpb.Channel_PENDING {
		t.Errorf("channel state = %v, want PENDING", ch.GetState())
	}

	got, err := s.GetChannel(ctx, &eventarcpb.GetChannelRequest{Name: ch.GetName()})
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if got.GetName() != ch.GetName() {
		t.Errorf("get channel = %+v", got)
	}

	prov, err := s.GetProvider(ctx, &eventarcpb.GetProviderRequest{
		Name: "projects/proj/locations/us-central1/providers/pubsub.googleapis.com",
	})
	if err != nil {
		t.Fatalf("GetProvider: %v", err)
	}
	if prov.GetDisplayName() == "" || len(prov.GetEventTypes()) == 0 {
		t.Errorf("provider = %+v", prov)
	}
	list, err := s.ListProviders(ctx, &eventarcpb.ListProvidersRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil {
		t.Fatalf("ListProviders: %v", err)
	}
	if len(list.GetProviders()) == 0 {
		t.Error("providers list empty")
	}

	if _, err := s.DeleteChannel(ctx, &eventarcpb.DeleteChannelRequest{Name: ch.GetName()}); err != nil {
		t.Fatalf("DeleteChannel: %v", err)
	}
	if _, err := s.GetChannel(ctx, &eventarcpb.GetChannelRequest{Name: ch.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetChannel after delete: code = %v, want NotFound", status.Code(err))
	}
}

func TestTriggerValidationMissingFilters(t *testing.T) {
	ctx := context.Background()
	s := newGRPCServiceWithWorkflow(t)
	_, err := s.CreateTrigger(ctx, &eventarcpb.CreateTriggerRequest{
		Parent:    "projects/proj/locations/us-central1",
		TriggerId: "bad",
		Trigger: &eventarcpb.Trigger{
			Destination: &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_Workflow{Workflow: "projects/proj/locations/us-central1/workflows/w1"}},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (%v)", status.Code(err), err)
	}
}

func TestTriggerIAM(t *testing.T) {
	ctx := context.Background()
	s := newGRPCServiceWithWorkflow(t)
	if _, err := s.CreateTrigger(ctx, &eventarcpb.CreateTriggerRequest{
		Parent: "projects/proj/locations/us-central1", TriggerId: "t1", Trigger: triggerProto(""),
	}); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	resource := "projects/proj/locations/us-central1/triggers/t1"
	if !s.Owns(resource) {
		t.Fatal("Owns(trigger) = false")
	}
	if !s.Owns("projects/proj/locations/us-central1/channels/c") {
		t.Fatal("Owns(channel name) = false")
	}
	if s.Owns("projects/proj/topics/t") {
		t.Fatal("Owns(topic) = true")
	}

	pol, err := s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: resource,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: "roles/eventarc.viewer", Members: []string{"user:a@example.com"}}},
		},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetEtag() == nil {
		t.Fatalf("policy = %+v", pol)
	}

	got, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: resource})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if len(got.GetBindings()) != 1 || got.GetBindings()[0].GetRole() != "roles/eventarc.viewer" {
		t.Fatalf("policy = %+v", got)
	}

	tp, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource: resource, Permissions: []string{"eventarc.triggers.get", "eventarc.triggers.update"},
	})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(tp.GetPermissions()) != 2 {
		t.Fatalf("permissions = %v", tp.GetPermissions())
	}

	// IAM against a resource name the service does not own is InvalidArgument.
	if _, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: "projects/proj/topics/t"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unowned resource: code = %v, want InvalidArgument", status.Code(err))
	}
	// IAM against a missing trigger is NotFound.
	if _, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: "projects/proj/locations/us-central1/triggers/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing trigger: code = %v, want NotFound", status.Code(err))
	}
}

// TestUnimplementedRPC verifies the non-trigger/channel/provider surface (e.g.
// ChannelConnection) falls through to the embedded Unimplemented stub.
func TestUnimplementedRPC(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	_, err := s.GetMessageBus(ctx, &eventarcpb.GetMessageBusRequest{Name: "projects/proj/locations/us-central1/messageBuses/mb"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("GetMessageBus code = %v, want Unimplemented", status.Code(err))
	}
}
