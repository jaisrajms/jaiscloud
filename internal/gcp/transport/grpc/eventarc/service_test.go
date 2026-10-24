package eventarc

import (
	"context"
	"strings"
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

// TestListTriggersFilterAndOrderGRPC verifies the proto's filter/order_by
// fields reach the core (the gRPC adapter previously passed neither on).
func TestListTriggersFilterAndOrderGRPC(t *testing.T) {
	ctx := context.Background()
	s := newGRPCServiceWithWorkflow(t)
	parent := "projects/proj/locations/us-central1"
	create := func(id string, dest *eventarcpb.Destination) {
		t.Helper()
		if _, err := s.CreateTrigger(ctx, &eventarcpb.CreateTriggerRequest{
			Parent: parent, TriggerId: id,
			Trigger: &eventarcpb.Trigger{
				Destination:  dest,
				EventFilters: []*eventarcpb.EventFilter{{Attribute: "type", Value: "x"}},
			},
		}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	create("t1", &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_Workflow{Workflow: "projects/proj/locations/us-central1/workflows/w1"}})
	create("t2", &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_CloudRun{CloudRun: &eventarcpb.CloudRun{Service: "svc", Region: "us-central1"}}})

	names := func(req *eventarcpb.ListTriggersRequest) []string {
		t.Helper()
		req.Parent = parent
		list, err := s.ListTriggers(ctx, req)
		if err != nil {
			t.Fatalf("ListTriggers %+v: %v", req, err)
		}
		out := make([]string, 0, len(list.GetTriggers()))
		for _, tr := range list.GetTriggers() {
			out = append(out, tr.GetName())
		}
		return out
	}

	if got := names(&eventarcpb.ListTriggersRequest{Filter: "destination:workflow"}); len(got) != 1 || !strings.HasSuffix(got[0], "/t1") {
		t.Errorf("filter=destination:workflow = %v", got)
	}
	got := names(&eventarcpb.ListTriggersRequest{OrderBy: "name desc"})
	if len(got) != 2 || !strings.HasSuffix(got[0], "/t2") || !strings.HasSuffix(got[1], "/t1") {
		t.Errorf("order_by=name desc = %v", got)
	}
	if _, err := s.ListTriggers(ctx, &eventarcpb.ListTriggersRequest{Parent: parent, OrderBy: "bogus"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad order_by: code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := s.ListTriggers(ctx, &eventarcpb.ListTriggersRequest{Parent: parent, Filter: "bogus=1"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad filter: code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestAdvancedSurfaceServed verifies the advanced surface (message buses,
// enrollments, pipelines, Google API sources, channel connections, Google
// channel config) is served by the Eventarc service rather than falling through
// to the embedded Unimplemented stub: a missing message bus is NotFound, not
// Unimplemented.
func TestAdvancedSurfaceServed(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	_, err := s.GetMessageBus(ctx, &eventarcpb.GetMessageBusRequest{Name: "projects/proj/locations/us-central1/messageBuses/mb"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetMessageBus code = %v, want NotFound", status.Code(err))
	}
	// A malformed advanced name is InvalidArgument, not Unimplemented.
	if _, err := s.GetMessageBus(ctx, &eventarcpb.GetMessageBusRequest{Name: "projects/proj/locations/us-central1/messageBuses/"}); status.Code(err) == codes.Unimplemented {
		t.Fatalf("GetMessageBus on an empty id must not be Unimplemented")
	}
}

// TestAdvancedMessageBusGRPC drives the advanced gRPC surface for a message bus
// (create LRO → get → list → update → delete), plus the Google channel config
// singleton and the listEnrollments custom method.
func TestAdvancedMessageBusGRPC(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	parent := "projects/proj/locations/us-central1"
	busName := parent + "/messageBuses/mb1"

	op, err := s.CreateMessageBus(ctx, &eventarcpb.CreateMessageBusRequest{
		Parent: parent, MessageBusId: "mb1",
		MessageBus: &eventarcpb.MessageBus{DisplayName: "demo"},
	})
	if err != nil {
		t.Fatalf("CreateMessageBus: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("create not done")
	}
	var created eventarcpb.MessageBus
	if err := op.GetResponse().UnmarshalTo(&created); err != nil {
		t.Fatalf("unpack response: %v", err)
	}
	if created.GetName() != busName || created.GetUid() == "" {
		t.Fatalf("created = %+v", &created)
	}

	got, err := s.GetMessageBus(ctx, &eventarcpb.GetMessageBusRequest{Name: busName})
	if err != nil || got.GetDisplayName() != "demo" {
		t.Fatalf("GetMessageBus = %+v, %v", got, err)
	}

	list, err := s.ListMessageBuses(ctx, &eventarcpb.ListMessageBusesRequest{Parent: parent})
	if err != nil || len(list.GetMessageBuses()) != 1 {
		t.Fatalf("ListMessageBuses = %+v, %v", list, err)
	}

	upd, err := s.UpdateMessageBus(ctx, &eventarcpb.UpdateMessageBusRequest{
		MessageBus: &eventarcpb.MessageBus{Name: busName, Labels: map[string]string{"x": "y"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateMessageBus: %v", err)
	}
	var updated eventarcpb.MessageBus
	if err := upd.GetResponse().UnmarshalTo(&updated); err != nil {
		t.Fatalf("unpack update: %v", err)
	}
	if updated.GetLabels()["x"] != "y" {
		t.Fatalf("updated labels = %v", updated.GetLabels())
	}

	// An enrollment attached to the bus is listed by listEnrollments.
	if _, err := s.CreateEnrollment(ctx, &eventarcpb.CreateEnrollmentRequest{
		Parent: parent, EnrollmentId: "e1",
		Enrollment: &eventarcpb.Enrollment{MessageBus: busName},
	}); err != nil {
		t.Fatalf("CreateEnrollment: %v", err)
	}
	enr, err := s.ListMessageBusEnrollments(ctx, &eventarcpb.ListMessageBusEnrollmentsRequest{Parent: busName})
	if err != nil || len(enr.GetEnrollments()) != 1 {
		t.Fatalf("ListMessageBusEnrollments = %+v, %v", enr, err)
	}

	// Google channel config singleton: default get then update (inline response).
	cfgName := parent + "/googleChannelConfig"
	def, err := s.GetGoogleChannelConfig(ctx, &eventarcpb.GetGoogleChannelConfigRequest{Name: cfgName})
	if err != nil || def.GetName() != cfgName {
		t.Fatalf("GetGoogleChannelConfig = %+v, %v", def, err)
	}
	if _, err := s.UpdateGoogleChannelConfig(ctx, &eventarcpb.UpdateGoogleChannelConfigRequest{
		GoogleChannelConfig: &eventarcpb.GoogleChannelConfig{Name: cfgName, CryptoKeyName: "projects/p/locations/us-central1/keyRings/r/cryptoKeys/k"},
		UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"crypto_key_name"}},
	}); err != nil {
		t.Fatalf("UpdateGoogleChannelConfig: %v", err)
	}

	del, err := s.DeleteMessageBus(ctx, &eventarcpb.DeleteMessageBusRequest{Name: busName})
	if err != nil || !del.GetDone() {
		t.Fatalf("DeleteMessageBus = %+v, %v", del, err)
	}
	if _, err := s.GetMessageBus(ctx, &eventarcpb.GetMessageBusRequest{Name: busName}); status.Code(err) != codes.NotFound {
		t.Fatalf("Get after delete = %v, want NotFound", err)
	}
}

// TestAdvancedIAMGRPC verifies the IAM router dispatches the advanced-surface
// families (a message bus here) and reports NotFound for a missing one.
func TestAdvancedIAMGRPC(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	name := "projects/proj/locations/us-central1/messageBuses/mb1"
	if _, err := s.CreateMessageBus(ctx, &eventarcpb.CreateMessageBusRequest{
		Parent: "projects/proj/locations/us-central1", MessageBusId: "mb1", MessageBus: &eventarcpb.MessageBus{},
	}); err != nil {
		t.Fatalf("CreateMessageBus: %v", err)
	}
	if !s.Owns(name) {
		t.Fatalf("Owns(%q) = false", name)
	}
	pol, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name})
	if err != nil || len(pol.GetBindings()) != 0 {
		t.Fatalf("GetIamPolicy = %+v, %v", pol, err)
	}
	if _, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: "projects/proj/locations/us-central1/pipelines/missing",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing pipeline IAM = %v, want NotFound", err)
	}
}

// TestListMessageBusesFilterAndOrder verifies the documented filter/orderBy
// request fields reach the core through gRPC rather than being dropped (TD17).
func TestListMessageBusesFilterAndOrder(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	parent := "projects/proj/locations/us-central1"
	for _, mb := range []struct{ id, dn, env string }{
		{"mb1", "alpha", "test"}, {"mb2", "beta", "prod"}, {"mb3", "gamma", "test"},
	} {
		if _, err := s.CreateMessageBus(ctx, &eventarcpb.CreateMessageBusRequest{
			Parent: parent, MessageBusId: mb.id,
			MessageBus: &eventarcpb.MessageBus{DisplayName: mb.dn, Labels: map[string]string{"env": mb.env}},
		}); err != nil {
			t.Fatalf("CreateMessageBus %s: %v", mb.id, err)
		}
	}

	names := func(req *eventarcpb.ListMessageBusesRequest) []string {
		t.Helper()
		list, err := s.ListMessageBuses(ctx, req)
		if err != nil {
			t.Fatalf("ListMessageBuses(%v): %v", req, err)
		}
		out := make([]string, 0, len(list.GetMessageBuses()))
		for _, mb := range list.GetMessageBuses() {
			out = append(out, mb.GetName())
		}
		return out
	}

	got := names(&eventarcpb.ListMessageBusesRequest{Parent: parent, Filter: "labels.env=test"})
	want := []string{
		"projects/proj/locations/us-central1/messageBuses/mb1",
		"projects/proj/locations/us-central1/messageBuses/mb3",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("filtered message buses = %v, want %v", got, want)
	}

	got = names(&eventarcpb.ListMessageBusesRequest{Parent: parent, OrderBy: "name desc"})
	want = []string{
		"projects/proj/locations/us-central1/messageBuses/mb3",
		"projects/proj/locations/us-central1/messageBuses/mb2",
		"projects/proj/locations/us-central1/messageBuses/mb1",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("message buses orderBy name desc = %v, want %v", got, want)
	}

	if _, err := s.ListMessageBuses(ctx, &eventarcpb.ListMessageBusesRequest{Parent: parent, Filter: "bogus=1"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad filter code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestListProvidersFilterAndOrder verifies providers.list honors filter/orderBy
// through gRPC (TD17).
func TestListProvidersFilterAndOrder(t *testing.T) {
	ctx := context.Background()
	s := newGRPCService()
	parent := "projects/proj/locations/us-central1"

	list, err := s.ListProviders(ctx, &eventarcpb.ListProvidersRequest{Parent: parent, OrderBy: "displayName desc"})
	if err != nil {
		t.Fatalf("ListProviders: %v", err)
	}
	if len(list.GetProviders()) < 2 {
		t.Fatalf("providers = %v", list.GetProviders())
	}
	if list.GetProviders()[0].GetDisplayName() != "Cloud Storage" {
		t.Errorf("first provider = %q, want Cloud Storage", list.GetProviders()[0].GetDisplayName())
	}

	list, err = s.ListProviders(ctx, &eventarcpb.ListProvidersRequest{Parent: parent, Filter: `displayName:"Storage"`})
	if err != nil {
		t.Fatalf("ListProviders filter: %v", err)
	}
	if len(list.GetProviders()) != 1 || list.GetProviders()[0].GetDisplayName() != "Cloud Storage" {
		t.Errorf("filtered providers = %v", list.GetProviders())
	}
}
