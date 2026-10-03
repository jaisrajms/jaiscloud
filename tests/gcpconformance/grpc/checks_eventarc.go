package grpcconformance

import (
	"context"
	"fmt"

	eventarc "cloud.google.com/go/eventarc/apiv1"
	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// eventarcChecks covers the Eventarc v1 control plane
// (google.cloud.eventarc.v1.Eventarc) via the official generated
// cloud.google.com/go/eventarc/apiv1 client: trigger CRUD, channel CRUD, and
// read-only provider discovery. Create/update/delete are long-running
// operations whose typed response is packed inline (done=true), so the client's
// Wait observes it without polling.
//
// The IAM trio is served through the shared google.iam.v1.IAMPolicy service
// (Eventarc's own proto carries no IAM RPCs), so those three probes are tagged
// under the "iam" fidelity service and exercise the router's Eventarc branch
// with a run-unique trigger fixture.
//
// Every other Eventarc RPC (ChannelConnection, GoogleChannelConfig, MessageBus,
// Enrollment, Pipeline, GoogleApiSource) is an explicit Unimplemented stub and
// is deliberately not probed.
func eventarcChecks() []Check {
	return []Check{
		{Service: "eventarc", RPC: "CreateTrigger", Method: "CreateTrigger", KeyField: "LRO done + trigger echo", Run: checkEACreateTrigger},
		{Service: "eventarc", RPC: "GetTrigger", Method: "GetTrigger", KeyField: "name round-trip", Run: checkEAGetTrigger},
		{Service: "eventarc", RPC: "ListTriggers", Method: "ListTriggers", KeyField: "created trigger present", Run: checkEAListTriggers},
		{Service: "eventarc", RPC: "UpdateTrigger", Method: "UpdateTrigger", KeyField: "labels updated via LRO", Run: checkEAUpdateTrigger},
		{Service: "eventarc", RPC: "DeleteTrigger", Method: "DeleteTrigger", KeyField: "LRO done + NotFound after", Run: checkEADeleteTrigger},
		{Service: "eventarc", RPC: "CreateChannel", Method: "CreateChannel", KeyField: "LRO done + PENDING channel", Run: checkEACreateChannel},
		{Service: "eventarc", RPC: "GetChannel", Method: "GetChannel", KeyField: "name round-trip", Run: checkEAGetChannel},
		{Service: "eventarc", RPC: "ListChannels", Method: "ListChannels", KeyField: "created channel present", Run: checkEAListChannels},
		{Service: "eventarc", RPC: "UpdateChannel", Method: "UpdateChannel", KeyField: "labels updated via LRO", Run: checkEAUpdateChannel},
		{Service: "eventarc", RPC: "DeleteChannel", Method: "DeleteChannel", KeyField: "LRO done + NotFound after", Run: checkEADeleteChannel},
		{Service: "eventarc", RPC: "GetProvider", Method: "GetProvider", KeyField: "catalogued provider present", Run: checkEAGetProvider},
		{Service: "eventarc", RPC: "ListProviders", Method: "ListProviders", KeyField: "provider list non-empty", Run: checkEAListProviders},
		{Service: "iam", RPC: "GetIamPolicy (Eventarc trigger)", Method: "GetIamPolicy", KeyField: "empty policy on a fresh trigger", Run: checkEATriggerIAMGet},
		{Service: "iam", RPC: "SetIamPolicy (Eventarc trigger)", Method: "SetIamPolicy", KeyField: "binding round-trips through the router", Run: checkEATriggerIAMSet},
		{Service: "iam", RPC: "TestIamPermissions (Eventarc trigger)", Method: "TestIamPermissions", KeyField: "requested permissions echoed back", Run: checkEATriggerIAMTest},
	}
}

const eventarcLocation = "us-central1"

// newEventarcClient dials the emulator and returns the official generated
// Eventarc client.
func newEventarcClient(ctx context.Context, cfg Config) (*eventarc.Client, error) {
	return eventarc.NewClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func eventarcParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, eventarcLocation)
}

func eventarcTriggerName(cfg Config, id string) string {
	return eventarcParent(cfg) + "/triggers/" + id
}

func eventarcChannelName(cfg Config, id string) string {
	return eventarcParent(cfg) + "/channels/" + id
}

// eventarcTriggerProto builds a minimal valid trigger body: an httpEndpoint
// destination plus the required eventFilters type filter. It avoids depending
// on any other service's fixture.
func eventarcTriggerProto() *eventarcpb.Trigger {
	return &eventarcpb.Trigger{
		Destination: &eventarcpb.Destination{
			Descriptor_: &eventarcpb.Destination_HttpEndpoint{
				HttpEndpoint: &eventarcpb.HttpEndpoint{Uri: "https://example.com/events"},
			},
		},
		EventFilters: []*eventarcpb.EventFilter{
			{Attribute: "type", Value: "google.cloud.storage.object.v1.finalized"},
		},
	}
}

// ensureEventarcTrigger creates the run-unique probe trigger, tolerating
// AlreadyExists so repeated probes are idempotent.
func ensureEventarcTrigger(ctx context.Context, client *eventarc.Client, cfg Config, id string) (string, error) {
	op, err := client.CreateTrigger(ctx, &eventarcpb.CreateTriggerRequest{
		Parent:    eventarcParent(cfg),
		TriggerId: id,
		Trigger:   eventarcTriggerProto(),
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return eventarcTriggerName(cfg, id), nil
		}
		return "", fmt.Errorf("create trigger: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait trigger: %w", err)
	}
	return eventarcTriggerName(cfg, id), nil
}

func ensureEventarcChannel(ctx context.Context, client *eventarc.Client, cfg Config, id string) (string, error) {
	op, err := client.CreateChannel(ctx, &eventarcpb.CreateChannelRequest{
		Parent:    eventarcParent(cfg),
		ChannelId: id,
		Channel:   &eventarcpb.Channel{},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return eventarcChannelName(cfg, id), nil
		}
		return "", fmt.Errorf("create channel: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait channel: %w", err)
	}
	return eventarcChannelName(cfg, id), nil
}

// Check 1: CreateTrigger returns a done operation whose response is the trigger.
func checkEACreateTrigger(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-ea-create")
	op, err := client.CreateTrigger(ctx, &eventarcpb.CreateTriggerRequest{
		Parent:    eventarcParent(cfg),
		TriggerId: id,
		Trigger:   eventarcTriggerProto(),
	})
	if err != nil {
		return fmt.Errorf("CreateTrigger: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateTrigger operation not done")
	}
	meta, err := op.Metadata()
	if err != nil {
		return fmt.Errorf("operation metadata: %w", err)
	}
	if meta.GetVerb() != "create" {
		return fmt.Errorf("operation verb = %q, want create", meta.GetVerb())
	}
	trig, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if trig.GetName() != eventarcTriggerName(cfg, id) {
		return fmt.Errorf("trigger name = %q, want %q", trig.GetName(), eventarcTriggerName(cfg, id))
	}
	if trig.GetUid() == "" || trig.GetEtag() == "" {
		return fmt.Errorf("trigger output-only fields missing: %+v", trig)
	}
	if trig.GetDestination().GetHttpEndpoint().GetUri() != "https://example.com/events" {
		return fmt.Errorf("destination not echoed: %+v", trig.GetDestination())
	}
	return nil
}

// Check 2: GetTrigger round-trips.
func checkEAGetTrigger(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEventarcTrigger(ctx, client, cfg, cfg.ResourceName("gcpc-grpc-ea-get"))
	if err != nil {
		return err
	}
	got, err := client.GetTrigger(ctx, &eventarcpb.GetTriggerRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetTrigger: %w", err)
	}
	if got.GetName() != name || got.GetUid() == "" {
		return fmt.Errorf("GetTrigger = %+v", got)
	}
	return nil
}

// Check 3: ListTriggers includes the probe trigger.
func checkEAListTriggers(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEventarcTrigger(ctx, client, cfg, cfg.ResourceName("gcpc-grpc-ea-list"))
	if err != nil {
		return err
	}
	it := client.ListTriggers(ctx, &eventarcpb.ListTriggersRequest{Parent: eventarcParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListTriggers did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListTriggers: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

// Check 4: UpdateTrigger applies labels through an LRO.
func checkEAUpdateTrigger(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEventarcTrigger(ctx, client, cfg, cfg.ResourceName("gcpc-grpc-ea-upd"))
	if err != nil {
		return err
	}
	op, err := client.UpdateTrigger(ctx, &eventarcpb.UpdateTriggerRequest{
		Trigger:    &eventarcpb.Trigger{Name: name, Labels: map[string]string{"updated": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateTrigger: %w", err)
	}
	trig, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if trig.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("updated labels = %v", trig.GetLabels())
	}
	return nil
}

// Check 5: DeleteTrigger returns a done operation and the trigger is then gone.
func checkEADeleteTrigger(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-ea-del")
	name := eventarcTriggerName(cfg, id)
	if op, err := client.CreateTrigger(ctx, &eventarcpb.CreateTriggerRequest{
		Parent: eventarcParent(cfg), TriggerId: id, Trigger: eventarcTriggerProto(),
	}); err != nil {
		return fmt.Errorf("create: %w", err)
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("create wait: %w", err)
	}
	delOp, err := client.DeleteTrigger(ctx, &eventarcpb.DeleteTriggerRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteTrigger: %w", err)
	}
	if !delOp.Done() {
		return fmt.Errorf("delete operation not done")
	}
	if _, err := delOp.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetTrigger(ctx, &eventarcpb.GetTriggerRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetTrigger after delete = %v, want NotFound", err)
	}
	return nil
}

// Check 6: CreateChannel returns a done operation whose response is PENDING.
func checkEACreateChannel(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-ea-ch-create")
	op, err := client.CreateChannel(ctx, &eventarcpb.CreateChannelRequest{
		Parent: eventarcParent(cfg), ChannelId: id, Channel: &eventarcpb.Channel{},
	})
	if err != nil {
		return fmt.Errorf("CreateChannel: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateChannel operation not done")
	}
	ch, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if ch.GetName() != eventarcChannelName(cfg, id) {
		return fmt.Errorf("channel name = %q, want %q", ch.GetName(), eventarcChannelName(cfg, id))
	}
	if ch.GetActivationToken() == "" {
		return fmt.Errorf("channel activation token missing: %+v", ch)
	}
	if ch.GetState() != eventarcpb.Channel_PENDING {
		return fmt.Errorf("channel state = %v, want PENDING", ch.GetState())
	}
	return nil
}

// Check 7: GetChannel round-trips.
func checkEAGetChannel(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEventarcChannel(ctx, client, cfg, cfg.ResourceName("gcpc-grpc-ea-ch-get"))
	if err != nil {
		return err
	}
	got, err := client.GetChannel(ctx, &eventarcpb.GetChannelRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetChannel: %w", err)
	}
	if got.GetName() != name {
		return fmt.Errorf("GetChannel = %+v", got)
	}
	return nil
}

// Check 8: ListChannels includes the probe channel.
func checkEAListChannels(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEventarcChannel(ctx, client, cfg, cfg.ResourceName("gcpc-grpc-ea-ch-list"))
	if err != nil {
		return err
	}
	it := client.ListChannels(ctx, &eventarcpb.ListChannelsRequest{Parent: eventarcParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListChannels did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListChannels: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

// Check 9: UpdateChannel applies labels through an LRO.
func checkEAUpdateChannel(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEventarcChannel(ctx, client, cfg, cfg.ResourceName("gcpc-grpc-ea-ch-upd"))
	if err != nil {
		return err
	}
	op, err := client.UpdateChannel(ctx, &eventarcpb.UpdateChannelRequest{
		Channel:    &eventarcpb.Channel{Name: name, Labels: map[string]string{"updated": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateChannel: %w", err)
	}
	ch, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if ch.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("updated labels = %v", ch.GetLabels())
	}
	return nil
}

// Check 10: DeleteChannel returns a done operation and the channel is then gone.
func checkEADeleteChannel(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-ea-ch-del")
	name := eventarcChannelName(cfg, id)
	if op, err := client.CreateChannel(ctx, &eventarcpb.CreateChannelRequest{
		Parent: eventarcParent(cfg), ChannelId: id, Channel: &eventarcpb.Channel{},
	}); err != nil {
		return fmt.Errorf("create: %w", err)
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("create wait: %w", err)
	}
	delOp, err := client.DeleteChannel(ctx, &eventarcpb.DeleteChannelRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteChannel: %w", err)
	}
	if !delOp.Done() {
		return fmt.Errorf("delete operation not done")
	}
	if _, err := delOp.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetChannel(ctx, &eventarcpb.GetChannelRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetChannel after delete = %v, want NotFound", err)
	}
	return nil
}

// Check 11: GetProvider returns a catalogued provider.
func checkEAGetProvider(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name := eventarcParent(cfg) + "/providers/pubsub.googleapis.com"
	got, err := client.GetProvider(ctx, &eventarcpb.GetProviderRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetProvider: %w", err)
	}
	if got.GetDisplayName() == "" || len(got.GetEventTypes()) == 0 {
		return fmt.Errorf("GetProvider = %+v", got)
	}
	return nil
}

// Check 12: ListProviders returns a non-empty catalog.
func checkEAListProviders(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	it := client.ListProviders(ctx, &eventarcpb.ListProvidersRequest{Parent: eventarcParent(cfg)})
	got, err := it.Next()
	if err == iterator.Done {
		return fmt.Errorf("ListProviders returned no providers")
	}
	if err != nil {
		return fmt.Errorf("ListProviders: %w", err)
	}
	if got.GetName() == "" {
		return fmt.Errorf("provider missing name: %+v", got)
	}
	return nil
}

// ─── IAM (Eventarc trigger, served via the shared IAMPolicy router) ───────────

// eventarcIAMFixture creates the run-unique trigger an IAM probe targets, and
// returns its name plus a cleanup func.
func eventarcIAMFixture(ctx context.Context, cfg Config, prefix string) (string, func(), error) {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return "", nil, err
	}
	name, err := ensureEventarcTrigger(ctx, client, cfg, cfg.ResourceName(prefix))
	if err != nil {
		client.Close()
		return "", nil, err
	}
	cleanup := func() {
		delOp, derr := client.DeleteTrigger(ctx, &eventarcpb.DeleteTriggerRequest{Name: name})
		if derr == nil {
			_, _ = delOp.Wait(ctx)
		}
		client.Close()
	}
	return name, cleanup, nil
}

// Check 13: GetIamPolicy on a fresh Eventarc trigger is empty.
func checkEATriggerIAMGet(ctx context.Context, cfg Config) error {
	name, cleanup, err := eventarcIAMFixture(ctx, cfg, "gcpc-grpc-ea-iam-get")
	if err != nil {
		return err
	}
	defer cleanup()
	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()
	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name})
	if err != nil {
		return fmt.Errorf("GetIamPolicy(%s): %w", name, err)
	}
	if len(pol.GetBindings()) != 0 {
		return fmt.Errorf("GetIamPolicy(%s) on a fresh trigger returned %d bindings, want 0", name, len(pol.GetBindings()))
	}
	return nil
}

// Check 14: SetIamPolicy round-trips through the router.
func checkEATriggerIAMSet(ctx context.Context, cfg Config) error {
	name, cleanup, err := eventarcIAMFixture(ctx, cfg, "gcpc-grpc-ea-iam-set")
	if err != nil {
		return err
	}
	defer cleanup()
	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()
	const role = "roles/eventarc.viewer"
	if _, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: name,
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: role, Members: []string{"allUsers"}}}},
	}); err != nil {
		return fmt.Errorf("SetIamPolicy(%s): %w", name, err)
	}
	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name})
	if err != nil {
		return fmt.Errorf("GetIamPolicy(%s): %w", name, err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != role {
		return fmt.Errorf("policy = %+v, want one %s binding", pol.GetBindings(), role)
	}
	return nil
}

// Check 15: TestIamPermissions echoes the requested permissions.
func checkEATriggerIAMTest(ctx context.Context, cfg Config) error {
	name, cleanup, err := eventarcIAMFixture(ctx, cfg, "gcpc-grpc-ea-iam-test")
	if err != nil {
		return err
	}
	defer cleanup()
	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()
	want := []string{"eventarc.triggers.get", "eventarc.triggers.update"}
	resp, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: name, Permissions: want})
	if err != nil {
		return fmt.Errorf("TestIamPermissions(%s): %w", name, err)
	}
	if len(resp.GetPermissions()) != len(want) {
		return fmt.Errorf("TestIamPermissions(%s) = %v, want %v", name, resp.GetPermissions(), want)
	}
	return nil
}
