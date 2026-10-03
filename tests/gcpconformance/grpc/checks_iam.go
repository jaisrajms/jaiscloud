package grpcconformance

import (
	"context"
	"fmt"

	iam "cloud.google.com/go/iam/apiv1"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// iamChecks covers the standalone google.iam.v1.IAMPolicy surface: the three
// RPCs registered once by grpcserver.NewIAMRouter and dispatched to whichever
// service owns the resource name. The router owns Pub/Sub topics/subscriptions
// and KMS resources, so each probe backs itself with its own run-unique Pub/Sub
// topic (projects/{project}/topics/{topic}) created and deleted in this file —
// it never depends on a fixture owned by another check.
func iamChecks() []Check {
	return []Check{
		{Service: "iam", RPC: "GetIamPolicy", Method: "GetIamPolicy", KeyField: "policy present, empty bindings by default", Run: checkIAMGetPolicy},
		{Service: "iam", RPC: "SetIamPolicy", Method: "SetIamPolicy", KeyField: "role+member binding round-trips via GetIamPolicy", Run: checkIAMSetPolicy},
		{Service: "iam", RPC: "TestIamPermissions", Method: "TestIamPermissions", KeyField: "requested permissions echoed back", Run: checkIAMTestPermissions},
	}
}

// newIAMPolicyClient dials the standalone IAMPolicy RPC service at the emulator
// with insecure transport and no authentication, mirroring the other official
// clients in this suite.
func newIAMPolicyClient(ctx context.Context, cfg Config) (*iam.IamPolicyClient, error) {
	return iam.NewIamPolicyClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// iamTopicName builds a run-unique Pub/Sub topic resource name for one IAM
// probe. The distinct prefix keeps the three probes from sharing fixtures.
func iamTopicName(cfg Config, prefix string) string {
	return fmt.Sprintf("projects/%s/topics/%s", cfg.Project, cfg.ResourceName(prefix))
}

// iamCreateTopic creates the backing topic, tolerating AlreadyExists so a probe
// stays idempotent against a long-lived emulator.
func iamCreateTopic(ctx context.Context, cfg Config, topic string) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new pubsub client: %w", err)
	}
	defer client.Close()
	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateTopic(%s): %w", topic, err)
	}
	return nil
}

// iamDeleteTopic removes a probe's backing topic. A NotFound is tolerated so
// cleanup is idempotent.
func iamDeleteTopic(ctx context.Context, cfg Config, topic string) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new pubsub client: %w", err)
	}
	defer client.Close()
	if err := client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topic}); err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("DeleteTopic(%s): %w", topic, err)
	}
	return nil
}

// checkIAMGetPolicy asserts GetIamPolicy on a resource the router owns returns a
// well-formed policy that is empty by default.
func checkIAMGetPolicy(ctx context.Context, cfg Config) (err error) {
	topic := iamTopicName(cfg, "gcpc-iam-get")
	if err := iamCreateTopic(ctx, cfg, topic); err != nil {
		return err
	}
	defer func() {
		if derr := iamDeleteTopic(ctx, cfg, topic); derr != nil && err == nil {
			err = derr
		}
	}()

	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: topic})
	if err != nil {
		return fmt.Errorf("GetIamPolicy(%s): %w", topic, err)
	}
	if pol == nil {
		return fmt.Errorf("GetIamPolicy(%s) returned a nil policy", topic)
	}
	if len(pol.GetBindings()) != 0 {
		return fmt.Errorf("GetIamPolicy(%s) on a fresh topic returned %d binding(s), want 0", topic, len(pol.GetBindings()))
	}
	return nil
}

// checkIAMSetPolicy sets a single role+member binding with SetIamPolicy on the
// standalone IAMPolicy service, then reads it back with GetIamPolicy to prove
// the write round-trips through the router.
func checkIAMSetPolicy(ctx context.Context, cfg Config) (err error) {
	topic := iamTopicName(cfg, "gcpc-iam-set")
	if err := iamCreateTopic(ctx, cfg, topic); err != nil {
		return err
	}
	defer func() {
		if derr := iamDeleteTopic(ctx, cfg, topic); derr != nil && err == nil {
			err = derr
		}
	}()

	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	const (
		role   = "roles/pubsub.viewer"
		member = "allUsers"
	)
	set, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: topic,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: role, Members: []string{member}}},
		},
	})
	if err != nil {
		return fmt.Errorf("SetIamPolicy(%s): %w", topic, err)
	}
	if len(set.GetBindings()) != 1 || set.GetBindings()[0].GetRole() != role {
		return fmt.Errorf("SetIamPolicy(%s) returned bindings %v, want one %s binding", topic, set.GetBindings(), role)
	}

	got, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: topic})
	if err != nil {
		return fmt.Errorf("GetIamPolicy(%s) after SetIamPolicy: %w", topic, err)
	}
	if len(got.GetBindings()) != 1 {
		return fmt.Errorf("GetIamPolicy(%s) after SetIamPolicy returned %d binding(s), want 1", topic, len(got.GetBindings()))
	}
	b := got.GetBindings()[0]
	if b.GetRole() != role {
		return fmt.Errorf("GetIamPolicy(%s) binding role = %q, want %q", topic, b.GetRole(), role)
	}
	want := map[string]bool{member: true}
	for _, m := range b.GetMembers() {
		delete(want, m)
	}
	if len(want) != 0 {
		return fmt.Errorf("GetIamPolicy(%s) binding members = %v, want to contain %q", topic, b.GetMembers(), member)
	}
	return nil
}

// checkIAMTestPermissions asserts TestIamPermissions echoes back exactly the
// requested permission list on a resource owned by the router.
func checkIAMTestPermissions(ctx context.Context, cfg Config) (err error) {
	topic := iamTopicName(cfg, "gcpc-iam-test")
	if err := iamCreateTopic(ctx, cfg, topic); err != nil {
		return err
	}
	defer func() {
		if derr := iamDeleteTopic(ctx, cfg, topic); derr != nil && err == nil {
			err = derr
		}
	}()

	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	want := []string{"pubsub.topics.get", "pubsub.topics.publish"}
	resp, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    topic,
		Permissions: want,
	})
	if err != nil {
		return fmt.Errorf("TestIamPermissions(%s): %w", topic, err)
	}
	got := map[string]bool{}
	for _, p := range resp.GetPermissions() {
		got[p] = true
	}
	if len(resp.GetPermissions()) != len(want) {
		return fmt.Errorf("TestIamPermissions(%s) returned %v, want exactly %v", topic, resp.GetPermissions(), want)
	}
	for _, p := range want {
		if !got[p] {
			return fmt.Errorf("TestIamPermissions(%s) returned %v, missing %q", topic, resp.GetPermissions(), p)
		}
	}
	return nil
}
