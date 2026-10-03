package grpcconformance

import (
	"context"
	"fmt"
	"os"

	pubsub "cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
)

// pubSubChecks covers the Pub/Sub topic-admin surface
// (google.pubsub.v1.Publisher) via the official Pub/Sub v2 client.
func pubSubChecks() []Check {
	return []Check{
		{Service: "pubsub", RPC: "CreateTopic", KeyField: "name", Run: checkPubSubCreateTopic},
		{Service: "pubsub", RPC: "GetTopic", KeyField: "name", Run: checkPubSubGetTopic},
		{Service: "pubsub", RPC: "ListTopics", KeyField: "topics[].name", Run: checkPubSubListTopics},
		{Service: "pubsub", RPC: "DeleteTopic", KeyField: "success", Run: checkPubSubDeleteTopic},
	}
}

// newPubSubClient points the official v2 client at the emulator. The v2 client
// has a first-class emulator hook keyed on PUBSUB_EMULATOR_HOST which installs
// exactly the insecure endpoint options (endpoint, insecure transport,
// no-auth); set it from the shared config so every probe targets the same
// endpoint as the other services.
func newPubSubClient(ctx context.Context, cfg Config) (*pubsub.Client, error) {
	if err := os.Setenv("PUBSUB_EMULATOR_HOST", cfg.GRPCAddr()); err != nil {
		return nil, fmt.Errorf("set PUBSUB_EMULATOR_HOST: %w", err)
	}
	return pubsub.NewClient(ctx, cfg.Project)
}

func pubSubTopicName(cfg Config) string {
	return fmt.Sprintf("projects/%s/topics/%s", cfg.Project, cfg.ResourceName("gcpc-grpc-topic"))
}

func checkPubSubCreateTopic(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	topic, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: pubSubTopicName(cfg)})
	if err != nil {
		return err
	}
	if topic.GetName() != pubSubTopicName(cfg) {
		return fmt.Errorf("CreateTopic returned name %q, want %q", topic.GetName(), pubSubTopicName(cfg))
	}
	return nil
}

func checkPubSubGetTopic(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	topic, err := client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: pubSubTopicName(cfg)})
	if err != nil {
		return err
	}
	if topic.GetName() != pubSubTopicName(cfg) {
		return fmt.Errorf("GetTopic returned name %q, want %q", topic.GetName(), pubSubTopicName(cfg))
	}
	return nil
}

func checkPubSubListTopics(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	it := client.TopicAdminClient.ListTopics(ctx, &pubsubpb.ListTopicsRequest{
		Project: "projects/" + cfg.Project,
	})
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if topic.GetName() == pubSubTopicName(cfg) {
			return nil
		}
	}
	return fmt.Errorf("ListTopics did not include %q", pubSubTopicName(cfg))
}

func checkPubSubDeleteTopic(ctx context.Context, cfg Config) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	return client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: pubSubTopicName(cfg)})
}
