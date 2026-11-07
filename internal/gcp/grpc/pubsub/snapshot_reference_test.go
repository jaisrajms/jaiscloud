package pubsub

import (
	"context"
	"testing"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestCreateSnapshotTopicReference covers FC4-5: Snapshot.topic is a
// google.api.resource_reference, so a snapshot of a subscription whose topic no
// longer exists is NOT_FOUND instead of storing a dangling name. A snapshot of
// a live topic still succeeds.
func TestCreateSnapshotTopicReference(t *testing.T) {
	pub, subc, _, _, cleanup := pubsubTestService(t)
	defer cleanup()
	ctx := context.Background()

	const topic = "projects/test/topics/snap-ref-topic"
	const sub = "projects/test/subscriptions/snap-ref-sub"
	pubsubSeedTopic(t, ctx, pub, subc, topic, sub)

	if _, err := subc.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{
		Name: "projects/test/snapshots/snap-ref-ok", Subscription: sub,
	}); err != nil {
		t.Fatalf("accept: %v", err)
	}

	if _, err := pub.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topic}); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	_, err := subc.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{
		Name: "projects/test/snapshots/snap-ref-missing", Subscription: sub,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("snapshot with deleted topic = %v, want NOT_FOUND", err)
	}
}
