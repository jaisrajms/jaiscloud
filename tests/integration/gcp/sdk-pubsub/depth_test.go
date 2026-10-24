// depth_test.go closes DC6 (plan_docs/gcp-depth-closure-wave-plan.md W2.4) for
// Cloud Pub/Sub: the p4 cell its record left exempt — paging over topic and
// subscription listings — asserted through the official Pub/Sub v2 admin client
// over gRPC and the shared depth-test kit (tests/integration/gcp/testdepth).
//
// The Discovery document declares exactly `pageSize`/`pageToken` on
// `projects.topics.list` and `projects.subscriptions.list` — no `filter`, no
// documented order — so the walk asserts the DC2 paging contract with
// OrderUnspecified, the created set as `Expect`, and the two walks compared as a
// set. The admin iterator hides the cursor (`Next()` walks the whole
// collection), so the adapter drives one page per call through the iterator's
// `InternalFetch`, which is the seam the kit needs to test the cursor itself.
package sdkpubsub_test

import (
	"context"
	"testing"

	pubsub "cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	testdepth "jaiscloud-gcp-testdepth"
)

// pubsubDepthPageSize is deliberately small so a walk of pubsubDepthItems
// resources crosses many page boundaries.
const pubsubDepthPageSize = 4

// pubsubDepthItems is the collection size the p4 walk pages through.
const pubsubDepthItems = 25

// walkTopics pages projects.topics.list to the end through the kit.
func walkTopics(t *testing.T, c *pubsub.Client, ctx context.Context, cfg testdepth.Paging[*pubsubpb.Topic]) {
	t.Helper()
	testdepth.AssertPaging(t, func() testdepth.Lister[*pubsubpb.Topic] {
		it := c.TopicAdminClient.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/" + projectID()})
		return func(req testdepth.ListRequest) (testdepth.Page[*pubsubpb.Topic], error) {
			items, next, err := it.InternalFetch(int(req.PageSize), req.PageToken)
			return testdepth.Page[*pubsubpb.Topic]{Items: items, NextPageToken: next}, err
		}
	}, cfg)
}

// walkSubscriptions pages projects.subscriptions.list to the end through the kit.
func walkSubscriptions(t *testing.T, c *pubsub.Client, ctx context.Context, cfg testdepth.Paging[*pubsubpb.Subscription]) {
	t.Helper()
	testdepth.AssertPaging(t, func() testdepth.Lister[*pubsubpb.Subscription] {
		it := c.SubscriptionAdminClient.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{Project: "projects/" + projectID()})
		return func(req testdepth.ListRequest) (testdepth.Page[*pubsubpb.Subscription], error) {
			items, next, err := it.InternalFetch(int(req.PageSize), req.PageToken)
			return testdepth.Page[*pubsubpb.Subscription]{Items: items, NextPageToken: next}, err
		}
	}, cfg)
}

// TestPubSubDepthListingPaging is pubsub p4: a small page size walks the
// project's topics and subscriptions end to end, asserting every resource
// appears exactly once, no page exceeds the requested size, an empty final
// token, an identical second walk, and every created resource present.
func TestPubSubDepthListingPaging(t *testing.T) {
	c, ctx := newClient(t)

	t.Run("Topics", func(t *testing.T) {
		want := make([]string, 0, pubsubDepthItems)
		for i := 0; i < pubsubDepthItems; i++ {
			name := topicName("depth")
			createTopic(t, c, ctx, name)
			want = append(want, name)
		}
		walkTopics(t, c, ctx, testdepth.Paging[*pubsubpb.Topic]{
			Name:                 func(tp *pubsubpb.Topic) string { return tp.GetName() },
			PageSize:             pubsubDepthPageSize,
			Order:                testdepth.OrderUnspecified,
			RequireNonEmptyPages: true,
			Expect:               want,
		})
	})

	t.Run("Subscriptions", func(t *testing.T) {
		topic := topicName("depth-sub")
		createTopic(t, c, ctx, topic)

		want := make([]string, 0, pubsubDepthItems)
		for i := 0; i < pubsubDepthItems; i++ {
			name := subName("depth")
			createSub(t, c, ctx, name, topic, nil)
			want = append(want, name)
		}
		walkSubscriptions(t, c, ctx, testdepth.Paging[*pubsubpb.Subscription]{
			Name:                 func(s *pubsubpb.Subscription) string { return s.GetName() },
			PageSize:             pubsubDepthPageSize,
			Order:                testdepth.OrderUnspecified,
			RequireNonEmptyPages: true,
			Expect:               want,
		})
	})
}
