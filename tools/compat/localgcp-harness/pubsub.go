package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	psPub pubsubpb.PublisherClient
	psSub pubsubpb.SubscriberClient
)

func initPubSub() {
	conn, err := grpc.NewClient(grpcEndpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	psPub = pubsubpb.NewPublisherClient(conn)
	psSub = pubsubpb.NewSubscriberClient(conn)
}

func psCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 20*time.Second)
}

func wantCode(err error, c codes.Code) error {
	if err == nil {
		return fmt.Errorf("expected %s, got nil", c)
	}
	if got := status.Code(err); got != c {
		return fmt.Errorf("expected %s, got %s (%v)", c, got, err)
	}
	return nil
}

func runPubSub() {
	initPubSub()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"TestCreateTopic", psCreateTopic},
		{"TestCreateDuplicateTopic", psCreateDuplicateTopic},
		{"TestGetTopic", psGetTopic},
		{"TestGetTopicNotFound", psGetTopicNotFound},
		{"TestListTopics", psListTopics},
		{"TestDeleteTopic", psDeleteTopic},
		{"TestDeleteTopicNotFound", psDeleteTopicNotFound},
		{"TestCreateSubscription", psCreateSubscription},
		{"TestCreateSubscriptionDefaultAckDeadline", psCreateSubscriptionDefaultAck},
		{"TestCreateSubscriptionNonExistentTopic", psCreateSubscriptionNonExistentTopic},
		{"TestCreateDuplicateSubscription", psCreateDuplicateSubscription},
		{"TestGetSubscription", psGetSubscription},
		{"TestGetSubscriptionNotFound", psGetSubscriptionNotFound},
		{"TestListSubscriptions", psListSubscriptions},
		{"TestDeleteSubscription", psDeleteSubscription},
		{"TestPublishAndPull", psPublishAndPull},
		{"TestPublishToNonExistentTopic", psPublishNonExistent},
		{"TestAcknowledge", psAcknowledge},
		{"TestFanOut", psFanOut},
		{"TestModifyAckDeadline", psModifyAckDeadline},
		{"TestPullEmptySubscription", psPullEmpty},
		{"TestPullNonExistentSubscription", psPullNonExistent},
		{"TestDeleteTopicOrphansSubscription", psDeleteTopicOrphansSub},
		{"TestMaxMessages", psMaxMessages},
		{"TestAnyProjectID", psAnyProjectID},
	}
	for _, c := range cases {
		record("pubsub", c.name, c.fn())
	}

	recordNA("pubsub", "TestPushSubscriptionDelivery", "requires an HTTP push receiver reachable from the emulator pod")
	recordNA("pubsub", "TestPushSubscriptionStreamingPullBlocked", "requires an HTTP push receiver reachable from the emulator pod")
	recordNA("pubsub", "TestPushSubscriptionDeadLetter", "requires an HTTP push receiver reachable from the emulator pod")
}

func psTopic(id string) string { return "projects/test-project/topics/" + id + suffix }
func psSubName(id string) string {
	return "projects/test-project/subscriptions/" + id + suffix
}

func psCreateTopic() error {
	ctx, cancel := psCtx()
	defer cancel()
	name := psTopic("create")
	t, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: name})
	if err != nil {
		return err
	}
	if t.Name != name {
		return fmt.Errorf("got %q", t.Name)
	}
	return nil
}

func psCreateDuplicateTopic() error {
	ctx, cancel := psCtx()
	defer cancel()
	name := psTopic("dup")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: name}); err != nil {
		return err
	}
	_, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: name})
	return wantCode(err, codes.AlreadyExists)
}

func psGetTopic() error {
	ctx, cancel := psCtx()
	defer cancel()
	name := psTopic("get")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: name}); err != nil {
		return err
	}
	t, err := psPub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name})
	if err != nil {
		return err
	}
	if t.Name != name {
		return fmt.Errorf("got %q", t.Name)
	}
	return nil
}

func psGetTopicNotFound() error {
	ctx, cancel := psCtx()
	defer cancel()
	_, err := psPub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: psTopic("missing")})
	return wantCode(err, codes.NotFound)
}

func psListTopics() error {
	ctx, cancel := psCtx()
	defer cancel()
	proj := "list-topics-" + suffix
	a := "projects/" + proj + "/topics/alpha"
	b := "projects/" + proj + "/topics/beta"
	for _, n := range []string{a, b, "projects/other-" + suffix + "/topics/gamma"} {
		if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: n}); err != nil {
			return err
		}
	}
	resp, err := psPub.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/" + proj})
	if err != nil {
		return err
	}
	if len(resp.Topics) != 2 {
		return fmt.Errorf("expected 2 topics, got %d", len(resp.Topics))
	}
	var got []string
	for _, t := range resp.Topics {
		got = append(got, t.Name)
	}
	sort.Strings(got)
	if got[0] != a || got[1] != b {
		return fmt.Errorf("unexpected topics: %v", got)
	}
	return nil
}

func psDeleteTopic() error {
	ctx, cancel := psCtx()
	defer cancel()
	name := psTopic("del")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: name}); err != nil {
		return err
	}
	if _, err := psPub.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: name}); err != nil {
		return err
	}
	_, err := psPub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name})
	return wantCode(err, codes.NotFound)
}

func psDeleteTopicNotFound() error {
	ctx, cancel := psCtx()
	defer cancel()
	_, err := psPub.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: psTopic("missing-del")})
	return wantCode(err, codes.NotFound)
}

func psCreateSubscription() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("sub")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	name := psSubName("sub")
	s, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: name, Topic: topic, AckDeadlineSeconds: 15})
	if err != nil {
		return err
	}
	if s.Name != name || s.Topic != topic || s.AckDeadlineSeconds != 15 {
		return fmt.Errorf("unexpected subscription: %+v", s)
	}
	return nil
}

func psCreateSubscriptionDefaultAck() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("defack")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	s, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: psSubName("defack"), Topic: topic})
	if err != nil {
		return err
	}
	if s.AckDeadlineSeconds != 10 {
		return fmt.Errorf("expected default ack 10, got %d", s.AckDeadlineSeconds)
	}
	return nil
}

func psCreateSubscriptionNonExistentTopic() error {
	ctx, cancel := psCtx()
	defer cancel()
	_, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  psSubName("orphan"),
		Topic: psTopic("does-not-exist"),
	})
	return wantCode(err, codes.NotFound)
}

func psCreateDuplicateSubscription() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("dupsub")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	name := psSubName("dupsub")
	if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: name, Topic: topic}); err != nil {
		return err
	}
	_, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: name, Topic: topic})
	return wantCode(err, codes.AlreadyExists)
}

func psGetSubscription() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("getsub")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	name := psSubName("getsub")
	if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: name, Topic: topic}); err != nil {
		return err
	}
	got, err := psSub.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name})
	if err != nil {
		return err
	}
	if got.Name != name {
		return fmt.Errorf("got %q", got.Name)
	}
	return nil
}

func psGetSubscriptionNotFound() error {
	ctx, cancel := psCtx()
	defer cancel()
	_, err := psSub.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: psSubName("missing")})
	return wantCode(err, codes.NotFound)
}

func psListSubscriptions() error {
	ctx, cancel := psCtx()
	defer cancel()
	proj := "list-subs-" + suffix
	topic := "projects/" + proj + "/topics/t"
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	for _, id := range []string{"sub-a", "sub-b"} {
		if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name: "projects/" + proj + "/subscriptions/" + id, Topic: topic,
		}); err != nil {
			return err
		}
	}
	// Different project should not appear.
	otherTopic := "projects/other-" + suffix + "/topics/t"
	psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: otherTopic})
	psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: "projects/other-" + suffix + "/subscriptions/s", Topic: otherTopic})

	resp, err := psSub.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{Project: "projects/" + proj})
	if err != nil {
		return err
	}
	if len(resp.Subscriptions) != 2 {
		return fmt.Errorf("expected 2 subscriptions, got %d", len(resp.Subscriptions))
	}
	return nil
}

func psDeleteSubscription() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("delsub")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	name := psSubName("delsub")
	if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: name, Topic: topic}); err != nil {
		return err
	}
	if _, err := psSub.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: name}); err != nil {
		return err
	}
	_, err := psSub.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: name})
	return wantCode(err, codes.NotFound)
}

func psPublishAndPull() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("msg")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	sub := psSubName("msg")
	if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic}); err != nil {
		return err
	}
	pr, err := psPub.Publish(ctx, &pubsubpb.PublishRequest{Topic: topic, Messages: []*pubsubpb.PubsubMessage{
		{Data: []byte("hello")}, {Data: []byte("world"), Attributes: map[string]string{"key": "value"}},
	}})
	if err != nil {
		return err
	}
	if len(pr.MessageIds) != 2 {
		return fmt.Errorf("expected 2 message ids, got %d", len(pr.MessageIds))
	}
	pull, err := psSub.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 10})
	if err != nil {
		return err
	}
	if len(pull.ReceivedMessages) != 2 {
		return fmt.Errorf("expected 2 messages, got %d", len(pull.ReceivedMessages))
	}
	msgs := pull.ReceivedMessages
	sort.Slice(msgs, func(i, j int) bool { return string(msgs[i].Message.Data) < string(msgs[j].Message.Data) })
	if string(msgs[0].Message.Data) != "hello" || string(msgs[1].Message.Data) != "world" {
		return fmt.Errorf("unexpected payloads %q %q", msgs[0].Message.Data, msgs[1].Message.Data)
	}
	if msgs[1].Message.Attributes["key"] != "value" {
		return fmt.Errorf("attribute lost: %v", msgs[1].Message.Attributes)
	}
	if msgs[0].Message.MessageId == "" || msgs[0].Message.PublishTime == nil {
		return fmt.Errorf("missing message id/publish time")
	}
	return nil
}

func psPublishNonExistent() error {
	ctx, cancel := psCtx()
	defer cancel()
	_, err := psPub.Publish(ctx, &pubsubpb.PublishRequest{Topic: psTopic("nonexistent"), Messages: []*pubsubpb.PubsubMessage{{Data: []byte("x")}}})
	return wantCode(err, codes.NotFound)
}

func psAcknowledge() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("ack")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	sub := psSubName("ack")
	if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic, AckDeadlineSeconds: 600}); err != nil {
		return err
	}
	if _, err := psPub.Publish(ctx, &pubsubpb.PublishRequest{Topic: topic, Messages: []*pubsubpb.PubsubMessage{
		{Data: []byte("msg1")}, {Data: []byte("msg2")},
	}}); err != nil {
		return err
	}
	pull, err := psSub.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 10})
	if err != nil {
		return err
	}
	if len(pull.ReceivedMessages) != 2 {
		return fmt.Errorf("expected 2, got %d", len(pull.ReceivedMessages))
	}
	if _, err := psSub.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{pull.ReceivedMessages[0].AckId}}); err != nil {
		return err
	}
	pull2, err := psSub.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 10})
	if err != nil {
		return err
	}
	if len(pull2.ReceivedMessages) != 0 {
		return fmt.Errorf("expected 0 after ack, got %d", len(pull2.ReceivedMessages))
	}
	return nil
}

func psFanOut() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("fanout")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	subs := []string{psSubName("fanout1"), psSubName("fanout2")}
	for _, s := range subs {
		if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: s, Topic: topic}); err != nil {
			return err
		}
	}
	if _, err := psPub.Publish(ctx, &pubsubpb.PublishRequest{Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("broadcast")}}}); err != nil {
		return err
	}
	for _, s := range subs {
		pull, err := psSub.Pull(ctx, &pubsubpb.PullRequest{Subscription: s, MaxMessages: 10})
		if err != nil {
			return err
		}
		if len(pull.ReceivedMessages) != 1 || string(pull.ReceivedMessages[0].Message.Data) != "broadcast" {
			return fmt.Errorf("%s: expected 1 broadcast, got %d", s, len(pull.ReceivedMessages))
		}
	}
	return nil
}

func psModifyAckDeadline() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("modack")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	sub := psSubName("modack")
	if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic, AckDeadlineSeconds: 600}); err != nil {
		return err
	}
	if _, err := psPub.Publish(ctx, &pubsubpb.PublishRequest{Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("deadline-test")}}}); err != nil {
		return err
	}
	pull, err := psSub.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 10})
	if err != nil {
		return err
	}
	if len(pull.ReceivedMessages) != 1 {
		return fmt.Errorf("expected 1, got %d", len(pull.ReceivedMessages))
	}
	if _, err := psSub.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{pull.ReceivedMessages[0].AckId}, AckDeadlineSeconds: 0,
	}); err != nil {
		return err
	}
	pull2, err := psSub.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 10})
	if err != nil {
		return err
	}
	if len(pull2.ReceivedMessages) != 1 || string(pull2.ReceivedMessages[0].Message.Data) != "deadline-test" {
		return fmt.Errorf("expected redelivery, got %d", len(pull2.ReceivedMessages))
	}
	return nil
}

func psPullEmpty() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("empty")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	sub := psSubName("empty")
	if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic}); err != nil {
		return err
	}
	pull, err := psSub.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 10})
	if err != nil {
		return err
	}
	if len(pull.ReceivedMessages) != 0 {
		return fmt.Errorf("expected 0, got %d", len(pull.ReceivedMessages))
	}
	return nil
}

func psPullNonExistent() error {
	ctx, cancel := psCtx()
	defer cancel()
	_, err := psSub.Pull(ctx, &pubsubpb.PullRequest{Subscription: psSubName("missing"), MaxMessages: 10})
	return wantCode(err, codes.NotFound)
}

func psDeleteTopicOrphansSub() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("orphan")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	sub := psSubName("orphan")
	if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic}); err != nil {
		return err
	}
	if _, err := psPub.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topic}); err != nil {
		return err
	}
	got, err := psSub.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		return err
	}
	if got.Topic != "_deleted-topic_" {
		return fmt.Errorf("expected _deleted-topic_, got %q", got.Topic)
	}
	return nil
}

func psMaxMessages() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := psTopic("max")
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	sub := psSubName("max")
	if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic}); err != nil {
		return err
	}
	var msgs []*pubsubpb.PubsubMessage
	for i := 0; i < 5; i++ {
		msgs = append(msgs, &pubsubpb.PubsubMessage{Data: []byte(fmt.Sprintf("msg-%d", i))})
	}
	if _, err := psPub.Publish(ctx, &pubsubpb.PublishRequest{Topic: topic, Messages: msgs}); err != nil {
		return err
	}
	pull, err := psSub.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 2})
	if err != nil {
		return err
	}
	if len(pull.ReceivedMessages) != 2 {
		return fmt.Errorf("expected 2, got %d", len(pull.ReceivedMessages))
	}
	return nil
}

func psAnyProjectID() error {
	ctx, cancel := psCtx()
	defer cancel()
	topic := "projects/my-custom-project-" + suffix + "/topics/custom"
	if _, err := psPub.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil {
		return err
	}
	sub := "projects/my-custom-project-" + suffix + "/subscriptions/custom"
	if _, err := psSub.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic}); err != nil {
		return err
	}
	if _, err := psPub.Publish(ctx, &pubsubpb.PublishRequest{Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: []byte("custom project")}}}); err != nil {
		return err
	}
	pull, err := psSub.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 10})
	if err != nil {
		return err
	}
	if len(pull.ReceivedMessages) != 1 {
		return fmt.Errorf("expected 1, got %d", len(pull.ReceivedMessages))
	}
	return nil
}
