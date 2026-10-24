package main

// Streaming-semantics tour (SDK_TOUR_MODE=streaming).
//
// The happy-path tour proves the official clients can drive every surface; this
// leg goes deeper on the *streaming* surfaces and asserts their semantics, not
// just that a message arrived: ack-deadline redelivery and extension, ordering
// keys, exactly-once ack, subscriber flow control, Firestore Listen resume
// tokens, Logging tail reconnects, and Storage BidiReadObject.
//
// Where the high-level client hides the semantics being asserted, the generated
// low-level client is used (Firestore Listen resume tokens, precise
// StreamingPull / ModifyAckDeadline control). Elsewhere the high-level client is
// used, because its behavior is part of what is under test.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	firestorehl "cloud.google.com/go/firestore"
	firestore "cloud.google.com/go/firestore/apiv1"
	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	pubsub "cloud.google.com/go/pubsub"
	pubsubadmin "cloud.google.com/go/pubsub/apiv1"
	pubsubpb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
	"google.golang.org/api/option"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// streamAckDeadline is the protocol minimum; the minimum makes an ignored
	// redelivery observable within a short, bounded wait.
	streamAckDeadline int32 = 10
	// redeliverWait is ack deadline + a margin. Redelivery is a property of the
	// emulator's own clock, so a small margin is not a timing race.
	redeliverWait = 13 * time.Second
	// flowMaxOutstanding is the subscriber flow-control cap the scenario pins.
	// It is deliberately small and below every client's default worker/thread
	// count, so the cap (not the client's concurrency) bounds delivery.
	flowMaxOutstanding = 3
	// flowBacklog is the number of messages published to fill a backlog well
	// past the cap.
	flowBacklog = 15
)

// runStreaming is the entry point for SDK_TOUR_MODE=streaming.
func runStreaming(r *runner) {
	streamPubSubScenarios(r)
	streamFirestoreScenarios(r)
	streamLoggingScenarios(r)
	streamStorageScenarios(r)
}

// ─── Pub/Sub ─────────────────────────────────────────────────────────────────

func newStreamTopic(ctx context.Context, c *pubsub.Client, topicID string) (*pubsub.Topic, error) {
	if _, err := c.CreateTopic(ctx, topicID); err != nil && status.Code(err) != codes.AlreadyExists {
		return nil, fmt.Errorf("create topic %s: %w", topicID, err)
	}
	return c.Topic(topicID), nil
}

func newStreamSub(ctx context.Context, c *pubsub.Client, subID string, topic *pubsub.Topic, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	cfg.Topic = topic
	if _, err := c.CreateSubscription(ctx, subID, cfg); err != nil && status.Code(err) != codes.AlreadyExists {
		return nil, fmt.Errorf("create subscription %s: %w", subID, err)
	}
	return c.Subscription(subID), nil
}

func publishOne(ctx context.Context, topic *pubsub.Topic, body string) error {
	res := topic.Publish(ctx, &pubsub.Message{Data: []byte(body)})
	if _, err := res.Get(ctx); err != nil {
		return fmt.Errorf("publish %q: %w", body, err)
	}
	return nil
}

// streamPullOne opens the generated StreamingPull, sends the initial request,
// and returns the ack id of the first message whose body is want. The stream is
// left open until ctx is cancelled, so the caller controls redelivery.
func streamPullOne(ctx context.Context, sc *pubsubadmin.SubscriberClient, subName, want string) (string, error) {
	stream, err := sc.StreamingPull(ctx)
	if err != nil {
		return "", fmt.Errorf("StreamingPull: %w", err)
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{
		Subscription:             subName,
		StreamAckDeadlineSeconds: streamAckDeadline,
	}); err != nil {
		return "", fmt.Errorf("StreamingPull send: %w", err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return "", fmt.Errorf("StreamingPull recv: %w", err)
		}
		for _, rm := range resp.GetReceivedMessages() {
			if string(rm.GetMessage().GetData()) == want {
				return rm.GetAckId(), nil
			}
		}
	}
}

func streamPubSubScenarios(r *runner) {
	cfg := r.cfg
	project := cfg.Project
	topicID := rid(cfg, "stream-topic")
	subName := func(id string) string { return "projects/" + project + "/subscriptions/" + id }

	r.run("streaming.pubsub_ack_deadline", "OK", func(ctx context.Context) (string, string, error) {
		c, err := pubsub.NewClient(ctx, project)
		if err != nil {
			return "", "", err
		}
		defer c.Close()
		topic, err := newStreamTopic(ctx, c, topicID)
		if err != nil {
			return "", "", err
		}
		subID := rid(cfg, "stream-ackdl-sub")
		if _, err := newStreamSub(ctx, c, subID, topic, pubsub.SubscriptionConfig{AckDeadline: streamAckDeadlineSeconds()}); err != nil {
			return "", "", err
		}
		if err := publishOne(ctx, topic, "ackdl"); err != nil {
			return "", "", err
		}
		sc, err := pubsubadmin.NewSubscriberClient(ctx, grpcOptions(cfg)...)
		if err != nil {
			return "", "", err
		}
		defer sc.Close()
		name := subName(subID)

		// First delivery: receive and deliberately do not ack or extend.
		firstCtx, cancelFirst := context.WithCancel(ctx)
		if _, err := streamPullOne(firstCtx, sc, name, "ackdl"); err != nil {
			cancelFirst()
			return "", "", fmt.Errorf("first delivery: %w", err)
		}
		cancelFirst()
		time.Sleep(redeliverWait)

		// After the ack deadline the message must be redelivered.
		secondCtx, cancelSecond := context.WithTimeout(ctx, 20*time.Second)
		ackID, err := streamPullOne(secondCtx, sc, name, "ackdl")
		if err != nil {
			cancelSecond()
			return "", "", fmt.Errorf("redelivery after ack deadline: %w", err)
		}
		cancelSecond()

		// Ack it; an acked message must not be delivered again.
		if err := sc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: name, AckIds: []string{ackID}}); err != nil {
			return "", "", fmt.Errorf("ack: %w", err)
		}
		pull, err := sc.Pull(ctx, &pubsubpb.PullRequest{Subscription: name, MaxMessages: 10, ReturnImmediately: true})
		if err != nil {
			return "", "", fmt.Errorf("pull after ack: %w", err)
		}
		for _, rm := range pull.GetReceivedMessages() {
			if string(rm.GetMessage().GetData()) == "ackdl" {
				return "", "", fmt.Errorf("message redelivered after ack")
			}
		}
		return "redelivered=yes", "unacked message redelivered after the 10s ack deadline; ack stopped it", nil
	})

	r.run("streaming.pubsub_ack_extension", "OK", func(ctx context.Context) (string, string, error) {
		c, err := pubsub.NewClient(ctx, project)
		if err != nil {
			return "", "", err
		}
		defer c.Close()
		topic, err := newStreamTopic(ctx, c, topicID)
		if err != nil {
			return "", "", err
		}
		subID := rid(cfg, "stream-ackext-sub")
		if _, err := newStreamSub(ctx, c, subID, topic, pubsub.SubscriptionConfig{AckDeadline: streamAckDeadlineSeconds()}); err != nil {
			return "", "", err
		}
		if err := publishOne(ctx, topic, "ackext"); err != nil {
			return "", "", err
		}
		sc, err := pubsubadmin.NewSubscriberClient(ctx, grpcOptions(cfg)...)
		if err != nil {
			return "", "", err
		}
		defer sc.Close()
		name := subName(subID)

		recvCtx, cancelRecv := context.WithCancel(ctx)
		ackID, err := streamPullOne(recvCtx, sc, name, "ackext")
		if err != nil {
			cancelRecv()
			return "", "", fmt.Errorf("first delivery: %w", err)
		}
		// Extend the deadline well past the original 10s, then hold the stream
		// open across the old deadline: the extension must keep it invisible.
		if err := sc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
			Subscription: name, AckIds: []string{ackID}, AckDeadlineSeconds: 600,
		}); err != nil {
			cancelRecv()
			return "", "", fmt.Errorf("ModifyAckDeadline: %w", err)
		}
		cancelRecv()
		time.Sleep(redeliverWait)

		pull, err := sc.Pull(ctx, &pubsubpb.PullRequest{Subscription: name, MaxMessages: 10, ReturnImmediately: true})
		if err != nil {
			return "", "", fmt.Errorf("pull after extension: %w", err)
		}
		for _, rm := range pull.GetReceivedMessages() {
			if string(rm.GetMessage().GetData()) == "ackext" {
				return "", "", fmt.Errorf("message redelivered despite ModifyAckDeadline(600s)")
			}
		}
		// Clean up: nack so the subscription is not left with an invisible
		// message for the emulator's lifetime.
		_ = sc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
			Subscription: name, AckIds: []string{ackID}, AckDeadlineSeconds: 0,
		})
		return "no_redelivery_while_extended=yes", "ModifyAckDeadline(600s) kept the message invisible past its original 10s deadline", nil
	})

	r.run("streaming.pubsub_ordering_keys", "OK", func(ctx context.Context) (string, string, error) {
		c, err := pubsub.NewClient(ctx, project)
		if err != nil {
			return "", "", err
		}
		defer c.Close()
		topic, err := newStreamTopic(ctx, c, topicID)
		if err != nil {
			return "", "", err
		}
		topic.EnableMessageOrdering = true
		subID := rid(cfg, "stream-order-sub")
		sub, err := newStreamSub(ctx, c, subID, topic, pubsub.SubscriptionConfig{
			EnableMessageOrdering: true,
		})
		if err != nil {
			return "", "", err
		}

		const n = 10
		for i := 0; i < n; i++ {
			res := topic.Publish(ctx, &pubsub.Message{
				Data:        []byte(fmt.Sprintf("order-%02d", i)),
				OrderingKey: "stream-order-key",
			})
			if _, err := res.Get(ctx); err != nil {
				return "", "", fmt.Errorf("publish %d: %w", i, err)
			}
		}
		topic.Stop()

		recvCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		var mu sync.Mutex
		var got []string
		done := make(chan error, 1)
		go func() {
			done <- sub.Receive(recvCtx, func(_ context.Context, m *pubsub.Message) {
				mu.Lock()
				got = append(got, string(m.Data))
				at := len(got)
				mu.Unlock()
				m.Ack()
				if at >= n {
					cancel()
				}
			})
		}()
		select {
		case <-done:
		case <-ctx.Done():
			return "", "", fmt.Errorf("ordering receive timed out (got %d)", len(got))
		}
		mu.Lock()
		defer mu.Unlock()
		if len(got) != n {
			return "", "", fmt.Errorf("received %d messages, want %d", len(got), n)
		}
		for i, body := range got {
			if want := fmt.Sprintf("order-%02d", i); body != want {
				return "", "", fmt.Errorf("out of order at %d: got %q want %q (all=%v)", i, body, want, got)
			}
		}
		return "ordered=yes", fmt.Sprintf("ordering key preserved order across %d messages", n), nil
	})

	r.run("streaming.pubsub_exactly_once_ack", "OK", func(ctx context.Context) (string, string, error) {
		c, err := pubsub.NewClient(ctx, project)
		if err != nil {
			return "", "", err
		}
		defer c.Close()
		topic, err := newStreamTopic(ctx, c, topicID)
		if err != nil {
			return "", "", err
		}
		subID := rid(cfg, "stream-eod-sub")
		if _, err := newStreamSub(ctx, c, subID, topic, pubsub.SubscriptionConfig{
			EnableExactlyOnceDelivery: true,
		}); err != nil {
			return "", "", err
		}
		if err := publishOne(ctx, topic, "eod"); err != nil {
			return "", "", err
		}
		sc, err := pubsubadmin.NewSubscriberClient(ctx, grpcOptions(cfg)...)
		if err != nil {
			return "", "", err
		}
		defer sc.Close()
		name := subName(subID)

		recvCtx, cancelRecv := context.WithCancel(ctx)
		ackID, err := streamPullOne(recvCtx, sc, name, "eod")
		if err != nil {
			cancelRecv()
			return "", "", fmt.Errorf("delivery: %w", err)
		}
		if err := sc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: name, AckIds: []string{ackID}}); err != nil {
			cancelRecv()
			return "", "", fmt.Errorf("ack: %w", err)
		}
		cancelRecv()

		// Settle: an acked message on an exactly-once subscription must not be
		// redelivered.
		time.Sleep(3 * time.Second)
		pull, err := sc.Pull(ctx, &pubsubpb.PullRequest{Subscription: name, MaxMessages: 10, ReturnImmediately: true})
		if err != nil {
			return "", "", fmt.Errorf("pull after ack: %w", err)
		}
		for _, rm := range pull.GetReceivedMessages() {
			if string(rm.GetMessage().GetData()) == "eod" {
				return "", "", fmt.Errorf("exactly-once message redelivered after ack")
			}
		}

		// Exactly-once acknowledgement-ID versioning: a superseded ackId must be
		// rejected with INVALID_ARGUMENT carrying the ErrorInfo detail the
		// official clients map to AcknowledgeStatusInvalidAckID, while the latest
		// id is accepted. The superseded check runs against the same low-level
		// client (its ErrorInfo sidecar is exactly what AckWithResult consumes),
		// so it does not change the scenario's `dup=0` observable.
		if err := publishOne(ctx, topic, "eod-supersede"); err != nil {
			return "", "", err
		}
		recvA, cancelA := context.WithCancel(ctx)
		superseded, err := streamPullOne(recvA, sc, name, "eod-supersede")
		cancelA()
		if err != nil {
			return "", "", fmt.Errorf("supersede delivery: %w", err)
		}
		if err := sc.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
			Subscription: name, AckIds: []string{superseded}, AckDeadlineSeconds: 0,
		}); err != nil {
			return "", "", fmt.Errorf("nack: %w", err)
		}
		recvB, cancelB := context.WithCancel(ctx)
		latest, err := streamPullOne(recvB, sc, name, "eod-supersede")
		cancelB()
		if err != nil {
			return "", "", fmt.Errorf("redelivery: %w", err)
		}
		if latest == superseded {
			return "", "", fmt.Errorf("redelivery reused the ackId; delivery version not encoded")
		}
		if err := sc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: name, AckIds: []string{superseded}}); err == nil {
			return "", "", fmt.Errorf("superseded ackId accepted; want INVALID_ARGUMENT")
		} else if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument || !hasInvalidAckID(st, superseded) {
			return "", "", fmt.Errorf("superseded ack: code=%v details=%v, want INVALID_ARGUMENT + PERMANENT_FAILURE_INVALID_ACK_ID", st.Code(), st.Details())
		}
		if err := sc.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: name, AckIds: []string{latest}}); err != nil {
			return "", "", fmt.Errorf("ack latest: %w", err)
		}
		return "dup=0", "acked exactly-once message was not redelivered; superseded ackId rejected", nil
	})

	r.run("streaming.pubsub_flow_control", "OK", func(ctx context.Context) (string, string, error) {
		c, err := pubsub.NewClient(ctx, project)
		if err != nil {
			return "", "", err
		}
		defer c.Close()
		topic, err := newStreamTopic(ctx, c, topicID)
		if err != nil {
			return "", "", err
		}
		subID := rid(cfg, "stream-flow-sub")
		sub, err := newStreamSub(ctx, c, subID, topic, pubsub.SubscriptionConfig{
			AckDeadline: streamAckDeadlineSeconds(),
		})
		if err != nil {
			return "", "", err
		}
		for i := 0; i < flowBacklog; i++ {
			if err := publishOne(ctx, topic, fmt.Sprintf("flow-%02d", i)); err != nil {
				return "", "", err
			}
		}
		topic.Stop()

		// The high-level subscriber under test: the configured cap must bound
		// how many unacked messages it holds at once. Callbacks hold their
		// message (no ack) so delivery would outrun acks and expose a missing
		// bound; the main goroutine releases them only after the cap is proved.
		sub.ReceiveSettings.MaxOutstandingMessages = flowMaxOutstanding
		var inflight, maxInflight, delivered atomic.Int32
		var released atomic.Bool
		recvCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		recvDone := make(chan error, 1)
		go func() {
			recvDone <- sub.Receive(recvCtx, func(_ context.Context, m *pubsub.Message) {
				cur := inflight.Add(1)
				for {
					mx := maxInflight.Load()
					if cur <= mx || maxInflight.CompareAndSwap(mx, cur) {
						break
					}
				}
				delivered.Add(1)
				for !released.Load() {
					time.Sleep(5 * time.Millisecond)
				}
				m.Ack()
				inflight.Add(-1)
			})
		}()

		waitFor := func(want int32) bool {
			deadline := time.Now().Add(30 * time.Second)
			for delivered.Load() < want && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			return delivered.Load() >= want
		}

		if !waitFor(flowMaxOutstanding) {
			cancel()
			return "", "", fmt.Errorf("flow control: only %d/%d messages delivered while held",
				delivered.Load(), flowMaxOutstanding)
		}
		// Hold at the cap: a client that ignores the bound keeps delivering.
		time.Sleep(1500 * time.Millisecond)
		held, maxSeen := delivered.Load(), maxInflight.Load()
		if held != flowMaxOutstanding {
			cancel()
			return "", "", fmt.Errorf("flow control: %d messages held concurrently, want max_outstanding=%d",
				held, flowMaxOutstanding)
		}
		if maxSeen != flowMaxOutstanding {
			cancel()
			return "", "", fmt.Errorf("flow control: max in flight %d, want %d", maxSeen, flowMaxOutstanding)
		}

		released.Store(true)
		if !waitFor(flowBacklog) {
			cancel()
			return "", "", fmt.Errorf("flow control: drained %d/%d messages",
				delivered.Load(), flowBacklog)
		}
		cancel()
		select {
		case <-recvDone:
		case <-time.After(5 * time.Second):
		}
		return fmt.Sprintf("flow_control=ok,max_in_flight<=%d", flowMaxOutstanding),
			fmt.Sprintf("held at most %d of %d messages outstanding", flowMaxOutstanding, flowBacklog), nil
	})
}

// hasInvalidAckID reports whether st carries the google.rpc.ErrorInfo detail the
// official clients read as AcknowledgeStatusInvalidAckID / INVALID_ACK_ID.
func hasInvalidAckID(st *status.Status, ackID string) bool {
	for _, d := range st.Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok && ei.Metadata[ackID] == "PERMANENT_FAILURE_INVALID_ACK_ID" {
			return true
		}
	}
	return false
}

func streamAckDeadlineSeconds() time.Duration {
	return time.Duration(streamAckDeadline) * time.Second
}

// ─── Firestore ───────────────────────────────────────────────────────────────

func streamFirestoreParent(cfg Config) string {
	return "projects/" + cfg.Project + "/databases/(default)/documents"
}

func streamFirestoreDB(cfg Config) string {
	return "projects/" + cfg.Project + "/databases/(default)"
}

func listenAddTargetRequest(cfg Config, targetID int32, collection string, token []byte) *firestorepb.ListenRequest {
	target := &firestorepb.Target{
		TargetId: targetID,
		TargetType: &firestorepb.Target_Query{Query: &firestorepb.Target_QueryTarget{
			Parent: streamFirestoreParent(cfg),
			QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
				StructuredQuery: &firestorepb.StructuredQuery{
					From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: collection}},
				},
			},
		}},
	}
	if token != nil {
		target.ResumeType = &firestorepb.Target_ResumeToken{ResumeToken: token}
	}
	return &firestorepb.ListenRequest{
		Database:     streamFirestoreDB(cfg),
		TargetChange: &firestorepb.ListenRequest_AddTarget{AddTarget: target},
	}
}

func listenRemoveTargetRequest(cfg Config, targetID int32) *firestorepb.ListenRequest {
	return &firestorepb.ListenRequest{
		Database:     streamFirestoreDB(cfg),
		TargetChange: &firestorepb.ListenRequest_RemoveTarget{RemoveTarget: targetID},
	}
}

// listenRecvUntilNoChange reads frames until the NO_CHANGE that concludes the
// snapshot/epoch, returning every DocumentChange name it saw (in order).
func listenRecvUntilNoChange(stream firestorepb.Firestore_ListenClient) ([]string, error) {
	var names []string
	for {
		resp, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if dc := resp.GetDocumentChange(); dc != nil {
			names = append(names, dc.GetDocument().GetName())
			continue
		}
		if tc := resp.GetTargetChange(); tc != nil && tc.GetTargetChangeType() == firestorepb.TargetChange_NO_CHANGE {
			return names, nil
		}
	}
}

// listenRecvUntil returns after reading a TargetChange with the given type.
func listenRecvUntil(stream firestorepb.Firestore_ListenClient, want firestorepb.TargetChange_TargetChangeType) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		if tc := resp.GetTargetChange(); tc != nil && tc.GetTargetChangeType() == want {
			return nil
		}
	}
}

func streamFirestoreScenarios(r *runner) {
	cfg := r.cfg

	r.run("streaming.firestore_listen_resume_token", "OK", func(ctx context.Context) (string, string, error) {
		hc, err := newFirestoreClient(ctx, cfg) // high-level client for writes
		if err != nil {
			return "", "", err
		}
		defer hc.Close()
		lc, err := firestore.NewClient(ctx, grpcOptions(cfg)...) // generated Listen
		if err != nil {
			return "", "", err
		}
		defer lc.Close()
		coll := rid(cfg, "stream-listen-resume")
		return streamListenResumeWithToken(ctx, cfg, hc, lc, coll)
	})

	r.run("streaming.firestore_snapshot_consistency", "OK", func(ctx context.Context) (string, string, error) {
		hc, err := newFirestoreClient(ctx, cfg)
		if err != nil {
			return "", "", err
		}
		defer hc.Close()
		lc, err := firestore.NewClient(ctx, grpcOptions(cfg)...)
		if err != nil {
			return "", "", err
		}
		defer lc.Close()
		coll := rid(cfg, "stream-listen-consistency")

		stream, err := lc.Listen(ctx)
		if err != nil {
			return "", "", err
		}
		if err := stream.Send(listenAddTargetRequest(cfg, 1, coll, nil)); err != nil {
			return "", "", err
		}
		if _, err := listenRecvUntilNoChange(stream); err != nil {
			return "", "", fmt.Errorf("initial snapshot: %w", err)
		}

		const n = 5
		for i := 0; i < n; i++ {
			if _, err := hc.Collection(coll).Doc(fmt.Sprintf("d%d", i)).Set(ctx, map[string]any{"i": i}); err != nil {
				return "", "", fmt.Errorf("write d%d: %w", i, err)
			}
		}

		counts := map[string]int{}
		var lastRead time.Time
		monotonic := true
		for len(counts) < n {
			resp, err := stream.Recv()
			if err != nil {
				return "", "", fmt.Errorf("listen recv: %w", err)
			}
			if dc := resp.GetDocumentChange(); dc != nil {
				counts[dc.GetDocument().GetName()]++
				continue
			}
			if tc := resp.GetTargetChange(); tc != nil && tc.GetTargetChangeType() == firestorepb.TargetChange_NO_CHANGE {
				if tc.GetReadTime() != nil {
					rt := tc.GetReadTime().AsTime()
					if !lastRead.IsZero() && rt.Before(lastRead) {
						monotonic = false
					}
					lastRead = rt
				}
			}
		}
		for name, c := range counts {
			if c != 1 {
				return "", "", fmt.Errorf("document %s delivered %d times, want 1", name, c)
			}
		}
		if !monotonic {
			return "", "", fmt.Errorf("read_time was not monotonic across NO_CHANGE frames")
		}
		return fmt.Sprintf("docs=%d,dup=0,monotonic=yes", n), "5 concurrent writes delivered once each with monotonic read_time", nil
	})
}

// streamListenResumeWithToken implements the resume half of the resume-token
// scenario. The extra write after the resumed stream is opened (before AddTarget
// is sent) is the race the exactly-once dedupe closes: without it the change is
// both replayed and delivered from the buffered subscription.
func streamListenResumeWithToken(ctx context.Context, cfg Config, hc *firestorehl.Client, lc *firestore.Client, coll string) (string, string, error) {
	// Stream A: snapshot, then read the token from the CURRENT frame.
	ctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	sA, err := lc.Listen(ctxA)
	if err != nil {
		return "", "", err
	}
	if err := sA.Send(listenAddTargetRequest(cfg, 1, coll, nil)); err != nil {
		return "", "", err
	}
	var token []byte
	for token == nil {
		resp, err := sA.Recv()
		if err != nil {
			return "", "", fmt.Errorf("stream A recv: %w", err)
		}
		if tc := resp.GetTargetChange(); tc != nil && tc.GetTargetChangeType() == firestorepb.TargetChange_CURRENT && len(tc.GetResumeToken()) > 0 {
			token = tc.GetResumeToken()
		}
	}
	cancelA()

	// Write "b" after A is closed: it must be replayed to the resumed stream.
	if _, err := hc.Collection(coll).Doc("b").Set(ctx, map[string]any{"v": "b"}); err != nil {
		return "", "", fmt.Errorf("write b: %w", err)
	}

	// Stream B: register the subscription (a harmless remove proves the handler
	// is running), then write "c" BEFORE AddTarget. "c" is therefore both in the
	// replayed history and buffered as a live delta — it must be delivered once.
	ctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	sB, err := lc.Listen(ctxB)
	if err != nil {
		return "", "", err
	}
	if err := sB.Send(listenRemoveTargetRequest(cfg, 999)); err != nil {
		return "", "", err
	}
	if err := listenRecvUntil(sB, firestorepb.TargetChange_REMOVE); err != nil {
		return "", "", fmt.Errorf("stream B barrier: %w", err)
	}
	if _, err := hc.Collection(coll).Doc("c").Set(ctx, map[string]any{"v": "c"}); err != nil {
		return "", "", fmt.Errorf("write c: %w", err)
	}
	if err := sB.Send(listenAddTargetRequest(cfg, 2, coll, token)); err != nil {
		return "", "", err
	}
	names, err := listenRecvUntilNoChange(sB)
	if err != nil {
		return "", "", fmt.Errorf("resumed stream recv: %w", err)
	}
	counts := map[string]int{}
	for _, name := range names {
		short := name[strings.LastIndex(name, "/")+1:]
		counts[short]++
	}
	for _, want := range []string{"b", "c"} {
		if counts[want] != 1 {
			return "", "", fmt.Errorf("resumed stream delivered %q %d times, want 1 (all=%v)", want, counts[want], counts)
		}
	}
	return "loss=0,dup=0", "resume token replayed b and delivered the racing c exactly once", nil
}

// ─── Logging ─────────────────────────────────────────────────────────────────

func streamWriteLog(ctx context.Context, client *logging.Client, logName, text string) error {
	_, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		LogName: logName,
		Resource: &mrpb.MonitoredResource{
			Type:   "global",
			Labels: map[string]string{"project_id": "jaiscloud-project"},
		},
		Entries: []*loggingpb.LogEntry{{
			LogName:  logName,
			Severity: ltype.LogSeverity_INFO,
			Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: text},
		}},
	})
	return err
}

func streamLoggingScenarios(r *runner) {
	parent := "projects/" + r.cfg.Project
	logID := rid(r.cfg, "stream-tail-log")
	logName := parent + "/logs/" + logID

	startTail := func(ctx context.Context, client *logging.Client) (loggingpb.LoggingServiceV2_TailLogEntriesClient, error) {
		stream, err := client.TailLogEntries(ctx)
		if err != nil {
			return nil, err
		}
		if err := stream.Send(&loggingpb.TailLogEntriesRequest{
			ResourceNames: []string{parent},
			Filter:        fmt.Sprintf("logName=%q", logName),
			BufferWindow:  durationpb.New(200 * time.Millisecond),
		}); err != nil {
			return nil, err
		}
		return stream, nil
	}

	// recvUntilPrefix keeps writing unique entries with prefix until one is
	// delivered, tolerating the emulator's "new since stream start" seed race.
	recvUntilPrefix := func(ctx context.Context, client *logging.Client, stream loggingpb.LoggingServiceV2_TailLogEntriesClient, prefix string) error {
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			for i := 0; ; i++ {
				_ = streamWriteLog(ctx, client, logName, fmt.Sprintf("%s-%d", prefix, i))
				select {
				case <-stop:
					return
				case <-ctx.Done():
					return
				case <-time.After(250 * time.Millisecond):
				}
			}
		}()
		for {
			resp, err := stream.Recv()
			if err != nil {
				return err
			}
			for _, e := range resp.GetEntries() {
				if strings.HasPrefix(e.GetTextPayload(), prefix) {
					return nil
				}
			}
		}
	}

	r.run("streaming.logging_tail_reconnect", "OK", func(ctx context.Context) (string, string, error) {
		client, err := logging.NewClient(ctx, grpcOptions(r.cfg)...)
		if err != nil {
			return "", "", err
		}
		defer client.Close()

		s1, err := startTail(ctx, client)
		if err != nil {
			return "", "", err
		}
		if err := recvUntilPrefix(ctx, client, s1, "stream-tail-a"); err != nil {
			return "", "", fmt.Errorf("first tail: %w", err)
		}
		_ = s1.CloseSend()

		// Written while disconnected: below the second stream's seed cursor, so
		// it must NOT be replayed (TailLogEntries has no resume cursor).
		if err := streamWriteLog(ctx, client, logName, "stream-tail-gap"); err != nil {
			return "", "", err
		}

		ctx2, cancel2 := context.WithCancel(ctx)
		defer cancel2()
		s2, err := startTail(ctx2, client)
		if err != nil {
			return "", "", err
		}
		if err := recvUntilPrefix(ctx2, client, s2, "stream-tail-c"); err != nil {
			return "", "", fmt.Errorf("second tail: %w", err)
		}
		return "reconnect_ok=yes,gap_replayed=no", "second tail delivered post-reconnect entries and did not replay the disconnected-window entry", nil
	})
}

// ─── Storage ─────────────────────────────────────────────────────────────────

func streambidiStorageClient(ctx context.Context, cfg Config) (*storage.Client, error) {
	opts := []option.ClientOption{
		option.WithEndpoint(cfg.GRPC),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		storage.WithDisabledClientMetrics(),
		experimental.WithGRPCBidiReads(),
	}
	return storage.NewGRPCClient(ctx, opts...)
}

func streamStorageScenarios(r *runner) {
	cfg := r.cfg

	r.run("streaming.storage_bidi_read", "OK", func(ctx context.Context) (string, string, error) {
		client, err := streambidiStorageClient(ctx, cfg)
		if err != nil {
			return "", "", fmt.Errorf("new gRPC storage client: %w", err)
		}
		defer client.Close()

		bucket := rid(cfg, "stream-bidi-bucket")
		if err := client.Bucket(bucket).Create(ctx, cfg.Project, &storage.BucketAttrs{Location: "US"}); err != nil && status.Code(err) != codes.AlreadyExists {
			return "", "", fmt.Errorf("create bucket: %w", err)
		}
		payload := []byte("bidi-stream-payload-0123456789")
		w := client.Bucket(bucket).Object("bidi.bin").NewWriter(ctx)
		if _, err := w.Write(payload); err != nil {
			return "", "", fmt.Errorf("write: %w", err)
		}
		if err := w.Close(); err != nil {
			return "", "", fmt.Errorf("close: %w", err)
		}

		r1, err := client.Bucket(bucket).Object("bidi.bin").NewRangeReader(ctx, 0, -1)
		if err != nil {
			return "", "", fmt.Errorf("full reader: %w", err)
		}
		defer r1.Close()
		full, err := io.ReadAll(r1)
		if err != nil {
			return "", "", fmt.Errorf("full read: %w", err)
		}
		if !bytes.Equal(full, payload) {
			return "", "", fmt.Errorf("full read = %q, want %q", full, payload)
		}

		r2, err := client.Bucket(bucket).Object("bidi.bin").NewRangeReader(ctx, 5, 4)
		if err != nil {
			return "", "", fmt.Errorf("range reader: %w", err)
		}
		defer r2.Close()
		sub, err := io.ReadAll(r2)
		if err != nil {
			return "", "", fmt.Errorf("range read: %w", err)
		}
		if !bytes.Equal(sub, payload[5:9]) {
			return "", "", fmt.Errorf("range read = %q, want %q", sub, payload[5:9])
		}
		return "full=ok,range=ok", "BidiReadObject served a full read and a subrange", nil
	})
}
