//go:build gcp_differential

package gcpdifferential

import (
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// Pub/Sub messaging-semantics gRPC oracle (PSM1). The REST half
// (pubsub_messaging_scenarios.go) covers ack/modack/ordered delivery over the
// JSON transport; this half backs the gRPC-only cells the fidelity matrix marks
// `ga` without a transcript — Acknowledge, ModifyAckDeadline, Pull, Seek and
// StreamingPull — driving the official proto client. Every resource is ensured
// (idempotently, uncaptured) inside the scenario and removed by CleanupGRPC, so
// a recording leaves nothing behind.
//
// StreamingPull is captured as a bounded, message-level contract rather than a
// raw frame sequence: the stream is drained until the published message arrives,
// then cancelled, and only the delivered messages are golden — the frame
// boundaries (real GCP emits a properties-only frame before the data frame)
// differ between the two backends and are not part of the contract.
func pubsubMessagingGRPCScenarios(project string, n ResourceNames) []GRPCScenario {
	topic := "projects/" + project + "/topics/" + n.MsgTopic
	streamTopic := "projects/" + project + "/topics/" + n.StreamTopic
	msgSub := "projects/" + project + "/subscriptions/" + n.MsgSub
	ackSub := "projects/" + project + "/subscriptions/" + n.AckSub
	modSub := "projects/" + project + "/subscriptions/" + n.ModSub
	seekSub := "projects/" + project + "/subscriptions/" + n.SeekSub
	seekSnap := "projects/" + project + "/snapshots/" + n.SeekSnap
	streamSub := "projects/" + project + "/subscriptions/" + n.StreamSub
	dlqMainTopic := "projects/" + project + "/topics/" + n.DLQMainTopic
	dlqMainSub := "projects/" + project + "/subscriptions/" + n.DLQMainSub
	dlqTopic := "projects/" + project + "/topics/" + n.DLQTopic
	dlqSub := "projects/" + project + "/subscriptions/" + n.DLQSub

	return []GRPCScenario{
		// ── Pull: publish one message, pull it (data + folded ackId/messageId) ─
		{
			Service: "pubsub", Op: "msg_grpc_pull",
			Method: "google.pubsub.v1.Subscriber/Pull", Path: msgSub,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				if err := ensureGRPCPubSub(ctx, t, topic, msgSub, false); err != nil {
					return nil, nil, err
				}
				if err := pubsubPublish(ctx, t, topic, "grpc-pull", ""); err != nil {
					return nil, nil, err
				}
				req := &pubsubpb.PullRequest{Subscription: msgSub, MaxMessages: 1}
				resp, err := pubsubPullUntil(ctx, t, msgSub)
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},

		// ── Acknowledge: publish → pull → ack; the ack id is accepted and the
		//    acknowledgement response is empty ────────────────────────────────
		{
			Service: "pubsub", Op: "msg_grpc_acknowledge",
			Method: "google.pubsub.v1.Subscriber/Acknowledge", Path: ackSub,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				if err := ensureGRPCPubSub(ctx, t, topic, ackSub, false); err != nil {
					return nil, nil, err
				}
				if err := pubsubPublish(ctx, t, topic, "grpc-ack", ""); err != nil {
					return nil, nil, err
				}
				pulled, err := pubsubPullUntil(ctx, t, ackSub)
				if err != nil {
					return nil, nil, err
				}
				req := &pubsubpb.AcknowledgeRequest{Subscription: ackSub, AckIds: ackIDs(pulled)}
				var resp *emptypb.Empty
				err = withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
					var cerr error
					resp, cerr = c.Acknowledge(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},

		// ── ModifyAckDeadline: extend the deadline of a pulled message ───────
		{
			Service: "pubsub", Op: "msg_grpc_modify_ack_deadline",
			Method: "google.pubsub.v1.Subscriber/ModifyAckDeadline", Path: modSub,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				if err := ensureGRPCPubSub(ctx, t, topic, modSub, false); err != nil {
					return nil, nil, err
				}
				if err := pubsubPublish(ctx, t, topic, "grpc-modack", ""); err != nil {
					return nil, nil, err
				}
				pulled, err := pubsubPullUntil(ctx, t, modSub)
				if err != nil {
					return nil, nil, err
				}
				req := &pubsubpb.ModifyAckDeadlineRequest{Subscription: modSub, AckIds: ackIDs(pulled), AckDeadlineSeconds: 30}
				var resp *emptypb.Empty
				err = withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
					var cerr error
					resp, cerr = c.ModifyAckDeadline(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},

		// ── CreateSnapshot: capture a subscription's backlog ─────────────────
		{
			Service: "pubsub", Op: "msg_grpc_create_snapshot",
			Method: "google.pubsub.v1.Subscriber/CreateSnapshot", Path: seekSnap,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				if err := ensureGRPCPubSub(ctx, t, topic, seekSub, false); err != nil {
					return nil, nil, err
				}
				if err := pubsubPublish(ctx, t, topic, "grpc-snapshot", ""); err != nil {
					return nil, nil, err
				}
				if _, err := pubsubPullUntil(ctx, t, seekSub); err != nil {
					return nil, nil, err
				}
				req := &pubsubpb.CreateSnapshotRequest{Name: seekSnap, Subscription: seekSub}
				var resp *pubsubpb.Snapshot
				err := withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
					var cerr error
					resp, cerr = c.CreateSnapshot(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},

		// ── Seek: reset the subscription to the snapshot ────────────────────
		{
			Service: "pubsub", Op: "msg_grpc_seek",
			Method: "google.pubsub.v1.Subscriber/Seek", Path: seekSub,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				if err := ensureGRPCPubSub(ctx, t, topic, seekSub, false); err != nil {
					return nil, nil, err
				}
				if err := ensureGRPCSnapshot(ctx, t, seekSnap, seekSub); err != nil {
					return nil, nil, err
				}
				req := &pubsubpb.SeekRequest{Subscription: seekSub, Target: &pubsubpb.SeekRequest_Snapshot{Snapshot: seekSnap}}
				var resp *pubsubpb.SeekResponse
				err := withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
					var cerr error
					resp, cerr = c.Seek(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},

		// ── Pull after seek: the snapshot's message is restored (redelivered) ─
		{
			Service: "pubsub", Op: "msg_grpc_seek_pull",
			Method: "google.pubsub.v1.Subscriber/Pull", Path: seekSub,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				if err := ensureGRPCPubSub(ctx, t, topic, seekSub, false); err != nil {
					return nil, nil, err
				}
				if err := ensureGRPCSnapshot(ctx, t, seekSnap, seekSub); err != nil {
					return nil, nil, err
				}
				if err := pubsubSeek(ctx, t, seekSub, seekSnap); err != nil {
					return nil, nil, err
				}
				req := &pubsubpb.PullRequest{Subscription: seekSub, MaxMessages: 1}
				resp, err := pubsubPullUntil(ctx, t, seekSub)
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},

		// ── StreamingPull: bounded, message-level capture ────────────────────
		{
			Service: "pubsub", Op: "msg_grpc_streaming_pull",
			Method: "google.pubsub.v1.Subscriber/StreamingPull", Path: streamSub,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				if err := ensureGRPCPubSub(ctx, t, streamTopic, streamSub, false); err != nil {
					return nil, nil, err
				}
				if err := pubsubPublish(ctx, t, streamTopic, "grpc-stream", ""); err != nil {
					return nil, nil, err
				}
				frames, err := pubsubStreamMessages(ctx, t, streamSub)
				if err != nil {
					return nil, nil, err
				}
				return &pubsubpb.StreamingPullRequest{Subscription: streamSub}, frames, nil
			},
		},

		// ── DeadLetter (PSM2): publish → nack past maxDeliveryAttempts → pull
		//    the dead-letter subscription. The forwarded copy carries real GCP's
		//    CloudPubSubDeadLetterSource* attributes; the nack loop is
		//    best-effort and not itself captured. ─────────────────────────────
		{
			Service: "pubsub", Op: "msg_grpc_dead_letter",
			Method: "google.pubsub.v1.Subscriber/Pull", Path: dlqSub,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				if err := ensureGRPCPubSubDLQ(ctx, t, dlqMainTopic, dlqMainSub, dlqTopic, dlqSub); err != nil {
					return nil, nil, err
				}
				if err := pubsubPublish(ctx, t, dlqMainTopic, "grpc-dlq", ""); err != nil {
					return nil, nil, err
				}
				if err := pubsubNackLoop(ctx, t, dlqMainSub); err != nil {
					return nil, nil, err
				}
				req := &pubsubpb.PullRequest{Subscription: dlqSub, MaxMessages: 1}
				resp, err := pubsubPullUntil(ctx, t, dlqSub)
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
	}
}

// withPubSubPublisher dials the target's Pub/Sub endpoint and runs fn with the
// publisher stub, closing the connection afterwards.
func withPubSubPublisher(ctx context.Context, t *GRPCTarget, fn func(pubsubpb.PublisherClient) error) error {
	conn, err := t.dial(ctx, "pubsub")
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(pubsubpb.NewPublisherClient(conn))
}

// withPubSubSubscriber dials the target's Pub/Sub endpoint and runs fn with the
// subscriber stub, closing the connection afterwards.
func withPubSubSubscriber(ctx context.Context, t *GRPCTarget, fn func(pubsubpb.SubscriberClient) error) error {
	conn, err := t.dial(ctx, "pubsub")
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(pubsubpb.NewSubscriberClient(conn))
}

// ensureGRPCPubSub makes a topic and its subscription exist (idempotently), so
// every scenario is self-contained and re-runnable. An ALREADY_EXISTS response
// is expected on replay of a prior scenario in the same run.
func ensureGRPCPubSub(ctx context.Context, t *GRPCTarget, topic, sub string, ordered bool) error {
	err := withPubSubPublisher(ctx, t, func(c pubsubpb.PublisherClient) error {
		_, e := c.CreateTopic(ctx, &pubsubpb.Topic{Name: topic})
		if status.Code(e) == codes.AlreadyExists {
			return nil
		}
		return e
	})
	if err != nil {
		return err
	}
	return withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
		s := &pubsubpb.Subscription{Name: sub, Topic: topic, AckDeadlineSeconds: 10}
		if ordered {
			s.EnableMessageOrdering = true
		}
		_, e := c.CreateSubscription(ctx, s)
		if status.Code(e) == codes.AlreadyExists {
			return nil
		}
		return e
	})
}

// ensureGRPCSnapshot creates a snapshot of a subscription's backlog if it does
// not already exist.
func ensureGRPCSnapshot(ctx context.Context, t *GRPCTarget, snap, sub string) error {
	return withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
		_, e := c.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{Name: snap, Subscription: sub})
		if status.Code(e) == codes.AlreadyExists {
			return nil
		}
		return e
	})
}

// ensureGRPCPubSubDLQ creates the PSM2 dead-letter topology: a dead-letter
// topic and its subscription, and a source topic and subscription configured
// with a deadLetterPolicy (maxDeliveryAttempts=5, the proto minimum).
// ALREADY_EXISTS is expected on replay of a prior run.
func ensureGRPCPubSubDLQ(ctx context.Context, t *GRPCTarget, topic, sub, dlqTopic, dlqSub string) error {
	err := withPubSubPublisher(ctx, t, func(c pubsubpb.PublisherClient) error {
		for _, tp := range []string{topic, dlqTopic} {
			if _, e := c.CreateTopic(ctx, &pubsubpb.Topic{Name: tp}); status.Code(e) != codes.OK && status.Code(e) != codes.AlreadyExists {
				return e
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
		if _, e := c.CreateSubscription(ctx, &pubsubpb.Subscription{Name: dlqSub, Topic: dlqTopic, AckDeadlineSeconds: 10}); status.Code(e) != codes.OK && status.Code(e) != codes.AlreadyExists {
			return e
		}
		_, e := c.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name: sub, Topic: topic, AckDeadlineSeconds: 10,
			DeadLetterPolicy: &pubsubpb.DeadLetterPolicy{DeadLetterTopic: dlqTopic, MaxDeliveryAttempts: 5},
		})
		if status.Code(e) != codes.OK && status.Code(e) != codes.AlreadyExists {
			return e
		}
		return nil
	})
}

// pubsubNackLoop pulls and immediately nacks the source subscription until the
// message has been forwarded to the dead-letter topic (a pull comes back
// empty). Bounded so a slow forward cannot hang a capture; the empty iterations
// are what the bound covers. It is best-effort state setup, not a diffed
// contract.
func pubsubNackLoop(ctx context.Context, t *GRPCTarget, sub string) error {
	for i := 0; i < 8; i++ {
		var pulled []string
		err := withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
			resp, cerr := c.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1, ReturnImmediately: true})
			if cerr != nil {
				return cerr
			}
			for _, rm := range resp.GetReceivedMessages() {
				pulled = append(pulled, rm.GetAckId())
			}
			if len(pulled) == 0 {
				return nil
			}
			_, cerr = c.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{Subscription: sub, AckIds: pulled, AckDeadlineSeconds: 0})
			return cerr
		})
		if err != nil {
			return err
		}
		if len(pulled) == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	return nil
}

// pubsubPublish publishes one message to a topic.
func pubsubPublish(ctx context.Context, t *GRPCTarget, topic, data, orderingKey string) error {
	return withPubSubPublisher(ctx, t, func(c pubsubpb.PublisherClient) error {
		_, e := c.Publish(ctx, &pubsubpb.PublishRequest{
			Topic:    topic,
			Messages: []*pubsubpb.PubsubMessage{{Data: []byte(data), OrderingKey: orderingKey}},
		})
		return e
	})
}

// pubsubPullUntil pulls until at least one message is delivered or the bounded
// window elapses. Real GCP delivers a just-published message asynchronously, so
// a single immediate pull can race; polling keeps the capture deterministic.
func pubsubPullUntil(ctx context.Context, t *GRPCTarget, sub string) (*pubsubpb.PullResponse, error) {
	deadline := time.Now().Add(t.Timeout)
	var last *pubsubpb.PullResponse
	for {
		var resp *pubsubpb.PullResponse
		err := withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
			var cerr error
			resp, cerr = c.Pull(ctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: 1})
			return cerr
		})
		if err != nil {
			return nil, err
		}
		last = resp
		if len(resp.GetReceivedMessages()) > 0 {
			return resp, nil
		}
		if time.Now().After(deadline) {
			return last, nil
		}
		time.Sleep(time.Second)
	}
}

// pubsubSeek resets a subscription to a snapshot.
func pubsubSeek(ctx context.Context, t *GRPCTarget, sub, snap string) error {
	return withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
		_, e := c.Seek(ctx, &pubsubpb.SeekRequest{Subscription: sub, Target: &pubsubpb.SeekRequest_Snapshot{Snapshot: snap}})
		return e
	})
}

// ackIDs returns the ack ids of a pull response, so an ack/modack reintroduces
// exactly the values the server minted (which the normalizer folds to <ackId>).
func ackIDs(resp *pubsubpb.PullResponse) []string {
	ids := make([]string, 0, len(resp.GetReceivedMessages()))
	for _, rm := range resp.GetReceivedMessages() {
		ids = append(ids, rm.GetAckId())
	}
	return ids
}

// pubsubStreamMessages opens a StreamingPull, drains it until one message is
// delivered or the window elapses, cancels, and returns the delivered messages
// as a synthetic {"messages": [...]} proto (mirroring the Firestore RunQuery
// frames capture). Frame boundaries are intentionally not part of the contract.
func pubsubStreamMessages(ctx context.Context, t *GRPCTarget, sub string) (*structpb.Struct, error) {
	conn, err := t.dial(ctx, "pubsub")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pubsubpb.NewSubscriberClient(conn)

	sctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	stream, err := client.StreamingPull(sctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&pubsubpb.StreamingPullRequest{Subscription: sub, StreamAckDeadlineSeconds: 10}); err != nil {
		return nil, err
	}

	var msgs []any
	deadline := time.Now().Add(15 * time.Second)
	for len(msgs) == 0 && time.Now().Before(deadline) {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		for _, rm := range resp.GetReceivedMessages() {
			b, merr := marshalProtoJSON(rm.GetMessage())
			if merr != nil {
				return nil, merr
			}
			var m any
			if jerr := json.Unmarshal(b, &m); jerr != nil {
				return nil, jerr
			}
			msgs = append(msgs, m)
		}
	}
	_ = stream.CloseSend()
	if msgs == nil {
		msgs = []any{}
	}
	return structpb.NewStruct(map[string]any{"messages": msgs})
}

// CleanupPubSubGRPC deletes every Pub/Sub resource the gRPC messaging scenarios
// create (snapshot, then subscriptions, then topics), so a capture or replay
// leaves nothing behind.
func (t *GRPCTarget) CleanupPubSubGRPC(ctx context.Context) []string {
	n := t.Names
	base := "projects/" + t.Project
	type target struct{ kind, name string }
	targets := []target{
		{"snapshot", base + "/snapshots/" + n.SeekSnap},
		{"subscription", base + "/subscriptions/" + n.MsgSub},
		{"subscription", base + "/subscriptions/" + n.OrderSub},
		{"subscription", base + "/subscriptions/" + n.AckSub},
		{"subscription", base + "/subscriptions/" + n.ModSub},
		{"subscription", base + "/subscriptions/" + n.SeekSub},
		{"subscription", base + "/subscriptions/" + n.StreamSub},
		{"subscription", base + "/subscriptions/" + n.DLQMainSub},
		{"subscription", base + "/subscriptions/" + n.DLQSub},
		{"topic", base + "/topics/" + n.MsgTopic},
		{"topic", base + "/topics/" + n.OrderTopic},
		{"topic", base + "/topics/" + n.StreamTopic},
		{"topic", base + "/topics/" + n.DLQMainTopic},
		{"topic", base + "/topics/" + n.DLQTopic},
	}
	var log []string
	for _, tg := range targets {
		err := withPubSubSubscriber(ctx, t, func(c pubsubpb.SubscriberClient) error {
			switch tg.kind {
			case "snapshot":
				_, e := c.DeleteSnapshot(ctx, &pubsubpb.DeleteSnapshotRequest{Snapshot: tg.name})
				return e
			default:
				_, e := c.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: tg.name})
				return e
			}
		})
		if err != nil {
			// A subscription delete may 404; fall through to the publisher for
			// topics, which the subscriber client cannot delete.
			_ = err
		}
		if tg.kind == "topic" {
			perr := withPubSubPublisher(ctx, t, func(c pubsubpb.PublisherClient) error {
				_, e := c.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: tg.name})
				return e
			})
			if perr != nil {
				log = append(log, fmt.Sprintf("cleanup pubsub %s %s: %v", tg.kind, tg.name, perr))
			} else {
				log = append(log, fmt.Sprintf("cleanup pubsub %s %s: ok", tg.kind, tg.name))
			}
			continue
		}
		if err != nil {
			log = append(log, fmt.Sprintf("cleanup pubsub %s %s: %v", tg.kind, tg.name, err))
		} else {
			log = append(log, fmt.Sprintf("cleanup pubsub %s %s: ok", tg.kind, tg.name))
		}
	}
	return log
}
