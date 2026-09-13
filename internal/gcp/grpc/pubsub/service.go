// Package pubsub implements the Cloud Pub/Sub gRPC services (Publisher,
// Subscriber, and the google.iam.v1.IAMPolicy surface for topic/subscription
// IAM) over the same shared store.ResourceStore + pubsubstore.Messages + policy
// + crypto.EnvelopeEncryptor backing the REST provider, so REST and gRPC share
// state.
package pubsub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/pubsubfilter"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Resource-type strings mirror the REST provider's persisted types so both
// transports share the same control-plane entries.
const (
	rtTopic              = "gcp_topic"
	rtSubscription       = "gcp_subscription"
	rtTopicPolicy        = "gcp_topic_policy"
	rtSubscriptionPolicy = "gcp_subscription_policy"

	// longPollTimeout bounds the returnImmediately=false poll window. Real GCP
	// waits ~20s for a message; the emulator uses a 1s window so empty-pull
	// tests don't hang.
	longPollTimeout  = time.Second
	longPollInterval = 50 * time.Millisecond

	// streamPullBatch bounds how many messages StreamingPull claims per poll so
	// the stream keeps up with flow-control without over-claiming.
	streamPullBatch = 100
)

// pushClient bounds push-subscription delivery so a hung or slow endpoint
// cannot block a publish request indefinitely.
var pushClient = &http.Client{Timeout: 10 * time.Second}

// Service implements pubsubpb.PublisherServer, pubsubpb.SubscriberServer, and
// iampb.IAMPolicyServer over the shared stores.
type Service struct {
	pubsubpb.UnimplementedPublisherServer
	pubsubpb.UnimplementedSubscriberServer
	iampb.UnimplementedIAMPolicyServer

	resources   store.ResourceStore  // topics + subscriptions (control-plane)
	messages    pubsubstore.Messages // published messages (data plane)
	encryptor   crypto.EnvelopeEncryptor
	defaultProj string
}

// NewService returns a Pub/Sub gRPC service backed by the shared stores.
// defaultProj is the config-default project used when a request carries none.
func NewService(resources store.ResourceStore, messages pubsubstore.Messages, encryptor crypto.EnvelopeEncryptor, defaultProj string) *Service {
	return &Service{resources: resources, messages: messages, encryptor: encryptor, defaultProj: defaultProj}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// ─── resource-name parsing ────────────────────────────────────────────────────

func topicName(project, id string) string {
	return "projects/" + project + "/topics/" + id
}

func subscriptionName(project, id string) string {
	return "projects/" + project + "/subscriptions/" + id
}

// splitTopicName parses "projects/{p}/topics/{t}".
func splitTopicName(name string) (project, topic string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "topics" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// splitSubscriptionName parses "projects/{p}/subscriptions/{s}".
func splitSubscriptionName(name string) (project, sub string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "subscriptions" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// projectFromListProject resolves the project for a List* request, which
// carries "projects/{p}" (or a bare project id) in the Project field.
func (s *Service) projectFromListProject(ctx context.Context, p string) string {
	if p != "" {
		return strings.TrimPrefix(p, "projects/")
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// ─── Publisher ────────────────────────────────────────────────────────────────

func (s *Service) CreateTopic(ctx context.Context, req *pubsubpb.Topic) (*pubsubpb.Topic, error) {
	project, t, ok := splitTopicName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid topic name", 400))
	}
	if project == "" {
		project = grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
	}
	meta := map[string]any{"name": topicName(project, t)}
	if req.GetMessageRetentionDuration() != nil {
		meta["messageRetentionDuration"] = req.GetMessageRetentionDuration().AsDuration().String()
	}
	if k := req.GetKmsKeyName(); k != "" {
		meta["kmsKeyName"] = k
	}
	if len(req.GetLabels()) > 0 {
		meta["labels"] = req.GetLabels()
	}
	data, _ := json.Marshal(meta)
	if err := s.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtTopic, ID: t, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, mapError(model.NewProviderError("AlreadyExists", "topic already exists", 409))
		}
		return nil, mapError(err)
	}
	return topicToProto(project, t, meta), nil
}

func (s *Service) GetTopic(ctx context.Context, req *pubsubpb.GetTopicRequest) (*pubsubpb.Topic, error) {
	project, t, ok := splitTopicName(req.GetTopic())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid topic name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtTopic, t)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "topic not found", 404))
		}
		return nil, mapError(err)
	}
	var m map[string]any
	json.Unmarshal(e.Data, &m)
	if m == nil {
		m = map[string]any{"name": topicName(project, t)}
	}
	return topicToProto(project, t, m), nil
}

func (s *Service) ListTopics(ctx context.Context, req *pubsubpb.ListTopicsRequest) (*pubsubpb.ListTopicsResponse, error) {
	project := s.projectFromListProject(ctx, req.GetProject())
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtTopic, "")
	if err != nil {
		return nil, mapError(err)
	}
	page, nextToken := paging.Apply(entries, map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	topics := make([]*pubsubpb.Topic, 0, len(page))
	for _, e := range page {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil {
			topics = append(topics, topicToProto(project, e.ID, m))
		}
	}
	return &pubsubpb.ListTopicsResponse{Topics: topics, NextPageToken: nextToken}, nil
}

func (s *Service) DeleteTopic(ctx context.Context, req *pubsubpb.DeleteTopicRequest) (*emptypb.Empty, error) {
	project, t, ok := splitTopicName(req.GetTopic())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid topic name", 400))
	}
	if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtTopic, t); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "topic not found", 404))
		}
		return nil, mapError(err)
	}
	// Existing subscriptions are not deleted; their topic is set to the
	// sentinel "_deleted-topic_" (real Pub/Sub behaviour).
	topicFull := topicName(project, t)
	if entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtSubscription, ""); err == nil {
		for _, e := range entries {
			var meta map[string]any
			if json.Unmarshal(e.Data, &meta) != nil {
				continue
			}
			if st, _ := meta["topic"].(string); st != topicFull {
				continue
			}
			meta["topic"] = "_deleted-topic_"
			data, _ := json.Marshal(meta)
			_ = s.resources.Update(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: e.ID, Data: data})
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) Publish(ctx context.Context, req *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
	project, t, ok := splitTopicName(req.GetTopic())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid topic name", 400))
	}
	topicEntry, err := s.resources.Get(ctx, project, store.GlobalRegion, rtTopic, t)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "topic not found", 404))
		}
		return nil, mapError(err)
	}
	kmsKeyName := ""
	var topicMeta map[string]any
	if json.Unmarshal(topicEntry.Data, &topicMeta) == nil {
		kmsKeyName, _ = topicMeta["kmsKeyName"].(string)
	}

	publishTime := clock.Now()
	ids := make([]string, 0, len(req.GetMessages()))
	stored := make([]pubsubstore.Message, 0, len(req.GetMessages()))
	plainData := make([]string, 0, len(req.GetMessages()))
	for _, m := range req.GetMessages() {
		id, err := s.messages.NextID(ctx)
		if err != nil {
			return nil, mapError(err)
		}
		// Envelope-encrypt the payload: AES-GCM with a fresh DEK, wrapping the
		// DEK under the topic's CMEK key (or the server DEK when no key).
		rawDEK, wrappedDEK, err := s.encryptor.Wrap(ctx, project, kmsKeyName)
		if err != nil {
			return nil, mapError(err)
		}
		ciphertext, err := kmsstore.EncryptData(rawDEK, m.GetData(), nil)
		if err != nil {
			return nil, mapError(err)
		}

		msg := pubsubstore.Message{
			Topic: t, MessageID: id, Data: base64.StdEncoding.EncodeToString(ciphertext),
			Attributes: m.GetAttributes(), PublishTime: publishTime,
			KmsKeyName: kmsKeyName, WrappedDEK: wrappedDEK,
		}
		if ok := m.GetOrderingKey(); ok != "" {
			msg.OrderingKey = ok
		}
		stored = append(stored, msg)
		plainData = append(plainData, base64.StdEncoding.EncodeToString(m.GetData()))
		ids = append(ids, id)
	}

	// Fan out: a copy per pull subscription so every subscription receives
	// every message with independent delivery/ack state.
	subIDs := s.pullSubscriptionIDs(ctx, project, t)
	for _, msg := range stored {
		for _, sid := range subIDs {
			copy := msg
			copy.Subscription = sid
			if err := s.messages.Put(ctx, copy); err != nil {
				return nil, mapError(err)
			}
		}
	}

	topicFull := topicName(project, t)
	for i, msg := range stored {
		s.deliverPush(ctx, project, topicFull, msg, plainData[i])
	}
	return &pubsubpb.PublishResponse{MessageIds: ids}, nil
}

// deliverPush POSTs a message to every push subscription of the topic (SNS
// deliverToHTTP analogue).
func (s *Service) deliverPush(ctx context.Context, accountID, topicFull string, msg pubsubstore.Message, data string) {
	entries, err := s.resources.List(ctx, accountID, store.GlobalRegion, rtSubscription, "")
	if err != nil {
		return
	}
	for _, e := range entries {
		var sub map[string]any
		if json.Unmarshal(e.Data, &sub) != nil {
			continue
		}
		if st, _ := sub["topic"].(string); st != topicFull {
			continue
		}
		pc, _ := sub["pushConfig"].(map[string]any)
		endpoint, _ := pc["pushEndpoint"].(string)
		if endpoint == "" {
			continue
		}
		payload := map[string]any{
			"message": map[string]any{
				"messageId":   msg.MessageID,
				"data":        data,
				"publishTime": msg.PublishTime.UTC().Format("2006-01-02T15:04:05.000000Z"),
			},
			"subscription": sub["name"],
		}
		if len(msg.Attributes) > 0 {
			payload["message"].(map[string]any)["attributes"] = msg.Attributes
		}
		body, _ := json.Marshal(payload)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		if resp, err := pushClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}
}

// ─── Subscriber ───────────────────────────────────────────────────────────────

func (s *Service) CreateSubscription(ctx context.Context, req *pubsubpb.Subscription) (*pubsubpb.Subscription, error) {
	project, sub, ok := splitSubscriptionName(req.GetName())
	if !ok || sub == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid subscription name", 400))
	}
	if project == "" {
		project = grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
	}
	// The topic must already exist (real Pub/Sub returns NotFound otherwise).
	_, topicID, ok := splitTopicName(req.GetTopic())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid topic name", 400))
	}
	if _, err := s.resources.Get(ctx, project, store.GlobalRegion, rtTopic, topicID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "topic not found", 404))
		}
		return nil, mapError(err)
	}
	ackDeadline := 10
	if req.GetAckDeadlineSeconds() != 0 {
		ackDeadline = int(req.GetAckDeadlineSeconds())
	}
	meta := map[string]any{
		"name":               subscriptionName(project, sub),
		"topic":              req.GetTopic(),
		"ackDeadlineSeconds": ackDeadline,
	}
	if f := req.GetFilter(); f != "" {
		if _, err := pubsubfilter.Compile(f); err != nil {
			return nil, mapError(model.NewProviderError("InvalidArgument", "invalid subscription filter: "+err.Error(), 400))
		}
		meta["filter"] = f
	}
	if len(req.GetLabels()) > 0 {
		meta["labels"] = req.GetLabels()
	}
	if dlp := req.GetDeadLetterPolicy(); dlp != nil {
		meta["deadLetterPolicy"] = map[string]any{
			"deadLetterTopic":     dlp.GetDeadLetterTopic(),
			"maxDeliveryAttempts": int(dlp.GetMaxDeliveryAttempts()),
		}
	}
	if pc := req.GetPushConfig(); pc != nil && pc.GetPushEndpoint() != "" {
		meta["pushConfig"] = map[string]any{"pushEndpoint": pc.GetPushEndpoint()}
	}
	data, _ := json.Marshal(meta)
	if err := s.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: sub, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, mapError(model.NewProviderError("AlreadyExists", "subscription already exists", 409))
		}
		return nil, mapError(err)
	}
	return subToProto(meta), nil
}

func (s *Service) GetSubscription(ctx context.Context, req *pubsubpb.GetSubscriptionRequest) (*pubsubpb.Subscription, error) {
	project, sub, ok := splitSubscriptionName(req.GetSubscription())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid subscription name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, sub)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "subscription not found", 404))
		}
		return nil, mapError(err)
	}
	var m map[string]any
	json.Unmarshal(e.Data, &m)
	return subToProto(m), nil
}

func (s *Service) ListSubscriptions(ctx context.Context, req *pubsubpb.ListSubscriptionsRequest) (*pubsubpb.ListSubscriptionsResponse, error) {
	project := s.projectFromListProject(ctx, req.GetProject())
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtSubscription, "")
	if err != nil {
		return nil, mapError(err)
	}
	page, nextToken := paging.Apply(entries, map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	subs := make([]*pubsubpb.Subscription, 0, len(page))
	for _, e := range page {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil {
			subs = append(subs, subToProto(m))
		}
	}
	return &pubsubpb.ListSubscriptionsResponse{Subscriptions: subs, NextPageToken: nextToken}, nil
}

func (s *Service) DeleteSubscription(ctx context.Context, req *pubsubpb.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	project, sub, ok := splitSubscriptionName(req.GetSubscription())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid subscription name", 400))
	}
	if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtSubscription, sub); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "subscription not found", 404))
		}
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) Pull(ctx context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
	project, sub, ok := splitSubscriptionName(req.GetSubscription())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid subscription name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, sub)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "subscription not found", 404))
		}
		return nil, mapError(err)
	}
	var meta map[string]any
	json.Unmarshal(e.Data, &meta)
	if detached, _ := meta["detached"].(bool); detached {
		return nil, mapError(&model.ProviderError{
			Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
			Message: "subscription " + sub + " is detached",
		})
	}
	subFilter, _ := meta["filter"].(string)
	topic, _ := meta["topic"].(string)
	topicID := topic
	if i := strings.LastIndex(topicID, "/"); i >= 0 {
		topicID = topicID[i+1:]
	}

	// Dead-letter policy (mirrors SQS RedrivePolicy/maxReceiveCount).
	dlqTopic := ""
	maxDeliveryAttempts := 0
	if dp, ok := meta["deadLetterPolicy"].(map[string]any); ok {
		if dt, _ := dp["deadLetterTopic"].(string); dt != "" {
			dlqTopic = dt
			if i := strings.LastIndex(dlqTopic, "/"); i >= 0 {
				dlqTopic = dlqTopic[i+1:]
			}
		}
		if mda, ok := dp["maxDeliveryAttempts"].(float64); ok {
			maxDeliveryAttempts = int(mda)
		}
	}

	ackDeadline := 10
	if ad, ok := meta["ackDeadlineSeconds"].(float64); ok {
		ackDeadline = int(ad)
	}
	retention := s.topicRetention(ctx, project, topicID)

	maxMsgs := 100
	if req.GetMaxMessages() > 0 {
		maxMsgs = int(req.GetMaxMessages())
	}

	var msgs []pubsubstore.Message
	if req.GetReturnImmediately() {
		msgs, err = s.messages.Pull(ctx, sub, maxMsgs, ackDeadline, retention, clock.Now())
	} else {
		msgs, err = s.longPoll(ctx, sub, maxMsgs, ackDeadline, retention)
	}
	if err != nil {
		return nil, mapError(err)
	}

	msgs = s.filterMessages(ctx, sub, subFilter, msgs)
	received, err := s.buildReceivedMessages(ctx, project, sub, dlqTopic, maxDeliveryAttempts, msgs)
	if err != nil {
		return nil, err
	}
	return &pubsubpb.PullResponse{ReceivedMessages: received}, nil
}

// buildReceivedMessages transcodes claimed messages into wire ReceivedMessages,
// applying DLQ republish + envelope decryption. Shared by Pull and StreamingPull
// so both transports emit the same ack-id scheme and payload shape.
func (s *Service) buildReceivedMessages(ctx context.Context, project, queue, dlqTopic string, maxDeliveryAttempts int, msgs []pubsubstore.Message) ([]*pubsubpb.ReceivedMessage, error) {
	received := make([]*pubsubpb.ReceivedMessage, 0, len(msgs))
	for _, m := range msgs {
		// DLQ: once delivery attempts exceed maxDeliveryAttempts, republish to
		// the dead-letter topic and drop the original.
		if dlqTopic != "" && maxDeliveryAttempts > 0 && m.DeliveryAttempt > maxDeliveryAttempts {
			_ = s.messages.Delete(ctx, queue, m.MessageID)
			for _, sid := range s.pullSubscriptionIDs(ctx, project, dlqTopic) {
				_ = s.messages.Put(ctx, pubsubstore.Message{
					Topic: dlqTopic, Subscription: sid, MessageID: m.MessageID, Data: m.Data, Attributes: m.Attributes,
					PublishTime: m.PublishTime, DeliveryAttempt: 0,
					KmsKeyName: m.KmsKeyName, WrappedDEK: m.WrappedDEK,
				})
			}
			continue
		}

		// Decrypt the stored ciphertext back to the plaintext payload.
		rawDEK, err := s.encryptor.Unwrap(ctx, project, m.KmsKeyName, m.WrappedDEK)
		if err != nil {
			return nil, mapError(err)
		}
		ciphertext, err := base64.StdEncoding.DecodeString(m.Data)
		if err != nil {
			return nil, mapError(err)
		}
		plain, err := kmsstore.DecryptData(rawDEK, ciphertext, nil)
		if err != nil {
			return nil, mapError(err)
		}

		pm := &pubsubpb.PubsubMessage{
			MessageId:   m.MessageID,
			Data:        plain,
			PublishTime: timestamppb.New(m.PublishTime),
		}
		if len(m.Attributes) > 0 {
			pm.Attributes = m.Attributes
		}
		if m.OrderingKey != "" {
			pm.OrderingKey = m.OrderingKey
		}
		received = append(received, &pubsubpb.ReceivedMessage{
			AckId:           encodeAckID(queue, m.MessageID),
			Message:         pm,
			DeliveryAttempt: int32(m.DeliveryAttempt),
		})
	}
	return received, nil
}

// longPoll repeatedly claims messages until at least one is deliverable or the
// bounded window elapses.
func (s *Service) longPoll(ctx context.Context, queue string, maxMsgs, ackDeadline, retention int) ([]pubsubstore.Message, error) {
	deadline := clock.Now().Add(longPollTimeout)
	for {
		msgs, err := s.messages.Pull(ctx, queue, maxMsgs, ackDeadline, retention, clock.Now())
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 || !clock.Now().Before(deadline) {
			return msgs, nil
		}
		time.Sleep(longPollInterval)
	}
}

// StreamingPull implements the bidirectional streaming pull used by the
// official Go SDK's Subscription.Receive. It mirrors unary Pull's
// subscription→topic resolution and ack-deadline handling, but streams messages
// back continuously and folds the client's ackIds / modifyDeadline* fields into
// the same store operations as Acknowledge / ModifyAckDeadline.
func (s *Service) StreamingPull(stream pubsubpb.Subscriber_StreamingPullServer) error {
	ctx := stream.Context()

	// The first request carries the subscription name.
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}

	project, sub, ok := splitSubscriptionName(first.GetSubscription())
	if !ok {
		return mapError(model.NewProviderError("InvalidArgument", "invalid subscription name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, sub)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return mapError(model.NewProviderError("NotFound", "subscription not found", 404))
		}
		return mapError(err)
	}
	var meta map[string]any
	json.Unmarshal(e.Data, &meta)
	if detached, _ := meta["detached"].(bool); detached {
		return mapError(&model.ProviderError{
			Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
			Message: "subscription " + sub + " is detached",
		})
	}
	subFilter, _ := meta["filter"].(string)
	topic, _ := meta["topic"].(string)
	topicID := topic
	if i := strings.LastIndex(topicID, "/"); i >= 0 {
		topicID = topicID[i+1:]
	}

	// Dead-letter policy (mirrors Pull).
	dlqTopic := ""
	maxDeliveryAttempts := 0
	if dp, ok := meta["deadLetterPolicy"].(map[string]any); ok {
		if dt, _ := dp["deadLetterTopic"].(string); dt != "" {
			dlqTopic = dt
			if i := strings.LastIndex(dlqTopic, "/"); i >= 0 {
				dlqTopic = dlqTopic[i+1:]
			}
		}
		if mda, ok := dp["maxDeliveryAttempts"].(float64); ok {
			maxDeliveryAttempts = int(mda)
		}
	}

	// Ack deadline: streamAckDeadlineSeconds wins over the subscription default.
	ackDeadline := 10
	if ad, ok := meta["ackDeadlineSeconds"].(float64); ok {
		ackDeadline = int(ad)
	}
	if first.GetStreamAckDeadlineSeconds() > 0 {
		ackDeadline = int(first.GetStreamAckDeadlineSeconds())
	}
	retention := s.topicRetention(ctx, project, topicID)

	// Flow control: honor the client's max_outstanding_messages /
	// max_outstanding_bytes (0 or omitted = unlimited). The counters are scoped
	// to this stream and track only messages this stream has sent and that are
	// still unacked. Per the proto, these fields are only valid on the initial
	// request; later values are ignored.
	flow := newStreamFlowControl(first.GetMaxOutstandingMessages(), first.GetMaxOutstandingBytes())

	// Handle the initial request's control fields.
	for _, id := range s.applyStreamAcks(ctx, sub, first) {
		flow.release(id)
	}
	for _, id := range s.applyStreamModifyDeadlines(ctx, sub, first) {
		flow.release(id)
	}

	// recvLoop consumes subsequent requests (acks / modify-deadlines) as they
	// arrive, so message delivery is never blocked on the client's control flow.
	recvErr := make(chan error, 1)
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			for _, id := range s.applyStreamAcks(ctx, sub, r) {
				flow.release(id)
			}
			for _, id := range s.applyStreamModifyDeadlines(ctx, sub, r) {
				flow.release(id)
			}
		}
	}()

	// sendLoop streams claimed messages until the stream is closed, never
	// claiming more than the client's declared flow-control budget allows.
	for {
		budget := flow.claimBudget(streamPullBatch)
		if budget <= 0 || flow.bytesExhausted() {
			// At the client's limit: release any messages acked out of band
			// (another stream / unary Acknowledge) and wait for a slot.
			if stored, err := s.messages.List(ctx, sub); err == nil {
				flow.reconcile(stored)
			}
			if flow.claimBudget(1) <= 0 || flow.bytesExhausted() {
				select {
				case <-ctx.Done():
					return nil
				case err := <-recvErr:
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				case <-flow.wake:
				case <-time.After(longPollInterval):
				}
				continue
			}
			budget = flow.claimBudget(streamPullBatch)
		}

		msgs, err := s.messages.Pull(ctx, sub, budget, ackDeadline, retention, clock.Now())
		if err != nil {
			return mapError(err)
		}
		if len(msgs) > 0 {
			msgs = s.filterMessages(ctx, sub, subFilter, msgs)
			received, err := s.buildReceivedMessages(ctx, project, sub, dlqTopic, maxDeliveryAttempts, msgs)
			if err != nil {
				return err
			}
			sendable := make([]*pubsubpb.ReceivedMessage, 0, len(received))
			for _, rm := range received {
				size := proto.Size(rm.GetMessage())
				// Let a lone oversized message through when nothing is
				// outstanding, so it cannot stall the stream forever.
				allowOverflow := len(sendable) == 0 && flow.outstandingCount() == 0
				if flow.reserve(rm.GetMessage().GetMessageId(), size, allowOverflow) {
					sendable = append(sendable, rm)
					continue
				}
				// Claimed but over the byte budget: make it visible again
				// immediately instead of stranding it until the ack deadline.
				if decoded, ok := decodeAckID(rm.GetAckId()); ok {
					if parts := strings.SplitN(decoded, "/", 2); len(parts) == 2 {
						_ = s.messages.ModifyAckDeadline(ctx, sub, []string{decoded}, 0, clock.Now())
					}
				}
			}
			if len(sendable) > 0 {
				if err := stream.Send(&pubsubpb.StreamingPullResponse{ReceivedMessages: sendable}); err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
				continue
			}
			// Everything this poll claimed was over the byte budget; fall
			// through to the wait so we don't busy-spin on re-claiming it.
		}

		// No sendable messages: long-poll by waiting a short interval rather
		// than busy-spinning, and stop promptly on stream close / context
		// cancel / a freed flow-control slot.
		select {
		case <-ctx.Done():
			return nil
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-flow.wake:
		case <-time.After(longPollInterval):
		}
	}
}

// applyStreamAcks acknowledges the ackIds in a StreamingPull request. It returns
// the decoded message IDs so the caller can release them from the stream's
// outstanding flow-control set.
func (s *Service) applyStreamAcks(ctx context.Context, _ string, req *pubsubpb.StreamingPullRequest) []string {
	var acked []string
	for _, a := range req.GetAckIds() {
		decoded, ok := decodeAckID(a)
		if !ok {
			continue
		}
		parts := strings.SplitN(decoded, "/", 2)
		if len(parts) == 2 {
			_ = s.messages.Delete(ctx, parts[0], parts[1])
			acked = append(acked, parts[1])
		}
	}
	return acked
}

// applyStreamModifyDeadlines applies the parallel modifyDeadlineAckIds /
// modifyDeadlineSeconds arrays, resetting each message's visibility deadline.
// A deadline of 0 is a nack (immediately redeliverable), so those IDs are
// returned for flow-control release; a positive extension keeps the message
// outstanding.
func (s *Service) applyStreamModifyDeadlines(ctx context.Context, queue string, req *pubsubpb.StreamingPullRequest) []string {
	ackIDs := req.GetModifyDeadlineAckIds()
	seconds := req.GetModifyDeadlineSeconds()
	n := len(seconds)
	if len(ackIDs) < n {
		n = len(ackIDs)
	}
	var nacked []string
	for i := 0; i < n; i++ {
		decoded, ok := decodeAckID(ackIDs[i])
		if !ok {
			continue
		}
		_ = s.messages.ModifyAckDeadline(ctx, queue, []string{decoded}, int(seconds[i]), clock.Now())
		if seconds[i] <= 0 {
			if parts := strings.SplitN(decoded, "/", 2); len(parts) == 2 {
				nacked = append(nacked, parts[1])
			}
		}
	}
	return nacked
}

func (s *Service) Acknowledge(ctx context.Context, req *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	for _, a := range req.GetAckIds() {
		decoded, ok := decodeAckID(a)
		if !ok {
			continue
		}
		parts := strings.SplitN(decoded, "/", 2)
		if len(parts) == 2 {
			_ = s.messages.Delete(ctx, parts[0], parts[1])
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) ModifyAckDeadline(ctx context.Context, req *pubsubpb.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	project, sub, ok := splitSubscriptionName(req.GetSubscription())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid subscription name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, sub)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "subscription not found", 404))
		}
		return nil, mapError(err)
	}
	var meta map[string]any
	json.Unmarshal(e.Data, &meta)
	decoded := make([]string, 0, len(req.GetAckIds()))
	for _, id := range req.GetAckIds() {
		if d, ok := decodeAckID(id); ok {
			decoded = append(decoded, d)
		}
	}
	if err := s.messages.ModifyAckDeadline(ctx, sub, decoded, int(req.GetAckDeadlineSeconds()), clock.Now()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// filterMessages drops (and deletes) messages that do not match a
// subscription's filter. Filters are immutable, so a non-matching message will
// never be delivered to this subscription and is discarded for it.
func (s *Service) filterMessages(ctx context.Context, queue, filterExpr string, msgs []pubsubstore.Message) []pubsubstore.Message {
	filter, err := pubsubfilter.Compile(filterExpr)
	if err != nil || filter == nil {
		return msgs
	}
	kept := make([]pubsubstore.Message, 0, len(msgs))
	for _, m := range msgs {
		if filter.Match(m.Attributes) {
			kept = append(kept, m)
		} else {
			_ = s.messages.Delete(ctx, queue, m.MessageID)
		}
	}
	return kept
}

// UpdateSubscription applies an update_mask to a subscription. The filter is
// immutable (rejected even when unchanged); labels and ack_deadline_seconds are
// supported.
func (s *Service) UpdateSubscription(ctx context.Context, req *pubsubpb.UpdateSubscriptionRequest) (*pubsubpb.Subscription, error) {
	in := req.GetSubscription()
	if in == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "subscription is required", 400))
	}
	project, id, ok := splitSubscriptionName(in.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid subscription name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "subscription not found", 404))
		}
		return nil, mapError(err)
	}
	var meta map[string]any
	json.Unmarshal(e.Data, &meta)

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = []string{"labels"}
	}
	for _, p := range paths {
		switch p {
		case "filter":
			return nil, mapError(model.NewProviderError("InvalidArgument",
				"subscription filter is immutable and cannot be updated", 400))
		case "labels":
			if len(in.GetLabels()) == 0 {
				delete(meta, "labels")
			} else {
				meta["labels"] = in.GetLabels()
			}
		case "ack_deadline_seconds", "ackDeadlineSeconds":
			meta["ackDeadlineSeconds"] = int(in.GetAckDeadlineSeconds())
		default:
			return nil, mapError(model.NewProviderError("InvalidArgument", "unsupported update_mask path: "+p, 400))
		}
	}
	data, _ := json.Marshal(meta)
	if err := s.resources.Update(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: id, Data: data}); err != nil {
		return nil, mapError(err)
	}
	return subToProto(meta), nil
}

// DetachSubscription detaches a subscription from its topic: it stops receiving
// messages, its backlog is dropped, and Pull returns FailedPrecondition. The
// subscription resource is retained (real Pub/Sub DetachSubscription).
func (s *Service) DetachSubscription(ctx context.Context, req *pubsubpb.DetachSubscriptionRequest) (*pubsubpb.DetachSubscriptionResponse, error) {
	project, id, ok := splitSubscriptionName(req.GetSubscription())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid subscription name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "subscription not found", 404))
		}
		return nil, mapError(err)
	}
	var meta map[string]any
	json.Unmarshal(e.Data, &meta)
	meta["detached"] = true
	data, _ := json.Marshal(meta)
	if err := s.resources.Update(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: id, Data: data}); err != nil {
		return nil, mapError(err)
	}
	if msgs, err := s.messages.List(ctx, id); err == nil {
		for _, m := range msgs {
			_ = s.messages.Delete(ctx, id, m.MessageID)
		}
	}
	return &pubsubpb.DetachSubscriptionResponse{}, nil
}

// ─── IAM (google.iam.v1.IAMPolicy over topics and subscriptions) ─────────────

// Owns reports whether the Pub/Sub service handles IAM for this resource name.
func (s *Service) Owns(resource string) bool {
	_, _, _, ok := s.parseIamResource(resource)
	return ok
}

// parseIamResource resolves a topic or subscription IAM resource name to its
// policy resource type + id (the underlying resource name disambiguates).
func (s *Service) parseIamResource(resource string) (project, policyType, id string, ok bool) {
	if p, t, ok := splitTopicName(resource); ok {
		return p, rtTopicPolicy, t, true
	}
	if p, sub, ok := splitSubscriptionName(resource); ok {
		return p, rtSubscriptionPolicy, sub, true
	}
	return "", "", "", false
}

// requireResource verifies the underlying topic/subscription exists before an
// IAM operation, mirroring the REST provider's requireTopic/requireSubscription.
func (s *Service) requireResource(ctx context.Context, project, policyType, id string) error {
	rt := rtTopic
	notFound := "topic not found"
	if policyType == rtSubscriptionPolicy {
		rt = rtSubscription
		notFound = "subscription not found"
	}
	if _, err := s.resources.Get(ctx, project, store.GlobalRegion, rt, id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return model.NewProviderError("NotFound", notFound, 404)
		}
		return err
	}
	return nil
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project, rt, id, ok := s.parseIamResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireResource(ctx, project, rt, id); err != nil {
		return nil, mapError(err)
	}
	return policyToProto(policy.Load(ctx, s.resources, project, rt, id)), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project, rt, id, ok := s.parseIamResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireResource(ctx, project, rt, id); err != nil {
		return nil, mapError(err)
	}
	pol, err := policy.Set(ctx, s.resources, project, rt, id, protoPolicyToBody(req.GetPolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	project, rt, id, ok := s.parseIamResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	// Fail open: testIamPermissions is a pure authz probe and does not require
	// the underlying resource to exist (unlike getIamPolicy/setIamPolicy). A
	// missing topic/subscription yields an empty permission list rather than
	// NotFound, matching real GCP and the REST provider.
	perms := []string{}
	if s.resourceExists(ctx, project, rt, id) {
		perms = policy.TestPermissions(req.GetPermissions())
	}
	return &iampb.TestIamPermissionsResponse{Permissions: perms}, nil
}

// resourceExists reports whether the underlying topic/subscription exists,
// without erroring (used by TestIamPermissions' fail-open behavior).
func (s *Service) resourceExists(ctx context.Context, project, policyType, id string) bool {
	rt := rtTopic
	if policyType == rtSubscriptionPolicy {
		rt = rtSubscription
	}
	_, err := s.resources.Get(ctx, project, store.GlobalRegion, rt, id)
	return err == nil
}

// ─── proto ↔ internal transcoding ─────────────────────────────────────────────

func topicToProto(project, id string, meta map[string]any) *pubsubpb.Topic {
	t := &pubsubpb.Topic{Name: topicName(project, id)}
	if k, _ := meta["kmsKeyName"].(string); k != "" {
		t.KmsKeyName = k
	}
	switch ls := meta["labels"].(type) {
	case map[string]string:
		if len(ls) > 0 {
			t.Labels = ls
		}
	case map[string]any:
		for k, v := range ls {
			if sv, ok := v.(string); ok {
				if t.Labels == nil {
					t.Labels = map[string]string{}
				}
				t.Labels[k] = sv
			}
		}
	}
	if ret, _ := meta["messageRetentionDuration"].(string); ret != "" {
		if d, err := time.ParseDuration(ret); err == nil {
			t.MessageRetentionDuration = durationpb.New(d)
		}
	}
	return t
}

func subToProto(meta map[string]any) *pubsubpb.Subscription {
	sub := &pubsubpb.Subscription{}
	sub.Name, _ = meta["name"].(string)
	sub.Topic, _ = meta["topic"].(string)
	if ad, ok := asInt(meta["ackDeadlineSeconds"]); ok {
		sub.AckDeadlineSeconds = int32(ad)
	}
	if f, _ := meta["filter"].(string); f != "" {
		sub.Filter = f
	}
	if detached, _ := meta["detached"].(bool); detached {
		sub.Detached = true
	}
	if labels, ok := meta["labels"].(map[string]any); ok {
		m := make(map[string]string, len(labels))
		for k, v := range labels {
			if s, ok := v.(string); ok {
				m[k] = s
			}
		}
		if len(m) > 0 {
			sub.Labels = m
		}
	} else if labels, ok := meta["labels"].(map[string]string); ok {
		sub.Labels = labels
	}
	if dlp, ok := meta["deadLetterPolicy"].(map[string]any); ok {
		p := &pubsubpb.DeadLetterPolicy{}
		if dt, _ := dlp["deadLetterTopic"].(string); dt != "" {
			p.DeadLetterTopic = dt
		}
		if mda, ok := asInt(dlp["maxDeliveryAttempts"]); ok {
			p.MaxDeliveryAttempts = int32(mda)
		}
		sub.DeadLetterPolicy = p
	}
	if pc, ok := meta["pushConfig"].(map[string]any); ok {
		p := &pubsubpb.PushConfig{}
		if ep, _ := pc["pushEndpoint"].(string); ep != "" {
			p.PushEndpoint = ep
		}
		sub.PushConfig = p
	}
	return sub
}

// asInt coerces a decoded JSON number (float64) or a freshly-built int into an
// int, so the transcoder works for both the store-read and just-created shapes.
func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return int(i), true
		}
	}
	return 0, false
}

func protoPolicyToBody(p *iampb.Policy) map[string]any {
	body := map[string]any{}
	if p == nil {
		return body
	}
	bindings := make([]any, 0, len(p.GetBindings()))
	for _, b := range p.GetBindings() {
		members := make([]any, 0, len(b.GetMembers()))
		for _, m := range b.GetMembers() {
			members = append(members, m)
		}
		bindings = append(bindings, map[string]any{"role": b.GetRole(), "members": members})
	}
	body["bindings"] = bindings
	if et := p.GetEtag(); len(et) > 0 {
		body["etag"] = string(et)
	}
	if p.GetVersion() != 0 {
		body["version"] = int(p.GetVersion())
	}
	return body
}

func policyToProto(p policy.Policy) *iampb.Policy {
	out := &iampb.Policy{Version: int32(p.Version)}
	if p.Etag != "" {
		out.Etag = []byte(p.Etag)
	}
	for _, b := range p.Bindings {
		m, ok := b.(map[string]any)
		if !ok {
			continue
		}
		binding := &iampb.Binding{}
		binding.Role, _ = m["role"].(string)
		for _, v := range toStrings(m["members"]) {
			binding.Members = append(binding.Members, v)
		}
		out.Bindings = append(out.Bindings, binding)
	}
	return out
}

func toStrings(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// ─── ack-id encoding (mirrors the REST provider's wire format) ────────────────

// encodeAckID renders the opaque wire ackId: base64url of "topicID/messageID".
func encodeAckID(topicID, messageID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(topicID + "/" + messageID))
}

// decodeAckID reverses encodeAckID, returning "topicID/messageID".
func decodeAckID(s string) (string, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return "", false
	}
	return string(raw), true
}

// topicRetention resolves a topic's messageRetentionDuration in seconds,
// defaulting to GCP's 7-day default.
// pullSubscriptionIDs returns the IDs of a topic's pull subscriptions. Push
// subscriptions are delivered over HTTP at publish time and are not queued.
func (s *Service) pullSubscriptionIDs(ctx context.Context, accountID, topicID string) []string {
	entries, err := s.resources.List(ctx, accountID, store.GlobalRegion, rtSubscription, "")
	if err != nil {
		return nil
	}
	var ids []string
	for _, e := range entries {
		var sub map[string]any
		if json.Unmarshal(e.Data, &sub) != nil {
			continue
		}
		st, _ := sub["topic"].(string)
		if lastSegment(st) != topicID {
			continue
		}
		if pc, _ := sub["pushConfig"].(map[string]any); pc != nil {
			if ep, _ := pc["pushEndpoint"].(string); ep != "" {
				continue
			}
		}
		ids = append(ids, e.ID)
	}
	return ids
}

func lastSegment(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}

func (s *Service) topicRetention(ctx context.Context, account, topicID string) int {
	const defaultRetentionSecs = 604800
	e, err := s.resources.Get(ctx, account, store.GlobalRegion, rtTopic, topicID)
	if err != nil {
		return defaultRetentionSecs
	}
	var meta map[string]any
	if json.Unmarshal(e.Data, &meta) != nil {
		return defaultRetentionSecs
	}
	ret, _ := meta["messageRetentionDuration"].(string)
	if d, err := time.ParseDuration(ret); err == nil && d > 0 {
		return int(d.Seconds())
	}
	return defaultRetentionSecs
}
