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
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
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
	rtSnapshot           = "gcp_snapshot"
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

	// regs holds one shared ack registry per subscription so ack state is
	// authoritative across every StreamingPull stream on that subscription.
	regMu sync.Mutex
	regs  map[string]*ackRegistry
}

// NewService returns a Pub/Sub gRPC service backed by the shared stores.
// defaultProj is the config-default project used when a request carries none.
func NewService(resources store.ResourceStore, messages pubsubstore.Messages, encryptor crypto.EnvelopeEncryptor, defaultProj string) *Service {
	return &Service{
		resources:   resources,
		messages:    messages,
		encryptor:   encryptor,
		defaultProj: defaultProj,
		regs:        map[string]*ackRegistry{},
	}
}

// registry returns the ack registry shared by every stream on a subscription,
// creating it lazily. All StreamingPull streams and the unary Acknowledge RPC
// for the same subscription resolve to the same instance, which is what makes
// an ack on one stream visible to the others.
func (s *Service) registry(sub string) *ackRegistry {
	s.regMu.Lock()
	defer s.regMu.Unlock()
	if s.regs == nil {
		s.regs = map[string]*ackRegistry{}
	}
	r, ok := s.regs[sub]
	if !ok {
		r = newAckRegistry(sub, s.messages)
		s.regs[sub] = r
	}
	return r
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// ─── resource-name parsing ────────────────────────────────────────────────────

func topicName(project, id string) string {
	return "projects/" + project + "/topics/" + id
}

func subscriptionName(project, id string) string {
	return "projects/" + project + "/subscriptions/" + id
}

func snapshotName(project, id string) string {
	return "projects/" + project + "/snapshots/" + id
}

// splitSnapshotName parses "projects/{p}/snapshots/{s}".
func splitSnapshotName(name string) (project, snap string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "snapshots" {
		return "", "", false
	}
	return parts[1], parts[3], true
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

// normalizeDeadLetterPolicy validates and canonicalizes a subscription's
// dead_letter_policy: a configured dead_letter_topic must already exist in the
// same project (NotFound otherwise), and max_delivery_attempts must be between
// 5 and 100 (0 selects the 5 default).
func (s *Service) normalizeDeadLetterPolicy(ctx context.Context, project string, dlp *pubsubpb.DeadLetterPolicy) (map[string]any, error) {
	out := map[string]any{}
	if dt := dlp.GetDeadLetterTopic(); dt != "" {
		dtProject, dtID, ok := splitTopicName(dt)
		if !ok {
			return nil, mapError(model.NewProviderError("InvalidArgument", "invalid dead letter topic name", 400))
		}
		if dtProject != project {
			return nil, mapError(model.NewProviderError("NotFound", "dead letter topic not found", 404))
		}
		if _, err := s.resources.Get(ctx, project, store.GlobalRegion, rtTopic, dtID); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, mapError(model.NewProviderError("NotFound", "dead letter topic not found", 404))
			}
			return nil, mapError(err)
		}
		out["deadLetterTopic"] = dt
	}
	attempts := int(dlp.GetMaxDeliveryAttempts())
	if attempts != 0 && (attempts < 5 || attempts > 100) {
		return nil, mapError(model.NewProviderError("InvalidArgument", fmt.Sprintf("maxDeliveryAttempts must be between 5 and 100 (got %d)", attempts), 400))
	}
	if attempts == 0 {
		attempts = 5
	}
	out["maxDeliveryAttempts"] = attempts
	return out, nil
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
	// Exactly-once delivery is a pull-only feature. A push subscription cannot
	// support it (the client never learns whether the push was processed), so
	// requesting both fails loud rather than silently dropping the flag.
	exactlyOnce := req.GetEnableExactlyOnceDelivery()
	if exactlyOnce && req.GetPushConfig().GetPushEndpoint() != "" {
		return nil, mapError(model.NewProviderError("InvalidArgument",
			"exactly-once delivery is not supported for push subscriptions", 400))
	}
	ackDeadline := 10
	if v := req.GetAckDeadlineSeconds(); v != 0 {
		// Proto: the value must be between 10 and 600 seconds; 0 selects the
		// default.
		if v < 10 || v > 600 {
			return nil, mapError(model.NewProviderError("InvalidArgument", fmt.Sprintf("ackDeadlineSeconds must be between 10 and 600 (got %d)", v), 400))
		}
		ackDeadline = int(v)
	} else if exactlyOnce {
		// Exactly-once subscriptions default to a 60-second ack deadline.
		ackDeadline = 60
	}
	meta := map[string]any{
		"name":               subscriptionName(project, sub),
		"topic":              req.GetTopic(),
		"ackDeadlineSeconds": ackDeadline,
	}
	if exactlyOnce {
		meta["enableExactlyOnceDelivery"] = true
	}
	if req.GetEnableMessageOrdering() {
		meta["enableMessageOrdering"] = true
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
		normalized, err := s.normalizeDeadLetterPolicy(ctx, project, dlp)
		if err != nil {
			return nil, err
		}
		meta["deadLetterPolicy"] = normalized
	}
	if rp := req.GetRetryPolicy(); rp != nil {
		normalized, err := normalizeRetryPolicy(rp)
		if err != nil {
			return nil, err
		}
		if len(normalized) > 0 {
			meta["retryPolicy"] = normalized
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
	hasDeadLetterPolicy := false
	if dp, ok := meta["deadLetterPolicy"].(map[string]any); ok {
		hasDeadLetterPolicy = true
		if dt, _ := dp["deadLetterTopic"].(string); dt != "" {
			dlqTopic = dt
			if i := strings.LastIndex(dlqTopic, "/"); i >= 0 {
				dlqTopic = dlqTopic[i+1:]
			}
		}
		if mda, ok := asInt(dp["maxDeliveryAttempts"]); ok {
			maxDeliveryAttempts = mda
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
	received, err := s.buildReceivedMessages(ctx, project, sub, dlqTopic, maxDeliveryAttempts, hasDeadLetterPolicy, msgs)
	if err != nil {
		return nil, err
	}
	return &pubsubpb.PullResponse{ReceivedMessages: received}, nil
}

// buildReceivedMessages transcodes claimed messages into wire ReceivedMessages,
// applying DLQ republish + envelope decryption. Shared by Pull and StreamingPull
// so both transports emit the same ack-id scheme and payload shape.
func (s *Service) buildReceivedMessages(ctx context.Context, project, queue, dlqTopic string, maxDeliveryAttempts int, hasDeadLetterPolicy bool, msgs []pubsubstore.Message) ([]*pubsubpb.ReceivedMessage, error) {
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
		// Proto: delivery_attempt is 0 unless the subscription has a
		// DeadLetterPolicy; with one it reflects the attempt count (>= 1).
		attempt := int32(0)
		if hasDeadLetterPolicy {
			attempt = int32(m.DeliveryAttempt)
		}
		received = append(received, &pubsubpb.ReceivedMessage{
			AckId:           encodeAckID(queue, m.MessageID),
			Message:         pm,
			DeliveryAttempt: attempt,
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
	exactlyOnce, _ := meta["enableExactlyOnceDelivery"].(bool)
	ordering, _ := meta["enableMessageOrdering"].(bool)
	var props *pubsubpb.StreamingPullResponse_SubscriptionProperties
	if exactlyOnce || ordering {
		// Advertise the subscription's non-default properties (the official
		// client reads these to keep its exactly-once / ordering mode in sync).
		props = &pubsubpb.StreamingPullResponse_SubscriptionProperties{
			ExactlyOnceDeliveryEnabled: exactlyOnce,
			MessageOrderingEnabled:     ordering,
		}
	}

	// The ack registry is subscription-scoped: every stream on this
	// subscription shares it, and it is reconciled against the store on a short
	// poll interval (acktrack.go). It keeps a message acked on one stream from
	// being redelivered on another.
	reg := s.registry(sub)
	reg.acquire()
	defer reg.release()
	subFilter, _ := meta["filter"].(string)
	topic, _ := meta["topic"].(string)
	topicID := topic
	if i := strings.LastIndex(topicID, "/"); i >= 0 {
		topicID = topicID[i+1:]
	}

	// Dead-letter policy (mirrors Pull).
	dlqTopic := ""
	maxDeliveryAttempts := 0
	hasDeadLetterPolicy := false
	if dp, ok := meta["deadLetterPolicy"].(map[string]any); ok {
		hasDeadLetterPolicy = true
		if dt, _ := dp["deadLetterTopic"].(string); dt != "" {
			dlqTopic = dt
			if i := strings.LastIndex(dlqTopic, "/"); i >= 0 {
				dlqTopic = dlqTopic[i+1:]
			}
		}
		if mda, ok := asInt(dp["maxDeliveryAttempts"]); ok {
			maxDeliveryAttempts = mda
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
			// Cross-stream ack reconciliation: drop any message this stream
			// claimed that was acknowledged on another stream (or by the unary
			// Acknowledge RPC) between the Pull and the send. Without this the
			// already-claimed copy would be delivered after its ack.
			msgs = s.dropAcked(ctx, sub, reg, msgs)
			received, err := s.buildReceivedMessages(ctx, project, sub, dlqTopic, maxDeliveryAttempts, hasDeadLetterPolicy, msgs)
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
				if err := stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages:       sendable,
					SubscriptionProperties: props,
				}); err != nil {
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
// outstanding flow-control set. Each successful ack also tombstones the message
// in the subscription-scoped registry so another stream cannot redeliver an
// already-claimed copy.
func (s *Service) applyStreamAcks(ctx context.Context, queue string, req *pubsubpb.StreamingPullRequest) []string {
	reg := s.registry(queue)
	var acked []string
	for _, a := range req.GetAckIds() {
		decoded, ok := decodeAckID(a)
		if !ok {
			continue
		}
		parts := strings.SplitN(decoded, "/", 2)
		// An ackId is valid only for the subscription that issued it.
		if len(parts) != 2 || parts[0] != queue {
			continue
		}
		if err := s.messages.Delete(ctx, queue, parts[1]); err != nil {
			// Leave it untombstoned so a later retry can still ack it.
			continue
		}
		reg.markAcked(parts[1])
		acked = append(acked, parts[1])
	}
	return acked
}

// dropAcked removes messages a stream claimed but that were acknowledged on
// another stream (or by the unary Acknowledge RPC) before the send. A dropped
// message that is somehow still present (a Seek restored it) is made visible
// again so it is not stranded until its ack deadline.
func (s *Service) dropAcked(ctx context.Context, queue string, reg *ackRegistry, msgs []pubsubstore.Message) []pubsubstore.Message {
	if len(msgs) == 0 {
		return msgs
	}
	kept := msgs[:0]
	for _, m := range msgs {
		if reg.isAcked(m.MessageID) {
			_ = s.messages.ModifyAckDeadline(ctx, queue, []string{queue + "/" + m.MessageID}, 0, clock.Now())
			continue
		}
		kept = append(kept, m)
	}
	return kept
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
		parts := strings.SplitN(decoded, "/", 2)
		// An ackId is valid only for the subscription that issued it.
		if len(parts) != 2 || parts[0] != queue {
			continue
		}
		_ = s.messages.ModifyAckDeadline(ctx, queue, []string{queue + "/" + parts[1]}, int(seconds[i]), clock.Now())
		if seconds[i] <= 0 {
			nacked = append(nacked, parts[1])
		}
	}
	return nacked
}

func (s *Service) Acknowledge(ctx context.Context, req *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	_, sub, named := splitSubscriptionName(req.GetSubscription())
	for _, a := range req.GetAckIds() {
		decoded, ok := decodeAckID(a)
		if !ok {
			continue
		}
		parts := strings.SplitN(decoded, "/", 2)
		if len(parts) != 2 {
			continue
		}
		// An ackId is valid only for the subscription that issued it. When the
		// request names a subscription, enforce that; otherwise trust the ackId.
		if named && parts[0] != sub {
			continue
		}
		queue := parts[0]
		if err := s.messages.Delete(ctx, queue, parts[1]); err != nil {
			return nil, mapError(err)
		}
		// Tombstone in the shared subscription registry so an active
		// StreamingPull on the same subscription drops its claimed copy.
		s.registry(queue).markAcked(parts[1])
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
	// Proto ModifyAckDeadline: valid values are 0 to 600 seconds.
	if v := req.GetAckDeadlineSeconds(); v < 0 || v > 600 {
		return nil, mapError(model.NewProviderError("InvalidArgument", fmt.Sprintf("ackDeadlineSeconds must be between 0 and 600 (got %d)", v), 400))
	}
	decoded := make([]string, 0, len(req.GetAckIds()))
	for _, id := range req.GetAckIds() {
		d, ok := decodeAckID(id)
		if !ok {
			continue
		}
		// An ackId is valid only for the subscription that issued it.
		parts := strings.SplitN(d, "/", 2)
		if len(parts) != 2 || parts[0] != sub {
			continue
		}
		decoded = append(decoded, sub+"/"+parts[1])
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
		// retry_policy is handled before the switch: its nested field-mask
		// leaves need per-leaf merge semantics (AIP-161), and folding the path
		// keeps snake_case/camelCase parity with the REST transport.
		if _, _, ok := retryPolicyLeaf(p); ok {
			if err := applyRetryPolicyUpdateProto(meta, in, p); err != nil {
				return nil, err
			}
			continue
		}
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
			v := int(in.GetAckDeadlineSeconds())
			// Proto: the value must be between 10 and 600 seconds; 0 selects
			// the 10-second default.
			if v != 0 && (v < 10 || v > 600) {
				return nil, mapError(model.NewProviderError("InvalidArgument", fmt.Sprintf("ackDeadlineSeconds must be between 10 and 600 (got %d)", v), 400))
			}
			if v == 0 {
				v = 10
			}
			meta["ackDeadlineSeconds"] = v
		case "enable_exactly_once_delivery", "enableExactlyOnceDelivery":
			if in.GetEnableExactlyOnceDelivery() {
				// Enabling exactly-once is unsupported for push subscriptions.
				if pc, ok := meta["pushConfig"].(map[string]any); ok {
					if ep, _ := pc["pushEndpoint"].(string); ep != "" {
						return nil, mapError(model.NewProviderError("InvalidArgument",
							"exactly-once delivery is not supported for push subscriptions", 400))
					}
				}
				meta["enableExactlyOnceDelivery"] = true
			} else {
				delete(meta, "enableExactlyOnceDelivery")
			}
		case "enable_message_ordering", "enableMessageOrdering":
			if in.GetEnableMessageOrdering() {
				meta["enableMessageOrdering"] = true
			} else {
				delete(meta, "enableMessageOrdering")
			}
		case "dead_letter_policy", "deadLetterPolicy":
			if in.GetDeadLetterPolicy() == nil {
				delete(meta, "deadLetterPolicy")
				break
			}
			normalized, err := s.normalizeDeadLetterPolicy(ctx, project, in.GetDeadLetterPolicy())
			if err != nil {
				return nil, err
			}
			meta["deadLetterPolicy"] = normalized
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

// UpdateTopic applies an update_mask to a topic's mutable fields. Labels and
// message_retention_duration are supported; anything else is rejected rather
// than silently ignored (mirrors UpdateSubscription).
func (s *Service) UpdateTopic(ctx context.Context, req *pubsubpb.UpdateTopicRequest) (*pubsubpb.Topic, error) {
	in := req.GetTopic()
	if in == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "topic is required", 400))
	}
	project, id, ok := splitTopicName(in.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid topic name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtTopic, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "topic not found", 404))
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
		case "labels":
			if len(in.GetLabels()) == 0 {
				delete(meta, "labels")
			} else {
				meta["labels"] = in.GetLabels()
			}
		case "message_retention_duration", "messageRetentionDuration":
			if in.GetMessageRetentionDuration() == nil {
				delete(meta, "messageRetentionDuration")
			} else {
				meta["messageRetentionDuration"] = in.GetMessageRetentionDuration().AsDuration().String()
			}
		default:
			return nil, mapError(model.NewProviderError("InvalidArgument", "unsupported update_mask path: "+p, 400))
		}
	}
	data, _ := json.Marshal(meta)
	if err := s.resources.Update(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtTopic, ID: id, Data: data}); err != nil {
		return nil, mapError(err)
	}
	return topicToProto(project, id, meta), nil
}

// ListTopicSubscriptions lists the full subscription names attached to a topic.
// A detached subscription still references its topic in this emulator, so it is
// included (real Pub/Sub keeps the resource until deleted).
func (s *Service) ListTopicSubscriptions(ctx context.Context, req *pubsubpb.ListTopicSubscriptionsRequest) (*pubsubpb.ListTopicSubscriptionsResponse, error) {
	project, topic, ok := splitTopicName(req.GetTopic())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid topic name", 400))
	}
	if _, err := s.resources.Get(ctx, project, store.GlobalRegion, rtTopic, topic); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "topic not found", 404))
		}
		return nil, mapError(err)
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtSubscription, "")
	if err != nil {
		return nil, mapError(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		var sub map[string]any
		if json.Unmarshal(e.Data, &sub) != nil {
			continue
		}
		if t, _ := sub["topic"].(string); lastSegment(t) != topic {
			continue
		}
		names = append(names, subscriptionName(project, e.ID))
	}
	page, nextToken := paging.Page(names, func(s string) string { return s },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	return &pubsubpb.ListTopicSubscriptionsResponse{Subscriptions: page, NextPageToken: nextToken}, nil
}

// ModifyPushConfig updates a subscription's stored push configuration. An empty
// push_config clears it, which resumes pull delivery.
func (s *Service) ModifyPushConfig(ctx context.Context, req *pubsubpb.ModifyPushConfigRequest) (*emptypb.Empty, error) {
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

	pc := req.GetPushConfig()
	if pc == nil || pc.GetPushEndpoint() == "" {
		delete(meta, "pushConfig")
	} else {
		// Exactly-once delivery cannot be enabled on a push subscription.
		if eod, _ := meta["enableExactlyOnceDelivery"].(bool); eod {
			return nil, mapError(model.NewProviderError("InvalidArgument",
				"exactly-once delivery is not supported for push subscriptions", 400))
		}
		pm := map[string]any{"pushEndpoint": pc.GetPushEndpoint()}
		if len(pc.GetAttributes()) > 0 {
			pm["attributes"] = pc.GetAttributes()
		}
		if ot := pc.GetOidcToken(); ot != nil {
			pm["oidcToken"] = map[string]any{
				"audience":            ot.GetAudience(),
				"serviceAccountEmail": ot.GetServiceAccountEmail(),
			}
		}
		meta["pushConfig"] = pm
	}
	data, _ := json.Marshal(meta)
	if err := s.resources.Update(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: id, Data: data}); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── snapshots + seek ─────────────────────────────────────────────────────────

// snapshotLifetime is the (metadata) snapshot expiry: Pub/Sub snapshots live no
// longer than 7 days from creation.
const snapshotLifetime = 7 * 24 * time.Hour

// snapshotMeta is the persisted form of a Snapshot resource. Backlog captures
// the source subscription's unacked messages at creation so a later Seek to the
// snapshot can faithfully restore the ack state (including messages acked
// afterwards) — without it, snapshot-based Seek would silently diverge from
// real Pub/Sub, which redelivers the snapshot backlog.
type snapshotMeta struct {
	Name         string                `json:"name"`
	Topic        string                `json:"topic"`
	Subscription string                `json:"subscription"`
	CreatedAt    time.Time             `json:"createdAt"`
	ExpireTime   time.Time             `json:"expireTime"`
	Labels       map[string]string     `json:"labels,omitempty"`
	Backlog      []pubsubstore.Message `json:"backlog,omitempty"`
}

// CreateSnapshot captures a subscription's backlog (unacked messages) into a
// snapshot resource. Messages published to the topic after creation are also
// retained by the subscription and are recovered by Seek.
func (s *Service) CreateSnapshot(ctx context.Context, req *pubsubpb.CreateSnapshotRequest) (*pubsubpb.Snapshot, error) {
	project, snap, ok := splitSnapshotName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid snapshot name", 400))
	}
	_, subID, ok := splitSubscriptionName(req.GetSubscription())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid subscription name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, subID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "subscription not found", 404))
		}
		return nil, mapError(err)
	}
	var sub map[string]any
	json.Unmarshal(e.Data, &sub)
	topic, _ := sub["topic"].(string)

	backlog, err := s.messages.List(ctx, subID)
	if err != nil {
		return nil, mapError(err)
	}
	now := clock.Now()
	meta := snapshotMeta{
		Name:         req.GetName(),
		Topic:        topic,
		Subscription: req.GetSubscription(),
		CreatedAt:    now,
		ExpireTime:   now.Add(snapshotLifetime),
		Labels:       req.GetLabels(),
		Backlog:      backlog,
	}
	data, _ := json.Marshal(meta)
	if err := s.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtSnapshot, ID: snap, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, mapError(model.NewProviderError("AlreadyExists", "snapshot already exists", 409))
		}
		return nil, mapError(err)
	}
	return snapshotToProto(meta), nil
}

func (s *Service) GetSnapshot(ctx context.Context, req *pubsubpb.GetSnapshotRequest) (*pubsubpb.Snapshot, error) {
	project, snap, ok := splitSnapshotName(req.GetSnapshot())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid snapshot name", 400))
	}
	meta, err := s.getSnapshotMeta(ctx, project, snap)
	if err != nil {
		return nil, err
	}
	return snapshotToProto(meta), nil
}

func (s *Service) ListSnapshots(ctx context.Context, req *pubsubpb.ListSnapshotsRequest) (*pubsubpb.ListSnapshotsResponse, error) {
	project := s.projectFromListProject(ctx, req.GetProject())
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtSnapshot, "")
	if err != nil {
		return nil, mapError(err)
	}
	page, nextToken := paging.Apply(entries, map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	snaps := make([]*pubsubpb.Snapshot, 0, len(page))
	for _, e := range page {
		var meta snapshotMeta
		if json.Unmarshal(e.Data, &meta) == nil {
			snaps = append(snaps, snapshotToProto(meta))
		}
	}
	return &pubsubpb.ListSnapshotsResponse{Snapshots: snaps, NextPageToken: nextToken}, nil
}

// UpdateSnapshot applies an update_mask (labels only) to a snapshot.
func (s *Service) UpdateSnapshot(ctx context.Context, req *pubsubpb.UpdateSnapshotRequest) (*pubsubpb.Snapshot, error) {
	in := req.GetSnapshot()
	if in == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "snapshot is required", 400))
	}
	project, snap, ok := splitSnapshotName(in.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid snapshot name", 400))
	}
	meta, err := s.getSnapshotMeta(ctx, project, snap)
	if err != nil {
		return nil, err
	}
	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = []string{"labels"}
	}
	for _, p := range paths {
		switch p {
		case "labels":
			if len(in.GetLabels()) == 0 {
				meta.Labels = nil
			} else {
				meta.Labels = in.GetLabels()
			}
		default:
			return nil, mapError(model.NewProviderError("InvalidArgument", "unsupported update_mask path: "+p, 400))
		}
	}
	data, _ := json.Marshal(meta)
	if err := s.resources.Update(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtSnapshot, ID: snap, Data: data}); err != nil {
		return nil, mapError(err)
	}
	return snapshotToProto(meta), nil
}

func (s *Service) DeleteSnapshot(ctx context.Context, req *pubsubpb.DeleteSnapshotRequest) (*emptypb.Empty, error) {
	project, snap, ok := splitSnapshotName(req.GetSnapshot())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid snapshot name", 400))
	}
	if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtSnapshot, snap); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "snapshot not found", 404))
		}
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ListTopicSnapshots lists the full snapshot names retaining messages from a
// topic.
func (s *Service) ListTopicSnapshots(ctx context.Context, req *pubsubpb.ListTopicSnapshotsRequest) (*pubsubpb.ListTopicSnapshotsResponse, error) {
	project, topic, ok := splitTopicName(req.GetTopic())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid topic name", 400))
	}
	if _, err := s.resources.Get(ctx, project, store.GlobalRegion, rtTopic, topic); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, mapError(model.NewProviderError("NotFound", "topic not found", 404))
		}
		return nil, mapError(err)
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtSnapshot, "")
	if err != nil {
		return nil, mapError(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		var meta snapshotMeta
		if json.Unmarshal(e.Data, &meta) != nil {
			continue
		}
		if lastSegment(meta.Topic) != topic {
			continue
		}
		names = append(names, meta.Name)
	}
	page, nextToken := paging.Page(names, func(s string) string { return s },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	return &pubsubpb.ListTopicSnapshotsResponse{Snapshots: page, NextPageToken: nextToken}, nil
}

// Seek resets a subscription's ack state, either to a timestamp (messages
// published before it are acknowledged, messages at/after it become
// unacknowledged) or to a snapshot (whose captured backlog is restored,
// including messages acked after the snapshot was created).
func (s *Service) Seek(ctx context.Context, req *pubsubpb.SeekRequest) (*pubsubpb.SeekResponse, error) {
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

	switch {
	case req.GetTime() != nil:
		if err := s.seekToTime(ctx, sub, req.GetTime().AsTime()); err != nil {
			return nil, mapError(err)
		}
	case req.GetSnapshot() != "":
		snapProject, snapID, ok := splitSnapshotName(req.GetSnapshot())
		if !ok || snapProject != project {
			return nil, mapError(model.NewProviderError("InvalidArgument", "invalid snapshot name", 400))
		}
		snap, err := s.getSnapshotMeta(ctx, project, snapID)
		if err != nil {
			return nil, err
		}
		subTopic, _ := meta["topic"].(string)
		if snap.Topic != subTopic {
			return nil, mapError(&model.ProviderError{
				Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
				Message: "snapshot topic " + snap.Topic + " does not match subscription topic " + subTopic,
			})
		}
		if err := s.restoreSnapshot(ctx, sub, snap); err != nil {
			return nil, mapError(err)
		}
	default:
		return nil, mapError(model.NewProviderError("InvalidArgument", "seek target (time or snapshot) is required", 400))
	}
	return &pubsubpb.SeekResponse{}, nil
}

// seekToTime marks the subscription's retained messages published before t as
// acknowledged (deleted) and makes those published at/after t visible again.
// Already-acked messages are gone, so they are not restored (proto-documented).
func (s *Service) seekToTime(ctx context.Context, queue string, t time.Time) error {
	msgs, err := s.messages.List(ctx, queue)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if m.PublishTime.Before(t) {
			_ = s.messages.Delete(ctx, queue, m.MessageID)
			continue
		}
		_ = s.messages.ModifyAckDeadline(ctx, queue, []string{m.MessageID}, 0, clock.Now())
	}
	return nil
}

// restoreSnapshot resets the subscription to the state captured by snap: the
// snapshot backlog is restored as unacknowledged, plus every message still
// retained in the subscription that was published at/after the snapshot was
// created. Messages acked before the snapshot are not restored.
func (s *Service) restoreSnapshot(ctx context.Context, queue string, snap snapshotMeta) error {
	current, err := s.messages.List(ctx, queue)
	if err != nil {
		return err
	}
	for _, m := range current {
		_ = s.messages.Delete(ctx, queue, m.MessageID)
	}
	restore := func(m pubsubstore.Message) error {
		m.Subscription = queue
		m.VisibleAt = time.Time{}
		return s.messages.Put(ctx, m)
	}
	for _, m := range snap.Backlog {
		if err := restore(m); err != nil {
			return err
		}
	}
	for _, m := range current {
		if m.PublishTime.Before(snap.CreatedAt) {
			continue
		}
		if err := restore(m); err != nil {
			return err
		}
	}
	return nil
}

// getSnapshotMeta loads and decodes a snapshot resource, mapping ErrNotFound to
// a gRPC NOT_FOUND.
func (s *Service) getSnapshotMeta(ctx context.Context, project, snap string) (snapshotMeta, error) {
	var meta snapshotMeta
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSnapshot, snap)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return meta, mapError(model.NewProviderError("NotFound", "snapshot not found", 404))
		}
		return meta, mapError(err)
	}
	if err := json.Unmarshal(e.Data, &meta); err != nil {
		return meta, mapError(model.NewProviderError("Internal", "corrupt snapshot metadata", 500))
	}
	return meta, nil
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
	if eod, _ := meta["enableExactlyOnceDelivery"].(bool); eod {
		sub.EnableExactlyOnceDelivery = true
	}
	if ord, _ := meta["enableMessageOrdering"].(bool); ord {
		sub.EnableMessageOrdering = true
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
	if rp, ok := meta["retryPolicy"].(map[string]any); ok {
		p := &pubsubpb.RetryPolicy{}
		if s, _ := rp["minimumBackoff"].(string); s != "" {
			if d, err := parseProtoDuration(s); err == nil {
				p.MinimumBackoff = durationpb.New(d)
			}
		}
		if s, _ := rp["maximumBackoff"].(string); s != "" {
			if d, err := parseProtoDuration(s); err == nil {
				p.MaximumBackoff = durationpb.New(d)
			}
		}
		if p.MinimumBackoff != nil || p.MaximumBackoff != nil {
			sub.RetryPolicy = p
		}
	}
	if pc, ok := meta["pushConfig"].(map[string]any); ok {
		p := &pubsubpb.PushConfig{}
		if ep, _ := pc["pushEndpoint"].(string); ep != "" {
			p.PushEndpoint = ep
		}
		switch attrs := pc["attributes"].(type) {
		case map[string]string:
			if len(attrs) > 0 {
				p.Attributes = attrs
			}
		case map[string]any:
			m := make(map[string]string, len(attrs))
			for k, v := range attrs {
				if s, ok := v.(string); ok {
					m[k] = s
				}
			}
			if len(m) > 0 {
				p.Attributes = m
			}
		}
		if ot, ok := pc["oidcToken"].(map[string]any); ok {
			aud, _ := ot["audience"].(string)
			email, _ := ot["serviceAccountEmail"].(string)
			if aud != "" || email != "" {
				p.AuthenticationMethod = &pubsubpb.PushConfig_OidcToken_{
					OidcToken: &pubsubpb.PushConfig_OidcToken{
						Audience:            aud,
						ServiceAccountEmail: email,
					},
				}
			}
		}
		sub.PushConfig = p
	}
	return sub
}

// snapshotToProto renders a persisted snapshot as its wire form.
func snapshotToProto(meta snapshotMeta) *pubsubpb.Snapshot {
	snap := &pubsubpb.Snapshot{Name: meta.Name, Topic: meta.Topic}
	if !meta.ExpireTime.IsZero() {
		snap.ExpireTime = timestamppb.New(meta.ExpireTime)
	}
	if len(meta.Labels) > 0 {
		snap.Labels = meta.Labels
	}
	return snap
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
