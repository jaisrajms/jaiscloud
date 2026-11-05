package pubsub

import (
	"context"
	"encoding/base64"
	"maps"
	"net"
	"testing"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"jaiscloud/internal/gcp/crypto"
	grpcpubsub "jaiscloud/internal/gcp/grpc/pubsub"
	"jaiscloud/internal/gcp/resource"
	kms "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newTestProvider() *Provider {
	return New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))
}

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

// TestTopicPublishRejectsEmpty pins the real-Pub/Sub rule the differential
// harness found missing: a publish request must carry at least one message, so
// an empty (or absent) messages array is INVALID_ARGUMENT (400).
func TestTopicPublishRejectsEmpty(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := newNR(map[string]any{"name": "topics/t"})
	if _, err := p.TopicCreate(ctx, nr); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	for _, body := range []map[string]any{{"messages": []any{}}, {}} {
		nr = newNR(map[string]any{"name": "topics/t", "body": body})
		_, err := p.TopicPublish(ctx, nr)
		pe, ok := err.(*model.ProviderError)
		if !ok || pe.HTTPStatus != 400 {
			t.Errorf("TopicPublish(%v) err = %v, want 400 ProviderError", body, err)
		}
	}
}

func TestPubSubRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Create topic.
	nr := newNR(map[string]any{"name": "topics/my-topic"})
	if _, err := p.TopicCreate(ctx, nr); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	// Create the subscription before publishing: Pub/Sub only delivers
	// messages published after the subscription exists.
	nr = newNR(map[string]any{"name": "subscriptions/my-sub", "body": map[string]any{"topic": "projects/proj/topics/my-topic"}})
	if _, err := p.SubscriptionCreate(ctx, nr); err != nil {
		t.Fatalf("subscription create: %v", err)
	}

	// Publish.
	nr = newNR(map[string]any{"name": "topics/my-topic", "body": map[string]any{"messages": []any{map[string]any{"data": "aGVsbG8="}}}})
	resp, err := p.TopicPublish(ctx, nr)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	ids, _ := resp.Data["messageIds"].([]string)
	if len(ids) != 1 {
		t.Fatalf("expected 1 message id, got %d", len(ids))
	}

	// Pull.
	nr = newNR(map[string]any{"name": "subscriptions/my-sub"})
	resp, err = p.SubscriptionPull(ctx, nr)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	received, _ := resp.Data["receivedMessages"].([]any)
	if len(received) != 1 {
		t.Fatalf("expected 1 received message, got %d", len(received))
	}
	rm := received[0].(map[string]any)
	ackID, _ := rm["ackId"].(string)
	msg := rm["message"].(map[string]any)
	if msg["data"] != "aGVsbG8=" {
		t.Errorf("unexpected message data: %v", msg["data"])
	}

	// Ack.
	nr = newNR(map[string]any{"name": "subscriptions/my-sub", "body": map[string]any{"ackIds": []any{ackID}}})
	if _, err := p.SubscriptionAcknowledge(ctx, nr); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestPubSubOpaqueAckID verifies the wire ackId is an opaque base64url of
// "topicID/messageID" (not the raw topic/message pair) and that both ack and
// modifyAckDeadline round-trip through the opaque form.
func TestPubSubOpaqueAckID(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/t"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{"name": "subscriptions/s", "body": map[string]any{"topic": "projects/proj/topics/t"}})); err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	nr := newNR(map[string]any{"name": "topics/t", "body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}}})
	if _, err := p.TopicPublish(ctx, nr); err != nil {
		t.Fatalf("publish: %v", err)
	}

	resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/s"}))
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	received, _ := resp.Data["receivedMessages"].([]any)
	if len(received) != 1 {
		t.Fatalf("expected 1 received message, got %d", len(received))
	}
	rm := received[0].(map[string]any)
	ackID, _ := rm["ackId"].(string)
	msgID, _ := rm["message"].(map[string]any)["messageId"].(string)

	// Opaque: not the raw "queue/messageID" string.
	if ackID == "s/"+msgID {
		t.Errorf("ackId %q is not opaque (raw queue/messageID leaked)", ackID)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(ackID)
	if err != nil {
		t.Fatalf("ackId is not base64url: %v", err)
	}
	if string(decoded) != "s/"+msgID+"/1" {
		t.Errorf("decoded ackId = %q, want s/%s/1", decoded, msgID)
	}

	// modifyAckDeadline round-trips through the opaque ackId.
	if _, err := p.SubscriptionModifyAckDeadline(ctx, newNR(map[string]any{
		"name": "subscriptions/s",
		"body": map[string]any{"ackIds": []any{ackID}, "ackDeadlineSeconds": float64(60)},
	})); err != nil {
		t.Fatalf("modifyAckDeadline: %v", err)
	}

	// Ack round-trips through the opaque ackId → message removed.
	if _, err := p.SubscriptionAcknowledge(ctx, newNR(map[string]any{
		"name": "subscriptions/s",
		"body": map[string]any{"ackIds": []any{ackID}},
	})); err != nil {
		t.Fatalf("ack: %v", err)
	}
	resp, err = p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/s", "body": map[string]any{"returnImmediately": true}}))
	if err != nil {
		t.Fatalf("pull after ack: %v", err)
	}
	if received, _ := resp.Data["receivedMessages"].([]any); len(received) != 0 {
		t.Fatalf("expected no messages after ack, got %d", len(received))
	}
}

// TestTopicGetListFieldFidelity verifies that labels and
// messageRetentionDuration round-trip through TopicGet and TopicList (the
// stored topic map is returned, not just {"name": ...}).
func TestTopicGetListFieldFidelity(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := newNR(map[string]any{
		"name": "topics/with-fields",
		"body": map[string]any{
			"messageRetentionDuration": "600s",
			"labels":                   map[string]any{"env": "test", "team": "platform"},
		},
	})
	if _, err := p.TopicCreate(ctx, nr); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	// Get round-trips both fields.
	nr = newNR(map[string]any{"name": "topics/with-fields"})
	get, err := p.TopicGet(ctx, nr)
	if err != nil {
		t.Fatalf("topic get: %v", err)
	}
	if ret, _ := get.Data["messageRetentionDuration"].(string); ret != "600s" {
		t.Errorf("messageRetentionDuration = %q, want 600s", ret)
	}
	labels, _ := get.Data["labels"].(map[string]any)
	if labels["env"] != "test" || labels["team"] != "platform" {
		t.Errorf("labels = %v, want env=test team=platform", labels)
	}

	// List round-trips both fields.
	nr = newNR(nil)
	list, err := p.TopicList(ctx, nr)
	if err != nil {
		t.Fatalf("topic list: %v", err)
	}
	topics, _ := list.Data["topics"].([]any)
	var found map[string]any
	for _, it := range topics {
		m, _ := it.(map[string]any)
		if m["name"] == "projects/proj/topics/with-fields" {
			found = m
		}
	}
	if found == nil {
		t.Fatal("expected topic in list result")
	}
	if ret, _ := found["messageRetentionDuration"].(string); ret != "600s" {
		t.Errorf("list messageRetentionDuration = %q, want 600s", ret)
	}
	labels, _ = found["labels"].(map[string]any)
	if labels["env"] != "test" || labels["team"] != "platform" {
		t.Errorf("list labels = %v, want env=test team=platform", labels)
	}
}

func TestPubSubCMEKRoundTrip(t *testing.T) {
	ctx := context.Background()
	kmsStore := kms.NewMemoryStore()
	enc := crypto.NewEnvelopeEncryptor(kmsStore)
	p := New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages(), enc)

	projectID := "my-project"
	location := "global"
	keyringID := "my-keyring"
	keyID := "my-key"
	if err := kmsStore.CreateKeyRing(ctx, projectID, location, keyringID, kms.KeyRing{ID: keyringID}); err != nil {
		t.Fatal(err)
	}
	if err := kmsStore.CreateCryptoKey(ctx, projectID, location, keyringID, keyID, kms.CryptoKey{ID: keyID, Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatal(err)
	}
	kmsKeyName := "projects/" + projectID + "/locations/" + location + "/keyRings/" + keyringID + "/cryptoKeys/" + keyID

	// Create a topic with CMEK.
	nr := newNR(map[string]any{"name": "topics/cmek-topic", "body": map[string]any{"kmsKeyName": kmsKeyName}})
	nr.AccountID = projectID
	if _, err := p.TopicCreate(ctx, nr); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	// Subscribe before publishing (a subscription only receives later messages).
	nr = newNR(map[string]any{"name": "subscriptions/cmek-sub", "body": map[string]any{"topic": "projects/" + projectID + "/topics/cmek-topic"}})
	nr.AccountID = projectID
	if _, err := p.SubscriptionCreate(ctx, nr); err != nil {
		t.Fatalf("subscription create: %v", err)
	}

	// Publish a message.
	payload := "aGVsbG8sIHdvcmxk" // base64 "hello, world"
	nr = newNR(map[string]any{"name": "topics/cmek-topic", "body": map[string]any{"messages": []any{map[string]any{"data": payload}}}})
	nr.AccountID = projectID
	if _, err := p.TopicPublish(ctx, nr); err != nil {
		t.Fatalf("publish: %v", err)
	}

	nr = newNR(map[string]any{"name": "subscriptions/cmek-sub"})
	nr.AccountID = projectID
	resp, err := p.SubscriptionPull(ctx, nr)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	received, _ := resp.Data["receivedMessages"].([]any)
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	data := received[0].(map[string]any)["message"].(map[string]any)["data"].(string)
	if data != payload {
		t.Fatalf("expected data %q, got %q", payload, data)
	}
}

// TestSubscriptionStateActive verifies a freshly created subscription reports
// state ACTIVE on create, get and list (the real-GCP divergence this fixes).
func TestSubscriptionStateActive(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/state-topic"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	created, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/state-sub",
		"body": map[string]any{"topic": "projects/proj/topics/state-topic"},
	}))
	if err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	if created.Data["state"] != "ACTIVE" {
		t.Fatalf("create state = %v, want ACTIVE", created.Data["state"])
	}
	if created.Data["messageRetentionDuration"] != "604800s" {
		t.Fatalf("messageRetentionDuration = %v, want 604800s", created.Data["messageRetentionDuration"])
	}
	ep, _ := created.Data["expirationPolicy"].(map[string]any)
	if ep["ttl"] != "2678400s" {
		t.Fatalf("expirationPolicy = %#v, want ttl 2678400s", created.Data["expirationPolicy"])
	}

	got, err := p.SubscriptionGet(ctx, newNR(map[string]any{"name": "subscriptions/state-sub"}))
	if err != nil {
		t.Fatalf("subscription get: %v", err)
	}
	if got.Data["state"] != "ACTIVE" {
		t.Fatalf("get state = %v, want ACTIVE", got.Data["state"])
	}

	list, err := p.SubscriptionList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("subscription list: %v", err)
	}
	items, _ := list.Data["subscriptions"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 subscription, got %d", len(items))
	}
	first, _ := items[0].(map[string]any)
	if first["state"] != "ACTIVE" {
		t.Fatalf("list state = %v, want ACTIVE", first["state"])
	}
}

// providerSubscriptionLabels are the five labels the terraform-google-pubsub
// module applies to every subscription it creates.
var providerSubscriptionLabels = map[string]string{
	"security_certified": "true",
	"managed-by":         "terraform",
	"module":             "terraform-google-pubsub",
	"team":               "platform",
	"project":            "proj",
}

// labelMap folds a labels value from a response into a map[string]string. The
// in-memory create response is map[string]string, a JSON round-trip yields
// map[string]any; both are the same wire shape.
func labelMap(t *testing.T, v any) map[string]string {
	t.Helper()
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, x := range m {
			s, ok := x.(string)
			if !ok {
				t.Fatalf("label %q = %T %v, want string", k, x, x)
			}
			out[k] = s
		}
		return out
	}
	t.Fatalf("labels = %T %v, want a map", v, v)
	return nil
}

// TestSubscriptionCreateStoresLabels covers PS1: REST SubscriptionCreate
// persists the request's labels and create, get and list all render them.
func TestSubscriptionCreateStoresLabels(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/labels-topic"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	requestLabels := make(map[string]any, len(providerSubscriptionLabels))
	for k, v := range providerSubscriptionLabels {
		requestLabels[k] = v
	}
	created, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/labels-sub",
		"body": map[string]any{
			"topic":  "projects/proj/topics/labels-topic",
			"labels": requestLabels,
		},
	}))
	if err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	if got := labelMap(t, created.Data["labels"]); !maps.Equal(got, providerSubscriptionLabels) {
		t.Errorf("create labels = %v, want %v", got, providerSubscriptionLabels)
	}

	read, err := p.SubscriptionGet(ctx, newNR(map[string]any{"name": "subscriptions/labels-sub"}))
	if err != nil {
		t.Fatalf("subscription get: %v", err)
	}
	if got := labelMap(t, read.Data["labels"]); !maps.Equal(got, providerSubscriptionLabels) {
		t.Errorf("get labels = %v, want %v", got, providerSubscriptionLabels)
	}

	list, err := p.SubscriptionList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("subscription list: %v", err)
	}
	items, _ := list.Data["subscriptions"].([]any)
	var found map[string]any
	for _, it := range items {
		if m, _ := it.(map[string]any); m["name"] == "projects/proj/subscriptions/labels-sub" {
			found = m
		}
	}
	if found == nil {
		t.Fatal("expected subscription in list result")
	}
	if got := labelMap(t, found["labels"]); !maps.Equal(got, providerSubscriptionLabels) {
		t.Errorf("list labels = %v, want %v", got, providerSubscriptionLabels)
	}
}

// TestSubscriptionCreateWithoutLabelsOmitsKey pins the TopicCreate shape: an
// absent or empty labels map leaves no "labels" key at all, never a null.
func TestSubscriptionCreateWithoutLabelsOmitsKey(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/no-labels-topic"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{
			name: "unlabelled-sub",
			body: map[string]any{"topic": "projects/proj/topics/no-labels-topic"},
		},
		{
			name: "empty-labels-sub",
			body: map[string]any{
				"topic":  "projects/proj/topics/no-labels-topic",
				"labels": map[string]any{},
			},
		},
	} {
		created, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
			"name": "subscriptions/" + tc.name,
			"body": tc.body,
		}))
		if err != nil {
			t.Fatalf("%s: subscription create: %v", tc.name, err)
		}
		if v, ok := created.Data["labels"]; ok {
			t.Errorf("%s: create labels key present, want absent (got %v)", tc.name, v)
		}

		read, err := p.SubscriptionGet(ctx, newNR(map[string]any{"name": "subscriptions/" + tc.name}))
		if err != nil {
			t.Fatalf("%s: subscription get: %v", tc.name, err)
		}
		if v, ok := read.Data["labels"]; ok {
			t.Errorf("%s: get labels key present, want absent (got %v)", tc.name, v)
		}
	}
}

// TestSubscriptionLabelsRESTGRPCParity pins the cross-transport contract: the
// same labels created over gRPC render identically over gRPC and REST. The
// gRPC path already stored labels; this guards the REST side against drifting
// from it.
func TestSubscriptionLabelsRESTGRPCParity(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	messages := pubsubstore.NewMemoryMessages()
	encryptor := crypto.NewEnvelopeEncryptor(kms.NewMemoryStore())
	rest := New(resources, messages, encryptor)
	svc := grpcpubsub.NewService(resources, messages, encryptor, "proj")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pubsubpb.RegisterSubscriberServer(srv, svc)
	go srv.Serve(ln)
	defer srv.Stop()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pubsubpb.NewSubscriberClient(conn)

	if _, err := rest.TopicCreate(ctx, newNR(map[string]any{"name": "topics/parity-topic"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	created, err := client.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:   "projects/proj/subscriptions/parity-sub",
		Topic:  "projects/proj/topics/parity-topic",
		Labels: providerSubscriptionLabels,
	})
	if err != nil {
		t.Fatalf("gRPC CreateSubscription: %v", err)
	}
	if got := created.GetLabels(); !maps.Equal(got, providerSubscriptionLabels) {
		t.Errorf("gRPC create labels = %v, want %v", got, providerSubscriptionLabels)
	}

	got, err := client.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: created.GetName()})
	if err != nil {
		t.Fatalf("gRPC GetSubscription: %v", err)
	}
	if labels := got.GetLabels(); !maps.Equal(labels, providerSubscriptionLabels) {
		t.Errorf("gRPC get labels = %v, want %v", labels, providerSubscriptionLabels)
	}

	// The REST read of the same stored subscription renders the same map.
	read, err := rest.SubscriptionGet(ctx, newNR(map[string]any{"name": "subscriptions/parity-sub"}))
	if err != nil {
		t.Fatalf("REST SubscriptionGet: %v", err)
	}
	if labels := labelMap(t, read.Data["labels"]); !maps.Equal(labels, providerSubscriptionLabels) {
		t.Errorf("REST labels = %v, want %v", labels, providerSubscriptionLabels)
	}
}
