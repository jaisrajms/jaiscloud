package secretmanager

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	secretmanagersvc "jaiscloud/internal/gcp/service/secretmanager"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// grpcPublishedMessage is one Pub/Sub publish the gRPC service emitted.
type grpcPublishedMessage struct {
	project string
	topic   string
	data    []byte
	attrs   map[string]string
}

type grpcFakePublisher struct {
	mu   sync.Mutex
	msgs []grpcPublishedMessage
}

func (f *grpcFakePublisher) PublishEvent(_ context.Context, accountID, topic string, data []byte, attributes map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msgs = append(f.msgs, grpcPublishedMessage{project: accountID, topic: topic, data: data, attrs: attributes})
	return "1", nil
}

func (f *grpcFakePublisher) drain() []grpcPublishedMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.msgs
	f.msgs = nil
	return out
}

// newEventTestServer dials an in-process gRPC server whose service publishes
// event notifications through the returned fake publisher.
func newEventTestServer(t *testing.T) (secretmanagerpb.SecretManagerServiceClient, *grpcFakePublisher, secretmanagerstore.Store, func()) {
	t.Helper()
	secrets := secretmanagerstore.NewMemoryStore()
	resources := store.NewMemoryResourceStore()
	svc := NewService(secrets, resources, crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore()), "test")
	pub := &grpcFakePublisher{}
	svc.SetEventPublisher(pub)

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	secretmanagerpb.RegisterSecretManagerServiceServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return secretmanagerpb.NewSecretManagerServiceClient(conn), pub, secrets, cleanup
}

func grpcEventTypes(msgs []grpcPublishedMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.attrs["eventType"])
	}
	return out
}

func wantGRPCEvents(t *testing.T, msgs []grpcPublishedMessage, want ...string) {
	t.Helper()
	if got := strings.Join(grpcEventTypes(msgs), ","); got != strings.Join(want, ",") {
		t.Fatalf("event types = %q, want %q", got, strings.Join(want, ","))
	}
}

func rotateTopic(name string) []*secretmanagerpb.Topic {
	return []*secretmanagerpb.Topic{{Name: name}}
}

// TestGRPCSecretTopicsAndEvents: a create carrying topics returns them and
// publishes TOPIC_CONFIGURED then SECRET_CREATE to each topic, with the new
// topics in the data payload.
func TestGRPCSecretTopicsAndEvents(t *testing.T) {
	client, pub, _, cleanup := newEventTestServer(t)
	defer cleanup()
	ctx := context.Background()

	const secretName = "projects/test/secrets/s"
	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "s",
		Secret: &secretmanagerpb.Secret{
			Topics: rotateTopic("projects/test/topics/a"),
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(created.GetTopics()) != 1 || created.GetTopics()[0].GetName() != "projects/test/topics/a" {
		t.Fatalf("create topics = %v", created.GetTopics())
	}

	msgs := pub.drain()
	wantGRPCEvents(t, msgs, "TOPIC_CONFIGURED", "SECRET_CREATE")
	if len(msgs[0].data) != 0 {
		t.Fatalf("TOPIC_CONFIGURED carried data %q", msgs[0].data)
	}
	if msgs[1].attrs["secretId"] != secretName || msgs[1].attrs["dataFormat"] != secretmanagersvc.DataFormatJSONAPIV1 {
		t.Fatalf("SECRET_CREATE attributes = %#v", msgs[1].attrs)
	}
	var body map[string]any
	if err := json.Unmarshal(msgs[1].data, &body); err != nil {
		t.Fatalf("SECRET_CREATE data is not JSON: %v", err)
	}
	topics, _ := body["topics"].([]any)
	if len(topics) != 1 {
		t.Fatalf("SECRET_CREATE data topics = %#v", body["topics"])
	}

	// A maskless update carrying no topics preserves them (absence, not an
	// empty list, is what "every populated field" means here).
	updated, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret: &secretmanagerpb.Secret{Name: secretName, Labels: map[string]string{"env": "dev"}},
	})
	if err != nil {
		t.Fatalf("maskless update: %v", err)
	}
	if len(updated.GetTopics()) != 1 {
		t.Fatalf("maskless update dropped topics: %v", updated.GetTopics())
	}
	pub.drain()

	// Get does not publish (nothing is due).
	if _, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: secretName}); err != nil {
		t.Fatalf("get: %v", err)
	}
	if msgs := pub.drain(); len(msgs) != 0 {
		t.Fatalf("read published %v", grpcEventTypes(msgs))
	}

	// A version add publishes SECRET_VERSION_ADD with the version name.
	version, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  secretName,
		Payload: &secretmanagerpb.SecretPayload{Data: []byte("hi")},
	})
	if err != nil {
		t.Fatalf("add version: %v", err)
	}
	msgs = pub.drain()
	wantGRPCEvents(t, msgs, "SECRET_VERSION_ADD")
	if msgs[0].attrs["versionId"] != version.GetName() {
		t.Fatalf("versionId = %q, want %q", msgs[0].attrs["versionId"], version.GetName())
	}

	// Delete publishes the pre-delete metadata.
	if _, err := client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: secretName}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	msgs = pub.drain()
	wantGRPCEvents(t, msgs, "SECRET_DELETE")
	if msgs[0].attrs["deleteType"] != "REQUESTED" {
		t.Fatalf("deleteType = %q", msgs[0].attrs["deleteType"])
	}
}

// TestGRPCRotationValidation: the documented schedule constraints are enforced
// on the gRPC surface too, and a period-only mask merges into the stored
// schedule.
func TestGRPCRotationValidation(t *testing.T) {
	client, _, secrets, cleanup := newEventTestServer(t)
	defer cleanup()
	ctx := context.Background()

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	at := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(t0.Add(d)) }

	for _, tc := range []struct {
		name   string
		secret *secretmanagerpb.Secret
	}{
		{"next under five minutes", &secretmanagerpb.Secret{Topics: rotateTopic("projects/test/topics/a"), Rotation: &secretmanagerpb.Rotation{NextRotationTime: at(4 * time.Minute)}}},
		{"period under an hour", &secretmanagerpb.Secret{Topics: rotateTopic("projects/test/topics/a"), Rotation: &secretmanagerpb.Rotation{NextRotationTime: at(time.Hour), RotationPeriod: durationpb.New(3599 * time.Second)}}},
		{"period without next", &secretmanagerpb.Secret{Topics: rotateTopic("projects/test/topics/a"), Rotation: &secretmanagerpb.Rotation{RotationPeriod: durationpb.New(time.Hour)}}},
		{"schedule without topics", &secretmanagerpb.Secret{Rotation: &secretmanagerpb.Rotation{NextRotationTime: at(time.Hour)}}},
		{"eleven topics", &secretmanagerpb.Secret{Topics: elevenGRPCTopics()}},
	} {
		_, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
			Parent:   "projects/test",
			SecretId: strings.ReplaceAll(tc.name, " ", "-"),
			Secret:   tc.secret,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: create = %v, want InvalidArgument", tc.name, err)
		}
	}

	// A valid schedule stores the period in the API's google-duration form.
	const secretName = "projects/test/secrets/rot"
	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "rot",
		Secret: &secretmanagerpb.Secret{
			Topics: rotateTopic("projects/test/topics/a"),
			Rotation: &secretmanagerpb.Rotation{
				NextRotationTime: at(time.Hour),
				RotationPeriod:   durationpb.New(time.Hour),
			},
		},
	}); err != nil {
		t.Fatalf("create valid schedule: %v", err)
	}
	stored, err := secrets.GetSecret(ctx, "test", "rot")
	if err != nil {
		t.Fatalf("get stored: %v", err)
	}
	if stored.Rotation.RotationPeriod != "3600s" {
		t.Fatalf("stored rotationPeriod = %q, want 3600s", stored.Rotation.RotationPeriod)
	}

	// A fractional period is preserved rather than truncated to whole seconds,
	// so the gRPC and REST surfaces agree on the stored value.
	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "frac",
		Secret: &secretmanagerpb.Secret{
			Topics: rotateTopic("projects/test/topics/a"),
			Rotation: &secretmanagerpb.Rotation{
				NextRotationTime: at(time.Hour),
				RotationPeriod:   durationpb.New(3600*time.Second + 500*time.Millisecond),
			},
		},
	}); err != nil {
		t.Fatalf("create fractional period: %v", err)
	}
	frac, err := secrets.GetSecret(ctx, "test", "frac")
	if err != nil {
		t.Fatalf("get fractional secret: %v", err)
	}
	if frac.Rotation.RotationPeriod != "3600.500s" {
		t.Fatalf("fractional rotationPeriod = %q, want 3600.500s", frac.Rotation.RotationPeriod)
	}

	// A period-only mask keeps the stored next rotation time.
	updated, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret: &secretmanagerpb.Secret{
			Name:     secretName,
			Rotation: &secretmanagerpb.Rotation{RotationPeriod: durationpb.New(2 * time.Hour)},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rotation.rotation_period"}},
	})
	if err != nil {
		t.Fatalf("update period: %v", err)
	}
	if got := updated.GetRotation().GetNextRotationTime().AsTime(); !got.Equal(t0.Add(time.Hour)) {
		t.Fatalf("nextRotationTime = %v, want the stored %v", got, t0.Add(time.Hour))
	}
	if got := updated.GetRotation().GetRotationPeriod().AsDuration(); got != 2*time.Hour {
		t.Fatalf("rotationPeriod = %v, want 2h", got)
	}
}

func elevenGRPCTopics() []*secretmanagerpb.Topic {
	out := make([]*secretmanagerpb.Topic, 0, 11)
	for i := 0; i < 11; i++ {
		out = append(out, &secretmanagerpb.Topic{Name: "projects/test/topics/t" + string(rune('a'+i))})
	}
	return out
}

// TestGRPCRotateNotification: a due schedule publishes SECRET_ROTATE when a
// read advances it, and adds no version.
func TestGRPCRotateNotification(t *testing.T) {
	client, pub, secrets, cleanup := newEventTestServer(t)
	defer cleanup()
	ctx := context.Background()

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	due := t0.Add(time.Hour)

	const secretName = "projects/test/secrets/rot"
	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "rot",
		Secret: &secretmanagerpb.Secret{
			Topics: rotateTopic("projects/test/topics/a"),
			Rotation: &secretmanagerpb.Rotation{
				NextRotationTime: timestamppb.New(due),
				RotationPeriod:   durationpb.New(time.Hour),
			},
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	pub.drain()

	clock.SetGlobalClock(clock.FixedClock{T: due.Add(time.Minute)})
	sec, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: secretName})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	msgs := pub.drain()
	wantGRPCEvents(t, msgs, "SECRET_ROTATE")
	if msgs[0].attrs["secretId"] != secretName {
		t.Fatalf("SECRET_ROTATE secretId = %q", msgs[0].attrs["secretId"])
	}
	// The reschedule advances from the read instant (the emulator evaluates the
	// schedule lazily), so a read one minute late lands at due+1m+period.
	if want := due.Add(time.Minute).Add(time.Hour); !sec.GetRotation().GetNextRotationTime().AsTime().Equal(want) {
		t.Fatalf("nextRotationTime = %v, want %v", sec.GetRotation().GetNextRotationTime().AsTime(), want)
	}
	versions, err := secrets.ListVersions(ctx, "test", "rot")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 0 {
		t.Fatalf("rotation created versions: %#v", versions)
	}
}
