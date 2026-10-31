package seams_test

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	gcpadapter "jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/eventing"
	pubsubprovider "jaiscloud/internal/gcp/provider/pubsub"
	"jaiscloud/internal/gcp/provider/storage"
	"jaiscloud/internal/gcp/seams"
	"jaiscloud/internal/gcp/store/gcs"
	kms "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

func newStorage() *storage.Provider {
	return storage.New(gcs.NewMemoryObjectStore(), store.NewMemoryResourceStore(), blobfs.NewMemoryBlobStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))
}

// H1: a service switched off has no handlers on the live registry, but the services that
// depend on it keep working, as they did before the seams.
func TestSeamFallsBackWhenTheTargetServiceIsSwitchedOff(t *testing.T) {
	p := newStorage()
	live := provider.NewRegistry() // storage is off: nothing registered
	all := provider.NewRegistry().Register(p)
	st := seams.New(live, gcpadapter.NewAdapter(""), clock.RealClock{}, 0, "us-central1", "proj").WithFallback(all).Storage()
	ctx := context.Background()
	if err := st.PutObjectBytes(ctx, "proj", "b1", "o", "text/plain", []byte("x")); err != nil {
		t.Fatalf("PutObjectBytes with storage off: %v", err)
	}
	if b, err := st.FetchObjectBytes(ctx, "b1", "o"); err != nil || string(b) != "x" {
		t.Fatalf("FetchObjectBytes with storage off = %q, %v", b, err)
	}
	// Without the fallback the same call is UnknownAction, which is what main did before this fix.
	bare := seams.New(live, gcpadapter.NewAdapter(""), clock.RealClock{}, 0, "us-central1", "proj").Storage()
	if err := bare.EnsureBucket(ctx, "proj", "b2", ""); err == nil {
		t.Fatal("expected UnknownAction without a fallback registry")
	}
}

// M2: objects the emulator stages are written the way they always were, whatever
// versioning or retention the user put on the bucket.
func TestStagedWritesIgnoreBucketRetentionAndVersioning(t *testing.T) {
	p := newStorage()
	reg := provider.NewRegistry().Register(p)
	st := seams.New(reg, gcpadapter.NewAdapter(""), clock.RealClock{}, 0, "us-central1", "proj").Storage()
	ctx := context.Background()

	nr := &model.NormalizedRequest{Service: "storage", Action: "BucketsInsert", AccountID: "proj", Cloud: model.CloudGCP,
		Params: map[string]any{"project": "proj", "body": map[string]any{
			"name": "locked", "retentionPolicy": map[string]any{"retentionPeriod": "3600"},
			"versioning": map[string]any{"enabled": true}}}, ResourceID: func(a, b string) string { return b }}
	if _, err := reg.Dispatch(ctx, "Storage.BucketsInsert", nr); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := st.PutObjectBytes(ctx, "proj", "locked", "driver-output", "text/plain", []byte{byte('a' + i)}); err != nil {
			t.Fatalf("staged write %d into a bucket with retention and versioning: %v", i+1, err)
		}
	}
	if b, _ := st.FetchObjectBytes(ctx, "locked", "driver-output"); string(b) != "c" {
		t.Fatalf("the last write must win, got %q", b)
	}
}

// M4: a listing whose next-page token never ends must stop with a clear error.
func TestObjectListingWithARepeatingTokenStops(t *testing.T) {
	p := newStorage()
	reg := provider.NewRegistry().Register(p)
	reg.Intercept("Storage.ObjectsList", func(provider.HandlerFunc) provider.HandlerFunc {
		return func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return provider.OK(map[string]any{"items": []any{map[string]any{"name": "o"}}, "nextPageToken": "same"}), nil
		}
	})
	st := seams.New(reg, gcpadapter.NewAdapter(""), clock.RealClock{}, 0, "us-central1", "proj").Storage()
	_, err := st.ListObjectNames(context.Background(), "b", "")
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.Code != "PluginProtocolError" {
		t.Fatalf("err = %v, want PluginProtocolError", err)
	}
}

// M3 + L1: re-pointing an Eventarc subscription keeps its settings and the id of a
// published message comes back from the built-in.
func TestEventarcSubscriptionIsRepointedInPlace(t *testing.T) {
	res := store.NewMemoryResourceStore()
	p := pubsubprovider.New(res, pubsubstore.NewMemoryMessages(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))
	reg := provider.NewRegistry().Register(p)
	ps := seams.New(reg, gcpadapter.NewAdapter(""), clock.RealClock{}, 0, "us-central1", "proj").PubSub()
	ctx := context.Background()
	call := func(action, name string, body map[string]any) *model.ProviderResponse {
		t.Helper()
		nr := &model.NormalizedRequest{Service: "pubsub", Action: action, AccountID: "proj", Cloud: model.CloudGCP,
			Params: map[string]any{"project": "proj", "name": name, "body": body}, ResourceID: func(a, b string) string { return "projects/proj/" + a[len("pubsub-"):] + "s/" + b }}
		resp, err := reg.Dispatch(ctx, "PubSub."+action, nr)
		if err != nil {
			t.Fatalf("%s %s: %v", action, name, err)
		}
		return resp
	}
	call("TopicCreate", "topics/a", nil)
	call("TopicCreate", "topics/b", nil)
	if _, err := ps.EnsureEventarcSubscription(ctx, "proj", "us-central1", "trg", "a"); err != nil {
		t.Fatal(err)
	}
	subName := "subscriptions/" + eventing.EventarcSubscriptionID("us-central1", "trg")
	// A user tunes the subscription, then publishes a message to it.
	call("SubscriptionUpdate", subName, map[string]any{"updateMask": "ackDeadlineSeconds,enableMessageOrdering",
		"ackDeadlineSeconds": float64(60), "enableMessageOrdering": true})
	id, err := ps.PublishEvent(ctx, "proj", "a", []byte("queued"), nil)
	if err != nil || id == "" {
		t.Fatalf("PublishEvent id = %q, %v (the built-in's message id must come back)", id, err)
	}

	if _, err := ps.EnsureEventarcSubscription(ctx, "proj", "us-central1", "trg", "b"); err != nil {
		t.Fatalf("re-point: %v", err)
	}
	got := call("SubscriptionGet", subName, nil).Data
	if topic, _ := got["topic"].(string); topic != "projects/proj/topics/b" {
		t.Fatalf("topic = %v, want it re-pointed to b", got["topic"])
	}
	if got["ackDeadlineSeconds"] != float64(60) && got["ackDeadlineSeconds"] != 60 {
		t.Fatalf("ackDeadlineSeconds = %v, the user's 60 must survive the re-point", got["ackDeadlineSeconds"])
	}
	if got["enableMessageOrdering"] != true {
		t.Fatalf("enableMessageOrdering = %v, must survive the re-point", got["enableMessageOrdering"])
	}
}
