package seams_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	gcpadapter "jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/provider/storage"
	"jaiscloud/internal/gcp/seams"
	"jaiscloud/internal/gcp/store/gcs"
	kms "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// The Cloud Storage seam behaves like the provider's own server-side helpers it
// replaces, and every call it makes is a registry call a plugin could wrap.
func TestStorageSeamRoundTripThroughTheRegistry(t *testing.T) {
	p := storage.New(gcs.NewMemoryObjectStore(), store.NewMemoryResourceStore(), blobfs.NewMemoryBlobStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))
	reg := provider.NewRegistry().Register(p)

	var mu sync.Mutex
	var seen []string
	for key := range p.Routes() {
		reg.Intercept(key, func(next provider.HandlerFunc) provider.HandlerFunc {
			return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
				mu.Lock()
				seen = append(seen, nr.Action+":"+string(nr.Origin))
				mu.Unlock()
				return next(ctx, nr)
			}
		})
	}
	st := seams.New(reg, gcpadapter.NewAdapter(""), clock.RealClock{}, 0, "us-central1", "proj").Storage()
	ctx := context.Background()

	if err := st.PutObjectBytes(ctx, "proj", "b1", "dir/a.txt", "text/plain", []byte("alpha")); err != nil {
		t.Fatalf("PutObjectBytes: %v", err)
	}
	if err := st.PutObjectBytes(ctx, "proj", "b1", "dir/b.txt", "", []byte("beta")); err != nil {
		t.Fatalf("PutObjectBytes second: %v", err)
	}
	got, err := st.FetchObjectBytes(ctx, "b1", "dir/a.txt")
	if err != nil || string(got) != "alpha" {
		t.Fatalf("FetchObjectBytes = %q, %v", got, err)
	}
	if _, err := st.FetchObjectBytes(ctx, "b1", "missing"); !errors.Is(err, gcs.ErrNoSuchObject) {
		t.Fatalf("a missing object must be gcs.ErrNoSuchObject, got %v", err)
	}
	names, err := st.ListObjectNames(ctx, "b1", "dir/")
	if err != nil || len(names) != 2 || names[0] != "dir/a.txt" || names[1] != "dir/b.txt" {
		t.Fatalf("ListObjectNames = %v, %v", names, err)
	}
	if names, err := st.ListObjectNames(ctx, "no-such-bucket", ""); err != nil || len(names) != 0 {
		t.Fatalf("an absent bucket lists nothing: %v, %v", names, err)
	}
	if err := st.EnsureBucket(ctx, "proj", "b1", ""); err != nil {
		t.Fatalf("EnsureBucket on an existing bucket: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("the seam made no registry call")
	}
	for _, s := range seen {
		if s[len(s)-len("internal"):] != "internal" {
			t.Errorf("call %q was not an internal client call", s)
		}
	}
}
