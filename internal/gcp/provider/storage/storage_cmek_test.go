package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/gcs"
	kms "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func TestGCSCMEKRoundTrip(t *testing.T) {
	ctx := context.Background()
	kmsStore := kms.NewMemoryStore()
	enc := crypto.NewEnvelopeEncryptor(kmsStore)
	p := New(gcs.NewMemoryObjectStore(), store.NewMemoryResourceStore(), blobfs.NewMemoryBlobStore(), enc)

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

	// Create a bucket with a default CMEK (GCS field: encryption.defaultKmsKeyName).
	nr := bucketParams()
	nr.Params["body"] = map[string]any{
		"name":       "cmek-bkt",
		"encryption": map[string]any{"defaultKmsKeyName": kmsKeyName},
	}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Insert an object; it must be envelope-encrypted under the bucket's CMEK.
	nr = bucketParams()
	nr.Params["bucket"] = "cmek-bkt"
	nr.Params["object"] = "secret.txt"
	nr.Params[wire.MediaKey] = []byte("top secret")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert object: %v", err)
	}

	// Metadata surfaces the resolved kmsKeyName.
	nr = bucketParams()
	nr.Params["bucket"] = "cmek-bkt"
	nr.Params["object"] = "secret.txt"
	get, err := p.ObjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if k, _ := get.Data["kmsKeyName"].(string); k != kmsKeyName {
		t.Fatalf("expected kmsKeyName %q, got %q", kmsKeyName, k)
	}

	// Media round-trips.
	nr = bucketParams()
	nr.Params["bucket"] = "cmek-bkt"
	nr.Params["object"] = "secret.txt"
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "top secret" {
		t.Fatalf("expected %q, got %q", "top secret", got)
	}
}

// TestGCSCMEKMetadataUpdateKeepsReadable verifies that objects.patch on a CMEK
// object preserves the stored wrapped DEK, so the object stays readable. The
// wire object carries no WrappedDEK, so a whole-struct store replacement would
// silently zero it and make the object undecryptable.
func TestGCSCMEKMetadataUpdateKeepsReadable(t *testing.T) {
	ctx := context.Background()
	kmsStore := kms.NewMemoryStore()
	enc := crypto.NewEnvelopeEncryptor(kmsStore)
	p := New(gcs.NewMemoryObjectStore(), store.NewMemoryResourceStore(), blobfs.NewMemoryBlobStore(), enc)

	projectID := "my-project"
	kmsKeyName := "projects/" + projectID + "/locations/global/keyRings/r/cryptoKeys/k"
	if err := kmsStore.CreateKeyRing(ctx, projectID, "global", "r", kms.KeyRing{ID: "r"}); err != nil {
		t.Fatal(err)
	}
	if err := kmsStore.CreateCryptoKey(ctx, projectID, "global", "r", "k", kms.CryptoKey{ID: "k", Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatal(err)
	}

	nr := bucketParams()
	nr.Params["body"] = map[string]any{
		"name":       "cmek-upd",
		"encryption": map[string]any{"defaultKmsKeyName": kmsKeyName},
	}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "cmek-upd"
	nr.Params["object"] = "secret.txt"
	nr.Params[wire.MediaKey] = []byte("top secret")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert object: %v", err)
	}

	// PATCH metadata; the wrapped DEK must survive so the media still decrypts.
	nr = bucketParamsWithObj("cmek-upd", "secret.txt")
	nr.Params["body"] = map[string]any{"contentType": "application/json"}
	if _, err := p.ObjectsPatch(ctx, nr); err != nil {
		t.Fatalf("patch: %v", err)
	}
	nr = bucketParamsWithObj("cmek-upd", "secret.txt")
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get media after patch: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "top secret" {
		t.Fatalf("media after patch = %q, want %q", got, "top secret")
	}
}

func TestGCSCSEKRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// CSEK: a 32-byte AES-256 key, base64-encoded plus its base64 SHA-256.
	key := []byte("0123456789abcdef0123456789abcdef")
	keyB64 := base64.StdEncoding.EncodeToString(key)
	sum := sha256.Sum256(key)
	shaB64 := base64.StdEncoding.EncodeToString(sum[:])

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	nr.Params[wire.MediaKey] = []byte("classified")
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = shaB64
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert object: %v", err)
	}

	// Read without the key header → 400 (missing CSEK header on a CSEK read).
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	if _, err := p.ObjectsGetMedia(ctx, nr); err == nil {
		t.Fatal("expected error reading CSEK object without key header")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 400 {
		t.Fatalf("expected 400, got %v", err)
	}

	// Read with the key header → round-trips.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = shaB64
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get media with key: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "classified" {
		t.Fatalf("expected %q, got %q", "classified", got)
	}
}
