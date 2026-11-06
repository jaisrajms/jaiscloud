package gcplifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"jaiscloud/internal/clock"

	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/grpc"

	"jaiscloud/internal/gcp/resource"
	kmscore "jaiscloud/internal/gcp/service/kms"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	grpckms "jaiscloud/internal/gcp/transport/grpc/kms"
	restkms "jaiscloud/internal/gcp/transport/rest/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const kmsLocation = "global"

// kmsManifest records the documented Cloud KMS ownership contract: a key ring
// has no delete operation at all (not in the JSON API, not in the proto, and
// not in real Cloud KMS either), so there is no cascade to pin. Ownership is
// exercised one level down, on the crypto key whose versions block a delete.
var kmsManifest = manifest{
	parent:   "kms key ring",
	policy:   NoDelete,
	noUpdate: true,
	note:     "Cloud KMS key rings are undeletable, so there is no ring->key cascade to assert; ownership is exercised one level down (versions block a crypto-key delete) in the extra step",
}

type kmsAdapter struct {
	core *kmscore.Service
	keys kmsstore.Store
	rest *restkms.Provider
	grpc kmspb.KeyManagementServiceClient
	proj string
}

func newKMSAdapter(t *testing.T) adapter {
	t.Helper()
	keys := kmsstore.NewMemoryStore()
	res := store.NewMemoryResourceStore()
	core := kmscore.NewService(keys, res, "proj")
	conn := serveGRPC(t, func(srv *grpc.Server) {
		kmspb.RegisterKeyManagementServiceServer(srv, grpckms.NewService(core, "proj"))
	})
	return &kmsAdapter{
		core: core,
		keys: keys,
		rest: restkms.New(core),
		grpc: kmspb.NewKeyManagementServiceClient(conn),
		proj: "proj",
	}
}

func (a *kmsAdapter) name() string { return "kms key ring" }

func (a *kmsAdapter) create(ctx context.Context, tr transport, id string) (string, error) {
	if tr == grpcT {
		_, err := a.grpc.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
			Parent:    "projects/" + a.proj + "/locations/" + kmsLocation,
			KeyRingId: id,
		})
		if err != nil {
			return "", err
		}
		return "", nil
	}
	if _, err := a.rest.KeyRingCreate(ctx, a.nr(map[string]any{"location": kmsLocation, "keyRingId": id})); err != nil {
		return "", err
	}
	return "", nil
}

func (a *kmsAdapter) get(ctx context.Context, tr transport, id string) (bool, error) {
	if tr == grpcT {
		_, err := a.grpc.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: a.keyRingName(id)})
		return err == nil, ignoreNotFound(err)
	}
	_, err := a.rest.KeyRingGet(ctx, a.nr(map[string]any{"name": "locations/" + kmsLocation + "/keyRings/" + id}))
	return err == nil, ignoreNotFound(err)
}

func (a *kmsAdapter) list(ctx context.Context, tr transport) ([]string, error) {
	if tr == grpcT {
		resp, err := a.grpc.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{Parent: "projects/" + a.proj + "/locations/" + kmsLocation})
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(resp.GetKeyRings()))
		for _, kr := range resp.GetKeyRings() {
			out = append(out, shortName(kr.GetName()))
		}
		return out, nil
	}
	resp, err := a.rest.KeyRingList(ctx, a.nr(map[string]any{"location": kmsLocation}))
	if err != nil {
		return nil, err
	}
	items, _ := resp.Data["keyRings"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		m, _ := it.(map[string]any)
		if name, _ := m["name"].(string); name != "" {
			out = append(out, shortName(name))
		}
	}
	return out, nil
}

func (a *kmsAdapter) update(context.Context, transport, string) error { return nil }

// remove is never called for a NoDelete manifest; keep it explicit so a future
// policy change fails loudly instead of silently deleting nothing.
func (a *kmsAdapter) remove(context.Context, transport, string) error {
	return errors.New("kms key rings have no delete operation")
}

func (a *kmsAdapter) blockedDelete(context.Context, transport, string) []blockedResult {
	return nil
}

func (a *kmsAdapter) populate(ctx context.Context, id string) error {
	_, err := a.createCryptoKey(ctx, restT, id, "populated")
	return err
}

func (a *kmsAdapter) counts(ctx context.Context, id string) (map[string]childCount, error) {
	keys, err := a.keys.ListCryptoKeys(ctx, a.proj, kmsLocation, id)
	if err != nil && !errors.Is(err, kmsstore.ErrNoSuchKeyRing) {
		return nil, err
	}
	return map[string]childCount{"cryptoKeys": {total: len(keys), attached: len(keys)}}, nil
}

func (a *kmsAdapter) clearRejecting(context.Context, string) error { return nil }

// extra pins the crypto-key lifecycle: versions block a key delete, a version
// must be DESTROYED before it can be deleted, a deleted key's name is retired,
// and a crypto key under a never-created key ring is NOT_FOUND (it used to be
// accepted).
func (a *kmsAdapter) extra(t *testing.T, ctx context.Context, tr transport, id string) {
	t.Helper()
	if _, err := a.createCryptoKey(ctx, tr, id, "key1"); err != nil {
		t.Fatalf("create crypto key: %v", err)
	}
	if _, err := a.createVersion(ctx, tr, id, "key1"); err != nil {
		t.Fatalf("create crypto key version: %v", err)
	}

	// A key with versions refuses to delete.
	err := a.deleteCryptoKey(ctx, tr, id, "key1")
	if err == nil {
		t.Fatalf("delete crypto key with live versions succeeded, want FAILED_PRECONDITION")
	}
	if code, status := codeOf(err); !codeMatches(code, status, "FailedPrecondition") {
		t.Fatalf("delete crypto key with versions = %v, want FailedPrecondition", err)
	}

	// An ENABLED version refuses to delete until it is destroyed.
	if err := a.deleteVersion(ctx, tr, id, "key1", "1"); err == nil {
		t.Fatalf("delete ENABLED version succeeded, want FAILED_PRECONDITION")
	}
	for _, version := range []string{"1", "2"} {
		if err := a.destroyVersion(ctx, tr, id, "key1", version); err != nil {
			t.Fatalf("destroy version %s: %v", version, err)
		}
		// A destroy is scheduled 30 days out (Cloud KMS default); the emulator
		// promotes lazily, so stand in for the passage of time and then delete the
		// DESTROYED version.
		if _, err := a.keys.PromoteDestroyed(ctx, a.proj, kmsLocation, id, "key1", clock.Now().Add(31*24*time.Hour)); err != nil {
			t.Fatalf("promote destroyed version %s: %v", version, err)
		}
		if err := a.deleteVersion(ctx, tr, id, "key1", version); err != nil {
			t.Fatalf("delete destroyed version %s: %v", version, err)
		}
	}
	if err := a.deleteCryptoKey(ctx, tr, id, "key1"); err != nil {
		t.Fatalf("delete crypto key after removing its versions: %v", err)
	}
	if found, err := a.getCryptoKey(ctx, tr, id, "key1"); err != nil || found {
		t.Fatalf("crypto key after delete = found=%v err=%v, want not found", found, err)
	}

	// A deleted key's name is retired: re-creating it answers AlreadyExists.
	if _, err := a.createCryptoKey(ctx, tr, id, "key1"); !isAlreadyExists(err) {
		t.Fatalf("re-create a retired crypto key = %v, want AlreadyExists", err)
	}

	// A crypto key under a key ring that does not exist is NOT_FOUND.
	if _, err := a.createCryptoKey(ctx, tr, "missing-ring", "orphan"); !isNotFound(err) {
		t.Fatalf("create crypto key under a missing key ring = %v, want NotFound", err)
	}
}

func (a *kmsAdapter) keyRingName(id string) string {
	return "projects/" + a.proj + "/locations/" + kmsLocation + "/keyRings/" + id
}

func (a *kmsAdapter) cryptoKeyName(ring, key string) string {
	return a.keyRingName(ring) + "/cryptoKeys/" + key
}

func (a *kmsAdapter) createCryptoKey(ctx context.Context, tr transport, ring, key string) (string, error) {
	if tr == grpcT {
		ck, err := a.grpc.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
			Parent:      a.keyRingName(ring),
			CryptoKeyId: key,
			CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
		})
		if err != nil {
			return "", err
		}
		return ck.GetName(), nil
	}
	_, err := a.rest.CryptoKeyCreate(ctx, a.nr(map[string]any{
		"name":        "locations/" + kmsLocation + "/keyRings/" + ring,
		"cryptoKeyId": key,
		"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	}))
	if err != nil {
		return "", err
	}
	return a.cryptoKeyName(ring, key), nil
}

func (a *kmsAdapter) getCryptoKey(ctx context.Context, tr transport, ring, key string) (bool, error) {
	if tr == grpcT {
		_, err := a.grpc.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: a.cryptoKeyName(ring, key)})
		return err == nil, ignoreNotFound(err)
	}
	_, err := a.rest.CryptoKeyGet(ctx, a.nr(map[string]any{"name": "locations/" + kmsLocation + "/keyRings/" + ring + "/cryptoKeys/" + key}))
	return err == nil, ignoreNotFound(err)
}

func (a *kmsAdapter) createVersion(ctx context.Context, tr transport, ring, key string) (string, error) {
	if tr == grpcT {
		v, err := a.grpc.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{
			Parent:           a.cryptoKeyName(ring, key),
			CryptoKeyVersion: &kmspb.CryptoKeyVersion{},
		})
		if err != nil {
			return "", err
		}
		return v.GetName(), nil
	}
	resp, err := a.rest.CryptoKeyVersionCreate(ctx, a.nr(map[string]any{
		"name": "locations/" + kmsLocation + "/keyRings/" + ring + "/cryptoKeys/" + key,
		"body": map[string]any{},
	}))
	if err != nil {
		return "", err
	}
	name, _ := resp.Data["name"].(string)
	return name, nil
}

func (a *kmsAdapter) destroyVersion(ctx context.Context, tr transport, ring, key, version string) error {
	name := a.cryptoKeyName(ring, key) + "/cryptoKeyVersions/" + version
	if tr == grpcT {
		_, err := a.grpc.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: name})
		return err
	}
	_, err := a.rest.CryptoKeyVersionDestroy(ctx, a.nr(map[string]any{"name": shortKMSPath(name)}))
	return err
}

func (a *kmsAdapter) deleteVersion(ctx context.Context, tr transport, ring, key, version string) error {
	name := a.cryptoKeyName(ring, key) + "/cryptoKeyVersions/" + version
	if tr == grpcT {
		_, err := a.grpc.DeleteCryptoKeyVersion(ctx, &kmspb.DeleteCryptoKeyVersionRequest{Name: name})
		return err
	}
	_, err := a.rest.CryptoKeyVersionDelete(ctx, a.nr(map[string]any{"name": shortKMSPath(name)}))
	return err
}

func (a *kmsAdapter) deleteCryptoKey(ctx context.Context, tr transport, ring, key string) error {
	if tr == grpcT {
		_, err := a.grpc.DeleteCryptoKey(ctx, &kmspb.DeleteCryptoKeyRequest{Name: a.cryptoKeyName(ring, key)})
		return err
	}
	_, err := a.rest.CryptoKeyDelete(ctx, a.nr(map[string]any{"name": "locations/" + kmsLocation + "/keyRings/" + ring + "/cryptoKeys/" + key}))
	return err
}

func (a *kmsAdapter) nr(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{AccountID: a.proj, Params: params, ResourceID: resource.ResourceID(a.proj)}
}

// shortKMSPath trims a full resource name to the "locations/…" form the REST
// adapter's resourceName helper expects.
func shortKMSPath(name string) string {
	for i := 0; i+len("locations/") <= len(name); i++ {
		if name[i:i+len("locations/")] == "locations/" {
			return name[i:]
		}
	}
	return name
}
