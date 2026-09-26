package storage

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/gcp/auth"
	"jaiscloud/internal/gcp/downscope"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// authorized returns nr with an Authorization header carrying token, so the
// provider's downscope enforcement sees it.
func authorized(nr *model.NormalizedRequest, token string) *model.NormalizedRequest {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	nr.Raw = req
	return nr
}

func wantDownscopedDenied(t *testing.T, err error) {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != http.StatusForbidden {
		t.Fatalf("expected 403 ProviderError, got %v", err)
	}
}

// TestDownscopeEnforcement exercises the access-boundary enforcement the GCS
// provider applies to object operations for a downscoped credential, and that
// ordinary requests are unrestricted.
func TestDownscopeEnforcement(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Bucket + object created without a credential.
	base := bucketParams()
	base.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, base); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	seed := bucketParams()
	seed.Params["bucket"] = "bkt"
	seed.Params["object"] = "allowed/file.txt"
	seed.Params[wire.MediaKey] = []byte("allowed")
	if _, err := p.ObjectsInsert(ctx, seed); err != nil {
		t.Fatalf("seed object: %v", err)
	}

	rules := []downscope.Rule{{
		Bucket:       "bkt",
		ObjectPrefix: "allowed/",
		Permissions: []string{
			downscope.PermissionLegacyObjectReader,
			downscope.PermissionObjectViewer,
			downscope.PermissionLegacyBucketWriter,
		},
	}}
	token := auth.MintDownscopedToken("sa@example.com", "proj", rules)

	// Inside the boundary: read, list, write a new allowed object, then delete.
	read := authorized(bucketParams(), token)
	read.Params["bucket"] = "bkt"
	read.Params["object"] = "allowed/file.txt"
	if _, err := p.ObjectsGet(ctx, read); err != nil {
		t.Fatalf("allowed get: %v", err)
	}

	list := authorized(bucketParams(), token)
	list.Params["bucket"] = "bkt"
	list.Params["prefix"] = "allowed/"
	if _, err := p.ObjectsList(ctx, list); err != nil {
		t.Fatalf("allowed list: %v", err)
	}

	write := authorized(bucketParams(), token)
	write.Params["bucket"] = "bkt"
	write.Params["object"] = "allowed/other.txt"
	write.Params[wire.MediaKey] = []byte("other")
	if _, err := p.ObjectsInsert(ctx, write); err != nil {
		t.Fatalf("allowed insert: %v", err)
	}

	deleteReq := authorized(bucketParams(), token)
	deleteReq.Params["bucket"] = "bkt"
	deleteReq.Params["object"] = "allowed/file.txt"
	if _, err := p.ObjectsDelete(ctx, deleteReq); err != nil {
		t.Fatalf("allowed delete: %v", err)
	}

	// Outside the boundary: sibling object and listing prefix.
	deniedRead := authorized(bucketParams(), token)
	deniedRead.Params["bucket"] = "bkt"
	deniedRead.Params["object"] = "allowed_sibling/file.txt"
	if _, err := p.ObjectsGet(ctx, deniedRead); err == nil {
		t.Fatal("expected sibling read to be denied")
	} else {
		wantDownscopedDenied(t, err)
	}

	deniedList := authorized(bucketParams(), token)
	deniedList.Params["bucket"] = "bkt"
	deniedList.Params["prefix"] = "allowed_sibling/"
	if _, err := p.ObjectsList(ctx, deniedList); err == nil {
		t.Fatal("expected sibling list to be denied")
	} else {
		wantDownscopedDenied(t, err)
	}

	deniedWrite := authorized(bucketParams(), token)
	deniedWrite.Params["bucket"] = "bkt"
	deniedWrite.Params["object"] = "allowed_sibling/file.txt"
	deniedWrite.Params[wire.MediaKey] = []byte("denied")
	if _, err := p.ObjectsInsert(ctx, deniedWrite); err == nil {
		t.Fatal("expected sibling create to be denied")
	} else {
		wantDownscopedDenied(t, err)
	}

	// Bucket-level operations are always denied for a downscoped credential.
	bucketGet := authorized(bucketParams(), token)
	bucketGet.Params["bucket"] = "bkt"
	if _, err := p.BucketsGet(ctx, bucketGet); err == nil {
		t.Fatal("expected bucket get to be denied")
	} else {
		wantDownscopedDenied(t, err)
	}

	// An ordinary (non-downscoped) request remains unrestricted.
	plain := bucketParams()
	plain.Params["bucket"] = "bkt"
	plain.Params["object"] = "allowed_sibling/file.txt"
	plain.Params[wire.MediaKey] = []byte("plain")
	plain.Raw = httptest.NewRequest(http.MethodGet, "/", nil)
	plain.Raw.Header.Set("Authorization", "Bearer ordinary-access-token")
	if _, err := p.ObjectsInsert(ctx, plain); err != nil {
		t.Fatalf("ordinary insert should be allowed: %v", err)
	}
}

// TestDownscopeMoveRequiresSourceDelete verifies a move needs delete on the
// source (not just read + destination write), matching real GCS / the reference.
func TestDownscopeMoveRequiresSourceDelete(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	for _, name := range []string{"src", "dst"} {
		nr := bucketParams()
		nr.Params["body"] = map[string]any{"name": name}
		if _, err := p.BucketsInsert(ctx, nr); err != nil {
			t.Fatalf("insert bucket %s: %v", name, err)
		}
	}
	seed := bucketParams()
	seed.Params["bucket"] = "src"
	seed.Params["object"] = "src.txt"
	seed.Params[wire.MediaKey] = []byte("data")
	if _, err := p.ObjectsInsert(ctx, seed); err != nil {
		t.Fatalf("seed object: %v", err)
	}

	// read src + write dst, but no delete on src -> denied.
	readOnly := auth.MintDownscopedToken("sa@example.com", "proj", []downscope.Rule{
		{Bucket: "src", Permissions: []string{downscope.PermissionObjectViewer}},
		{Bucket: "dst", Permissions: []string{downscope.PermissionLegacyBucketWriter}},
	})
	denied := authorized(bucketParams(), readOnly)
	denied.Params["sourceBucket"] = "src"
	denied.Params["sourceObject"] = "src.txt"
	denied.Params["destinationBucket"] = "dst"
	denied.Params["destinationObject"] = "dst.txt"
	if _, err := p.ObjectsMove(ctx, denied); err == nil {
		t.Fatal("expected move without source delete permission to be denied")
	} else {
		wantDownscopedDenied(t, err)
	}

	// read + delete src + write dst -> allowed.
	writer := auth.MintDownscopedToken("sa@example.com", "proj", []downscope.Rule{
		{Bucket: "src", Permissions: []string{downscope.PermissionObjectViewer, downscope.PermissionLegacyBucketWriter}},
		{Bucket: "dst", Permissions: []string{downscope.PermissionLegacyBucketWriter}},
	})
	allowed := authorized(bucketParams(), writer)
	allowed.Params["sourceBucket"] = "src"
	allowed.Params["sourceObject"] = "src.txt"
	allowed.Params["destinationBucket"] = "dst"
	allowed.Params["destinationObject"] = "dst.txt"
	if _, err := p.ObjectsMove(ctx, allowed); err != nil {
		t.Fatalf("expected move with full permissions to be allowed: %v", err)
	}
}
