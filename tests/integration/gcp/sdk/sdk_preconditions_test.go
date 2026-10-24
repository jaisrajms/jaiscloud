package sdk_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

// requirePreconditionFailed asserts the call failed with GCS's 412 Precondition
// Failed (the wire signal for a lost-update/cas guard), not some other error.
func requirePreconditionFailed(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err, "expected a precondition failure, got nil")
	var gerr *googleapi.Error
	require.True(t, errors.As(err, &gerr), "expected *googleapi.Error, got %T: %v", err, err)
	require.Equal(t, 412, gerr.Code, "expected 412, got %d: %s", gerr.Code, gerr.Message)
}

// TestSDKObjectPreconditions drives GCS's generation/metageneration preconditions
// through the official client (`ObjectHandle.If`). The emulator has provider-level
// coverage for these; this suite pins that the *client's* conditions (sent as
// ifGenerationMatch/ifMetagenerationMatch/ifGenerationMatch=0 on insert, PATCH,
// GET and DELETE) are honored end-to-end — the compare-and-swap / safe-delete
// idiom real workloads build on.
func TestSDKObjectPreconditions(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-precond-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))
	obj := client.Bucket(bucket).Object("guarded.txt")

	// Create-only-if-absent (ifGenerationMatch=0): a fresh object is created.
	w := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	_, err := w.Write([]byte("v1"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	first, err := obj.Attrs(ctx)
	require.NoError(t, err)
	gen1 := first.Generation

	// A second create-only-if-absent on the same object is a lost race → 412.
	w = obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	_, _ = w.Write([]byte("v2"))
	requirePreconditionFailed(t, w.Close())

	// A write guarded by the matching generation succeeds...
	w = obj.If(storage.Conditions{GenerationMatch: gen1}).NewWriter(ctx)
	_, err = w.Write([]byte("v2"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// ...and the generation advances.
	second, err := obj.Attrs(ctx)
	require.NoError(t, err)
	gen2 := second.Generation
	require.NotEqual(t, gen1, gen2, "overwrite must produce a new generation")

	// A write guarded by the now-stale generation is 412 (the lost-update guard).
	w = obj.If(storage.Conditions{GenerationMatch: gen1}).NewWriter(ctx)
	_, _ = w.Write([]byte("v3"))
	requirePreconditionFailed(t, w.Close())

	// A read guarded by a stale generation is 412; by the current one succeeds.
	r, err := obj.If(storage.Conditions{GenerationMatch: gen1}).NewReader(ctx)
	if err == nil {
		r.Close()
	}
	requirePreconditionFailed(t, err)

	r, err = obj.If(storage.Conditions{GenerationMatch: gen2}).NewReader(ctx)
	require.NoError(t, err)
	r.Close()

	// Metadata update guarded by the current metageneration succeeds and bumps it.
	meta1 := second.Metageneration
	updated, err := obj.If(storage.Conditions{MetagenerationMatch: meta1}).Update(ctx,
		storage.ObjectAttrsToUpdate{ContentType: "text/plain"})
	require.NoError(t, err)
	require.NotEqual(t, meta1, updated.Metageneration)

	// The same (now-stale) metageneration guard is 412.
	_, err = obj.If(storage.Conditions{MetagenerationMatch: meta1}).Update(ctx,
		storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
	requirePreconditionFailed(t, err)

	// Safe delete: a stale generation is 412; the object survives.
	requirePreconditionFailed(t, obj.If(storage.Conditions{GenerationMatch: gen1}).Delete(ctx))
	_, err = obj.Attrs(ctx)
	require.NoError(t, err, "object must survive the rejected delete")

	// Delete guarded by the current generation succeeds.
	require.NoError(t, obj.If(storage.Conditions{GenerationMatch: gen2}).Delete(ctx))
	require.NoError(t, client.Bucket(bucket).Delete(ctx))
}
