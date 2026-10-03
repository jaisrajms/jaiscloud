// Package sdk_test exercises the jaiscloud-gcp emulator through the official
// Google Cloud Storage Go client. This validates wire-level compatibility with
// the real SDK (resumable/multipart uploads, raw media downloads, listing).
//
// Run with the GCP binary running and STORAGE_EMULATOR_HOST set:
//
//	./jaiscloud-gcp start &
//	STORAGE_EMULATOR_HOST=http://localhost:8080 go test -race ./...
//
// This is a separate Go module so the heavy cloud.google.com/go/storage
// dependency does not bloat the main module.
package sdk_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func emulatorHost() string {
	if h := os.Getenv("STORAGE_EMULATOR_HOST"); h != "" {
		return h
	}
	return "http://localhost:8080"
}

func newClient(t *testing.T) *storage.Client {
	t.Helper()
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client
}

// TestSDKWriteReadListDelete covers the core object lifecycle through the SDK.
func TestSDKWriteReadListDelete(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-bucket-%d", time.Now().UnixNano())

	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", &storage.BucketAttrs{Location: "US"}))

	// Write (SDK chooses the upload mechanism: multipart for small objects).
	w := client.Bucket(bucket).Object("hello.txt").NewWriter(ctx)
	w.ContentType = "text/plain"
	if _, err := w.Write([]byte("hello from sdk")); err != nil {
		t.Fatalf("write: %v", err)
	}
	require.NoError(t, w.Close())

	// Read (raw media download path).
	r, err := client.Bucket(bucket).Object("hello.txt").NewReader(ctx)
	require.NoError(t, err)
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	r.Close()
	require.Equal(t, "hello from sdk", string(b))

	// List.
	var names []string
	it := client.Bucket(bucket).Objects(ctx, nil)
	for {
		attrs, err := it.Next()
		if err != nil {
			break
		}
		names = append(names, attrs.Name)
	}
	require.Equal(t, []string{"hello.txt"}, names)

	// Delete object, then bucket.
	require.NoError(t, client.Bucket(bucket).Object("hello.txt").Delete(ctx))
	require.NoError(t, client.Bucket(bucket).Delete(ctx))
}

// TestSDKResumableLargeObject writes a multi-megabyte object with a small
// chunk size, forcing the SDK into a chunked resumable upload (including the
// "bytes */N" status query used to finalize), and verifies the bytes round-trip.
func TestSDKResumableLargeObject(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-large-%d", time.Now().UnixNano())

	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	payload := bytes.Repeat([]byte("abcdefgh"), 1<<20) // 8 MiB

	w := client.Bucket(bucket).Object("large.bin").NewWriter(ctx)
	w.ChunkSize = 2 << 20 // force a multi-chunk resumable upload
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	require.NoError(t, w.Close())

	r, err := client.Bucket(bucket).Object("large.bin").NewReader(ctx)
	require.NoError(t, err)
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, len(payload), len(got))
	require.True(t, bytes.Equal(payload, got), "large object bytes must round-trip exactly")

	require.NoError(t, client.Bucket(bucket).Object("large.bin").Delete(ctx))
	require.NoError(t, client.Bucket(bucket).Delete(ctx))
}
