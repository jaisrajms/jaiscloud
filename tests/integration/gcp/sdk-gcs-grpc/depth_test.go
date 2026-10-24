// depth_test.go closes TD10's Cloud Storage gRPC p2/p5 gap. gcs_grpc_test.go
// proves the gRPC surface's baseline lifecycle (bucket, direct/resumable
// write, read, update, compose, list); this file adds the two behaviour classes
// the gRPC transport previously had no assertion for:
//
//	p2 concurrency   - parallel writer streams to distinct objects, parallel
//	                   reader streams, and preconditions under contention: many
//	                   writers racing DoesNotExist (or one generation) on a
//	                   single object leave exactly one winner and the rest
//	                   FAILED_PRECONDITION, so no write is silently lost. The
//	                   client runs under -race.
//	p5 scale         - a large object streamed up and read back through the gRPC
//	                   read surface (length + content hash verified while
//	                   streaming, plus a mid-object range read) and a
//	                   many-object listing.
//
// p3 (error envelopes) and p4 (listing/paging) for the service already have
// REST coverage in the `sdk` suite (docs/test-depth/storage.yaml); this file
// records sdk-gcs-grpc against p2/p5 instead.
//
// BidiReadObject stays out of scope (plan_docs/gcp-test-depth-wave-plan.md §5).
package gcs_grpc_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// depthBucket creates a bucket for one depth case and reaps it (with its
// objects) on cleanup. The client is closed through a cleanup registered
// *before* the bucket reaper, so the LIFO order deletes the objects while the
// client is still open.
func depthBucket(t *testing.T, client *storage.Client, suffix string) *storage.BucketHandle {
	t.Helper()
	ctx := context.Background()
	bucket := client.Bucket(uniqueName("go-gcs-grpc-" + suffix))
	require.NoError(t, bucket.Create(ctx, ProjectID(), &storage.BucketAttrs{Location: "US"}))
	t.Cleanup(func() {
		it := bucket.Objects(ctx, nil)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				break
			}
			_ = bucket.Object(attrs.Name).Delete(ctx)
		}
		require.NoError(t, bucket.Delete(ctx))
	})
	return bucket
}

// wantPreconditionFailed asserts err is the FAILED_PRECONDITION a losing
// concurrent writer gets. The storage client passes the gRPC status through
// unchanged (it only wraps NOT_FOUND as storage.ErrObjectNotExist), so an
// exact code match is what catches a parity regression.
func wantPreconditionFailed(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "want FAILED_PRECONDITION, got %v", err)
}

// putObject writes payload to the object and fails the test on error.
func putObject(t *testing.T, obj *storage.ObjectHandle, payload []byte) {
	t.Helper()
	w := obj.NewWriter(context.Background())
	w.ContentType = "application/octet-stream"
	_, err := w.Write(payload)
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

// TestGCSGRPCDepthConcurrency is the p2 class for the gRPC transport: parallel
// streams and precondition contention.
func TestGCSGRPCDepthConcurrency(t *testing.T) {
	ctx := context.Background()
	client := StorageGRPCClient(ctx)
	t.Cleanup(func() { _ = client.Close() })
	bucket := depthBucket(t, client, "depth-conc")

	t.Run("ParallelWriterAndReaderStreams", func(t *testing.T) {
		const objects = 8
		names := make([]string, objects)
		payloads := make([][]byte, objects)
		var wg sync.WaitGroup

		// Distinct objects written and read concurrently: no stream
		// interferes with another.
		start := make(chan struct{})
		errs := make([]error, objects)
		for i := 0; i < objects; i++ {
			names[i] = fmt.Sprintf("stream-%d.bin", i)
			payloads[i] = bytes.Repeat([]byte(fmt.Sprintf("chunk-%d-", i)), 8*1024)
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				w := bucket.Object(names[i]).NewWriter(ctx)
				w.ChunkSize = 0
				if _, err := w.Write(payloads[i]); err != nil {
					errs[i] = err
					return
				}
				errs[i] = w.Close()
			}(i)
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			require.NoError(t, err, "writer %d", i)
		}

		// Read every object back concurrently and verify the bytes.
		start = make(chan struct{})
		readErrs := make([]error, objects)
		for i := 0; i < objects; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				r, err := bucket.Object(names[i]).NewReader(ctx)
				if err != nil {
					readErrs[i] = err
					return
				}
				defer r.Close()
				got, err := io.ReadAll(r)
				if err != nil {
					readErrs[i] = err
					return
				}
				if !bytes.Equal(got, payloads[i]) {
					readErrs[i] = fmt.Errorf("object %s: read %d bytes, want %d", names[i], len(got), len(payloads[i]))
				}
			}(i)
		}
		close(start)
		wg.Wait()
		for i, err := range readErrs {
			require.NoError(t, err, "reader %d", i)
		}
	})

	t.Run("DoesNotExistContention", func(t *testing.T) {
		const writers = 8
		obj := bucket.Object("contended-exclusive.bin")
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, writers)
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				w := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
				w.ChunkSize = 0
				if _, err := w.Write([]byte(fmt.Sprintf("writer-%d", i))); err != nil {
					errs[i] = err
					return
				}
				errs[i] = w.Close()
			}(i)
		}
		close(start)
		wg.Wait()

		winners := 0
		for _, err := range errs {
			if err == nil {
				winners++
				continue
			}
			wantPreconditionFailed(t, err)
		}
		require.Equal(t, 1, winners, "exactly one DoesNotExist writer may win")

		// The winner's bytes are the ones that landed.
		r, err := obj.NewReader(ctx)
		require.NoError(t, err)
		defer r.Close()
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Regexp(t, `^writer-[0-7]$`, string(got))
	})

	t.Run("GenerationMatchContention", func(t *testing.T) {
		obj := bucket.Object("contended-generation.bin")
		putObject(t, obj, []byte("initial"))
		attrs, err := obj.Attrs(ctx)
		require.NoError(t, err)
		gen := attrs.Generation

		const writers = 8
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, writers)
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				w := obj.If(storage.Conditions{GenerationMatch: gen}).NewWriter(ctx)
				w.ChunkSize = 0
				if _, err := w.Write([]byte(fmt.Sprintf("gen-%d", i))); err != nil {
					errs[i] = err
					return
				}
				errs[i] = w.Close()
			}(i)
		}
		close(start)
		wg.Wait()

		winners := 0
		for _, err := range errs {
			if err == nil {
				winners++
				continue
			}
			wantPreconditionFailed(t, err)
		}
		require.Equal(t, 1, winners, "a generation-matched write is first-writer-wins")

		// The object advanced to a new generation (no lost update). GCS does
		// not promise the next generation is +1, only that it is unique.
		after, err := obj.Attrs(ctx)
		require.NoError(t, err)
		assert.NotEqual(t, gen, after.Generation)
	})
}

// TestGCSGRPCDepthStreamedRead is the p5 class for the gRPC transport: a large
// object streamed up and read back, plus a range read and a many-object list.
func TestGCSGRPCDepthStreamedRead(t *testing.T) {
	ctx := context.Background()
	client := StorageGRPCClient(ctx)
	t.Cleanup(func() { _ = client.Close() })
	bucket := depthBucket(t, client, "depth-scale")

	t.Run("LargeObjectStreamedRead", func(t *testing.T) {
		const size = 32 << 20 // 32 MiB
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i*7 + 3)
		}
		sum := sha256.Sum256(payload)

		obj := bucket.Object("large.bin")
		w := obj.NewWriter(ctx)
		// 8 MiB chunks force several on-wire write messages.
		w.ChunkSize = 8 << 20
		for off := 0; off < len(payload); off += 1 << 20 {
			end := min(off+(1<<20), len(payload))
			_, err := w.Write(payload[off:end])
			require.NoError(t, err)
		}
		require.NoError(t, w.Close())

		attrs, err := obj.Attrs(ctx)
		require.NoError(t, err)
		require.EqualValues(t, size, attrs.Size)

		// Stream the read back in chunks, hashing as we go so the whole object
		// is never held a second time.
		r, err := obj.NewReader(ctx)
		require.NoError(t, err)
		defer r.Close()
		var (
			hasher hash.Hash = sha256.New()
			total            = 0
		)
		buf := make([]byte, 1<<20)
		for {
			n, err := r.Read(buf)
			hasher.Write(buf[:n])
			total += n
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		assert.Equal(t, size, total)
		assert.Equal(t, hex.EncodeToString(sum[:]), hex.EncodeToString(hasher.Sum(nil)))

		// A mid-object range read returns exactly that slice.
		const offset, length = 5 << 20, 1 << 20
		rr, err := obj.NewRangeReader(ctx, offset, length)
		require.NoError(t, err)
		defer rr.Close()
		rangeBytes, err := io.ReadAll(rr)
		require.NoError(t, err)
		assert.Equal(t, payload[offset:offset+length], rangeBytes)
	})

	t.Run("ManyObjectListing", func(t *testing.T) {
		const objects = 50
		want := make(map[string]bool, objects+1)
		want["large.bin"] = true
		for i := 0; i < objects; i++ {
			name := fmt.Sprintf("many-%03d.bin", i)
			putObject(t, bucket.Object(name), []byte("x"))
			want[name] = true
		}
		it := bucket.Objects(ctx, nil)
		got := map[string]bool{}
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			require.NoError(t, err)
			got[attrs.Name] = true
		}
		assert.Equal(t, want, got, "the listing returns every object exactly once")
	})
}
