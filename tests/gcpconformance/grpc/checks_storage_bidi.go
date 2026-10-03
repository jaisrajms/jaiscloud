package grpcconformance

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
)

// newBidiStorageClient builds the official gRPC client with the experimental
// bidi-read option, which routes both Reader downloads and
// MultiRangeDownloader through google.storage.v2.Storage/BidiReadObject.
func newBidiStorageClient(ctx context.Context, cfg Config) (*storage.Client, error) {
	return newStorageClient(ctx, cfg, experimental.WithGRPCBidiReads())
}

// checkStorageBidiReadObject drives BidiReadObject through the official Go
// client: MultiRangeDownloader sends several independent ranges (and, with
// MinConnections=2, opens a second stream with the read handle minted by the
// first), then the single-range Reader exercises the same RPC via Read. The
// probe asserts byte-exact ranges and a minted read handle.
func checkStorageBidiReadObject(ctx context.Context, cfg Config) error {
	client, err := newBidiStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	object := cfg.ResourceName("gcpc-grpc-bidi-read")
	// 0-9 then a-z: offset 0 gives "01234", offset 25 gives "p", offset 10
	// reads to EOF, and the Reader subrange at 5/4 gives "5678".
	payload := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	if _, err := putStorageObject(ctx, client, bucket, object, "text/plain", payload); err != nil {
		return err
	}

	// Multiple ranges on one bidirectional stream. MinConnections=2 encourages
	// the downloader to grow to a second stream opened with the read handle
	// returned by the first; the explicit ReadHandle path below proves the
	// handle flow deterministically.
	mrd, err := client.Bucket(bucket).Object(object).NewMultiRangeDownloader(ctx, storage.WithMinConnections(2))
	if err != nil {
		return fmt.Errorf("NewMultiRangeDownloader: %w", err)
	}
	defer mrd.Close()

	type rangeResult struct {
		buf []byte
		err error
	}
	results := make(chan rangeResult, 3)
	add := func(offset, length int64) {
		var buf bytes.Buffer
		mrd.Add(&buf, offset, length, func(_, _ int64, err error) {
			results <- rangeResult{buf: buf.Bytes(), err: err}
		})
	}
	add(0, 5)  // "01234"
	add(25, 1) // "p"
	add(10, 0) // "abcdefghijklmnopqrstuvwxyz" (read to end)
	mrd.Wait()
	close(results)

	got := map[string]bool{}
	for r := range results {
		if r.err != nil {
			return fmt.Errorf("MultiRangeDownloader range: %w", r.err)
		}
		got[string(r.buf)] = true
	}
	for _, want := range []string{"01234", "p", "abcdefghijklmnopqrstuvwxyz"} {
		if !got[want] {
			return fmt.Errorf("MultiRangeDownloader results %v missing %q", got, want)
		}
	}
	if h := mrd.GetHandle(); len(h) == 0 {
		return fmt.Errorf("BidiReadObject did not mint a read handle")
	}

	// The single-range Reader (Reader.Read over BidiReadObject).
	r, err := client.Bucket(bucket).Object(object).NewRangeReader(ctx, 5, 4)
	if err != nil {
		return fmt.Errorf("NewRangeReader: %w", err)
	}
	defer r.Close()
	sub, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("NewRangeReader read: %w", err)
	}
	if string(sub) != "5678" {
		return fmt.Errorf("BidiReadObject range = %q, want 5678", sub)
	}

	// Read the whole object, capture its read handle, then re-open the object
	// through the public ObjectHandle.ReadHandle API with only that handle.
	full, err := client.Bucket(bucket).Object(object).NewRangeReader(ctx, 0, -1)
	if err != nil {
		return fmt.Errorf("NewRangeReader(full): %w", err)
	}
	fullData, err := io.ReadAll(full)
	if err != nil {
		full.Close()
		return fmt.Errorf("NewRangeReader(full) read: %w", err)
	}
	handle := full.ReadHandle()
	full.Close()
	if len(handle) == 0 {
		return fmt.Errorf("Reader.ReadHandle returned an empty handle")
	}
	if !bytes.Equal(fullData, payload) {
		return fmt.Errorf("BidiReadObject full read = %q, want %q", fullData, payload)
	}
	handleReader, err := client.Bucket(bucket).Object(object).ReadHandle(handle).NewRangeReader(ctx, 0, -1)
	if err != nil {
		return fmt.Errorf("NewRangeReader(read handle): %w", err)
	}
	defer handleReader.Close()
	handleData, err := io.ReadAll(handleReader)
	if err != nil {
		return fmt.Errorf("NewRangeReader(read handle) read: %w", err)
	}
	if !bytes.Equal(handleData, payload) {
		return fmt.Errorf("read-handle stream = %q, want %q", handleData, payload)
	}

	// A negative-offset read through the handle needs the metadata the server
	// sends alongside the handle: with no metadata the SDK sizes the object as
	// 0 and a negative-offset reader terminates immediately with no bytes.
	negReader, err := client.Bucket(bucket).Object(object).ReadHandle(handle).NewRangeReader(ctx, -5, -1)
	if err != nil {
		return fmt.Errorf("NewRangeReader(read handle, negative offset): %w", err)
	}
	defer negReader.Close()
	negData, err := io.ReadAll(negReader)
	if err != nil {
		return fmt.Errorf("negative-offset handle read: %w", err)
	}
	if string(negData) != "vwxyz" {
		return fmt.Errorf("negative-offset handle read = %q, want vwxyz", negData)
	}
	return nil
}
