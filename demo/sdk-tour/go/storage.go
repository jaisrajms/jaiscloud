package main

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type fixtures struct {
	cfg       Config
	bucket    string
	topic     string
	sub       string
	bigBucket string
}

func runAll(r *runner) {
	f := &fixtures{
		cfg:       r.cfg,
		bucket:    rid(r.cfg, "sdk-tour-bucket"),
		topic:     rid(r.cfg, "sdk-tour-topic"),
		sub:       rid(r.cfg, "sdk-tour-sub"),
		bigBucket: rid(r.cfg, "sdk-tour-bq"),
	}
	storageScenarios(r, f)
	pubsubScenarios(r, f)
	firestoreScenarios(r, f)
	loggingScenarios(r, f)
	secretScenarios(r, f)
	kmsScenarios(r, f)
	bigqueryScenarios(r, f)
	iamScenarios(r, f)
	dataprocScenarios(r, f)
}

// newStorageClient uses the STORAGE_EMULATOR_HOST hook (set in main) so the
// official REST client auto-selects anonymous credentials.
func newStorageClient(ctx context.Context) (*storage.Client, error) {
	return storage.NewClient(ctx, option.WithoutAuthentication())
}

func storageScenarios(r *runner, f *fixtures) {
	var client *storage.Client
	get := func() (*storage.Client, error) {
		if client == nil {
			c, err := newStorageClient(context.Background())
			if err != nil {
				return nil, err
			}
			client = c
		}
		return client, nil
	}

	r.run("storage.create_bucket", "OK", func(ctx context.Context) (string, string, error) {
		c, err := get()
		if err != nil {
			return "", "", fmt.Errorf("new storage client: %w", err)
		}
		if err := c.Bucket(f.bucket).Create(ctx, f.cfg.Project, &storage.BucketAttrs{Location: "US"}); err != nil {
			return "", "", fmt.Errorf("create bucket: %w", err)
		}
		attrs, err := c.Bucket(f.bucket).Attrs(ctx)
		if err != nil {
			return "", "", fmt.Errorf("bucket attrs: %w", err)
		}
		return "created", "bucket=" + attrs.Name, nil
	})

	r.run("storage.resumable_upload", "OK", func(ctx context.Context) (string, string, error) {
		c, err := get()
		if err != nil {
			return "", "", err
		}
		payload := resumablePayload()
		w := c.Bucket(f.bucket).Object("resumable/payload.bin").NewWriter(ctx)
		w.ChunkSize = 256 * 1024 // forces the resumable protocol, not a single-shot put
		w.ContentType = "application/octet-stream"
		if _, err := w.Write(payload); err != nil {
			return "", "", fmt.Errorf("resumable write: %w", err)
		}
		if err := w.Close(); err != nil {
			return "", "", fmt.Errorf("resumable close: %w", err)
		}
		attrs, err := c.Bucket(f.bucket).Object("resumable/payload.bin").Attrs(ctx)
		if err != nil {
			return "", "", fmt.Errorf("attrs: %w", err)
		}
		obs := fmt.Sprintf("%d", attrs.Size)
		return obs, fmt.Sprintf("size=%d md5=%s", attrs.Size, attrs.MD5), nil
	})

	r.run("storage.stream_download_checksum", "OK", func(ctx context.Context) (string, string, error) {
		c, err := get()
		if err != nil {
			return "", "", err
		}
		obj := c.Bucket(f.bucket).Object("resumable/payload.bin")
		rd, err := obj.NewReader(ctx)
		if err != nil {
			return "", "", fmt.Errorf("new reader: %w", err)
		}
		defer rd.Close()
		// Small read buffer so the transfer is chunked, not buffered whole.
		var got []byte
		buf := make([]byte, 64*1024)
		for {
			n, err := rd.Read(buf)
			got = append(got, buf[:n]...)
			if err == io.EOF {
				break
			}
			if err != nil {
				return "", "", fmt.Errorf("stream read: %w", err)
			}
		}
		sum := sha256Hex(got)
		want := sha256Hex(resumablePayload())
		if sum != want {
			return "", "", fmt.Errorf("checksum mismatch: got %s want %s (bytes=%d)", sum, want, len(got))
		}
		return sum, fmt.Sprintf("sha256=%s bytes=%d", sum, len(got)), nil
	})

	r.run("storage.list_pagination", "OK", func(ctx context.Context) (string, string, error) {
		c, err := get()
		if err != nil {
			return "", "", err
		}
		for i := 0; i < 7; i++ {
			w := c.Bucket(f.bucket).Object(fmt.Sprintf("page/%02d.txt", i)).NewWriter(ctx)
			if _, err := w.Write([]byte(fmt.Sprintf("page-%d", i))); err != nil {
				return "", "", err
			}
			if err := w.Close(); err != nil {
				return "", "", err
			}
		}
		it := c.Bucket(f.bucket).Objects(ctx, &storage.Query{Prefix: "page/"})
		pager := iterator.NewPager(it, 2, "")
		pages, total := 0, 0
		for {
			var page []*storage.ObjectAttrs
			next, err := pager.NextPage(&page)
			if err != nil {
				return "", "", fmt.Errorf("list page: %w", err)
			}
			pages++
			total += len(page)
			if next == "" {
				break
			}
		}
		if total != 7 {
			return "", "", fmt.Errorf("listed %d objects, want 7", total)
		}
		return fmt.Sprintf("pages=%d,total=%d", pages, total), fmt.Sprintf("pageSize=2 pages=%d total=%d", pages, total), nil
	})
}
