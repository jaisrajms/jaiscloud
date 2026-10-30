// Package blobfs provides the BlobFetcher interface for URI-addressed
// read-only access to blob storage.
//
// Current implementations:
//   - S3BlobFetcher: handles s3:// and s3a:// URIs via the local S3 store.
//
// Future implementations (not yet provided):
//   - AzureBlobFetcher: would handle abfss:// URIs via Azure Data Lake Storage.
//   - GCSBlobFetcher: would handle gs:// URIs via Google Cloud Storage.
//
// To add support for a new URI scheme, implement BlobFetcher and either:
//
//	(a) register it in a scheme-keyed map at construction time, or
//	(b) pass it directly to the consumer (PodTemplateLoader, bootstrap.Resolve).
//
// The interface is deliberately small to keep implementations thin.
package blobfs

import (
	"context"
	"fmt"
	"strings"
)

// BlobFetcher fetches raw bytes from a URI-addressed object store.
// Only s3:// and s3a:// URIs are currently supported.
type BlobFetcher interface {
	Fetch(ctx context.Context, uri string) ([]byte, error)
}

// S3BlobFetcher implements BlobFetcher using a BlobStore (bucket="s3").
type S3BlobFetcher struct {
	store BlobStore
}

// NewS3BlobFetcher returns a BlobFetcher backed by store.
// The bucket and key are parsed from each s3:// or s3a:// URI at Fetch time.
func NewS3BlobFetcher(store BlobStore) *S3BlobFetcher {
	return &S3BlobFetcher{store: store}
}

// Fetch retrieves the object at uri. Accepts s3:// and s3a:// schemes.
func (f *S3BlobFetcher) Fetch(ctx context.Context, uri string) ([]byte, error) {
	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return nil, err
	}
	data, err := f.store.Get(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("blobfetch %s: %w", uri, err)
	}
	return data, nil
}

// ParseS3URI parses an s3:// or s3a:// URI into (bucket, key).
func ParseS3URI(uri string) (bucket, key string, err error) { return parseS3URI(uri) }

// parseS3URI parses an s3:// or s3a:// URI into (bucket, key).
func parseS3URI(uri string) (bucket, key string, err error) {
	var rest string
	switch {
	case strings.HasPrefix(uri, "s3a://"):
		rest = uri[len("s3a://"):]
	case strings.HasPrefix(uri, "s3://"):
		rest = uri[len("s3://"):]
	default:
		scheme := uri
		if idx := strings.Index(uri, "://"); idx >= 0 {
			scheme = uri[:idx]
		}
		return "", "", fmt.Errorf("blobfetch: unsupported URI scheme %q (want s3:// or s3a://)", scheme)
	}

	idx := strings.IndexByte(rest, '/')
	if idx < 0 {
		// URI has bucket but no key (e.g. s3://mybucket)
		bucket = rest
		if bucket == "" {
			return "", "", fmt.Errorf("blobfetch: empty bucket in URI %q", uri)
		}
		return bucket, "", fmt.Errorf("blobfetch: missing key in URI %q", uri)
	}
	bucket = rest[:idx]
	key = rest[idx+1:]
	if bucket == "" {
		return "", "", fmt.Errorf("blobfetch: empty bucket in URI %q", uri)
	}
	if key == "" {
		return "", "", fmt.Errorf("blobfetch: empty key in URI %q", uri)
	}
	return bucket, key, nil
}
