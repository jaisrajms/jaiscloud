package seams

import (
	"context"
	"errors"
	"fmt"
	"io"

	"jaiscloud/internal/gcp/paging"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// Storage is Cloud Storage as seen by the services that read or write objects
// on their own: Cloud Functions and Cloud Run (deployment sources), BigQuery
// (gs:// load sources) and Dataproc (job output). It satisfies
// functions.SourceFetcher and SourceBucketEnsurer, bigquery.SourceReader and
// dataproc.BlobSink.
//
// Object bytes travel in-process exactly as for a REST client (the media param
// in, the _stream answer out), so a plugin can observe or wrap these calls; an
// out-of-process plugin cannot replace a stream (see the SDK limits).
type Storage struct{ s *Set }

// Storage returns the Cloud Storage seam.
func (s *Set) Storage() *Storage { return &Storage{s: s} }

func (st *Storage) project(project string) string {
	if project == "" {
		return st.s.project
	}
	return project
}

func (st *Storage) call(ctx context.Context, action, project string, params map[string]any) (*model.ProviderResponse, error) {
	return st.s.dispatch(ctx, "storage", action, st.project(project), params, nil)
}

func isStatus(err error, code int) bool {
	var pe *model.ProviderError
	return errors.As(err, &pe) && pe.HTTPStatus == code
}

// FetchObjectBytes returns the live bytes of bucket/object, or gcs.ErrNoSuchObject.
func (st *Storage) FetchObjectBytes(ctx context.Context, bucket, object string) ([]byte, error) {
	resp, err := st.call(ctx, "ObjectsGetMedia", "", map[string]any{"bucket": bucket, "object": object})
	if err != nil {
		if isStatus(err, 404) {
			return nil, gcs.ErrNoSuchObject
		}
		return nil, err
	}
	rc, ok := resp.Data["_stream"].(io.ReadCloser)
	if !ok {
		return nil, fmt.Errorf("storage: the answer for %s/%s carries no object bytes", bucket, object)
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// ListObjectNames returns the names of the live objects in bucket that start
// with prefix, in the order the listing gives them (ascending). An absent bucket
// yields no names.
func (st *Storage) ListObjectNames(ctx context.Context, bucket, prefix string) ([]string, error) {
	var names []string
	err := paging.Follow(ctx, func(token string) (string, error) {
		params := map[string]any{"bucket": bucket, "prefix": prefix, "maxResults": "1000"}
		if token != "" {
			params["pageToken"] = token
		}
		resp, err := st.call(ctx, "ObjectsList", "", params)
		if err != nil {
			return "", err
		}
		names = append(names, itemNames(resp.Data["items"])...)
		next, _ := resp.Data["nextPageToken"].(string)
		return next, nil
	})
	switch {
	case err == nil:
		return names, nil
	case isStatus(err, 404):
		return nil, nil
	case errors.Is(err, paging.ErrPageLoop):
		return nil, model.NewProviderError("PluginProtocolError",
			"the listing of "+bucket+" never ends: its next page token repeats", 502)
	}
	return nil, err
}

// itemNames reads "name" from a listing's items, whether the built-in produced
// them as Go maps or a plugin as decoded JSON.
func itemNames(items any) []string {
	var out []string
	add := func(m map[string]any) {
		if n, _ := m["name"].(string); n != "" {
			out = append(out, n)
		}
	}
	switch v := items.(type) {
	case []map[string]any:
		for _, m := range v {
			add(m)
		}
	case []any:
		for _, e := range v {
			if m, ok := e.(map[string]any); ok {
				add(m)
			}
		}
	}
	return out
}

// EnsureBucket creates bucket unless it exists.
func (st *Storage) EnsureBucket(ctx context.Context, project, bucket, location string) error {
	if _, err := st.call(ctx, "BucketsGet", project, map[string]any{"bucket": bucket}); err == nil {
		return nil
	} else if !isStatus(err, 404) {
		return err
	}
	if location == "" {
		location = "US"
	}
	_, err := st.call(ctx, "BucketsInsert", project, map[string]any{
		"project": project,
		"body":    map[string]any{"name": bucket, "location": location},
	})
	if err != nil && !isStatus(err, 409) {
		return err
	}
	return nil
}

// PutObjectBytes writes (or replaces) an object, creating the bucket when absent. It is
// the emulator staging an object for another service, so the write carries
// storage.StagedWriteMeta: a bucket's versioning and retention do not apply, as they
// never did to this helper. A plugin sees it as an ordinary ObjectsInsert (origin internal).
func (st *Storage) PutObjectBytes(ctx context.Context, project, bucket, object, contentType string, data []byte) error {
	if bucket == "" || object == "" {
		return fmt.Errorf("storage: PutObjectBytes requires bucket and object")
	}
	if err := st.EnsureBucket(ctx, project, bucket, ""); err != nil {
		return err
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	_, err := st.s.dispatch(ctx, "storage", "ObjectsInsert", st.project(project), map[string]any{
		"bucket": bucket, "object": object,
		wire.MediaKey: data, wire.ContentTypeKey: contentType,
	}, map[string]string{storageprovider.StagedWriteMeta: "1"})
	return err
}
