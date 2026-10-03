package sdk_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
)

// writeObject writes a small object via the SDK writer (multipart upload path).
func writeObject(t *testing.T, client *storage.Client, bucket, name, data string, opts ...func(*storage.Writer)) {
	t.Helper()
	ctx := context.Background()
	w := client.Bucket(bucket).Object(name).NewWriter(ctx)
	for _, o := range opts {
		o(w)
	}
	if _, err := w.Write([]byte(data)); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	require.NoError(t, w.Close())
}

// TestSDKListPrefixDelimiter verifies prefix filtering + delimiter grouping
// (directory-style listing) through the SDK iterator.
func TestSDKListPrefixDelimiter(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-pd-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	for _, name := range []string{"root.txt", "dir/a.txt", "dir/sub/b.txt"} {
		writeObject(t, client, bucket, name, name)
	}

	// Whole-bucket delimiter listing: items = root.txt, prefixes = dir/.
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Delimiter: "/"})
	var items, prefixes []string
	for {
		attrs, err := it.Next()
		if err != nil {
			break
		}
		if attrs.Prefix != "" {
			prefixes = append(prefixes, attrs.Prefix)
		} else {
			items = append(items, attrs.Name)
		}
	}
	require.Equal(t, []string{"root.txt"}, items)
	require.Equal(t, []string{"dir/"}, prefixes)

	// Prefix "dir/" + delimiter "/": item dir/a.txt, prefix dir/sub/.
	it = client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: "dir/", Delimiter: "/"})
	items, prefixes = nil, nil
	for {
		attrs, err := it.Next()
		if err != nil {
			break
		}
		if attrs.Prefix != "" {
			prefixes = append(prefixes, attrs.Prefix)
		} else {
			items = append(items, attrs.Name)
		}
	}
	require.Equal(t, []string{"dir/a.txt"}, items)
	require.Equal(t, []string{"dir/sub/"}, prefixes)
}

// TestSDKObjectMetadataRoundTrip verifies contentType + custom metadata survive
// a write/read round-trip (they are carried in the multipart metadata part).
func TestSDKObjectMetadataRoundTrip(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-meta-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	writeObject(t, client, bucket, "meta.txt", "payload", func(w *storage.Writer) {
		w.ContentType = "application/x-custom"
		w.Metadata = map[string]string{"env": "test", "owner": "sdk"}
	})

	attrs, err := client.Bucket(bucket).Object("meta.txt").Attrs(ctx)
	require.NoError(t, err)
	require.Equal(t, "application/x-custom", attrs.ContentType)
	require.Equal(t, "test", attrs.Metadata["env"])
	require.Equal(t, "sdk", attrs.Metadata["owner"])
	require.NotEmpty(t, attrs.Generation)
	require.NotEmpty(t, attrs.MD5)
}

// TestSDKDeleteNotFound verifies deleting a missing object surfaces
// storage.ErrObjectNotExist (the SDK's canonical not-found error).
func TestSDKDeleteNotFound(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-del-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	writeObject(t, client, bucket, "gone.txt", "x")
	require.NoError(t, client.Bucket(bucket).Object("gone.txt").Delete(ctx))

	// Second delete must report not-found.
	err := client.Bucket(bucket).Object("gone.txt").Delete(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, storage.ErrObjectNotExist), "expected ErrObjectNotExist, got %v", err)

	// Attrs on the same missing object also maps to ErrObjectNotExist.
	_, err = client.Bucket(bucket).Object("gone.txt").Attrs(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, storage.ErrObjectNotExist))
}

// TestSDKBucketIAMPolicy exercises getIamPolicy/setIamPolicy through the SDK's
// bucket IAM handle (REST-backed).
func TestSDKBucketIAMPolicy(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-iam-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	h := client.Bucket(bucket).IAM()
	pol, err := h.Policy(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, pol.Roles(), "default policy should carry legacy bindings")

	// Replace with a single viewer binding and read it back.
	newPol := &iam.Policy{}
	newPol.Add("allUsers", "roles/storage.objectViewer")
	require.NoError(t, h.SetPolicy(ctx, newPol))
	pol, err = h.Policy(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"allUsers"}, pol.Members("roles/storage.objectViewer"))
}

// TestSDKBucketACL verifies bucketAccessControls listing through the SDK.
func TestSDKBucketACL(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-acl-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	acls, err := client.Bucket(bucket).ACL().List(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, acls, "default bucket ACL should include project owner/editor/viewer")
}

// TestSDKObjectNameSpaceAndSlash verifies objects whose names contain spaces
// and path segments are addressable and round-trip through the SDK (the client
// percent-encodes the name on the wire).
func TestSDKObjectNameSpaceAndSlash(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-name-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	name := "folder with space/sub dir/file name.txt"
	writeObject(t, client, bucket, name, "hello spaced path")

	r, err := client.Bucket(bucket).Object(name).NewReader(ctx)
	require.NoError(t, err)
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	r.Close()
	require.Equal(t, "hello spaced path", string(b))

	// The object is discoverable by listing (name round-trips exactly).
	it := client.Bucket(bucket).Objects(ctx, nil)
	found := false
	for {
		attrs, err := it.Next()
		if err != nil {
			break
		}
		if attrs.Name == name {
			found = true
		}
	}
	require.True(t, found, "object %q not found in listing", name)
}

// signedURLOpts builds a V4 path-style signed URL option set against the
// emulator host, using a throwaway SignBytes callback (the emulator does not
// verify signatures).
func signedURLOpts(method, contentType string, expires time.Time) *storage.SignedURLOptions {
	host := strings.TrimPrefix(emulatorHost(), "http://")
	host = strings.TrimPrefix(host, "https://")
	return &storage.SignedURLOptions{
		GoogleAccessID: "test@test.iam.gserviceaccount.com",
		SignBytes:      func([]byte) ([]byte, error) { return []byte("fake-signature"), nil },
		Method:         method,
		ContentType:    contentType,
		Expires:        expires,
		Scheme:         storage.SigningSchemeV4,
		Style:          storage.PathStyle(),
		Insecure:       true,
		Hostname:       host,
	}
}

// TestSDKSignedURLRoundTrip exercises V4 signed GET and PUT through the SDK's
// own URL builder (produces the exact X-Goog-* query shape).
func TestSDKSignedURLRoundTrip(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-signed-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	writeObject(t, client, bucket, "o.txt", "signed via sdk")

	// Signed GET -> 200 + object bytes.
	getURL, err := client.Bucket(bucket).SignedURL("o.txt", signedURLOpts("GET", "", time.Now().Add(time.Hour)))
	require.NoError(t, err)
	resp, err := http.Get(getURL)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "signed via sdk", string(body))

	// Signed PUT -> 200, object stored.
	putURL, err := client.Bucket(bucket).SignedURL("put.txt", signedURLOpts("PUT", "text/plain", time.Now().Add(time.Hour)))
	require.NoError(t, err)
	req, err := http.NewRequest("PUT", putURL, strings.NewReader("put bytes"))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "text/plain")
	putResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	putResp.Body.Close()
	require.Equal(t, http.StatusOK, putResp.StatusCode)

	// Tampered signature is accepted (records the deliberate auth bypass).
	u, err := url.Parse(getURL)
	require.NoError(t, err)
	q := u.Query()
	q.Set("X-Goog-Signature", "deadbeef-forged")
	u.RawQuery = q.Encode()
	resp, err = http.Get(u.String())
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
