package gcp_test

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGCSMultipartUpload uploads an object via multipart/related (JSON metadata
// part + media part) — the mechanism the GCS SDK uses for small objects.
func TestGCSMultipartUpload(t *testing.T) {
	resetState(t)
	createBucket(t, "mp-bucket")

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	require.NoError(t, mw.SetBoundary("jaiscloud_boundary"))

	meta, err := mw.CreatePart(map[string][]string{"Content-Type": {"application/json"}})
	require.NoError(t, err)
	_, err = meta.Write([]byte(`{"name":"greeting.txt","contentType":"text/plain"}`))
	require.NoError(t, err)

	media, err := mw.CreatePart(map[string][]string{"Content-Type": {"text/plain"}})
	require.NoError(t, err)
	_, err = media.Write([]byte("hello multipart"))
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	resp, body := do(t, "POST", "/upload/storage/v1/b/mp-bucket/o?uploadType=multipart", buf.Bytes(),
		map[string]string{"Content-Type": mw.FormDataContentType()})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	obj := jsonMap(t, body)
	require.Equal(t, "greeting.txt", obj["name"])
	require.Equal(t, "15", obj["size"])

	_, body = do(t, "GET", "/storage/v1/b/mp-bucket/o/greeting.txt?alt=media", nil, nil)
	require.Equal(t, "hello multipart", string(body))
}

// TestGCSObjectUpdateVsPatch verifies wire-level PUT (strict replace) vs
// PATCH (merge) semantics for object metadata.
func TestGCSObjectUpdateVsPatch(t *testing.T) {
	resetState(t)
	createBucket(t, "up-bucket")

	resp, _ := do(t, "POST", "/upload/storage/v1/b/up-bucket/o?uploadType=multipart", nil, nil)
	_ = resp // (unused; upload below carries the metadata)

	// Insert via multipart with contentType + metadata.
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	meta, _ := mw.CreatePart(map[string][]string{"Content-Type": {"application/json"}})
	meta.Write([]byte(`{"name":"o.txt","contentType":"text/plain","metadata":{"keep":"yes"}}`))
	media, _ := mw.CreatePart(map[string][]string{"Content-Type": {"text/plain"}})
	media.Write([]byte("data"))
	require.NoError(t, mw.Close())
	resp, body := do(t, "POST", "/upload/storage/v1/b/up-bucket/o?uploadType=multipart", buf.Bytes(),
		map[string]string{"Content-Type": mw.FormDataContentType()})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/plain", jsonMap(t, body)["contentType"])

	// PATCH with only contentType — metadata must be preserved.
	resp, body = do(t, "PATCH", "/storage/v1/b/up-bucket/o/o.txt",
		[]byte(`{"contentType":"application/json"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	obj := jsonMap(t, body)
	require.Equal(t, "application/json", obj["contentType"])
	require.Equal(t, "yes", obj["metadata"].(map[string]any)["keep"])

	// PUT with only contentType — metadata must be cleared (strict replace).
	resp, body = do(t, "PUT", "/storage/v1/b/up-bucket/o/o.txt",
		[]byte(`{"contentType":"image/png"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	obj = jsonMap(t, body)
	require.Equal(t, "image/png", obj["contentType"])
	require.Nil(t, obj["metadata"])
}

// TestGCSVersionedMetadataUpdateKeepsGenerations verifies that patching an
// object's metadata in a versioned bucket updates the live generation in place:
// the generation is unchanged and every noncurrent generation survives, as in
// real GCS (generation is immutable; only metageneration increments).
func TestGCSVersionedMetadataUpdateKeepsGenerations(t *testing.T) {
	resetState(t)
	createBucket(t, "vmeta-bucket")

	resp, _ := do(t, "PATCH", "/storage/v1/b/vmeta-bucket",
		[]byte(`{"versioning":{"enabled":true}}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	upload := func(data string) string {
		resp, body := do(t, "POST", "/upload/storage/v1/b/vmeta-bucket/o?uploadType=media&name=v.txt",
			[]byte(data), map[string]string{"Content-Type": "text/plain"})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		gen, _ := jsonMap(t, body)["generation"].(string)
		require.NotEmpty(t, gen)
		return gen
	}
	gen1 := upload("one")
	gen2 := upload("two")
	require.NotEqual(t, gen1, gen2)

	// PATCH the live generation's metadata; the generation must not change.
	resp, body := do(t, "PATCH", "/storage/v1/b/vmeta-bucket/o/v.txt",
		[]byte(`{"contentType":"application/json"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	patched := jsonMap(t, body)
	require.Equal(t, gen2, patched["generation"], "metadata patch must not change the generation")
	require.Equal(t, "application/json", patched["contentType"])

	// Both generations survive in a versions listing.
	resp, body = do(t, "GET", "/storage/v1/b/vmeta-bucket/o?versions=true", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	items, _ := jsonMap(t, body)["items"].([]any)
	require.Len(t, items, 2, "metadata patch must preserve the noncurrent generation")

	// The noncurrent generation is unchanged and still addressable by id.
	resp, body = do(t, "GET", "/storage/v1/b/vmeta-bucket/o/v.txt?generation="+gen1, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/plain", jsonMap(t, body)["contentType"])
}

// TestGCSObjectIAM exercises objects.getIamPolicy/setIamPolicy over the wire.
func TestGCSObjectIAM(t *testing.T) {
	resetState(t)
	createBucket(t, "oiam-bucket")

	resp, _ := do(t, "POST", "/upload/storage/v1/b/oiam-bucket/o?uploadType=media&name=o.txt",
		[]byte("x"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, body := do(t, "GET", "/storage/v1/b/oiam-bucket/o/o.txt/iam", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	pol := jsonMap(t, body)
	require.Equal(t, "projects/_/buckets/oiam-bucket/objects/o.txt", pol["resourceId"])
	require.NotEmpty(t, pol["bindings"])

	resp, _ = do(t, "PUT", "/storage/v1/b/oiam-bucket/o/o.txt/iam",
		[]byte(`{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}]}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, body = do(t, "GET", "/storage/v1/b/oiam-bucket/o/o.txt/iam", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	bindings, _ := jsonMap(t, body)["bindings"].([]any)
	require.Len(t, bindings, 1)
}

// TestGCSSetIamPolicyEtagOCC verifies wire-level optimistic concurrency
// control: a setIamPolicy carrying a stale etag is rejected with 409.
func TestGCSSetIamPolicyEtagOCC(t *testing.T) {
	resetState(t)
	createBucket(t, "occ-bucket")

	resp, body := do(t, "GET", "/storage/v1/b/occ-bucket/iam", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	currentEtag, _ := jsonMap(t, body)["etag"].(string)
	require.NotEmpty(t, currentEtag)

	// Mismatched etag → 409 conflict envelope.
	resp, _ = do(t, "PUT", "/storage/v1/b/occ-bucket/iam",
		[]byte(`{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}],"etag":"BOGUS="}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusConflict, resp.StatusCode)

	// Matching etag → 200, and the policy is stored.
	resp, body = do(t, "PUT", "/storage/v1/b/occ-bucket/iam",
		[]byte(`{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}],"etag":"`+currentEtag+`"}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	bindings, _ := jsonMap(t, body)["bindings"].([]any)
	require.Len(t, bindings, 1)
}

// TestGCSObjectIAMNotFound verifies object IAM endpoints 404 for a missing
// object.
func TestGCSObjectIAMNotFound(t *testing.T) {
	resetState(t)
	createBucket(t, "oiam404-bucket")

	resp, _ := do(t, "GET", "/storage/v1/b/oiam404-bucket/o/missing.txt/iam", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestGCSDelimiterPagination verifies delimiter listing composes with
// maxResults/pageToken over the wire.
func TestGCSDelimiterPagination(t *testing.T) {
	resetState(t)
	createBucket(t, "dp-bucket")

	for _, name := range []string{"a.txt", "b.txt", "dir/c.txt", "dir/sub/d.txt"} {
		resp, _ := do(t, "POST", "/upload/storage/v1/b/dp-bucket/o?uploadType=media&name="+name,
			[]byte(name), map[string]string{"Content-Type": "text/plain"})
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// Combined result set: a.txt, b.txt, dir/ → page size 2.
	resp, body := do(t, "GET", "/storage/v1/b/dp-bucket/o?delimiter=/&maxResults=2", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	page := jsonMap(t, body)
	items, _ := page["items"].([]any)
	require.Len(t, items, 2)
	token, _ := page["nextPageToken"].(string)
	require.NotEmpty(t, token)

	// Page 2: the dir/ prefix, no further token.
	_, body = do(t, "GET", "/storage/v1/b/dp-bucket/o?delimiter=/&maxResults=2&pageToken="+token, nil, nil)
	page = jsonMap(t, body)
	items, _ = page["items"].([]any)
	prefixes, _ := page["prefixes"].([]any)
	require.Len(t, items, 0)
	require.Len(t, prefixes, 1)
	require.Equal(t, "dir/", prefixes[0])
	_, hasNext := page["nextPageToken"]
	require.False(t, hasNext)
}

// TestGCSObjectACL exercises objectAccessControls list + insert over the wire.
func TestGCSObjectACL(t *testing.T) {
	resetState(t)
	createBucket(t, "oacl-bucket")

	resp, _ := do(t, "POST", "/upload/storage/v1/b/oacl-bucket/o?uploadType=media&name=o.txt",
		[]byte("x"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, body := do(t, "GET", "/storage/v1/b/oacl-bucket/o/o.txt/acl", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotEmpty(t, jsonMap(t, body)["items"])

	resp, _ = do(t, "POST", "/storage/v1/b/oacl-bucket/o/o.txt/acl",
		[]byte(`{"entity":"allUsers","role":"READER"}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, body = do(t, "GET", "/storage/v1/b/oacl-bucket/o/o.txt/acl", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	items, _ := jsonMap(t, body)["items"].([]any)
	found := false
	for _, it := range items {
		if it.(map[string]any)["entity"] == "allUsers" {
			found = true
		}
	}
	require.True(t, found, "expected allUsers object ACL entry")
}

// TestGCSDelimiter verifies directory-style listing: objects beyond a delimiter
// are collapsed into common prefixes while leaf objects are returned as items.
func TestGCSDelimiter(t *testing.T) {
	resetState(t)
	createBucket(t, "delim-bucket")

	for _, name := range []string{"root.txt", "dir/a.txt", "dir/sub/b.txt"} {
		resp, _ := do(t, "POST", "/upload/storage/v1/b/delim-bucket/o?uploadType=media&name="+name,
			[]byte(name), map[string]string{"Content-Type": "text/plain"})
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	resp, body := do(t, "GET", "/storage/v1/b/delim-bucket/o?delimiter=/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	page := jsonMap(t, body)

	items, _ := page["items"].([]any)
	require.Len(t, items, 1)
	require.Equal(t, "root.txt", items[0].(map[string]any)["name"])

	prefixes, _ := page["prefixes"].([]any)
	require.Len(t, prefixes, 1)
	require.Equal(t, "dir/", prefixes[0])
}

// TestGCSObjectNameWithSlash verifies objects whose names contain slashes are
// addressable via the JSON API, where the name is percent-encoded as %2F.
func TestGCSObjectNameWithSlash(t *testing.T) {
	resetState(t)
	createBucket(t, "slash-bucket")

	resp, _ := do(t, "POST", "/upload/storage/v1/b/slash-bucket/o?uploadType=media&name=dir%2Fsub%2Ffile.txt",
		[]byte("nested"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Fetch via the JSON API with the %2F-encoded name.
	resp, body := do(t, "GET", "/storage/v1/b/slash-bucket/o/dir%2Fsub%2Ffile.txt", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "dir/sub/file.txt", jsonMap(t, body)["name"])

	// Media download round-trips the bytes.
	_, body = do(t, "GET", "/storage/v1/b/slash-bucket/o/dir%2Fsub%2Ffile.txt?alt=media", nil, nil)
	require.Equal(t, "nested", string(body))
}

// TestGCSConcurrentUploads uploads many objects in parallel and verifies all
// are stored and listed (no lost updates or duplicate generations).
func TestGCSConcurrentUploads(t *testing.T) {
	resetState(t)
	createBucket(t, "conc-bucket")

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("obj-%03d.txt", i)
			resp, body := do(t, "POST", "/upload/storage/v1/b/conc-bucket/o?uploadType=media&name="+name,
				[]byte(fmt.Sprintf("data-%d", i)), map[string]string{"Content-Type": "text/plain"})
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("%s: HTTP %d: %s", name, resp.StatusCode, body)
				return
			}
			errs <- nil
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	resp, body := do(t, "GET", "/storage/v1/b/conc-bucket/o?maxResults=1000", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	items, _ := jsonMap(t, body)["items"].([]any)
	require.Len(t, items, n)

	// Generations must be unique across all objects.
	gens := make(map[string]bool, n)
	for _, it := range items {
		gen, _ := it.(map[string]any)["generation"].(string)
		require.NotEmpty(t, gen)
		require.False(t, gens[gen], "duplicate generation %q", gen)
		gens[gen] = true
	}
}

// TestGCSObjectHoldsAndGenerationDelete verifies the wire contract for object
// holds (bucket default inheritance, delete blocked with a message naming the
// hold) and objects.delete?generation= (only the requested revision is removed;
// removing the live revision does not promote a survivor).
func TestGCSObjectHoldsAndGenerationDelete(t *testing.T) {
	resetState(t)

	// A bucket with defaultEventBasedHold: a new object inherits the hold.
	resp, body := do(t, "POST", "/storage/v1/b?project=proj",
		[]byte(`{"name":"hold-bucket","defaultEventBasedHold":true}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, true, jsonMap(t, body)["defaultEventBasedHold"])

	resp, body = do(t, "POST", "/upload/storage/v1/b/hold-bucket/o?uploadType=media&name=h.txt",
		[]byte("held"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, true, jsonMap(t, body)["eventBasedHold"])

	// Deleting the held object is forbidden with a message naming the hold.
	resp, body = do(t, "DELETE", "/storage/v1/b/hold-bucket/o/h.txt", nil, nil)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.Contains(t, string(body), "event-based hold")

	// Releasing the hold (PATCH) allows the delete.
	resp, _ = do(t, "PATCH", "/storage/v1/b/hold-bucket/o/h.txt",
		[]byte(`{"eventBasedHold":false}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp, _ = do(t, "DELETE", "/storage/v1/b/hold-bucket/o/h.txt", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Versioned bucket: delete-by-generation removes only that revision.
	createBucket(t, "ver-bucket")
	resp, _ = do(t, "PATCH", "/storage/v1/b/ver-bucket",
		[]byte(`{"versioning":{"enabled":true}}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, body = do(t, "POST", "/upload/storage/v1/b/ver-bucket/o?uploadType=media&name=v.txt",
		[]byte("one"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	gen1, _ := jsonMap(t, body)["generation"].(string)
	resp, body = do(t, "POST", "/upload/storage/v1/b/ver-bucket/o?uploadType=media&name=v.txt",
		[]byte("two"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	gen2, _ := jsonMap(t, body)["generation"].(string)
	require.NotEqual(t, gen1, gen2)

	// Deleting the noncurrent v1 leaves the live v2 in place.
	resp, _ = do(t, "DELETE", "/storage/v1/b/ver-bucket/o/v.txt?generation="+gen1, nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	resp, body = do(t, "GET", "/storage/v1/b/ver-bucket/o/v.txt", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, gen2, jsonMap(t, body)["generation"])

	// Deleting the live v2 by generation removes the object (no promotion).
	resp, _ = do(t, "DELETE", "/storage/v1/b/ver-bucket/o/v.txt?generation="+gen2, nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	resp, _ = do(t, "GET", "/storage/v1/b/ver-bucket/o/v.txt", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// A generation that does not exist is NotFound.
	resp, _ = do(t, "DELETE", "/storage/v1/b/ver-bucket/o/v.txt?generation=999999", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
