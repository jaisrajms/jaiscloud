package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// publishedEventType reports whether the fake publisher recorded an event whose
// (reserved) eventType attribute equals want.
func publishedEventType(events []publishedEvent, want string) bool {
	for _, e := range events {
		if e.attrs["eventType"] == want {
			return true
		}
	}
	return false
}

// TestCSEKMetadataWithholdsChecksumsWithoutKey covers J37: a CSEK object's
// md5Hash/crc32c are encrypted with the customer key and withheld from metadata
// reads until the matching key is supplied.
func TestCSEKMetadataWithholdsChecksumsWithoutKey(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	key := []byte("0123456789abcdef0123456789abcdef")
	keyB64, shaB64 := csekMaterial(key)
	newCSEKBucket(t, p, keyB64, shaB64)

	// Metadata without the key: customerEncryption is described, hashes withheld.
	nr := bucketParamsWithObj("bkt", "csek.txt")
	get, err := p.ObjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get metadata without key: %v", err)
	}
	if get.Data["customerEncryption"] == nil {
		t.Fatalf("expected customerEncryption, got %#v", get.Data)
	}
	if v, present := get.Data["md5Hash"]; present && v != "" {
		t.Errorf("md5Hash should be withheld without the key, got %v", v)
	}
	if v, present := get.Data["crc32c"]; present && v != "" {
		t.Errorf("crc32c should be withheld without the key, got %v", v)
	}

	// Metadata with the matching key: hashes are returned.
	nr = bucketParamsWithObj("bkt", "csek.txt")
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = shaB64
	get, err = p.ObjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get metadata with key: %v", err)
	}
	if v, _ := get.Data["md5Hash"].(string); v == "" {
		t.Errorf("md5Hash should be present with the key, got %#v", get.Data)
	}
	if v, _ := get.Data["crc32c"].(string); v == "" {
		t.Errorf("crc32c should be present with the key, got %#v", get.Data)
	}
}

// TestCSEKListWithholdsChecksumsWithoutKey covers the list half of J37.
func TestCSEKListWithholdsChecksumsWithoutKey(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))
	newCSEKBucket(t, p, keyB64, shaB64)

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	list, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	item, _ := items[0].(map[string]any)
	if v, present := item["md5Hash"]; present && v != "" {
		t.Errorf("list md5Hash should be withheld without the key, got %v", v)
	}
	if v, present := item["crc32c"]; present && v != "" {
		t.Errorf("list crc32c should be withheld without the key, got %v", v)
	}
}

// TestPatchTargetGenerationUpdatesNoncurrent covers J57: objects.patch with
// ?generation= updates the selected (noncurrent) revision in place, leaving the
// live generation untouched.
func TestPatchTargetGenerationUpdatesNoncurrent(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "ver-gen", map[string]any{"versioning": map[string]any{"enabled": true}})

	v1 := putObject(t, p, "ver-gen", "obj.txt", nil)
	v2 := putObject(t, p, "ver-gen", "obj.txt", nil)
	gen1, _ := v1.Data["generation"].(string)
	gen2, _ := v2.Data["generation"].(string)

	nr := bucketParamsWithObj("ver-gen", "obj.txt")
	nr.Params["generation"] = gen1
	nr.Params["body"] = map[string]any{"contentType": "application/json"}
	patched, err := p.ObjectsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch target generation: %v", err)
	}
	if patched.Data["generation"] != gen1 {
		t.Fatalf("patched generation = %v, want %v", patched.Data["generation"], gen1)
	}
	if patched.Data["contentType"] != "application/json" {
		t.Fatalf("patched contentType = %v, want application/json", patched.Data["contentType"])
	}
	if patched.Data["metageneration"] != "2" {
		t.Fatalf("patched metageneration = %v, want 2", patched.Data["metageneration"])
	}

	// The selected revision now carries the new metadata...
	old := getByGeneration(t, p, "ver-gen", "obj.txt", gen1)
	if old.Data["contentType"] != "application/json" {
		t.Errorf("target generation contentType = %v, want application/json", old.Data["contentType"])
	}
	// ...and the live generation is untouched.
	live := getByGeneration(t, p, "ver-gen", "obj.txt", gen2)
	if live.Data["contentType"] != "text/plain" || live.Data["metageneration"] != "1" {
		t.Errorf("live generation mutated: contentType=%v metageneration=%v",
			live.Data["contentType"], live.Data["metageneration"])
	}
}

// TestUpdateTargetGeneration covers the strict-replace (PUT) half of J57.
func TestUpdateTargetGeneration(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "ver-gen-upd", map[string]any{"versioning": map[string]any{"enabled": true}})

	v1 := putObject(t, p, "ver-gen-upd", "obj.txt", nil)
	v2 := putObject(t, p, "ver-gen-upd", "obj.txt", nil)
	gen1, _ := v1.Data["generation"].(string)
	gen2, _ := v2.Data["generation"].(string)

	nr := bucketParamsWithObj("ver-gen-upd", "obj.txt")
	nr.Params["generation"] = gen1
	nr.Params["body"] = map[string]any{"contentType": "text/csv"}
	updated, err := p.ObjectsUpdate(ctx, nr)
	if err != nil {
		t.Fatalf("update target generation: %v", err)
	}
	if updated.Data["generation"] != gen1 || updated.Data["contentType"] != "text/csv" {
		t.Fatalf("update = gen %v contentType %v, want %v/text/csv",
			updated.Data["generation"], updated.Data["contentType"], gen1)
	}
	if live := getByGeneration(t, p, "ver-gen-upd", "obj.txt", gen2); live.Data["contentType"] != "text/plain" {
		t.Errorf("live generation mutated: contentType=%v", live.Data["contentType"])
	}
}

// TestXMLMediaHeadersContentEncodingAndComponentCount covers J69: the XML API
// media response carries x-goog-stored-content-encoding and, for a composite
// object, x-goog-component-count.
func TestXMLMediaHeadersContentEncodingAndComponentCount(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "xmlhdr")

	nr := bucketParams()
	nr.Params["bucket"] = "xmlhdr"
	nr.Params["object"] = "gzip.txt"
	nr.Params["contentEncoding"] = "gzip"
	nr.Params[wire.MediaKey] = []byte("compressed-bytes")
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "xmlhdr"
	nr.Params["object"] = "gzip.txt"
	nr.Params[wire.XMLAPIKey] = true
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	hdr, _ := media.Data[wire.HeadersKey].(map[string]string)
	if hdr["x-goog-stored-content-encoding"] != "gzip" {
		t.Errorf("x-goog-stored-content-encoding = %q, want gzip", hdr["x-goog-stored-content-encoding"])
	}

	// A composite object reports its component count on the XML API.
	for _, name := range []string{"a.txt", "b.txt"} {
		nr = bucketParams()
		nr.Params["bucket"] = "xmlhdr"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(name)
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}
	nr = bucketParams()
	nr.Params["bucket"] = "xmlhdr"
	nr.Params["object"] = "ab.txt"
	nr.Params["body"] = map[string]any{"sourceObjects": []any{
		map[string]any{"name": "a.txt"},
		map[string]any{"name": "b.txt"},
	}}
	if _, err := p.ObjectsCompose(ctx, nr); err != nil {
		t.Fatalf("compose: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "xmlhdr"
	nr.Params["object"] = "ab.txt"
	nr.Params[wire.XMLAPIKey] = true
	media, err = p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get composite media: %v", err)
	}
	hdr, _ = media.Data[wire.HeadersKey].(map[string]string)
	if hdr["x-goog-component-count"] != "2" {
		t.Errorf("x-goog-component-count = %q, want 2", hdr["x-goog-component-count"])
	}
}

// TestXMLMediaExpirationHeader covers the lifecycle-derived x-goog-expiration
// header (J69): it is present only when an age-based Delete rule applies.
func TestXMLMediaExpirationHeader(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "xml-exp", map[string]any{
		"lifecycle": map[string]any{
			"rule": []any{map[string]any{
				"action":    map[string]any{"type": "Delete"},
				"condition": map[string]any{"age": float64(30)},
			}},
		},
	})

	nr := bucketParams()
	nr.Params["bucket"] = "xml-exp"
	nr.Params["object"] = "obj.txt"
	nr.Params[wire.MediaKey] = []byte("data")
	ins, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	created, _ := ins.Data["timeCreated"].(string)

	nr = bucketParams()
	nr.Params["bucket"] = "xml-exp"
	nr.Params["object"] = "obj.txt"
	nr.Params[wire.XMLAPIKey] = true
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	hdr, _ := media.Data[wire.HeadersKey].(map[string]string)
	want := ""
	if t0, perr := time.Parse(time.RFC3339Nano, created); perr == nil {
		want = t0.Add(30 * 24 * time.Hour).UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	}
	if hdr["x-goog-expiration"] != want || want == "" {
		t.Errorf("x-goog-expiration = %q, want %q", hdr["x-goog-expiration"], want)
	}
}

// TestXMLMediaMissingBucketError covers the J61 bucket/object distinction: a
// raw media read of a missing bucket reports NoSuchBucket, not NoSuchKey.
func TestXMLMediaMissingBucketError(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["bucket"] = "nope"
	nr.Params["object"] = "obj.txt"
	nr.Params[wire.XMLAPIKey] = true
	_, err := p.ObjectsGetMedia(ctx, nr)
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %v", err)
	}
	if pe.Code != "NotFound" || pe.Data["xmlCode"] != "NoSuchBucket" {
		t.Fatalf("missing bucket error = %s / xmlCode %v, want NotFound/NoSuchBucket", pe.Code, pe.Data["xmlCode"])
	}

	// A missing object in an existing bucket maps to NoSuchKey (default NotFound).
	createBucket(t, p, "bkt")
	_, err = p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "missing.txt"))
	if !errors.As(err, &pe) || pe.Code != "NotFound" {
		t.Fatalf("missing object error = %v, want NotFound", err)
	}
	if xc, _ := pe.Data["xmlCode"].(string); xc != "" {
		t.Fatalf("missing object xmlCode = %q, want empty (adapter defaults to NoSuchKey)", xc)
	}
}

// TestResumableUploadPreservesContentEncoding covers the REST resumable half of
// J69: contentEncoding set on the initiation body survives to the stored object.
func TestResumableUploadPreservesContentEncoding(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "r.txt"
	nr.Params[wire.ContentTypeKey] = "text/plain"
	nr.Params["body"] = map[string]any{"name": "r.txt", "contentEncoding": "gzip"}
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start resumable: %v", err)
	}
	loc, _ := start.Data[wire.LocationKey].(string)
	uploadID := ""
	for _, q := range strings.Split(strings.SplitN(loc, "?", 2)[1], "&") {
		if k, v, ok := strings.Cut(q, "="); ok && k == "upload_id" {
			uploadID = v
		}
	}
	if uploadID == "" {
		t.Fatalf("no upload_id in %q", loc)
	}

	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-2/3"
	nr.Params[wire.MediaKey] = []byte("abc")
	if _, err := p.ObjectsInsertResumable(ctx, nr); err != nil {
		t.Fatalf("final chunk: %v", err)
	}

	get, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "r.txt"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ce, _ := get.Data["contentEncoding"].(string); ce != "gzip" {
		t.Fatalf("contentEncoding = %q, want gzip", ce)
	}
}

// TestNotificationMetadataUpdateEvent covers J31: objects.patch/update publishes
// OBJECT_METADATA_UPDATE.
func TestNotificationMetadataUpdateEvent(t *testing.T) {
	ctx := context.Background()
	p, fp := newNotifProvider()
	createBucket(t, p, "meta-notif")
	addNotification(t, p, "meta-notif", map[string]any{
		"topic":       "projects/proj/topics/events",
		"event_types": []any{"OBJECT_METADATA_UPDATE"},
	})
	insertObject(t, p, "meta-notif", "obj.txt", []byte("v1"))

	nr := bucketParamsWithObj("meta-notif", "obj.txt")
	nr.Params["body"] = map[string]any{"contentType": "application/json"}
	if _, err := p.ObjectsPatch(ctx, nr); err != nil {
		t.Fatalf("patch: %v", err)
	}
	if !publishedEventType(fp.snapshot(), "OBJECT_METADATA_UPDATE") {
		t.Errorf("expected OBJECT_METADATA_UPDATE event, got %+v", fp.snapshot())
	}
}

// TestNotificationFinalizeOnRestore covers the restore half of J31.
func TestNotificationFinalizeOnRestore(t *testing.T) {
	ctx := context.Background()
	p, fp := newNotifProvider()
	createVersionedBucket(t, p, "restore-notif")
	addNotification(t, p, "restore-notif", map[string]any{
		"topic":       "projects/proj/topics/events",
		"event_types": []any{"OBJECT_FINALIZE"},
	})

	nr := notificationParams("restore-notif")
	nr.Params["object"] = "obj.txt"
	nr.Params[wire.MediaKey] = []byte("hello")
	ins, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	gen, _ := ins.Data["generation"].(string)

	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("restore-notif", "obj.txt")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Discard the insert/delete events so only the restore finalize remains.
	fp.mu.Lock()
	fp.calls = nil
	fp.mu.Unlock()

	nr = bucketParamsWithObj("restore-notif", "obj.txt")
	nr.Params["generation"] = gen
	if _, err := p.ObjectsRestore(ctx, nr); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !publishedEventType(fp.snapshot(), "OBJECT_FINALIZE") {
		t.Errorf("expected OBJECT_FINALIZE on restore, got %+v", fp.snapshot())
	}
}
