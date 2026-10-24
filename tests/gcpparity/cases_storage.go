//go:build gcp_parity

package gcpparity

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// storageScenario compares the Cloud Storage object surface over REST and gRPC
// (AUD3-4). Storage's REST and gRPC adapters share one core (blobfs + the
// object store), but the object resource each transport returns was never
// cross-diffed, and the REST media-upload flow — the reason the service carried
// a parity exemption — was never driven end to end.
//
// The gRPC side is driven with the emulator's own generated stub
// (jaiscloud/internal/gcp/grpc/storage/storagepb), because the official
// cloud.google.com/go/storage module does not export the storage v2 proto and
// returns *storage.ObjectAttrs rather than a proto.Message to diff. The parity
// binary already links the stub (errors_test.go), so no duplicate proto
// registration arises.
//
// Every upload encoding the REST surface serves — media, multipart and
// resumable — is created with the same logical bytes/metadata and diffed
// head-to-head against the gRPC BidiWriteObject (the RPC the official Go client
// uses) on its own twin object. The bucket lifecycle is out of scope here (it
// is scheduled separately, AUD3-15); the scenario only creates one prerequisite
// bucket, undiffed, for the object writes to target.
func storageScenario() Scenario {
	bkt := func(e *Env) string { return e.Cfg.ResourceName("parity-storage") }
	obj := func(e *Env) string { return e.Resource("parity-object") }
	project := func(e *Env) string { return e.Cfg.Project }

	const (
		payload     = "storage-parity-payload"
		contentType = "text/plain"
	)

	createBucket := func(ctx context.Context, e *Env) error {
		body := fmt.Sprintf(`{"name":%q}`, bkt(e))
		_, err := e.Rest(ctx, http.MethodPost, "/storage/v1/b?project="+url.QueryEscape(project(e)), body)
		return err
	}
	deleteBucket := func(ctx context.Context, e *Env) error {
		return e.RestDelete(ctx, "/storage/v1/b/"+bkt(e))
	}
	mediaREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return storageRESTMediaUpload(ctx, e, bkt(e), obj(e), []byte(payload), contentType)
	}
	multipartREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return storageRESTMultipartUpload(ctx, e, bkt(e), obj(e), []byte(payload), contentType)
	}
	resumableREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return storageRESTResumableUpload(ctx, e, bkt(e), obj(e), []byte(payload), contentType)
	}
	writeGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		return storageGRPCWriteObject(ctx, e, bkt(e), obj(e), []byte(payload), contentType)
	}
	// updateREST / updateGRPC stage the twin (the create half, discarded) so
	// both transports diff an update response rather than a second create.
	updateREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		if _, err := storageRESTMediaUpload(ctx, e, bkt(e), obj(e), []byte(payload), contentType); err != nil {
			return nil, err
		}
		return storageRESTPatchObject(ctx, e, bkt(e), obj(e))
	}
	updateGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		if _, err := storageGRPCWriteObject(ctx, e, bkt(e), obj(e), []byte(payload), contentType); err != nil {
			return nil, err
		}
		return storageGRPCUpdateObject(ctx, e, bkt(e), obj(e))
	}
	cleanupTwin := func(ctx context.Context, e *Env) error {
		return e.RestDelete(ctx, "/storage/v1/b/"+bkt(e)+"/o/"+url.PathEscape(obj(e)))
	}
	getGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *storagepb.Object
		err := storageDial(ctx, e, func(c storagepb.StorageClient) error {
			var cerr error
			out, cerr = c.GetObject(ctx, &storagepb.GetObjectRequest{
				Bucket: "projects/_/buckets/" + bkt(e),
				Object: obj(e),
			})
			return cerr
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	getREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodGet, "/storage/v1/b/"+bkt(e)+"/o/"+url.PathEscape(obj(e)), "")
	}
	listGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *storagepb.ListObjectsResponse
		err := storageDial(ctx, e, func(c storagepb.StorageClient) error {
			var cerr error
			out, cerr = c.ListObjects(ctx, &storagepb.ListObjectsRequest{
				Parent: "projects/_/buckets/" + bkt(e),
			})
			return cerr
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	listREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodGet, "/storage/v1/b/"+bkt(e)+"/o", "")
	}
	// createCanonical / patchCanonical establish the state the read steps
	// observe; the update is performed over REST and read back over both
	// transports so a transport-specific update rendering cannot hide.
	createCanonical := func(ctx context.Context, e *Env) error {
		_, err := storageRESTMediaUpload(ctx, e, bkt(e), obj(e), []byte(payload), contentType)
		return err
	}
	patchCanonical := func(ctx context.Context, e *Env) error {
		_, err := storageRESTPatchObject(ctx, e, bkt(e), obj(e))
		return err
	}
	deleteObject := func(ctx context.Context, e *Env) error {
		return e.RestDelete(ctx, "/storage/v1/b/"+bkt(e)+"/o/"+url.PathEscape(obj(e)))
	}

	return Scenario{Service: "storage", Steps: []Step{
		{Op: "CreateBucket", Mutate: createBucket},
		{
			Op: "ObjectsInsert(media) (parity)",
			Mutation: &MutationParity{
				GRPC: writeGRPC, REST: mediaREST, Cleanup: cleanupTwin, Project: storageObjectProjection,
			},
		},
		{
			Op: "ObjectsInsert(multipart) (parity)",
			Mutation: &MutationParity{
				GRPC: writeGRPC, REST: multipartREST, Cleanup: cleanupTwin, Project: storageObjectProjection,
			},
		},
		{
			Op: "ObjectsInsert(resumable) (parity)",
			Mutation: &MutationParity{
				GRPC: writeGRPC, REST: resumableREST, Cleanup: cleanupTwin, Project: storageObjectProjection,
			},
		},
		{Op: "CreateObject", Mutate: createCanonical},
		{
			Op: "GetObject", Project: storageObjectProjection,
			GRPC: getGRPC, REST: getREST,
		},
		{
			Op: "ListObjects", Scope: true, Project: storageObjectProjection,
			GRPC: listGRPC, REST: listREST,
		},
		{
			Op: "UpdateObject", Mutate: patchCanonical, Project: storageObjectProjection,
			GRPC: getGRPC, REST: getREST,
		},
		{
			Op: "ObjectsUpdate (parity)",
			Mutation: &MutationParity{
				GRPC: updateGRPC, REST: updateREST, Cleanup: cleanupTwin, Project: storageObjectProjection,
			},
		},
		{Op: "DeleteObject", Mutate: deleteObject},
		{Op: "DeleteBucket", Mutate: deleteBucket},
	}}
}

// storageDial opens a one-shot Cloud Storage gRPC connection and runs fn with
// the emulator's generated stub client, mirroring dsDial in cases_datastore.go
// (the generated pb package exports the stub client, not a high-level
// option-taking client).
func storageDial(ctx context.Context, e *Env, fn func(storagepb.StorageClient) error) error {
	conn, err := grpc.NewClient(e.Cfg.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(storagepb.NewStorageClient(conn))
}

// storageGRPCWriteObject writes one object with the bytes and content type the
// REST upload paths use, through BidiWriteObject (the RPC the official Go
// client's writer drives). A single spec + data + finish_write stream settles on
// exactly one resource response, so one Recv returns the created Object.
func storageGRPCWriteObject(ctx context.Context, e *Env, bkt, obj string, payload []byte, contentType string) (*storagepb.Object, error) {
	var out *storagepb.Object
	err := storageDial(ctx, e, func(c storagepb.StorageClient) error {
		stream, err := c.BidiWriteObject(ctx)
		if err != nil {
			return err
		}
		if err := stream.Send(&storagepb.BidiWriteObjectRequest{
			FirstMessage: &storagepb.BidiWriteObjectRequest_WriteObjectSpec{
				WriteObjectSpec: &storagepb.WriteObjectSpec{
					Resource: &storagepb.Object{Bucket: bkt, Name: obj, ContentType: contentType},
				},
			},
		}); err != nil {
			return err
		}
		if err := stream.Send(&storagepb.BidiWriteObjectRequest{
			Data: &storagepb.BidiWriteObjectRequest_ChecksummedData{
				ChecksummedData: &storagepb.ChecksummedData{Content: payload},
			},
		}); err != nil {
			return err
		}
		if err := stream.Send(&storagepb.BidiWriteObjectRequest{FinishWrite: true}); err != nil {
			return err
		}
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		out = resp.GetResource()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// storageGRPCUpdateObject applies a metadata update with the same mask the REST
// patch step expresses as a merge body.
func storageGRPCUpdateObject(ctx context.Context, e *Env, bkt, obj string) (*storagepb.Object, error) {
	var out *storagepb.Object
	err := storageDial(ctx, e, func(c storagepb.StorageClient) error {
		var cerr error
		out, cerr = c.UpdateObject(ctx, &storagepb.UpdateObjectRequest{
			Object:     &storagepb.Object{Bucket: bkt, Name: obj, Metadata: map[string]string{"marker": "v2"}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata"}},
		})
		return cerr
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// storageRESTMediaUpload uploads raw bytes through the media path
// (POST /upload/storage/v1/b/{b}/o?uploadType=media&name=...).
func storageRESTMediaUpload(ctx context.Context, e *Env, bkt, obj string, payload []byte, contentType string) (json.RawMessage, error) {
	path := "/upload/storage/v1/b/" + bkt + "/o?uploadType=media&name=" + url.QueryEscape(obj)
	body, status, _, err := e.RestRaw(ctx, http.MethodPost, path, payload, contentType, nil)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("media upload %s: HTTP %d: %s", path, status, truncateBody(body))
	}
	return body, nil
}

// storageRESTMultipartUpload uploads through the multipart/related path (a
// JSON metadata part followed by the media part), the encoding the Go SDK uses
// for small Writer uploads.
func storageRESTMultipartUpload(ctx context.Context, e *Env, bkt, obj string, payload []byte, contentType string) (json.RawMessage, error) {
	body, ct, err := storageMultipartBody(obj, payload, contentType)
	if err != nil {
		return nil, err
	}
	path := "/upload/storage/v1/b/" + bkt + "/o?uploadType=multipart"
	out, status, _, err := e.RestRaw(ctx, http.MethodPost, path, body, ct, nil)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("multipart upload %s: HTTP %d: %s", path, status, truncateBody(out))
	}
	return out, nil
}

// storageRESTResumableUpload drives the session/offset path: start a session
// (the upload URL comes back in the Location header), then post the whole
// payload as one terminal chunk carrying a Content-Range. The chunk URL is
// rebuilt from the emulator base rather than followed from Location, whose host
// is only a default when the request carried no base.
func storageRESTResumableUpload(ctx context.Context, e *Env, bkt, obj string, payload []byte, contentType string) (json.RawMessage, error) {
	startPath := "/upload/storage/v1/b/" + bkt + "/o?uploadType=resumable&name=" + url.QueryEscape(obj)
	extra := http.Header{}
	extra.Set("X-Upload-Content-Type", contentType)
	_, status, hdr, err := e.RestRaw(ctx, http.MethodPost, startPath, nil, "", extra)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("resumable start %s: HTTP %d", startPath, status)
	}
	uploadID := uploadIDFromLocation(hdr.Get("Location"))
	if uploadID == "" {
		return nil, fmt.Errorf("resumable start: no upload_id in Location %q", hdr.Get("Location"))
	}
	chunkPath := "/upload/storage/v1/b/" + bkt + "/o?uploadType=resumable&upload_id=" + url.QueryEscape(uploadID)
	cextra := http.Header{}
	cextra.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
	body, cstatus, _, err := e.RestRaw(ctx, http.MethodPost, chunkPath, payload, "application/octet-stream", cextra)
	if err != nil {
		return nil, err
	}
	if cstatus < 200 || cstatus >= 300 {
		return nil, fmt.Errorf("resumable chunk %s: HTTP %d: %s", chunkPath, cstatus, truncateBody(body))
	}
	return body, nil
}

// storageRESTPatchObject merges an object's metadata (objects.patch), the REST
// analogue of the gRPC UpdateObject with a metadata mask.
func storageRESTPatchObject(ctx context.Context, e *Env, bkt, obj string) (json.RawMessage, error) {
	path := "/storage/v1/b/" + bkt + "/o/" + url.PathEscape(obj)
	return e.Rest(ctx, http.MethodPatch, path, `{"metadata":{"marker":"v2"}}`)
}

// storageMultipartBody builds a multipart/related upload body: a JSON metadata
// part naming the object and its content type, then the media part.
func storageMultipartBody(obj string, payload []byte, contentType string) ([]byte, string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	metaHeader := textproto.MIMEHeader{}
	metaHeader.Set("Content-Type", "application/json; charset=UTF-8")
	metaPart, err := w.CreatePart(metaHeader)
	if err != nil {
		return nil, "", err
	}
	if _, err := fmt.Fprintf(metaPart, `{"name":%q,"contentType":%q}`, obj, contentType); err != nil {
		return nil, "", err
	}
	mediaHeader := textproto.MIMEHeader{}
	mediaHeader.Set("Content-Type", contentType)
	mediaPart, err := w.CreatePart(mediaHeader)
	if err != nil {
		return nil, "", err
	}
	if _, err := mediaPart.Write(payload); err != nil {
		return nil, "", err
	}
	if err := w.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), "multipart/related; boundary=" + w.Boundary(), nil
}

// uploadIDFromLocation extracts the upload_id query parameter from a resumable
// session's Location header.
func uploadIDFromLocation(loc string) string {
	u, err := url.Parse(loc)
	if err != nil {
		return ""
	}
	return u.Query().Get("upload_id")
}

// storageObjectProjection canonicalizes a Cloud Storage Object body — or an
// objects.list envelope — into one logical form shared by the REST JSON and the
// protojson rendering, leaving any genuine logical difference intact.
//
// The two encodings differ in ways the shared normalizer cannot fold:
//
//   - REST-only derived members (`kind`, `id`, `selfLink`, `mediaLink`,
//     `timeFinalized`, `timeStorageClassUpdated`) that the proto Object defines
//     no field for (selfLink/mediaLink would otherwise survive as volatile
//     sentinels on one side only);
//   - timestamp key names: REST `timeCreated`/`updated` vs proto
//     `createTime`/`updateTime`;
//   - content digests: REST top-level `crc32c` (base64 big-endian uint32) and
//     `md5Hash` vs proto `checksums{crc32c (fixed32 number), md5Hash}`;
//   - `bucket`: REST bare name vs the proto's `projects/_/buckets/{bucket}`
//     resource name;
//   - the list envelope: REST `items[]` vs proto `objects[]`.
//
// crc32c is reconciled (not dropped) so a real digest divergence still gates.
func storageObjectProjection(raw json.RawMessage) (json.RawMessage, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	projectStorageBody(v)
	return json.Marshal(v)
}

// projectStorageBody renames the list envelope's `items` array to `objects` and
// canonicalizes the object (or each listed object).
func projectStorageBody(v any) {
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	if items, ok := m["items"]; ok {
		delete(m, "items")
		m["objects"] = items
	}
	canonicalizeStorageObject(m)
	if arr, ok := m["objects"].([]any); ok {
		for _, e := range arr {
			if em, ok := e.(map[string]any); ok {
				canonicalizeStorageObject(em)
			}
		}
	}
}

// canonicalizeStorageObject rewrites one Object map into logical form.
func canonicalizeStorageObject(m map[string]any) {
	delete(m, "kind")
	delete(m, "id")
	delete(m, "selfLink")
	delete(m, "mediaLink")
	delete(m, "timeFinalized")
	delete(m, "timeStorageClassUpdated")
	if v, ok := m["timeCreated"]; ok {
		delete(m, "timeCreated")
		m["createTime"] = v
	}
	if v, ok := m["updated"]; ok {
		delete(m, "updated")
		m["updateTime"] = v
	}
	if b, ok := m["bucket"].(string); ok {
		m["bucket"] = strings.TrimPrefix(b, "projects/_/buckets/")
	}
	crc, hasCRC := m["crc32c"]
	md5, hasMD5 := m["md5Hash"]
	if hasCRC || hasMD5 {
		cs, _ := m["checksums"].(map[string]any)
		if cs == nil {
			cs = map[string]any{}
		}
		if hasCRC {
			if s, ok := crc.(string); ok {
				if n, ok := crc32cBase64ToNumber(s); ok {
					cs["crc32c"] = n
				}
			}
		}
		if hasMD5 {
			cs["md5Hash"] = md5
		}
		m["checksums"] = cs
		delete(m, "crc32c")
		delete(m, "md5Hash")
	}
}

// crc32cBase64ToNumber decodes the REST rendering of Object.crc32c — base64 of
// the big-endian uint32 — into the number protojson renders for the proto's
// fixed32 checksum, so the two compare equal.
func crc32cBase64ToNumber(b64 string) (json.Number, bool) {
	b, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(b) != 4 {
		return "", false
	}
	v := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	return json.Number(strconv.FormatUint(uint64(v), 10)), true
}
