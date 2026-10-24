//go:build gcp_parity

package gcpparity

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// failFindings returns the findings that would fail the gate.
func failFindings(fs []Finding) []Finding {
	var out []Finding
	for _, f := range fs {
		if f.failing() {
			out = append(out, f)
		}
	}
	return out
}

func kinds(fs []Finding) map[string]int {
	m := map[string]int{}
	for _, f := range fs {
		m[f.Kind]++
	}
	return m
}

// TestMutationNormalizeFoldsTwinSide proves the gRPC (...-grpc-…) and REST
// (...-rest-…) twins normalize to one canonical mutation-response body, so the
// twin token itself never decides a mutation-parity comparison (AUD3-12).
func TestMutationNormalizeFoldsTwinSide(t *testing.T) {
	grpc := json.RawMessage(`{"name":"projects/p/topics/topic-grpc-abc123","topic":"projects/p/topics/topic-grpc-abc123"}`)
	rest := json.RawMessage(`{"name":"projects/p/topics/topic-rest-abc123","topic":"projects/p/topics/topic-rest-abc123"}`)

	gn, err := mutationNormalize(grpc, nil)
	if err != nil {
		t.Fatal(err)
	}
	rn, err := mutationNormalize(rest, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(gn) != string(rn) {
		t.Fatalf("twins must fold to one body:\n grpc=%s\n rest=%s", gn, rn)
	}
	if fs := compareNormalized("pubsub", "CreateTopic", rn, gn, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("folded twins must not diverge, got %+v", failFindings(fs))
	}
}

// TestMutationParityFailsOnDivergentResponse is the seeded proof that a field
// one transport drops from a Create/Update response is a gate failure — the bug
// class AUD3-12 closes.
func TestMutationParityFailsOnDivergentResponse(t *testing.T) {
	rest := json.RawMessage(`{"name":"projects/p/topics/topic-abc","labels":{"env":"prod"}}`)
	grpc := json.RawMessage(`{"name":"projects/p/topics/topic-abc"}`)

	rn, _ := mutationNormalize(rest, nil)
	gn, _ := mutationNormalize(grpc, nil)
	fs := compareNormalized("pubsub", "CreateTopic", rn, gn, nil)
	if k := kinds(fs); k["missing_field"] != 1 {
		t.Fatalf("want 1 missing_field for the dropped labels, got kinds=%v", k)
	}
	if len(failFindings(fs)) == 0 {
		t.Fatal("a dropped mutation-response field must fail the gate")
	}
}

// TestDifferFailsOnDroppedField is the seeded proof that a field the REST body
// exposes and the gRPC transcode drops is a gate failure — the bug class this
// harness exists to catch.
func TestDifferFailsOnDroppedField(t *testing.T) {
	rest := json.RawMessage(`{"name":"projects/p/secrets/s","labels":{"env":"prod"},"etag":"abc"}`)
	grpc := json.RawMessage(`{"name":"projects/p/secrets/s","etag":"different"}`)

	rn, err := normalizeJSON(rest)
	if err != nil {
		t.Fatal(err)
	}
	gn, err := normalizeJSON(grpc)
	if err != nil {
		t.Fatal(err)
	}
	fs := compareNormalized("secretmanager", "GetSecret", rn, gn, nil)

	k := kinds(fs)
	if k["missing_field"] != 1 {
		t.Fatalf("want 1 missing_field for the dropped labels, got kinds=%v", k)
	}
	var loc string
	for _, f := range fs {
		if f.Kind == "missing_field" {
			loc = f.Location
		}
	}
	if loc != "response.labels" {
		t.Errorf("missing_field location = %q, want response.labels", loc)
	}
	if len(failFindings(fs)) == 0 {
		t.Fatal("a dropped logical field must fail the gate")
	}
}

// TestDifferFailsOnRenamedField proves a renamed field surfaces as both a
// missing (REST) and an extra (gRPC) finding.
func TestDifferFailsOnRenamedField(t *testing.T) {
	rest := json.RawMessage(`{"displayName":"prod"}`)
	grpc := json.RawMessage(`{"name":"prod"}`)

	rn, _ := normalizeJSON(rest)
	gn, _ := normalizeJSON(grpc)
	fs := compareNormalized("resourcemanager", "GetProject", rn, gn, nil)

	k := kinds(fs)
	if k["missing_field"] != 1 || k["extra_field"] != 1 {
		t.Fatalf("want missing+extra for a renamed field, got kinds=%v", k)
	}
}

// TestDifferPassesOnEquivalentBodies proves the normalizer equalizes the two
// legitimate representation differences without manufacturing a failure:
// protojson renders a 100ms duration as "0.100s" vs REST "0.1s", and int64 as a
// quoted string vs an unquoted number.
func TestDifferPassesOnEquivalentBodies(t *testing.T) {
	rest := json.RawMessage(`{"retryConfig":{"minBackoff":"0.1s","maxBackoff":"3600s"},"memoryBytes":3221225472,"etag":"x"}`)
	grpc := json.RawMessage(`{"retryConfig":{"minBackoff":"0.100s","maxBackoff":"3600s"},"memoryBytes":"3221225472","etag":"y"}`)

	rn, _ := normalizeJSON(rest)
	gn, _ := normalizeJSON(grpc)
	fs := compareNormalized("tasks", "GetQueue", rn, gn, nil)
	if len(failFindings(fs)) != 0 {
		t.Fatalf("equivalent bodies must not fail, got %+v", failFindings(fs))
	}
}

// TestNullValueEncodingsAgreeWithoutProjection proves that once the REST codecs
// render google.protobuf.NullValue as JSON null — the protojson form real GCP
// emits (AUD3-13) — the Datastore and Firestore Value-union nulls compare equal
// with no per-service projection, at the top level and nested inside an
// entityValue/mapValue/arrayValue. The Discovery enum name "NULL_VALUE" (the
// pre-fix REST encoding) still diverges, so a regression is not masked.
func TestNullValueEncodingsAgreeWithoutProjection(t *testing.T) {
	cases := []struct{ svc, op, body string }{
		{"datastore", "Lookup", `{"found":[{"entity":{"properties":{
			"nul":{"nullValue":null},
			"nest":{"entityValue":{"properties":{"nul":{"nullValue":null}}}},
			"arr":{"arrayValue":{"values":[{"nullValue":null},{"stringValue":"x"}]}},
			"str":{"stringValue":"x"}}}}]}`},
		{"firestore", "GetDocument", `{"name":"projects/p/databases/(default)/documents/c/d","fields":{
			"nul":{"nullValue":null},
			"map":{"mapValue":{"fields":{"nul":{"nullValue":null}}}},
			"arr":{"arrayValue":{"values":[{"nullValue":null},{"stringValue":"x"}]}},
			"str":{"stringValue":"x"}}}`},
	}
	for _, tc := range cases {
		a, err := normalizeJSON(json.RawMessage(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		b, err := normalizeJSON(json.RawMessage(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		if fs := compareNormalized(tc.svc, tc.op, a, b, nil); len(failFindings(fs)) != 0 {
			t.Fatalf("%s/%s: identical JSON-null bodies must agree, got %+v", tc.svc, tc.op, failFindings(fs))
		}
		regressed, err := normalizeJSON(json.RawMessage(bytes.ReplaceAll([]byte(tc.body), []byte(":null"), []byte(`:"NULL_VALUE"`))))
		if err != nil {
			t.Fatal(err)
		}
		if len(failFindings(compareNormalized(tc.svc, tc.op, a, regressed, nil))) == 0 {
			t.Fatalf("%s/%s: the Discovery enum form must still gate", tc.svc, tc.op)
		}
	}
}

// TestMutationProjectionAppliesSymmetric proves the mutation path applies a
// MutationParity projection to both sides after the twin token is folded out
// (the datastore CommitResponse carries no Value, so the scenario projection is
// a no-op there; this pins the plumbing directly).
func TestMutationProjectionAppliesSymmetric(t *testing.T) {
	// The two sides differ only in the nullValue encoding; a projection that
	// runs after the twin token is folded must equalize them.
	grpc := json.RawMessage(`{"name":"x-grpc-abc","nul":"null"}`)
	rest := json.RawMessage(`{"name":"x-rest-abc","nul":"NULL_VALUE"}`)
	proj := func(raw json.RawMessage) (json.RawMessage, error) {
		return bytes.ReplaceAll(raw, []byte(`"null"`), []byte(`"NULL_VALUE"`)), nil
	}

	// Without the projection the encoding difference is a real divergence.
	wn, _ := mutationNormalize(grpc, nil)
	wr, _ := mutationNormalize(rest, nil)
	if len(failFindings(compareNormalized("datastore", "Commit", wr, wn, nil))) == 0 {
		t.Fatal("without the projection the two encodings must diverge")
	}

	gn, err := mutationNormalize(grpc, proj)
	if err != nil {
		t.Fatal(err)
	}
	rn, err := mutationNormalize(rest, proj)
	if err != nil {
		t.Fatal(err)
	}
	if string(gn) != string(rn) {
		t.Fatalf("projection must apply after twin folding on both sides:\n grpc=%s\n rest=%s", gn, rn)
	}
	if fs := compareNormalized("datastore", "Commit", rn, gn, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("projected twins must not diverge, got %+v", failFindings(fs))
	}
}

// TestAllowanceAcceptsDocumentedDifference proves an allowance with a reason
// turns a divergence into an accepted, non-failing finding.
func TestAllowanceAcceptsDocumentedDifference(t *testing.T) {
	rest := json.RawMessage(`{"name":"c","bootstrapAddress":"broker:9092"}`)
	grpc := json.RawMessage(`{"name":"c"}`)
	rn, _ := normalizeJSON(rest)
	gn, _ := normalizeJSON(grpc)

	allow := []Allowance{{Service: "managedkafka", Op: "GetCluster", Path: "response.bootstrapAddress", Reason: "upstream proto has no bootstrap_address"}}
	fs := compareNormalized("managedkafka", "GetCluster", rn, gn, allow)
	if len(failFindings(fs)) != 0 {
		t.Fatalf("an allowed divergence must not fail, got %+v", failFindings(fs))
	}
	var allowed int
	for _, f := range fs {
		if f.Allowed {
			allowed++
		}
	}
	if allowed != 1 {
		t.Fatalf("want 1 allowed finding, got %d", allowed)
	}
}

// TestNormalizeDropsZeroAndEmptyMembers proves a transport that omits a
// defaulted field does not diverge from one that emits it.
func TestNormalizeDropsZeroAndEmptyMembers(t *testing.T) {
	rest := json.RawMessage(`{"name":"n","labels":{},"count":0,"enabled":false,"desc":""}`)
	grpc := json.RawMessage(`{"name":"n"}`)
	rn, _ := normalizeJSON(rest)
	gn, _ := normalizeJSON(grpc)
	fs := compareNormalized("svc", "Op", rn, gn, nil)
	if len(failFindings(fs)) != 0 {
		t.Fatalf("defaulted members must cancel, got %+v", failFindings(fs))
	}
}

// TestNormalizeFoldsDatastoreSnapshotVersion checks the AUD6-5 output-only
// batch snapshot version — a per-render server value, so the REST and gRPC
// queries return different numbers — cancels across transports while a real
// logical difference still fails.
func TestNormalizeFoldsDatastoreSnapshotVersion(t *testing.T) {
	rest := json.RawMessage(`{"batch":{"snapshotVersion":"1791489529880689","readTime":"2026-10-08T20:00:00Z","moreResults":"NO_MORE_RESULTS"}}`)
	grpc := json.RawMessage(`{"batch":{"snapshotVersion":"1791489529880199","readTime":"2026-10-08T20:00:01Z","moreResults":"NO_MORE_RESULTS"}}`)
	rn, _ := normalizeJSON(rest)
	gn, _ := normalizeJSON(grpc)
	if fs := failFindings(compareNormalized("datastore", "RunQuery", rn, gn, nil)); len(fs) != 0 {
		t.Fatalf("snapshotVersion must fold across transports, got %+v", fs)
	}
	// A genuine logical difference must still fail: moreResults is not volatile.
	restBad := json.RawMessage(`{"batch":{"moreResults":"MORE_RESULTS_AFTER_LIMIT"}}`)
	rnBad, _ := normalizeJSON(restBad)
	if fs := failFindings(compareNormalized("datastore", "RunQuery", rnBad, gn, nil)); len(fs) == 0 {
		t.Fatal("a real moreResults difference must still surface")
	}
}

// TestStorageBucketProjectionReconcilesEncodings proves the bucket projection
// equalizes the REST and protojson renderings of one Bucket and the
// buckets.list envelope: the REST-only members (kind/id/selfLink/projectNumber/
// generation), the gRPC-only bucketId, the iamConfiguration→iamConfig key, the
// timestamp key names, the soft-delete retention shape
// (retentionDurationSeconds vs a "…s" Duration), the name prefix and the
// items[]→buckets[] envelope — leaving a real dropped field failing.
func TestStorageBucketProjectionReconcilesEncodings(t *testing.T) {
	rest := json.RawMessage(`{
		"kind":"storage#bucket","id":"b","name":"b","location":"US",
		"locationType":"multi-region","storageClass":"STANDARD",
		"timeCreated":"2024-01-01T00:00:00Z","updated":"2024-01-01T00:00:00Z",
		"generation":"0","metageneration":"1","projectNumber":"123",
		"selfLink":"http://localhost/storage/v1/b/b","etag":"CAE=",
		"versioning":{"enabled":false},
		"iamConfiguration":{"uniformBucketLevelAccess":{"enabled":false},"bucketPolicyOnly":{"enabled":false},"publicAccessPrevention":"inherited"},
		"softDeletePolicy":{"retentionDurationSeconds":"604800","effectiveTime":"2024-01-01T00:00:00Z"},
		"defaultEventBasedHold":false}`)
	grpc := json.RawMessage(`{
		"name":"projects/_/buckets/b","bucketId":"b","etag":"CAE=","metageneration":"1",
		"location":"US","locationType":"multi-region","storageClass":"STANDARD",
		"createTime":"2024-01-01T00:00:00Z","updateTime":"2024-01-01T00:00:00Z",
		"iamConfig":{"uniformBucketLevelAccess":{},"publicAccessPrevention":"inherited"},
		"softDeletePolicy":{"retentionDuration":"604800s","effectiveTime":"2024-01-01T00:00:00Z"}}`)

	rn, err := storageBucketProjection(rest)
	if err != nil {
		t.Fatal(err)
	}
	gn, err := storageBucketProjection(grpc)
	if err != nil {
		t.Fatal(err)
	}
	nr, _ := normalizeJSON(rn)
	ng, _ := normalizeJSON(gn)
	if fs := compareNormalized("storage", "GetBucket", nr, ng, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("projected bucket bodies must not diverge, got %+v", failFindings(fs))
	}

	// A real dropped field must still gate after projection.
	dropped, err := storageBucketProjection(json.RawMessage(`{"name":"projects/_/buckets/b","location":"US"}`))
	if err != nil {
		t.Fatal(err)
	}
	nd, _ := normalizeJSON(dropped)
	if len(failFindings(compareNormalized("storage", "GetBucket", nr, nd, nil))) == 0 {
		t.Fatal("a logical field one transport drops must still gate after projection")
	}

	// The list envelope's items[] is renamed to buckets[] so it aligns with the
	// gRPC list response.
	list, err := storageBucketProjection(json.RawMessage(`{"kind":"storage#buckets","items":[{"name":"b","location":"US"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	gl, err := storageBucketProjection(json.RawMessage(`{"buckets":[{"name":"projects/_/buckets/b","location":"US"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	nl, _ := normalizeJSON(list)
	ngl, _ := normalizeJSON(gl)
	if fs := compareNormalized("storage", "ListBuckets", nl, ngl, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("bucket list envelopes must align after projection, got %+v", failFindings(fs))
	}
}

// TestStorageObjectProjectionReconcilesEncodings proves the storage projection
// equalizes the REST and protojson renderings of one Object — the REST-only
// derived members (kind/id/selfLink/mediaLink/timeFinalized/
// timeStorageClassUpdated), the timestamp key names (timeCreated/updated vs
// createTime/updateTime), the content-digest shape (top-level base64 crc32c/
// md5Hash vs a checksums message) and the bucket resource prefix — and renames
// the objects.list `items[]` envelope to the proto's `objects[]`, while leaving
// a real dropped field failing.
func TestStorageObjectProjectionReconcilesEncodings(t *testing.T) {
	rest := json.RawMessage(`{
		"kind":"storage#object","id":"b/o/1","name":"o","bucket":"b","size":"17",
		"contentType":"text/plain","crc32c":"EjRWeA==","md5Hash":"1B2M2Y8AsgTpgAmY7PhCfg==",
		"etag":"CAE=","selfLink":"http://localhost/storage/v1/b/b/o/o",
		"mediaLink":"http://localhost/download/storage/v1/b/b/o/o?alt=media",
		"generation":"1","metageneration":"1","storageClass":"STANDARD",
		"timeCreated":"2024-01-01T00:00:00Z","updated":"2024-01-01T00:00:00Z",
		"timeFinalized":"2024-01-01T00:00:00Z","timeStorageClassUpdated":"2024-01-01T00:00:00Z"}`)
	grpc := json.RawMessage(`{
		"name":"o","bucket":"projects/_/buckets/b","etag":"CAE=",
		"generation":"1","metageneration":"1","storageClass":"STANDARD","size":"17",
		"contentType":"text/plain",
		"checksums":{"crc32c":305419896,"md5Hash":"1B2M2Y8AsgTpgAmY7PhCfg=="},
		"createTime":"2024-01-01T00:00:00Z","updateTime":"2024-01-01T00:00:00Z"}`)

	rn, err := storageObjectProjection(rest)
	if err != nil {
		t.Fatal(err)
	}
	gn, err := storageObjectProjection(grpc)
	if err != nil {
		t.Fatal(err)
	}
	nr, _ := normalizeJSON(rn)
	ng, _ := normalizeJSON(gn)
	if fs := compareNormalized("storage", "GetObject", nr, ng, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("projected object bodies must not diverge, got %+v", failFindings(fs))
	}

	// A real dropped field must still gate after projection.
	dropped, err := storageObjectProjection(json.RawMessage(`{"name":"o","bucket":"projects/_/buckets/b","size":"17"}`))
	if err != nil {
		t.Fatal(err)
	}
	nd, _ := normalizeJSON(dropped)
	if len(failFindings(compareNormalized("storage", "GetObject", nr, nd, nil))) == 0 {
		t.Fatal("a logical field one transport drops must still gate after projection")
	}

	// The list envelope's items[] is renamed to objects[] so it aligns with the
	// gRPC list response.
	list, err := storageObjectProjection(json.RawMessage(`{"kind":"storage#objects","items":[{"name":"o","bucket":"b"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	gl, err := storageObjectProjection(json.RawMessage(`{"objects":[{"name":"o","bucket":"projects/_/buckets/b"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	nl, _ := normalizeJSON(list)
	ngl, _ := normalizeJSON(gl)
	if fs := compareNormalized("storage", "ListObjects", nl, ngl, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("list envelopes must align after projection, got %+v", failFindings(fs))
	}
}

// TestAggregateNDJSONReadsFrames proves the REST NDJSON reader turns a
// newline-delimited stream body into one JSON array of frames — the logical form
// the gRPC iterator is aggregated to — folds a blank line, and rejects a
// malformed line rather than silently dropping it (a dropped frame must not let
// a broken stream pass by omission).
func TestAggregateNDJSONReadsFrames(t *testing.T) {
	ndjson := json.RawMessage("{\"document\":{\"name\":\"d1\"},\"readTime\":\"2024-01-01T00:00:00Z\"}\n" +
		"\n" +
		"{\"document\":{\"name\":\"d2\"}}\n" +
		"{\"done\":true}\n")
	got, err := aggregateNDJSON(ndjson)
	if err != nil {
		t.Fatalf("aggregate NDJSON: %v", err)
	}
	want := json.RawMessage(`[{"document":{"name":"d1"},"readTime":"2024-01-01T00:00:00Z"},{"document":{"name":"d2"}},{"done":true}]`)
	var gv, wv any
	if err := json.Unmarshal(got, &gv); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &wv); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gv, wv) {
		t.Fatalf("NDJSON aggregation mismatch:\n got=%s\nwant=%s", got, want)
	}

	if _, err := aggregateNDJSON(json.RawMessage("{\"ok\":true}\nnot json\n")); err == nil {
		t.Fatal("a malformed NDJSON line must be an error, not dropped")
	}
	if empty, err := aggregateNDJSON(nil); err != nil || string(empty) != "[]" {
		t.Fatalf("empty body must aggregate to [], got %s (err=%v)", empty, err)
	}
}

// TestStreamParityAggregatesFrames is the seeded proof of the AUD3-14 pipeline:
// the gRPC iterator frames and the REST NDJSON body of the same runQuery stream
// aggregate to equal logical arrays, while a frame one transport drops is a
// gate failure — the class of streaming divergence the single-body contract
// could not see.
func TestStreamParityAggregatesFrames(t *testing.T) {
	e := &Env{}
	doc := &firestorepb.RunQueryResponse{
		Document: &firestorepb.Document{Name: "projects/p/databases/(default)/documents/c/d"},
	}
	done := &firestorepb.RunQueryResponse{
		ContinuationSelector: &firestorepb.RunQueryResponse_Done{Done: true},
	}
	restBody := json.RawMessage("{\"document\":{\"name\":\"projects/p/databases/(default)/documents/c/d\"}}\n{\"done\":true}")

	match := &StreamParity{
		GRPC: func(context.Context, *Env) ([]protoMessage, error) { return []protoMessage{doc, done}, nil },
		REST: func(context.Context, *Env) (json.RawMessage, error) { return restBody, nil },
	}
	if fs := runStreamParity(context.Background(), e, "firestore", "RunQuery", match, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("equal aggregated frames must not diverge, got %+v", failFindings(fs))
	}

	dropped := &StreamParity{
		GRPC: func(context.Context, *Env) ([]protoMessage, error) { return []protoMessage{doc}, nil },
		REST: func(context.Context, *Env) (json.RawMessage, error) { return restBody, nil },
	}
	fs := runStreamParity(context.Background(), e, "firestore", "RunQuery", dropped, nil)
	if k := kinds(fs); k["array_length_mismatch"] != 1 {
		t.Fatalf("a dropped stream frame must surface as an array length mismatch, got kinds=%v", k)
	}
	if len(failFindings(fs)) == 0 {
		t.Fatal("a dropped stream frame must fail the gate")
	}
}

// TestResourceManagerProjectionReconcilesEncodings proves the Resource Manager
// projection equalizes the v1 REST and v3 protojson renderings of one Project:
// the overloaded `name` (v1 display name vs the v3 "projects/{id}" resource
// name), `lifecycleState` vs `state`, the v1 ResourceId parent vs the v3 parent
// string, and the fields only one schema defines (v1 `projectNumber`; v3
// `etag`/`updateTime`/`deleteTime`) — while a genuine logical field one
// transport drops still fails after projection. It also aligns the
// projects.list envelope, which both transports key on `projects`.
func TestResourceManagerProjectionReconcilesEncodings(t *testing.T) {
	rest := json.RawMessage(`{
		"projectId":"rm-abc123","projectNumber":"415104041262","name":"My Project",
		"lifecycleState":"ACTIVE","parent":{"type":"organization","id":"123"},
		"labels":{"env":"parity"},"createTime":"2024-01-01T00:00:00Z"}`)
	grpc := json.RawMessage(`{
		"name":"projects/rm-abc123","parent":"organizations/123","projectId":"rm-abc123",
		"state":"ACTIVE","displayName":"My Project","labels":{"env":"parity"},
		"createTime":"2024-01-01T00:00:00Z","updateTime":"2024-01-01T00:00:00Z","etag":"abc"}`)

	rn, err := resourceManagerProjection(rest)
	if err != nil {
		t.Fatal(err)
	}
	gn, err := resourceManagerProjection(grpc)
	if err != nil {
		t.Fatal(err)
	}
	nr, _ := normalizeJSON(rn)
	ng, _ := normalizeJSON(gn)
	if fs := compareNormalized("resourcemanager", "GetProject", nr, ng, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("projected project bodies must not diverge, got %+v", failFindings(fs))
	}

	// A real dropped logical field must still gate after projection.
	dropped, err := resourceManagerProjection(json.RawMessage(`{
		"name":"projects/rm-abc123","projectId":"rm-abc123","state":"ACTIVE","parent":"organizations/123"}`))
	if err != nil {
		t.Fatal(err)
	}
	nd, _ := normalizeJSON(dropped)
	if len(failFindings(compareNormalized("resourcemanager", "GetProject", nr, nd, nil))) == 0 {
		t.Fatal("a logical field one transport drops must still gate after projection")
	}

	// The projects.list envelope aligns after projection: both transports use
	// the `projects` key, and `name` is normalized so list-scoping can filter.
	// The other project is out of scope, so scoping must drop it.
	listREST, err := resourceManagerProjection(json.RawMessage(`{
		"projects":[
			{"projectId":"rm-abc123","projectNumber":"9","name":"My Project","lifecycleState":"ACTIVE"},
			{"projectId":"other-zzz999","projectNumber":"8","name":"Other","lifecycleState":"ACTIVE"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	listGRPC, err := resourceManagerProjection(json.RawMessage(`{
		"projects":[
			{"name":"projects/rm-abc123","projectId":"rm-abc123","displayName":"My Project","state":"ACTIVE"},
			{"name":"projects/other-zzz999","projectId":"other-zzz999","displayName":"Other","state":"ACTIVE"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	nl, _ := normalizeJSON(listREST)
	ngl, _ := normalizeJSON(listGRPC)
	if fs := compareNormalized("resourcemanager", "ListProjects", nl, ngl, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("project list envelopes must align after projection, got %+v", failFindings(fs))
	}
	// Scoping on the normalized `name` keeps only the run project on both sides.
	scopedREST, _ := normalizeScoped(listREST, "abc123")
	scopedGRPC, _ := normalizeScoped(listGRPC, "abc123")
	if fs := compareNormalized("resourcemanager", "ListProjects", scopedREST, scopedGRPC, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("scoped list envelopes must align, got %+v", failFindings(fs))
	}
	var scoped struct {
		Projects []map[string]any `json:"projects"`
	}
	if err := json.Unmarshal(scopedREST, &scoped); err != nil {
		t.Fatal(err)
	}
	if len(scoped.Projects) != 1 || scoped.Projects[0]["projectId"] != "rm-abc123" {
		t.Fatalf("list scoping on the normalized name must keep only the run project, got %s", scopedREST)
	}
}

// TestIamPolicyBodiesAgree proves the shared IAMPolicy Policy envelope renders
// identically over the two transports after normalization (AUD3-6): the REST
// Discovery JSON and the gRPC protojson form carry the same version and bindings
// (member order is not part of the contract), while the per-render `etag` folds
// to the volatile sentinel. A binding one transport drops still gates, so the
// fold does not mask a real logical divergence.
func TestIamPolicyBodiesAgree(t *testing.T) {
	rest := json.RawMessage(`{"version":1,"etag":"ACAB","bindings":[{"role":"roles/pubsub.viewer","members":["user:a@example.com","user:b@example.com"]}]}`)
	grpc := json.RawMessage(`{"version":1,"etag":"QUNBQg==","bindings":[{"role":"roles/pubsub.viewer","members":["user:b@example.com","user:a@example.com"]}]}`)

	rn, err := normalizeJSON(rest)
	if err != nil {
		t.Fatal(err)
	}
	gn, err := normalizeJSON(grpc)
	if err != nil {
		t.Fatal(err)
	}
	if fs := compareNormalized("iam", "GetIamPolicy", rn, gn, nil); len(failFindings(fs)) != 0 {
		t.Fatalf("equivalent Policy bodies must not diverge, got %+v", failFindings(fs))
	}

	// A binding the gRPC side drops must still gate: the etag fold must not hide
	// a real logical difference.
	dropped, err := normalizeJSON(json.RawMessage(`{"version":1,"etag":"QUNBQg=="}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(failFindings(compareNormalized("iam", "GetIamPolicy", rn, dropped, nil))) == 0 {
		t.Fatal("a dropped binding must still gate after normalization")
	}
}
