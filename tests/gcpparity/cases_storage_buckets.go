//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// storageBucketsScenario compares the Cloud Storage bucket surface over REST and
// gRPC (AUD3-15), the counterpart to the object/media scenario in
// cases_storage.go. The bucket resource is created, read, listed, updated and
// deleted over each transport, with create/update mutation-parity steps, so a
// field one transport's transcode drops or invents is caught the same way the
// object surface is.
//
// gRPC is driven with the emulator's own storagepb stub (see cases_storage.go).
func storageBucketsScenario() Scenario {
	bkt := func(e *Env) string { return e.Resource("parity-bucket") }
	project := func(e *Env) string { return e.Cfg.Project }

	bucketBody := func(e *Env) string {
		return fmt.Sprintf(`{"name":%q,"location":"US","storageClass":"STANDARD"}`, bkt(e))
	}
	createREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodPost, "/storage/v1/b?project="+url.QueryEscape(project(e)), bucketBody(e))
	}
	getREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodGet, "/storage/v1/b/"+bkt(e), "")
	}
	listREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodGet, "/storage/v1/b?project="+url.QueryEscape(project(e)), "")
	}
	updateREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodPatch, "/storage/v1/b/"+bkt(e), `{"labels":{"marker":"v2"}}`)
	}
	deleteREST := func(ctx context.Context, e *Env) error {
		return e.RestDelete(ctx, "/storage/v1/b/"+bkt(e))
	}
	createGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *storagepb.Bucket
		err := storageDial(ctx, e, func(c storagepb.StorageClient) error {
			var cerr error
			out, cerr = c.CreateBucket(ctx, &storagepb.CreateBucketRequest{
				Parent:   "projects/" + project(e),
				BucketId: bkt(e),
				Bucket:   &storagepb.Bucket{Location: "US", StorageClass: "STANDARD"},
			})
			return cerr
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	getGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *storagepb.Bucket
		err := storageDial(ctx, e, func(c storagepb.StorageClient) error {
			var cerr error
			out, cerr = c.GetBucket(ctx, &storagepb.GetBucketRequest{Name: "projects/_/buckets/" + bkt(e)})
			return cerr
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	listGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *storagepb.ListBucketsResponse
		err := storageDial(ctx, e, func(c storagepb.StorageClient) error {
			var cerr error
			out, cerr = c.ListBuckets(ctx, &storagepb.ListBucketsRequest{Parent: "projects/" + project(e)})
			return cerr
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	updateBucketGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *storagepb.Bucket
		err := storageDial(ctx, e, func(c storagepb.StorageClient) error {
			var cerr error
			out, cerr = c.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
				Bucket:     &storagepb.Bucket{Name: "projects/_/buckets/" + bkt(e), Labels: map[string]string{"marker": "v2"}},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
			})
			return cerr
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	// updateGRPCMutation / updateRESTMutation stage the twin (the create half,
	// discarded) so both transports diff an update response.
	updateGRPCMutation := func(ctx context.Context, e *Env) (protoMessage, error) {
		if _, err := createGRPC(ctx, e); err != nil {
			return nil, err
		}
		return updateBucketGRPC(ctx, e)
	}
	updateRESTMutation := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		if _, err := createREST(ctx, e); err != nil {
			return nil, err
		}
		return updateREST(ctx, e)
	}
	createCanonical := func(ctx context.Context, e *Env) error {
		_, err := createREST(ctx, e)
		return err
	}
	updateCanonical := func(ctx context.Context, e *Env) error {
		_, err := updateREST(ctx, e)
		return err
	}

	return Scenario{Service: "storage", Steps: []Step{
		{
			Op: "BucketsInsert (parity)",
			Mutation: &MutationParity{
				GRPC: createGRPC, REST: createREST, Cleanup: deleteREST, Project: storageBucketProjection,
			},
		},
		{Op: "CreateBucket", Mutate: createCanonical},
		{
			Op: "GetBucket", Project: storageBucketProjection,
			GRPC: getGRPC, REST: getREST,
		},
		{
			Op: "ListBuckets", Scope: true, Project: storageBucketProjection,
			GRPC: listGRPC, REST: listREST,
		},
		{
			Op: "UpdateBucket", Mutate: updateCanonical, Project: storageBucketProjection,
			GRPC: getGRPC, REST: getREST,
		},
		{
			Op: "BucketsUpdate (parity)",
			Mutation: &MutationParity{
				GRPC: updateGRPCMutation, REST: updateRESTMutation, Cleanup: deleteREST, Project: storageBucketProjection,
			},
		},
		{Op: "DeleteBucket", Mutate: deleteREST},
	}}
}

// storageBucketProjection canonicalizes a Cloud Storage bucket body — or a
// buckets.list envelope — into one logical form shared by the REST JSON and the
// protojson rendering, leaving any genuine logical difference intact.
//
//   - REST-only members with no proto Bucket field (`kind`, `id`, `selfLink`,
//     `projectNumber`, `generation`);
//   - the gRPC-only `bucketId` (the REST equivalent is `id`, dropped above);
//   - the REST key `iamConfiguration` vs the proto `iamConfig`;
//   - timestamp key names: REST `timeCreated`/`updated` vs proto
//     `createTime`/`updateTime`;
//   - the soft-delete retention: REST `retentionDurationSeconds` (seconds) vs
//     proto `retentionDuration` (a Duration string);
//   - `name`: REST bare name vs the proto's `projects/_/buckets/{bucket}`
//     resource name;
//   - the list envelope: REST `items[]` vs proto `buckets[]`.
func storageBucketProjection(raw json.RawMessage) (json.RawMessage, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	projectStorageBucketBody(v)
	return json.Marshal(v)
}

func projectStorageBucketBody(v any) {
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	if items, ok := m["items"]; ok {
		delete(m, "items")
		m["buckets"] = items
	}
	canonicalizeStorageBucket(m)
	if arr, ok := m["buckets"].([]any); ok {
		for _, e := range arr {
			if em, ok := e.(map[string]any); ok {
				canonicalizeStorageBucket(em)
			}
		}
	}
}

func canonicalizeStorageBucket(m map[string]any) {
	delete(m, "kind")
	delete(m, "id")
	delete(m, "selfLink")
	delete(m, "projectNumber")
	delete(m, "generation")
	delete(m, "bucketId")
	if v, ok := m["timeCreated"]; ok {
		delete(m, "timeCreated")
		m["createTime"] = v
	}
	if v, ok := m["updated"]; ok {
		delete(m, "updated")
		m["updateTime"] = v
	}
	if v, ok := m["iamConfiguration"]; ok {
		delete(m, "iamConfiguration")
		m["iamConfig"] = v
	}
	if n, ok := m["name"].(string); ok {
		m["name"] = strings.TrimPrefix(n, "projects/_/buckets/")
	}
	// softDeletePolicy.retentionDurationSeconds (REST, seconds) and
	// retentionDuration (proto, a "<n>s" Duration) fold to one seconds number.
	if sd, ok := m["softDeletePolicy"].(map[string]any); ok {
		if v, ok := sd["retentionDurationSeconds"]; ok {
			delete(sd, "retentionDurationSeconds")
			sd["retentionDuration"] = v
		}
		if v, ok := sd["retentionDuration"]; ok {
			if n, ok := durationSecondsNumber(v); ok {
				sd["retentionDuration"] = n
			}
		}
	}
}

// durationSecondsNumber parses a duration rendered either as a bare seconds
// count ("604800") or as a protobuf duration string ("604800s") into the
// canonical seconds number both transports compare.
func durationSecondsNumber(v any) (json.Number, bool) {
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	s = strings.TrimSuffix(s, "s")
	if _, err := strconv.ParseInt(s, 10, 64); err != nil {
		return "", false
	}
	return json.Number(s), true
}
