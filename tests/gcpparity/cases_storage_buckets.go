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

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// storageBucketsScenario compares the Cloud Storage bucket surface over REST and
// gRPC (AUD3-15), the counterpart to the object/media scenario in
// cases_storage.go. The bucket resource is created, read, listed, updated and
// deleted over each transport, with create/update mutation-parity steps, so a
// field one transport's transcode drops or invents is caught the same way the
// object surface is.
//
// storageBucketCMEKeys creates (idempotently) the two US CMEK keys the bucket
// scenario names, in the run's project, and returns their resource names. A
// bucket's default KMS key is a resolved reference (FC4-1), so the keys must
// exist before the bucket that names them.
func storageBucketCMEKeys(ctx context.Context, e *Env) (string, string, error) {
	ring := e.Resource("parity-ref-ring")
	base := fmt.Sprintf("/v1/projects/%s/locations/us", e.Cfg.Project)
	if _, err := e.Rest(ctx, http.MethodPost, base+"/keyRings?keyRingId="+ring, `{}`); err != nil && !statusIs(err, codes.AlreadyExists) {
		return "", "", fmt.Errorf("create CMEK key ring: %w", err)
	}
	ringName := fmt.Sprintf("projects/%s/locations/us/keyRings/%s", e.Cfg.Project, ring)
	names := make([]string, 0, 2)
	for _, id := range []string{e.Resource("parity-ref-key"), e.Resource("parity-ref-key2")} {
		if _, err := e.Rest(ctx, http.MethodPost, "/v1/"+ringName+"/cryptoKeys?cryptoKeyId="+id, `{"purpose":"ENCRYPT_DECRYPT"}`); err != nil && !statusIs(err, codes.AlreadyExists) {
			return "", "", fmt.Errorf("create CMEK key %s: %w", id, err)
		}
		names = append(names, ringName+"/cryptoKeys/"+id)
	}
	return names[0], names[1], nil
}

// gRPC is driven with the emulator's own storagepb stub (see cases_storage.go).
func storageBucketsScenario() Scenario {
	bkt := func(e *Env) string { return e.Resource("parity-bucket") }
	project := func(e *Env) string { return e.Cfg.Project }

	bucketBody := func(e *Env, kmsKey string) string {
		// The bucket is created with uniform bucket-level access on and public
		// access prevention enforced, so every create/get/list/update step below
		// compares a non-default iamConfiguration across the two transports
		// (DGD10). The hashicorp/google provider sends exactly this shape: the
		// iamConfiguration field on insert and in the PATCH body on update.
		// The lifecycle/CORS/CMEK blocks exercise the storage.v2 ↔ v1 mapping
		// (FC2-1/2/3).
		return fmt.Sprintf(`{"name":%q,"location":"US","storageClass":"STANDARD",`+
			`"lifecycle":{"rule":[{"action":{"type":"Delete"},"condition":{"age":30}}]},`+
			`"cors":[{"origin":["https://example.com"],"method":["GET"],"responseHeader":["Content-Type"],"maxAgeSeconds":3600}],`+
			`"encryption":{"defaultKmsKeyName":%q},`+
			`"iamConfiguration":{"uniformBucketLevelAccess":{"enabled":true},"publicAccessPrevention":"enforced"}}`, bkt(e), kmsKey)
	}
	createREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		key, _, err := storageBucketCMEKeys(ctx, e)
		if err != nil {
			return nil, err
		}
		return e.Rest(ctx, http.MethodPost, "/storage/v1/b?project="+url.QueryEscape(project(e)), bucketBody(e, key))
	}
	getREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodGet, "/storage/v1/b/"+bkt(e), "")
	}
	listREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodGet, "/storage/v1/b?project="+url.QueryEscape(project(e)), "")
	}
	updateREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		// The update toggles uniform bucket-level access off and public access
		// prevention back to inherited through the PATCH body — the write path
		// google_storage_bucket uses to change either setting. It also replaces
		// the lifecycle/CORS/CMEK blocks so their update path is compared.
		_, key2, err := storageBucketCMEKeys(ctx, e)
		if err != nil {
			return nil, err
		}
		return e.Rest(ctx, http.MethodPatch, "/storage/v1/b/"+bkt(e),
			`{"labels":{"marker":"v2"},`+
				`"lifecycle":{"rule":[{"action":{"type":"SetStorageClass","storageClass":"NEARLINE"},"condition":{"age":7}}]},`+
				`"cors":[{"origin":["*"],"method":["GET"],"maxAgeSeconds":60}],`+
				`"encryption":{"defaultKmsKeyName":`+strconv.Quote(key2)+`},`+
				`"iamConfiguration":{"uniformBucketLevelAccess":{"enabled":false},"publicAccessPrevention":"inherited"}}`)
	}
	deleteREST := func(ctx context.Context, e *Env) error {
		return e.RestDelete(ctx, "/storage/v1/b/"+bkt(e))
	}
	createGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		key, _, err := storageBucketCMEKeys(ctx, e)
		if err != nil {
			return nil, err
		}
		age := int32(30)
		var out *storagepb.Bucket
		err = storageDial(ctx, e, func(c storagepb.StorageClient) error {
			var cerr error
			out, cerr = c.CreateBucket(ctx, &storagepb.CreateBucketRequest{
				Parent:   "projects/" + project(e),
				BucketId: bkt(e),
				Bucket: &storagepb.Bucket{
					Location:     "US",
					StorageClass: "STANDARD",
					Lifecycle: &storagepb.Bucket_Lifecycle{Rule: []*storagepb.Bucket_Lifecycle_Rule{{
						Action:    &storagepb.Bucket_Lifecycle_Rule_Action{Type: "Delete"},
						Condition: &storagepb.Bucket_Lifecycle_Rule_Condition{AgeDays: &age},
					}}},
					Cors: []*storagepb.Bucket_Cors{{
						Origin:         []string{"https://example.com"},
						Method:         []string{"GET"},
						ResponseHeader: []string{"Content-Type"},
						MaxAgeSeconds:  3600,
					}},
					Encryption: &storagepb.Bucket_Encryption{DefaultKmsKey: key},
					IamConfig: &storagepb.Bucket_IamConfig{
						UniformBucketLevelAccess: &storagepb.Bucket_IamConfig_UniformBucketLevelAccess{Enabled: true},
						PublicAccessPrevention:   "enforced",
					},
				},
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
		_, key2, err := storageBucketCMEKeys(ctx, e)
		if err != nil {
			return nil, err
		}
		age := int32(7)
		var out *storagepb.Bucket
		err = storageDial(ctx, e, func(c storagepb.StorageClient) error {
			var cerr error
			out, cerr = c.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
				Bucket: &storagepb.Bucket{
					Name:   "projects/_/buckets/" + bkt(e),
					Labels: map[string]string{"marker": "v2"},
					Lifecycle: &storagepb.Bucket_Lifecycle{Rule: []*storagepb.Bucket_Lifecycle_Rule{{
						Action:    &storagepb.Bucket_Lifecycle_Rule_Action{Type: "SetStorageClass", StorageClass: "NEARLINE"},
						Condition: &storagepb.Bucket_Lifecycle_Rule_Condition{AgeDays: &age},
					}}},
					Cors: []*storagepb.Bucket_Cors{{
						Origin:        []string{"*"},
						Method:        []string{"GET"},
						MaxAgeSeconds: 60,
					}},
					Encryption: &storagepb.Bucket_Encryption{DefaultKmsKey: key2},
					IamConfig: &storagepb.Bucket_IamConfig{
						UniformBucketLevelAccess: &storagepb.Bucket_IamConfig_UniformBucketLevelAccess{Enabled: false},
						PublicAccessPrevention:   "inherited",
					},
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
					"labels",
					"lifecycle",
					"cors",
					"encryption",
					"iam_config.uniform_bucket_level_access.enabled",
					"iam_config.public_access_prevention",
				}},
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
//     `projectNumber`, `generation`); projectNumber folds to the proto
//     `project` (both rendered as the bare project number);
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
	delete(m, "generation")
	delete(m, "bucketId")
	// REST reports projectNumber (a bare number) where the proto reports
	// project ("projects/{number}"). Fold both to the bare number so the same
	// bucket compares equal across transports (FC2-5).
	if v, ok := m["projectNumber"]; ok {
		delete(m, "projectNumber")
		if _, ok := m["project"]; !ok {
			m["project"] = v
		}
	}
	if p, ok := m["project"].(string); ok {
		m["project"] = strings.TrimPrefix(p, "projects/")
	}
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
	// The REST iamConfiguration mirrors uniformBucketLevelAccess into the
	// deprecated bucketPolicyOnly alias; the proto Bucket.IamConfig has no such
	// field, so the alias carries no information the uniform flag does not.
	if ic, ok := m["iamConfig"].(map[string]any); ok {
		delete(ic, "bucketPolicyOnly")
	}
	// lifecycle/encryption member names differ between JSON v1 and
	// storage.v2: the lifecycle condition "age" is proto "ageDays", and
	// encryption "defaultKmsKeyName" is proto "defaultKmsKey".
	if lc, ok := m["lifecycle"].(map[string]any); ok {
		if rules, ok := lc["rule"].([]any); ok {
			for _, r := range rules {
				rm, _ := r.(map[string]any)
				cm, _ := rm["condition"].(map[string]any)
				if v, ok := cm["age"]; ok {
					delete(cm, "age")
					cm["ageDays"] = v
				}
			}
		}
	}
	if enc, ok := m["encryption"].(map[string]any); ok {
		if v, ok := enc["defaultKmsKeyName"]; ok {
			delete(enc, "defaultKmsKeyName")
			enc["defaultKmsKey"] = v
		}
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
