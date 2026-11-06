package gcplifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/gcp/crypto"
	grpcstorage "jaiscloud/internal/gcp/grpc/storage"
	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/store/gcs"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const (
	gcsTypeNotification = "gcs_notification"
	gcsTypeBucketIAM    = "gcs_bucket_iam"
	gcsTypeObjectIAM    = "gcs_object_iam"
)

// storageManifest is the FC5 ownership contract for a GCS bucket: real GCS
// refuses to delete a bucket that still has object generations
// (reject-if-nonempty), and deletes the bucket's notification configs and IAM
// policy with the bucket (cascade).
var storageManifest = manifest{
	parent: "storage bucket",
	policy: Cascade,
	children: []childSpec{
		{"objects", RejectIfNonEmpty},
		{"notifications", Cascade},
		{"bucketIAM", Cascade},
	},
	note: "objects (including noncurrent generations) block the delete with 409; soft-deleted generations do not block it but go with the bucket",
}

type storageAdapter struct {
	prov      *storageprovider.Provider
	objects   gcs.ObjectStore
	resources store.ResourceStore
	client    storagepb.StorageClient
	project   string
}

func newStorageAdapter(t *testing.T) adapter {
	t.Helper()
	objects := gcs.NewMemoryObjectStore()
	resources := store.NewMemoryResourceStore()
	blobs := blobfs.NewMemoryBlobStore()
	prov := storageprovider.New(objects, resources, blobs, crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore()))
	conn := serveGRPC(t, func(srv *grpc.Server) {
		storagepb.RegisterStorageServer(srv, grpcstorage.NewService(objects, resources, prov, "proj"))
	})
	return &storageAdapter{
		prov:      prov,
		objects:   objects,
		resources: resources,
		client:    storagepb.NewStorageClient(conn),
		project:   "proj",
	}
}

func (a *storageAdapter) name() string { return "storage bucket" }

func (a *storageAdapter) create(ctx context.Context, tr transport, id string) (string, error) {
	if tr == grpcT {
		b, err := a.client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
			Parent:   "projects/_",
			BucketId: id,
			Bucket:   &storagepb.Bucket{Project: "projects/" + a.project},
		})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", b.GetMetageneration()), nil
	}
	resp, err := a.prov.BucketsInsert(ctx, a.bucketNR(id, map[string]any{"name": id}))
	if err != nil {
		return "", err
	}
	metageneration, _ := resp.Data["metageneration"].(string)
	return metageneration, nil
}

func (a *storageAdapter) get(ctx context.Context, tr transport, id string) (bool, error) {
	if tr == grpcT {
		_, err := a.client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: "projects/_/buckets/" + id})
		if err != nil {
			if isNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	_, err := a.prov.BucketsGet(ctx, a.bucketNR(id, nil))
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (a *storageAdapter) list(ctx context.Context, tr transport) ([]string, error) {
	if tr == grpcT {
		resp, err := a.client.ListBuckets(ctx, &storagepb.ListBucketsRequest{Parent: "projects/" + a.project})
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(resp.GetBuckets()))
		for _, b := range resp.GetBuckets() {
			// "projects/_/buckets/{name}"
			out = append(out, shortName(b.GetName()))
		}
		return out, nil
	}
	resp, err := a.prov.BucketsList(ctx, &model.NormalizedRequest{AccountID: a.project, Params: map[string]any{}})
	if err != nil {
		return nil, err
	}
	items, _ := resp.Data["items"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		m, _ := it.(map[string]any)
		if name, _ := m["name"].(string); name != "" {
			out = append(out, name)
		}
	}
	return out, nil
}

func (a *storageAdapter) update(ctx context.Context, tr transport, id string) error {
	if tr == grpcT {
		_, err := a.client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
			Bucket:     &storagepb.Bucket{Name: "projects/_/buckets/" + id, Labels: map[string]string{"lifecycle": "update"}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		})
		return err
	}
	_, err := a.prov.BucketsUpdate(ctx, a.bucketNR(id, map[string]any{"labels": map[string]any{"lifecycle": "update"}}))
	return err
}

func (a *storageAdapter) remove(ctx context.Context, tr transport, id string) error {
	if tr == grpcT {
		_, err := a.client.DeleteBucket(ctx, &storagepb.DeleteBucketRequest{Name: "projects/_/buckets/" + id})
		return err
	}
	_, err := a.prov.BucketsDelete(ctx, a.bucketNR(id, nil))
	return err
}

func (a *storageAdapter) blockedDelete(ctx context.Context, tr transport, id string) []blockedResult {
	// GCS has no etag precondition on bucket delete in the emulator; the
	// documented block is a non-empty bucket. Stage an object, attempt the
	// delete, and leave the bucket as it was.
	if err := a.putObject(ctx, id, "blocked-obj"); err != nil {
		return []blockedResult{{name: "stage-object", err: err, wantCode: "FailedPrecondition"}}
	}
	want := "bucketNotEmpty"
	if tr == grpcT {
		want = "FailedPrecondition"
	}
	res := []blockedResult{{name: "non-empty", err: a.remove(ctx, tr, id), wantCode: want}}
	if err := a.objects.DeleteObjectMeta(ctx, id, "blocked-obj"); err != nil {
		res = append(res, blockedResult{name: "cleanup-object", err: err, wantCode: ""})
	}
	return res
}

func (a *storageAdapter) populate(ctx context.Context, id string) error {
	if err := a.putObject(ctx, id, "obj1"); err != nil {
		return err
	}
	// A notification config is a bucket child in the control-plane store.
	if _, err := a.prov.NotificationsInsert(ctx, &model.NormalizedRequest{
		AccountID: a.project,
		Params: map[string]any{
			"bucket": id,
			"body":   map[string]any{"topic": "projects/" + a.project + "/topics/lc"},
		},
	}); err != nil {
		return err
	}
	// A bucket IAM policy is stored under the bucket name.
	return a.resources.Upsert(ctx, a.project, store.GlobalRegion, store.ResourceEntry{
		Type: gcsTypeBucketIAM,
		ID:   id,
		Data: []byte(`{"bindings":[]}`),
	})
}

func (a *storageAdapter) counts(ctx context.Context, id string) (map[string]childCount, error) {
	out := map[string]childCount{}
	objects, err := a.objects.ListObjects(ctx, id)
	if err != nil && !errors.Is(err, gcs.ErrNoSuchBucket) {
		return nil, err
	}
	out["objects"] = childCount{total: len(objects), attached: len(objects)}

	notifications, err := a.resources.List(ctx, a.project, store.GlobalRegion, gcsTypeNotification, id+"/")
	if err != nil {
		return nil, err
	}
	out["notifications"] = childCount{total: len(notifications), attached: len(notifications)}

	_, err = a.resources.Get(ctx, a.project, store.GlobalRegion, gcsTypeBucketIAM, id)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	total := 0
	if err == nil {
		total = 1
	}
	out["bucketIAM"] = childCount{total: total, attached: total}
	return out, nil
}

func (a *storageAdapter) clearRejecting(ctx context.Context, id string) error {
	objects, err := a.objects.ListObjects(ctx, id)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil
		}
		return err
	}
	for _, obj := range objects {
		if err := a.objects.DeleteObjectMeta(ctx, id, obj.Name); err != nil && !errors.Is(err, gcs.ErrNoSuchObject) {
			return err
		}
	}
	return nil
}

// extra pins the object-level ownership edge: an object's IAM policy dies with
// the object, so re-creating an object of the same name does not resurrect the
// old policy.
func (a *storageAdapter) extra(t *testing.T, ctx context.Context, tr transport, id string) {
	t.Helper()
	if _, err := a.create(ctx, tr, id); err != nil {
		t.Fatalf("create bucket before object recreate: %v", err)
	}
	if err := a.putObject(ctx, id, "iam-obj"); err != nil {
		t.Fatalf("put object: %v", err)
	}
	if err := a.resources.Upsert(ctx, a.project, store.GlobalRegion, store.ResourceEntry{
		Type: gcsTypeObjectIAM,
		ID:   id + "/iam-obj",
		Data: []byte(`{"bindings":[]}`),
	}); err != nil {
		t.Fatalf("stage object IAM: %v", err)
	}
	if _, err := a.prov.ObjectsDelete(ctx, &model.NormalizedRequest{
		AccountID: a.project,
		Params:    map[string]any{"bucket": id, "object": "iam-obj"},
	}); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	if _, err := a.resources.Get(ctx, a.project, store.GlobalRegion, gcsTypeObjectIAM, id+"/iam-obj"); err == nil {
		t.Errorf("object IAM policy survived the object delete; a same-name recreate would inherit it")
	} else if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get object IAM: %v", err)
	}
}

func (a *storageAdapter) putObject(ctx context.Context, bucket, object string) error {
	return a.objects.PutObjectMeta(ctx, bucket, object, gcs.ObjectMeta{
		Bucket:         bucket,
		Name:           object,
		Generation:     "1",
		Metageneration: "1",
		StorageClass:   "STANDARD",
		TimeCreated:    time.Now(),
	})
}

// bucketNR builds a bucket-scoped NormalizedRequest for the REST provider.
func (a *storageAdapter) bucketNR(bucket string, body map[string]any) *model.NormalizedRequest {
	params := map[string]any{}
	if bucket != "" {
		params["bucket"] = bucket
	}
	if body != nil {
		params["body"] = body
	}
	return &model.NormalizedRequest{AccountID: a.project, Params: params, ResourceID: resource.ResourceID(a.project)}
}
