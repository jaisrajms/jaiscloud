package storage

import (
	"context"
	"testing"
	"time"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
	"jaiscloud/internal/gcp/resource"

	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestGRPCBucketLifecycleCorsEncryptionRoundTrip covers FC2-1/FC2-2/FC2-3:
// lifecycle, CORS and the CMEK default key are stored on create and update and
// served back on the gRPC surface, using the same bucket record REST v1 writes.
func TestGRPCBucketLifecycleCorsEncryptionRoundTrip(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	age := int32(30)
	isLive := true
	kmsKey := "projects/test-project/locations/us/keyRings/ring/cryptoKeys/key"

	created, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-config",
		Bucket: &storagepb.Bucket{
			Project:  "projects/test-project",
			Location: "US",
			Lifecycle: &storagepb.Bucket_Lifecycle{Rule: []*storagepb.Bucket_Lifecycle_Rule{{
				Action: &storagepb.Bucket_Lifecycle_Rule_Action{
					Type:         "SetStorageClass",
					StorageClass: "NEARLINE",
				},
				Condition: &storagepb.Bucket_Lifecycle_Rule_Condition{
					AgeDays:             &age,
					IsLive:              &isLive,
					MatchesPrefix:       []string{"logs/"},
					MatchesSuffix:       []string{".tmp"},
					CreatedBefore:       &date.Date{Year: 2025, Month: 1, Day: 15},
					MatchesStorageClass: []string{"STANDARD"},
				},
			}}},
			Cors: []*storagepb.Bucket_Cors{{
				Origin:         []string{"https://example.com"},
				Method:         []string{"GET", "PUT"},
				ResponseHeader: []string{"Content-Type"},
				MaxAgeSeconds:  3600,
			}},
			Encryption: &storagepb.Bucket_Encryption{DefaultKmsKey: kmsKey},
		},
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	name := created.GetName()

	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: name})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	rule := got.GetLifecycle().GetRule()
	if len(rule) != 1 {
		t.Fatalf("lifecycle rules = %d, want 1", len(rule))
	}
	if a := rule[0].GetAction(); a.GetType() != "SetStorageClass" || a.GetStorageClass() != "NEARLINE" {
		t.Errorf("lifecycle action = %+v, want SetStorageClass/NEARLINE", a)
	}
	c := rule[0].GetCondition()
	if c.GetAgeDays() != 30 || !c.GetIsLive() || len(c.GetMatchesPrefix()) != 1 || len(c.GetMatchesSuffix()) != 1 {
		t.Errorf("lifecycle condition = %+v, want age 30/live/prefix/suffix", c)
	}
	if c.GetCreatedBefore().GetYear() != 2025 || c.GetCreatedBefore().GetMonth() != 1 || c.GetCreatedBefore().GetDay() != 15 {
		t.Errorf("createdBefore = %+v, want 2025-01-15", c.GetCreatedBefore())
	}
	if cors := got.GetCors(); len(cors) != 1 || cors[0].GetMaxAgeSeconds() != 3600 ||
		len(cors[0].GetOrigin()) != 1 || len(cors[0].GetMethod()) != 2 || len(cors[0].GetResponseHeader()) != 1 {
		t.Errorf("cors = %+v, want the created rule", cors)
	}
	if got.GetEncryption().GetDefaultKmsKey() != kmsKey {
		t.Errorf("encryption.default_kms_key = %q, want %q", got.GetEncryption().GetDefaultKmsKey(), kmsKey)
	}

	// Masked updates replace the blocks; a masked clear removes them.
	age = 7
	updated, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name: name,
			Lifecycle: &storagepb.Bucket_Lifecycle{Rule: []*storagepb.Bucket_Lifecycle_Rule{{
				Action:    &storagepb.Bucket_Lifecycle_Rule_Action{Type: "Delete"},
				Condition: &storagepb.Bucket_Lifecycle_Rule_Condition{AgeDays: &age},
			}}},
			Cors: []*storagepb.Bucket_Cors{{
				Origin:        []string{"*"},
				Method:        []string{"GET"},
				MaxAgeSeconds: 60,
			}},
			Encryption: &storagepb.Bucket_Encryption{DefaultKmsKey: kmsKey + "-2"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"lifecycle", "cors", "encryption"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket: %v", err)
	}
	if rule := updated.GetLifecycle().GetRule(); len(rule) != 1 || rule[0].GetAction().GetType() != "Delete" || rule[0].GetCondition().GetAgeDays() != 7 {
		t.Errorf("lifecycle after update = %+v, want Delete/7", rule)
	}
	if cors := updated.GetCors(); len(cors) != 1 || cors[0].GetMaxAgeSeconds() != 60 {
		t.Errorf("cors after update = %+v, want the new rule", cors)
	}
	if updated.GetEncryption().GetDefaultKmsKey() != kmsKey+"-2" {
		t.Errorf("encryption after update = %q, want the new key", updated.GetEncryption().GetDefaultKmsKey())
	}

	cleared, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket:     &storagepb.Bucket{Name: name},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"lifecycle", "cors", "encryption"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket(clear): %v", err)
	}
	if cleared.GetLifecycle() != nil || cleared.GetCors() != nil || cleared.GetEncryption() != nil {
		t.Errorf("cleared blocks = lifecycle %+v / cors %+v / encryption %+v, want nil",
			cleared.GetLifecycle(), cleared.GetCors(), cleared.GetEncryption())
	}
}

// TestGRPCBucketSoftDeletePolicyAndProject covers FC2-17 and FC2-5: a
// soft-delete policy supplied on gRPC is stored and served back, and the
// bucket's project is rendered from the owning project in the output form real
// GCS uses (project number).
func TestGRPCBucketSoftDeletePolicyAndProject(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-soft-delete",
		Bucket: &storagepb.Bucket{
			Project:          "projects/test-project",
			Location:         "US",
			SoftDeletePolicy: &storagepb.Bucket_SoftDeletePolicy{RetentionDuration: durationpb.New(14 * 24 * time.Hour)},
		},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: "projects/_/buckets/bucket-soft-delete"})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if d := got.GetSoftDeletePolicy().GetRetentionDuration().AsDuration(); d != 14*24*time.Hour {
		t.Errorf("soft_delete_policy.retention_duration = %v, want 336h", d)
	}
	if got.GetSoftDeletePolicy().GetEffectiveTime() == nil {
		t.Error("soft_delete_policy.effective_time not set")
	}
	wantProject := resource.ResourceID(resource.ProjectNumber("test-project"))("project", "")
	if got.GetProject() != wantProject {
		t.Errorf("project = %q, want %q", got.GetProject(), wantProject)
	}

	// An update applies the new retention duration.
	updated, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:             got.GetName(),
			SoftDeletePolicy: &storagepb.Bucket_SoftDeletePolicy{RetentionDuration: durationpb.New(7 * 24 * time.Hour)},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"soft_delete_policy"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket(soft_delete_policy): %v", err)
	}
	if d := updated.GetSoftDeletePolicy().GetRetentionDuration().AsDuration(); d != 7*24*time.Hour {
		t.Errorf("soft_delete_policy after update = %v, want 168h", d)
	}
}

// TestGRPCBucketOutputOnlyPolicyMembersIgnored covers FC2-16: a caller-supplied
// retention_policy.is_locked/effective_time and soft_delete_policy.effective_time
// are ignored, while the server keeps the canonical values.
func TestGRPCBucketOutputOnlyPolicyMembersIgnored(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-output-only",
		Bucket: &storagepb.Bucket{
			Project:  "projects/test-project",
			Location: "US",
			RetentionPolicy: &storagepb.Bucket_RetentionPolicy{
				RetentionDuration: durationpb.New(3600 * time.Second),
				IsLocked:          true,
				EffectiveTime:     timestamppb.New(time.Date(2031, 2, 3, 4, 5, 6, 0, time.UTC)),
			},
			SoftDeletePolicy: &storagepb.Bucket_SoftDeletePolicy{
				RetentionDuration: durationpb.New(7 * 24 * time.Hour),
				EffectiveTime:     timestamppb.New(time.Date(2031, 2, 3, 4, 5, 6, 0, time.UTC)),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	rp := created.GetRetentionPolicy()
	if rp.GetIsLocked() {
		t.Error("retention_policy.is_locked was accepted from the caller")
	}
	if rp.GetEffectiveTime().AsTime().Year() == 2031 {
		t.Error("retention_policy.effective_time was copied from the caller")
	}
	if created.GetSoftDeletePolicy().GetEffectiveTime().AsTime().Year() == 2031 {
		t.Error("soft_delete_policy.effective_time was copied from the caller")
	}
}

// TestGRPCBucketEtagPrecondition covers FC2-19: Bucket.etag on UpdateBucket is
// an optimistic-concurrency precondition; a stale etag is ABORTED and a current
// one succeeds.
func TestGRPCBucketEtagPrecondition(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-etag",
		Bucket:   &storagepb.Bucket{Project: "projects/test-project", Location: "US"},
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if created.GetEtag() == "" {
		t.Fatal("created bucket has no etag")
	}

	update := func(etag string) error {
		_, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
			Bucket: &storagepb.Bucket{
				Name:   created.GetName(),
				Etag:   etag,
				Labels: map[string]string{"env": "test"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		})
		return err
	}
	if err := update(created.GetEtag()); err != nil {
		t.Fatalf("UpdateBucket with the current etag: %v", err)
	}
	// The metageneration (and therefore the etag) advanced; the old etag is now
	// stale and the update must be rejected.
	if err := update(created.GetEtag()); status.Code(err) != codes.Aborted {
		t.Fatalf("UpdateBucket with a stale etag = %v, want Aborted", err)
	}
}
