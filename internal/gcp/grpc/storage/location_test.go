package storage

import (
	"context"
	"testing"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestBucketToProtoCanonicalLocation pins the gRPC side of the TGM10
// canonicalisation: bucketToProto reads the same stored map the REST provider
// serves, so a legacy lowercase record and a custom dual-region must map to the
// same location/locationType a REST client sees.
func TestBucketToProtoCanonicalLocation(t *testing.T) {
	legacy := bucketToProto(map[string]any{
		"name":           "legacy-lower",
		"location":       "us-central1",
		"metageneration": "1",
	})
	if got := legacy.GetLocation(); got != "US-CENTRAL1" {
		t.Errorf("legacy location = %q, want US-CENTRAL1", got)
	}
	if got := legacy.GetLocationType(); got != "region" {
		t.Errorf("legacy locationType = %q, want region", got)
	}

	custom := bucketToProto(map[string]any{
		"name":                  "configurable-dual",
		"location":              "us",
		"metageneration":        "1",
		"customPlacementConfig": map[string]any{"dataLocations": []any{"us-central1", "us-east1"}},
	})
	if got := custom.GetLocation(); got != "US" {
		t.Errorf("custom-placement location = %q, want US", got)
	}
	if got := custom.GetLocationType(); got != "dual-region" {
		t.Errorf("custom-placement locationType = %q, want dual-region", got)
	}

	predefined := bucketToProto(map[string]any{
		"name":           "predefined-dual",
		"location":       "nam4",
		"metageneration": "1",
	})
	if got := predefined.GetLocationType(); got != "dual-region" {
		t.Errorf("NAM4 locationType = %q, want dual-region", got)
	}
}

// TestGRPCUpdateBucketRejectsLocationChange is the gRPC half of FC2-4: a
// masked UpdateBucket that changes the location is INVALID_ARGUMENT and leaves
// the bucket untouched. Resending the stored location (in any case, since GCS
// serves it uppercase) is a no-op and the rest of the mask still applies.
func TestGRPCUpdateBucketRejectsLocationChange(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-loc", "US")
	before, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucketLoc})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	for _, tc := range []struct {
		location string
		label    string
		mask     []string
	}{
		{"EU", "", []string{"location"}},
		{"europe-west1", "", []string{"location"}},
		{"nam4", "", []string{"*"}},
		// A rejected request must not apply the fields that came before the
		// location check either: the whole update is refused.
		{"EU", "storage", []string{"location", "labels"}},
	} {
		bucket := &storagepb.Bucket{Name: testBucketLoc, Location: tc.location}
		if tc.label != "" {
			bucket.Labels = map[string]string{"team": tc.label}
		}
		_, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
			Bucket:     bucket,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: tc.mask},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("UpdateBucket(location=%q, mask %v) err = %v, want InvalidArgument", tc.location, tc.mask, err)
		}
		got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucketLoc})
		if err != nil {
			t.Fatalf("GetBucket: %v", err)
		}
		if got.GetLocation() != "US" {
			t.Fatalf("rejected update of location=%q changed the bucket to %q", tc.location, got.GetLocation())
		}
		if len(got.GetLabels()) != 0 {
			t.Fatalf("rejected update of location=%q applied labels: %v", tc.location, got.GetLabels())
		}
		if got.GetMetageneration() != before.GetMetageneration() {
			t.Fatalf("rejected update of location=%q bumped the metageneration %d -> %d",
				tc.location, before.GetMetageneration(), got.GetMetageneration())
		}
	}

	// The stored location in a different case is a no-op; the labels in the
	// same request still apply.
	updated, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:     testBucketLoc,
			Location: " us ",
			Labels:   map[string]string{"team": "storage"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"location", "labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket(same location): %v", err)
	}
	if updated.GetLocation() != "US" {
		t.Errorf("same-location update = %q, want US", updated.GetLocation())
	}
	if updated.GetLabels()["team"] != "storage" {
		t.Errorf("same-location update did not apply labels: %v", updated.GetLabels())
	}
}

const testBucketLoc = "projects/_/buckets/bucket-loc"
