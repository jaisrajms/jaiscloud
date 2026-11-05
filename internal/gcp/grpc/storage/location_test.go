package storage

import (
	"testing"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

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

// TestApplyBucketMaskCanonicalisesLocation covers UpdateBucket: a masked
// lowercase location is stored in the canonical uppercase form.
func TestApplyBucketMaskCanonicalisesLocation(t *testing.T) {
	meta := map[string]any{"location": "US"}
	mask := &fieldmaskpb.FieldMask{Paths: []string{"location"}}
	if err := applyBucketMask(meta, &storagepb.Bucket{Location: "europe-west1"}, mask); err != nil {
		t.Fatalf("applyBucketMask: %v", err)
	}
	if got, _ := meta["location"].(string); got != "EUROPE-WEST1" {
		t.Errorf("masked location = %q, want EUROPE-WEST1", got)
	}
}
