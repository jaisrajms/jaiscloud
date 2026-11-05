package storage

import (
	"context"
	"strings"
	"testing"
)

// locationOf / locationTypeOf pull the two fields out of a bucket or
// storageLayout response body.
func locationOf(data map[string]any) string {
	s, _ := data["location"].(string)
	return s
}

func locationTypeOf(data map[string]any) string {
	s, _ := data["locationType"].(string)
	return s
}

// storageLayout fetches buckets.getStorageLayout for a bucket.
func storageLayout(t *testing.T, p *Provider, bucket string) map[string]any {
	t.Helper()
	nr := bucketParams()
	nr.Params["bucket"] = bucket
	resp, err := p.BucketsGetStorageLayout(context.Background(), nr)
	if err != nil {
		t.Fatalf("getStorageLayout(%s): %v", bucket, err)
	}
	return resp.Data
}

// TestBucketInsertCanonicalisesLocation is the TGM10 regression: GCS is
// case-insensitive about a bucket location and always serves it uppercase.
// google_storage_bucket's `location` is Required+ForceNew with
// StateFunc strings.ToUpper, so a bucket created with "us-central1" must read
// back "US-CENTRAL1" or every plan after apply proposes a replacement.
func TestBucketInsertCanonicalisesLocation(t *testing.T) {
	tests := []struct {
		name     string
		location string // empty means "not supplied"
		want     string
		wantType string
	}{
		{"lowercase region", "us-central1", "US-CENTRAL1", "region"},
		{"mixed-case dual region", "Nam4", "NAM4", "dual-region"},
		{"surrounding whitespace", "  eu  ", "EU", "multi-region"},
		{"already canonical", "US-CENTRAL1", "US-CENTRAL1", "region"},
		{"default when absent", "", "US", "multi-region"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestProvider()
			name := "loc-" + strings.ToLower(tc.want)
			body := map[string]any{"name": name}
			if tc.location != "" {
				body["location"] = tc.location
			}
			inserted := insertBucket(t, p, body)
			if got := locationOf(inserted); got != tc.want {
				t.Errorf("insert location = %q, want %q", got, tc.want)
			}
			if got := locationTypeOf(inserted); got != tc.wantType {
				t.Errorf("insert locationType = %q, want %q", got, tc.wantType)
			}

			fetched := getBucket(t, p, name)
			if got := locationOf(fetched); got != tc.want {
				t.Errorf("GET location = %q, want %q", got, tc.want)
			}
			if got := locationTypeOf(fetched); got != tc.wantType {
				t.Errorf("GET locationType = %q, want %q", got, tc.wantType)
			}

			layout := storageLayout(t, p, name)
			if got := locationOf(layout); got != tc.want {
				t.Errorf("storageLayout location = %q, want %q", got, tc.want)
			}
			if got := locationTypeOf(layout); got != tc.wantType {
				t.Errorf("storageLayout locationType = %q, want %q", got, tc.wantType)
			}
		})
	}
}

// TestBucketsUpdateCanonicalisesLocation covers a PATCH carrying a lowercase
// location: the response (and a fresh GET) must report the canonical form.
func TestBucketsUpdateCanonicalisesLocation(t *testing.T) {
	p := newTestProvider()
	insertBucket(t, p, map[string]any{"name": "loc-update", "location": "US"})

	patched := patchBucket(t, p, "loc-update", map[string]any{"location": " europe-west1 "})
	if got := locationOf(patched); got != "EUROPE-WEST1" {
		t.Errorf("PATCH location = %q, want EUROPE-WEST1", got)
	}
	if got := locationTypeOf(patched); got != "region" {
		t.Errorf("PATCH locationType = %q, want region", got)
	}
	if got := locationOf(getBucket(t, p, "loc-update")); got != "EUROPE-WEST1" {
		t.Errorf("GET after PATCH location = %q, want EUROPE-WEST1", got)
	}
}

// TestEnsureBucketCanonicalisesLocation covers the internal provisioning path
// (Dataproc staging, Cloud Functions sources), which passes a service region.
func TestEnsureBucketCanonicalisesLocation(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if err := p.EnsureBucket(ctx, "proj", "ens-lower", "us-central1"); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
	if got := locationOf(getBucket(t, p, "ens-lower")); got != "US-CENTRAL1" {
		t.Errorf("EnsureBucket location = %q, want US-CENTRAL1", got)
	}
}

// TestBucketLegacyLowercaseRecordRendersCanonical pins the read-time repair: the
// stores hold persisted JSON blobs, so a record written before the
// canonicalisation must read back uppercase without a stored migration.
func TestBucketLegacyLowercaseRecordRendersCanonical(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	legacy := map[string]any{
		"name":         "legacy-lower",
		"location":     "us-central1",
		"storageClass": "STANDARD",
		// A custom placement written before dataLocations were canonicalised.
		"customPlacementConfig": map[string]any{"dataLocations": []any{"us-east1", "us-west1"}},
	}
	if err := p.objects.CreateBucket(ctx, "proj", "legacy-lower", legacy); err != nil {
		t.Fatalf("seed legacy bucket: %v", err)
	}

	data := getBucket(t, p, "legacy-lower")
	if got := locationOf(data); got != "US-CENTRAL1" {
		t.Errorf("legacy GET location = %q, want US-CENTRAL1", got)
	}
	// A custom placement makes the bucket a dual-region, custom or predefined.
	if got := locationTypeOf(data); got != "dual-region" {
		t.Errorf("legacy GET locationType = %q, want dual-region", got)
	}
	assertJSONEqual(t, "legacy customPlacementConfig", data["customPlacementConfig"],
		map[string]any{"dataLocations": []any{"US-EAST1", "US-WEST1"}})

	layout := storageLayout(t, p, "legacy-lower")
	if got := locationOf(layout); got != "US-CENTRAL1" {
		t.Errorf("legacy storageLayout location = %q, want US-CENTRAL1", got)
	}
}

// TestBucketCustomPlacementCanonicalisesDataLocations covers create: the
// dataLocations region codes are served uppercase and the bucket's
// locationType is dual-region even though its location field says US.
func TestBucketCustomPlacementCanonicalisesDataLocations(t *testing.T) {
	p := newTestProvider()
	inserted := insertBucket(t, p, map[string]any{
		"name":     "custom-placement",
		"location": "us",
		"customPlacementConfig": map[string]any{
			"dataLocations": []any{"us-central1", "us-east1"},
		},
	})
	assertJSONEqual(t, "create customPlacementConfig", inserted["customPlacementConfig"],
		map[string]any{"dataLocations": []any{"US-CENTRAL1", "US-EAST1"}})
	if got := locationOf(inserted); got != "US" {
		t.Errorf("create location = %q, want US", got)
	}
	if got := locationTypeOf(inserted); got != "dual-region" {
		t.Errorf("create locationType = %q, want dual-region", got)
	}

	fetched := getBucket(t, p, "custom-placement")
	assertJSONEqual(t, "GET customPlacementConfig", fetched["customPlacementConfig"],
		map[string]any{"dataLocations": []any{"US-CENTRAL1", "US-EAST1"}})
	if got := locationTypeOf(fetched); got != "dual-region" {
		t.Errorf("GET locationType = %q, want dual-region", got)
	}
	if got := locationTypeOf(storageLayout(t, p, "custom-placement")); got != "dual-region" {
		t.Errorf("storageLayout locationType = %q, want dual-region", got)
	}
}

// TestBucketLocationType pins the GCS locationType classification: multi-region
// codes, predefined dual-region codes, single regions, and the configurable
// dual region a customPlacementConfig expresses.
func TestBucketLocationType(t *testing.T) {
	tests := []struct {
		location           string
		hasCustomPlacement bool
		want               string
	}{
		{"US", false, "multi-region"},
		{"eu", false, "multi-region"},
		{"ASIA", false, "multi-region"},
		{"", false, "multi-region"},
		{"NAM4", false, "dual-region"},
		{"nam4", false, "dual-region"},
		{"EUR4", false, "dual-region"},
		{"EUR5", false, "dual-region"},
		{"EUR7", false, "dual-region"},
		{"EUR8", false, "dual-region"},
		{"ASIA1", false, "dual-region"},
		{"us-central1", false, "region"},
		{"US-CENTRAL1", false, "region"},
		{"US", true, "dual-region"},
		{"US-CENTRAL1", true, "dual-region"},
	}
	for _, tc := range tests {
		if got := BucketLocationTypeFor(tc.location, tc.hasCustomPlacement); got != tc.want {
			t.Errorf("BucketLocationTypeFor(%q, %v) = %q, want %q",
				tc.location, tc.hasCustomPlacement, got, tc.want)
		}
	}
	// The location-only entry point stays available and case-insensitive.
	if got := BucketLocationType("us-central1"); got != "region" {
		t.Errorf("BucketLocationType(us-central1) = %q, want region", got)
	}
	if got := BucketLocationType("NAM4"); got != "dual-region" {
		t.Errorf("BucketLocationType(NAM4) = %q, want dual-region", got)
	}
}
