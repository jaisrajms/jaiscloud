package storage

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/model"
)

// insertErr inserts a bucket body and returns the error (nil on success).
func insertErr(p *Provider, body map[string]any) error {
	nr := bucketParams()
	nr.Params["body"] = body
	_, err := p.BucketsInsert(context.Background(), nr)
	return err
}

// wantInvalidArgument asserts err is the documented INVALID_ARGUMENT/400 shape.
func wantInvalidArgument(t *testing.T, label string, err error) {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
		t.Fatalf("%s err = %v, want 400 ProviderError", label, err)
	}
	if pe.Code != "InvalidArgument" {
		t.Errorf("%s code = %q, want InvalidArgument", label, pe.Code)
	}
}

// TestBucketsInsertRejectsInvalidStorageClass is TF5-1 over the provider core:
// Bucket.storageClass is the documented seven-value enum (Discovery
// schemas.Bucket.storageClass), so a create naming anything else — including a
// lowercase spelling of a valid class — is 400 and creates nothing. Real GCS
// rejected storageClass=BOGUS with 400 while the emulator accepted it with 200.
func TestBucketsInsertRejectsInvalidStorageClass(t *testing.T) {
	for _, sc := range []string{"BOGUS", "standard", "NEARLINE ", "STANDARD_CLASS", "REGIONAL1"} {
		p := newTestProvider()
		body := map[string]any{"name": "sc-invalid", "storageClass": sc}
		wantInvalidArgument(t, "insert storageClass="+sc, insertErr(p, body))
		nr := bucketParams()
		nr.Params["bucket"] = "sc-invalid"
		if _, err := p.BucketsGet(context.Background(), nr); err == nil {
			t.Errorf("insert storageClass=%q created the bucket despite the 400", sc)
		}
	}

	// The valid control: every documented value is accepted and read back.
	for _, sc := range []string{
		"STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE",
		"MULTI_REGIONAL", "REGIONAL", "DURABLE_REDUCED_AVAILABILITY",
	} {
		p := newTestProvider()
		inserted := insertBucket(t, p, map[string]any{"name": "sc-valid", "storageClass": sc})
		if got, _ := inserted["storageClass"].(string); got != sc {
			t.Errorf("insert storageClass=%q read back %q", sc, got)
		}
	}

	// An absent storageClass keeps the GCS default instead of being rejected.
	p := newTestProvider()
	if got, _ := insertBucket(t, p, map[string]any{"name": "sc-default"})["storageClass"].(string); got != "STANDARD" {
		t.Errorf("default storageClass = %q, want STANDARD", got)
	}
}

// TestBucketsInsertRejectsInvalidLocation is the location half of TF5-1: GCS
// documents the location set (https://cloud.google.com/storage/docs/locations)
// and rejects a value that is not a location code with 400. Discovery declares
// Bucket.location as a string with no enum, so the emulator validates the
// documented grammar (see bucket_validation.go) — the invalid bucket name the
// lifecycle case already exercised is not the only create-time 400.
func TestBucketsInsertRejectsInvalidLocation(t *testing.T) {
	for _, loc := range []string{
		"BOGUS", "US-CENTRAL", "NOT-A-LOCATION", "US-CENTRAL1-", "GLOBAL", "EUROPE", "US CENTRAL1",
	} {
		p := newTestProvider()
		body := map[string]any{"name": "loc-invalid", "location": loc}
		wantInvalidArgument(t, "insert location="+loc, insertErr(p, body))
	}

	// The valid control: the documented multi-region, predefined dual-region,
	// regional, zonal and custom dual-region spellings are all accepted and
	// served in canonical uppercase form.
	for _, tc := range []struct{ in, want string }{
		{"US", "US"},
		{"eu", "EU"},
		{"ASIA", "ASIA"},
		{"NAM4", "NAM4"},
		{"EUR4", "EUR4"},
		{"us-central1", "US-CENTRAL1"},
		{"northamerica-northeast1", "NORTHAMERICA-NORTHEAST1"},
		{"australia-southeast1", "AUSTRALIA-SOUTHEAST1"},
		{"europe-west12", "EUROPE-WEST12"},
		{"us-central1-a", "US-CENTRAL1-A"},
		{"us-central1+us-east1", "US-CENTRAL1+US-EAST1"},
	} {
		p := newTestProvider()
		inserted := insertBucket(t, p, map[string]any{"name": "loc-valid", "location": tc.in})
		if got, _ := inserted["location"].(string); got != tc.want {
			t.Errorf("insert location=%q read back %q, want %q", tc.in, got, tc.want)
		}
	}

	// An absent location still defaults to the US multi-region.
	p := newTestProvider()
	if got, _ := insertBucket(t, p, map[string]any{"name": "loc-default"})["location"].(string); got != "US" {
		t.Errorf("default location = %q, want US", got)
	}
}

// TestBucketsUpdateRejectsInvalidStorageClass: the default storage class is
// mutable (Discovery lists storageClass on buckets.patch/update), so a patch
// naming a value outside the enum is the same INVALID_ARGUMENT/400 as create
// and leaves the bucket (including its metageneration) untouched.
func TestBucketsUpdateRejectsInvalidStorageClass(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertBucket(t, p, map[string]any{"name": "sc-patch", "storageClass": "STANDARD"})
	before := getBucket(t, p, "sc-patch")

	nr := bucketParams()
	nr.Params["bucket"] = "sc-patch"
	nr.Params["body"] = map[string]any{"storageClass": "BOGUS"}
	_, err := p.BucketsUpdate(ctx, nr)
	wantInvalidArgument(t, "patch storageClass=BOGUS", err)

	after := getBucket(t, p, "sc-patch")
	if got, _ := after["storageClass"].(string); got != "STANDARD" {
		t.Errorf("rejected patch changed storageClass to %q, want STANDARD", got)
	}
	if after["metageneration"] != before["metageneration"] {
		t.Errorf("rejected patch bumped metageneration %v -> %v", before["metageneration"], after["metageneration"])
	}

	// The valid control: a documented class still applies.
	if got, _ := patchBucket(t, p, "sc-patch", map[string]any{"storageClass": "NEARLINE"})["storageClass"].(string); got != "NEARLINE" {
		t.Errorf("patch storageClass=NEARLINE read back %q", got)
	}
}

// TestBucketValidationRejectsAre400 pins the two helpers directly, so the rule
// stays testable without going through a provider instance.
func TestBucketValidationRejectsAre400(t *testing.T) {
	wantInvalidArgument(t, "ValidateBucketStorageClass", ValidateBucketStorageClass("BOGUS"))
	wantInvalidArgument(t, "ValidateBucketLocation", ValidateBucketLocation("BOGUS"))
	for _, loc := range []string{"", "US", "EU", "ASIA", "NAM4", "ASIA1", "us-central1", "US-CENTRAL1"} {
		if err := ValidateBucketLocation(loc); err != nil {
			t.Errorf("ValidateBucketLocation(%q) = %v, want nil", loc, err)
		}
	}
	if err := ValidateBucketStorageClass(""); err != nil {
		t.Errorf("ValidateBucketStorageClass(\"\") = %v, want nil (default STANDARD)", err)
	}
}
