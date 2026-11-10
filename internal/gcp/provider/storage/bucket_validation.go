package storage

import (
	"regexp"
	"strings"

	"jaiscloud/internal/model"
)

// Bucket create/patch field validation.
//
// The JSON API Discovery document (storage/v1, schemas.Bucket) declares
// storageClass and location as plain strings and iamConfiguration.
// publicAccessPrevention likewise, but the descriptions and the official docs
// close each set:
//
//   - storageClass: "Values include MULTI_REGIONAL, REGIONAL, STANDARD, NEARLINE,
//     COLDLINE, ARCHIVE, and DURABLE_REDUCED_AVAILABILITY" (Bucket.storageClass;
//     https://cloud.google.com/storage/docs/storage-classes). Those seven names
//     are the accepted set; the API is case-sensitive, unlike a bucket location.
//   - iamConfiguration.publicAccessPrevention: "Currently, 'inherited' and
//     'enforced' are supported" — validated by MergeIAMConfig on both create and
//     patch (bucket_iam.go).
//   - location: the docs point at https://cloud.google.com/storage/docs/locations
//     for "the authoritative list"; Discovery carries no enum. The list grows
//     every quarter, so the emulator validates the documented *grammar* rather
//     than freezing a region catalog: a multi-region code (US/EU/ASIA), a
//     predefined dual-region code (ASIA1/EUR4/EUR5/EUR7/EUR8/NAM4), a single
//     region name (lowercase words, the last ending in a number — us-central1,
//     northamerica-northeast1, australia-southeast1, europe-west12), an optional
//     Rapid Bucket zone suffix (us-central1-a), or a custom dual-region pair
//     (us-central1+us-east1). A value that is not shaped like any of those
//     ("BOGUS", "US-CENTRAL", a trailing space) is INVALID_ARGUMENT/400, which
//     is the boundary: the grammar is checked, region existence is not.

// bucketStorageClasses is the documented Bucket.storageClass enum (see the file
// comment). Keys are the canonical uppercase spellings.
var bucketStorageClasses = map[string]bool{
	"STANDARD":                     true,
	"NEARLINE":                     true,
	"COLDLINE":                     true,
	"ARCHIVE":                      true,
	"MULTI_REGIONAL":               true,
	"REGIONAL":                     true,
	"DURABLE_REDUCED_AVAILABILITY": true,
}

// bucketRegionPattern matches a single-region code in canonical uppercase form:
// one or more words separated by hyphens, the last of which ends in a number,
// with an optional trailing one-letter zone for a zonal Rapid Bucket
// (US-CENTRAL1, NORTHAMERICA-NORTHEAST1, AUSTRALIA-SOUTHEAST1, EUROPE-WEST12,
// US-CENTRAL1-A).
var bucketRegionPattern = regexp.MustCompile(`^[A-Z]+(-[A-Z]+[0-9]+)+(-[A-Z])?$`)

// ValidateBucketStorageClass rejects a storageClass outside the documented
// enum. The empty string means "not supplied" and is valid (GCS defaults to
// STANDARD).
func ValidateBucketStorageClass(storageClass string) error {
	if storageClass == "" || bucketStorageClasses[storageClass] {
		return nil
	}
	return model.NewProviderError("InvalidArgument",
		"Invalid storage class: "+storageClass+
			" (must be one of STANDARD, NEARLINE, COLDLINE, ARCHIVE, MULTI_REGIONAL, REGIONAL, DURABLE_REDUCED_AVAILABILITY)",
		400)
}

// ValidateBucketLocation rejects a location that is not a documented GCS
// location code (see the file comment for the grammar and its boundary). The
// empty string means "not supplied" and is valid (GCS defaults to US).
func ValidateBucketLocation(location string) error {
	loc := BucketLocationCanonical(location)
	if loc == "" {
		return nil
	}
	switch loc {
	case "US", "EU", "ASIA":
		return nil
	}
	if predefinedDualRegions[loc] || bucketRegionPattern.MatchString(loc) {
		return nil
	}
	// A custom dual-region may be spelled as the two-region pair
	// (us-central1+us-east1), the form the docs use to name one.
	if parts := strings.Split(loc, "+"); len(parts) > 1 {
		valid := true
		for _, part := range parts {
			if !bucketRegionPattern.MatchString(part) {
				valid = false
				break
			}
		}
		if valid {
			return nil
		}
	}
	return model.NewProviderError("InvalidArgument",
		"Invalid location constraint: "+location+
			" (see https://cloud.google.com/storage/docs/locations for the supported locations)",
		400)
}
