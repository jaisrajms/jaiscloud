// Package reference validates cross-service resource references at write time:
// the CryptoKey on a bucket/cluster/service, the compute network a Dataproc
// Metastore service attaches to, and similar fields whose proto carries a
// google.api.resource_reference.
//
// A reference is checked against the store of the service that owns the target,
// as real GCP does when the owning resource is created or updated, rather than
// being stored opaque and failing later at use time. The checks that need no
// state (resource-name shape) always run; the existence checks run only when
// the caller supplies a store, so unit tests that exercise one service in
// isolation can construct it without the target service.
//
// The errors are the referring resource's documented answer: a malformed,
// missing or wrong-purpose reference is INVALID_ARGUMENT (real GCS, Cloud Run,
// Cloud Functions, Dataproc and Pub/Sub all reject the owning write this way).
package reference

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"jaiscloud/internal/gcp/resource"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// Persisted ResourceStore types of the compute resources referenced from other
// services. They mirror internal/gcp/provider/compute's own constants.
const (
	ComputeNetworkType = "gcp_compute_network"
)

// invalidArgument is the provider error every failed reference answers with.
func invalidArgument(format string, args ...any) error {
	return model.NewProviderError("InvalidArgument", fmt.Sprintf(format, args...), 400)
}

// CryptoKeyErrorKind classifies a refused CryptoKey reference. The message is
// the generic one (INVALID_ARGUMENT) every referring service has always used;
// a consumer whose API documents its own wording (Cloud Storage does: see the
// GCS JSON/XML status references) reads the kind from
// ProviderError.Data["cryptoKeyError"] and renders its own message, so no other
// service's text changes.
const (
	CryptoKeyErrorMalformed = "malformed"
	CryptoKeyErrorNotFound  = "notFound"
	CryptoKeyErrorLocation  = "location"
	CryptoKeyErrorPurpose   = "purpose"
)

// cryptoKeyError builds the classified refusal.
func cryptoKeyError(kind, msg string, extra map[string]any) error {
	data := map[string]any{"cryptoKeyError": kind}
	for k, v := range extra {
		data[k] = v
	}
	return model.NewProviderError("InvalidArgument", msg, 400).WithData(data)
}

// SplitCryptoKeyName parses the Cloud KMS CryptoKey resource name
// "projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}".
func SplitCryptoKeyName(name string) (project, location, ring, key string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 8 || parts[0] != "projects" || parts[2] != "locations" || parts[4] != "keyRings" || parts[6] != "cryptoKeys" {
		return "", "", "", "", false
	}
	return parts[1], parts[3], parts[5], parts[7], true
}

// KMSLocationMatches reports whether a CryptoKey in keyLocation may protect a
// resource in resourceLocation. The comparison is case-insensitive (buckets
// canonicalise locations to uppercase, KMS serves lowercase) and maps GCS's
// multi-region codes to their KMS multi-region locations: a US bucket uses a
// key in "us", EU in "europe", ASIA in "asia". Every other location must match
// exactly, as the Cloud KMS CMEK documentation requires ("The Cloud KMS key
// must be in the same location as the resource").
func KMSLocationMatches(resourceLocation, keyLocation string) bool {
	r := strings.ToLower(strings.TrimSpace(resourceLocation))
	k := strings.ToLower(strings.TrimSpace(keyLocation))
	if r == "" || k == "" || r == k {
		return true
	}
	switch r {
	case "eu":
		return k == "europe"
	case "asia":
		return k == "asia"
	}
	return false
}

// ValidateCryptoKey validates a CryptoKey reference for a resource in
// resourceLocation. field names the referring field in the error message (for
// example "encryption.defaultKmsKeyName"). A non-nil keys store additionally
// requires the key to exist and to be an ENCRYPT_DECRYPT key; a nil store
// checks only the documented resource-name shape and location.
func ValidateCryptoKey(ctx context.Context, keys kmsstore.Store, field, name, resourceLocation string) error {
	project, location, ring, key, ok := SplitCryptoKeyName(name)
	if !ok {
		return cryptoKeyError(CryptoKeyErrorMalformed,
			fmt.Sprintf("%s must be a CryptoKey resource name (projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key})", field),
			map[string]any{"cryptoKeyName": name})
	}
	if !KMSLocationMatches(resourceLocation, location) {
		return cryptoKeyError(CryptoKeyErrorLocation,
			fmt.Sprintf("%s must be in the same location as the resource (%s)", field, resourceLocation),
			map[string]any{"cryptoKeyName": name, "keyLocation": location, "resourceLocation": resourceLocation})
	}
	if keys == nil {
		return nil
	}
	ck, err := keys.GetCryptoKey(ctx, project, location, ring, key)
	switch {
	case errors.Is(err, kmsstore.ErrNoSuchCryptoKey), errors.Is(err, kmsstore.ErrNoSuchKeyRing):
		return cryptoKeyError(CryptoKeyErrorNotFound,
			fmt.Sprintf("%s refers to a key that does not exist: %s", field, name),
			map[string]any{"cryptoKeyName": name})
	case err != nil:
		return model.NewProviderError("Internal", err.Error(), 500)
	}
	if ck.Purpose != "" && ck.Purpose != "ENCRYPT_DECRYPT" {
		return cryptoKeyError(CryptoKeyErrorPurpose,
			fmt.Sprintf("%s must have purpose ENCRYPT_DECRYPT", field),
			map[string]any{"cryptoKeyName": name, "purpose": ck.Purpose})
	}
	return nil
}

// SplitComputeNetworkName parses the documented compute-network reference
// "projects/{project}/global/networks/{network}". It also accepts a bare
// network id, which real GCP resolves relative to the referring project.
func SplitComputeNetworkName(name string) (project, network string, ok bool) {
	if name == "" {
		return "", "", false
	}
	parts := strings.Split(name, "/")
	if len(parts) == 1 {
		return "", parts[0], true
	}
	if len(parts) != 5 || parts[0] != "projects" || parts[2] != "global" || parts[3] != "networks" {
		return "", "", false
	}
	return parts[1], parts[4], true
}

// ValidateComputeNetwork checks a compute network reference owned by project.
// The reference may name the project by id or by its synthesized project
// number (resource.ProjectNumber), as the compute API's documented form does.
// A nil resources store checks only the resource-name shape.
func ValidateComputeNetwork(ctx context.Context, resources store.ResourceStore, project, name string) error {
	refProject, network, ok := SplitComputeNetworkName(name)
	if !ok || network == "" {
		return invalidArgument("network must be a compute network resource name (projects/{project}/global/networks/{network})")
	}
	if refProject != "" && refProject != project && refProject != resource.ProjectNumberFor(project) {
		return invalidArgument("network must belong to the same project: %s", name)
	}
	if resources == nil {
		return nil
	}
	if _, err := resources.Get(ctx, project, store.GlobalRegion, ComputeNetworkType, network); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Every real GCP project starts with a "default" network, and the
			// compute surface already treats it as implicit (an instance,
			// firewall or subnetwork that names no network gets this one). The
			// emulator does not seed the record, so resolve it without one.
			if network == "default" {
				return nil
			}
			return invalidArgument("network does not exist: %s", name)
		}
		return model.NewProviderError("Internal", err.Error(), 500)
	}
	return nil
}
