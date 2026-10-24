// Package kms is the transport-neutral core of Cloud Key Management Service
// (cloudkms.googleapis.com).
//
// Both transports map onto this core: the REST adapter
// (internal/gcp/transport/rest/kms) converts NormalizedRequest bodies to typed
// calls, and the gRPC adapter (internal/gcp/transport/grpc/kms) converts the
// official kmspb protos. The core owns every piece of domain logic — key
// lifecycle, version destruction/restoration, rotation, envelope and raw
// crypto, asymmetric signing, MACs, IAM, import jobs, HSM trusted-key wrapping,
// and KEM decapsulation — so REST and gRPC cannot drift (the KMS REST/gRPC
// version/IAM drift was the motivating example for the dual-protocol core
// pattern).
//
// Key material is DEK-wrapped at rest by kmsstore; the core passes raw material
// only across the import/export boundary. Exported/imported key material uses
// the official Cloud KMS wrapping schemes: RSA-OAEP (+ AES-KWP RFC 5649) or
// post-quantum HPKE-KEM import methods, and AES-256-KWP for HSM trusted
// wrapping. KEM key types (ML-KEM-768/1024 and the X-Wing hybrid) are supported
// natively by crypto/mlkem and crypto/hpke.
package kms

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// Resource-type strings for KMS IAM policies plus the RetiredResource /
// ImportJob payloads stored in the shared resource store.
const (
	rtKeyRingPolicy   = "gcp_keyring_policy"
	rtCryptoKeyPolicy = "gcp_cryptokey_policy"
	rtImportJobPolicy = "gcp_kms_importjob_policy"
	rtRetiredResource = "gcp_kms_retiredresource"
	rtImportJob       = "gcp_kms_importjob"
)

// Service is the transport-neutral Cloud KMS core.
type Service struct {
	keys           kmsstore.Store
	resources      store.ResourceStore
	defaultProject string
}

// NewService returns a KMS core backed by the shared stores. defaultProject is
// the configured project used when a transport hands the core no project.
func NewService(keys kmsstore.Store, resources store.ResourceStore, defaultProject string) *Service {
	return &Service{keys: keys, resources: resources, defaultProject: defaultProject}
}

// DefaultProject returns the configured default project.
func (s *Service) DefaultProject() string { return s.defaultProject }

// ─── Resource names ───────────────────────────────────────────────────────────

// KeyRingName formats "projects/{p}/locations/{l}/keyRings/{id}".
func KeyRingName(project, location, id string) string {
	return resource.ResourceID(project)("kms-keyring", location+"/"+id)
}

// CryptoKeyName formats a crypto key resource name.
func CryptoKeyName(project, location, kr, key string) string {
	return resource.ResourceID(project)("kms-cryptokey", location+"/"+kr+"/"+key)
}

// VersionName formats a crypto-key-version resource name.
func VersionName(project, location, kr, key, version string) string {
	return resource.ResourceID(project)("kms-cryptokey-version", location+"/"+kr+"/"+key+"/"+version)
}

// PublicKeyName formats the GetPublicKey sub-resource name.
func PublicKeyName(project, location, kr, key, version string) string {
	return VersionName(project, location, kr, key, version) + "/publicKey"
}

// ImportJobName formats an import job resource name.
func ImportJobName(project, location, kr, id string) string {
	return resource.ResourceID(project)("kms-importjob", location+"/"+kr+"/"+id)
}

// RetiredResourceName formats a retired resource name.
func RetiredResourceName(project, location, id string) string {
	return resource.ResourceID(project)("kms-retiredresource", location+"/"+id)
}

// SplitLocationName parses "projects/{p}/locations/{l}".
func SplitLocationName(name string) (project, location string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 4 && parts[0] == "projects" && parts[2] == "locations" {
		return parts[1], parts[3], true
	}
	return "", "", false
}

// SplitKeyRingName parses "projects/{p}/locations/{l}/keyRings/{kr}".
func SplitKeyRingName(name string) (project, location, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 6 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "keyRings" {
		return parts[1], parts[3], parts[5], true
	}
	return "", "", "", false
}

// SplitCryptoKeyName parses "projects/{p}/locations/{l}/keyRings/{kr}/cryptoKeys/{k}".
func SplitCryptoKeyName(name string) (project, location, kr, key string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 8 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "keyRings" && parts[6] == "cryptoKeys" {
		return parts[1], parts[3], parts[5], parts[7], true
	}
	return "", "", "", "", false
}

// SplitVersionName parses a crypto-key-version resource name.
func SplitVersionName(name string) (project, location, kr, key, version string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 10 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "keyRings" && parts[6] == "cryptoKeys" && parts[8] == "cryptoKeyVersions" {
		return parts[1], parts[3], parts[5], parts[7], parts[9], true
	}
	return "", "", "", "", "", false
}

// SplitKeyOrVersionName accepts a crypto key name (version empty) or a
// crypto-key-version name, so Encrypt can be addressed by either.
func SplitKeyOrVersionName(name string) (project, location, kr, key, version string, ok bool) {
	if p, l, kr, k, v, ok := SplitVersionName(name); ok {
		return p, l, kr, k, v, true
	}
	if p, l, kr, k, ok := SplitCryptoKeyName(name); ok {
		return p, l, kr, k, "", true
	}
	return "", "", "", "", "", false
}

// SplitImportJobName parses "projects/{p}/locations/{l}/keyRings/{kr}/importJobs/{id}".
func SplitImportJobName(name string) (project, location, kr, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 8 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "keyRings" && parts[6] == "importJobs" {
		return parts[1], parts[3], parts[5], parts[7], true
	}
	return "", "", "", "", false
}

// SplitRetiredResourceName parses
// "projects/{p}/locations/{l}/retiredResources/{id}".
func SplitRetiredResourceName(name string) (project, location, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 6 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "retiredResources" {
		return parts[1], parts[3], parts[5], true
	}
	return "", "", "", false
}

// ─── Errors ───────────────────────────────────────────────────────────────────

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

func notFound(msg string) error {
	return model.NewProviderError("NotFound", msg, 404)
}

func alreadyExists(msg string) error {
	return model.NewProviderError("AlreadyExists", msg, 409)
}

func failedPrecondition(msg string) error {
	return model.NewProviderError("FailedPrecondition", msg, 400)
}

func internal(msg string) error {
	return model.NewProviderError("Internal", msg, 500)
}

func keyRingErr(err error) error {
	if errors.Is(err, kmsstore.ErrNoSuchKeyRing) {
		return notFound("key ring not found")
	}
	return err
}

func keyErr(err error) error {
	if errors.Is(err, kmsstore.ErrNoSuchCryptoKey) {
		return notFound("crypto key not found")
	}
	return err
}

func versionErr(err error) error {
	switch {
	case errors.Is(err, kmsstore.ErrNoSuchCryptoKey):
		return notFound("crypto key not found")
	case errors.Is(err, kmsstore.ErrNoSuchVersion):
		return notFound("crypto key version not found")
	case errors.Is(err, kmsstore.ErrNotDestroyable):
		return failedPrecondition("CryptoKeyVersion must be ENABLED or DISABLED to destroy")
	case errors.Is(err, kmsstore.ErrNotRestorable):
		return failedPrecondition("CryptoKeyVersion must be DESTROY_SCHEDULED to restore")
	}
	return err
}

func versionNotEnabledErr(version, state string) error {
	return failedPrecondition("CryptoKeyVersion " + version + " is " + state)
}

// ─── Shared helpers ───────────────────────────────────────────────────────────

// PromoteDestroyed lazily applies elapsed destruction windows before a key's
// versions are observed (Cloud KMS transitions DESTROY_SCHEDULED to DESTROYED
// automatically; the emulator has no scheduler).
func (s *Service) PromoteDestroyed(ctx context.Context, project, location, kr, key string) {
	kmsstore.PromoteDestroyedIfDue(ctx, s.keys, project, location, kr, key, clock.Now())
}

// requireVersionEnabled rejects a version that is not ENABLED.
func (s *Service) requireVersionEnabled(ctx context.Context, project, location, kr, key, version string) error {
	s.PromoteDestroyed(ctx, project, location, kr, key)
	v, err := s.keys.GetVersion(ctx, project, location, kr, key, version)
	if err != nil {
		return versionErr(err)
	}
	if v.State != "ENABLED" {
		return versionNotEnabledErr(version, v.State)
	}
	return nil
}

// PrimaryVersion returns a crypto key's primary version, used to render the
// embedded CryptoKey.primary, falling back to a synthetic version.
func (s *Service) PrimaryVersion(ctx context.Context, project string, k kmsstore.CryptoKey) kmsstore.Version {
	fallback := kmsstore.Version{Version: k.PrimaryVersion, State: "ENABLED", Algorithm: k.Algorithm, CreateTime: k.CreateTime, ProtectionLevel: k.ProtectionLevel}
	if k.PrimaryVersion == "" {
		return fallback
	}
	v, err := s.keys.GetVersion(ctx, project, k.Location, k.KeyRingID, k.ID, k.PrimaryVersion)
	if err != nil || v.State == "" {
		return fallback
	}
	if v.CreateTime.IsZero() {
		v.CreateTime = k.CreateTime
	}
	return v
}

// authorizeCryptoKey enforces a crypto key's IAM policy for one permission. It
// is default-permissive: a key with no policy allows every operation.
func (s *Service) authorizeCryptoKey(ctx context.Context, project, location, kr, key, permission string) error {
	return policy.AuthorizeKMS(ctx, s.resources, project, rtCryptoKeyPolicy,
		location+"/"+kr+"/"+key, permission, CryptoKeyName(project, location, kr, key))
}

// ─── IAM ──────────────────────────────────────────────────────────────────────

// parseIamResource splits a KMS resource name into its policy resource type and
// location-qualified id. IAM is scoped to key rings, crypto keys and import
// jobs; versions have no IAM. A name may be the full "projects/{p}/..." form or
// the relative form the REST codec hands the core.
func parseIamResource(name string) (policyType, id string, ok bool) {
	if _, _, _, _, version, ok := SplitVersionName(name); ok && version != "" {
		return "", "", false
	}
	if loc, kr, job, ok := importJobRel(name); ok {
		return rtImportJobPolicy, loc + "/" + kr + "/" + job, true
	}
	if loc, kr, key, ok := parseCryptoKeyRel(name); ok {
		return rtCryptoKeyPolicy, loc + "/" + kr + "/" + key, true
	}
	if loc, kr, ok := parseKeyRingRel(name); ok {
		return rtKeyRingPolicy, loc + "/" + kr, true
	}
	return "", "", false
}

// importJobRel splits an import-job name that may or may not carry a
// projects/{p} prefix into location/keyRing/job.
func importJobRel(name string) (location, kr, job string, ok bool) {
	if _, loc, kr, job, ok := SplitImportJobName(name); ok {
		return loc, kr, job, true
	}
	name = strings.TrimPrefix(strings.TrimPrefix(name, "/"), "locations/")
	parts := strings.Split(name, "/")
	if len(parts) == 5 && parts[1] == "keyRings" && parts[3] == "importJobs" {
		return parts[0], parts[2], parts[4], true
	}
	return "", "", "", false
}

// parseCryptoKeyRel splits a name that may or may not carry a projects/{p}
// prefix into location/keyRing/key.
func parseCryptoKeyRel(name string) (location, kr, key string, ok bool) {
	name = strings.TrimPrefix(name, "/")
	if _, loc, kr, key, ok := SplitCryptoKeyName(name); ok {
		return loc, kr, key, true
	}
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/")
	if len(parts) == 5 && parts[1] == "keyRings" && parts[3] == "cryptoKeys" {
		return parts[0], parts[2], parts[4], true
	}
	return "", "", "", false
}

// parseKeyRingRel splits a key-ring name that may or may not carry a
// projects/{p} prefix into location/keyRing.
func parseKeyRingRel(name string) (location, kr string, ok bool) {
	if _, loc, kr, ok := SplitKeyRingName(name); ok {
		return loc, kr, true
	}
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/")
	if len(parts) == 3 && parts[1] == "keyRings" {
		return parts[0], parts[2], true
	}
	return "", "", false
}

// IamResourceExists verifies the key ring, crypto key or import job backing an
// IAM request exists.
func (s *Service) IamResourceExists(ctx context.Context, project, name string) error {
	if _, _, _, _, version, ok := SplitVersionName(name); ok && version != "" {
		return invalidArgument("invalid resource name")
	}
	if loc, kr, job, ok := importJobRel(name); ok {
		_, err := s.GetImportJob(ctx, project, loc, kr, job)
		return err
	}
	if loc, kr, key, ok := parseCryptoKeyRel(name); ok {
		_, err := s.keys.GetCryptoKey(ctx, project, loc, kr, key)
		return keyErr(err)
	}
	if loc, kr, ok := parseKeyRingRel(name); ok {
		_, err := s.keys.GetKeyRing(ctx, project, loc, kr)
		return keyRingErr(err)
	}
	return invalidArgument("invalid resource name")
}

// IamPolicyInfo returns the policy resource type and id for a resource name.
func IamPolicyInfo(name string) (policyType, id string, ok bool) {
	return parseIamResource(name)
}

// GetIamPolicy returns the stored policy for a key ring or crypto key.
func (s *Service) GetIamPolicy(ctx context.Context, project, name string) (policy.Policy, error) {
	policyType, id, ok := parseIamResource(name)
	if !ok {
		return policy.Policy{}, invalidArgument("invalid resource name")
	}
	if err := s.IamResourceExists(ctx, project, name); err != nil {
		return policy.Policy{}, err
	}
	return policy.Load(ctx, s.resources, project, policyType, id), nil
}

// SetIamPolicy stores the policy for a key ring or crypto key, enforcing etag
// optimistic concurrency.
func (s *Service) SetIamPolicy(ctx context.Context, project, name string, body map[string]any) (policy.Policy, error) {
	policyType, id, ok := parseIamResource(name)
	if !ok {
		return policy.Policy{}, invalidArgument("invalid resource name")
	}
	if err := s.IamResourceExists(ctx, project, name); err != nil {
		return policy.Policy{}, err
	}
	return policy.Set(ctx, s.resources, project, policyType, id, body)
}

// TestIamPermissions echoes the requested permissions (no authz enforcement).
func (s *Service) TestIamPermissions(ctx context.Context, project, name string, permissions []string) ([]string, error) {
	if _, _, ok := parseIamResource(name); !ok {
		return nil, invalidArgument("invalid resource name")
	}
	if err := s.IamResourceExists(ctx, project, name); err != nil {
		return nil, err
	}
	return policy.TestPermissions(permissions), nil
}

// ─── Randomness ───────────────────────────────────────────────────────────────

// RandomBytesMin and RandomBytesMax are the documented bounds of
// locations.generateRandomBytes: "Minimum 8 bytes, maximum 1024 bytes".
const (
	RandomBytesMin = 8
	RandomBytesMax = 1024
)

// GenerateRandomBytes implements locations.generateRandomBytes: it returns
// length cryptographically random bytes. The location name is validated for
// shape only — the emulator models no KMS location registry, so (as with the
// other KMS calls) a well-formed location under any project is served. The
// request's protectionLevel is accepted and ignored: the emulator's key
// material is software-only, so it always behaves as a SOFTWARE RNG.
func (s *Service) GenerateRandomBytes(ctx context.Context, location string, length int, protectionLevel string) ([]byte, error) {
	if !validLocationName(location) {
		return nil, invalidArgument("invalid location name")
	}
	if length < RandomBytesMin || length > RandomBytesMax {
		return nil, invalidArgument("length_bytes must be between 8 and 1024")
	}
	data := make([]byte, length)
	if _, err := rand.Read(data); err != nil {
		return nil, internal("random generation failed")
	}
	return data, nil
}

// validLocationName accepts a location resource name in either the full
// "projects/{p}/locations/{l}" form (gRPC) or the relative "locations/{l}" form
// the REST codec hands the core.
func validLocationName(name string) bool {
	if _, _, ok := SplitLocationName(name); ok {
		return true
	}
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	return len(parts) == 2 && parts[0] == "locations" && parts[1] != ""
}

// timeNow exposes clock.Now for the core's time-stamped writes.
func timeNow() time.Time { return clock.Now() }
