// Package kms implements the Cloud KMS gRPC service
// (google.cloud.kms.v1.KeyManagementService plus the google.iam.v1.IAMPolicy
// surface for keyring/crypto-key/version IAM) over the same shared
// kmsstore.Store + policy backing the REST provider, so REST and gRPC share
// state.
package kms

import (
	"context"
	"crypto"
	"crypto/rand"
	"encoding/json"
	"errors"
	"hash/crc32"
	"strings"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"jaiscloud/internal/clock"
	gcpcrypto "jaiscloud/internal/gcp/crypto"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Resource-type strings for KMS IAM policies stored in the shared resource
// store. These mirror the Pub/Sub / Secret Manager policy types. IAM is scoped
// to key rings and crypto keys only — real KMS has no version-level IAM.
const (
	rtKeyRingPolicy   = "gcp_keyring_policy"
	rtCryptoKeyPolicy = "gcp_cryptokey_policy"

	// rtRetiredResource / rtImportJob live in the shared resource store (not the
	// kmsstore) so their lifecycle persists identically in memory and postgres
	// modes without a dedicated table.
	rtRetiredResource = "gcp_kms_retiredresource"
	rtImportJob       = "gcp_kms_importjob"
)

// retiredResourceData is the JSON payload stored for a RetiredResource. The
// store id is "<location>/<cryptoKeyID>" so a location-scoped list is a prefix
// scan.
type retiredResourceData struct {
	Name             string    `json:"name"`
	OriginalResource string    `json:"originalResource"`
	ResourceType     string    `json:"resourceType"`
	DeleteTime       time.Time `json:"deleteTime"`
}

// importJobData is the JSON payload stored for an ImportJob. The store id is
// "<location>/<keyringID>/<importJobID>". WrappedPrivateKey is the import job's
// RSA private key, DEK-wrapped at rest like every other KMS key material.
type importJobData struct {
	Name              string    `json:"name"`
	ImportMethod      string    `json:"importMethod"`
	ProtectionLevel   int32     `json:"protectionLevel"`
	CreateTime        time.Time `json:"createTime"`
	GenerateTime      time.Time `json:"generateTime"`
	ExpireTime        time.Time `json:"expireTime"`
	State             string    `json:"state"`
	PublicKeyPEM      string    `json:"publicKeyPem"`
	WrappedPrivateKey []byte    `json:"wrappedPrivateKey,omitempty"`
}

// Service implements kmspb.KeyManagementServiceServer and
// iampb.IAMPolicyServer over the shared stores. KMS generates and DEK-wraps its
// own key material via the store (CreateCryptoKey/CreateVersion), so the
// envelope encryptor is retained only for constructor parity with the Pub/Sub
// and Secret Manager gRPC services and is unused here.
type Service struct {
	kmspb.UnimplementedKeyManagementServiceServer
	iampb.UnimplementedIAMPolicyServer

	keys        kmsstore.Store
	resources   store.ResourceStore // IAM policies (control-plane)
	encryptor   gcpcrypto.EnvelopeEncryptor
	defaultProj string
}

// NewService returns a KMS gRPC service backed by the shared stores.
// defaultProj is the config-default project used when a request carries none.
func NewService(keys kmsstore.Store, resources store.ResourceStore, encryptor gcpcrypto.EnvelopeEncryptor, defaultProj string) *Service {
	return &Service{keys: keys, resources: resources, encryptor: encryptor, defaultProj: defaultProj}
}

// authorizeCryptoKey enforces the cryptoKey's IAM policy for one crypto-use
// permission, mirroring the REST provider's Provider.authorizeCryptoKey. It is
// default-permissive: a key with no policy (or no bindings) allows every
// operation. The returned error is already mapped to a gRPC status.
func (s *Service) authorizeCryptoKey(ctx context.Context, project, loc, kr, key, permission string) error {
	if err := policy.AuthorizeKMS(ctx, s.resources, project, rtCryptoKeyPolicy,
		loc+"/"+kr+"/"+key, permission, cryptoKeyName(project, loc, kr, key)); err != nil {
		return mapError(err)
	}
	return nil
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func keyRingErr(err error) error {
	if errors.Is(err, kmsstore.ErrNoSuchKeyRing) {
		return mapError(model.NewProviderError("NotFound", "key ring not found", 404))
	}
	return mapError(err)
}

// promoteDestroyed lazily applies any elapsed destruction windows for a crypto
// key before its versions are observed. Cloud KMS transitions DESTROY_SCHEDULED
// to DESTROYED automatically; without a scheduler the emulator does it on
// access (mirrors RotateIfDue).
func (s *Service) promoteDestroyed(ctx context.Context, project, loc, kr, key string) {
	kmsstore.PromoteDestroyedIfDue(ctx, s.keys, project, loc, kr, key, clock.Now())
}

// requireVersionEnabled rejects use of a crypto-key version that is not ENABLED.
func (s *Service) requireVersionEnabled(ctx context.Context, project, loc, kr, key, version string) error {
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return versionErr(err)
	}
	if v.State != "ENABLED" {
		return mapError(versionNotEnabledErr(version, v.State))
	}
	return nil
}

// primaryVersion returns a crypto key's primary version, used to render the
// embedded CryptoKey.primary. A missing/unknown primary falls back to a
// synthetic ENABLED version stamped with the key's own create time.
func (s *Service) primaryVersion(ctx context.Context, project string, k kmsstore.CryptoKey) kmsstore.Version {
	fallback := kmsstore.Version{Version: k.PrimaryVersion, State: "ENABLED", Algorithm: k.Algorithm, CreateTime: k.CreateTime}
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

func versionNotEnabledErr(version, state string) *model.ProviderError {
	return &model.ProviderError{
		Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
		Message: "CryptoKeyVersion " + version + " is " + state,
	}
}

func keyErr(err error) error {
	if errors.Is(err, kmsstore.ErrNoSuchCryptoKey) {
		return mapError(model.NewProviderError("NotFound", "crypto key not found", 404))
	}
	return mapError(err)
}

func versionErr(err error) error {
	switch {
	case errors.Is(err, kmsstore.ErrNoSuchCryptoKey):
		return mapError(model.NewProviderError("NotFound", "crypto key not found", 404))
	case errors.Is(err, kmsstore.ErrNoSuchVersion):
		return mapError(model.NewProviderError("NotFound", "crypto key version not found", 404))
	case errors.Is(err, kmsstore.ErrNotDestroyable):
		return mapError(model.NewProviderError("FailedPrecondition", "CryptoKeyVersion must be ENABLED or DISABLED to destroy", 400))
	case errors.Is(err, kmsstore.ErrNotRestorable):
		return mapError(model.NewProviderError("FailedPrecondition", "CryptoKeyVersion must be DESTROY_SCHEDULED to restore", 400))
	}
	return mapError(err)
}

// ─── resource-name parsing ────────────────────────────────────────────────────

func keyRingName(project, location, id string) string {
	return "projects/" + project + "/locations/" + location + "/keyRings/" + id
}

func cryptoKeyName(project, location, kr, key string) string {
	return keyRingName(project, location, kr) + "/cryptoKeys/" + key
}

func versionName(project, location, kr, key, v string) string {
	return cryptoKeyName(project, location, kr, key) + "/cryptoKeyVersions/" + v
}

// splitLocationName parses "projects/{p}/locations/{l}".
func splitLocationName(name string) (project, location string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 4 && parts[0] == "projects" && parts[2] == "locations" {
		return parts[1], parts[3], true
	}
	return "", "", false
}

// splitKeyRingName parses "projects/{p}/locations/{l}/keyRings/{kr}".
func splitKeyRingName(name string) (project, location, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 6 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "keyRings" {
		return parts[1], parts[3], parts[5], true
	}
	return "", "", "", false
}

// splitCryptoKeyName parses "projects/{p}/locations/{l}/keyRings/{kr}/cryptoKeys/{k}".
func splitCryptoKeyName(name string) (project, location, kr, key string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 8 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "keyRings" && parts[6] == "cryptoKeys" {
		return parts[1], parts[3], parts[5], parts[7], true
	}
	return "", "", "", "", false
}

// splitVersionName parses
// "projects/{p}/locations/{l}/keyRings/{kr}/cryptoKeys/{k}/cryptoKeyVersions/{v}".
func splitVersionName(name string) (project, location, kr, key, version string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 10 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "keyRings" && parts[6] == "cryptoKeys" && parts[8] == "cryptoKeyVersions" {
		return parts[1], parts[3], parts[5], parts[7], parts[9], true
	}
	return "", "", "", "", "", false
}

// splitKeyOrVersionName accepts either a crypto key name (version empty) or a
// crypto key version name, so Encrypt/Decrypt can be addressed by either.
func splitKeyOrVersionName(name string) (project, location, kr, key, version string, ok bool) {
	if p, l, kr, k, v, ok := splitVersionName(name); ok {
		return p, l, kr, k, v, true
	}
	if p, l, kr, k, ok := splitCryptoKeyName(name); ok {
		return p, l, kr, k, "", true
	}
	return "", "", "", "", "", false
}

// splitLocationParent resolves the project + location from a ListKeyRings /
// CreateKeyRing parent ("projects/{p}/locations/{l}"). A bare location id (or a
// "locations/{l}" suffix) falls back to the metadata-derived project.
func (s *Service) splitLocationParent(ctx context.Context, parent string) (project, location string, ok bool) {
	if p, l, ok := splitLocationName(parent); ok {
		return p, l, true
	}
	loc := strings.TrimPrefix(parent, "locations/")
	if loc != "" && !strings.Contains(loc, "/") {
		return grpcutil.ProjectFromMetadata(ctx, s.defaultProj), loc, true
	}
	return "", "", false
}

// ─── KeyRings ─────────────────────────────────────────────────────────────────

func (s *Service) ListKeyRings(ctx context.Context, req *kmspb.ListKeyRingsRequest) (*kmspb.ListKeyRingsResponse, error) {
	project, loc, ok := s.splitLocationParent(ctx, req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	krs, err := s.keys.ListKeyRings(ctx, project, loc)
	if err != nil {
		return nil, mapError(err)
	}
	page, next := paging.Page(krs, func(kr kmsstore.KeyRing) string { return kr.ID },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	out := make([]*kmspb.KeyRing, 0, len(page))
	for _, kr := range page {
		out = append(out, keyRingToProto(project, loc, kr))
	}
	return &kmspb.ListKeyRingsResponse{KeyRings: out, NextPageToken: next, TotalSize: int32(len(krs))}, nil
}

func (s *Service) CreateKeyRing(ctx context.Context, req *kmspb.CreateKeyRingRequest) (*kmspb.KeyRing, error) {
	project, loc, ok := s.splitLocationParent(ctx, req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	id := req.GetKeyRingId()
	if id == "" {
		if req.GetKeyRing() != nil {
			_, _, id, _ = splitKeyRingName(req.GetKeyRing().GetName())
		}
	}
	if id == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing keyRingId", 400))
	}
	now := clock.Now()
	kr := kmsstore.KeyRing{Location: loc, ID: id, CreateTime: now}
	if err := s.keys.CreateKeyRing(ctx, project, loc, id, kr); err != nil {
		if errors.Is(err, kmsstore.ErrAlreadyExists) {
			return nil, mapError(model.NewProviderError("AlreadyExists", "key ring already exists", 409))
		}
		return nil, mapError(err)
	}
	return keyRingToProto(project, loc, kr), nil
}

func (s *Service) GetKeyRing(ctx context.Context, req *kmspb.GetKeyRingRequest) (*kmspb.KeyRing, error) {
	project, loc, id, ok := splitKeyRingName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	kr, err := s.keys.GetKeyRing(ctx, project, loc, id)
	if err != nil {
		return nil, keyRingErr(err)
	}
	return keyRingToProto(project, loc, kr), nil
}

// ─── CryptoKeys ───────────────────────────────────────────────────────────────

func (s *Service) ListCryptoKeys(ctx context.Context, req *kmspb.ListCryptoKeysRequest) (*kmspb.ListCryptoKeysResponse, error) {
	project, loc, kr, ok := splitKeyRingName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	keys, err := s.keys.ListCryptoKeys(ctx, project, loc, kr)
	if err != nil {
		return nil, mapError(err)
	}
	// Lazily execute any due rotation schedules before paging, then re-list so
	// the page reflects the rotated primaries.
	for _, k := range keys {
		kmsstore.RotateIfDue(ctx, s.keys, project, loc, kr, k.ID, clock.Now())
		s.promoteDestroyed(ctx, project, loc, kr, k.ID)
	}
	if keys, err = s.keys.ListCryptoKeys(ctx, project, loc, kr); err != nil {
		return nil, mapError(err)
	}
	page, next := paging.Page(keys, func(k kmsstore.CryptoKey) string { return k.ID },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	out := make([]*kmspb.CryptoKey, 0, len(page))
	for _, k := range page {
		out = append(out, cryptoKeyToProto(project, k, s.primaryVersion(ctx, project, k)))
	}
	return &kmspb.ListCryptoKeysResponse{CryptoKeys: out, NextPageToken: next, TotalSize: int32(len(keys))}, nil
}

func (s *Service) CreateCryptoKey(ctx context.Context, req *kmspb.CreateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	project, loc, kr, ok := splitKeyRingName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	key := req.GetCryptoKeyId()
	if key == "" {
		if ck := req.GetCryptoKey(); ck != nil {
			_, _, _, key, _ = splitCryptoKeyName(ck.GetName())
		}
	}
	if key == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing cryptoKeyId", 400))
	}
	purpose := "ENCRYPT_DECRYPT"
	algorithm := ""
	var labels map[string]string
	var rotationPeriod time.Duration
	if ck := req.GetCryptoKey(); ck != nil {
		purpose = purposeFromProto(ck.GetPurpose())
		if vt := ck.GetVersionTemplate(); vt != nil {
			algorithm = algorithmFromProto(vt.GetAlgorithm())
		}
		if len(ck.GetLabels()) > 0 {
			labels = ck.GetLabels()
		}
		if rp := ck.GetRotationPeriod(); rp != nil {
			d := rp.AsDuration()
			if d <= 0 {
				return nil, mapError(model.NewProviderError("InvalidArgument", "rotation period must be positive", 400))
			}
			rotationPeriod = d
		}
	}
	if algorithm == "" {
		algorithm = defaultAlgorithmForPurpose(purpose)
	}
	// A deleted CryptoKey leaves a RetiredResource behind; its name cannot be
	// reused (Cloud KMS RetiredResource semantics).
	if _, err := s.resources.Get(ctx, project, store.GlobalRegion, rtRetiredResource, retiredResourceID(loc, key)); err == nil {
		return nil, mapError(model.NewProviderError("AlreadyExists", "crypto key name is retired and cannot be reused", 409))
	}
	now := clock.Now()
	ck := kmsstore.CryptoKey{Location: loc, KeyRingID: kr, ID: key, Purpose: purpose, CreateTime: now, PrimaryVersion: "1", Algorithm: algorithm, Labels: labels, RotationPeriod: rotationPeriod}
	if rotationPeriod > 0 {
		ck.NextRotationTime = now.Add(rotationPeriod)
	}
	if err := s.keys.CreateCryptoKey(ctx, project, loc, kr, key, ck); err != nil {
		if errors.Is(err, kmsstore.ErrAlreadyExists) {
			return nil, mapError(model.NewProviderError("AlreadyExists", "crypto key already exists", 409))
		}
		return nil, mapError(err)
	}
	return cryptoKeyToProto(project, ck, kmsstore.Version{Version: ck.PrimaryVersion, State: "ENABLED", Algorithm: ck.Algorithm, CreateTime: ck.CreateTime}), nil
}

func (s *Service) GetCryptoKey(ctx context.Context, req *kmspb.GetCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	project, loc, kr, key, ok := splitCryptoKeyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	kmsstore.RotateIfDue(ctx, s.keys, project, loc, kr, key, clock.Now())
	s.promoteDestroyed(ctx, project, loc, kr, key)
	k, err := s.keys.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		return nil, keyErr(err)
	}
	return cryptoKeyToProto(project, k, s.primaryVersion(ctx, project, k)), nil
}

// UpdateCryptoKey applies the update_mask to the mutable fields. `labels` and
// `rotation_period` are supported; setting rotation_period (re)derives
// next_rotation_time = now + period, and clearing it clears next_rotation_time.
// Purpose/algorithm are immutable. An unsupported mask path fails loud with
// Unimplemented. The read-modify-write runs inside the store's atomic update so
// a concurrent masked patch can't be lost.
func (s *Service) UpdateCryptoKey(ctx context.Context, req *kmspb.UpdateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	ck := req.GetCryptoKey()
	if ck == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "crypto key is required", 400))
	}
	project, loc, kr, key, ok := splitCryptoKeyName(ck.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	k, err := s.keys.UpdateCryptoKeyAtomic(ctx, project, loc, kr, key, func(stored kmsstore.CryptoKey) (kmsstore.CryptoKey, error) {
		paths := req.GetUpdateMask().GetPaths()
		if len(paths) == 0 {
			// No mask: replace every mutable field (labels, rotation schedule).
			paths = []string{"labels", "rotation_period"}
		}
		return applyCryptoKeyMask(stored, ck, paths)
	})
	if err != nil {
		return nil, keyErr(err)
	}
	return cryptoKeyToProto(project, k, s.primaryVersion(ctx, project, k)), nil
}

// applyCryptoKeyMask merges an incoming crypto key into the stored key
// according to updateMask. Masked paths take the incoming value; unmasked
// fields retain the stored value. Supported paths are labels and
// rotation_period; any other path returns an error mapped to Unimplemented.
func applyCryptoKeyMask(stored kmsstore.CryptoKey, incoming *kmspb.CryptoKey, updateMask []string) (kmsstore.CryptoKey, error) {
	for _, path := range updateMask {
		switch path {
		case "labels":
			stored.Labels = incoming.GetLabels()
		case "rotation_period":
			rp := incoming.GetRotationPeriod()
			if rp == nil {
				stored.RotationPeriod = 0
				stored.NextRotationTime = time.Time{}
				continue
			}
			d := rp.AsDuration()
			if d <= 0 {
				return stored, model.NewProviderError("InvalidArgument", "rotation period must be positive", 400)
			}
			stored.RotationPeriod = d
			stored.NextRotationTime = clock.Now().Add(d)
		default:
			return stored, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+path, 501)
		}
	}
	return stored, nil
}

func (s *Service) UpdateCryptoKeyPrimaryVersion(ctx context.Context, req *kmspb.UpdateCryptoKeyPrimaryVersionRequest) (*kmspb.CryptoKey, error) {
	project, loc, kr, key, ok := splitCryptoKeyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	versionID := req.GetCryptoKeyVersionId()
	if versionID == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing cryptoKeyVersionId", 400))
	}
	if err := s.keys.UpdatePrimaryVersion(ctx, project, loc, kr, key, versionID); err != nil {
		return nil, versionErr(err)
	}
	ck, _ := s.keys.GetCryptoKey(ctx, project, loc, kr, key)
	return cryptoKeyToProto(project, ck, s.primaryVersion(ctx, project, ck)), nil
}

// ─── CryptoKeyVersions ────────────────────────────────────────────────────────

func (s *Service) CreateCryptoKeyVersion(ctx context.Context, req *kmspb.CreateCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, ok := splitCryptoKeyName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	ck, err := s.keys.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		return nil, keyErr(err)
	}
	now := clock.Now()
	version, err := s.keys.CreateVersion(ctx, project, loc, kr, key, kmsstore.Version{CreateTime: now, Algorithm: ck.Algorithm})
	if err != nil {
		return nil, versionErr(err)
	}
	return versionToProto(project, loc, kr, key, kmsstore.Version{Version: version, State: "ENABLED", Algorithm: ck.Algorithm, CreateTime: now}), nil
}

func (s *Service) ListCryptoKeyVersions(ctx context.Context, req *kmspb.ListCryptoKeyVersionsRequest) (*kmspb.ListCryptoKeyVersionsResponse, error) {
	project, loc, kr, key, ok := splitCryptoKeyName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	s.promoteDestroyed(ctx, project, loc, kr, key)
	versions, err := s.keys.ListVersions(ctx, project, loc, kr, key)
	if err != nil {
		return nil, versionErr(err)
	}
	page, next := paging.Page(versions, func(v kmsstore.Version) string { return v.Version },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	out := make([]*kmspb.CryptoKeyVersion, 0, len(page))
	for _, v := range page {
		out = append(out, versionToProto(project, loc, kr, key, v))
	}
	return &kmspb.ListCryptoKeyVersionsResponse{CryptoKeyVersions: out, NextPageToken: next, TotalSize: int32(len(versions))}, nil
}

func (s *Service) GetCryptoKeyVersion(ctx context.Context, req *kmspb.GetCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

// DestroyCryptoKeyVersion schedules a version for destruction: it moves to
// DESTROY_SCHEDULED with a destroy_time one destroy_scheduled_duration (30 days
// by default) in the future. A version already DESTROY_SCHEDULED or DESTROYED is
// returned unchanged (idempotent, as in Cloud KMS). A subsequent read whose
// destroy_time has elapsed promotes it to DESTROYED (see promoteDestroyed).
func (s *Service) DestroyCryptoKeyVersion(ctx context.Context, req *kmspb.DestroyCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	// Apply any elapsed destroy window first, so a version already past its
	// destroy_time is reported (and kept) DESTROYED rather than stale
	// DESTROY_SCHEDULED.
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.DestroyVersion(ctx, project, loc, kr, key, version, clock.Now().Add(kmsstore.DefaultDestroyScheduledDuration))
	if err != nil {
		return nil, versionErr(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

// RestoreCryptoKeyVersion reverses a scheduled destruction: a DESTROY_SCHEDULED
// version becomes DISABLED and its destroy_time is cleared. Restoring any other
// state fails with FAILED_PRECONDITION.
func (s *Service) RestoreCryptoKeyVersion(ctx context.Context, req *kmspb.RestoreCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	// Apply any elapsed destroy window first: once destroy_time has passed the
	// version is DESTROYED and restoration is irreversible.
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.RestoreVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

func (s *Service) UpdateCryptoKeyVersion(ctx context.Context, req *kmspb.UpdateCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	p := req.GetCryptoKeyVersion()
	if p == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "crypto key version is required", 400))
	}
	project, loc, kr, key, version, ok := splitVersionName(p.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	// Apply any elapsed destroy window first, then reject a scheduled or
	// destroyed version: UpdateCryptoKeyVersion only moves between ENABLED and
	// DISABLED (a DESTROY_SCHEDULED version is restored, not updated).
	s.promoteDestroyed(ctx, project, loc, kr, key)
	if p.GetState() != kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_STATE_UNSPECIFIED {
		state := stateFromProto(p.GetState())
		if state != "ENABLED" && state != "DISABLED" {
			return nil, mapError(model.NewProviderError("InvalidArgument", "cryptoKeyVersion.state must be ENABLED or DISABLED", 400))
		}
		cur, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
		if err != nil {
			return nil, versionErr(err)
		}
		if cur.State == "DESTROY_SCHEDULED" || cur.State == "DESTROYED" {
			return nil, mapError(model.NewProviderError("FailedPrecondition",
				"CryptoKeyVersion is "+cur.State+"; use RestoreCryptoKeyVersion", 400))
		}
		if err := s.keys.UpdateVersionState(ctx, project, loc, kr, key, version, state); err != nil {
			return nil, versionErr(err)
		}
	}
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

// ─── Crypto operations ────────────────────────────────────────────────────────

func (s *Service) Encrypt(ctx context.Context, req *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error) {
	project, loc, kr, key, version, ok := splitKeyOrVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.authorizeCryptoKey(ctx, project, loc, kr, key, policy.PermCryptoKeyUseToEncrypt); err != nil {
		return nil, err
	}
	kmsstore.RotateIfDue(ctx, s.keys, project, loc, kr, key, clock.Now())
	ck, err := s.keys.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		return nil, keyErr(err)
	}
	if version == "" {
		version = ck.PrimaryVersion
	}
	if err := s.requireVersionEnabled(ctx, project, loc, kr, key, version); err != nil {
		return nil, err
	}
	keyMat, err := s.keys.KeyMaterial(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if err := verifyCRC("plaintext_crc32c", req.GetPlaintextCrc32C(), req.GetPlaintext()); err != nil {
		return nil, err
	}
	if err := verifyCRC("additional_authenticated_data_crc32c", req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData()); err != nil {
		return nil, err
	}
	ct, err := kmsstore.EncryptData(keyMat, req.GetPlaintext(), req.GetAdditionalAuthenticatedData())
	if err != nil {
		return nil, mapError(model.NewProviderError("Internal", "encryption failed", 500))
	}
	blob := kmsstore.EncodeVersionedCiphertext(version, ct)
	return &kmspb.EncryptResponse{
		Name:                    versionName(project, loc, kr, key, version),
		Ciphertext:              blob,
		CiphertextCrc32C:        wrapperspb.Int64(crc32cOf(blob)),
		VerifiedPlaintextCrc32C: req.GetPlaintextCrc32C() != nil,
		VerifiedAdditionalAuthenticatedDataCrc32C: req.GetAdditionalAuthenticatedDataCrc32C() != nil,
		ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) Decrypt(ctx context.Context, req *kmspb.DecryptRequest) (*kmspb.DecryptResponse, error) {
	project, loc, kr, key, ok := splitCryptoKeyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.authorizeCryptoKey(ctx, project, loc, kr, key, policy.PermCryptoKeyUseToDecrypt); err != nil {
		return nil, err
	}
	ck, err := s.keys.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		return nil, keyErr(err)
	}
	version, ct, err := kmsstore.DecodeVersionedCiphertext(req.GetCiphertext())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid ciphertext", 400))
	}
	if err := s.requireVersionEnabled(ctx, project, loc, kr, key, version); err != nil {
		return nil, err
	}
	keyMat, err := s.keys.KeyMaterial(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	pt, err := kmsstore.DecryptData(keyMat, ct, req.GetAdditionalAuthenticatedData())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "decryption failed", 400))
	}
	return &kmspb.DecryptResponse{
		Plaintext:       pt,
		PlaintextCrc32C: wrapperspb.Int64(crc32cOf(pt)),
		UsedPrimary:     version == ck.PrimaryVersion,
		ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

// RawEncrypt mirrors Encrypt for the portable AES-GCM primitive used by
// purpose RAW_ENCRYPT_DECRYPT. Unlike Encrypt the emitted blob is not versioned
// (RawDecrypt addresses the version explicitly) and the caller-supplied IV is
// honored; when absent a random 12-byte nonce is generated and returned. The
// authentication tag is appended to the ciphertext per Cloud KMS semantics.
func (s *Service) RawEncrypt(ctx context.Context, req *kmspb.RawEncryptRequest) (*kmspb.RawEncryptResponse, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, mapError(versionNotEnabledErr(version, v.State))
	}
	if !strings.Contains(v.Algorithm, "GCM") {
		return nil, mapError(model.NewProviderError("FailedPrecondition", "key is not for raw AES-GCM encryption", 400))
	}
	keyMat, err := s.keys.KeyMaterial(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if err := verifyCRC("plaintext_crc32c", req.GetPlaintextCrc32C(), req.GetPlaintext()); err != nil {
		return nil, err
	}
	if err := verifyCRC("additional_authenticated_data_crc32c", req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData()); err != nil {
		return nil, err
	}
	if err := verifyCRC("initialization_vector_crc32c", req.GetInitializationVectorCrc32C(), req.GetInitializationVector()); err != nil {
		return nil, err
	}
	iv := req.GetInitializationVector()
	if len(iv) == 0 {
		iv = make([]byte, 12)
		if _, err := rand.Read(iv); err != nil {
			return nil, mapError(model.NewProviderError("Internal", "random generation failed", 500))
		}
	}
	ct, err := kmsstore.RawEncryptGCM(keyMat, req.GetPlaintext(), req.GetAdditionalAuthenticatedData(), iv)
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "encryption failed", 400))
	}
	return &kmspb.RawEncryptResponse{
		Name:                       versionName(project, loc, kr, key, version),
		Ciphertext:                 ct,
		InitializationVector:       iv,
		TagLength:                  16,
		CiphertextCrc32C:           wrapperspb.Int64(crc32cOf(ct)),
		InitializationVectorCrc32C: wrapperspb.Int64(crc32cOf(iv)),
		VerifiedPlaintextCrc32C:    req.GetPlaintextCrc32C() != nil,
		VerifiedAdditionalAuthenticatedDataCrc32C: req.GetAdditionalAuthenticatedDataCrc32C() != nil,
		VerifiedInitializationVectorCrc32C:        req.GetInitializationVectorCrc32C() != nil,
		ProtectionLevel:                           kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

// RawDecrypt is the inverse of RawEncrypt: it uses the caller-supplied IV and
// AAD to authenticate and recover the plaintext.
func (s *Service) RawDecrypt(ctx context.Context, req *kmspb.RawDecryptRequest) (*kmspb.RawDecryptResponse, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, mapError(versionNotEnabledErr(version, v.State))
	}
	if !strings.Contains(v.Algorithm, "GCM") {
		return nil, mapError(model.NewProviderError("FailedPrecondition", "key is not for raw AES-GCM decryption", 400))
	}
	if tl := req.GetTagLength(); tl != 0 && tl != 16 {
		return nil, mapError(model.NewProviderError("InvalidArgument", "unsupported tag_length", 400))
	}
	keyMat, err := s.keys.KeyMaterial(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if err := verifyCRC("ciphertext_crc32c", req.GetCiphertextCrc32C(), req.GetCiphertext()); err != nil {
		return nil, err
	}
	if err := verifyCRC("additional_authenticated_data_crc32c", req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData()); err != nil {
		return nil, err
	}
	if err := verifyCRC("initialization_vector_crc32c", req.GetInitializationVectorCrc32C(), req.GetInitializationVector()); err != nil {
		return nil, err
	}
	pt, err := kmsstore.RawDecryptGCM(keyMat, req.GetCiphertext(), req.GetAdditionalAuthenticatedData(), req.GetInitializationVector())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "decryption failed", 400))
	}
	return &kmspb.RawDecryptResponse{
		Plaintext:                pt,
		PlaintextCrc32C:          wrapperspb.Int64(crc32cOf(pt)),
		VerifiedCiphertextCrc32C: req.GetCiphertextCrc32C() != nil,
		VerifiedAdditionalAuthenticatedDataCrc32C: req.GetAdditionalAuthenticatedDataCrc32C() != nil,
		VerifiedInitializationVectorCrc32C:        req.GetInitializationVectorCrc32C() != nil,
		ProtectionLevel:                           kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) AsymmetricSign(ctx context.Context, req *kmspb.AsymmetricSignRequest) (*kmspb.AsymmetricSignResponse, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.authorizeCryptoKey(ctx, project, loc, kr, key, policy.PermCryptoKeyUseToSign); err != nil {
		return nil, err
	}
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, mapError(versionNotEnabledErr(version, v.State))
	}
	digest, err := digestBytes(req.GetDigest(), req.GetData(), v.Algorithm)
	if err != nil {
		return nil, mapError(err)
	}
	if err := verifyCRC("digest_crc32c", req.GetDigestCrc32C(), digest); err != nil {
		return nil, err
	}
	if err := verifyCRC("data_crc32c", req.GetDataCrc32C(), req.GetData()); err != nil {
		return nil, err
	}
	priv, err := s.keys.PrivateKey(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	var sig []byte
	switch {
	case strings.HasPrefix(v.Algorithm, "RSA_SIGN"):
		sig, err = kmsstore.RSASign(priv, digest, v.Algorithm)
	case strings.HasPrefix(v.Algorithm, "EC_SIGN"):
		sig, err = kmsstore.ECSign(priv, digest)
	default:
		return nil, mapError(model.NewProviderError("FailedPrecondition", "key is not for asymmetric signing", 400))
	}
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "signing failed", 400))
	}
	return &kmspb.AsymmetricSignResponse{
		Name:                 versionName(project, loc, kr, key, version),
		Signature:            sig,
		SignatureCrc32C:      wrapperspb.Int64(crc32cOf(sig)),
		VerifiedDigestCrc32C: req.GetDigestCrc32C() != nil,
		VerifiedDataCrc32C:   req.GetDataCrc32C() != nil,
		ProtectionLevel:      kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) AsymmetricDecrypt(ctx context.Context, req *kmspb.AsymmetricDecryptRequest) (*kmspb.AsymmetricDecryptResponse, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.authorizeCryptoKey(ctx, project, loc, kr, key, policy.PermCryptoKeyUseToDecrypt); err != nil {
		return nil, err
	}
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, mapError(versionNotEnabledErr(version, v.State))
	}
	priv, err := s.keys.PrivateKey(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if !strings.HasPrefix(v.Algorithm, "RSA_DECRYPT") {
		return nil, mapError(model.NewProviderError("FailedPrecondition", "key is not for asymmetric decryption", 400))
	}
	if err := verifyCRC("ciphertext_crc32c", req.GetCiphertextCrc32C(), req.GetCiphertext()); err != nil {
		return nil, err
	}
	pt, err := kmsstore.RSADecryptOAEP(priv, req.GetCiphertext(), v.Algorithm)
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "decryption failed", 400))
	}
	return &kmspb.AsymmetricDecryptResponse{
		Plaintext:                pt,
		PlaintextCrc32C:          wrapperspb.Int64(crc32cOf(pt)),
		VerifiedCiphertextCrc32C: req.GetCiphertextCrc32C() != nil,
		ProtectionLevel:          kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

// GenerateRandomBytes returns length_bytes of cryptographically-secure random
// bytes for the given location, mirroring the real KMS API shape. The emulator
// always produces software-protection-level randomness regardless of the
// requested protection level.
func (s *Service) GenerateRandomBytes(ctx context.Context, req *kmspb.GenerateRandomBytesRequest) (*kmspb.GenerateRandomBytesResponse, error) {
	length := int(req.GetLengthBytes())
	if length <= 0 {
		return nil, mapError(model.NewProviderError("InvalidArgument", "length_bytes must be positive", 400))
	}
	data := make([]byte, length)
	if _, err := rand.Read(data); err != nil {
		return nil, mapError(model.NewProviderError("Internal", "random generation failed", 500))
	}
	return &kmspb.GenerateRandomBytesResponse{
		Data:       data,
		DataCrc32C: wrapperspb.Int64(crc32cOf(data)),
	}, nil
}

func (s *Service) GetPublicKey(ctx context.Context, req *kmspb.GetPublicKeyRequest) (*kmspb.PublicKey, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.authorizeCryptoKey(ctx, project, loc, kr, key, policy.PermCryptoKeyViewPublicKey); err != nil {
		return nil, err
	}
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, mapError(versionNotEnabledErr(version, v.State))
	}
	pub, err := s.keys.PublicKey(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	pemStr, err := kmsstore.PublicKeyPEM(pub)
	if err != nil {
		return nil, mapError(model.NewProviderError("Internal", "public key encode failed", 500))
	}
	return &kmspb.PublicKey{
		Pem:             pemStr,
		Algorithm:       algorithmToProto(v.Algorithm),
		PemCrc32C:       wrapperspb.Int64(crc32cOf([]byte(pemStr))),
		Name:            versionName(project, loc, kr, key, version),
		ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) MacSign(ctx context.Context, req *kmspb.MacSignRequest) (*kmspb.MacSignResponse, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.authorizeCryptoKey(ctx, project, loc, kr, key, policy.PermCryptoKeyUseToSign); err != nil {
		return nil, err
	}
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, mapError(versionNotEnabledErr(version, v.State))
	}
	if !strings.HasPrefix(v.Algorithm, "HMAC_") {
		return nil, mapError(model.NewProviderError("FailedPrecondition", "key is not for MAC", 400))
	}
	mat, err := s.keys.KeyMaterial(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if err := verifyCRC("data_crc32c", req.GetDataCrc32C(), req.GetData()); err != nil {
		return nil, err
	}
	mac, err := kmsstore.HMACSign(mat, req.GetData(), v.Algorithm)
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "mac sign failed", 400))
	}
	return &kmspb.MacSignResponse{
		Name:               versionName(project, loc, kr, key, version),
		Mac:                mac,
		MacCrc32C:          wrapperspb.Int64(crc32cOf(mac)),
		VerifiedDataCrc32C: req.GetDataCrc32C() != nil,
		ProtectionLevel:    kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) MacVerify(ctx context.Context, req *kmspb.MacVerifyRequest) (*kmspb.MacVerifyResponse, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.authorizeCryptoKey(ctx, project, loc, kr, key, policy.PermCryptoKeyUseToVerify); err != nil {
		return nil, err
	}
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, mapError(versionNotEnabledErr(version, v.State))
	}
	if !strings.HasPrefix(v.Algorithm, "HMAC_") {
		return nil, mapError(model.NewProviderError("FailedPrecondition", "key is not for MAC", 400))
	}
	mat, err := s.keys.KeyMaterial(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if err := verifyCRC("data_crc32c", req.GetDataCrc32C(), req.GetData()); err != nil {
		return nil, err
	}
	if err := verifyCRC("mac_crc32c", req.GetMacCrc32C(), req.GetMac()); err != nil {
		return nil, err
	}
	return &kmspb.MacVerifyResponse{
		Name:               versionName(project, loc, kr, key, version),
		Success:            kmsstore.HMACVerify(mat, req.GetData(), req.GetMac(), v.Algorithm),
		VerifiedDataCrc32C: req.GetDataCrc32C() != nil,
		VerifiedMacCrc32C:  req.GetMacCrc32C() != nil,
		ProtectionLevel:    kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

// ─── Deletion + RetiredResources ──────────────────────────────────────────────

// doneOperation is the terminal long-running operation shape the emulator
// returns for delete RPCs: Cloud KMS models deletes as LROs, but the emulator
// applies them synchronously. A non-nil Empty response is required so the
// generated long-running client can unmarshal the terminal result.
func doneOperation(name string) *longrunningpb.Operation {
	resp, _ := anypb.New(&emptypb.Empty{})
	return &longrunningpb.Operation{
		Name:   name + "/operations/delete",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: resp},
	}
}

// DeleteCryptoKeyVersion permanently removes a crypto-key version. Mirroring
// Cloud KMS, the version must already be DESTROYED, IMPORT_FAILED or
// GENERATION_FAILED.
func (s *Service) DeleteCryptoKeyVersion(ctx context.Context, req *kmspb.DeleteCryptoKeyVersionRequest) (*longrunningpb.Operation, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	s.promoteDestroyed(ctx, project, loc, kr, key)
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	switch v.State {
	case "DESTROYED", "IMPORT_FAILED", "GENERATION_FAILED":
	default:
		return nil, mapError(model.NewProviderError("FailedPrecondition", "CryptoKeyVersion must be DESTROYED before deletion", 400))
	}
	if err := s.keys.DeleteVersion(ctx, project, loc, kr, key, version); err != nil {
		return nil, versionErr(err)
	}
	return doneOperation(req.GetName()), nil
}

// DeleteCryptoKey permanently removes a crypto key after every version has been
// deleted, and records a RetiredResource so the name cannot be reused.
func (s *Service) DeleteCryptoKey(ctx context.Context, req *kmspb.DeleteCryptoKeyRequest) (*longrunningpb.Operation, error) {
	project, loc, kr, key, ok := splitCryptoKeyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if _, err := s.keys.GetCryptoKey(ctx, project, loc, kr, key); err != nil {
		return nil, keyErr(err)
	}
	versions, err := s.keys.ListVersions(ctx, project, loc, kr, key)
	if err != nil {
		return nil, versionErr(err)
	}
	if len(versions) > 0 {
		return nil, mapError(model.NewProviderError("FailedPrecondition", "all CryptoKeyVersions must be deleted before the CryptoKey", 400))
	}
	if err := s.keys.DeleteCryptoKey(ctx, project, loc, kr, key); err != nil {
		return nil, keyErr(err)
	}
	rr := retiredResourceData{
		Name:             retiredResourceName(project, loc, key),
		OriginalResource: req.GetName(),
		ResourceType:     "CRYPTO_KEY",
		DeleteTime:       clock.Now(),
	}
	data, _ := json.Marshal(rr)
	if err := s.resources.Upsert(ctx, project, store.GlobalRegion, store.ResourceEntry{
		Type: rtRetiredResource,
		ID:   retiredResourceID(loc, key),
		Data: data,
	}); err != nil {
		return nil, mapError(err)
	}
	return doneOperation(req.GetName()), nil
}

func retiredResourceID(location, keyID string) string { return location + "/" + keyID }

func retiredResourceName(project, location, keyID string) string {
	return "projects/" + project + "/locations/" + location + "/retiredResources/" + keyID
}

// splitRetiredResourceName parses
// "projects/{p}/locations/{l}/retiredResources/{id}".
func splitRetiredResourceName(name string) (project, location, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 6 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "retiredResources" {
		return parts[1], parts[3], parts[5], true
	}
	return "", "", "", false
}

func (s *Service) GetRetiredResource(ctx context.Context, req *kmspb.GetRetiredResourceRequest) (*kmspb.RetiredResource, error) {
	project, loc, id, ok := splitRetiredResourceName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtRetiredResource, retiredResourceID(loc, id))
	if err != nil {
		return nil, mapError(model.NewProviderError("NotFound", "retired resource not found", 404))
	}
	var rr retiredResourceData
	if err := json.Unmarshal(e.Data, &rr); err != nil {
		return nil, mapError(err)
	}
	return retiredResourceToProto(rr), nil
}

func (s *Service) ListRetiredResources(ctx context.Context, req *kmspb.ListRetiredResourcesRequest) (*kmspb.ListRetiredResourcesResponse, error) {
	project, loc, ok := splitLocationName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtRetiredResource, loc+"/")
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*kmspb.RetiredResource, 0, len(entries))
	for _, e := range entries {
		var rr retiredResourceData
		if err := json.Unmarshal(e.Data, &rr); err != nil {
			continue
		}
		out = append(out, retiredResourceToProto(rr))
	}
	return &kmspb.ListRetiredResourcesResponse{RetiredResources: out, TotalSize: int64(len(out))}, nil
}

func retiredResourceToProto(rr retiredResourceData) *kmspb.RetiredResource {
	out := &kmspb.RetiredResource{
		Name:             rr.Name,
		OriginalResource: rr.OriginalResource,
		ResourceType:     rr.ResourceType,
	}
	if !rr.DeleteTime.IsZero() {
		out.DeleteTime = timestamppb.New(rr.DeleteTime)
	}
	return out
}

// ─── ImportJobs ───────────────────────────────────────────────────────────────

// splitImportJobName parses
// "projects/{p}/locations/{l}/keyRings/{kr}/importJobs/{id}".
func splitImportJobName(name string) (project, location, kr, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) == 8 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "keyRings" && parts[6] == "importJobs" {
		return parts[1], parts[3], parts[5], parts[7], true
	}
	return "", "", "", "", false
}

func importJobID(location, kr, id string) string { return location + "/" + kr + "/" + id }

func importJobName(project, location, kr, id string) string {
	return keyRingName(project, location, kr) + "/importJobs/" + id
}

// CreateImportJob generates an RSA-3072 wrapping key and stores the import job.
// The private key is DEK-wrapped at rest; the public key is returned in PEM.
// ImportCryptoKeyVersion (which would consume the wrapping key) is not
// implemented, so the job is a real, addressable control-plane resource.
func (s *Service) CreateImportJob(ctx context.Context, req *kmspb.CreateImportJobRequest) (*kmspb.ImportJob, error) {
	project, loc, kr, ok := splitKeyRingName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	id := req.GetImportJobId()
	if id == "" {
		if ij := req.GetImportJob(); ij != nil {
			_, _, _, id, _ = splitImportJobName(ij.GetName())
		}
	}
	if id == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing importJobId", 400))
	}
	method := req.GetImportJob().GetImportMethod()
	if method == kmspb.ImportJob_IMPORT_METHOD_UNSPECIFIED {
		return nil, mapError(model.NewProviderError("InvalidArgument", "import_method is required", 400))
	}
	privDER, pubDER, err := kmsstore.GenerateRSAKeyPair(3072)
	if err != nil {
		return nil, mapError(model.NewProviderError("Internal", "wrapping key generation failed", 500))
	}
	pemStr, err := kmsstore.PublicKeyPEM(pubDER)
	if err != nil {
		return nil, mapError(model.NewProviderError("Internal", "public key encode failed", 500))
	}
	dek, err := s.keys.ServerDEK(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	wrappedPriv, err := kmsstore.EncryptData(dek, privDER, []byte(id))
	if err != nil {
		return nil, mapError(model.NewProviderError("Internal", "wrapping key protection failed", 500))
	}
	now := clock.Now()
	ij := importJobData{
		Name:              importJobName(project, loc, kr, id),
		ImportMethod:      method.String(),
		ProtectionLevel:   int32(req.GetImportJob().GetProtectionLevel()),
		CreateTime:        now,
		GenerateTime:      now,
		ExpireTime:        now.Add(72 * time.Hour),
		State:             "ACTIVE",
		PublicKeyPEM:      pemStr,
		WrappedPrivateKey: wrappedPriv,
	}
	data, _ := json.Marshal(ij)
	if err := s.resources.Upsert(ctx, project, store.GlobalRegion, store.ResourceEntry{
		Type: rtImportJob,
		ID:   importJobID(loc, kr, id),
		Data: data,
	}); err != nil {
		return nil, mapError(err)
	}
	return importJobToProto(ij), nil
}

func (s *Service) GetImportJob(ctx context.Context, req *kmspb.GetImportJobRequest) (*kmspb.ImportJob, error) {
	project, loc, kr, id, ok := splitImportJobName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtImportJob, importJobID(loc, kr, id))
	if err != nil {
		return nil, mapError(model.NewProviderError("NotFound", "import job not found", 404))
	}
	var ij importJobData
	if err := json.Unmarshal(e.Data, &ij); err != nil {
		return nil, mapError(err)
	}
	return importJobToProto(ij), nil
}

func (s *Service) ListImportJobs(ctx context.Context, req *kmspb.ListImportJobsRequest) (*kmspb.ListImportJobsResponse, error) {
	project, loc, kr, ok := splitKeyRingName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtImportJob, loc+"/"+kr+"/")
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*kmspb.ImportJob, 0, len(entries))
	for _, e := range entries {
		var ij importJobData
		if err := json.Unmarshal(e.Data, &ij); err != nil {
			continue
		}
		out = append(out, importJobToProto(ij))
	}
	return &kmspb.ListImportJobsResponse{ImportJobs: out, TotalSize: int32(len(out))}, nil
}

func importJobToProto(ij importJobData) *kmspb.ImportJob {
	out := &kmspb.ImportJob{
		Name:            ij.Name,
		ImportMethod:    importMethodToProto(ij.ImportMethod),
		ProtectionLevel: kmspb.ProtectionLevel(ij.ProtectionLevel),
		State:           kmspb.ImportJob_ImportJobState(kmspb.ImportJob_ImportJobState_value[ij.State]),
		PublicKeyFormat: kmspb.PublicKey_PEM,
	}
	if !ij.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(ij.CreateTime)
	}
	if !ij.GenerateTime.IsZero() {
		out.GenerateTime = timestamppb.New(ij.GenerateTime)
	}
	if !ij.ExpireTime.IsZero() {
		out.ExpireTime = timestamppb.New(ij.ExpireTime)
	}
	if ij.PublicKeyPEM != "" {
		out.PublicKey = &kmspb.ImportJob_WrappingPublicKey{Pem: ij.PublicKeyPEM}
	}
	return out
}

func importMethodToProto(s string) kmspb.ImportJob_ImportMethod {
	if v, ok := kmspb.ImportJob_ImportMethod_value[s]; ok {
		return kmspb.ImportJob_ImportMethod(v)
	}
	return kmspb.ImportJob_IMPORT_METHOD_UNSPECIFIED
}

// ─── IAM (google.iam.v1.IAMPolicy over keyrings/keys) ─────────────────────────

// Owns reports whether the KMS service handles IAM for this resource name.
func (s *Service) Owns(resource string) bool {
	_, _, _, ok := s.parseIamResource(resource)
	return ok
}

// parseIamResource resolves a keyring/crypto-key IAM resource name to its
// policy resource type + id (location-qualified so IDs stay unique within a
// project). Version names are not IAM resources (real KMS has no version-level
// IAM), so they resolve to ok=false.
func (s *Service) parseIamResource(resource string) (project, policyType, id string, ok bool) {
	if p, l, kr, ok := splitKeyRingName(resource); ok {
		return p, rtKeyRingPolicy, l + "/" + kr, true
	}
	if p, l, kr, k, ok := splitCryptoKeyName(resource); ok {
		return p, rtCryptoKeyPolicy, l + "/" + kr + "/" + k, true
	}
	return "", "", "", false
}

// requireIamResource verifies the KMS resource backing an IAM request exists.
func (s *Service) requireIamResource(ctx context.Context, resource string) error {
	if p, l, kr, ok := splitKeyRingName(resource); ok {
		if _, err := s.keys.GetKeyRing(ctx, p, l, kr); err != nil {
			if errors.Is(err, kmsstore.ErrNoSuchKeyRing) {
				return model.NewProviderError("NotFound", "key ring not found", 404)
			}
			return err
		}
		return nil
	}
	if p, l, kr, k, ok := splitCryptoKeyName(resource); ok {
		if _, err := s.keys.GetCryptoKey(ctx, p, l, kr, k); err != nil {
			if errors.Is(err, kmsstore.ErrNoSuchCryptoKey) {
				return model.NewProviderError("NotFound", "crypto key not found", 404)
			}
			return err
		}
		return nil
	}
	return model.NewProviderError("InvalidArgument", "invalid resource name", 400)
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project, rt, id, ok := s.parseIamResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireIamResource(ctx, req.GetResource()); err != nil {
		return nil, mapError(err)
	}
	return policyToProto(policy.Load(ctx, s.resources, project, rt, id)), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project, rt, id, ok := s.parseIamResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireIamResource(ctx, req.GetResource()); err != nil {
		return nil, mapError(err)
	}
	pol, err := policy.Set(ctx, s.resources, project, rt, id, protoPolicyToBody(req.GetPolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	_, _, _, ok := s.parseIamResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireIamResource(ctx, req.GetResource()); err != nil {
		return nil, mapError(err)
	}
	return &iampb.TestIamPermissionsResponse{Permissions: policy.TestPermissions(req.GetPermissions())}, nil
}

// ─── proto ↔ internal transcoding ─────────────────────────────────────────────

func keyRingToProto(project, location string, kr kmsstore.KeyRing) *kmspb.KeyRing {
	out := &kmspb.KeyRing{Name: keyRingName(project, location, kr.ID)}
	if !kr.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(kr.CreateTime)
	}
	return out
}

// cryptoKeyToProto renders a CryptoKey. primaryVersion is the key's primary
// version (see primaryVersion); its destruction timestamps are carried through
// when the primary is DESTROY_SCHEDULED/DESTROYED.
func cryptoKeyToProto(project string, k kmsstore.CryptoKey, primaryVersion kmsstore.Version) *kmspb.CryptoKey {
	primaryState := primaryVersion.State
	if primaryState == "" {
		primaryState = "ENABLED"
	}
	primary := &kmspb.CryptoKeyVersion{
		Name:            versionName(project, k.Location, k.KeyRingID, k.ID, k.PrimaryVersion),
		State:           stateToProto(primaryState),
		Algorithm:       algorithmToProto(k.Algorithm),
		ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
	}
	if primaryVersion.State == "DESTROY_SCHEDULED" && !primaryVersion.DestroyTime.IsZero() {
		primary.DestroyTime = timestamppb.New(primaryVersion.DestroyTime)
	}
	if primaryVersion.State == "DESTROYED" && !primaryVersion.DestroyEventTime.IsZero() {
		primary.DestroyEventTime = timestamppb.New(primaryVersion.DestroyEventTime)
	}
	out := &kmspb.CryptoKey{
		Name:    cryptoKeyName(project, k.Location, k.KeyRingID, k.ID),
		Purpose: purposeToProto(k.Purpose),
		Primary: primary,
		VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
			Algorithm:       algorithmToProto(k.Algorithm),
			ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
		},
	}
	if !k.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(k.CreateTime)
	}
	if len(k.Labels) > 0 {
		out.Labels = k.Labels
	}
	if k.RotationPeriod > 0 {
		out.RotationSchedule = &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(k.RotationPeriod)}
	}
	if !k.NextRotationTime.IsZero() {
		out.NextRotationTime = timestamppb.New(k.NextRotationTime)
	}
	return out
}

func versionToProto(project, location, kr, key string, v kmsstore.Version) *kmspb.CryptoKeyVersion {
	out := &kmspb.CryptoKeyVersion{
		Name:            versionName(project, location, kr, key, v.Version),
		State:           stateToProto(v.State),
		Algorithm:       algorithmToProto(v.Algorithm),
		ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
	}
	if !v.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(v.CreateTime)
	}
	// destroy_time is present only while DESTROY_SCHEDULED; destroy_event_time
	// only once DESTROYED (both output-only in Cloud KMS).
	if v.State == "DESTROY_SCHEDULED" && !v.DestroyTime.IsZero() {
		out.DestroyTime = timestamppb.New(v.DestroyTime)
	}
	if v.State == "DESTROYED" && !v.DestroyEventTime.IsZero() {
		out.DestroyEventTime = timestamppb.New(v.DestroyEventTime)
	}
	return out
}

// defaultAlgorithmForPurpose maps a GCP KMS purpose to its default algorithm
// (mirrors the REST provider).
func defaultAlgorithmForPurpose(purpose string) string {
	switch purpose {
	case "ASYMMETRIC_SIGN":
		return "RSA_SIGN_PKCS1_2048_SHA256"
	case "ASYMMETRIC_DECRYPT":
		return "RSA_DECRYPT_OAEP_2048_SHA256"
	case "MAC":
		return "HMAC_SHA256"
	case "RAW_ENCRYPT_DECRYPT":
		return "AES_256_GCM"
	default:
		return "GOOGLE_SYMMETRIC_ENCRYPTION"
	}
}

func purposeFromProto(p kmspb.CryptoKey_CryptoKeyPurpose) string {
	if p == kmspb.CryptoKey_CRYPTO_KEY_PURPOSE_UNSPECIFIED {
		return "ENCRYPT_DECRYPT"
	}
	return p.String()
}

func purposeToProto(s string) kmspb.CryptoKey_CryptoKeyPurpose {
	if v, ok := kmspb.CryptoKey_CryptoKeyPurpose_value[s]; ok {
		return kmspb.CryptoKey_CryptoKeyPurpose(v)
	}
	return kmspb.CryptoKey_CRYPTO_KEY_PURPOSE_UNSPECIFIED
}

func algorithmFromProto(a kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm) string {
	if a == kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED {
		return ""
	}
	return a.String()
}

func algorithmToProto(s string) kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm {
	if v, ok := kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm_value[s]; ok {
		return kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm(v)
	}
	return kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED
}

func stateFromProto(s kmspb.CryptoKeyVersion_CryptoKeyVersionState) string {
	switch s {
	case kmspb.CryptoKeyVersion_ENABLED:
		return "ENABLED"
	case kmspb.CryptoKeyVersion_DISABLED:
		return "DISABLED"
	case kmspb.CryptoKeyVersion_DESTROYED:
		return "DESTROYED"
	}
	return ""
}

func stateToProto(s string) kmspb.CryptoKeyVersion_CryptoKeyVersionState {
	if v, ok := kmspb.CryptoKeyVersion_CryptoKeyVersionState_value[s]; ok {
		return kmspb.CryptoKeyVersion_CryptoKeyVersionState(v)
	}
	return kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_STATE_UNSPECIFIED
}

// digestBytes extracts the signing digest from the request: the Digest oneof
// (sha256/sha384/sha512) takes precedence, then the raw data field is hashed
// with the algorithm's digest. Mirrors the REST provider's digest handling and
// additionally supports the proto's raw-data form.
func digestBytes(d *kmspb.Digest, data []byte, algo string) ([]byte, error) {
	if d != nil {
		if dg := d.GetSha256(); len(dg) > 0 {
			return dg, nil
		}
		if dg := d.GetSha384(); len(dg) > 0 {
			return dg, nil
		}
		if dg := d.GetSha512(); len(dg) > 0 {
			return dg, nil
		}
	}
	if len(data) > 0 {
		h := signHashFor(algo).New()
		if _, err := h.Write(data); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}
	return nil, model.NewProviderError("InvalidArgument", "digest is required", 400)
}

// signHashFor mirrors the store's digest-hash selection for the raw-data path.
func signHashFor(algo string) crypto.Hash {
	switch {
	case strings.Contains(algo, "SHA512"):
		return crypto.SHA512
	case strings.Contains(algo, "SHA384"):
		return crypto.SHA384
	default:
		return crypto.SHA256
	}
}

// crc32cOf returns the CRC32C-Castagnoli checksum of b as an int64
// (google.protobuf.Int64Value encoding).
func crc32cOf(b []byte) int64 {
	return int64(crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
}

// verifyCRC enforces the Cloud KMS integrity contract: when the caller supplies
// a CRC32C checksum for an input field, it must match the computed checksum of
// that input. A mismatch fails the request with InvalidArgument (400); real KMS
// never reports an input as verified when the checksums differ. A nil checksum
// means the caller opted out of verification and is accepted.
func verifyCRC(field string, supplied *wrapperspb.Int64Value, data []byte) error {
	if supplied == nil {
		return nil
	}
	if supplied.GetValue() != crc32cOf(data) {
		return mapError(model.NewProviderError("InvalidArgument", field+" checksum mismatch", 400))
	}
	return nil
}

func protoPolicyToBody(p *iampb.Policy) map[string]any {
	body := map[string]any{}
	if p == nil {
		return body
	}
	bindings := make([]any, 0, len(p.GetBindings()))
	for _, b := range p.GetBindings() {
		members := make([]any, 0, len(b.GetMembers()))
		for _, m := range b.GetMembers() {
			members = append(members, m)
		}
		bindings = append(bindings, map[string]any{"role": b.GetRole(), "members": members})
	}
	body["bindings"] = bindings
	if et := p.GetEtag(); len(et) > 0 {
		body["etag"] = string(et)
	}
	if p.GetVersion() != 0 {
		body["version"] = int(p.GetVersion())
	}
	return body
}

func policyToProto(p policy.Policy) *iampb.Policy {
	out := &iampb.Policy{Version: int32(p.Version)}
	if p.Etag != "" {
		out.Etag = []byte(p.Etag)
	}
	for _, b := range p.Bindings {
		m, ok := b.(map[string]any)
		if !ok {
			continue
		}
		binding := &iampb.Binding{}
		binding.Role, _ = m["role"].(string)
		for _, v := range toStrings(m["members"]) {
			binding.Members = append(binding.Members, v)
		}
		out.Bindings = append(out.Bindings, binding)
	}
	return out
}

func toStrings(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
