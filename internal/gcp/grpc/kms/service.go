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
	"errors"
	"hash/crc32"
	"strings"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"

	"jaiscloud/internal/clock"
	gcpcrypto "jaiscloud/internal/gcp/crypto"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Resource-type strings for KMS IAM policies stored in the shared resource
// store. These mirror the Pub/Sub / Secret Manager policy types; KMS has no
// REST IAM provider, so these types are gRPC-only.
const (
	rtKeyRingPolicy   = "gcp_keyring_policy"
	rtCryptoKeyPolicy = "gcp_cryptokey_policy"
	rtVersionPolicy   = "gcp_cryptokeyversion_policy"
)

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

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func keyRingErr(err error) error {
	if errors.Is(err, kmsstore.ErrNoSuchKeyRing) {
		return mapError(model.NewProviderError("NotFound", "key ring not found", 404))
	}
	return mapError(err)
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
	page, next := paging.Page(keys, func(k kmsstore.CryptoKey) string { return k.ID },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	out := make([]*kmspb.CryptoKey, 0, len(page))
	for _, k := range page {
		out = append(out, cryptoKeyToProto(project, k))
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
	if ck := req.GetCryptoKey(); ck != nil {
		purpose = purposeFromProto(ck.GetPurpose())
		if vt := ck.GetVersionTemplate(); vt != nil {
			algorithm = algorithmFromProto(vt.GetAlgorithm())
		}
	}
	if algorithm == "" {
		algorithm = defaultAlgorithmForPurpose(purpose)
	}
	now := clock.Now()
	ck := kmsstore.CryptoKey{Location: loc, KeyRingID: kr, ID: key, Purpose: purpose, CreateTime: now, PrimaryVersion: "1", Algorithm: algorithm}
	if err := s.keys.CreateCryptoKey(ctx, project, loc, kr, key, ck); err != nil {
		if errors.Is(err, kmsstore.ErrAlreadyExists) {
			return nil, mapError(model.NewProviderError("AlreadyExists", "crypto key already exists", 409))
		}
		return nil, mapError(err)
	}
	return cryptoKeyToProto(project, ck), nil
}

func (s *Service) GetCryptoKey(ctx context.Context, req *kmspb.GetCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	project, loc, kr, key, ok := splitCryptoKeyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	k, err := s.keys.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		return nil, keyErr(err)
	}
	return cryptoKeyToProto(project, k), nil
}

// UpdateCryptoKey reads back the current key. Purpose and algorithm are
// immutable, and the store persists no other mutable metadata (labels,
// rotation schedule), so this is effectively an idempotent read matching the
// emulator's minimal surface.
func (s *Service) UpdateCryptoKey(ctx context.Context, req *kmspb.UpdateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	ck := req.GetCryptoKey()
	if ck == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "crypto key is required", 400))
	}
	project, loc, kr, key, ok := splitCryptoKeyName(ck.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	k, err := s.keys.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		return nil, keyErr(err)
	}
	return cryptoKeyToProto(project, k), nil
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
	return cryptoKeyToProto(project, ck), nil
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
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

func (s *Service) DestroyCryptoKeyVersion(ctx context.Context, req *kmspb.DestroyCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	return s.setVersionState(ctx, req.GetName(), "DESTROYED")
}

func (s *Service) RestoreCryptoKeyVersion(ctx context.Context, req *kmspb.RestoreCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	return s.setVersionState(ctx, req.GetName(), "DISABLED")
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
	if state := stateFromProto(p.GetState()); state != "" {
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

func (s *Service) setVersionState(ctx context.Context, name, state string) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, version, ok := splitVersionName(name)
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.keys.UpdateVersionState(ctx, project, loc, kr, key, version, state); err != nil {
		return nil, versionErr(err)
	}
	v, _ := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	return versionToProto(project, loc, kr, key, v), nil
}

// ─── Crypto operations ────────────────────────────────────────────────────────

func (s *Service) Encrypt(ctx context.Context, req *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error) {
	project, loc, kr, key, version, ok := splitKeyOrVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	ck, err := s.keys.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		return nil, keyErr(err)
	}
	if version == "" {
		version = ck.PrimaryVersion
	}
	keyMat, err := s.keys.KeyMaterial(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
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
	ck, err := s.keys.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		return nil, keyErr(err)
	}
	version, ct, err := kmsstore.DecodeVersionedCiphertext(req.GetCiphertext())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid ciphertext", 400))
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

func (s *Service) AsymmetricSign(ctx context.Context, req *kmspb.AsymmetricSignRequest) (*kmspb.AsymmetricSignResponse, error) {
	project, loc, kr, key, version, ok := splitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	digest, err := digestBytes(req.GetDigest(), req.GetData(), v.Algorithm)
	if err != nil {
		return nil, mapError(err)
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
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	priv, err := s.keys.PrivateKey(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if !strings.HasPrefix(v.Algorithm, "RSA_DECRYPT") {
		return nil, mapError(model.NewProviderError("FailedPrecondition", "key is not for asymmetric decryption", 400))
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
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
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
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if !strings.HasPrefix(v.Algorithm, "HMAC_") {
		return nil, mapError(model.NewProviderError("FailedPrecondition", "key is not for MAC", 400))
	}
	mat, err := s.keys.KeyMaterial(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
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
	v, err := s.keys.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	if !strings.HasPrefix(v.Algorithm, "HMAC_") {
		return nil, mapError(model.NewProviderError("FailedPrecondition", "key is not for MAC", 400))
	}
	mat, err := s.keys.KeyMaterial(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	return &kmspb.MacVerifyResponse{
		Name:               versionName(project, loc, kr, key, version),
		Success:            kmsstore.HMACVerify(mat, req.GetData(), req.GetMac(), v.Algorithm),
		VerifiedDataCrc32C: req.GetDataCrc32C() != nil,
		VerifiedMacCrc32C:  req.GetMacCrc32C() != nil,
		ProtectionLevel:    kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

// ─── IAM (google.iam.v1.IAMPolicy over keyrings/keys/versions) ────────────────

// Owns reports whether the KMS service handles IAM for this resource name.
func (s *Service) Owns(resource string) bool {
	_, _, _, ok := s.parseIamResource(resource)
	return ok
}

// parseIamResource resolves a keyring/crypto-key/version IAM resource name to
// its policy resource type + id (location-qualified so IDs stay unique within a
// project).
func (s *Service) parseIamResource(resource string) (project, policyType, id string, ok bool) {
	if p, l, kr, ok := splitKeyRingName(resource); ok {
		return p, rtKeyRingPolicy, l + "/" + kr, true
	}
	if p, l, kr, k, ok := splitCryptoKeyName(resource); ok {
		return p, rtCryptoKeyPolicy, l + "/" + kr + "/" + k, true
	}
	if p, l, kr, k, v, ok := splitVersionName(resource); ok {
		return p, rtVersionPolicy, l + "/" + kr + "/" + k + "/" + v, true
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
	if p, l, kr, k, v, ok := splitVersionName(resource); ok {
		if _, err := s.keys.GetVersion(ctx, p, l, kr, k, v); err != nil {
			if errors.Is(err, kmsstore.ErrNoSuchVersion) {
				return model.NewProviderError("NotFound", "crypto key version not found", 404)
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

func cryptoKeyToProto(project string, k kmsstore.CryptoKey) *kmspb.CryptoKey {
	out := &kmspb.CryptoKey{
		Name:    cryptoKeyName(project, k.Location, k.KeyRingID, k.ID),
		Purpose: purposeToProto(k.Purpose),
		Primary: &kmspb.CryptoKeyVersion{
			Name:            versionName(project, k.Location, k.KeyRingID, k.ID, k.PrimaryVersion),
			State:           kmspb.CryptoKeyVersion_ENABLED,
			Algorithm:       algorithmToProto(k.Algorithm),
			ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
		},
		VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
			Algorithm:       algorithmToProto(k.Algorithm),
			ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
		},
	}
	if !k.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(k.CreateTime)
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
