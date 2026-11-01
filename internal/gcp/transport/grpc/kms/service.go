// Package kms is the gRPC transport adapter for Cloud KMS. It maps the official
// google.cloud.kms.v1 protos onto the transport-neutral core
// (internal/gcp/service/kms) and serves the google.iam.v1.IAMPolicy surface for
// key-ring / crypto-key IAM.
package kms

import (
	"context"
	"crypto"
	"hash/crc32"
	"strings"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/policy"
	kmscore "jaiscloud/internal/gcp/service/kms"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
)

// Service implements kmspb.KeyManagementServiceServer and
// iampb.IAMPolicyServer over the shared KMS core.
type Service struct {
	kmspb.UnimplementedKeyManagementServiceServer
	iampb.UnimplementedIAMPolicyServer

	core        *kmscore.Service
	defaultProj string
}

// NewService returns a KMS gRPC adapter over the shared core.
func NewService(core *kmscore.Service, defaultProj string) *Service {
	return &Service{core: core, defaultProj: defaultProj}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// kmsListQuery renders the documented ListXRequest filter/orderBy/page fields
// into the core's ListQuery.
func kmsListQuery(filter, orderBy string, pageSize int32, pageToken string) kmscore.ListQuery {
	return kmscore.ListQuery{
		Filter:    filter,
		OrderBy:   orderBy,
		PageSize:  int(pageSize),
		PageToken: pageToken,
	}
}

// splitLocationParent resolves project + location from a ListKeyRings /
// CreateKeyRing parent. A bare location id falls back to the metadata-derived
// project.
func (s *Service) splitLocationParent(ctx context.Context, parent string) (project, location string, ok bool) {
	if p, l, ok := kmscore.SplitLocationName(parent); ok {
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
	page, next, total, err := s.core.PageKeyRings(ctx, project, loc, kmsListQuery(req.GetFilter(), req.GetOrderBy(), req.GetPageSize(), req.GetPageToken()))
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*kmspb.KeyRing, 0, len(page))
	for _, kr := range page {
		out = append(out, keyRingToProto(project, loc, kr))
	}
	return &kmspb.ListKeyRingsResponse{KeyRings: out, NextPageToken: next, TotalSize: int32(total)}, nil
}

func (s *Service) CreateKeyRing(ctx context.Context, req *kmspb.CreateKeyRingRequest) (*kmspb.KeyRing, error) {
	project, loc, ok := s.splitLocationParent(ctx, req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	id := req.GetKeyRingId()
	if id == "" {
		if req.GetKeyRing() != nil {
			_, _, id, _ = kmscore.SplitKeyRingName(req.GetKeyRing().GetName())
		}
	}
	if id == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing keyRingId", 400))
	}
	kr, err := s.core.CreateKeyRing(ctx, project, loc, id)
	if err != nil {
		return nil, mapError(err)
	}
	return keyRingToProto(project, loc, kr), nil
}

func (s *Service) GetKeyRing(ctx context.Context, req *kmspb.GetKeyRingRequest) (*kmspb.KeyRing, error) {
	project, loc, id, ok := kmscore.SplitKeyRingName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	kr, err := s.core.GetKeyRing(ctx, project, loc, id)
	if err != nil {
		return nil, mapError(err)
	}
	return keyRingToProto(project, loc, kr), nil
}

// ─── CryptoKeys ───────────────────────────────────────────────────────────────

func (s *Service) ListCryptoKeys(ctx context.Context, req *kmspb.ListCryptoKeysRequest) (*kmspb.ListCryptoKeysResponse, error) {
	project, loc, kr, ok := kmscore.SplitKeyRingName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	page, next, total, err := s.core.PageCryptoKeys(ctx, project, loc, kr, kmsListQuery(req.GetFilter(), req.GetOrderBy(), req.GetPageSize(), req.GetPageToken()))
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*kmspb.CryptoKey, 0, len(page))
	for _, k := range page {
		out = append(out, cryptoKeyToProto(project, k, s.core.PrimaryVersion(ctx, project, k)))
	}
	return &kmspb.ListCryptoKeysResponse{CryptoKeys: out, NextPageToken: next, TotalSize: int32(total)}, nil
}

func (s *Service) CreateCryptoKey(ctx context.Context, req *kmspb.CreateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	project, loc, kr, ok := kmscore.SplitKeyRingName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	key := req.GetCryptoKeyId()
	if key == "" {
		if ck := req.GetCryptoKey(); ck != nil {
			_, _, _, key, _ = kmscore.SplitCryptoKeyName(ck.GetName())
		}
	}
	if key == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing cryptoKeyId", 400))
	}
	purpose := "ENCRYPT_DECRYPT"
	algorithm := ""
	in := kmscore.CryptoKeyInput{}
	if ck := req.GetCryptoKey(); ck != nil {
		purpose = purposeFromProto(ck.GetPurpose())
		if vt := ck.GetVersionTemplate(); vt != nil {
			algorithm = algorithmFromProto(vt.GetAlgorithm())
			in.ProtectionLevel = protectionLevelFromProto(vt.GetProtectionLevel())
		}
		in.Labels = ck.GetLabels()
		if rp := ck.GetRotationPeriod(); rp != nil {
			d := rp.AsDuration()
			if d <= 0 {
				return nil, mapError(model.NewProviderError("InvalidArgument", "rotation period must be positive", 400))
			}
			in.RotationPeriod = d
		}
		if nrt := ck.GetNextRotationTime(); nrt != nil {
			t := nrt.AsTime()
			in.NextRotationTime = &t
		}
		in.ImportOnly = ck.GetImportOnly()
	}
	in.SkipInitialVersion = req.GetSkipInitialVersionCreation()
	in.TrustedWrappingEnabled = req.GetTrustedWrappingEnabled()
	in.Purpose = purpose
	in.Algorithm = algorithm
	ck, err := s.core.CreateCryptoKey(ctx, project, loc, kr, key, in)
	if err != nil {
		return nil, mapError(err)
	}
	return cryptoKeyToProto(project, ck, kmsstore.Version{Version: ck.PrimaryVersion, State: "ENABLED", Algorithm: ck.Algorithm, CreateTime: ck.CreateTime, ProtectionLevel: ck.ProtectionLevel}), nil
}

func (s *Service) GetCryptoKey(ctx context.Context, req *kmspb.GetCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	project, loc, kr, key, ok := kmscore.SplitCryptoKeyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	k, err := s.core.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		return nil, mapError(err)
	}
	return cryptoKeyToProto(project, k, s.core.PrimaryVersion(ctx, project, k)), nil
}

func (s *Service) UpdateCryptoKey(ctx context.Context, req *kmspb.UpdateCryptoKeyRequest) (*kmspb.CryptoKey, error) {
	ck := req.GetCryptoKey()
	if ck == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "crypto key is required", 400))
	}
	project, loc, kr, key, ok := kmscore.SplitCryptoKeyName(ck.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	// UpdateCryptoKeyRequest.update_mask is REQUIRED; distinguish an omitted mask
	// (nil, rejected by the core) from an explicitly empty one (a no-op).
	var maskPaths []string
	if req.GetUpdateMask() != nil {
		maskPaths = req.GetUpdateMask().GetPaths()
		if maskPaths == nil {
			maskPaths = []string{}
		}
	}
	in := kmscore.CryptoKeyUpdate{UpdateMask: maskPaths, Labels: ck.GetLabels()}
	if rp := ck.GetRotationPeriod(); rp != nil {
		d := rp.AsDuration()
		in.RotationPeriod = &d
	}
	if nrt := ck.GetNextRotationTime(); nrt != nil {
		t := nrt.AsTime()
		in.NextRotationTime = &t
	}
	k, err := s.core.UpdateCryptoKey(ctx, project, loc, kr, key, in)
	if err != nil {
		return nil, mapError(err)
	}
	return cryptoKeyToProto(project, k, s.core.PrimaryVersion(ctx, project, k)), nil
}

func (s *Service) UpdateCryptoKeyPrimaryVersion(ctx context.Context, req *kmspb.UpdateCryptoKeyPrimaryVersionRequest) (*kmspb.CryptoKey, error) {
	project, loc, kr, key, ok := kmscore.SplitCryptoKeyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if req.GetCryptoKeyVersionId() == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing cryptoKeyVersionId", 400))
	}
	ck, err := s.core.SetPrimaryVersion(ctx, project, loc, kr, key, req.GetCryptoKeyVersionId())
	if err != nil {
		return nil, mapError(err)
	}
	return cryptoKeyToProto(project, ck, s.core.PrimaryVersion(ctx, project, ck)), nil
}

// ─── CryptoKeyVersions ────────────────────────────────────────────────────────

func (s *Service) CreateCryptoKeyVersion(ctx context.Context, req *kmspb.CreateCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, ok := kmscore.SplitCryptoKeyName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	in := kmscore.CreateVersionInput{
		TrustedWrappingEnabled:         req.GetCryptoKeyVersion().GetTrustedWrappingEnabled(),
		ExternalProtectionLevelOptions: req.GetCryptoKeyVersion().GetExternalProtectionLevelOptions() != nil,
	}
	v, err := s.core.CreateVersion(ctx, project, loc, kr, key, in)
	if err != nil {
		return nil, mapError(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

func (s *Service) ListCryptoKeyVersions(ctx context.Context, req *kmspb.ListCryptoKeyVersionsRequest) (*kmspb.ListCryptoKeyVersionsResponse, error) {
	project, loc, kr, key, ok := kmscore.SplitCryptoKeyName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	page, next, total, err := s.core.PageVersions(ctx, project, loc, kr, key, kmsListQuery(req.GetFilter(), req.GetOrderBy(), req.GetPageSize(), req.GetPageToken()))
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*kmspb.CryptoKeyVersion, 0, len(page))
	for _, v := range page {
		out = append(out, versionToProto(project, loc, kr, key, v))
	}
	return &kmspb.ListCryptoKeyVersionsResponse{CryptoKeyVersions: out, NextPageToken: next, TotalSize: int32(total)}, nil
}

func (s *Service) GetCryptoKeyVersion(ctx context.Context, req *kmspb.GetCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	v, err := s.core.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, mapError(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

func (s *Service) DestroyCryptoKeyVersion(ctx context.Context, req *kmspb.DestroyCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	v, err := s.core.DestroyVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, mapError(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

func (s *Service) RestoreCryptoKeyVersion(ctx context.Context, req *kmspb.RestoreCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	v, err := s.core.RestoreVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, mapError(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

func (s *Service) UpdateCryptoKeyVersion(ctx context.Context, req *kmspb.UpdateCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	p := req.GetCryptoKeyVersion()
	if p == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "crypto key version is required", 400))
	}
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(p.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if p.GetState() != kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_STATE_UNSPECIFIED {
		state := stateFromProto(p.GetState())
		if state == "" {
			return nil, mapError(model.NewProviderError("InvalidArgument", "cryptoKeyVersion.state must be ENABLED or DISABLED", 400))
		}
		if _, err := s.core.UpdateVersionState(ctx, project, loc, kr, key, version, state); err != nil {
			return nil, mapError(err)
		}
	}
	v, err := s.core.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, mapError(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

// ─── Crypto operations ────────────────────────────────────────────────────────

func (s *Service) Encrypt(ctx context.Context, req *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error) {
	project, loc, kr, key, version, ok := kmscore.SplitKeyOrVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := verifyCRC("plaintext_crc32c", req.GetPlaintextCrc32C(), req.GetPlaintext()); err != nil {
		return nil, err
	}
	if err := verifyCRC("additional_authenticated_data_crc32c", req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData()); err != nil {
		return nil, err
	}
	res, err := s.core.Encrypt(ctx, project, loc, kr, key, version, req.GetPlaintext(), req.GetAdditionalAuthenticatedData())
	if err != nil {
		return nil, mapError(err)
	}
	return &kmspb.EncryptResponse{
		Name:                    kmscore.VersionName(project, loc, kr, key, res.Version),
		Ciphertext:              res.Ciphertext,
		CiphertextCrc32C:        wrapperspb.Int64(crc32cOf(res.Ciphertext)),
		VerifiedPlaintextCrc32C: req.GetPlaintextCrc32C() != nil,
		VerifiedAdditionalAuthenticatedDataCrc32C: req.GetAdditionalAuthenticatedDataCrc32C() != nil,
		ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) Decrypt(ctx context.Context, req *kmspb.DecryptRequest) (*kmspb.DecryptResponse, error) {
	project, loc, kr, key, ok := kmscore.SplitCryptoKeyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := verifyCRC("ciphertext_crc32c", req.GetCiphertextCrc32C(), req.GetCiphertext()); err != nil {
		return nil, err
	}
	if err := verifyCRC("additional_authenticated_data_crc32c", req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData()); err != nil {
		return nil, err
	}
	res, err := s.core.Decrypt(ctx, project, loc, kr, key, req.GetCiphertext(), req.GetAdditionalAuthenticatedData())
	if err != nil {
		return nil, mapError(err)
	}
	return &kmspb.DecryptResponse{
		Plaintext:       res.Plaintext,
		PlaintextCrc32C: wrapperspb.Int64(crc32cOf(res.Plaintext)),
		UsedPrimary:     res.UsedPrimary,
		ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) RawEncrypt(ctx context.Context, req *kmspb.RawEncryptRequest) (*kmspb.RawEncryptResponse, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	for _, c := range []struct {
		field string
		crc   *wrapperspb.Int64Value
		data  []byte
	}{
		{"plaintext_crc32c", req.GetPlaintextCrc32C(), req.GetPlaintext()},
		{"additional_authenticated_data_crc32c", req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData()},
		{"initialization_vector_crc32c", req.GetInitializationVectorCrc32C(), req.GetInitializationVector()},
	} {
		if err := verifyCRC(c.field, c.crc, c.data); err != nil {
			return nil, err
		}
	}
	res, err := s.core.RawEncrypt(ctx, project, loc, kr, key, version, req.GetPlaintext(), req.GetAdditionalAuthenticatedData(), req.GetInitializationVector())
	if err != nil {
		return nil, mapError(err)
	}
	return &kmspb.RawEncryptResponse{
		Name:                       kmscore.VersionName(project, loc, kr, key, version),
		Ciphertext:                 res.Ciphertext,
		InitializationVector:       res.IV,
		TagLength:                  16,
		CiphertextCrc32C:           wrapperspb.Int64(crc32cOf(res.Ciphertext)),
		InitializationVectorCrc32C: wrapperspb.Int64(crc32cOf(res.IV)),
		VerifiedPlaintextCrc32C:    req.GetPlaintextCrc32C() != nil,
		VerifiedAdditionalAuthenticatedDataCrc32C: req.GetAdditionalAuthenticatedDataCrc32C() != nil,
		VerifiedInitializationVectorCrc32C:        req.GetInitializationVectorCrc32C() != nil,
		ProtectionLevel:                           kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) RawDecrypt(ctx context.Context, req *kmspb.RawDecryptRequest) (*kmspb.RawDecryptResponse, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	for _, c := range []struct {
		field string
		crc   *wrapperspb.Int64Value
		data  []byte
	}{
		{"ciphertext_crc32c", req.GetCiphertextCrc32C(), req.GetCiphertext()},
		{"additional_authenticated_data_crc32c", req.GetAdditionalAuthenticatedDataCrc32C(), req.GetAdditionalAuthenticatedData()},
		{"initialization_vector_crc32c", req.GetInitializationVectorCrc32C(), req.GetInitializationVector()},
	} {
		if err := verifyCRC(c.field, c.crc, c.data); err != nil {
			return nil, err
		}
	}
	pt, err := s.core.RawDecrypt(ctx, project, loc, kr, key, version, req.GetCiphertext(), req.GetAdditionalAuthenticatedData(), req.GetInitializationVector(), int(req.GetTagLength()))
	if err != nil {
		return nil, mapError(err)
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
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	v, err := s.core.GetVersion(ctx, project, loc, kr, key, version)
	if err != nil {
		return nil, mapError(err)
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
	res, err := s.core.AsymmetricSign(ctx, project, loc, kr, key, version, digest)
	if err != nil {
		return nil, mapError(err)
	}
	return &kmspb.AsymmetricSignResponse{
		Name:                 kmscore.VersionName(project, loc, kr, key, version),
		Signature:            res.Signature,
		SignatureCrc32C:      wrapperspb.Int64(crc32cOf(res.Signature)),
		VerifiedDigestCrc32C: req.GetDigestCrc32C() != nil,
		VerifiedDataCrc32C:   req.GetDataCrc32C() != nil,
		ProtectionLevel:      kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) AsymmetricDecrypt(ctx context.Context, req *kmspb.AsymmetricDecryptRequest) (*kmspb.AsymmetricDecryptResponse, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := verifyCRC("ciphertext_crc32c", req.GetCiphertextCrc32C(), req.GetCiphertext()); err != nil {
		return nil, err
	}
	pt, err := s.core.AsymmetricDecrypt(ctx, project, loc, kr, key, version, req.GetCiphertext())
	if err != nil {
		return nil, mapError(err)
	}
	return &kmspb.AsymmetricDecryptResponse{
		Plaintext:                pt,
		PlaintextCrc32C:          wrapperspb.Int64(crc32cOf(pt)),
		VerifiedCiphertextCrc32C: req.GetCiphertextCrc32C() != nil,
		ProtectionLevel:          kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) MacSign(ctx context.Context, req *kmspb.MacSignRequest) (*kmspb.MacSignResponse, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := verifyCRC("data_crc32c", req.GetDataCrc32C(), req.GetData()); err != nil {
		return nil, err
	}
	res, err := s.core.MacSign(ctx, project, loc, kr, key, version, req.GetData())
	if err != nil {
		return nil, mapError(err)
	}
	return &kmspb.MacSignResponse{
		Name:               kmscore.VersionName(project, loc, kr, key, version),
		Mac:                res.Mac,
		MacCrc32C:          wrapperspb.Int64(crc32cOf(res.Mac)),
		VerifiedDataCrc32C: req.GetDataCrc32C() != nil,
		ProtectionLevel:    kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) MacVerify(ctx context.Context, req *kmspb.MacVerifyRequest) (*kmspb.MacVerifyResponse, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := verifyCRC("data_crc32c", req.GetDataCrc32C(), req.GetData()); err != nil {
		return nil, err
	}
	if err := verifyCRC("mac_crc32c", req.GetMacCrc32C(), req.GetMac()); err != nil {
		return nil, err
	}
	success, err := s.core.MacVerify(ctx, project, loc, kr, key, version, req.GetData(), req.GetMac())
	if err != nil {
		return nil, mapError(err)
	}
	return &kmspb.MacVerifyResponse{
		Name:               kmscore.VersionName(project, loc, kr, key, version),
		Success:            success,
		VerifiedDataCrc32C: req.GetDataCrc32C() != nil,
		VerifiedMacCrc32C:  req.GetMacCrc32C() != nil,
		ProtectionLevel:    kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

func (s *Service) GenerateRandomBytes(ctx context.Context, req *kmspb.GenerateRandomBytesRequest) (*kmspb.GenerateRandomBytesResponse, error) {
	data, err := s.core.GenerateRandomBytes(ctx, req.GetLocation(), int(req.GetLengthBytes()), protectionLevelFromProto(req.GetProtectionLevel()))
	if err != nil {
		return nil, mapError(err)
	}
	return &kmspb.GenerateRandomBytesResponse{
		Data:       data,
		DataCrc32C: wrapperspb.Int64(crc32cOf(data)),
	}, nil
}

func (s *Service) GetPublicKey(ctx context.Context, req *kmspb.GetPublicKeyRequest) (*kmspb.PublicKey, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	res, err := s.core.GetPublicKey(ctx, project, loc, kr, key, version, req.GetPublicKeyFormat().String())
	if err != nil {
		return nil, mapError(err)
	}
	out := &kmspb.PublicKey{
		Algorithm:       algorithmToProto(res.Algorithm),
		Name:            kmscore.VersionName(project, loc, kr, key, version),
		ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
	}
	switch res.Format {
	case "NIST_PQC":
		out.PublicKeyFormat = kmspb.PublicKey_NIST_PQC
		out.PublicKey = &kmspb.ChecksummedData{Data: res.Raw, Crc32CChecksum: wrapperspb.Int64(crc32cOf(res.Raw))}
	case "XWING_RAW_BYTES":
		out.PublicKeyFormat = kmspb.PublicKey_XWING_RAW_BYTES
		out.PublicKey = &kmspb.ChecksummedData{Data: res.Raw, Crc32CChecksum: wrapperspb.Int64(crc32cOf(res.Raw))}
	case "DER":
		out.PublicKeyFormat = kmspb.PublicKey_DER
		out.PublicKey = &kmspb.ChecksummedData{Data: res.Raw, Crc32CChecksum: wrapperspb.Int64(crc32cOf(res.Raw))}
	default:
		out.PublicKeyFormat = kmspb.PublicKey_PEM
		out.Pem = res.PEM
		out.PemCrc32C = wrapperspb.Int64(crc32cOf([]byte(res.PEM)))
	}
	return out, nil
}

// ─── Import / trusted wrapping / KEM ──────────────────────────────────────────

func (s *Service) ImportCryptoKeyVersion(ctx context.Context, req *kmspb.ImportCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, ok := kmscore.SplitCryptoKeyName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	wrapped := req.GetWrappedKey()
	if len(wrapped) == 0 {
		wrapped = req.GetRsaAesWrappedKey()
	}
	if len(wrapped) == 0 {
		return nil, mapError(model.NewProviderError("InvalidArgument", "wrapped_key is required", 400))
	}
	v, err := s.core.ImportCryptoKeyVersion(ctx, project, loc, kr, key, kmscore.ImportVersionRequest{
		ImportJob:              req.GetImportJob(),
		Algorithm:              algorithmFromProto(req.GetAlgorithm()),
		WrappedKey:             wrapped,
		CryptoKeyVersion:       req.GetCryptoKeyVersion(),
		TrustedWrappingEnabled: req.GetTrustedWrappingEnabled(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

func (s *Service) ImportTrustedKeyWrappedCryptoKeyVersion(ctx context.Context, req *kmspb.ImportTrustedKeyWrappedCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error) {
	project, loc, kr, key, ok := kmscore.SplitCryptoKeyName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	v, err := s.core.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, project, loc, kr, key, kmscore.ImportVersionRequest{
		ImportingKey:     req.GetImportingKey(),
		Algorithm:        algorithmFromProto(req.GetAlgorithm()),
		WrappedKey:       req.GetWrappedKey(),
		CryptoKeyVersion: req.GetCryptoKeyVersion(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return versionToProto(project, loc, kr, key, v), nil
}

func (s *Service) ExportTrustedKeyWrappedCryptoKeyVersion(ctx context.Context, req *kmspb.ExportTrustedKeyWrappedCryptoKeyVersionRequest) (*kmspb.ExportTrustedKeyWrappedCryptoKeyVersionResponse, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	wrapped, err := s.core.ExportTrustedKeyWrappedCryptoKeyVersion(ctx, project, loc, kr, key, version, req.GetWrappingKey())
	if err != nil {
		return nil, mapError(err)
	}
	return &kmspb.ExportTrustedKeyWrappedCryptoKeyVersionResponse{
		WrappedKey:       wrapped,
		WrappedKeyCrc32C: wrapperspb.Int64(crc32cOf(wrapped)),
	}, nil
}

func (s *Service) Decapsulate(ctx context.Context, req *kmspb.DecapsulateRequest) (*kmspb.DecapsulateResponse, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := verifyCRC("ciphertext_crc32c", req.GetCiphertextCrc32C(), req.GetCiphertext()); err != nil {
		return nil, err
	}
	ss, err := s.core.Decapsulate(ctx, project, loc, kr, key, version, req.GetCiphertext())
	if err != nil {
		return nil, mapError(err)
	}
	crc := crc32cOf(ss)
	return &kmspb.DecapsulateResponse{
		Name:                     kmscore.VersionName(project, loc, kr, key, version),
		SharedSecret:             ss,
		SharedSecretCrc32C:       &crc,
		VerifiedCiphertextCrc32C: req.GetCiphertextCrc32C() != nil,
		ProtectionLevel:          kmspb.ProtectionLevel_SOFTWARE,
	}, nil
}

// ─── Deletion + RetiredResources ──────────────────────────────────────────────

func doneOperation(name string) *longrunningpb.Operation {
	resp, _ := anypb.New(&emptypb.Empty{})
	return &longrunningpb.Operation{
		Name:   name + "/operations/delete",
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: resp},
	}
}

func (s *Service) DeleteCryptoKeyVersion(ctx context.Context, req *kmspb.DeleteCryptoKeyVersionRequest) (*longrunningpb.Operation, error) {
	project, loc, kr, key, version, ok := kmscore.SplitVersionName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.core.DeleteVersion(ctx, project, loc, kr, key, version); err != nil {
		return nil, mapError(err)
	}
	return doneOperation(req.GetName()), nil
}

func (s *Service) DeleteCryptoKey(ctx context.Context, req *kmspb.DeleteCryptoKeyRequest) (*longrunningpb.Operation, error) {
	project, loc, kr, key, ok := kmscore.SplitCryptoKeyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if _, err := s.core.DeleteCryptoKey(ctx, project, loc, kr, key); err != nil {
		return nil, mapError(err)
	}
	return doneOperation(req.GetName()), nil
}

func (s *Service) GetRetiredResource(ctx context.Context, req *kmspb.GetRetiredResourceRequest) (*kmspb.RetiredResource, error) {
	project, loc, id, ok := kmscore.SplitRetiredResourceName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	rr, err := s.core.GetRetiredResource(ctx, project, loc, id)
	if err != nil {
		return nil, mapError(err)
	}
	return retiredResourceToProto(rr), nil
}

func (s *Service) ListRetiredResources(ctx context.Context, req *kmspb.ListRetiredResourcesRequest) (*kmspb.ListRetiredResourcesResponse, error) {
	project, loc, ok := kmscore.SplitLocationName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	rrs, err := s.core.ListRetiredResources(ctx, project, loc)
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*kmspb.RetiredResource, 0, len(rrs))
	for _, rr := range rrs {
		out = append(out, retiredResourceToProto(rr))
	}
	return &kmspb.ListRetiredResourcesResponse{RetiredResources: out, TotalSize: int64(len(out))}, nil
}

// ─── ImportJobs ───────────────────────────────────────────────────────────────

func (s *Service) CreateImportJob(ctx context.Context, req *kmspb.CreateImportJobRequest) (*kmspb.ImportJob, error) {
	project, loc, kr, ok := kmscore.SplitKeyRingName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	id := req.GetImportJobId()
	if id == "" {
		if ij := req.GetImportJob(); ij != nil {
			_, _, _, id, _ = kmscore.SplitImportJobName(ij.GetName())
		}
	}
	if id == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing importJobId", 400))
	}
	method := req.GetImportJob().GetImportMethod()
	if method == kmspb.ImportJob_IMPORT_METHOD_UNSPECIFIED {
		return nil, mapError(model.NewProviderError("InvalidArgument", "import_method is required", 400))
	}
	ij, err := s.core.CreateImportJob(ctx, project, loc, kr, id, kmscore.ImportJobInput{
		ImportMethod:    method.String(),
		ProtectionLevel: protectionLevelFromProto(req.GetImportJob().GetProtectionLevel()),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return importJobToProto(ij), nil
}

func (s *Service) GetImportJob(ctx context.Context, req *kmspb.GetImportJobRequest) (*kmspb.ImportJob, error) {
	project, loc, kr, id, ok := kmscore.SplitImportJobName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	ij, err := s.core.GetImportJob(ctx, project, loc, kr, id)
	if err != nil {
		return nil, mapError(err)
	}
	return importJobToProto(ij), nil
}

func (s *Service) ListImportJobs(ctx context.Context, req *kmspb.ListImportJobsRequest) (*kmspb.ListImportJobsResponse, error) {
	project, loc, kr, ok := kmscore.SplitKeyRingName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	jobs, next, total, err := s.core.PageImportJobs(ctx, project, loc, kr, kmsListQuery(req.GetFilter(), req.GetOrderBy(), req.GetPageSize(), req.GetPageToken()))
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*kmspb.ImportJob, 0, len(jobs))
	for _, ij := range jobs {
		out = append(out, importJobToProto(ij))
	}
	return &kmspb.ListImportJobsResponse{ImportJobs: out, NextPageToken: next, TotalSize: int32(total)}, nil
}

// ─── IAM (google.iam.v1.IAMPolicy over keyrings/keys) ─────────────────────────

// Owns reports whether the KMS service handles IAM for this resource name.
func (s *Service) Owns(resource string) bool {
	_, _, ok := kmscore.IamPolicyInfo(resource)
	return ok
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project := grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
	pol, err := s.core.GetIamPolicy(ctx, project, req.GetResource())
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project := grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
	pol, err := s.core.SetIamPolicy(ctx, project, req.GetResource(), protoPolicyToBody(req.GetPolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	project := grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
	perms, err := s.core.TestIamPermissions(ctx, project, req.GetResource(), req.GetPermissions())
	if err != nil {
		return nil, mapError(err)
	}
	return &iampb.TestIamPermissionsResponse{Permissions: perms}, nil
}

// ─── rendering / helpers ──────────────────────────────────────────────────────

func keyRingToProto(project, location string, kr kmsstore.KeyRing) *kmspb.KeyRing {
	out := &kmspb.KeyRing{Name: kmscore.KeyRingName(project, location, kr.ID)}
	if !kr.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(kr.CreateTime)
	}
	return out
}

func cryptoKeyToProto(project string, k kmsstore.CryptoKey, primaryVersion kmsstore.Version) *kmspb.CryptoKey {
	primaryState := primaryVersion.State
	if primaryState == "" {
		primaryState = "ENABLED"
	}
	pl := protectionLevelToProto(k.ProtectionLevel)
	primary := &kmspb.CryptoKeyVersion{
		Name:            kmscore.VersionName(project, k.Location, k.KeyRingID, k.ID, k.PrimaryVersion),
		State:           stateToProto(primaryState),
		Algorithm:       algorithmToProto(k.Algorithm),
		ProtectionLevel: pl,
	}
	// A CryptoKeyVersion always carries createTime + generateTime; the REST
	// adapter renders them from the version's stored CreateTime (falling back to
	// the key's), so the gRPC render must too — otherwise the two transports
	// disagree on a field the proto defines. Software-generated key material has
	// createTime == generateTime.
	primaryCreateTime := primaryVersion.CreateTime
	if primaryCreateTime.IsZero() {
		primaryCreateTime = k.CreateTime
	}
	if !primaryCreateTime.IsZero() {
		primary.CreateTime = timestamp.Proto(primaryCreateTime)
		primary.GenerateTime = timestamp.Proto(primaryCreateTime)
	}
	if primaryVersion.State == "DESTROY_SCHEDULED" && !primaryVersion.DestroyTime.IsZero() {
		primary.DestroyTime = timestamp.Proto(primaryVersion.DestroyTime)
	}
	if primaryVersion.State == "DESTROYED" && !primaryVersion.DestroyEventTime.IsZero() {
		primary.DestroyEventTime = timestamp.Proto(primaryVersion.DestroyEventTime)
	}
	out := &kmspb.CryptoKey{
		Name:       kmscore.CryptoKeyName(project, k.Location, k.KeyRingID, k.ID),
		Purpose:    purposeToProto(k.Purpose),
		ImportOnly: k.ImportOnly,
		VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
			Algorithm:       algorithmToProto(k.Algorithm),
			ProtectionLevel: pl,
		},
	}
	// Discovery: only ENCRYPT_DECRYPT keys carry a primary; an import-only key
	// has no version until the first import.
	if k.Purpose == "ENCRYPT_DECRYPT" && k.PrimaryVersion != "" {
		out.Primary = primary
	}
	if !k.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(k.CreateTime)
	}
	if len(k.Labels) > 0 {
		out.Labels = k.Labels
	}
	if k.RotationPeriod > 0 {
		out.RotationSchedule = &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(k.RotationPeriod)}
	}
	if !k.NextRotationTime.IsZero() {
		out.NextRotationTime = timestamp.Proto(k.NextRotationTime)
	}
	return out
}

func versionToProto(project, location, kr, key string, v kmsstore.Version) *kmspb.CryptoKeyVersion {
	out := &kmspb.CryptoKeyVersion{
		Name:                   kmscore.VersionName(project, location, kr, key, v.Version),
		State:                  stateToProto(v.State),
		Algorithm:              algorithmToProto(v.Algorithm),
		ProtectionLevel:        protectionLevelToProto(v.ProtectionLevel),
		TrustedWrappingEnabled: v.TrustedWrappingEnabled,
		HsmTrusted:             v.HsmTrusted,
	}
	if !v.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(v.CreateTime)
	}
	if !v.ImportTime.IsZero() {
		out.ImportTime = timestamp.Proto(v.ImportTime)
	}
	if v.State == "DESTROY_SCHEDULED" && !v.DestroyTime.IsZero() {
		out.DestroyTime = timestamp.Proto(v.DestroyTime)
	}
	if v.State == "DESTROYED" && !v.DestroyEventTime.IsZero() {
		out.DestroyEventTime = timestamp.Proto(v.DestroyEventTime)
	}
	return out
}

func importJobToProto(ij kmscore.ImportJob) *kmspb.ImportJob {
	out := &kmspb.ImportJob{
		Name:            ij.Name,
		ImportMethod:    importMethodToProto(ij.ImportMethod),
		ProtectionLevel: protectionLevelToProto(ij.ProtectionLevel),
		State:           kmspb.ImportJob_ImportJobState(kmspb.ImportJob_ImportJobState_value[ij.State]),
		PublicKeyFormat: kmspb.PublicKey_PEM,
	}
	if !ij.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(ij.CreateTime)
	}
	if !ij.GenerateTime.IsZero() {
		out.GenerateTime = timestamp.Proto(ij.GenerateTime)
	}
	if !ij.ExpireTime.IsZero() {
		out.ExpireTime = timestamp.Proto(ij.ExpireTime)
	}
	// The wrapping public key is documented "Only returned if state is ACTIVE",
	// so an EXPIRED job keeps its format (which echoes the requested format) but
	// omits the key material.
	switch {
	case len(ij.PublicKeyRaw) > 0:
		out.PublicKeyFormat = kmspb.PublicKey_NIST_PQC
		if ij.ImportMethod == "HPKE_KEM_XWING_HKDF_SHA256_AES_256_GCM" {
			out.PublicKeyFormat = kmspb.PublicKey_XWING_RAW_BYTES
		}
		if ij.State == "ACTIVE" {
			out.PublicKey = &kmspb.ImportJob_WrappingPublicKey{Data: ij.PublicKeyRaw}
		}
	case ij.PublicKeyPEM != "":
		if ij.State == "ACTIVE" {
			out.PublicKey = &kmspb.ImportJob_WrappingPublicKey{Pem: ij.PublicKeyPEM}
		}
	}
	return out
}

func retiredResourceToProto(rr kmscore.RetiredResource) *kmspb.RetiredResource {
	out := &kmspb.RetiredResource{
		Name:             rr.Name,
		OriginalResource: rr.OriginalResource,
		ResourceType:     rr.ResourceType,
	}
	if !rr.DeleteTime.IsZero() {
		out.DeleteTime = timestamp.Proto(rr.DeleteTime)
	}
	return out
}

func importMethodToProto(s string) kmspb.ImportJob_ImportMethod {
	if v, ok := kmspb.ImportJob_ImportMethod_value[s]; ok {
		return kmspb.ImportJob_ImportMethod(v)
	}
	return kmspb.ImportJob_IMPORT_METHOD_UNSPECIFIED
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

func protectionLevelFromProto(p kmspb.ProtectionLevel) string {
	switch p {
	case kmspb.ProtectionLevel_HSM:
		return "HSM"
	case kmspb.ProtectionLevel_HSM_SINGLE_TENANT:
		return "HSM_SINGLE_TENANT"
	case kmspb.ProtectionLevel_EXTERNAL:
		return "EXTERNAL"
	case kmspb.ProtectionLevel_EXTERNAL_VPC:
		return "EXTERNAL_VPC"
	default:
		return "SOFTWARE"
	}
}

func protectionLevelToProto(s string) kmspb.ProtectionLevel {
	if v, ok := kmspb.ProtectionLevel_value[s]; ok {
		return kmspb.ProtectionLevel(v)
	}
	return kmspb.ProtectionLevel_SOFTWARE
}

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

func crc32cOf(b []byte) int64 {
	return int64(crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
}

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
	// Mirror the REST renderer (internal/gcp/transport/rest/kms.iamPolicyMap),
	// which matches real Cloud KMS: an empty/default policy renders as just
	// {"etag": ...} — the semantic default version 1 is not emitted, and empty
	// bindings are omitted by protojson. Without this the same stored policy
	// rendered `version: 1` over gRPC while the REST route omitted it, a
	// cross-transport divergence the parity harness caught (AUD3-16).
	out := &iampb.Policy{}
	if p.Version > 1 {
		out.Version = int32(p.Version)
	}
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
