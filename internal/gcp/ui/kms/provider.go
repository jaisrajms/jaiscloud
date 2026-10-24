package kmsui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *kms.Provider used by the KMS UI handlers.
type ProviderInterface interface {
	KeyRingCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	KeyRingList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	KeyRingGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	CryptoKeyCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	CryptoKeyVersionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyVersionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyVersionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyVersionDestroy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyVersionUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyUpdatePrimaryVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	CryptoKeyEncrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyVersionAsymmetricSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyVersionAsymmetricDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyVersionMacSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyVersionMacVerify(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CryptoKeyVersionGetPublicKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
