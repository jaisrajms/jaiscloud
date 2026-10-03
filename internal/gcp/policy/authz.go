package policy

import (
	"context"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// Cloud KMS crypto-operation permissions. Each KMS data-plane operation
// requires the caller to hold the matching permission on the cryptoKey; the
// names are the ones in the Cloud IAM permissions reference
// (https://cloud.google.com/kms/docs/reference/permissions-and-roles).
const (
	PermCryptoKeyUseToEncrypt  = "cloudkms.cryptoKeyVersions.useToEncrypt"
	PermCryptoKeyUseToDecrypt  = "cloudkms.cryptoKeyVersions.useToDecrypt"
	PermCryptoKeyUseToSign     = "cloudkms.cryptoKeyVersions.useToSign"
	PermCryptoKeyUseToVerify   = "cloudkms.cryptoKeyVersions.useToVerify"
	PermCryptoKeyViewPublicKey = "cloudkms.cryptoKeyVersions.viewPublicKey"
)

// kmsCryptoPermissions is the full crypto-use permission set, granted by the
// basic owner/editor roles and roles/cloudkms.admin.
var kmsCryptoPermissions = []string{
	PermCryptoKeyUseToEncrypt,
	PermCryptoKeyUseToDecrypt,
	PermCryptoKeyUseToSign,
	PermCryptoKeyUseToVerify,
	PermCryptoKeyViewPublicKey,
}

// kmsRolePermissions maps the Cloud KMS predefined roles a key policy can bind
// to the crypto-use permissions they grant. Keys are lower-cased because role
// lookup is case-insensitive. Roles that carry no crypto use (for example
// roles/cloudkms.viewer or the key-management-only roles) are intentionally
// absent, so a policy scoped to them denies a crypto operation.
var kmsRolePermissions = map[string][]string{
	"roles/owner":                                kmsCryptoPermissions,
	"roles/editor":                               kmsCryptoPermissions,
	"roles/cloudkms.admin":                       kmsCryptoPermissions,
	"roles/cloudkms.cryptooperator":              {PermCryptoKeyUseToEncrypt, PermCryptoKeyUseToDecrypt, PermCryptoKeyUseToSign, PermCryptoKeyUseToVerify},
	"roles/cloudkms.cryptokeyencrypterdecrypter": {PermCryptoKeyUseToEncrypt, PermCryptoKeyUseToDecrypt},
	"roles/cloudkms.cryptokeyencrypter":          {PermCryptoKeyUseToEncrypt},
	"roles/cloudkms.cryptokeydecrypter":          {PermCryptoKeyUseToDecrypt},
	"roles/cloudkms.signerverifier":              {PermCryptoKeyUseToSign, PermCryptoKeyUseToVerify, PermCryptoKeyViewPublicKey},
	"roles/cloudkms.signer":                      {PermCryptoKeyUseToSign},
	"roles/cloudkms.verifier":                    {PermCryptoKeyUseToVerify, PermCryptoKeyViewPublicKey},
	"roles/cloudkms.publickeyviewer":             {PermCryptoKeyViewPublicKey},
}

// PermissionDenied returns the canonical Cloud IAM denial (HTTP 403 /
// google.rpc.PERMISSION_DENIED) with the real Cloud KMS message shape.
func PermissionDenied(permission, resource string) error {
	return &model.ProviderError{
		Code:       "PermissionDenied",
		Message:    "Permission '" + permission + "' denied on resource '" + resource + "' (or it may not exist).",
		HTTPStatus: 403,
		Status:     "PERMISSION_DENIED",
	}
}

// AuthorizeKMS evaluates a cryptoKey's stored IAM policy for one crypto-use
// permission. It mirrors the AWS KMS emulator's resource-policy hook
// (internal/aws/key/provider.go checkKeyPolicy + internal/aws/key/policy_eval.go)
// in a default-permissive way:
//
//   - a key with no stored policy or no bindings is allowed — the emulator's
//     allow-all default, analogous to AWS KMS's defaultKeyPolicy;
//   - otherwise the caller is allowed only when some binding grants a role that
//     includes permission.
//
// The caller identity and binding members are not resolved: the emulator does
// not model a caller principal (AWS evaluates every caller as the account
// root), so a binding grants its role's permissions regardless of its members,
// and a binding condition is treated as satisfied (AWS's evaluator ignores
// Condition too). A non-empty policy scoped to roles that omit the permission
// therefore denies — the denial path a client can exercise.
//
// It returns nil when the operation is allowed, or a PermissionDenied error
// otherwise.
func AuthorizeKMS(ctx context.Context, s store.ResourceStore, account, policyType, id, permission, resource string) error {
	p := Load(ctx, s, account, policyType, id)
	if len(p.Bindings) == 0 {
		return nil
	}
	for _, raw := range p.Bindings {
		b, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		role, _ := b["role"].(string)
		for _, granted := range kmsRolePermissions[strings.ToLower(strings.TrimSpace(role))] {
			if granted == permission {
				return nil
			}
		}
	}
	return PermissionDenied(permission, resource)
}
