// Package kmsui serves the Cloud KMS UI API. Handlers call the KMS provider
// directly (in-process) and reshape its Discovery-shaped response maps for the
// console. Scope is key rings, crypto keys and crypto-key versions plus IAM
// policy and the crypto operations: symmetric encrypt/decrypt, asymmetric
// sign/decrypt, MAC sign/verify and public-key download.
package kmsui

import "jaiscloud/internal/gcp/ui/uihelper"

// KeyRing is the UI summary of a KMS key ring.
type KeyRing struct {
	Name       string `json:"name"`
	KeyRingID  string `json:"keyRingId,omitempty"`
	Location   string `json:"location,omitempty"`
	CreateTime string `json:"createTime,omitempty"`
}

// ListKeyRingsResponse is the response for GET /locations/{location}/keyRings.
type ListKeyRingsResponse struct {
	KeyRings      []KeyRing `json:"keyRings"`
	Total         int       `json:"total"`
	NextPageToken string    `json:"nextPageToken,omitempty"`
}

// CreateKeyRingRequest is the body for POST /locations/{location}/keyRings.
type CreateKeyRingRequest struct {
	KeyRingID string `json:"keyRingId"`
}

// CryptoKey is the UI summary of a KMS crypto key.
type CryptoKey struct {
	Name             string            `json:"name"`
	CryptoKeyID      string            `json:"cryptoKeyId,omitempty"`
	Purpose          string            `json:"purpose,omitempty"`
	Algorithm        string            `json:"algorithm,omitempty"`
	PrimaryState     string            `json:"primaryState,omitempty"`
	PrimaryVersion   string            `json:"primaryVersion,omitempty"`
	CreateTime       string            `json:"createTime,omitempty"`
	RotationPeriod   string            `json:"rotationPeriod,omitempty"`
	NextRotationTime string            `json:"nextRotationTime,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
}

// ListCryptoKeysResponse is the response for the cryptoKeys collection.
type ListCryptoKeysResponse struct {
	CryptoKeys    []CryptoKey `json:"cryptoKeys"`
	Total         int         `json:"total"`
	NextPageToken string      `json:"nextPageToken,omitempty"`
}

// CreateCryptoKeyRequest is the body for POST .../cryptoKeys. Only cryptoKeyId
// is required. When RotationPeriod is set the console supplies the paired
// nextRotationTime (real GCP requires next_rotation_time with a
// rotation_schedule); a caller may pass NextRotationTime explicitly, otherwise
// the handler derives it as now + RotationPeriod.
type CreateCryptoKeyRequest struct {
	CryptoKeyID      string            `json:"cryptoKeyId"`
	Purpose          string            `json:"purpose,omitempty"`
	Algorithm        string            `json:"algorithm,omitempty"`
	RotationPeriod   string            `json:"rotationPeriod,omitempty"`
	NextRotationTime string            `json:"nextRotationTime,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
}

// CryptoKeyVersion is the UI summary of a crypto-key version.
type CryptoKeyVersion struct {
	Name             string `json:"name"`
	VersionID        string `json:"versionId,omitempty"`
	State            string `json:"state,omitempty"`
	Algorithm        string `json:"algorithm,omitempty"`
	CreateTime       string `json:"createTime,omitempty"`
	DestroyTime      string `json:"destroyTime,omitempty"`
	DestroyEventTime string `json:"destroyEventTime,omitempty"`
}

// ListCryptoKeyVersionsResponse is the response for the versions collection.
type ListCryptoKeyVersionsResponse struct {
	CryptoKeyVersions []CryptoKeyVersion `json:"cryptoKeyVersions"`
	Total             int                `json:"total"`
	NextPageToken     string             `json:"nextPageToken,omitempty"`
}

// SetVersionStateRequest is the body for POST .../versions/{version}/disable
// and /enable. KMS only allows ENABLED or DISABLED.
type SetVersionStateRequest struct {
	State string `json:"state"`
}

// SetPrimaryVersionRequest is the body for POST .../cryptoKeys/{key}/setPrimary.
type SetPrimaryVersionRequest struct {
	CryptoKeyVersionID string `json:"cryptoKeyVersionId"`
}

// IamBinding is one role-to-members binding in an IAM policy.
type IamBinding = uihelper.IamBinding

// IamPolicy mirrors google.iam.v1.Policy.
type IamPolicy = uihelper.IamPolicy

// SetIamPolicyRequest is the body for PUT .../iam.
type SetIamPolicyRequest struct {
	Policy IamPolicy `json:"policy"`
}

// TestIamPermissionsRequest is the body for .../iam/testIamPermissions.
type TestIamPermissionsRequest struct {
	Permissions []string `json:"permissions"`
}

// TestIamPermissionsResponse is the response for testIamPermissions.
type TestIamPermissionsResponse struct {
	Permissions []string `json:"permissions"`
}

// EncryptRequest is the body for POST .../cryptoKeys/{key}/encrypt. plaintext
// is base64-encoded, matching Cloud KMS's EncryptRequest.
type EncryptRequest struct {
	Plaintext                   string `json:"plaintext"`
	AdditionalAuthenticatedData string `json:"additionalAuthenticatedData,omitempty"`
}

// DecryptRequest is the body for POST .../cryptoKeys/{key}/decrypt.
type DecryptRequest struct {
	Ciphertext                  string `json:"ciphertext"`
	AdditionalAuthenticatedData string `json:"additionalAuthenticatedData,omitempty"`
}

// AsymmetricSignRequest is the body for POST
// .../versions/{version}/asymmetricSign. Digest carries exactly one of
// sha256/sha384/sha512, base64-encoded, matching Cloud KMS's Digest.
type AsymmetricSignRequest struct {
	Digest map[string]any `json:"digest"`
}

// AsymmetricDecryptRequest is the body for POST
// .../versions/{version}/asymmetricDecrypt.
type AsymmetricDecryptRequest struct {
	Ciphertext string `json:"ciphertext"`
}

// MacSignRequest is the body for POST .../versions/{version}/macSign. data is
// base64-encoded.
type MacSignRequest struct {
	Data string `json:"data"`
}

// MacVerifyRequest is the body for POST .../versions/{version}/macVerify. Both
// data and mac are base64-encoded.
type MacVerifyRequest struct {
	Data string `json:"data"`
	Mac  string `json:"mac"`
}
