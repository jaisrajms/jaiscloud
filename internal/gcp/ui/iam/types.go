// Package iamui serves the IAM (service accounts) UI API. Handlers call the IAM
// provider directly (in-process) and reshape its Discovery-shaped response maps
// for the console. Scope is the service-account control plane plus its keys,
// signing (signBlob/signJwt) and IAM policy; undelete has no console surface (see
// the console-UI wave plan for the rationale).
package iamui

import "jaiscloud/internal/gcp/ui/uihelper"

// ServiceAccount is the UI summary of an IAM service account.
type ServiceAccount struct {
	Name        string `json:"name"`
	Email       string `json:"email"`
	DisplayName string `json:"displayName,omitempty"`
	ProjectID   string `json:"projectId,omitempty"`
	UniqueID    string `json:"uniqueId,omitempty"`
	Description string `json:"description,omitempty"`
	Disabled    bool   `json:"disabled,omitempty"`
	Etag        string `json:"etag,omitempty"`
}

// ListServiceAccountsResponse is the response for GET /serviceAccounts.
type ListServiceAccountsResponse struct {
	Accounts      []ServiceAccount `json:"accounts"`
	Total         int              `json:"total"`
	NextPageToken string           `json:"nextPageToken,omitempty"`
}

// CreateServiceAccountRequest is the body for POST /serviceAccounts.
type CreateServiceAccountRequest struct {
	AccountID   string `json:"accountId"`
	DisplayName string `json:"displayName,omitempty"`
	Description string `json:"description,omitempty"`
}

// UpdateServiceAccountRequest is the body for PATCH /serviceAccounts/{email}.
// Only displayName and description are writable; a supplied etag enforces
// optimistic concurrency.
type UpdateServiceAccountRequest struct {
	DisplayName string `json:"displayName,omitempty"`
	Description string `json:"description,omitempty"`
	Etag        string `json:"etag,omitempty"`
}

// ServiceAccountKey is the UI summary of a service-account key. PrivateKeyData
// is only present on the create response.
type ServiceAccountKey struct {
	Name           string `json:"name"`
	KeyID          string `json:"keyId"`
	KeyAlgorithm   string `json:"keyAlgorithm,omitempty"`
	KeyOrigin      string `json:"keyOrigin,omitempty"`
	KeyType        string `json:"keyType,omitempty"`
	ValidAfterTime string `json:"validAfterTime,omitempty"`
	Disabled       bool   `json:"disabled,omitempty"`
	DisableTime    string `json:"disableTime,omitempty"`
	PublicKeyData  string `json:"publicKeyData,omitempty"`
	PrivateKeyData string `json:"privateKeyData,omitempty"`
}

// ListServiceAccountKeysResponse is the response for GET
// /serviceAccounts/{email}/keys.
type ListServiceAccountKeysResponse struct {
	Keys          []ServiceAccountKey `json:"keys"`
	Total         int                 `json:"total"`
	NextPageToken string              `json:"nextPageToken,omitempty"`
}

// IamBinding is one role-to-members binding in an IAM policy.
type IamBinding = uihelper.IamBinding

// IamPolicy mirrors google.iam.v1.Policy.
type IamPolicy = uihelper.IamPolicy

// SetIamPolicyRequest is the body for PUT /serviceAccounts/{email}/iam.
type SetIamPolicyRequest struct {
	Policy IamPolicy `json:"policy"`
}

// TestIamPermissionsRequest is the body for POST
// /serviceAccounts/{email}/iam:testIamPermissions.
type TestIamPermissionsRequest struct {
	Permissions []string `json:"permissions"`
}

// TestIamPermissionsResponse is the response for testIamPermissions.
type TestIamPermissionsResponse struct {
	Permissions []string `json:"permissions"`
}

// SignBlobRequest is the body for POST /serviceAccounts/{email}/signBlob. The
// payload is base64-encoded, matching IAM's bytesToSign field.
type SignBlobRequest struct {
	BytesToSign string `json:"bytesToSign"`
}

// SignBlobResponse mirrors IAM's SignBlobResponse. The provider emits the IAM
// `signature` field (not the IAM Credentials `signedBlob`) because the UI calls
// it in-process without a host.
type SignBlobResponse struct {
	KeyID     string `json:"keyId,omitempty"`
	Signature string `json:"signature"`
}

// SignJwtRequest is the body for POST /serviceAccounts/{email}/signJwt. The
// payload is a serialized JSON claims object.
type SignJwtRequest struct {
	Payload string `json:"payload"`
}

// SignJwtResponse mirrors IAM's SignJwtResponse.
type SignJwtResponse struct {
	KeyID     string `json:"keyId,omitempty"`
	SignedJwt string `json:"signedJwt"`
}
