// Package secretmanagerui serves the Secret Manager UI API. Handlers call the
// Secret Manager provider directly (in-process) and reshape its
// Discovery-shaped response maps for the console. Scope is secrets, versions
// and IAM policy; managed rotation execution and replication metadata are
// deferred (see the console-UI wave plan).
package secretmanagerui

import "jaiscloud/internal/gcp/ui/uihelper"

// Secret is the UI summary of a Secret Manager secret.
type Secret struct {
	Name             string            `json:"name"`
	SecretID         string            `json:"secretId,omitempty"`
	CreateTime       string            `json:"createTime,omitempty"`
	Etag             string            `json:"etag,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	Annotations      map[string]string `json:"annotations,omitempty"`
	Topics           []string          `json:"topics,omitempty"`
	RotationPeriod   string            `json:"rotationPeriod,omitempty"`
	NextRotationTime string            `json:"nextRotationTime,omitempty"`
	KmsKeyName       string            `json:"kmsKeyName,omitempty"`
}

// ListSecretsResponse is the response for GET /secrets.
type ListSecretsResponse struct {
	Secrets       []Secret `json:"secrets"`
	Total         int      `json:"total"`
	NextPageToken string   `json:"nextPageToken,omitempty"`
}

// CreateSecretRequest is the body for POST /secrets. secretId is required.
type CreateSecretRequest struct {
	SecretID         string            `json:"secretId"`
	Labels           map[string]string `json:"labels,omitempty"`
	Annotations      map[string]string `json:"annotations,omitempty"`
	Topics           []string          `json:"topics,omitempty"`
	RotationPeriod   string            `json:"rotationPeriod,omitempty"`
	NextRotationTime string            `json:"nextRotationTime,omitempty"`
	KmsKeyName       string            `json:"kmsKeyName,omitempty"`
}

// UpdateSecretRequest is the body for PATCH /secrets/{secret}. Absent fields
// are preserved.
type UpdateSecretRequest struct {
	Labels           map[string]string `json:"labels,omitempty"`
	Annotations      map[string]string `json:"annotations,omitempty"`
	Topics           []string          `json:"topics,omitempty"`
	RotationPeriod   string            `json:"rotationPeriod,omitempty"`
	NextRotationTime string            `json:"nextRotationTime,omitempty"`
	KmsKeyName       string            `json:"kmsKeyName,omitempty"`
	// RemoveRotation sends an empty `rotation` object, which is how the API
	// removes a schedule (`updateMask=rotation` with `{}`). The two schedule
	// fields above cannot express it: both empty means "leave the schedule
	// alone".
	RemoveRotation bool `json:"removeRotation,omitempty"`
}

// SecretVersion is the UI summary of a Secret Manager version.
type SecretVersion struct {
	Name        string `json:"name"`
	VersionID   string `json:"versionId,omitempty"`
	State       string `json:"state,omitempty"`
	CreateTime  string `json:"createTime,omitempty"`
	DestroyTime string `json:"destroyTime,omitempty"`
	Etag        string `json:"etag,omitempty"`
}

// ListSecretVersionsResponse is the response for GET /secrets/{secret}/versions.
type ListSecretVersionsResponse struct {
	Versions      []SecretVersion `json:"versions"`
	Total         int             `json:"total"`
	NextPageToken string          `json:"nextPageToken,omitempty"`
}

// AddVersionRequest is the body for POST /secrets/{secret}/versions. Payload is
// base64-encoded, matching the wire API's payload.data.
type AddVersionRequest struct {
	Payload string `json:"payload"`
}

// AccessSecretVersionResponse is the response for
// GET /secrets/{secret}/versions/{version}/access. Data is base64-encoded.
type AccessSecretVersionResponse struct {
	Name string `json:"name"`
	Data string `json:"data,omitempty"`
}

// IamBinding is one role-to-members binding in an IAM policy.
type IamBinding = uihelper.IamBinding

// IamPolicy mirrors google.iam.v1.Policy.
type IamPolicy = uihelper.IamPolicy

// SetIamPolicyRequest is the body for PUT /secrets/{secret}/iam.
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
