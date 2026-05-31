package secretsmanagerui

// Secret is the UI representation of a SecretsManager secret.
type Secret struct {
	ARN              string            `json:"arn"`
	Name             string            `json:"name"`
	Description      string            `json:"description,omitempty"`
	KMSKeyID         string            `json:"kmsKeyId,omitempty"`
	LastChangedDate  string            `json:"lastChangedDate,omitempty"`
	LastAccessedDate string            `json:"lastAccessedDate,omitempty"`
	CreatedDate      string            `json:"createdDate,omitempty"`
	DeletedDate      string            `json:"deletedDate,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
}

// ListSecretsResponse is the response for GET /secretsmanager/secrets.
type ListSecretsResponse struct {
	Items     []Secret `json:"items"`
	NextToken string   `json:"nextToken,omitempty"`
	Total     int      `json:"total"`
}

// CreateSecretRequest is the body for POST /secretsmanager/secrets.
type CreateSecretRequest struct {
	Name         string            `json:"name"`
	Description  string            `json:"description,omitempty"`
	SecretString string            `json:"secretString,omitempty"`
	SecretBinary []byte            `json:"secretBinary,omitempty"`
	KMSKeyID     string            `json:"kmsKeyId,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
}

// PutSecretValueRequest is the body for POST /secretsmanager/secrets/{name}/value.
type PutSecretValueRequest struct {
	SecretString string `json:"secretString,omitempty"`
	SecretBinary []byte `json:"secretBinary,omitempty"`
}

// SecretVersion is a version entry for a secret.
type SecretVersion struct {
	VersionID    string   `json:"versionId"`
	VersionStages []string `json:"versionStages,omitempty"`
	CreatedDate  string   `json:"createdDate,omitempty"`
	LastAccessedDate string `json:"lastAccessedDate,omitempty"`
}

// ListSecretVersionsResponse is the response for GET /secretsmanager/secrets/{name}/versions.
type ListSecretVersionsResponse struct {
	Items     []SecretVersion `json:"items"`
	NextToken string          `json:"nextToken,omitempty"`
}
