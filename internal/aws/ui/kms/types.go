package kmsui

// KMSKey is the UI representation of a KMS key.
type KMSKey struct {
	KeyID        string            `json:"keyId"`
	ARN          string            `json:"arn"`
	Description  string            `json:"description,omitempty"`
	KeyUsage     string            `json:"keyUsage"`
	KeySpec      string            `json:"keySpec"`
	KeyState     string            `json:"keyState"`
	Enabled      bool              `json:"enabled"`
	Origin       string            `json:"origin,omitempty"`
	CreatedAt    string            `json:"createdAt,omitempty"`
	DeletionDate string            `json:"deletionDate,omitempty"`
	Aliases      []string          `json:"aliases,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
}

// ListKeysResponse is the response for GET /kms/keys.
type ListKeysResponse struct {
	Items     []KMSKey `json:"items"`
	NextToken string   `json:"nextToken,omitempty"`
	Total     int      `json:"total"`
}

// CreateKeyRequest is the body for POST /kms/keys.
type CreateKeyRequest struct {
	Description string            `json:"description,omitempty"`
	KeyUsage    string            `json:"keyUsage,omitempty"` // ENCRYPT_DECRYPT | SIGN_VERIFY | GENERATE_VERIFY_MAC
	KeySpec     string            `json:"keySpec,omitempty"`  // SYMMETRIC_DEFAULT | RSA_2048 | ...
	Tags        map[string]string `json:"tags,omitempty"`
}

// KMSAlias is the UI representation of a KMS alias.
type KMSAlias struct {
	AliasName   string `json:"aliasName"`
	AliasARN    string `json:"aliasArn"`
	TargetKeyID string `json:"targetKeyId,omitempty"`
}

// ListAliasesResponse is the response for GET /kms/aliases.
type ListAliasesResponse struct {
	Items     []KMSAlias `json:"items"`
	NextToken string     `json:"nextToken,omitempty"`
}

// CreateAliasRequest is the body for POST /kms/aliases.
type CreateAliasRequest struct {
	AliasName   string `json:"aliasName"`
	TargetKeyID string `json:"targetKeyId"`
}
