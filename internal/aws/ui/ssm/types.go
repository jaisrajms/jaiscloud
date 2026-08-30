package ssmui

// Parameter is the UI representation of an SSM parameter.
type Parameter struct {
	Name             string            `json:"name"`
	Type             string            `json:"type"` // String | StringList | SecureString
	Value            string            `json:"value,omitempty"`
	Version          int64             `json:"version"`
	LastModifiedDate string            `json:"lastModifiedDate,omitempty"`
	LastModifiedUser string            `json:"lastModifiedUser,omitempty"`
	ARN              string            `json:"arn,omitempty"`
	DataType         string            `json:"dataType,omitempty"`
	Description      string            `json:"description,omitempty"`
	KeyID            string            `json:"keyId,omitempty"`
	Tier             string            `json:"tier,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
}

// ListParametersResponse is the response for GET /ssm/parameters.
type ListParametersResponse struct {
	Items     []Parameter `json:"items"`
	NextToken string      `json:"nextToken,omitempty"`
	Total     int         `json:"total"`
}

// PutParameterRequest is the body for POST /ssm/parameters.
type PutParameterRequest struct {
	Name        string            `json:"name"`
	Value       string            `json:"value"`
	Type        string            `json:"type"`        // String | StringList | SecureString
	Description string            `json:"description,omitempty"`
	KeyID       string            `json:"keyId,omitempty"`
	Overwrite   bool              `json:"overwrite,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// ParameterHistoryEntry is a version entry for a parameter.
type ParameterHistoryEntry struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	Value            string `json:"value,omitempty"`
	Version          int64  `json:"version"`
	LastModifiedDate string `json:"lastModifiedDate,omitempty"`
	LastModifiedUser string `json:"lastModifiedUser,omitempty"`
	Description      string `json:"description,omitempty"`
}

// ListParameterHistoryResponse is the response for GET /ssm/parameters/{name}/history.
type ListParameterHistoryResponse struct {
	Items     []ParameterHistoryEntry `json:"items"`
	NextToken string                  `json:"nextToken,omitempty"`
}
