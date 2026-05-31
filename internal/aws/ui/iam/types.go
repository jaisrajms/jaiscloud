package iamui

// IAMRole is the UI representation of an IAM role.
type IAMRole struct {
	RoleName                 string `json:"roleName"`
	RoleID                   string `json:"roleId,omitempty"`
	ARN                      string `json:"arn"`
	Description              string `json:"description,omitempty"`
	AssumeRolePolicyDocument string `json:"assumeRolePolicyDocument,omitempty"`
	CreateDate               string `json:"createDate,omitempty"`
	MaxSessionDuration       int    `json:"maxSessionDuration,omitempty"`
}

// ListRolesResponse is the response for GET /iam/roles.
type ListRolesResponse struct {
	Items     []IAMRole `json:"items"`
	Marker    string    `json:"marker,omitempty"`
	Total     int       `json:"total"`
}

// CreateRoleRequest is the body for POST /iam/roles.
type CreateRoleRequest struct {
	RoleName                 string `json:"roleName"`
	AssumeRolePolicyDocument string `json:"assumeRolePolicyDocument"`
	Description              string `json:"description,omitempty"`
	MaxSessionDuration       int    `json:"maxSessionDuration,omitempty"`
}

// IAMUser is the UI representation of an IAM user.
type IAMUser struct {
	UserName   string `json:"userName"`
	UserID     string `json:"userId,omitempty"`
	ARN        string `json:"arn"`
	CreateDate string `json:"createDate,omitempty"`
}

// ListUsersResponse is the response for GET /iam/users.
type ListUsersResponse struct {
	Items  []IAMUser `json:"items"`
	Marker string    `json:"marker,omitempty"`
	Total  int       `json:"total"`
}

// CreateUserRequest is the body for POST /iam/users.
type CreateUserRequest struct {
	UserName string `json:"userName"`
	Path     string `json:"path,omitempty"`
}

// AccessKey is the UI representation of an IAM access key.
type AccessKey struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"` // only on creation
	Status          string `json:"status"`
	CreateDate      string `json:"createDate,omitempty"`
}

// IAMPolicy is the UI representation of an IAM managed policy.
type IAMPolicy struct {
	PolicyName      string `json:"policyName"`
	PolicyID        string `json:"policyId,omitempty"`
	ARN             string `json:"arn"`
	Description     string `json:"description,omitempty"`
	CreateDate      string `json:"createDate,omitempty"`
	UpdateDate      string `json:"updateDate,omitempty"`
	AttachmentCount int    `json:"attachmentCount"`
}

// ListPoliciesResponse is the response for GET /iam/policies.
type ListPoliciesResponse struct {
	Items  []IAMPolicy `json:"items"`
	Marker string      `json:"marker,omitempty"`
	Total  int         `json:"total"`
}

// CreatePolicyRequest is the body for POST /iam/policies.
type CreatePolicyRequest struct {
	PolicyName     string `json:"policyName"`
	PolicyDocument string `json:"policyDocument"`
	Description    string `json:"description,omitempty"`
}

// AttachedPolicy is a policy attached to a role/user.
type AttachedPolicy struct {
	PolicyName string `json:"policyName"`
	PolicyARN  string `json:"policyArn"`
}
