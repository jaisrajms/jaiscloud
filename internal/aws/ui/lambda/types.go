package lambdaui

// Function is the typed response struct for a single Lambda function.
type Function struct {
	Name                string            `json:"name"`
	ARN                 string            `json:"arn"`
	Runtime             string            `json:"runtime"`
	Handler             string            `json:"handler"`
	RoleARN             string            `json:"roleArn"`
	Timeout             int               `json:"timeout"`
	MemorySize          int               `json:"memorySize"`
	State               string            `json:"state"`
	LastModified        string            `json:"lastModified"`
	Description         string            `json:"description,omitempty"`
	EnvVars             map[string]string `json:"envVars,omitempty"`
	ReservedConcurrency *int              `json:"reservedConcurrency,omitempty"`
}

type ListFunctionsResponse struct {
	Items     []Function `json:"items"`
	NextToken string     `json:"nextToken,omitempty"`
}

type InvokeRequest struct {
	Payload        string `json:"payload"`
	InvocationType string `json:"invocationType"` // "RequestResponse" | "Event" | "DryRun"
}

type InvokeResponse struct {
	StatusCode      int    `json:"statusCode"`
	FunctionError   string `json:"functionError,omitempty"`
	ExecutedVersion string `json:"executedVersion"`
	Payload         string `json:"payload"`
	LogResult       string `json:"logResult"`
	BilledDuration  int64  `json:"billedDurationMs,omitempty"`
	RequestId       string `json:"requestId,omitempty"`
}

type UpdateConfigRequest struct {
	Timeout     *int              `json:"timeout"`
	MemorySize  *int              `json:"memorySize"`
	Handler     *string           `json:"handler"`
	Description *string           `json:"description"`
	EnvVars     map[string]string `json:"envVars"`
	RoleARN     *string           `json:"roleArn"`
}
