package cfnui

// Stack is a CloudFormation stack summary.
type Stack struct {
	Name        string `json:"name"`
	Status      string `json:"status,omitempty"`
	StackID     string `json:"stackId,omitempty"`
	Description string `json:"description,omitempty"`
	CreatedAt   string `json:"createdAt,omitempty"`
}

// ListStacksResponse is returned by GET /stacks.
type ListStacksResponse struct {
	Items     []Stack `json:"items"`
	Total     int     `json:"total"`
	NextToken string  `json:"nextToken,omitempty"`
}
