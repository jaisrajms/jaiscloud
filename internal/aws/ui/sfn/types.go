package sfnui

// StateMachine is a Step Functions state machine.
type StateMachine struct {
	ARN        string `json:"arn"`
	Name       string `json:"name"`
	Type       string `json:"type,omitempty"`
	Status     string `json:"status,omitempty"`
	Definition string `json:"definition,omitempty"`
	RoleARN    string `json:"roleArn,omitempty"`
	CreatedAt  int64  `json:"createdAt,omitempty"`
}

// ListStateMachinesResponse is returned by GET /state-machines.
type ListStateMachinesResponse struct {
	Items     []StateMachine `json:"items"`
	Total     int            `json:"total"`
	NextToken string         `json:"nextToken,omitempty"`
}

// Execution is a Step Functions execution.
type Execution struct {
	ARN             string `json:"arn"`
	Name            string `json:"name"`
	StateMachineARN string `json:"stateMachineArn"`
	Status          string `json:"status"`
	StartDate       int64  `json:"startDate,omitempty"`
	StopDate        int64  `json:"stopDate,omitempty"`
	Input           string `json:"input,omitempty"`
	Output          string `json:"output,omitempty"`
}

// ListExecutionsResponse is returned by GET /state-machines/{arn}/executions.
type ListExecutionsResponse struct {
	Items     []Execution `json:"items"`
	Total     int         `json:"total"`
	NextToken string      `json:"nextToken,omitempty"`
}

// HistoryEvent is a single entry in an execution's history.
type HistoryEvent struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
}

// ExecutionHistoryResponse is returned by GET /executions/{arn}/history.
type ExecutionHistoryResponse struct {
	Events []HistoryEvent `json:"events"`
}
