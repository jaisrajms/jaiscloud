package eventbridgeui

// Rule is an EventBridge rule.
type Rule struct {
	Name               string `json:"name"`
	ARN                string `json:"arn,omitempty"`
	EventBusName       string `json:"eventBusName,omitempty"`
	State              string `json:"state"`
	EventPattern       string `json:"eventPattern,omitempty"`
	ScheduleExpression string `json:"scheduleExpression,omitempty"`
	Description        string `json:"description,omitempty"`
}

// ListRulesResponse is returned by GET /rules.
type ListRulesResponse struct {
	Items     []Rule `json:"items"`
	Total     int    `json:"total"`
	NextToken string `json:"nextToken,omitempty"`
}

// Target is an EventBridge rule target.
type Target struct {
	ID  string `json:"id"`
	ARN string `json:"arn"`
}

// ListTargetsResponse is returned by GET /rules/{name}/targets.
type ListTargetsResponse struct {
	Items []Target `json:"items"`
	Total int      `json:"total"`
}

// EventBus is an EventBridge event bus.
type EventBus struct {
	Name string `json:"name"`
	ARN  string `json:"arn,omitempty"`
}

// ListEventBusesResponse is returned by GET /buses.
type ListEventBusesResponse struct {
	Items []EventBus `json:"items"`
	Total int        `json:"total"`
}
