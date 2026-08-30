package snsui

// Topic is the UI representation of an SNS topic.
type Topic struct {
	ARN               string            `json:"arn"`
	Name              string            `json:"name"`
	DisplayName       string            `json:"displayName,omitempty"`
	SubscriptionCount int               `json:"subscriptionCount"`
	Type              string            `json:"type"` // "Standard" | "FIFO"
	Attributes        map[string]string `json:"attributes,omitempty"`
}

// ListTopicsResponse is the response for GET /sns/topics.
type ListTopicsResponse struct {
	Items     []Topic `json:"items"`
	NextToken string  `json:"nextToken,omitempty"`
	Total     int     `json:"total"`
}

// Subscription is the UI representation of an SNS subscription.
type Subscription struct {
	SubscriptionARN string `json:"subscriptionArn"`
	TopicARN        string `json:"topicArn"`
	Protocol        string `json:"protocol"`
	Endpoint        string `json:"endpoint"`
	Owner           string `json:"owner,omitempty"`
}

// ListSubscriptionsResponse is the response for GET /sns/topics/{arn}/subscriptions.
type ListSubscriptionsResponse struct {
	Items     []Subscription `json:"items"`
	NextToken string         `json:"nextToken,omitempty"`
}

// CreateTopicRequest is the body for POST /sns/topics.
type CreateTopicRequest struct {
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	FIFO       bool              `json:"fifo,omitempty"`
}

// SubscribeRequest is the body for POST /sns/topics/{arn}/subscriptions.
type SubscribeRequest struct {
	Protocol string `json:"protocol"`
	Endpoint string `json:"endpoint"`
}

// PublishRequest is the body for POST /sns/topics/{arn}/publish.
type PublishRequest struct {
	Message  string `json:"message"`
	Subject  string `json:"subject,omitempty"`
}
