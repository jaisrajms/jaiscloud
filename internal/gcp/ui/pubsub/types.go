package pubsubui

// Topic is the UI representation of a Pub/Sub topic. Name is the short topic ID
// (the last path segment of the provider's full resource name).
type Topic struct {
	Name                     string            `json:"name"`
	FullName                 string            `json:"fullName,omitempty"`
	MessageRetentionDuration string            `json:"messageRetentionDuration,omitempty"`
	KmsKeyName               string            `json:"kmsKeyName,omitempty"`
	Labels                   map[string]string `json:"labels,omitempty"`
}

// ListTopicsResponse is the response for GET /topics.
type ListTopicsResponse struct {
	Topics        []Topic `json:"topics"`
	Total         int     `json:"total"`
	NextPageToken string  `json:"nextPageToken,omitempty"`
}

// Subscription is the UI representation of a Pub/Sub subscription. Name is the
// short subscription ID; Topic is the short topic ID (or the "_deleted-topic_"
// sentinel once the topic has been removed).
type Subscription struct {
	Name                      string            `json:"name"`
	FullName                  string            `json:"fullName,omitempty"`
	Topic                     string            `json:"topic,omitempty"`
	TopicFull                 string            `json:"topicFull,omitempty"`
	AckDeadlineSeconds        int               `json:"ackDeadlineSeconds,omitempty"`
	MessageRetentionDuration  string            `json:"messageRetentionDuration,omitempty"`
	ExpirationTtl             string            `json:"expirationTtl,omitempty"`
	EnableExactlyOnceDelivery bool              `json:"enableExactlyOnceDelivery,omitempty"`
	EnableMessageOrdering     bool              `json:"enableMessageOrdering,omitempty"`
	Filter                    string            `json:"filter,omitempty"`
	PushEndpoint              string            `json:"pushEndpoint,omitempty"`
	State                     string            `json:"state,omitempty"`
	Detached                  bool              `json:"detached,omitempty"`
	DeadLetterTopic           string            `json:"deadLetterTopic,omitempty"`
	MaxDeliveryAttempts       int               `json:"maxDeliveryAttempts,omitempty"`
	RetryPolicy               map[string]any    `json:"retryPolicy,omitempty"`
	Labels                    map[string]string `json:"labels,omitempty"`
}

// ListSubscriptionsResponse is the response for GET /subscriptions.
type ListSubscriptionsResponse struct {
	Subscriptions []Subscription `json:"subscriptions"`
	Total         int            `json:"total"`
	NextPageToken string         `json:"nextPageToken,omitempty"`
}

// CreateTopicRequest is the body for POST /topics.
type CreateTopicRequest struct {
	Name                     string            `json:"name"`
	MessageRetentionDuration string            `json:"messageRetentionDuration,omitempty"`
	KmsKeyName               string            `json:"kmsKeyName,omitempty"`
	Labels                   map[string]string `json:"labels,omitempty"`
}

// PublishMessage is one message in a publish request. Data is base64-encoded,
// matching the Pub/Sub wire representation.
type PublishMessage struct {
	Data        string            `json:"data"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	OrderingKey string            `json:"orderingKey,omitempty"`
}

// PublishRequest is the body for POST /topics/{topic}/publish.
type PublishRequest struct {
	Messages []PublishMessage `json:"messages"`
}

// CreateSubscriptionRequest is the body for POST /subscriptions. Topic is the
// short topic ID; the handler expands both Topic and DeadLetterTopic to full
// resource names.
type CreateSubscriptionRequest struct {
	Name                      string `json:"name"`
	Topic                     string `json:"topic"`
	AckDeadlineSeconds        int    `json:"ackDeadlineSeconds,omitempty"`
	MessageRetentionDuration  string `json:"messageRetentionDuration,omitempty"`
	EnableExactlyOnceDelivery bool   `json:"enableExactlyOnceDelivery,omitempty"`
	EnableMessageOrdering     bool   `json:"enableMessageOrdering,omitempty"`
	Filter                    string `json:"filter,omitempty"`
	PushEndpoint              string `json:"pushEndpoint,omitempty"`
	DeadLetterTopic           string `json:"deadLetterTopic,omitempty"`
	MaxDeliveryAttempts       int    `json:"maxDeliveryAttempts,omitempty"`
}

// UpdateSubscriptionRequest is the body for PATCH /subscriptions/{subscription}.
// UpdateMask names the fields carried in Subscription. When the mask is empty
// the handler derives it from the subscription's keys, so a partial body with
// only the changed fields is enough.
type UpdateSubscriptionRequest struct {
	UpdateMask   string         `json:"updateMask,omitempty"`
	Subscription map[string]any `json:"subscription"`
}
