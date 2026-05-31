package sqsui

// Queue is the typed response struct for a single SQS queue.
type Queue struct {
	Name               string            `json:"name"`
	URL                string            `json:"url"`
	ARN                string            `json:"arn"`
	Type               string            `json:"type"` // "Standard" | "FIFO"
	MessagesAvailable  int64             `json:"messagesAvailable"`
	MessagesInFlight   int64             `json:"messagesInFlight"`
	VisibilityTimeout  int64             `json:"visibilityTimeout"`
	RetentionPeriod    int64             `json:"retentionPeriod"`
	MaxMessageSize     int64             `json:"maxMessageSize"`
	DLQArn             string            `json:"dlqArn,omitempty"`
	DLQMaxReceive      int64             `json:"dlqMaxReceive,omitempty"`
	CreatedAt          string            `json:"createdAt"`
	Tags               map[string]string `json:"tags,omitempty"`
}

type ListQueuesResponse struct {
	Items     []Queue `json:"items"`
	NextToken string  `json:"nextToken,omitempty"`
	Total     *int64  `json:"total,omitempty"`
}

type CreateQueueRequest struct {
	Name               string            `json:"name"`
	Type               string            `json:"type"` // "Standard" | "FIFO"
	VisibilityTimeout  *int              `json:"visibilityTimeout"`
	RetentionPeriod    *int              `json:"retentionPeriod"`
	DLQArn             string            `json:"dlqArn"`
	DLQMaxReceive      *int              `json:"dlqMaxReceive"`
	Tags               map[string]string `json:"tags"`
}

type SendMessageRequest struct {
	Body                    string         `json:"body"`
	DelaySeconds            *int           `json:"delaySeconds"`
	MessageAttributes       map[string]any `json:"messageAttributes"`
	MessageGroupId          string         `json:"messageGroupId"`
	MessageDeduplicationId  string         `json:"messageDeduplicationId"`
}

type Message struct {
	MessageId     string         `json:"messageId"`
	Body          string         `json:"body"`
	ReceiptHandle string         `json:"receiptHandle"`
	Attributes    map[string]any `json:"attributes"`
	SentAt        string         `json:"sentAt"`
}

type ReceiveMessagesResponse struct {
	Messages []Message `json:"messages"`
}

type DLQSourceQueuesResponse struct {
	Items     []Queue `json:"items"`
	NextToken string  `json:"nextToken,omitempty"`
}

type PeekedMessage struct {
	MessageId     string `json:"messageId"`
	Body          string `json:"body"`
	SentAt        string `json:"sentAt"`
	ReceiveCount  int    `json:"receiveCount"`
	Status        string `json:"status"` // "visible" | "in-flight" | "delayed"
	GroupId       string `json:"groupId,omitempty"`
}

type PeekMessagesResponse struct {
	Messages []PeekedMessage `json:"messages"`
	Total    int             `json:"total"`
	Offset   int             `json:"offset"`
	Limit    int             `json:"limit"`
}
