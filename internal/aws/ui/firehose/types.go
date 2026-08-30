package firehoseui

// DeliveryStream is a Firehose delivery stream summary.
type DeliveryStream struct {
	Name   string `json:"name"`
	ARN    string `json:"arn,omitempty"`
	Status string `json:"status,omitempty"`
	Type   string `json:"type,omitempty"`
}

// ListDeliveryStreamsResponse is returned by GET /streams.
type ListDeliveryStreamsResponse struct {
	Items []DeliveryStream `json:"items"`
	Total int              `json:"total"`
}
