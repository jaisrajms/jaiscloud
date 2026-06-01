package kinesisui

// Stream is a Kinesis stream summary.
type Stream struct {
	Name   string `json:"name"`
	ARN    string `json:"arn,omitempty"`
	Status string `json:"status,omitempty"`
	Mode   string `json:"mode,omitempty"`
}

// ListStreamsResponse is returned by GET /streams.
type ListStreamsResponse struct {
	Items     []Stream `json:"items"`
	Total     int      `json:"total"`
	NextToken string   `json:"nextToken,omitempty"`
}
