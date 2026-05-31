package logsui

// LogGroup is the typed response struct for a CloudWatch log group.
type LogGroup struct {
	Name          string `json:"name"`
	ARN           string `json:"arn"`
	RetentionDays *int   `json:"retentionDays,omitempty"`
	StoredBytes   int64  `json:"storedBytes"`
	CreatedAt     int64  `json:"createdAt"` // Unix millis
}

// LogStream is the typed response struct for a CloudWatch log stream.
type LogStream struct {
	Name                string `json:"name"`
	ARN                 string `json:"arn"`
	LastEventAt         *int64 `json:"lastEventAt,omitempty"`
	FirstEventAt        *int64 `json:"firstEventAt,omitempty"`
	UploadSequenceToken string `json:"uploadSequenceToken,omitempty"`
}

// LogEvent is a single CloudWatch log event.
type LogEvent struct {
	Timestamp     int64  `json:"timestamp"` // Unix millis
	Message       string `json:"message"`
	IngestionTime int64  `json:"ingestionTime"`
}

// LogEventsResponse is the response for GET events.
type LogEventsResponse struct {
	Events             []LogEvent `json:"events"`
	NextForwardToken   string     `json:"nextForwardToken,omitempty"`
	NextBackwardToken  string     `json:"nextBackwardToken,omitempty"`
	Truncated          bool       `json:"truncated"`
}

// FilterLogEventsRequest is the body for POST filter.
type FilterLogEventsRequest struct {
	LogGroupName  string `json:"logGroupName"`
	FilterPattern string `json:"filterPattern"`
	StartTime     *int64 `json:"startTime"`
	EndTime       *int64 `json:"endTime"`
	NextToken     string `json:"nextToken"`
	Limit         int    `json:"limit"`
}

// StartQueryRequest is the body for POST /queries.
type StartQueryRequest struct {
	QueryString   string   `json:"queryString"`
	LogGroupName  string   `json:"logGroupName,omitempty"`
	LogGroupNames []string `json:"logGroupNames,omitempty"`
	StartTime     int64    `json:"startTime,omitempty"`
	EndTime       int64    `json:"endTime,omitempty"`
}

// QueryResult is the response from GET /queries/{id}.
type QueryResult struct {
	QueryID    string              `json:"queryId"`
	Status     string              `json:"status"`
	Results    [][]map[string]string `json:"results"`
	Statistics map[string]float64  `json:"statistics"`
}
