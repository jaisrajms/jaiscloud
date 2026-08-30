package emrui

// ClusterSummary is a single item in the ListClusters response.
type ClusterSummary struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"`
	ARN   string `json:"arn,omitempty"`
}

// ListClustersResponse is returned by GET /clusters.
type ListClustersResponse struct {
	Items     []ClusterSummary `json:"items"`
	NextToken string           `json:"nextToken,omitempty"`
	Total     int              `json:"total"`
}

// ClusterDetail is the full cluster detail returned by GET /clusters/{id}.
type ClusterDetail struct {
	ID                   string         `json:"id"`
	Name                 string         `json:"name"`
	State                string         `json:"state"`
	StateChangeReason    string         `json:"stateChangeReason,omitempty"`
	ARN                  string         `json:"arn,omitempty"`
	ReleaseLabel         string         `json:"releaseLabel,omitempty"`
	LogURI               string         `json:"logUri,omitempty"`
	AutoTerminate        bool           `json:"autoTerminate"`
	TerminationProtected bool           `json:"terminationProtected"`
	Applications         []string       `json:"applications,omitempty"`
	Tags                 []TagEntry     `json:"tags,omitempty"`
}

// TagEntry is a key/value tag.
type TagEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ClusterStatus is returned by GET /clusters/{id}/status.
type ClusterStatus struct {
	State             string        `json:"state"`
	StateChangeReason string        `json:"stateChangeReason,omitempty"`
	Steps             []StepSummary `json:"steps"`
}

// StepSummary is a brief step description within ClusterStatus.
type StepSummary struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"`
}

// Step is a full step description.
type Step struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	State  string `json:"state"`
	Config any    `json:"config,omitempty"`
}

// ListStepsResponse is returned by GET /clusters/{id}/steps.
type ListStepsResponse struct {
	Items     []Step `json:"items"`
	NextToken string `json:"nextToken,omitempty"`
	Total     int    `json:"total"`
}
