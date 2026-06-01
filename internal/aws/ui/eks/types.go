package eksui

// Cluster is an EKS cluster summary.
type Cluster struct {
	Name      string `json:"name"`
	Status    string `json:"status,omitempty"`
	ARN       string `json:"arn,omitempty"`
	Version   string `json:"version,omitempty"`
	CreatedAt string `json:"createdAt,omitempty"`
}

// ListClustersResponse is returned by GET /clusters.
type ListClustersResponse struct {
	Items     []Cluster `json:"items"`
	Total     int       `json:"total"`
	NextToken string    `json:"nextToken,omitempty"`
}
