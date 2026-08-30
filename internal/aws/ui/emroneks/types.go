package emroneksui

// VirtualCluster is an EMR on EKS virtual cluster.
type VirtualCluster struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	State      string `json:"state"`
	EksCluster string `json:"eksCluster,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	ARN        string `json:"arn,omitempty"`
}

// ListVirtualClustersResponse is returned by GET /virtual-clusters.
type ListVirtualClustersResponse struct {
	Items []VirtualCluster `json:"items"`
	Total int              `json:"total"`
}

// JobRun is a single job run.
type JobRun struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	VirtualClusterID string `json:"virtualClusterId"`
	State            string `json:"state"`
	ReleaseLabel     string `json:"releaseLabel,omitempty"`
	ARN              string `json:"arn,omitempty"`
}

// ListJobRunsResponse is returned by GET /virtual-clusters/{vcId}/jobs.
type ListJobRunsResponse struct {
	Items []JobRun `json:"items"`
	Total int      `json:"total"`
}
