package ecsui

// ECSCluster is an ECS cluster summary.
type ECSCluster struct {
	Name   string `json:"name"`
	ARN    string `json:"arn,omitempty"`
	Status string `json:"status,omitempty"`
}

// ListECSClustersResponse is returned by GET /clusters.
type ListECSClustersResponse struct {
	Items     []ECSCluster `json:"items"`
	Total     int          `json:"total"`
	NextToken string       `json:"nextToken,omitempty"`
}

// ECSTask is an ECS task summary.
type ECSTask struct {
	ARN              string `json:"arn"`
	TaskDefinition   string `json:"taskDefinition,omitempty"`
	LastStatus       string `json:"lastStatus,omitempty"`
	DesiredStatus    string `json:"desiredStatus,omitempty"`
	ClusterARN       string `json:"clusterArn,omitempty"`
}

// ListECSTasksResponse is returned by GET /clusters/{name}/tasks.
type ListECSTasksResponse struct {
	Items []ECSTask `json:"items"`
	Total int       `json:"total"`
}

// ECSService is an ECS service summary.
type ECSService struct {
	Name        string `json:"name"`
	ARN         string `json:"arn,omitempty"`
	Status      string `json:"status,omitempty"`
	TaskDef     string `json:"taskDefinition,omitempty"`
	DesiredCount int   `json:"desiredCount,omitempty"`
	RunningCount int   `json:"runningCount,omitempty"`
}

// ListECSServicesResponse is returned by GET /clusters/{name}/services.
type ListECSServicesResponse struct {
	Items []ECSService `json:"items"`
	Total int          `json:"total"`
}
