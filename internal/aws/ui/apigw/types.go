package apigwui

// RestAPI is an API Gateway REST API.
type RestAPI struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	CreatedDate string `json:"createdDate,omitempty"`
}

// ListRestAPIsResponse is returned by GET /apis.
type ListRestAPIsResponse struct {
	Items []RestAPI `json:"items"`
	Total int       `json:"total"`
}

// Resource is an API Gateway resource.
type Resource struct {
	ID       string `json:"id"`
	Path     string `json:"path"`
	PathPart string `json:"pathPart,omitempty"`
	ParentID string `json:"parentId,omitempty"`
}

// ListResourcesResponse is returned by GET /apis/{id}/resources.
type ListResourcesResponse struct {
	Items []Resource `json:"items"`
	Total int        `json:"total"`
}

// Stage is an API Gateway stage.
type Stage struct {
	Name         string `json:"name"`
	DeploymentID string `json:"deploymentId,omitempty"`
	Description  string `json:"description,omitempty"`
}

// ListStagesResponse is returned by GET /apis/{id}/stages.
type ListStagesResponse struct {
	Items []Stage `json:"items"`
	Total int     `json:"total"`
}

// Deployment is an API Gateway deployment.
type Deployment struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
	CreatedDate string `json:"createdDate,omitempty"`
}

// ListDeploymentsResponse is returned by GET /apis/{id}/deployments.
type ListDeploymentsResponse struct {
	Items []Deployment `json:"items"`
	Total int          `json:"total"`
}
