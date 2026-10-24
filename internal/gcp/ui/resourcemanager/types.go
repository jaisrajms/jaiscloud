// Package resourcemanagerui serves the Cloud Resource Manager UI API: the
// console project manager (list/create/delete/undelete projects and show
// lifecycle state). Handlers call the transport-neutral Resource Manager core
// directly (in-process) rather than over the wire, so the console and the wire
// API share one project registry.
//
// The console is project-agnostic here: unlike the per-project service pages it
// enumerates every project the emulator knows, including DELETE_REQUESTED ones
// so a deleted project can be restored.
package resourcemanagerui

// Project is the console rendering of a Cloud Resource Manager project,
// flattening the canonical core fields with RFC3339 timestamps.
type Project struct {
	ProjectID     string            `json:"projectId"`
	ProjectNumber string            `json:"projectNumber,omitempty"`
	DisplayName   string            `json:"displayName,omitempty"`
	State         string            `json:"state"`
	Etag          string            `json:"etag,omitempty"`
	Parent        string            `json:"parent,omitempty"`
	CreateTime    string            `json:"createTime,omitempty"`
	UpdateTime    string            `json:"updateTime,omitempty"`
	DeleteTime    string            `json:"deleteTime,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

// ListProjectsResponse is the response for GET /projects.
type ListProjectsResponse struct {
	Projects []Project `json:"projects"`
	Total    int       `json:"total"`
}

// CreateProjectInput is the create form body. The project number, lifecycle
// state, and timestamps are derived by the core.
type CreateProjectInput struct {
	ProjectID   string            `json:"projectId"`
	DisplayName string            `json:"displayName"`
	Labels      map[string]string `json:"labels"`
}
