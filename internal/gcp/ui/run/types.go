// Package runui serves the Cloud Run UI API. Handlers call the transport-neutral
// Cloud Run core directly (in-process) rather than over the wire.
//
// The console is region-optional: the list aggregates every service in the
// project across all locations (no region picker) and shows the location as a
// read-only field. Detail/delete link back with the region carried from the
// list row, so a service name never has to be disambiguated by hand.
package runui

import "jaiscloud/internal/gcp/ui/uihelper"

// Service is the UI summary of a Cloud Run service, flattened across locations
// for the list page.
type Service struct {
	ID                    string `json:"id"`
	Region                string `json:"region"`
	Name                  string `json:"name"`
	URI                   string `json:"uri,omitempty"`
	LatestReadyRevision   string `json:"latestReadyRevision,omitempty"`
	LatestCreatedRevision string `json:"latestCreatedRevision,omitempty"`
	Generation            string `json:"generation,omitempty"`
	CreateTime            string `json:"createTime,omitempty"`
	UpdateTime            string `json:"updateTime,omitempty"`
	Ready                 bool   `json:"ready"`
}

// ListServicesResponse is the response for GET /services.
type ListServicesResponse struct {
	Services []Service `json:"services"`
	Total    int       `json:"total"`
}

// ListRevisionsResponse is the response for GET /services/{region}/{service}/revisions.
// Revisions carry the full google.cloud.run.v2.Revision wire map, so the detail
// page can render the container template verbatim.
type ListRevisionsResponse struct {
	Revisions []map[string]any `json:"revisions"`
	Total     int              `json:"total"`
}

// DeployServiceRequest is the body for POST /services. The console is
// region-optional, so the location travels in the body with the service id and
// the google.cloud.run.v2.Service object.
type DeployServiceRequest struct {
	Region       string         `json:"region"`
	ID           string         `json:"id"`
	Body         map[string]any `json:"body"`
	ValidateOnly bool           `json:"validateOnly,omitempty"`
}

// UpdateServiceRequest is the body for PATCH /services/{region}/{service}.
// UpdateMask carries the camelCase field paths the user touched; the handler
// joins them into the comma-separated form the core expects.
type UpdateServiceRequest struct {
	Body         map[string]any `json:"body"`
	UpdateMask   []string       `json:"updateMask,omitempty"`
	ValidateOnly bool           `json:"validateOnly,omitempty"`
	AllowMissing bool           `json:"allowMissing,omitempty"`
}

// SetIamPolicyRequest is the body for PUT .../iam.
type SetIamPolicyRequest struct {
	Policy uihelper.IamPolicy `json:"policy"`
}

// TestIamPermissionsRequest is the body for .../iam/testIamPermissions.
type TestIamPermissionsRequest struct {
	Permissions []string `json:"permissions"`
}

// TestIamPermissionsResponse is the response for testIamPermissions.
type TestIamPermissionsResponse struct {
	Permissions []string `json:"permissions"`
}
