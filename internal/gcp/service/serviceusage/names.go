package serviceusage

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// ServiceName is the Service Usage v1 resource name of a service
// (projects/{project}/services/{service}).
func ServiceName(project, service string) string {
	return resource.ResourceID(project)("serviceusage-service", service)
}

// ParentName is the consumer resource name of a project (projects/{project}).
func ParentName(project string) string {
	return resource.ResourceID(project)("serviceusage-parent", "")
}

// OperationName is the resource name of a Service Usage long-running operation
// (operations/{id}).
func OperationName(id string) string {
	return resource.ResourceID("")("serviceusage-operation", id)
}

// splitServiceName parses a full Service Usage service resource name
// (projects/{project}/services/{service}) into its project and service parts.
// It reports false for any other shape, including a bare DNS id.
func splitServiceName(name string) (project, service string, ok bool) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "services" ||
		parts[1] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// buildAPI renders the typed form of a service.
func buildAPI(project, service string, state State) API {
	return API{
		Name:       ServiceName(project, service),
		Parent:     ParentName(project),
		ConfigName: service,
		State:      state,
	}
}
