package serviceusage

import (
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

// buildAPI renders the typed form of a service.
func buildAPI(project, service string, state State) API {
	return API{
		Name:       ServiceName(project, service),
		Parent:     ParentName(project),
		ConfigName: service,
		State:      state,
	}
}
