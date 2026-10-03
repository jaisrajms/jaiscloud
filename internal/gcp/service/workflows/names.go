package workflows

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// WorkflowName is the Cloud Workflows resource name for a workflow
// (projects/{p}/locations/{l}/workflows/{w}).
func WorkflowName(project, location, workflowID string) string {
	return resource.ResourceID(project)("workflow", location+"/"+workflowID)
}

// OperationName is the Cloud Workflows long-running operation resource name
// (projects/{p}/locations/{l}/operations/{id}).
func OperationName(project, location, operationID string) string {
	return resource.ResourceID(project)("workflow-operation", location+"/"+operationID)
}

// DefaultServiceAccount returns the project's Compute Engine default service
// account, the identity real GCP assigns to a workflow whose service_account is
// unset. The account name is the project number (the emulator synthesizes one
// via resource.ProjectNumber), matching real GCP's
// "{project-number}-compute@developer.gserviceaccount.com".
func DefaultServiceAccount(project string) string {
	account := resource.ProjectNumber(project) + "-compute@developer.gserviceaccount.com"
	return resource.ResourceID(project)("service-account", account)
}

// ProjectFromName returns the project id from a "projects/{p}/..." resource
// name, or "" when the name is not project-scoped.
func ProjectFromName(name string) string { return segmentAfter(name, "projects") }

// LocationFromName returns the location segment from a name of the form
// "locations/{l}/...", or "".
func LocationFromName(name string) string { return segmentAfter(name, "locations") }

// WorkflowIDFromName extracts the workflow ID from a relative name
// ("locations/{l}/workflows/{id}") or a full projects/{p}/-prefixed name.
func WorkflowIDFromName(name string) string { return segmentAfter(name, "workflows") }

// OperationIDFromName extracts the operation ID from a name ending in
// /operations/{id}.
func OperationIDFromName(name string) string { return segmentAfter(name, "operations") }

// segmentAfter returns the path segment immediately following marker, or ""
// when marker is absent or last.
func segmentAfter(name, marker string) string {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		if p == marker && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}
