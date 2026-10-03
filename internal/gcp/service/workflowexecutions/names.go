package workflowexecutions

import (
	"jaiscloud/internal/gcp/resource"
)

// ExecutionName is the Cloud Workflow Executions resource name for an
// execution (projects/{p}/locations/{l}/workflows/{w}/executions/{e}).
func ExecutionName(project, location, workflowID, executionID string) string {
	return resource.ResourceID(project)("workflow-execution", location+"/"+workflowID+"/"+executionID)
}
