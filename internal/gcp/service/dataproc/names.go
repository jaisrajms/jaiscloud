package dataproc

import "jaiscloud/internal/gcp/resource"

// ClusterName is the Dataproc cluster resource name
// (projects/{p}/regions/{r}/clusters/{name}).
func ClusterName(project, region, name string) string {
	return resource.ResourceID(project)("dataproc-cluster", region+"/"+name)
}

// JobName is the Dataproc job resource name
// (projects/{p}/regions/{r}/jobs/{id}).
func JobName(project, region, jobID string) string {
	return resource.ResourceID(project)("dataproc-job", region+"/"+jobID)
}

// OperationName is the Dataproc long-running operation resource name
// (projects/{p}/regions/{r}/operations/{id}).
func OperationName(project, region, id string) string {
	return resource.ResourceID(project)("dataproc-operation", region+"/"+id)
}

// WorkflowTemplateName is the Dataproc workflow template resource name
// (projects/{p}/regions/{r}/workflowTemplates/{id}).
func WorkflowTemplateName(project, region, id string) string {
	return resource.ResourceID(project)("dataproc-workflowTemplate", region+"/"+id)
}
