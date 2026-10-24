package tasks

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// QueueName returns the canonical Cloud Tasks queue resource name
// (projects/{project}/locations/{location}/queues/{queue}).
func QueueName(project, location, queue string) string {
	return resource.ResourceID(project)("tasks-queue", location+"/"+queue)
}

// TaskName returns the canonical Cloud Tasks task resource name
// (projects/{project}/locations/{location}/queues/{queue}/tasks/{task}).
func TaskName(project, location, queue, task string) string {
	return resource.ResourceID(project)("tasks-task", location+"/"+queue+"/"+task)
}

// OperationName returns the canonical Cloud Tasks long-running operation
// resource name (projects/{project}/locations/{location}/operations/{id}). The
// REST batchCreate/batchDelete methods return a google.longrunning.Operation in
// this namespace; the emulator performs the work inline and returns a done
// operation, so the id is ephemeral.
func OperationName(project, location, id string) string {
	return resource.ResourceID(project)("tasks-operation", location+"/"+id)
}

// ParseQueueName splits a canonical queue name into its parts. It returns
// ok=false when the name is not a
// projects/{p}/locations/{l}/queues/{q} path.
func ParseQueueName(name string) (project, location, queue string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 6 {
		return "", "", "", false
	}
	if segs[0] != "projects" || segs[2] != "locations" || segs[4] != "queues" {
		return "", "", "", false
	}
	return segs[1], segs[3], segs[5], true
}

// ParseTaskName splits a canonical task name into its parts. It returns
// ok=false when the name is not a
// projects/{p}/locations/{l}/queues/{q}/tasks/{t} path.
func ParseTaskName(name string) (project, location, queue, task string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 8 {
		return "", "", "", "", false
	}
	if segs[0] != "projects" || segs[2] != "locations" || segs[4] != "queues" || segs[6] != "tasks" {
		return "", "", "", "", false
	}
	return segs[1], segs[3], segs[5], segs[7], true
}

// ParseQueueParent splits a location parent name projects/{p}/locations/{l}.
func ParseQueueParent(parent string) (project, location string, ok bool) {
	segs := strings.Split(strings.Trim(parent, "/"), "/")
	if len(segs) != 4 || segs[0] != "projects" || segs[2] != "locations" {
		return "", "", false
	}
	return segs[1], segs[3], true
}

// ParseTaskParent splits a queue parent name
// projects/{p}/locations/{l}/queues/{q}.
func ParseTaskParent(parent string) (project, location, queue string, ok bool) {
	segs := strings.Split(strings.Trim(parent, "/"), "/")
	if len(segs) != 6 || segs[0] != "projects" || segs[2] != "locations" || segs[4] != "queues" {
		return "", "", "", false
	}
	return segs[1], segs[3], segs[5], true
}
