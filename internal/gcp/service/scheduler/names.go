package scheduler

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// JobName returns the canonical Cloud Scheduler job resource name
// (projects/{project}/locations/{location}/jobs/{job}).
func JobName(project, location, job string) string {
	return resource.ResourceID(project)("scheduler-job", location+"/"+job)
}

// ParseJobName splits a canonical job name into its parts. It returns ok=false
// when the name is not a projects/{p}/locations/{l}/jobs/{j} path.
func ParseJobName(name string) (project, location, job string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 6 {
		return "", "", "", false
	}
	if segs[0] != "projects" || segs[2] != "locations" || segs[4] != "jobs" {
		return "", "", "", false
	}
	return segs[1], segs[3], segs[5], true
}

// ParseParent splits a location parent name projects/{p}/locations/{l}.
func ParseParent(parent string) (project, location string, ok bool) {
	segs := strings.Split(strings.Trim(parent, "/"), "/")
	if len(segs) != 4 || segs[0] != "projects" || segs[2] != "locations" {
		return "", "", false
	}
	return segs[1], segs[3], true
}
