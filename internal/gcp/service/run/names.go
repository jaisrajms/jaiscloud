package run

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// ServiceName returns the canonical Cloud Run service resource name
// (projects/{project}/locations/{location}/services/{service}).
func ServiceName(project, location, id string) string {
	return resource.ResourceID(project)("run-service", location+"/"+id)
}

// RevisionName returns the canonical Cloud Run revision resource name
// (projects/{project}/locations/{location}/services/{service}/revisions/{rev}).
func RevisionName(project, location, service, rev string) string {
	return resource.ResourceID(project)("run-revision", location+"/"+service+"/"+rev)
}

// OperationName returns the canonical Cloud Run operation resource name
// (projects/{project}/locations/{location}/operations/{operation}).
func OperationName(project, location, op string) string {
	return resource.ResourceID(project)("run-operation", location+"/"+op)
}

// ParseParent splits a location parent name projects/{p}/locations/{l}.
func ParseParent(parent string) (project, location string, ok bool) {
	segs := strings.Split(strings.Trim(parent, "/"), "/")
	if len(segs) != 4 || segs[0] != "projects" || segs[2] != "locations" {
		return "", "", false
	}
	return segs[1], segs[3], true
}

// ParseServiceName splits a canonical service name into its parts. ok is false
// when the name is not projects/{p}/locations/{l}/services/{s}.
func ParseServiceName(name string) (project, location, id string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 6 || segs[0] != "projects" || segs[2] != "locations" || segs[4] != "services" {
		return "", "", "", false
	}
	return segs[1], segs[3], segs[5], true
}

// ParseRevisionName splits a canonical revision name into its parts. ok is
// false when the name is not
// projects/{p}/locations/{l}/services/{s}/revisions/{r}.
func ParseRevisionName(name string) (project, location, service, rev string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 8 || segs[0] != "projects" || segs[2] != "locations" ||
		segs[4] != "services" || segs[6] != "revisions" {
		return "", "", "", "", false
	}
	return segs[1], segs[3], segs[5], segs[7], true
}

// ParseOperationName splits a canonical operation name into its parts. ok is
// false when the name is not projects/{p}/locations/{l}/operations/{o}.
func ParseOperationName(name string) (project, location, op string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 6 || segs[0] != "projects" || segs[2] != "locations" || segs[4] != "operations" {
		return "", "", "", false
	}
	return segs[1], segs[3], segs[5], true
}

// lastSegment returns the final path segment of name (e.g. the service id in a
// full resource name), or "" when name is empty.
func lastSegment(name string) string {
	name = strings.Trim(name, "/")
	if name == "" {
		return ""
	}
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}
