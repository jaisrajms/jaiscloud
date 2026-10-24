package container

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// ClusterName returns the canonical GKE cluster resource name
// (projects/{project}/locations/{location}/clusters/{cluster}).
func ClusterName(project, location, cluster string) string {
	return resource.ResourceID(project)("container-cluster", location+"/"+cluster)
}

// OperationName returns the canonical GKE operation resource name
// (projects/{project}/locations/{location}/operations/{operation}).
func OperationName(project, location, operation string) string {
	return resource.ResourceID(project)("container-operation", location+"/"+operation)
}

// ParseClusterName splits a canonical cluster name into its parts. It returns
// ok=false when the name is not a projects/{p}/locations/{l}/clusters/{c} path.
func ParseClusterName(name string) (project, location, cluster string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 6 {
		return "", "", "", false
	}
	if segs[0] != "projects" || segs[2] != "locations" || segs[4] != "clusters" {
		return "", "", "", false
	}
	return segs[1], segs[3], segs[5], true
}

// NodePoolName returns the canonical GKE node pool resource name
// (projects/{project}/locations/{location}/clusters/{cluster}/nodePools/{pool}).
func NodePoolName(project, location, cluster, pool string) string {
	return resource.ResourceID(project)("container-node-pool", location+"/"+cluster+"/"+pool)
}

// ParseNodePoolName splits a canonical node pool name into its parts. It
// returns ok=false when the name is not a
// projects/{p}/locations/{l}/clusters/{c}/nodePools/{np} path.
func ParseNodePoolName(name string) (project, location, cluster, pool string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 8 {
		return "", "", "", "", false
	}
	if segs[0] != "projects" || segs[2] != "locations" || segs[4] != "clusters" || segs[6] != "nodePools" {
		return "", "", "", "", false
	}
	return segs[1], segs[3], segs[5], segs[7], true
}

// ParseOperationName splits a canonical operation name into its parts. It
// returns ok=false when the name is not a
// projects/{p}/locations/{l}/operations/{o} path.
func ParseOperationName(name string) (project, location, operation string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 6 {
		return "", "", "", false
	}
	if segs[0] != "projects" || segs[2] != "locations" || segs[4] != "operations" {
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
