package resource

import "strings"

// Compute Engine self-link helpers.
//
// A Compute Engine resource is named three ways and callers mix them: a short
// name ("default", "us-central1-a"), a project-relative partial resource name
// ("projects/p/global/networks/default") and the absolute self-link every
// Compute API returns ("https://www.googleapis.com/compute/v1/projects/p/global/
// networks/default"). Services that report a Compute link back — Dataproc's
// GceClusterConfig zoneUri/networkUri/subnetworkUri/machineTypeUri and GKE's
// derived networkConfig paths — must resolve all three to one canonical value,
// so the resolution lives here instead of once per service.

// ComputeBasePath is the public Compute Engine REST root that self-links are
// absolute under (the same link shape the emulated Compute service renders for
// its own selfLinks).
const ComputeBasePath = "https://www.googleapis.com/compute/v1/"

// LinkTail returns the last path segment of a Compute Engine resource link: the
// name of a short form ("n1-standard-4"), a partial resource name
// ("zones/us-central1-a/machineTypes/n1-standard-4") and a full self-link all
// name the same resource in their last segment.
func LinkTail(uri string) string {
	s := strings.TrimRight(strings.TrimSpace(uri), "/")
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	return s
}

// PathSegmentAfter returns the segment following marker in a slash-separated
// resource link ("regions/us-central1/subnetworks/default" -> "us-central1"),
// or "" when the link does not carry the marker as a path segment.
func PathSegmentAfter(uri, marker string) string {
	parts := strings.Split(strings.Trim(strings.TrimSpace(uri), "/"), "/")
	for i, p := range parts {
		if p == marker && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// ProjectFromURI returns the project a Compute Engine link names ("projects/
// {p}/..."), or "" for a short name or a project-relative partial resource name.
func ProjectFromURI(uri string) string {
	return PathSegmentAfter(uri, "projects")
}

// ComputePath is the project-relative resource name of a Compute Engine
// resource: ComputeLink without the REST root.
func ComputePath(project, rt, name string) string {
	return ResourceID(project)(rt, name)
}

// ComputePathFor is ComputePath for a caller-supplied link: the project the
// caller named wins (a Shared VPC subnetwork lives in the host project, and a
// read must never rename it into the consuming project), while a short name or
// a project-relative partial resource name is resolved against fallback.
func ComputePathFor(uri, fallback, rt, name string) string {
	if p := ProjectFromURI(uri); p != "" {
		fallback = p
	}
	return ComputePath(fallback, rt, name)
}

// ComputeLink is the absolute Compute Engine self-link for a resource.
func ComputeLink(project, rt, name string) string {
	return ComputeBasePath + ComputePath(project, rt, name)
}

// ComputeLinkFor is ComputeLink for a caller-supplied link, preserving the
// project the caller named (see ComputePathFor).
func ComputeLinkFor(uri, fallback, rt, name string) string {
	return ComputeBasePath + ComputePathFor(uri, fallback, rt, name)
}
