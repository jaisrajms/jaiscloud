package eventarc

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// TriggerName is the Eventarc trigger resource name
// (projects/{p}/locations/{l}/triggers/{id}).
func TriggerName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-trigger", location+"/"+id)
}

// ChannelName is the Eventarc channel resource name
// (projects/{p}/locations/{l}/channels/{id}).
func ChannelName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-channel", location+"/"+id)
}

// ProviderName is the Eventarc provider resource name
// (projects/{p}/locations/{l}/providers/{id}).
func ProviderName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-provider", location+"/"+id)
}

// OperationName is the Eventarc long-running operation resource name
// (projects/{p}/locations/{l}/operations/{id}).
func OperationName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-operation", location+"/"+id)
}

// ResourceName is the parsed form of an Eventarc resource name.
type ResourceName struct {
	Project   string
	Location  string
	Trigger   string
	Channel   string
	Provider  string
	Operation string
}

// ProjectFromName returns the project id from a "projects/{p}/..." resource
// name, or "" when the name is not project-scoped.
func ProjectFromName(name string) string { return ParseName(name).Project }

// ParseName extracts the hierarchical components of an Eventarc resource name.
// Segments are matched by their collection keyword; an unknown segment is
// skipped.
func ParseName(name string) ResourceName {
	var out ResourceName
	segs := strings.Split(name, "/")
	for i := 0; i < len(segs); i++ {
		switch segs[i] {
		case "projects":
			if i+1 < len(segs) {
				out.Project = segs[i+1]
				i++
			}
		case "locations":
			if i+1 < len(segs) {
				out.Location = segs[i+1]
				i++
			}
		case "triggers":
			if i+1 < len(segs) {
				out.Trigger = segs[i+1]
				i++
			}
		case "channels":
			if i+1 < len(segs) {
				out.Channel = segs[i+1]
				i++
			}
		case "providers":
			if i+1 < len(segs) {
				out.Provider = segs[i+1]
				i++
			}
		case "operations":
			if i+1 < len(segs) {
				out.Operation = segs[i+1]
				i++
			}
		}
	}
	return out
}

// lastSegment returns the final path segment of a resource name (the resource
// id), e.g. "projects/p/topics/t" -> "t".
func lastSegment(name string) string {
	seg := strings.Split(strings.Trim(name, "/"), "/")
	if len(seg) == 0 {
		return ""
	}
	return seg[len(seg)-1]
}

// NameID returns the resource id (final segment) of a resource name. It is the
// transport-neutral accessor the REST adapter uses to resolve the id from a
// NormalizedRequest "name" param.
func NameID(name string) string { return lastSegment(name) }

// locationOf returns the location segment of a projects/{p}/locations/{l}/...
// name, or "" when the name has no locations segment.
func locationOf(name string) string {
	seg := strings.Split(strings.Trim(name, "/"), "/")
	for i, s := range seg {
		if s == "locations" && i+1 < len(seg) {
			return seg[i+1]
		}
	}
	return ""
}
