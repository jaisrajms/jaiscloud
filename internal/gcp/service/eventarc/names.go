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

// SplitOperationName parses an operation resource name
// (projects/{p}/locations/{l}/operations/{id}) into its project, location and
// id. ok is false when the name is not an operations resource name.
func SplitOperationName(name string) (project, location, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[1] == "" ||
		parts[2] != "locations" || parts[3] == "" || parts[4] != "operations" || parts[5] == "" {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

// MessageBusName is the Eventarc message-bus resource name
// (projects/{p}/locations/{l}/messageBuses/{id}).
func MessageBusName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-message-bus", location+"/"+id)
}

// EnrollmentName is the Eventarc enrollment resource name
// (projects/{p}/locations/{l}/enrollments/{id}).
func EnrollmentName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-enrollment", location+"/"+id)
}

// PipelineName is the Eventarc pipeline resource name
// (projects/{p}/locations/{l}/pipelines/{id}).
func PipelineName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-pipeline", location+"/"+id)
}

// GoogleApiSourceName is the Eventarc Google API source resource name
// (projects/{p}/locations/{l}/googleApiSources/{id}).
func GoogleApiSourceName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-google-api-source", location+"/"+id)
}

// ChannelConnectionName is the Eventarc channel-connection resource name
// (projects/{p}/locations/{l}/channelConnections/{id}).
func ChannelConnectionName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-channel-connection", location+"/"+id)
}

// GoogleChannelConfigName is the Eventarc Google channel config resource name
// (projects/{p}/locations/{l}/googleChannelConfig) — a per-location singleton.
func GoogleChannelConfigName(project, location string) string {
	return resource.ResourceID(project)("eventarc-google-channel-config", location)
}

// ResourceName is the parsed form of an Eventarc resource name.
type ResourceName struct {
	Project             string
	Location            string
	Trigger             string
	Channel             string
	Provider            string
	Operation           string
	MessageBus          string
	Enrollment          string
	Pipeline            string
	GoogleApiSource     string
	ChannelConnection   string
	GoogleChannelConfig bool
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
		case "messageBuses":
			if i+1 < len(segs) {
				out.MessageBus = segs[i+1]
				i++
			}
		case "enrollments":
			if i+1 < len(segs) {
				out.Enrollment = segs[i+1]
				i++
			}
		case "pipelines":
			if i+1 < len(segs) {
				out.Pipeline = segs[i+1]
				i++
			}
		case "googleApiSources":
			if i+1 < len(segs) {
				out.GoogleApiSource = segs[i+1]
				i++
			}
		case "channelConnections":
			if i+1 < len(segs) {
				out.ChannelConnection = segs[i+1]
				i++
			}
		case "googleChannelConfig":
			out.GoogleChannelConfig = true
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

// AdvancedKindForName resolves an advanced-surface resource name to its kind and
// id. It is the accessor the gRPC IAM router uses to dispatch IAM for the new
// families (the singleton GoogleChannelConfig carries no IAM, so it is not
// returned).
func AdvancedKindForName(name string) (Kind, string, bool) {
	rn := ParseName(name)
	switch {
	case rn.MessageBus != "":
		return MessageBusKind, rn.MessageBus, true
	case rn.Enrollment != "":
		return EnrollmentKind, rn.Enrollment, true
	case rn.Pipeline != "":
		return PipelineKind, rn.Pipeline, true
	case rn.GoogleApiSource != "":
		return GoogleApiSourceKind, rn.GoogleApiSource, true
	case rn.ChannelConnection != "":
		return ChannelConnectionKind, rn.ChannelConnection, true
	}
	return Kind{}, "", false
}

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
