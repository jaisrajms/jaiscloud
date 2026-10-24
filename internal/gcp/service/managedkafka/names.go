package managedkafka

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// ClusterName is the Managed Kafka cluster resource name
// (projects/{p}/locations/{l}/clusters/{c}).
func ClusterName(project, location, cluster string) string {
	return resource.ResourceID(project)("managedkafka-cluster", location+"/"+cluster)
}

// TopicName is the Managed Kafka topic resource name
// (projects/{p}/locations/{l}/clusters/{c}/topics/{t}).
func TopicName(project, location, cluster, topic string) string {
	return resource.ResourceID(project)("managedkafka-topic", location+"/"+cluster+"/"+topic)
}

// AclName is the Managed Kafka ACL resource name
// (projects/{p}/locations/{l}/clusters/{c}/acls/{acl_id}).
func AclName(project, location, cluster, aclID string) string {
	return resource.ResourceID(project)("managedkafka-acl", location+"/"+cluster+"/"+aclID)
}

// ConsumerGroupName is the Managed Kafka consumer-group resource name
// (projects/{p}/locations/{l}/clusters/{c}/consumerGroups/{g}).
func ConsumerGroupName(project, location, cluster, group string) string {
	return resource.ResourceID(project)("managedkafka-consumergroup", location+"/"+cluster+"/"+group)
}

// OperationName is the Managed Kafka long-running operation resource name
// (projects/{p}/locations/{l}/operations/{id}). It shares the
// google.longrunning.Operations name shape with Cloud Workflows.
func OperationName(project, location, id string) string {
	return resource.ResourceID(project)("managedkafka-operation", location+"/"+id)
}

// ParseParent splits a location parent name projects/{p}/locations/{l}. ok is
// false for any other shape (e.g. a resource name or a collection-less path).
func ParseParent(parent string) (project, location string, ok bool) {
	segs := strings.Split(strings.Trim(parent, "/"), "/")
	if len(segs) != 4 || segs[0] != "projects" || segs[2] != "locations" {
		return "", "", false
	}
	return segs[1], segs[3], true
}

// BootstrapAddress is the synthesized bootstrap address of the mock topology:
// when no real broker is running (the default), the emulator returns the
// documented legacy GCP format but nothing listens at it:
//
//	bootstrap.{cluster}.{location}.managedkafka.{project}.cloud.goog
//
// With a real broker (broker modes k8s/native) a cluster renders the broker's
// live endpoint instead; see ClusterJSON. Real GCP omits the port so a client
// selects its own listener (:9092 TLS, :9094 mTLS); the address is present while
// the cluster is ACTIVE.
func BootstrapAddress(project, location, cluster string) string {
	return "bootstrap." + cluster + "." + location + ".managedkafka." + project + ".cloud.goog"
}

// ResourceName is the parsed form of a Managed Kafka resource name. Multi-
// segment identifiers (an acl id such as "topic/my-topic", or a consumer-group
// id) are rejoined with "/" so callers get the raw identifier.
type ResourceName struct {
	Project       string
	Location      string
	Cluster       string
	Topic         string
	AclID         string
	ConsumerGroup string
}

// ProjectFromName returns the project id from a "projects/{p}/..." resource
// name, or "" when the name is not project-scoped.
func ProjectFromName(name string) string { return ParseName(name).Project }

// ParseName extracts the hierarchical components of a Managed Kafka resource
// name. Segments are matched by their collection keyword; an unknown segment is
// skipped. Anything after "acls"/"consumerGroups" is joined with "/" into the
// leaf identifier, matching the proto's multi-segment acl_id/consumer-group id.
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
		case "clusters":
			if i+1 < len(segs) {
				out.Cluster = segs[i+1]
				i++
			}
		case "topics":
			if i+1 < len(segs) {
				out.Topic = segs[i+1]
				i++
			}
		case "acls":
			out.AclID = strings.Join(segs[i+1:], "/")
			return out
		case "consumerGroups":
			out.ConsumerGroup = strings.Join(segs[i+1:], "/")
			return out
		}
	}
	return out
}
