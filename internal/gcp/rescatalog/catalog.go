// Package rescatalog holds the canonical catalog of well-known monitored
// resource descriptors (google.api.MonitoredResourceDescriptor). It is shared
// by the Cloud Monitoring and Cloud Logging gRPC services so the two surfaces
// serve one set of types/labels.
//
// The label sets mirror the descriptors published by Cloud Monitoring
// (cloud.google.com/monitoring/api/resources). The catalog is deliberately
// small and stable: it carries the types the emulator's services can plausibly
// emit logs/metrics for.
package rescatalog

import (
	"sort"

	labelpb "google.golang.org/genproto/googleapis/api/label"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
)

// Descriptor is one canonical monitored-resource definition: the type, its
// human-readable display name/description, and the labels that identify an
// instance of the type.
type Descriptor struct {
	typ         string
	displayName string
	description string
	labels      []*labelpb.LabelDescriptor
}

// Type returns the monitored resource type (e.g. "gce_instance").
func (d Descriptor) Type() string { return d.typ }

// DisplayName returns the descriptor's human-readable display name.
func (d Descriptor) DisplayName() string { return d.displayName }

// Description returns the descriptor's human-readable description.
func (d Descriptor) Description() string { return d.description }

// Label is a transport-neutral view of one label descriptor. ValueType is the
// proto enum name ("STRING", "BOOL", or "INT64"), so a transport can render the
// label without importing the protobuf label package.
type Label struct {
	Key         string
	ValueType   string
	Description string
}

// Labels returns a copy of the descriptor's labels as neutral values.
func (d Descriptor) Labels() []Label {
	out := make([]Label, 0, len(d.labels))
	for _, l := range d.labels {
		out = append(out, Label{
			Key:         l.GetKey(),
			ValueType:   labelValueTypeName(l.GetValueType()),
			Description: l.GetDescription(),
		})
	}
	return out
}

// labelValueTypeName maps a label descriptor value type to its proto enum name.
func labelValueTypeName(v labelpb.LabelDescriptor_ValueType) string {
	switch v {
	case labelpb.LabelDescriptor_BOOL:
		return "BOOL"
	case labelpb.LabelDescriptor_INT64:
		return "INT64"
	default:
		return "STRING"
	}
}

func strLabel(key, description string) *labelpb.LabelDescriptor {
	return &labelpb.LabelDescriptor{
		Key:         key,
		ValueType:   labelpb.LabelDescriptor_STRING,
		Description: description,
	}
}

func int64Label(key, description string) *labelpb.LabelDescriptor {
	return &labelpb.LabelDescriptor{
		Key:         key,
		ValueType:   labelpb.LabelDescriptor_INT64,
		Description: description,
	}
}

// catalog is the canonical set of well-known monitored resource types the
// emulator serves, keyed by type.
var catalog = []Descriptor{
	{
		typ:         "api",
		displayName: "API",
		description: "An API request, such as a call to a Google Cloud service.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource, such as \"my-project\"."),
			strLabel("service", "The service name as used for authentication, e.g. \"compute.googleapis.com\"."),
			strLabel("method", "The API method name, e.g. \"compute.instances.get\"."),
			strLabel("version", "The API version, e.g. \"v1\"."),
			strLabel("location", "The location of the API request, e.g. \"us-east1\"."),
		},
	},
	{
		typ:         "cloudsql_database",
		displayName: "Cloud SQL Database",
		description: "A relational database hosted by Google Cloud SQL.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("database_id", "The name of the Cloud SQL database."),
			strLabel("region", "The region in which the database is running, e.g. \"us-central1\"."),
		},
	},
	{
		typ:         "dataproc_cluster",
		displayName: "Cloud Dataproc Cluster",
		description: "A cluster running in Google Cloud Dataproc.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("region", "The region in which the cluster is running, e.g. \"us-central1\"."),
			strLabel("cluster_name", "The name of the cluster."),
		},
	},
	{
		typ:         "gce_instance",
		displayName: "GCE VM Instance",
		description: "A virtual machine instance running in Google Compute Engine.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			int64Label("instance_id", "The numeric VM instance identifier assigned by Compute Engine."),
			strLabel("zone", "The Compute Engine zone in which the VM is running, e.g. \"us-central1-a\"."),
		},
	},
	{
		typ:         "gcs_bucket",
		displayName: "GCS Bucket",
		description: "A bucket in Google Cloud Storage.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("bucket_name", "The name of the bucket."),
			strLabel("location", "The location of the bucket, e.g. \"us-central1\" or \"US\"."),
		},
	},
	{
		typ:         "global",
		displayName: "Global",
		description: "A resource type that is not associated with any specific resource.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource, such as \"my-project\"."),
		},
	},
	{
		typ:         "k8s_container",
		displayName: "Kubernetes Container",
		description: "A container running in a Kubernetes cluster.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("location", "The location of the cluster, e.g. \"us-central1-a\"."),
			strLabel("cluster_name", "The name of the cluster."),
			strLabel("namespace_name", "The name of the namespace."),
			strLabel("pod_name", "The name of the pod."),
			strLabel("container_name", "The name of the container."),
		},
	},
	{
		typ:         "k8s_node",
		displayName: "Kubernetes Node",
		description: "A node running in a Kubernetes cluster.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("location", "The location of the cluster, e.g. \"us-central1-a\"."),
			strLabel("cluster_name", "The name of the cluster."),
			strLabel("node_name", "The name of the node."),
		},
	},
	{
		typ:         "k8s_pod",
		displayName: "Kubernetes Pod",
		description: "A pod running in a Kubernetes cluster.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("location", "The location of the cluster, e.g. \"us-central1-a\"."),
			strLabel("cluster_name", "The name of the cluster."),
			strLabel("namespace_name", "The name of the namespace."),
			strLabel("pod_name", "The name of the pod."),
		},
	},
	{
		typ:         "pubsub_subscription",
		displayName: "Pub/Sub Subscription",
		description: "A subscription in Google Cloud Pub/Sub.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("subscription_id", "The name of the subscription."),
		},
	},
	{
		typ:         "pubsub_topic",
		displayName: "Pub/Sub Topic",
		description: "A topic in Google Cloud Pub/Sub.",
		labels: []*labelpb.LabelDescriptor{
			strLabel("project_id", "The identifier of the GCP project associated with this resource."),
			strLabel("topic_id", "The name of the topic."),
		},
	},
}

// Catalog returns a type-sorted copy of the canonical catalog.
func Catalog() []Descriptor {
	out := make([]Descriptor, len(catalog))
	copy(out, catalog)
	sort.Slice(out, func(i, j int) bool { return out[i].typ < out[j].typ })
	return out
}

// Lookup returns the canonical descriptor for typ, or ok=false when the type is
// not in the catalog.
func Lookup(typ string) (Descriptor, bool) {
	for _, d := range catalog {
		if d.typ == typ {
			return d, true
		}
	}
	return Descriptor{}, false
}

// Proto builds the wire google.api.MonitoredResourceDescriptor for d. When name
// is non-empty it is set as the descriptor's resource name — Cloud Monitoring
// names its descriptors (projects/{p}/monitoredResourceDescriptors/{type});
// Cloud Logging does not. Label descriptors are copied so callers cannot
// mutate the shared catalog.
func Proto(d Descriptor, name string) *monitoredrespb.MonitoredResourceDescriptor {
	labels := make([]*labelpb.LabelDescriptor, 0, len(d.labels))
	for _, l := range d.labels {
		labels = append(labels, &labelpb.LabelDescriptor{
			Key:         l.GetKey(),
			ValueType:   l.GetValueType(),
			Description: l.GetDescription(),
		})
	}
	return &monitoredrespb.MonitoredResourceDescriptor{
		Name:        name,
		Type:        d.typ,
		DisplayName: d.displayName,
		Description: d.description,
		Labels:      labels,
	}
}
