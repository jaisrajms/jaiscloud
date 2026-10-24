// Package managedkafkaui serves the Managed Kafka (Apache Kafka for BigQuery)
// console UI API. Handlers call the transport-neutral Managed Kafka core
// directly (in-process) rather than over the wire.
//
// The console is location-optional: the cluster and topic lists aggregate every
// resource in the project across all locations (no location picker) and show
// the location as a read-only field. Detail/actions link back with the location
// carried from the list row, so a resource id never has to be disambiguated by
// hand.
//
// The console covers the full management surface: clusters list/detail/create/
// update, topics list/detail/create/update/delete, and ACL and consumer-group
// writes on the cluster detail (the core owns the wire behavior; the console
// reuses it in-process).
package managedkafkaui

import "encoding/json"

// Cluster is the console rendering of a Managed Kafka cluster, flattened across
// locations for the list page. Config carries the caller's wire body verbatim
// and is only populated on the detail read, not the list.
type Cluster struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Location         string            `json:"location"`
	State            string            `json:"state,omitempty"`
	BootstrapAddress string            `json:"bootstrapAddress,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	Config           json.RawMessage   `json:"config,omitempty"`
	CreateTime       string            `json:"createTime,omitempty"`
	UpdateTime       string            `json:"updateTime,omitempty"`
}

// ListClustersResponse is the response for GET /clusters.
type ListClustersResponse struct {
	Clusters []Cluster `json:"clusters"`
	Total    int       `json:"total"`
}

// Topic is the console rendering of a Managed Kafka topic. Cluster and Location
// identify the parent cluster, so a topic row is addressable without context.
// Configs holds the Kafka property overrides (the wire `configs` map) and is
// only populated on the detail read.
type Topic struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	Location          string            `json:"location"`
	Cluster           string            `json:"cluster"`
	PartitionCount    int               `json:"partitionCount"`
	ReplicationFactor int               `json:"replicationFactor"`
	Configs           map[string]string `json:"configs,omitempty"`
	CreateTime        string            `json:"createTime,omitempty"`
	UpdateTime        string            `json:"updateTime,omitempty"`
}

// ListTopicsResponse is the response for GET /topics and
// GET /clusters/{location}/{cluster}/topics.
type ListTopicsResponse struct {
	Topics []Topic `json:"topics"`
	Total  int     `json:"total"`
}

// AclEntry is one access grant within an ACL.
type AclEntry struct {
	Principal      string `json:"principal,omitempty"`
	PermissionType string `json:"permissionType,omitempty"`
	Operation      string `json:"operation,omitempty"`
	Host           string `json:"host,omitempty"`
}

// Acl is the console rendering of a cluster ACL.
type Acl struct {
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Location     string     `json:"location"`
	Cluster      string     `json:"cluster"`
	ResourceType string     `json:"resourceType,omitempty"`
	ResourceName string     `json:"resourceName,omitempty"`
	PatternType  string     `json:"patternType,omitempty"`
	Etag         string     `json:"etag,omitempty"`
	Entries      []AclEntry `json:"aclEntries"`
}

// ListAclsResponse is the response for GET /clusters/{location}/{cluster}/acls.
type ListAclsResponse struct {
	Acls  []Acl `json:"acls"`
	Total int   `json:"total"`
}

// ConsumerGroupOffset is one committed partition offset, flattened from the
// core's topic-keyed map for the console table.
type ConsumerGroupOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Metadata  string `json:"metadata,omitempty"`
}

// ConsumerGroup is the console rendering of a Kafka consumer group. Offsets is
// flattened from the core's topic-keyed map and sorted by topic then partition.
// With the default mock topology (no live broker) the list is empty.
type ConsumerGroup struct {
	ID       string                `json:"id"`
	Name     string                `json:"name"`
	Location string                `json:"location"`
	Cluster  string                `json:"cluster"`
	Offsets  []ConsumerGroupOffset `json:"offsets"`
}

// ListConsumerGroupsResponse is the response for
// GET /clusters/{location}/{cluster}/consumer-groups.
type ListConsumerGroupsResponse struct {
	Groups []ConsumerGroup `json:"groups"`
	Total  int             `json:"total"`
}

// ─── write inputs ────────────────────────────────────────────────────────────

// ClusterWriteInput is the console's cluster create/update body. ID and Location
// are required on create (and ignored on update, where the target comes from the
// path). Labels is the extracted labels map; Config is the verbatim cluster body
// (capacityConfig, gcpConfig, ...) the core stores and renders back.
type ClusterWriteInput struct {
	ID       string            `json:"id"`
	Location string            `json:"location"`
	Labels   map[string]string `json:"labels,omitempty"`
	Config   json.RawMessage   `json:"config,omitempty"`
}

// TopicWriteInput is the console's topic create/update body. PartitionCount and
// ReplicationFactor are required on create and immutable on update (the core
// rejects a change); Configs are the Kafka property overrides.
type TopicWriteInput struct {
	ID                string            `json:"id"`
	PartitionCount    int               `json:"partitionCount,omitempty"`
	ReplicationFactor int               `json:"replicationFactor,omitempty"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// AclWriteInput is the console's ACL create/update body. ID is the ACL id, which
// encodes the resource pattern (e.g. "topic/orders", "allTopics", "cluster").
// Etag is required on update (optimistic concurrency); Entries replaces the
// entry list.
type AclWriteInput struct {
	ID      string     `json:"id"`
	Etag    string     `json:"etag,omitempty"`
	Entries []AclEntry `json:"aclEntries"`
}

// ConsumerGroupWriteInput is the console's consumer-group update body: the
// committed offsets to set, keyed by topic and partition.
type ConsumerGroupWriteInput struct {
	Offsets []ConsumerGroupOffset `json:"offsets"`
}
