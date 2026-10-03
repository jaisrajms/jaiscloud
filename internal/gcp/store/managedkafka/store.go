// Package managedkafka provides the Apache Kafka for BigQuery (Managed Kafka)
// store. Resources are project+location scoped with canonical names
// projects/{project}/locations/{location}/clusters/{name} (and
// .../clusters/{name}/topics/{topic}). A cluster record carries the caller's
// metadata; a real Kafka-wire broker behind its bootstrapAddress is optional
// (broker modes k8s/native) and its live endpoint is runtime state, not stored
// here. Consumer groups are not stored either: they live on the cluster's Kafka
// broker (its group coordinator is the source of truth) and the core reads them
// through the injected broker, so with no live broker the list is empty.
// Cluster mutations persist a done
// google.longrunning.Operation under .../locations/{location}/operations/{id}
// so it can be read back via GetOperation/ListOperations.
package managedkafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNoSuchCluster   = errors.New("NoSuchCluster")
	ErrNoSuchTopic     = errors.New("NoSuchTopic")
	ErrNoSuchOperation = errors.New("NoSuchOperation")
	ErrNoSuchAcl       = errors.New("NoSuchAcl")
	ErrAlreadyExists   = errors.New("AlreadyExists")
)

// Cluster is a logical Managed Kafka cluster. Config holds the wire request
// body (capacityConfig, gcpConfig, ...) verbatim as JSON so read-back echoes
// what the caller sent; Labels are the extracted labels map.
type Cluster struct {
	ProjectID  string            `json:"projectId"`
	Location   string            `json:"location"`
	Name       string            `json:"clusterName"`
	Config     json.RawMessage   `json:"config,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	CreateTime time.Time         `json:"createTime"`
	UpdateTime time.Time         `json:"updateTime"`

	// BootstrapAddress is the live endpoint of the cluster's real broker when
	// one is running (k8s/native broker modes). It is runtime state owned by
	// the broker manager, not persisted metadata: json:"-" keeps it out of
	// snapshots, and the core sets it on the copies it returns for rendering.
	// When empty, rendering falls back to the synthesized cloud.goog name
	// (BootstrapAddress in the service package) — the mock topology.
	BootstrapAddress string `json:"-"`
}

// Topic is a logical Managed Kafka topic under a cluster. PartitionCount and
// ReplicationFactor are the explicit ints the REST surface round-trips; Config
// holds any additional wire fields verbatim.
type Topic struct {
	ProjectID         string          `json:"projectId"`
	Location          string          `json:"location"`
	ClusterName       string          `json:"clusterName"`
	Name              string          `json:"topicName"`
	PartitionCount    int             `json:"partitionCount"`
	ReplicationFactor int             `json:"replicationFactor"`
	Config            json.RawMessage `json:"config,omitempty"`
	CreateTime        time.Time       `json:"createTime"`
	UpdateTime        time.Time       `json:"updateTime"`
}

// Operation is a done google.longrunning.Operation returned by a cluster
// mutation. Metadata and Response are the already-rendered JSON wire objects
// (Metadata carries its @type) stored verbatim, mirroring the Dataproc and
// Dataproc Metastore operation shapes.
type Operation struct {
	ID         string    `json:"id"`
	ProjectID  string    `json:"projectId"`
	Location   string    `json:"location"`
	Done       bool      `json:"done"`
	Metadata   string    `json:"metadata,omitempty"`
	Response   string    `json:"response,omitempty"`
	Verb       string    `json:"verb"`
	Target     string    `json:"target"`
	CreateTime time.Time `json:"createTime"`
	EndTime    time.Time `json:"endTime"`
}

// AclEntry is one access grant within an Acl. The fields are the caller-supplied
// Kafka ACL entry values; PermissionType/Operation are stored verbatim.
type AclEntry struct {
	Principal      string `json:"principal"`
	PermissionType string `json:"permissionType"`
	Operation      string `json:"operation"`
	Host           string `json:"host"`
}

// Acl is a managed Kafka ACL: a set of entries for one resource pattern. The
// acl ID (Name) encodes the pattern; ResourceType, ResourceName, and
// PatternType are the derived output-only fields, and Etag provides optimistic
// concurrency control across mutations.
type Acl struct {
	ProjectID    string     `json:"projectId"`
	Location     string     `json:"location"`
	ClusterName  string     `json:"clusterName"`
	Name         string     `json:"aclId"`
	AclEntries   []AclEntry `json:"aclEntries"`
	Etag         string     `json:"etag"`
	ResourceType string     `json:"resourceType"`
	ResourceName string     `json:"resourceName"`
	PatternType  string     `json:"patternType"`
}

// Store is the Managed Kafka store.
type Store interface {
	CreateCluster(ctx context.Context, projectID, location string, c Cluster) error
	GetCluster(ctx context.Context, projectID, location, name string) (Cluster, error)
	UpdateCluster(ctx context.Context, projectID, location string, c Cluster) error
	// UpdateClusterAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current cluster and returns the version to persist, or an
	// error to abort without writing. Unlike a separate GetCluster followed
	// by UpdateCluster, this is atomic with respect to concurrent updates on
	// the same cluster, so two concurrent PATCH requests merging different
	// fields can't lose one or the other.
	UpdateClusterAtomic(ctx context.Context, projectID, location, name string, mutate func(Cluster) (Cluster, error)) (Cluster, error)
	DeleteCluster(ctx context.Context, projectID, location, name string) error
	ListClusters(ctx context.Context, projectID, location string) ([]Cluster, error)

	CreateTopic(ctx context.Context, projectID, location, clusterName string, t Topic) error
	GetTopic(ctx context.Context, projectID, location, clusterName, topicName string) (Topic, error)
	UpdateTopic(ctx context.Context, projectID, location, clusterName string, t Topic) error
	// UpdateTopicAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current topic and returns the version to persist, or an
	// error to abort without writing. Unlike a separate GetTopic followed by
	// UpdateTopic, this is atomic with respect to concurrent updates on the
	// same topic, so two concurrent PATCH requests merging different fields
	// can't lose one or the other.
	UpdateTopicAtomic(ctx context.Context, projectID, location, clusterName, topicName string, mutate func(Topic) (Topic, error)) (Topic, error)
	DeleteTopic(ctx context.Context, projectID, location, clusterName, topicName string) error
	ListTopics(ctx context.Context, projectID, location, clusterName string) ([]Topic, error)

	// Operations persist the done google.longrunning.Operation returned by
	// cluster create/update/delete so a poll can read it back. They are
	// project+location scoped and keyed by their opaque id.
	CreateOperation(ctx context.Context, projectID, location string, op Operation) error
	GetOperation(ctx context.Context, projectID, location, id string) (Operation, error)
	ListOperations(ctx context.Context, projectID, location string) ([]Operation, error)

	// ACLs are cluster-scoped metadata keyed by the acl id (which encodes the
	// resource pattern). UpdateAclAtomic performs the etag-checked
	// get-mutate-set cycle so a concurrent UpdateAcl on the same acl cannot
	// lose a write.
	CreateAcl(ctx context.Context, projectID, location, clusterName string, a Acl) error
	GetAcl(ctx context.Context, projectID, location, clusterName, name string) (Acl, error)
	UpdateAcl(ctx context.Context, projectID, location, clusterName string, a Acl) error
	UpdateAclAtomic(ctx context.Context, projectID, location, clusterName, name string, mutate func(Acl) (Acl, error)) (Acl, error)
	DeleteAcl(ctx context.Context, projectID, location, clusterName, name string) error
	ListAcls(ctx context.Context, projectID, location, clusterName string) ([]Acl, error)

	Reset(ctx context.Context)
}
