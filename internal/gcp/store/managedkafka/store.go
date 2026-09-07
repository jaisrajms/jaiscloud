// Package managedkafka provides the Apache Kafka for BigQuery (Managed Kafka)
// store. Resources are project+location scoped with canonical names
// projects/{project}/locations/{location}/clusters/{name} (and
// .../clusters/{name}/topics/{topic}). A cluster is a logical record only — the
// emulator never stands up a real broker. Consumer groups are not tracked (their
// list endpoint always returns an empty list).
package managedkafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNoSuchCluster = errors.New("NoSuchCluster")
	ErrNoSuchTopic   = errors.New("NoSuchTopic")
	ErrAlreadyExists = errors.New("AlreadyExists")
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

	Reset(ctx context.Context)
}
