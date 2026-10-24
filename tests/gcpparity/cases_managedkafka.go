//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"

	managedkafka "cloud.google.com/go/managedkafka/apiv1"
	"cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"google.golang.org/api/iterator"
)

const managedKafkaLocation = "us-central1"

// managedKafkaScenario compares the Managed Kafka cluster + topic surface over
// REST and gRPC: create cluster → get/list → create topic → get/list → delete.
func managedKafkaScenario() Scenario {
	clusterID := "parity-mk"
	topicID := "parity-topic"
	parent := func(e *Env) string {
		return fmt.Sprintf("projects/%s/locations/%s", e.Cfg.Project, managedKafkaLocation)
	}
	clusterName := func(e *Env) string { return parent(e) + "/clusters/" + e.Resource(clusterID) }
	topicName := func(e *Env) string { return clusterName(e) + "/topics/" + e.Resource(topicID) }

	newClient := func(ctx context.Context, e *Env) (*managedkafka.Client, error) {
		return managedkafka.NewClient(ctx, e.GRPCClientOptions()...)
	}
	createClusterGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		op, err := c.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
			Parent:    parent(e),
			ClusterId: e.Resource(clusterID),
			Cluster: &managedkafkapb.Cluster{
				Labels:         map[string]string{"parity": "true"},
				CapacityConfig: &managedkafkapb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3221225472},
			},
		})
		if err != nil {
			return nil, err
		}
		return op.Wait(ctx)
	}
	createClusterREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.RestOperationResource(ctx, "POST", "/v1/"+parent(e)+"/clusters?clusterId="+e.Resource(clusterID),
			`{"labels":{"parity":"true"},"capacityConfig":{"vcpuCount":3,"memoryBytes":3221225472}}`)
	}
	createTopicGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.CreateTopic(ctx, &managedkafkapb.CreateTopicRequest{
			Parent:  clusterName(e),
			TopicId: e.Resource(topicID),
			Topic:   &managedkafkapb.Topic{PartitionCount: 3, ReplicationFactor: 3},
		})
	}
	createTopicREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "POST", "/v1/"+clusterName(e)+"/topics?topicId="+e.Resource(topicID),
			`{"partitionCount":3,"replicationFactor":3}`)
	}
	delTopic := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v1/"+topicName(e)) }
	delCluster := func(ctx context.Context, e *Env) error { return e.RestDelete(ctx, "/v1/"+clusterName(e)) }
	delTopicAndCluster := func(ctx context.Context, e *Env) error {
		if err := delTopic(ctx, e); err != nil {
			return err
		}
		return delCluster(ctx, e)
	}

	return Scenario{Service: "managedkafka", Steps: []Step{
		{
			Op: "CreateCluster",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
					Parent:    parent(e),
					ClusterId: e.Resource(clusterID),
					Cluster: &managedkafkapb.Cluster{
						Labels:         map[string]string{"parity": "true"},
						CapacityConfig: &managedkafkapb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3221225472},
					},
				})
				if err != nil {
					return err
				}
				_, err = op.Wait(ctx)
				return err
			},
		},
		{
			Op: "GetCluster",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetCluster(ctx, &managedkafkapb.GetClusterRequest{Name: clusterName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+clusterName(e), "")
			},
		},
		{
			Op:    "ListClusters",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListClusters(ctx, &managedkafkapb.ListClustersRequest{Parent: parent(e)})
				out := &managedkafkapb.ListClustersResponse{}
				for {
					cl, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Clusters = append(out.Clusters, cl)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+parent(e)+"/clusters", "")
			},
		},
		{
			Op: "CreateTopic",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.CreateTopic(ctx, &managedkafkapb.CreateTopicRequest{
					Parent:  clusterName(e),
					TopicId: e.Resource(topicID),
					Topic:   &managedkafkapb.Topic{PartitionCount: 3, ReplicationFactor: 3},
				})
				return err
			},
		},
		{
			Op: "GetTopic",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetTopic(ctx, &managedkafkapb.GetTopicRequest{Name: topicName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+topicName(e), "")
			},
		},
		{
			Op:    "ListTopics",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListTopics(ctx, &managedkafkapb.ListTopicsRequest{Parent: clusterName(e)})
				out := &managedkafkapb.ListTopicsResponse{}
				for {
					t, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Topics = append(out.Topics, t)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+clusterName(e)+"/topics", "")
			},
		},
		{
			Op: "DeleteTopic",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "DELETE", "/v1/"+topicName(e), "")
				return err
			},
		},
		{
			Op: "DeleteCluster",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.DeleteCluster(ctx, &managedkafkapb.DeleteClusterRequest{Name: clusterName(e)})
				if err != nil {
					return err
				}
				return op.Wait(ctx)
			},
		},
		{
			Op: "CreateCluster (parity)",
			Mutation: &MutationParity{
				GRPC:    createClusterGRPC,
				REST:    createClusterREST,
				Cleanup: delCluster,
			},
		},
		{
			Op: "CreateTopic (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					if _, err := createClusterGRPC(ctx, e); err != nil {
						return nil, err
					}
					return createTopicGRPC(ctx, e)
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createClusterREST(ctx, e); err != nil {
						return nil, err
					}
					return createTopicREST(ctx, e)
				},
				Cleanup: delTopicAndCluster,
			},
		},
	}}
}
