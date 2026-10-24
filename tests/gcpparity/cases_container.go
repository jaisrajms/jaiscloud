//go:build gcp_parity

package gcpparity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	container "cloud.google.com/go/container/apiv1"
	containerpb "cloud.google.com/go/container/apiv1/containerpb"
)

// containerLocation is the region every GKE scenario resource lives in. GKE
// accepts a region (a regional cluster) or a zone; the emulator is location-
// agnostic, so one fixed region keeps the two transports aligned.
const containerLocation = "us-central1"

// containerClusterID / containerNodePoolID are the short resource prefixes;
// Env.Resource appends the run suffix (and, for a mutation-parity step, the step
// tag + twin token). They stay short because GKE validates a cluster/node-pool
// id as an RFC 1035 label capped at 40 characters and the twin form adds
// "-s<idx>-grpc-".
const (
	containerClusterID  = "gke-c"
	containerNodePoolID = "gke-np"
)

// containerOperationID matches the server-generated operation id
// (operation-<24 hex>) the core assigns. It is folded for mutation parity: the
// two transports each create their own operation, so the ids differ even though
// the operation is otherwise identical. The same token also appears inside
// Operation.selfLink, which the normalizer already folds wholesale.
var containerOperationID = regexp.MustCompile(`operation-[0-9a-f]{24}`)

// containerFoldOperation folds the server-generated operation id out of a
// mutation response so the two transports' create/update Operations compare.
func containerFoldOperation(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return raw, nil
	}
	return containerOperationID.ReplaceAll(raw, []byte("operation-<id>")), nil
}

// containerScenario compares the GKE v1 (ClusterManager) surface over REST and
// gRPC. GKE mutations return the container-specific google.container.v1.Operation
// (not google.longrunning), which the core materializes already DONE, so no LRO
// polling is needed — the create/delete/set mutation responses are diffed
// directly. It covers the cluster lifecycle (create → get → list → setLabels →
// get → delete), the node-pool lifecycle (create → get → list → setSize → get →
// delete), the operation read surface (get/list — the container-specific
// Operation type), and mutation-parity Create/Update steps for the cluster and
// the node pool (AUD3-12).
func containerScenario() Scenario {
	project := func(e *Env) string { return e.Cfg.Project }
	parent := func(e *Env) string {
		return "projects/" + project(e) + "/locations/" + containerLocation
	}
	clusterName := func(e *Env) string { return e.Resource(containerClusterID) }
	clusterFull := func(e *Env) string { return parent(e) + "/clusters/" + clusterName(e) }
	poolName := func(e *Env) string { return e.Resource(containerNodePoolID) }
	poolFull := func(e *Env) string { return clusterFull(e) + "/nodePools/" + poolName(e) }

	// restLoc is the run's REST parent; GKE is reachable both under a
	// "/container/v1" path prefix and by host, and the codec locates the
	// projects/{p} segment regardless of what precedes it.
	restLoc := func(e *Env) string { return "/container/v1/" + parent(e) }
	restCluster := func(e *Env) string { return restLoc(e) + "/clusters/" + clusterName(e) }
	restPool := func(e *Env) string { return restCluster(e) + "/nodePools/" + poolName(e) }

	newClient := func(ctx context.Context, e *Env) (*container.ClusterManagerClient, error) {
		return container.NewClusterManagerClient(ctx, e.GRPCClientOptions()...)
	}

	// createClusterGRPC creates the cluster and returns the CREATE_CLUSTER
	// operation (already DONE).
	createClusterGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.CreateCluster(ctx, &containerpb.CreateClusterRequest{
			Parent: parent(e),
			Cluster: &containerpb.Cluster{
				Name:             clusterName(e),
				InitialNodeCount: 1,
				ResourceLabels:   map[string]string{"parity": "true"},
			},
		})
	}
	createClusterREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "POST", restLoc(e)+"/clusters",
			`{"cluster":{"name":"`+clusterName(e)+`","initialNodeCount":1,"resourceLabels":{"parity":"true"}}}`)
	}
	deleteClusterREST := func(ctx context.Context, e *Env) error {
		return e.RestDelete(ctx, restCluster(e))
	}
	createNodePoolGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.CreateNodePool(ctx, &containerpb.CreateNodePoolRequest{
			Parent:   clusterFull(e),
			NodePool: &containerpb.NodePool{Name: poolName(e), InitialNodeCount: 1},
		})
	}
	createNodePoolREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "POST", restCluster(e)+"/nodePools",
			`{"nodePool":{"name":"`+poolName(e)+`","initialNodeCount":1}}`)
	}
	setLabelsGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.SetLabels(ctx, &containerpb.SetLabelsRequest{
			Name:           clusterFull(e),
			ResourceLabels: map[string]string{"parity": "updated"},
		})
	}
	setLabelsREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "POST", restCluster(e)+":setResourceLabels", `{"resourceLabels":{"parity":"updated"}}`)
	}
	setPoolSizeGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.SetNodePoolSize(ctx, &containerpb.SetNodePoolSizeRequest{Name: poolFull(e), NodeCount: 3})
	}
	setPoolSizeREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "POST", restPool(e)+":setSize", `{"nodeCount":3}`)
	}
	getClusterGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.GetCluster(ctx, &containerpb.GetClusterRequest{Name: clusterFull(e)})
	}
	getClusterREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "GET", restCluster(e), "")
	}
	getNodePoolGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.GetNodePool(ctx, &containerpb.GetNodePoolRequest{Name: poolFull(e)})
	}
	getNodePoolREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "GET", restPool(e), "")
	}

	// createOpID is the operation the lifecycle CreateCluster returned; the
	// GetOperation step reads it back over both transports.
	var createOpID string

	return Scenario{Service: "container", Steps: []Step{
		// --- Cluster lifecycle ---
		{
			Op: "CreateCluster",
			Mutate: func(ctx context.Context, e *Env) error {
				op, err := createClusterGRPC(ctx, e)
				if err != nil {
					return err
				}
				cop, ok := op.(*containerpb.Operation)
				if !ok {
					return fmt.Errorf("CreateCluster returned %T, want *containerpb.Operation", op)
				}
				createOpID = cop.GetName()
				return nil
			},
		},
		{
			Op:   "GetCluster",
			GRPC: getClusterGRPC,
			REST: getClusterREST,
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
				return c.ListClusters(ctx, &containerpb.ListClustersRequest{Parent: parent(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", restLoc(e)+"/clusters", "")
			},
		},
		{
			// The container-specific google.container.v1.Operation type, read
			// back at the operation the create returned.
			Op: "GetOperation",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetOperation(ctx, &containerpb.GetOperationRequest{Name: parent(e) + "/operations/" + createOpID})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", restLoc(e)+"/operations/"+createOpID, "")
			},
		},
		{
			// Operation ids are server-generated (operation-<hex>) and do not
			// embed the run suffix, so this list cannot be scoped the way a
			// cluster/node-pool list is — but both transports read the same
			// store, so the list is symmetric and a leftover op from another
			// run cannot diverge it.
			Op: "ListOperations",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.ListOperations(ctx, &containerpb.ListOperationsRequest{Parent: parent(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", restLoc(e)+"/operations", "")
			},
		},
		{
			Op: "SetLabels (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := setLabelsREST(ctx, e)
				return err
			},
		},
		{
			Op:   "GetClusterAfterLabels",
			GRPC: getClusterGRPC,
			REST: getClusterREST,
		},

		// --- Node-pool lifecycle ---
		{
			Op: "CreateNodePool",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := createNodePoolGRPC(ctx, e)
				return err
			},
		},
		{
			Op:   "GetNodePool",
			GRPC: getNodePoolGRPC,
			REST: getNodePoolREST,
		},
		{
			Op:    "ListNodePools",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.ListNodePools(ctx, &containerpb.ListNodePoolsRequest{Parent: clusterFull(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", restCluster(e)+"/nodePools", "")
			},
		},
		{
			Op: "SetNodePoolSize (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := setPoolSizeREST(ctx, e)
				return err
			},
		},
		{
			Op:   "GetNodePoolAfterResize",
			GRPC: getNodePoolGRPC,
			REST: getNodePoolREST,
		},
		{
			Op: "DeleteNodePool",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				_, err = c.DeleteNodePool(ctx, &containerpb.DeleteNodePoolRequest{Name: poolFull(e)})
				return err
			},
		},

		// --- Mutation parity (AUD3-12): each transport mutates its own twin ---
		{
			Op: "CreateCluster (parity)",
			Mutation: &MutationParity{
				GRPC:    createClusterGRPC,
				REST:    createClusterREST,
				Cleanup: deleteClusterREST,
				Project: containerFoldOperation,
			},
		},
		{
			Op: "SetLabels (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					if _, err := createClusterGRPC(ctx, e); err != nil {
						return nil, err
					}
					return setLabelsGRPC(ctx, e)
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createClusterREST(ctx, e); err != nil {
						return nil, err
					}
					return setLabelsREST(ctx, e)
				},
				Cleanup: deleteClusterREST,
				Project: containerFoldOperation,
			},
		},
		{
			Op: "CreateNodePool (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					if _, err := createClusterGRPC(ctx, e); err != nil {
						return nil, err
					}
					return createNodePoolGRPC(ctx, e)
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createClusterREST(ctx, e); err != nil {
						return nil, err
					}
					return createNodePoolREST(ctx, e)
				},
				Cleanup: deleteClusterREST,
				Project: containerFoldOperation,
			},
		},
		{
			Op: "SetNodePoolSize (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					if _, err := createClusterGRPC(ctx, e); err != nil {
						return nil, err
					}
					if _, err := createNodePoolGRPC(ctx, e); err != nil {
						return nil, err
					}
					return setPoolSizeGRPC(ctx, e)
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createClusterREST(ctx, e); err != nil {
						return nil, err
					}
					if _, err := createNodePoolREST(ctx, e); err != nil {
						return nil, err
					}
					return setPoolSizeREST(ctx, e)
				},
				Cleanup: deleteClusterREST,
				Project: containerFoldOperation,
			},
		},

		// --- Cleanup the fixture cluster ---
		{
			Op: "DeleteCluster",
			Mutate: func(ctx context.Context, e *Env) error {
				return deleteClusterREST(ctx, e)
			},
		},
	}}
}
