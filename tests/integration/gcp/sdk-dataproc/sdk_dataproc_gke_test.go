package sdk_dataproc_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	container "google.golang.org/api/container/v1"
	"google.golang.org/api/dataproc/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// gkeEndpoint is the emulator's REST root with the "/container/" path prefix,
// so GKE's shared /v1/projects/{p}/locations/{l}/clusters path is routed to the
// GKE service rather than Managed Kafka (see the sdk-container suite).
func gkeEndpoint() string { return endpoint() + "container/" }

// gkeOpts are the client options for the GKE apiary client.
func gkeOpts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(gkeEndpoint()), option.WithoutAuthentication()}
}

// TestSDKDataprocOnGKE reconciles a Dataproc-on-GKE virtual cluster's node pools
// against the emulated GKE API and reads them back through the GKE client — the
// cross-API case users actually exercise. Every assertion is a call to one of
// the two documented APIs; nothing here inspects pods, nodes or manifests.
func TestSDKDataprocOnGKE(t *testing.T) {
	ctx := context.Background()
	dp, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)
	gke, err := container.NewService(ctx, gkeOpts()...)
	require.NoError(t, err)

	const project = "proj"
	const region = "us-central1"
	gkeName := unique("gke")
	gkeParent := "projects/" + project + "/locations/" + region + "/clusters/" + gkeName

	// A standard, Workload-Identity-enabled GKE cluster is the prerequisite:
	// Dataproc on GKE rejects an Autopilot or non-WI cluster.
	_, err = gke.Projects.Locations.Clusters.Create(
		"projects/"+project+"/locations/"+region,
		&container.CreateClusterRequest{Cluster: &container.Cluster{
			Name:                   gkeName,
			InitialClusterVersion:  "1.30",
			WorkloadIdentityConfig: &container.WorkloadIdentityConfig{WorkloadPool: project + ".svc.id.goog"},
		}},
	).Do()
	require.NoError(t, err)

	// One caller-shaped pool and one named-only pool (nodePoolConfig omitted →
	// "any node pool with the specified name is used").
	clusterName := unique("dpgke")
	vcc := &dataproc.VirtualClusterConfig{
		StagingBucket: "dataproc-staging-" + project,
		KubernetesClusterConfig: &dataproc.KubernetesClusterConfig{
			KubernetesNamespace: "dataproc",
			GkeClusterConfig: &dataproc.GkeClusterConfig{
				GkeClusterTarget: gkeParent,
				NodePoolTarget: []*dataproc.GkeNodePoolTarget{
					{
						NodePool: gkeParent + "/nodePools/shaped-pool",
						Roles:    []string{"DEFAULT", "CONTROLLER"},
						NodePoolConfig: &dataproc.GkeNodePoolConfig{
							Config: &dataproc.GkeNodeConfig{
								MachineType:   "n1-standard-4",
								LocalSsdCount: 1,
							},
							Autoscaling: &dataproc.GkeNodePoolAutoscalingConfig{MinNodeCount: 1, MaxNodeCount: 4},
							Locations:   []string{region + "-a"},
						},
					},
					{
						NodePool: gkeParent + "/nodePools/spark-pool",
						Roles:    []string{"SPARK_DRIVER", "SPARK_EXECUTOR"},
					},
				},
			},
			KubernetesSoftwareConfig: &dataproc.KubernetesSoftwareConfig{
				// The image-line alias: real GCP accepts `dataproc-2.2` and
				// dereferences it to the newest engine release (DPG36).
				ComponentVersion: map[string]string{"SPARK": "dataproc-2.2"},
			},
		},
	}
	op, err := dp.Projects.Regions.Clusters.Create(project, region, &dataproc.Cluster{
		ProjectId:            project,
		ClusterName:          clusterName,
		VirtualClusterConfig: vcc,
	}).Do()
	require.NoError(t, err)
	created := pollOperation(t, dp, op.Name)
	require.True(t, created.Done)
	var cluster dataproc.Cluster
	require.NoError(t, json.Unmarshal(created.Response, &cluster))
	require.Equal(t, "RUNNING", cluster.Status.State)

	// The image-line alias is dereferenced at create and read back concrete.
	require.Equal(t, "3.5-dataproc-28",
		cluster.VirtualClusterConfig.KubernetesClusterConfig.KubernetesSoftwareConfig.ComponentVersion["SPARK"])

	// nodePoolConfig is input-only: the read never echoes it.
	require.NotNil(t, cluster.VirtualClusterConfig)
	require.NotNil(t, cluster.VirtualClusterConfig.KubernetesClusterConfig)
	readBack := cluster.VirtualClusterConfig.KubernetesClusterConfig.GkeClusterConfig.NodePoolTarget
	require.Len(t, readBack, 2)
	for _, target := range readBack {
		require.Nil(t, target.NodePoolConfig, "nodePoolConfig must not be returned by the API")
	}

	// Both pools exist in the GKE API with the applied shape and the Dataproc
	// auto labels.
	pools, err := gke.Projects.Locations.Clusters.NodePools.List(gkeParent).Do()
	require.NoError(t, err)
	byName := map[string]*container.NodePool{}
	for _, p := range pools.NodePools {
		byName[p.Name] = p
	}
	shaped, ok := byName["shaped-pool"]
	require.True(t, ok, "shaped pool not created: %v", byName)
	require.Equal(t, "n1-standard-4", shaped.Config.MachineType)
	require.EqualValues(t, 1, shaped.Config.LocalSsdCount)
	require.EqualValues(t, 4, shaped.Autoscaling.MaxNodeCount)
	require.Equal(t, []string{region + "-a"}, shaped.Locations)
	require.Equal(t, clusterName, shaped.Config.Labels["goog-dataproc-cluster-name"])
	require.Equal(t, region, shaped.Config.Labels["goog-dataproc-location"])
	require.NotEmpty(t, shaped.Config.Labels["goog-dataproc-cluster-uuid"])

	adopted, ok := byName["spark-pool"]
	require.True(t, ok, "named-only pool not created: %v", byName)
	require.NotNil(t, adopted.Autoscaling)
	require.EqualValues(t, 1, adopted.Autoscaling.MinNodeCount)
	require.EqualValues(t, 10, adopted.Autoscaling.MaxNodeCount)

	// A job submitted to a reconciled virtual cluster still runs to DONE.
	jobID := unique("gkejob")
	job, err := dp.Projects.Regions.Jobs.Submit(project, region, &dataproc.SubmitJobRequest{
		Job: &dataproc.Job{
			Reference: &dataproc.JobReference{ProjectId: project, JobId: jobID},
			Placement: &dataproc.JobPlacement{ClusterName: clusterName},
			PysparkJob: &dataproc.PySparkJob{
				MainPythonFileUri: "gs://jaiscloud-bucket/jobs/pi.py",
			},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "DONE", pollJob(t, dp, project, region, jobID).Status.State)
	_ = job

	// Deleting the virtual cluster leaves its node pools in place (real Dataproc
	// does not delete them).
	del, err := dp.Projects.Regions.Clusters.Delete(project, region, clusterName).Do()
	require.NoError(t, err)
	require.True(t, pollOperation(t, dp, del.Name).Done)
	pools, err = gke.Projects.Locations.Clusters.NodePools.List(gkeParent).Do()
	require.NoError(t, err)
	post := map[string]bool{}
	for _, p := range pools.NodePools {
		post[p.Name] = true
	}
	require.True(t, post["shaped-pool"])
	require.True(t, post["spark-pool"])
}

// TestSDKDataprocOnGKEConflictingPoolShape verifies a nodePoolConfig that
// disagrees with an existing pool fails the create operation: the cluster is
// persisted in ERROR with a status.detail naming the pool and field.
func TestSDKDataprocOnGKEConflictingPoolShape(t *testing.T) {
	ctx := context.Background()
	dp, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)
	gke, err := container.NewService(ctx, gkeOpts()...)
	require.NoError(t, err)

	const project = "proj"
	const region = "us-central1"
	gkeName := unique("gke-c")
	gkeParent := "projects/" + project + "/locations/" + region + "/clusters/" + gkeName
	_, err = gke.Projects.Locations.Clusters.Create(
		"projects/"+project+"/locations/"+region,
		&container.CreateClusterRequest{Cluster: &container.Cluster{
			Name:                   gkeName,
			WorkloadIdentityConfig: &container.WorkloadIdentityConfig{WorkloadPool: project + ".svc.id.goog"},
		}},
	).Do()
	require.NoError(t, err)
	// An existing pool with a shape the virtual cluster disagrees with.
	_, err = gke.Projects.Locations.Clusters.NodePools.Create(gkeParent, &container.CreateNodePoolRequest{
		NodePool: &container.NodePool{
			Name:   "taken-pool",
			Config: &container.NodeConfig{MachineType: "e2-medium"},
		},
	}).Do()
	require.NoError(t, err)

	clusterName := unique("dpgke-c")
	op, err := dp.Projects.Regions.Clusters.Create(project, region, &dataproc.Cluster{
		ProjectId:   project,
		ClusterName: clusterName,
		VirtualClusterConfig: &dataproc.VirtualClusterConfig{
			StagingBucket: "dataproc-staging-" + project,
			KubernetesClusterConfig: &dataproc.KubernetesClusterConfig{
				GkeClusterConfig: &dataproc.GkeClusterConfig{
					GkeClusterTarget: gkeParent,
					NodePoolTarget: []*dataproc.GkeNodePoolTarget{{
						NodePool: gkeParent + "/nodePools/taken-pool",
						Roles:    []string{"DEFAULT"},
						NodePoolConfig: &dataproc.GkeNodePoolConfig{
							Config: &dataproc.GkeNodeConfig{MachineType: "n1-standard-4"},
						},
					}},
				},
				KubernetesSoftwareConfig: &dataproc.KubernetesSoftwareConfig{
					ComponentVersion: map[string]string{"SPARK": "3.5"},
				},
			},
		},
	}).Do()
	require.NoError(t, err)
	created := pollOperation(t, dp, op.Name)
	require.True(t, created.Done)
	var cluster dataproc.Cluster
	require.NoError(t, json.Unmarshal(created.Response, &cluster))
	require.Equal(t, "ERROR", cluster.Status.State)
	require.Contains(t, cluster.Status.Detail, "taken-pool")
	require.Contains(t, cluster.Status.Detail, "machineType")
}

// gkePrereq creates the documented Dataproc-on-GKE prerequisite — a standard,
// Workload-Identity-enabled GKE cluster — through container.v1 and returns its
// canonical target.
func gkePrereq(t *testing.T, gke *container.Service, prefix string) string {
	t.Helper()
	parent := "projects/" + dpProject + "/locations/" + dpRegion
	name := unique(prefix)
	_, err := gke.Projects.Locations.Clusters.Create(parent, &container.CreateClusterRequest{
		Cluster: &container.Cluster{
			Name:                   name,
			WorkloadIdentityConfig: &container.WorkloadIdentityConfig{WorkloadPool: dpProject + ".svc.id.goog"},
		},
	}).Do()
	require.NoError(t, err)
	return parent + "/clusters/" + name
}

// gkePoolName is the full node-pool resource name a nodePoolTarget must carry
// (Discovery's documented format; a bare id is InvalidArgument, DPG44).
func gkePoolName(target, id string) string { return target + "/nodePools/" + id }

// gkeVCC builds a VirtualClusterConfig naming target and applies mut to it. The
// default is one DEFAULT pool with no nodePoolConfig ("any node pool with the
// specified name is used").
func gkeVCC(target string, mut ...func(*dataproc.VirtualClusterConfig)) *dataproc.VirtualClusterConfig {
	vcc := &dataproc.VirtualClusterConfig{
		StagingBucket: "dataproc-staging-" + dpProject,
		KubernetesClusterConfig: &dataproc.KubernetesClusterConfig{
			GkeClusterConfig: &dataproc.GkeClusterConfig{GkeClusterTarget: target},
			KubernetesSoftwareConfig: &dataproc.KubernetesSoftwareConfig{
				ComponentVersion: map[string]string{"SPARK": "3.5"},
			},
		},
	}
	for _, m := range mut {
		m(vcc)
	}
	return vcc
}

// gkePools returns the target GKE cluster's pools keyed by pool name.
func gkePools(t *testing.T, gke *container.Service, parent string) map[string]*container.NodePool {
	t.Helper()
	pools, err := gke.Projects.Locations.Clusters.NodePools.List(parent).Do()
	require.NoError(t, err)
	byName := map[string]*container.NodePool{}
	for _, p := range pools.NodePools {
		byName[p.Name] = p
	}
	return byName
}

// createVirtualCluster creates a GKE-backed virtual cluster and waits for its
// create operation, returning the cluster it settles on.
func createVirtualCluster(t *testing.T, dp *dataproc.Service, name string, vcc *dataproc.VirtualClusterConfig) *dataproc.Cluster {
	t.Helper()
	op, err := dp.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
		ProjectId:            dpProject,
		ClusterName:          name,
		VirtualClusterConfig: vcc,
	}).Do()
	require.NoError(t, err)
	done := pollOperation(t, dp, op.Name)
	require.True(t, done.Done)
	var cluster dataproc.Cluster
	require.NoError(t, json.Unmarshal(done.Response, &cluster))
	return &cluster
}

// TestSDKDataprocOnGKEPoolReuse covers the second half of the cross-API story:
// the pools a virtual cluster reconciled belong to the GKE cluster, so a second
// virtual cluster naming them adopts them instead of duplicating them — the
// pool count through container.v1 stays put.
func TestSDKDataprocOnGKEPoolReuse(t *testing.T) {
	ctx := context.Background()
	dp, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)
	gke, err := container.NewService(ctx, gkeOpts()...)
	require.NoError(t, err)

	target := gkePrereq(t, gke, "gke-reuse")
	parent := target
	// The GKE mock materialises its own default pool on cluster create, so the
	// assertions are deltas against that baseline.
	baseline := len(gkePools(t, gke, parent))
	shaped := &dataproc.GkeNodePoolTarget{
		NodePool: gkePoolName(target, "reuse-shaped"),
		Roles:    []string{"DEFAULT", "CONTROLLER"},
		NodePoolConfig: &dataproc.GkeNodePoolConfig{
			Config:      &dataproc.GkeNodeConfig{MachineType: "n1-standard-4", LocalSsdCount: 1},
			Autoscaling: &dataproc.GkeNodePoolAutoscalingConfig{MinNodeCount: 1, MaxNodeCount: 4},
			Locations:   []string{dpRegion + "-a"},
		},
	}
	named := &dataproc.GkeNodePoolTarget{
		NodePool: gkePoolName(target, "reuse-named"),
		Roles:    []string{"SPARK_DRIVER", "SPARK_EXECUTOR"},
	}
	first := createVirtualCluster(t, dp, unique("dpgke-reuse-a"), gkeVCC(target, func(v *dataproc.VirtualClusterConfig) {
		v.KubernetesClusterConfig.GkeClusterConfig.NodePoolTarget = []*dataproc.GkeNodePoolTarget{shaped, named}
	}))
	require.Equal(t, "RUNNING", first.Status.State)
	require.Len(t, gkePools(t, gke, parent), baseline+2)

	// The pools outlive the virtual cluster that created them.
	del, err := dp.Projects.Regions.Clusters.Delete(dpProject, dpRegion, first.ClusterName).Do()
	require.NoError(t, err)
	require.True(t, pollOperation(t, dp, del.Name).Done)
	afterDelete := gkePools(t, gke, parent)
	require.Len(t, afterDelete, baseline+2)
	require.Contains(t, afterDelete, "reuse-shaped")
	require.Contains(t, afterDelete, "reuse-named")

	// A second virtual cluster on the same GKE cluster adopts those pools: the
	// same two, verified against the same shape, with no duplicate created.
	uuidBefore := afterDelete["reuse-shaped"].Config.Labels["goog-dataproc-cluster-uuid"]
	second := createVirtualCluster(t, dp, unique("dpgke-reuse-b"), gkeVCC(target, func(v *dataproc.VirtualClusterConfig) {
		v.KubernetesClusterConfig.GkeClusterConfig.NodePoolTarget = []*dataproc.GkeNodePoolTarget{shaped, named}
	}))
	require.Equal(t, "RUNNING", second.Status.State, "status detail: %s", second.Status.Detail)
	afterReuse := gkePools(t, gke, parent)
	require.Len(t, afterReuse, baseline+2, "a second virtual cluster must not duplicate the pools")
	require.Equal(t, uuidBefore, afterReuse["reuse-shaped"].Config.Labels["goog-dataproc-cluster-uuid"],
		"an adopted pool keeps the labels of the virtual cluster that created it")
}

// TestSDKDataprocOnGKEFailurePaths asserts every documented create-time rule as
// an API error through the Dataproc client, and the recreate-only/namespace
// rules on the existing-cluster path.
func TestSDKDataprocOnGKEFailurePaths(t *testing.T) {
	ctx := context.Background()
	dp, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)
	gke, err := container.NewService(ctx, gkeOpts()...)
	require.NoError(t, err)

	target := gkePrereq(t, gke, "gke-fail")

	cases := []struct {
		name    string
		mut     func(*dataproc.VirtualClusterConfig)
		code    int
		message string
	}{
		{
			name: "missing DEFAULT role",
			mut: func(v *dataproc.VirtualClusterConfig) {
				v.KubernetesClusterConfig.GkeClusterConfig.NodePoolTarget = []*dataproc.GkeNodePoolTarget{
					{NodePool: gkePoolName(target, "only-driver"), Roles: []string{"SPARK_DRIVER"}},
				}
			},
			code: 400, message: "DEFAULT",
		},
		{
			name: "two pools claim one role",
			mut: func(v *dataproc.VirtualClusterConfig) {
				v.KubernetesClusterConfig.GkeClusterConfig.NodePoolTarget = []*dataproc.GkeNodePoolTarget{
					{NodePool: gkePoolName(target, "role-a"), Roles: []string{"DEFAULT"}},
					{NodePool: gkePoolName(target, "role-b"), Roles: []string{"DEFAULT"}},
				}
			},
			code: 400, message: "only one nodePoolTarget",
		},
		{
			name: "pool outside the region",
			mut: func(v *dataproc.VirtualClusterConfig) {
				v.KubernetesClusterConfig.GkeClusterConfig.NodePoolTarget = []*dataproc.GkeNodePoolTarget{{
					NodePool: gkePoolName(target, "wrong-zone"),
					Roles:    []string{"DEFAULT"},
					NodePoolConfig: &dataproc.GkeNodePoolConfig{
						Locations: []string{"us-east1-b"},
					},
				}}
			},
			code: 400, message: "not a zone in region",
		},
		{
			name: "autoscaler bounds",
			mut: func(v *dataproc.VirtualClusterConfig) {
				v.KubernetesClusterConfig.GkeClusterConfig.NodePoolTarget = []*dataproc.GkeNodePoolTarget{{
					NodePool: gkePoolName(target, "bad-bounds"),
					Roles:    []string{"DEFAULT"},
					NodePoolConfig: &dataproc.GkeNodePoolConfig{
						Autoscaling: &dataproc.GkeNodePoolAutoscalingConfig{MinNodeCount: 5, MaxNodeCount: 2},
					},
				}}
			},
			code: 400, message: "0 <= minNodeCount <= maxNodeCount",
		},
		{
			// A bare pool id is not the resource name the API requires (DPG44).
			name: "short node pool name",
			mut: func(v *dataproc.VirtualClusterConfig) {
				v.KubernetesClusterConfig.GkeClusterConfig.NodePoolTarget = []*dataproc.GkeNodePoolTarget{
					{NodePool: "bare-pool", Roles: []string{"DEFAULT"}},
				}
			},
			code: 400, message: "must conform to pattern",
		},
		{
			name: "target in another project",
			mut: func(v *dataproc.VirtualClusterConfig) {
				v.KubernetesClusterConfig.GkeClusterConfig.GkeClusterTarget =
					"projects/another-project/locations/" + dpRegion + "/clusters/elsewhere"
			},
			code: 400, message: "must be in the same project",
		},
		{
			// Only `dataproc-2.2` is a valid image line; the others are rejected
			// by real GCP too, so they stay InvalidArgument here (DPG36).
			name: "unknown image-line alias",
			mut: func(v *dataproc.VirtualClusterConfig) {
				v.KubernetesClusterConfig.KubernetesSoftwareConfig.ComponentVersion["SPARK"] = "dataproc-2.3"
			},
			code: 400, message: "unknown Spark engine version",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vcc := gkeVCC(target, tc.mut)
			_, err := dp.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
				ProjectId:            dpProject,
				ClusterName:          unique("dpgke-bad"),
				VirtualClusterConfig: vcc,
			}).Do()
			requireCode(t, err, tc.code, tc.name)
			var gerr *googleapi.Error
			require.ErrorAs(t, err, &gerr)
			require.Contains(t, gerr.Message, tc.message, tc.name)
		})
	}

	// The target cluster itself must exist, be standard, and have Workload
	// Identity: each is a create-time rejection, not an ERROR-status cluster.
	t.Run("missing GKE cluster", func(t *testing.T) {
		missing := "projects/" + dpProject + "/locations/" + dpRegion + "/clusters/does-not-exist"
		_, err := dp.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
			ProjectId:            dpProject,
			ClusterName:          unique("dpgke-bad"),
			VirtualClusterConfig: gkeVCC(missing),
		}).Do()
		requireCode(t, err, 404, "missing GKE cluster")
	})

	t.Run("target without Workload Identity", func(t *testing.T) {
		name := unique("gke-no-wi")
		_, err := gke.Projects.Locations.Clusters.Create("projects/"+dpProject+"/locations/"+dpRegion,
			&container.CreateClusterRequest{Cluster: &container.Cluster{Name: name}}).Do()
		require.NoError(t, err)
		_, err = dp.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
			ProjectId:            dpProject,
			ClusterName:          unique("dpgke-bad"),
			VirtualClusterConfig: gkeVCC("projects/" + dpProject + "/locations/" + dpRegion + "/clusters/" + name),
		}).Do()
		requireCode(t, err, 400, "target without Workload Identity")
	})

	t.Run("Autopilot target", func(t *testing.T) {
		name := unique("gke-autopilot")
		_, err := gke.Projects.Locations.Clusters.Create("projects/"+dpProject+"/locations/"+dpRegion,
			&container.CreateClusterRequest{Cluster: &container.Cluster{
				Name:                   name,
				Autopilot:              &container.Autopilot{Enabled: true},
				WorkloadIdentityConfig: &container.WorkloadIdentityConfig{WorkloadPool: dpProject + ".svc.id.goog"},
			}}).Do()
		require.NoError(t, err)
		_, err = dp.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
			ProjectId:            dpProject,
			ClusterName:          unique("dpgke-bad"),
			VirtualClusterConfig: gkeVCC("projects/" + dpProject + "/locations/" + dpRegion + "/clusters/" + name),
		}).Do()
		requireCode(t, err, 400, "Autopilot target")
	})

	// Recreate-only: a patch that would apply virtualClusterConfig is rejected;
	// the same patch on a GCE cluster is not (covered by the depth suite).
	t.Run("patch is recreate-only", func(t *testing.T) {
		name := unique("dpgke-patch")
		created := createVirtualCluster(t, dp, name, gkeVCC(target))
		require.Equal(t, "RUNNING", created.Status.State)
		_, err := dp.Projects.Regions.Clusters.Patch(dpProject, dpRegion, name, &dataproc.Cluster{
			VirtualClusterConfig: gkeVCC(target),
		}).UpdateMask("virtualClusterConfig").Do()
		requireCode(t, err, 400, "patch is recreate-only")
		var gerr *googleapi.Error
		require.ErrorAs(t, err, &gerr)
		require.Contains(t, gerr.Message, "recreate-only")
	})

	// A kubernetesNamespace is owned by one virtual cluster at a time; a second
	// one on the same GKE cluster is FailedPrecondition. Dropping the namespace
	// (which then defaults to the new cluster's name) allows the recreate.
	t.Run("duplicate kubernetesNamespace", func(t *testing.T) {
		namespace := unique("ns")
		first := createVirtualCluster(t, dp, unique("dpgke-ns-a"), gkeVCC(target, func(v *dataproc.VirtualClusterConfig) {
			v.KubernetesClusterConfig.KubernetesNamespace = namespace
		}))
		require.Equal(t, "RUNNING", first.Status.State)
		_, err := dp.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
			ProjectId:   dpProject,
			ClusterName: unique("dpgke-ns-b"),
			VirtualClusterConfig: gkeVCC(target, func(v *dataproc.VirtualClusterConfig) {
				v.KubernetesClusterConfig.KubernetesNamespace = namespace
			}),
		}).Do()
		requireCode(t, err, 400, "duplicate kubernetesNamespace")
		var gerr *googleapi.Error
		require.ErrorAs(t, err, &gerr)
		require.Contains(t, gerr.Message, "already used by Dataproc virtual cluster")
	})
}

// TestSDKDataprocOnGKERecreateAfterNamespaceDrop proves the documented
// recreate-only recipe over REST (README-GCP.md §Dataproc workload placement):
// a virtual cluster owns the kubernetesNamespace it names, so a second live
// cluster still naming that namespace is FailedPrecondition; after the owner is
// deleted, recreating the same name WITHOUT kubernetesNamespace defaults it to
// the new cluster's name and the create settles RUNNING. It proves the recipe,
// not the ownership rule itself (validateGKENamespaceOwnership has its own unit
// tests).
func TestSDKDataprocOnGKERecreateAfterNamespaceDrop(t *testing.T) {
	ctx := context.Background()
	dp, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)
	gke, err := container.NewService(ctx, gkeOpts()...)
	require.NoError(t, err)

	target := gkePrereq(t, gke, "gke-ns-recreate")
	namespace := unique("ns-recreate")
	name := unique("dpgke-ns-recreate")

	// The owner: an explicit namespace, not the cluster-name default.
	first := createVirtualCluster(t, dp, name, gkeVCC(target, func(v *dataproc.VirtualClusterConfig) {
		v.KubernetesClusterConfig.KubernetesNamespace = namespace
	}))
	require.Equal(t, "RUNNING", first.Status.State, "status detail: %s", first.Status.Detail)

	// The negative half: a recreate that still names the old namespace is
	// rejected while a live virtual cluster owns it.
	_, err = dp.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
		ProjectId:   dpProject,
		ClusterName: unique("dpgke-ns-claim"),
		VirtualClusterConfig: gkeVCC(target, func(v *dataproc.VirtualClusterConfig) {
			v.KubernetesClusterConfig.KubernetesNamespace = namespace
		}),
	}).Do()
	requireCode(t, err, 400, "namespace still owned")
	var gerr *googleapi.Error
	require.ErrorAs(t, err, &gerr)
	require.Contains(t, gerr.Message, "already used by Dataproc virtual cluster")

	// Delete the owner, then recreate the same name without a namespace.
	del, err := dp.Projects.Regions.Clusters.Delete(dpProject, dpRegion, name).Do()
	require.NoError(t, err)
	require.True(t, pollOperation(t, dp, del.Name).Done)

	recreated := createVirtualCluster(t, dp, name, gkeVCC(target))
	require.Equal(t, "RUNNING", recreated.Status.State, "status detail: %s", recreated.Status.Detail)
	require.Equal(t, name, recreated.VirtualClusterConfig.KubernetesClusterConfig.KubernetesNamespace,
		"a recreate without kubernetesNamespace defaults it to the cluster name")
}
