package grpcconformance

import (
	"context"
	"fmt"
	"strings"

	container "cloud.google.com/go/container/apiv1"
	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// dataprocChecks covers the Cloud Dataproc v1 surface
// (google.cloud.dataproc.v1.ClusterController and JobController) via the
// official generated cloud.google.com/go/dataproc/v2 client: cluster CRUD +
// start/stop, and job submit/get/list/update/cancel/delete. Cluster mutations
// and SubmitJobAsOperation are long-running operations whose response is packed
// inline as a typed Any, so the client's Wait observes them without polling.
//
// DiagnoseCluster is deliberately not probed: it is an explicit Unimplemented
// stub. Every probe is self-contained (a run-unique cluster/job via
// cfg.ResourceName), so a long-lived emulator never sees cross-run collisions.
func dataprocChecks() []Check {
	return []Check{
		{Service: "dataproc", RPC: "CreateCluster", Method: "CreateCluster", KeyField: "LRO done + RUNNING cluster", Run: checkDPCreateCluster},
		{Service: "dataproc", RPC: "CreateCluster (no placement)", Method: "CreateCluster", KeyField: "InvalidArgument when neither config nor virtualClusterConfig is set", Run: checkDPCreateClusterNoPlacement},
		{Service: "dataproc", RPC: "CreateCluster (on GKE, unknown target)", Method: "CreateCluster", KeyField: "NotFound for an unresolvable gkeClusterTarget", Run: checkDPCreateClusterGKEUnknownTarget},
		{Service: "dataproc", RPC: "GetCluster", Method: "GetCluster", KeyField: "clusterName round-trip", Run: checkDPGetCluster}, {Service: "dataproc", RPC: "ListClusters", Method: "ListClusters", KeyField: "created cluster present", Run: checkDPListClusters},
		{Service: "dataproc", RPC: "UpdateCluster", Method: "UpdateCluster", KeyField: "labels updated via LRO", Run: checkDPUpdateCluster},
		{Service: "dataproc", RPC: "StopCluster", Method: "StopCluster", KeyField: "STOPPED cluster returned", Run: checkDPStopCluster},
		{Service: "dataproc", RPC: "StartCluster", Method: "StartCluster", KeyField: "RUNNING cluster returned", Run: checkDPStartCluster},
		{Service: "dataproc", RPC: "DeleteCluster", Method: "DeleteCluster", KeyField: "NotFound after delete", Run: checkDPDeleteCluster},
		{Service: "dataproc", RPC: "SubmitJob", Method: "SubmitJob", KeyField: "PENDING job polls to DONE", Run: checkDPSubmitJob},
		{Service: "dataproc", RPC: "SubmitJobAsOperation", Method: "SubmitJobAsOperation", KeyField: "in-flight LRO -> typed Job response", Run: checkDPSubmitJobAsOperation},
		{Service: "dataproc", RPC: "GetJob", Method: "GetJob", KeyField: "job reference round-trip", Run: checkDPGetJob},
		{Service: "dataproc", RPC: "ListJobs", Method: "ListJobs", KeyField: "submitted job present", Run: checkDPListJobs},
		{Service: "dataproc", RPC: "ListJobs", Method: "ListJobs", KeyField: "filter selects matching labels", Run: checkDPListJobsFilter},
		{Service: "dataproc", RPC: "UpdateJob", Method: "UpdateJob", KeyField: "labels updated", Run: checkDPUpdateJob},
		{Service: "dataproc", RPC: "CancelJob", Method: "CancelJob", KeyField: "CANCEL_PENDING polls to CANCELLED", Run: checkDPCancelJob},
		{Service: "dataproc", RPC: "DeleteJob", Method: "DeleteJob", KeyField: "NotFound after delete", Run: checkDPDeleteJob},
		{Service: "dataproc", RPC: "CreateCluster (vcc)", Method: "CreateCluster", KeyField: "GKE virtualClusterConfig round-trip", Run: checkDPVirtualClusterConfig},
		{Service: "dataproc", RPC: "CreateCluster (vcc pools)", Method: "CreateCluster", KeyField: "reconciled pools read back through ClusterManager + survive delete", Run: checkDPGKEVirtualClusterPools},
		{Service: "dataproc", RPC: "CreateCluster (vcc reuse)", Method: "CreateCluster", KeyField: "a second virtual cluster adopts existing pools", Run: checkDPGKEVirtualClusterPoolReuse},
		{Service: "dataproc", RPC: "CreateCluster (vcc rules)", Method: "CreateCluster", KeyField: "GKE placement/pool validation as client-visible errors", Run: checkDPGKEVirtualClusterRules},
		{Service: "dataproc", RPC: "CreateCluster (vcc pool shape)", Method: "CreateCluster", KeyField: "done LRO -> ERROR cluster with the pool and field in status.detail", Run: checkDPGKEVirtualClusterPoolShapeConflict},
		{Service: "dataproc", RPC: "UpdateCluster (vcc)", Method: "UpdateCluster", KeyField: "recreate-only: virtualClusterConfig patch is InvalidArgument", Run: checkDPVirtualClusterRecreateOnly},
		{Service: "dataproc", RPC: "CreateCluster (vcc image alias)", Method: "CreateCluster", KeyField: "dataproc-2.2 dereferences to the newest engine release", Run: checkDPGKEImageVersionAlias},
		{Service: "dataproc", RPC: "CreateCluster (metastore)", Method: "CreateCluster", KeyField: "auxiliaryServicesConfig.metastoreConfig echo", Run: checkDPMetastoreAttachment},
		{Service: "dataproc", RPC: "CreateCluster (spark history server)", Method: "CreateCluster", KeyField: "PHS cluster validated + spark.eventLog.* properties", Run: checkDPHistoryServerAttachment},
		{Service: "dataproc", RPC: "GetJob (placement)", Method: "GetJob", KeyField: "placement.clusterUuid", Run: checkDPJobPlacementClusterUUID},
		{Service: "dataproc", RPC: "SubmitJob (driver output)", Method: "SubmitJob", KeyField: "driver output/control URIs present", Run: checkDPDriverOutputURIs},
		{Service: "dataproc", RPC: "CreateWorkflowTemplate", Method: "CreateWorkflowTemplate", KeyField: "name/version round-trip", Run: checkDPCreateWorkflowTemplate},
		{Service: "dataproc", RPC: "GetWorkflowTemplate", Method: "GetWorkflowTemplate", KeyField: "job stepId round-trip", Run: checkDPGetWorkflowTemplate},
		{Service: "dataproc", RPC: "ListWorkflowTemplates", Method: "ListWorkflowTemplates", KeyField: "created template present", Run: checkDPListWorkflowTemplates},
		{Service: "dataproc", RPC: "UpdateWorkflowTemplate", Method: "UpdateWorkflowTemplate", KeyField: "version bumped", Run: checkDPUpdateWorkflowTemplate},
		{Service: "dataproc", RPC: "DeleteWorkflowTemplate", Method: "DeleteWorkflowTemplate", KeyField: "NotFound after delete", Run: checkDPDeleteWorkflowTemplate},
		{Service: "dataproc", RPC: "InstantiateInlineWorkflowTemplate", Method: "InstantiateInlineWorkflowTemplate", KeyField: "LRO completes with DONE WorkflowMetadata", Run: checkDPInstantiateInlineWorkflowTemplate},
		{Service: "dataproc", RPC: "InstantiateWorkflowTemplate", Method: "InstantiateWorkflowTemplate", KeyField: "stored template + parameters run to DONE", Run: checkDPInstantiateWorkflowTemplate},
	}
}

const dataprocRegion = "us-central1"

func dataprocClientOptions(cfg Config) []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

func newDataprocClusterClient(ctx context.Context, cfg Config) (*dataproc.ClusterControllerClient, error) {
	return dataproc.NewClusterControllerClient(ctx, dataprocClientOptions(cfg)...)
}

func newDataprocJobClient(ctx context.Context, cfg Config) (*dataproc.JobControllerClient, error) {
	return dataproc.NewJobControllerClient(ctx, dataprocClientOptions(cfg)...)
}

// dataprocClusterID is the run-unique cluster shared by the probes.
func dataprocClusterID(cfg Config) string { return cfg.ResourceName("gcpc-grpc-dp") }

// probeClusterConfig is the minimal Compute-Engine placement: a create must
// carry exactly one of config or virtualClusterConfig ("Exactly one of
// ClusterConfig or VirtualClusterConfig must be specified").
func probeClusterConfig() *dataprocpb.ClusterConfig {
	return &dataprocpb.ClusterConfig{GceClusterConfig: &dataprocpb.GceClusterConfig{ZoneUri: "us-central1-a"}}
}

// ensureDataprocCluster creates the run-unique probe cluster, treating
// AlreadyExists as success so repeated probes are idempotent.
func ensureDataprocCluster(ctx context.Context, client *dataproc.ClusterControllerClient, cfg Config) (string, error) {
	id := dataprocClusterID(cfg)
	op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id, Config: probeClusterConfig(), Labels: map[string]string{"probe": "gcpc"}},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return id, nil
		}
		return "", fmt.Errorf("create cluster: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait cluster: %w", err)
	}
	return id, nil
}

// dataprocJob returns a minimal run-unique PySpark job body.
func dataprocJob(jobID, cluster string) *dataprocpb.Job {
	return &dataprocpb.Job{
		Reference: &dataprocpb.JobReference{JobId: jobID},
		Placement: &dataprocpb.JobPlacement{ClusterName: cluster},
		TypeJob: &dataprocpb.Job_PysparkJob{
			PysparkJob: &dataprocpb.PySparkJob{MainPythonFileUri: "gs://bucket/main.py"},
		},
	}
}

// dataprocJobWithLabels is dataprocJob plus labels, for the ListJobs filter
// probe.
func dataprocJobWithLabels(jobID, cluster string, labels map[string]string) *dataprocpb.Job {
	j := dataprocJob(jobID, cluster)
	j.Labels = labels
	return j
}

// ─── Clusters ─────────────────────────────────────────────────────────────────

// Check 1: CreateCluster returns a done operation whose response is a RUNNING
// cluster.
func checkDPCreateCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-dp-create")
	op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id, Config: probeClusterConfig(), Labels: map[string]string{"probe": "gcpc"}},
	})
	if err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	// Cluster create is asynchronous: the operation is returned in flight and
	// completes only when polled through google.longrunning.Operations.
	if op.Done() {
		return fmt.Errorf("CreateCluster completed inline; want an in-flight LRO")
	}
	meta, err := op.Metadata()
	if err != nil {
		return fmt.Errorf("operation metadata: %w", err)
	}
	if meta.GetClusterName() != id || meta.GetOperationType() != "CREATE" {
		return fmt.Errorf("operation metadata = %+v", meta)
	}
	if _, err := op.Poll(ctx); err != nil {
		return fmt.Errorf("poll create operation: %w", err)
	}
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if cluster.GetClusterName() != id {
		return fmt.Errorf("cluster name = %q, want %q", cluster.GetClusterName(), id)
	}
	if cluster.GetStatus().GetState() != dataprocpb.ClusterStatus_RUNNING {
		return fmt.Errorf("cluster state = %v, want RUNNING", cluster.GetStatus().GetState())
	}
	// A caller names the zone by short name; a read answers the absolute
	// Compute self-link (DPG40).
	wantZone := "https://www.googleapis.com/compute/v1/projects/" + cfg.Project + "/zones/us-central1-a"
	if got := cluster.GetConfig().GetGceClusterConfig().GetZoneUri(); got != wantZone {
		return fmt.Errorf("gceClusterConfig.zoneUri = %q, want %q", got, wantZone)
	}
	return nil
}

// checkDPCreateClusterNoPlacement verifies Discovery's "Exactly one of
// ClusterConfig or VirtualClusterConfig must be specified": a create carrying
// neither is InvalidArgument, and no cluster is persisted from it.
func checkDPCreateClusterNoPlacement(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-dp-noplacement")
	_, err = client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id},
	})
	if status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("create without config or virtualClusterConfig: code = %v, want InvalidArgument", status.Code(err))
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "exactly one of config or virtualClusterConfig") {
		return fmt.Errorf("create without placement message = %q", msg)
	}
	if _, err := client.GetCluster(ctx, &dataprocpb.GetClusterRequest{
		ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id,
	}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("rejected create left a cluster behind: code = %v, want NotFound", status.Code(err))
	}
	return nil
}

// checkDPCreateClusterGKEUnknownTarget verifies a Dataproc-on-GKE create
// resolves gkeClusterTarget through the GKE service on both transports: an
// unknown target is NotFound, not a silently-accepted metadata-only cluster.
func checkDPCreateClusterGKEUnknownTarget(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-dp-gke-missing")
	missingTarget := "projects/" + cfg.Project + "/locations/" + dataprocRegion + "/clusters/gcpc-missing-gke"
	_, err = client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster: &dataprocpb.Cluster{
			ClusterName: id,
			VirtualClusterConfig: &dataprocpb.VirtualClusterConfig{
				StagingBucket: "dataproc-staging-" + cfg.Project,
				InfrastructureConfig: &dataprocpb.VirtualClusterConfig_KubernetesClusterConfig{
					KubernetesClusterConfig: &dataprocpb.KubernetesClusterConfig{
						Config: &dataprocpb.KubernetesClusterConfig_GkeClusterConfig{
							GkeClusterConfig: &dataprocpb.GkeClusterConfig{
								GkeClusterTarget: missingTarget,
								NodePoolTarget: []*dataprocpb.GkeNodePoolTarget{{
									NodePool: missingTarget + "/nodePools/default-pool",
									Roles:    []dataprocpb.GkeNodePoolTarget_Role{dataprocpb.GkeNodePoolTarget_DEFAULT},
								}},
							},
						},
						KubernetesSoftwareConfig: &dataprocpb.KubernetesSoftwareConfig{
							ComponentVersion: map[string]string{"SPARK": "3.5"},
						},
					},
				},
			},
		},
	})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("CreateCluster with an unknown gkeClusterTarget: got %v, want NotFound", err)
	}
	return nil
}

// Check 2: GetCluster round-trips.
func checkDPGetCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id, err := ensureDataprocCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	got, err := client.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if got.GetClusterName() != id || got.GetLabels()["probe"] != "gcpc" {
		return fmt.Errorf("cluster = %+v", got)
	}
	return nil
}

// Check 3: ListClusters includes the probe cluster.
func checkDPListClusters(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id, err := ensureDataprocCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	it := client.ListClusters(ctx, &dataprocpb.ListClustersRequest{ProjectId: cfg.Project, Region: dataprocRegion})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListClusters did not include %q", id)
		}
		if err != nil {
			return fmt.Errorf("ListClusters: %w", err)
		}
		if got.GetClusterName() == id {
			return nil
		}
	}
}

// Check 4: UpdateCluster applies labels through an LRO.
func checkDPUpdateCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id, err := ensureDataprocCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	op, err := client.UpdateCluster(ctx, &dataprocpb.UpdateClusterRequest{
		ProjectId:   cfg.Project,
		Region:      dataprocRegion,
		ClusterName: id,
		Cluster:     &dataprocpb.Cluster{Labels: map[string]string{"updated": "yes"}},
		UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateCluster: %w", err)
	}
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if cluster.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("updated labels = %v", cluster.GetLabels())
	}
	return nil
}

// Check 5: StopCluster returns a STOPPED cluster.
func checkDPStopCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id, err := ensureDataprocCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	op, err := client.StopCluster(ctx, &dataprocpb.StopClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("StopCluster: %w", err)
	}
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if cluster.GetStatus().GetState() != dataprocpb.ClusterStatus_STOPPED {
		return fmt.Errorf("cluster state = %v, want STOPPED", cluster.GetStatus().GetState())
	}
	return nil
}

// Check 6: StartCluster returns a RUNNING cluster.
func checkDPStartCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id, err := ensureDataprocCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	op, err := client.StartCluster(ctx, &dataprocpb.StartClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("StartCluster: %w", err)
	}
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if cluster.GetStatus().GetState() != dataprocpb.ClusterStatus_RUNNING {
		return fmt.Errorf("cluster state = %v, want RUNNING", cluster.GetStatus().GetState())
	}
	return nil
}

// Check 7: DeleteCluster removes the cluster.
func checkDPDeleteCluster(ctx context.Context, cfg Config) error {
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-dp-del")
	if op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project, Region: dataprocRegion, Cluster: &dataprocpb.Cluster{ClusterName: id, Config: probeClusterConfig()},
	}); err != nil {
		return fmt.Errorf("create: %w", err)
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("create wait: %w", err)
	}
	delOp, err := client.DeleteCluster(ctx, &dataprocpb.DeleteClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("DeleteCluster: %w", err)
	}
	// Delete is asynchronous as well: poll the operation to completion.
	if delOp.Done() {
		return fmt.Errorf("DeleteCluster completed inline; want an in-flight LRO")
	}
	if err := delOp.Poll(ctx); err != nil {
		return fmt.Errorf("poll delete operation: %w", err)
	}
	if err := delOp.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetCluster after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── Jobs ─────────────────────────────────────────────────────────────────────

// dpPollJobTerminal polls GetJob until the job reaches a terminal state,
// driving the emulator's lazy job state machine one hop per read.
func dpPollJobTerminal(ctx context.Context, jc *dataproc.JobControllerClient, project, jobID string) (*dataprocpb.Job, error) {
	for i := 0; i < 32; i++ {
		j, err := jc.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: project, Region: dataprocRegion, JobId: jobID})
		if err != nil {
			return nil, fmt.Errorf("GetJob: %w", err)
		}
		switch j.GetStatus().GetState() {
		case dataprocpb.JobStatus_DONE, dataprocpb.JobStatus_ERROR, dataprocpb.JobStatus_CANCELLED:
			return j, nil
		}
	}
	return nil, fmt.Errorf("job %s did not reach a terminal state", jobID)
}

// Check 8: SubmitJob returns an in-flight job that walks to DONE in mock mode.
func checkDPSubmitJob(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job")
	j, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)})
	if err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	if j.GetReference().GetJobId() != jobID {
		return fmt.Errorf("job reference = %+v", j.GetReference())
	}
	if j.GetStatus().GetState() != dataprocpb.JobStatus_PENDING {
		return fmt.Errorf("submitted job state = %v, want PENDING", j.GetStatus().GetState())
	}
	final, err := dpPollJobTerminal(ctx, jc, cfg.Project, jobID)
	if err != nil {
		return err
	}
	if final.GetStatus().GetState() != dataprocpb.JobStatus_DONE {
		return fmt.Errorf("job state = %v, want DONE", final.GetStatus().GetState())
	}
	return nil
}

// Check 9: SubmitJobAsOperation returns an in-flight LRO that completes with a
// typed Job response.
func checkDPSubmitJobAsOperation(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-jobsop")
	op, err := jc.SubmitJobAsOperation(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)})
	if err != nil {
		return fmt.Errorf("SubmitJobAsOperation: %w", err)
	}
	// The job is asynchronous: the operation is returned in flight and
	// completes when polled through google.longrunning.Operations.
	if op.Done() {
		return fmt.Errorf("SubmitJobAsOperation completed inline; want an in-flight LRO")
	}
	meta, err := op.Metadata()
	if err != nil {
		return fmt.Errorf("operation metadata: %w", err)
	}
	if meta.GetJobId() != jobID {
		return fmt.Errorf("operation metadata = %+v", meta)
	}
	job, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if job.GetReference().GetJobId() != jobID {
		return fmt.Errorf("job = %+v", job)
	}
	if job.GetStatus().GetState() != dataprocpb.JobStatus_DONE {
		return fmt.Errorf("job state = %v, want DONE", job.GetStatus().GetState())
	}
	return nil
}

// Check 10: GetJob round-trips.
func checkDPGetJob(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-get")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	got, err := jc.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, JobId: jobID})
	if err != nil {
		return fmt.Errorf("GetJob: %w", err)
	}
	if got.GetReference().GetJobId() != jobID {
		return fmt.Errorf("job = %+v", got)
	}
	return nil
}

// Check 11: ListJobs includes the probe job.
func checkDPListJobs(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-list")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	it := jc.ListJobs(ctx, &dataprocpb.ListJobsRequest{ProjectId: cfg.Project, Region: dataprocRegion})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListJobs did not include %q", jobID)
		}
		if err != nil {
			return fmt.Errorf("ListJobs: %w", err)
		}
		if got.GetReference().GetJobId() == jobID {
			return nil
		}
	}
}

// Check 11b: ListJobs honors a labels.<key> = value filter, returning the
// matching job and excluding a job whose label value differs.
func checkDPListJobsFilter(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()

	matchingID := cfg.ResourceName("gcpc-grpc-dp-job-filter-on")
	otherID := cfg.ResourceName("gcpc-grpc-dp-job-filter-off")
	for id, label := range map[string]string{matchingID: "only", otherID: "other"} {
		if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{
			ProjectId: cfg.Project, Region: dataprocRegion,
			Job: dataprocJobWithLabels(id, cluster, map[string]string{"probe-filter": label}),
		}); err != nil {
			return fmt.Errorf("SubmitJob(%s): %w", id, err)
		}
	}

	it := jc.ListJobs(ctx, &dataprocpb.ListJobsRequest{
		ProjectId: cfg.Project, Region: dataprocRegion,
		Filter: `labels.probe-filter = only`,
	})
	seen := map[string]bool{}
	for {
		got, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListJobs: %w", err)
		}
		seen[got.GetReference().GetJobId()] = true
	}
	if !seen[matchingID] {
		return fmt.Errorf("filtered ListJobs did not include %q", matchingID)
	}
	if seen[otherID] {
		return fmt.Errorf("filtered ListJobs included non-matching %q", otherID)
	}
	return nil
}

// Check 12: UpdateJob applies labels.
func checkDPUpdateJob(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-upd")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	got, err := jc.UpdateJob(ctx, &dataprocpb.UpdateJobRequest{
		ProjectId:  cfg.Project,
		Region:     dataprocRegion,
		JobId:      jobID,
		Job:        &dataprocpb.Job{Reference: &dataprocpb.JobReference{JobId: jobID}, Labels: map[string]string{"updated": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateJob: %w", err)
	}
	if got.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("job labels = %v", got.GetLabels())
	}
	return nil
}

// Check 13: CancelJob starts the cancel progression; polling settles CANCELLED.
func checkDPCancelJob(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-cancel")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	// CancelJob starts the cancel progression (CANCEL_PENDING); the job settles
	// to CANCELLED when polled.
	if _, err := jc.CancelJob(ctx, &dataprocpb.CancelJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, JobId: jobID}); err != nil {
		return fmt.Errorf("CancelJob: %w", err)
	}
	final, err := dpPollJobTerminal(ctx, jc, cfg.Project, jobID)
	if err != nil {
		return err
	}
	if final.GetStatus().GetState() != dataprocpb.JobStatus_CANCELLED {
		return fmt.Errorf("job state after cancel = %v, want CANCELLED", final.GetStatus().GetState())
	}
	return nil
}

// ─── New-surface probes (W4.1 / DPG8) ────────────────────────────────────────

// gkeVirtualClusterConfig returns a minimal valid dataproc.v1.VirtualClusterConfig
// for an existing-cluster GKE target.
func gkeVirtualClusterConfig(target string) *dataprocpb.VirtualClusterConfig {
	return &dataprocpb.VirtualClusterConfig{
		StagingBucket: "dataproc-staging-probe",
		InfrastructureConfig: &dataprocpb.VirtualClusterConfig_KubernetesClusterConfig{
			KubernetesClusterConfig: &dataprocpb.KubernetesClusterConfig{
				Config: &dataprocpb.KubernetesClusterConfig_GkeClusterConfig{
					GkeClusterConfig: &dataprocpb.GkeClusterConfig{GkeClusterTarget: target},
				},
			},
		},
	}
}

// dpGkeTarget is the GKE cluster target the probes reference. Dataproc on GKE
// resolves it through the emulated GKE service, so the GKE cluster is a real
// prerequisite (see ensureGKEPrereqCluster), not just a name.
func dpGkeTarget(cfg Config) string {
	return "projects/" + cfg.Project + "/locations/" + dataprocRegion + "/clusters/" + cfg.ResourceName("gcpc-gke")
}

// Check 15: a GKE-backed cluster keeps its virtualClusterConfig through gRPC and
// never invents a GCE config.
func checkDPVirtualClusterConfig(ctx context.Context, cfg Config) error {
	if _, err := ensureGKEPrereqCluster(ctx, cfg, "gcpc-gke"); err != nil {
		return err
	}
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-dp-vcc")
	target := dpGkeTarget(cfg)
	op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id, VirtualClusterConfig: gkeVirtualClusterConfig(target)},
	})
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("CreateCluster: %w", err)
		}
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	got, err := client.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if got.GetConfig() != nil {
		return fmt.Errorf("GKE cluster carries a GCE config: %+v", got.GetConfig())
	}
	if t := got.GetVirtualClusterConfig().GetKubernetesClusterConfig().GetGkeClusterConfig().GetGkeClusterTarget(); t != target {
		return fmt.Errorf("gkeClusterTarget = %q, want %q", t, target)
	}
	return nil
}

// Check 15b: a Dataproc-on-GKE virtual cluster is recreate-only. A create with
// no kubernetesNamespace renders the cluster name as the effective namespace and
// dereferences the SPARK alias to a concrete release; a patch touching
// virtualClusterConfig is InvalidArgument, while a labels-only patch succeeds.
func checkDPVirtualClusterRecreateOnly(ctx context.Context, cfg Config) error {
	if _, err := ensureGKEPrereqCluster(ctx, cfg, "gcpc-gke"); err != nil {
		return err
	}
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-dp-vccupd")
	vcc := &dataprocpb.VirtualClusterConfig{
		InfrastructureConfig: &dataprocpb.VirtualClusterConfig_KubernetesClusterConfig{
			KubernetesClusterConfig: &dataprocpb.KubernetesClusterConfig{
				Config: &dataprocpb.KubernetesClusterConfig_GkeClusterConfig{
					GkeClusterConfig: &dataprocpb.GkeClusterConfig{GkeClusterTarget: dpGkeTarget(cfg)},
				},
				KubernetesSoftwareConfig: &dataprocpb.KubernetesSoftwareConfig{
					ComponentVersion: map[string]string{"SPARK": "3.5"},
				},
			},
		},
	}
	op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id, VirtualClusterConfig: vcc},
	})
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("CreateCluster: %w", err)
		}
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}

	got, err := client.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	kcc := got.GetVirtualClusterConfig().GetKubernetesClusterConfig()
	if ns := kcc.GetKubernetesNamespace(); ns != id {
		return fmt.Errorf("kubernetesNamespace = %q, want the cluster name %q", ns, id)
	}
	if v := kcc.GetKubernetesSoftwareConfig().GetComponentVersion()["SPARK"]; v != "3.5-dataproc-28" {
		return fmt.Errorf("componentVersion[SPARK] = %q, want the dereferenced release 3.5-dataproc-28", v)
	}

	// The patch that changes virtualClusterConfig is rejected.
	_, err = client.UpdateCluster(ctx, &dataprocpb.UpdateClusterRequest{
		ProjectId:   cfg.Project,
		Region:      dataprocRegion,
		ClusterName: id,
		Cluster:     &dataprocpb.Cluster{ClusterName: id, VirtualClusterConfig: gkeVirtualClusterConfig(dpGkeTarget(cfg))},
		UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"virtual_cluster_config"}},
	})
	if status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("virtualClusterConfig patch: got %v, want InvalidArgument", err)
	}
	if !strings.Contains(status.Convert(err).Message(), "recreate-only") {
		return fmt.Errorf("virtualClusterConfig patch message %q does not explain the recreate-only rule", status.Convert(err).Message())
	}

	// A labels-only patch is still allowed.
	upd, err := client.UpdateCluster(ctx, &dataprocpb.UpdateClusterRequest{
		ProjectId:   cfg.Project,
		Region:      dataprocRegion,
		ClusterName: id,
		Cluster:     &dataprocpb.Cluster{ClusterName: id, Labels: map[string]string{"probe": "true"}},
		UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("labels patch: %w", err)
	}
	if _, err := upd.Wait(ctx); err != nil {
		return fmt.Errorf("labels patch Wait: %w", err)
	}
	got, err = client.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("GetCluster after patch: %w", err)
	}
	if got.GetLabels()["probe"] != "true" {
		return fmt.Errorf("labels patch not applied: %v", got.GetLabels())
	}
	if got.GetConfig() != nil {
		return fmt.Errorf("labels patch invented a GCE config: %+v", got.GetConfig())
	}
	return nil
}

// Check 15c: the image-line alias `dataproc-<major.minor>` is a documented Spark
// version form, and the only line real GCP accepts for a virtual cluster is
// `dataproc-2.2`, which dereferences to the newest engine release — it resolves
// exactly like `latest` (recorded from real GCP, DPG36). Any other image line
// stays InvalidArgument rather than being mapped by inference.
func checkDPGKEImageVersionAlias(ctx context.Context, cfg Config) error {
	if _, err := ensureGKEPrereqCluster(ctx, cfg, "gcpc-gke"); err != nil {
		return err
	}
	client, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	vccWithVersion := func(version string) *dataprocpb.VirtualClusterConfig {
		vcc := gkeVirtualClusterConfig(dpGkeTarget(cfg))
		vcc.GetKubernetesClusterConfig().KubernetesSoftwareConfig = &dataprocpb.KubernetesSoftwareConfig{
			ComponentVersion: map[string]string{"SPARK": version},
		}
		return vcc
	}

	id := cfg.ResourceName("gcpc-grpc-dp-vccimg")
	op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id, VirtualClusterConfig: vccWithVersion("dataproc-2.2")},
	})
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("CreateCluster(dataproc-2.2): %w", err)
		}
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	got, err := client.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if v := got.GetVirtualClusterConfig().GetKubernetesClusterConfig().GetKubernetesSoftwareConfig().GetComponentVersion()["SPARK"]; v != "3.5-dataproc-28" {
		return fmt.Errorf("componentVersion[SPARK] = %q, want dataproc-2.2 dereferenced to 3.5-dataproc-28", v)
	}

	// An image line real GCP does not accept is not invented here either.
	_, err = client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: cfg.ResourceName("gcpc-grpc-dp-vccimg-bad"), VirtualClusterConfig: vccWithVersion("dataproc-2.3")},
	})
	if status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("CreateCluster(dataproc-2.3): got %v, want InvalidArgument", err)
	}
	return nil
}

// ensureGKEPrereqCluster creates a run-unique standard, Workload-Identity-enabled
// GKE cluster through the official ClusterManager client and returns its
// canonical name. AlreadyExists is success so repeated runs are idempotent.
func ensureGKEPrereqCluster(ctx context.Context, cfg Config, prefix string) (string, error) {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return "", fmt.Errorf("new container client: %w", err)
	}
	defer client.Close()
	name := cfg.ResourceName(prefix)
	op, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent: containerParent(cfg),
		Cluster: &containerpb.Cluster{
			Name:                   name,
			InitialNodeCount:       1,
			WorkloadIdentityConfig: &containerpb.WorkloadIdentityConfig{WorkloadPool: cfg.Project + ".svc.id.goog"},
		},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return containerNameRaw(cfg, name), nil
		}
		return "", fmt.Errorf("CreateCluster: %w", err)
	}
	// The emulator completes GKE operations inline (like real GKE's fast path),
	// so a DONE operation is the create's completion signal.
	if op.GetStatus() != containerpb.Operation_DONE {
		return "", fmt.Errorf("CreateCluster operation status = %v, want DONE", op.GetStatus())
	}
	return containerNameRaw(cfg, name), nil
}

// gkePoolTargets is the two-pool target set the cross-API probes use: one pool
// the caller shapes (config + autoscaler + zone) and one named-only pool
// ("any node pool with the specified name is used").
func gkePoolTargets(target string) []*dataprocpb.GkeNodePoolTarget {
	return []*dataprocpb.GkeNodePoolTarget{
		{
			NodePool: target + "/nodePools/shaped-pool",
			Roles: []dataprocpb.GkeNodePoolTarget_Role{
				dataprocpb.GkeNodePoolTarget_DEFAULT,
				dataprocpb.GkeNodePoolTarget_CONTROLLER,
			},
			NodePoolConfig: &dataprocpb.GkeNodePoolConfig{
				Config:      &dataprocpb.GkeNodePoolConfig_GkeNodeConfig{MachineType: "n1-standard-4", LocalSsdCount: 1},
				Autoscaling: &dataprocpb.GkeNodePoolConfig_GkeNodePoolAutoscalingConfig{MinNodeCount: 1, MaxNodeCount: 4},
				Locations:   []string{dataprocRegion + "-a"},
			},
		},
		{
			NodePool: target + "/nodePools/named-pool",
			Roles: []dataprocpb.GkeNodePoolTarget_Role{
				dataprocpb.GkeNodePoolTarget_SPARK_DRIVER,
				dataprocpb.GkeNodePoolTarget_SPARK_EXECUTOR,
			},
		},
	}
}

// gkePoolVirtualCluster builds a virtual cluster naming pools on target.
func gkePoolVirtualCluster(cfg Config, target string, pools []*dataprocpb.GkeNodePoolTarget) *dataprocpb.VirtualClusterConfig {
	return &dataprocpb.VirtualClusterConfig{
		StagingBucket: "dataproc-staging-" + cfg.Project,
		InfrastructureConfig: &dataprocpb.VirtualClusterConfig_KubernetesClusterConfig{
			KubernetesClusterConfig: &dataprocpb.KubernetesClusterConfig{
				Config: &dataprocpb.KubernetesClusterConfig_GkeClusterConfig{
					GkeClusterConfig: &dataprocpb.GkeClusterConfig{
						GkeClusterTarget: target,
						NodePoolTarget:   pools,
					},
				},
				KubernetesSoftwareConfig: &dataprocpb.KubernetesSoftwareConfig{
					ComponentVersion: map[string]string{"SPARK": "3.5"},
				},
			},
		},
	}
}

// checkDPGKEVirtualClusterPools is the cross-API case users actually exercise:
// a virtual cluster created through dataproc.v1 names two pools, and both are
// visible through the GKE ClusterManager client with the requested shape, the
// Dataproc auto labels and the documented autoscaler defaults; deleting the
// virtual cluster leaves the pools in place (real GCP does not delete them).
func checkDPGKEVirtualClusterPools(ctx context.Context, cfg Config) error {
	target, err := ensureGKEPrereqCluster(ctx, cfg, "gcpc-grpc-dp-gkepools")
	if err != nil {
		return err
	}
	dp, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new dataproc client: %w", err)
	}
	defer dp.Close()
	gke, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new container client: %w", err)
	}
	defer gke.Close()

	id := cfg.ResourceName("gcpc-grpc-dp-vcppools")
	op, err := dp.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster: &dataprocpb.Cluster{
			ClusterName:          id,
			VirtualClusterConfig: gkePoolVirtualCluster(cfg, target, gkePoolTargets(target)),
		},
	})
	if err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}

	pools, err := listGKEPools(ctx, gke, target)
	if err != nil {
		return err
	}
	shaped, ok := pools["shaped-pool"]
	if !ok {
		return fmt.Errorf("shaped-pool not created through the GKE API: %v", poolNames(pools))
	}
	if got := shaped.GetConfig().GetMachineType(); got != "n1-standard-4" {
		return fmt.Errorf("shaped-pool machineType = %q", got)
	}
	if got := shaped.GetConfig().GetLocalSsdCount(); got != 1 {
		return fmt.Errorf("shaped-pool localSsdCount = %d", got)
	}
	if got := shaped.GetAutoscaling().GetMaxNodeCount(); got != 4 {
		return fmt.Errorf("shaped-pool autoscaling.maxNodeCount = %d, want 4", got)
	}
	if got := shaped.GetLocations(); len(got) != 1 || got[0] != dataprocRegion+"-a" {
		return fmt.Errorf("shaped-pool locations = %v", got)
	}
	labels := shaped.GetConfig().GetLabels()
	if labels["goog-dataproc-cluster-name"] != id {
		return fmt.Errorf("shaped-pool goog-dataproc-cluster-name = %q, want %q", labels["goog-dataproc-cluster-name"], id)
	}
	if labels["goog-dataproc-location"] != dataprocRegion {
		return fmt.Errorf("shaped-pool goog-dataproc-location = %q", labels["goog-dataproc-location"])
	}
	if labels["goog-dataproc-cluster-uuid"] == "" {
		return fmt.Errorf("shaped-pool has no goog-dataproc-cluster-uuid label: %v", labels)
	}
	named, ok := pools["named-pool"]
	if !ok {
		return fmt.Errorf("named-pool not created through the GKE API: %v", poolNames(pools))
	}
	if got := named.GetAutoscaling(); got.GetMinNodeCount() != 1 || got.GetMaxNodeCount() != 10 {
		return fmt.Errorf("named-pool autoscaler = %d/%d, want the 1/10 defaults", got.GetMinNodeCount(), got.GetMaxNodeCount())
	}

	// The virtual cluster's read never echoes nodePoolConfig: it is input-only.
	got, err := dp.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	for _, t := range got.GetVirtualClusterConfig().GetKubernetesClusterConfig().GetGkeClusterConfig().GetNodePoolTarget() {
		if t.GetNodePoolConfig() != nil {
			return fmt.Errorf("nodePoolTarget %s echoes nodePoolConfig %v: it is input-only", t.GetNodePool(), t.GetNodePoolConfig())
		}
	}

	// Pools outlive the virtual cluster: delete it, then read the pools again.
	del, err := dp.DeleteCluster(ctx, &dataprocpb.DeleteClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("DeleteCluster: %w", err)
	}
	if err := del.Wait(ctx); err != nil {
		return fmt.Errorf("delete Wait: %w", err)
	}
	for _, pool := range []string{"shaped-pool", "named-pool"} {
		if _, err := gke.GetNodePool(ctx, &containerpb.GetNodePoolRequest{
			Name: target + "/nodePools/" + pool,
		}); err != nil {
			return fmt.Errorf("node pool %s did not survive the virtual cluster delete: %w", pool, err)
		}
	}
	return nil
}

// checkDPGKEVirtualClusterPoolReuse proves the pools belong to the GKE cluster,
// not the virtual cluster: a second virtual cluster naming the same pools adopts
// them, so the pool set through the GKE API does not grow.
func checkDPGKEVirtualClusterPoolReuse(ctx context.Context, cfg Config) error {
	target, err := ensureGKEPrereqCluster(ctx, cfg, "gcpc-grpc-dp-gkereuse")
	if err != nil {
		return err
	}
	dp, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new dataproc client: %w", err)
	}
	defer dp.Close()
	gke, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new container client: %w", err)
	}
	defer gke.Close()

	create := func(name string) error {
		op, err := dp.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
			ProjectId: cfg.Project,
			Region:    dataprocRegion,
			Cluster: &dataprocpb.Cluster{
				ClusterName:          name,
				VirtualClusterConfig: gkePoolVirtualCluster(cfg, target, gkePoolTargets(target)),
			},
		})
		if err != nil {
			return fmt.Errorf("CreateCluster %s: %w", name, err)
		}
		if _, err := op.Wait(ctx); err != nil {
			return fmt.Errorf("CreateCluster %s wait: %w", name, err)
		}
		return nil
	}
	// The two virtual clusters are created sequentially: the second one adopts
	// the pools the first reconciled.
	first := cfg.ResourceName("gcpc-grpc-dp-gkea")
	if err := create(first); err != nil {
		return err
	}
	before, err := listGKEPools(ctx, gke, target)
	if err != nil {
		return err
	}
	second := cfg.ResourceName("gcpc-grpc-dp-gkeb")
	if err := create(second); err != nil {
		return err
	}
	after, err := listGKEPools(ctx, gke, target)
	if err != nil {
		return err
	}
	if len(after) != len(before) {
		return fmt.Errorf("second virtual cluster duplicated pools: %v -> %v", poolNames(before), poolNames(after))
	}
	for _, pool := range []string{"shaped-pool", "named-pool"} {
		if _, ok := after[pool]; !ok {
			return fmt.Errorf("pool %s disappeared after the second virtual cluster: %v", pool, poolNames(after))
		}
	}
	return nil
}

// checkDPGKEVirtualClusterRules asserts the documented GKE placement/pool rules
// as client-visible gRPC codes, and the namespace-ownership conflict.
func checkDPGKEVirtualClusterRules(ctx context.Context, cfg Config) error {
	target, err := ensureGKEPrereqCluster(ctx, cfg, "gcpc-grpc-dp-gkerules")
	if err != nil {
		return err
	}
	dp, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new dataproc client: %w", err)
	}
	defer dp.Close()

	missing := "projects/" + cfg.Project + "/locations/" + dataprocRegion + "/clusters/" + cfg.ResourceName("gcpc-grpc-missing-gke")
	cases := []struct {
		name  string
		pools []*dataprocpb.GkeNodePoolTarget
		tgt   string
		code  codes.Code
		text  string
	}{
		{
			// A bare pool id is not a resource name: real GCP rejects it with the
			// documented pattern (DPG44).
			name: "short node pool name",
			pools: []*dataprocpb.GkeNodePoolTarget{{
				NodePool: "bare-pool",
				Roles:    []dataprocpb.GkeNodePoolTarget_Role{dataprocpb.GkeNodePoolTarget_DEFAULT},
			}},
			code: codes.InvalidArgument, text: "must conform to pattern",
		},
		{
			name: "missing DEFAULT role",
			pools: []*dataprocpb.GkeNodePoolTarget{{
				NodePool: target + "/nodePools/driver-only",
				Roles:    []dataprocpb.GkeNodePoolTarget_Role{dataprocpb.GkeNodePoolTarget_SPARK_DRIVER},
			}},
			code: codes.InvalidArgument, text: "DEFAULT",
		},
		{
			name: "two pools claim one role",
			pools: []*dataprocpb.GkeNodePoolTarget{
				{NodePool: target + "/nodePools/role-a", Roles: []dataprocpb.GkeNodePoolTarget_Role{dataprocpb.GkeNodePoolTarget_DEFAULT}},
				{NodePool: target + "/nodePools/role-b", Roles: []dataprocpb.GkeNodePoolTarget_Role{dataprocpb.GkeNodePoolTarget_DEFAULT}},
			},
			code: codes.InvalidArgument, text: "only one nodePoolTarget",
		},
		{
			name: "pool outside the region",
			pools: []*dataprocpb.GkeNodePoolTarget{{
				NodePool: target + "/nodePools/wrong-zone",
				Roles:    []dataprocpb.GkeNodePoolTarget_Role{dataprocpb.GkeNodePoolTarget_DEFAULT},
				NodePoolConfig: &dataprocpb.GkeNodePoolConfig{
					Locations: []string{"us-east1-b"},
				},
			}},
			code: codes.InvalidArgument, text: "not a zone in region",
		},
		{
			name:  "unknown GKE cluster",
			pools: nil,
			tgt:   missing,
			code:  codes.NotFound, text: "",
		},
	}
	for i, tc := range cases {
		tgt := tc.tgt
		if tgt == "" {
			tgt = target
		}
		_, err := dp.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
			ProjectId: cfg.Project,
			Region:    dataprocRegion,
			Cluster: &dataprocpb.Cluster{
				ClusterName:          cfg.ResourceName(fmt.Sprintf("gcpc-grpc-dp-gkerule-%d", i)),
				VirtualClusterConfig: gkePoolVirtualCluster(cfg, tgt, tc.pools),
			},
		})
		if err == nil {
			// A rejected request is synchronous; a nil error means it was
			// accepted, which is the bug this probe guards.
			return fmt.Errorf("%s: CreateCluster was accepted, want %v", tc.name, tc.code)
		}
		if status.Code(err) != tc.code {
			return fmt.Errorf("%s: got %v (%v), want %v", tc.name, status.Code(err), err, tc.code)
		}
		if tc.text != "" && !strings.Contains(status.Convert(err).Message(), tc.text) {
			return fmt.Errorf("%s: message %q does not mention %q", tc.name, status.Convert(err).Message(), tc.text)
		}
	}

	// A kubernetesNamespace is owned by one live virtual cluster at a time.
	namespace := cfg.ResourceName("gcpc-dp-ns")
	owned := cfg.ResourceName("gcpc-grpc-dp-gkens-a")
	vcc := gkePoolVirtualCluster(cfg, target, gkePoolTargets(target))
	vcc.GetKubernetesClusterConfig().KubernetesNamespace = namespace
	op, err := dp.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: owned, VirtualClusterConfig: vcc},
	})
	if err != nil {
		return fmt.Errorf("CreateCluster %s: %w", owned, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("CreateCluster %s wait: %w", owned, err)
	}
	duplicate := gkePoolVirtualCluster(cfg, target, gkePoolTargets(target))
	duplicate.GetKubernetesClusterConfig().KubernetesNamespace = namespace
	_, err = dp.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: cfg.ResourceName("gcpc-grpc-dp-gkens-b"), VirtualClusterConfig: duplicate},
	})
	if status.Code(err) != codes.FailedPrecondition {
		return fmt.Errorf("duplicate kubernetesNamespace: got %v, want FailedPrecondition", err)
	}
	if !strings.Contains(status.Convert(err).Message(), "already used by Dataproc virtual cluster") {
		return fmt.Errorf("duplicate namespace message %q does not name the owning cluster", status.Convert(err).Message())
	}
	return nil
}

// checkDPGKEVirtualClusterPoolShapeConflict is the gRPC half of the ERROR-status
// path: a nodePoolConfig that disagrees with an existing pool does not reject the
// request, it fails the provisioned cluster. The create's LRO completes
// successfully and its response cluster is persisted in ERROR with a
// status.detail naming the pool and the field that differed, the documented
// shape (README-GCP.md §Dataproc workload placement, wire-contract §5). The REST
// half is TestSDKDataprocOnGKEConflictingPoolShape in the sdk-dataproc suite.
func checkDPGKEVirtualClusterPoolShapeConflict(ctx context.Context, cfg Config) error {
	target, err := ensureGKEPrereqCluster(ctx, cfg, "gcpc-grpc-dp-gkeconflict")
	if err != nil {
		return err
	}
	dp, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new dataproc client: %w", err)
	}
	defer dp.Close()
	gke, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new container client: %w", err)
	}
	defer gke.Close()

	// An existing pool whose shape the virtual cluster's nodePoolConfig
	// contradicts. It is created through the GKE API, so the mismatch is a
	// verification failure rather than a create-time validation error.
	pool := "taken-pool"
	if _, err := gke.CreateNodePool(ctx, &containerpb.CreateNodePoolRequest{
		Parent: target,
		NodePool: &containerpb.NodePool{
			Name:             pool,
			InitialNodeCount: 1,
			Config:           &containerpb.NodeConfig{MachineType: "e2-medium"},
		},
	}); err != nil {
		return fmt.Errorf("CreateNodePool: %w", err)
	}

	id := cfg.ResourceName("gcpc-grpc-dp-vccconflict")
	op, err := dp.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster: &dataprocpb.Cluster{
			ClusterName: id,
			VirtualClusterConfig: gkePoolVirtualCluster(cfg, target, []*dataprocpb.GkeNodePoolTarget{{
				NodePool: target + "/nodePools/" + pool,
				Roles:    []dataprocpb.GkeNodePoolTarget_Role{dataprocpb.GkeNodePoolTarget_DEFAULT},
				NodePoolConfig: &dataprocpb.GkeNodePoolConfig{
					Config: &dataprocpb.GkeNodePoolConfig_GkeNodeConfig{MachineType: "n1-standard-4"},
				},
			}}),
		},
	})
	if err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	// The reconciliation failure lands on the cluster, not on the operation:
	// Wait must succeed and still return the ERROR cluster.
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w (the pool-shape failure belongs on the cluster, not the LRO)", err)
	}
	if got := cluster.GetStatus().GetState(); got != dataprocpb.ClusterStatus_ERROR {
		return fmt.Errorf("cluster state = %v, want ERROR", got)
	}
	detail := cluster.GetStatus().GetDetail()
	if !strings.Contains(detail, pool) {
		return fmt.Errorf("status.detail %q does not name the pool %q", detail, pool)
	}
	if !strings.Contains(detail, "machineType") {
		return fmt.Errorf("status.detail %q does not name the differing field machineType", detail)
	}
	return nil
}

// listGKEPools lists target's pools keyed by pool id.
func listGKEPools(ctx context.Context, client *container.ClusterManagerClient, target string) (map[string]*containerpb.NodePool, error) {
	resp, err := client.ListNodePools(ctx, &containerpb.ListNodePoolsRequest{Parent: target})
	if err != nil {
		return nil, fmt.Errorf("ListNodePools: %w", err)
	}
	out := map[string]*containerpb.NodePool{}
	for _, p := range resp.GetNodePools() {
		out[p.GetName()] = p
	}
	return out, nil
}

func poolNames(pools map[string]*containerpb.NodePool) []string {
	out := make([]string, 0, len(pools))
	for _, p := range pools {
		out = append(out, p.GetName())
	}
	return out
}

// Check 16: a cluster's auxiliaryServicesConfig.metastoreConfig round-trips over
// gRPC. The attachment is validated against the Metastore control plane at
// create, so a run-unique service is created first.
func checkDPMetastoreAttachment(ctx context.Context, cfg Config) error {
	mc, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new metastore client: %w", err)
	}
	defer mc.Close()
	if _, err := ensureGKEPrereqCluster(ctx, cfg, "gcpc-gke"); err != nil {
		return err
	}
	svcName, err := ensureMetastoreService(ctx, mc, cfg)
	if err != nil {
		return err
	}

	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new dataproc client: %w", err)
	}
	defer cc.Close()

	id := cfg.ResourceName("gcpc-grpc-dp-hms")
	vcc := gkeVirtualClusterConfig(dpGkeTarget(cfg))
	vcc.AuxiliaryServicesConfig = &dataprocpb.AuxiliaryServicesConfig{
		MetastoreConfig: &dataprocpb.MetastoreConfig{DataprocMetastoreService: svcName},
	}
	op, err := cc.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id, VirtualClusterConfig: vcc},
	})
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("CreateCluster: %w", err)
		}
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	got, err := cc.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	ref := got.GetVirtualClusterConfig().GetAuxiliaryServicesConfig().GetMetastoreConfig().GetDataprocMetastoreService()
	if ref != svcName {
		return fmt.Errorf("metastore attachment = %q, want %q", ref, svcName)
	}
	return nil
}

// checkDPHistoryServerAttachment covers
// virtualClusterConfig.auxiliaryServicesConfig.sparkHistoryServerConfig: the
// referenced Dataproc cluster must exist (a missing one is NotFound) and a
// successful create returns the documented spark.eventLog.* workload
// properties. The PHS source is an ordinary Dataproc cluster, so it is created
// first (run-uniquely, AlreadyExists tolerated on a long-lived emulator).
func checkDPHistoryServerAttachment(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new dataproc client: %w", err)
	}
	defer cc.Close()

	phsID := cfg.ResourceName("gcpc-grpc-dp-phs-src")
	op, err := cc.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster: &dataprocpb.Cluster{
			ClusterName: phsID,
			Config: &dataprocpb.ClusterConfig{
				SoftwareConfig: &dataprocpb.SoftwareConfig{ImageVersion: "2.2.0"},
			},
		},
	})
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("create PHS source cluster: %w", err)
		}
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("PHS source operation Wait: %w", err)
	}
	phsRef := "projects/" + cfg.Project + "/regions/" + dataprocRegion + "/clusters/" + phsID

	// A reference to a cluster that does not exist is NotFound.
	missing := gkeVirtualClusterConfig(dpGkeTarget(cfg))
	missing.AuxiliaryServicesConfig = &dataprocpb.AuxiliaryServicesConfig{
		SparkHistoryServerConfig: &dataprocpb.SparkHistoryServerConfig{
			DataprocCluster: "projects/" + cfg.Project + "/regions/" + dataprocRegion +
				"/clusters/" + cfg.ResourceName("gcpc-grpc-dp-phs-missing"),
		},
	}
	if _, err := cc.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: cfg.ResourceName("gcpc-grpc-dp-phs-bad"), VirtualClusterConfig: missing},
	}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("missing PHS cluster: got %v, want NotFound", err)
	}

	id := cfg.ResourceName("gcpc-grpc-dp-phs")
	vcc := gkeVirtualClusterConfig(dpGkeTarget(cfg))
	vcc.AuxiliaryServicesConfig = &dataprocpb.AuxiliaryServicesConfig{
		SparkHistoryServerConfig: &dataprocpb.SparkHistoryServerConfig{DataprocCluster: phsRef},
	}
	op, err = cc.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id, VirtualClusterConfig: vcc},
	})
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("CreateCluster: %w", err)
		}
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	got, err := cc.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: id})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if ref := got.GetVirtualClusterConfig().GetAuxiliaryServicesConfig().GetSparkHistoryServerConfig().GetDataprocCluster(); ref != phsRef {
		return fmt.Errorf("spark history server ref = %q, want %q", ref, phsRef)
	}
	props := got.GetVirtualClusterConfig().GetKubernetesClusterConfig().GetKubernetesSoftwareConfig().GetProperties()
	if props["spark:spark.eventLog.enabled"] != "true" {
		return fmt.Errorf("spark.eventLog.enabled = %q, want true", props["spark:spark.eventLog.enabled"])
	}
	wantDir := "gs://dataproc-staging-probe/" + id + "/spark-job-history"
	if props["spark:spark.eventLog.dir"] != wantDir {
		return fmt.Errorf("spark.eventLog.dir = %q, want %q", props["spark:spark.eventLog.dir"], wantDir)
	}
	return nil
}

// Check 17: a submitted job's placement.clusterUuid is the cluster's
// output-only UUID.
func checkDPJobPlacementClusterUUID(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	cl, err := cc.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: cfg.Project, Region: dataprocRegion, ClusterName: cluster})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if cl.GetClusterUuid() == "" {
		return fmt.Errorf("cluster %q has no clusterUuid", cluster)
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-uuid")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	got, err := jc.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, JobId: jobID})
	if err != nil {
		return fmt.Errorf("GetJob: %w", err)
	}
	if uuid := got.GetPlacement().GetClusterUuid(); uuid != cl.GetClusterUuid() {
		return fmt.Errorf("placement.clusterUuid = %q, want %q", uuid, cl.GetClusterUuid())
	}
	return nil
}

// Check 18: a terminal job advertises non-empty driver output/control URIs.
func checkDPDriverOutputURIs(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-out")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	final, err := dpPollJobTerminal(ctx, jc, cfg.Project, jobID)
	if err != nil {
		return err
	}
	if final.GetDriverOutputResourceUri() == "" || final.GetDriverControlFilesUri() == "" {
		return fmt.Errorf("terminal job driver URIs = %q / %q, want non-empty",
			final.GetDriverOutputResourceUri(), final.GetDriverControlFilesUri())
	}
	return nil
}

// Check 14: DeleteJob removes a terminal job.
func checkDPDeleteJob(ctx context.Context, cfg Config) error {
	cc, err := newDataprocClusterClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cc.Close()
	cluster, err := ensureDataprocCluster(ctx, cc, cfg)
	if err != nil {
		return err
	}
	jc, err := newDataprocJobClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer jc.Close()
	jobID := cfg.ResourceName("gcpc-grpc-dp-job-del")
	if _, err := jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, Job: dataprocJob(jobID, cluster)}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	// Only a terminal job can be deleted; poll it to DONE first.
	if _, err := dpPollJobTerminal(ctx, jc, cfg.Project, jobID); err != nil {
		return err
	}
	if err := jc.DeleteJob(ctx, &dataprocpb.DeleteJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, JobId: jobID}); err != nil {
		return fmt.Errorf("DeleteJob: %w", err)
	}
	if _, err := jc.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: cfg.Project, Region: dataprocRegion, JobId: jobID}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetJob after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── Workflow templates (DPW1) ───────────────────────────────────────────────

// dataprocWorkflowParent is the workflowTemplates parent
// (projects/{project}/regions/{region}).
func dataprocWorkflowParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/regions/%s", cfg.Project, dataprocRegion)
}

func newDataprocWorkflowTemplateClient(ctx context.Context, cfg Config) (*dataproc.WorkflowTemplateClient, error) {
	return dataproc.NewWorkflowTemplateClient(ctx, dataprocClientOptions(cfg)...)
}

// dataprocWorkflowTemplate builds a minimal inline workflow template.
func dataprocWorkflowTemplate(id string) *dataprocpb.WorkflowTemplate {
	return &dataprocpb.WorkflowTemplate{
		Id: id,
		Placement: &dataprocpb.WorkflowTemplatePlacement{
			Placement: &dataprocpb.WorkflowTemplatePlacement_ManagedCluster{
				ManagedCluster: &dataprocpb.ManagedCluster{ClusterName: "gcpc-wf-probe-cluster"},
			},
		},
		Jobs: []*dataprocpb.OrderedJob{{
			StepId:  "a",
			JobType: &dataprocpb.OrderedJob_PysparkJob{PysparkJob: &dataprocpb.PySparkJob{MainPythonFileUri: "gs://bucket/main.py"}},
		}},
	}
}

// Check 19: CreateWorkflowTemplate round-trips a template.
func checkDPCreateWorkflowTemplate(ctx context.Context, cfg Config) error {
	client, err := newDataprocWorkflowTemplateClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-dpwf")
	got, err := client.CreateWorkflowTemplate(ctx, &dataprocpb.CreateWorkflowTemplateRequest{
		Parent:   dataprocWorkflowParent(cfg),
		Template: dataprocWorkflowTemplate(id),
	})
	if err != nil {
		return fmt.Errorf("CreateWorkflowTemplate: %w", err)
	}
	wantName := dataprocWorkflowParent(cfg) + "/workflowTemplates/" + id
	if got.GetName() != wantName || got.GetId() != id || got.GetVersion() != 1 {
		return fmt.Errorf("created template = name %q id %q version %d, want %q/%q/1",
			got.GetName(), got.GetId(), got.GetVersion(), wantName, id)
	}
	return nil
}

// Check 20: GetWorkflowTemplate round-trips the job definition.
func checkDPGetWorkflowTemplate(ctx context.Context, cfg Config) error {
	client, err := newDataprocWorkflowTemplateClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-dpwf-get")
	if _, err := client.CreateWorkflowTemplate(ctx, &dataprocpb.CreateWorkflowTemplateRequest{
		Parent: dataprocWorkflowParent(cfg), Template: dataprocWorkflowTemplate(id),
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateWorkflowTemplate: %w", err)
	}
	got, err := client.GetWorkflowTemplate(ctx, &dataprocpb.GetWorkflowTemplateRequest{
		Name: dataprocWorkflowParent(cfg) + "/workflowTemplates/" + id,
	})
	if err != nil {
		return fmt.Errorf("GetWorkflowTemplate: %w", err)
	}
	if len(got.GetJobs()) != 1 || got.GetJobs()[0].GetStepId() != "a" {
		return fmt.Errorf("jobs = %+v", got.GetJobs())
	}
	return nil
}

// Check 21: ListWorkflowTemplates includes the created template.
func checkDPListWorkflowTemplates(ctx context.Context, cfg Config) error {
	client, err := newDataprocWorkflowTemplateClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-dpwf-list")
	if _, err := client.CreateWorkflowTemplate(ctx, &dataprocpb.CreateWorkflowTemplateRequest{
		Parent: dataprocWorkflowParent(cfg), Template: dataprocWorkflowTemplate(id),
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateWorkflowTemplate: %w", err)
	}
	it := client.ListWorkflowTemplates(ctx, &dataprocpb.ListWorkflowTemplatesRequest{Parent: dataprocWorkflowParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListWorkflowTemplates did not include %q", id)
		}
		if err != nil {
			return fmt.Errorf("ListWorkflowTemplates: %w", err)
		}
		if got.GetId() == id {
			return nil
		}
	}
}

// Check 22: UpdateWorkflowTemplate bumps the version.
func checkDPUpdateWorkflowTemplate(ctx context.Context, cfg Config) error {
	client, err := newDataprocWorkflowTemplateClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-dpwf-upd")
	name := dataprocWorkflowParent(cfg) + "/workflowTemplates/" + id
	if _, err := client.CreateWorkflowTemplate(ctx, &dataprocpb.CreateWorkflowTemplateRequest{
		Parent: dataprocWorkflowParent(cfg), Template: dataprocWorkflowTemplate(id),
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateWorkflowTemplate: %w", err)
	}
	tmpl := dataprocWorkflowTemplate(id)
	tmpl.Name = name
	tmpl.Version = 1
	got, err := client.UpdateWorkflowTemplate(ctx, &dataprocpb.UpdateWorkflowTemplateRequest{Template: tmpl})
	if err != nil {
		return fmt.Errorf("UpdateWorkflowTemplate: %w", err)
	}
	if got.GetVersion() != 2 {
		return fmt.Errorf("version = %d, want 2", got.GetVersion())
	}
	return nil
}

// Check 23: DeleteWorkflowTemplate removes the template.
func checkDPDeleteWorkflowTemplate(ctx context.Context, cfg Config) error {
	client, err := newDataprocWorkflowTemplateClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-dpwf-del")
	name := dataprocWorkflowParent(cfg) + "/workflowTemplates/" + id
	if _, err := client.CreateWorkflowTemplate(ctx, &dataprocpb.CreateWorkflowTemplateRequest{
		Parent: dataprocWorkflowParent(cfg), Template: dataprocWorkflowTemplate(id),
	}); err != nil {
		return fmt.Errorf("CreateWorkflowTemplate: %w", err)
	}
	if err := client.DeleteWorkflowTemplate(ctx, &dataprocpb.DeleteWorkflowTemplateRequest{Name: name}); err != nil {
		return fmt.Errorf("DeleteWorkflowTemplate: %w", err)
	}
	if _, err := client.GetWorkflowTemplate(ctx, &dataprocpb.GetWorkflowTemplateRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetWorkflowTemplate after delete = %v, want NotFound", err)
	}
	return nil
}

// managedClusterPlacement builds a workflow placement that owns a managed
// cluster of the given name.
func managedClusterPlacement(cluster string) *dataprocpb.WorkflowTemplatePlacement {
	return &dataprocpb.WorkflowTemplatePlacement{
		Placement: &dataprocpb.WorkflowTemplatePlacement_ManagedCluster{
			ManagedCluster: &dataprocpb.ManagedCluster{ClusterName: cluster},
		},
	}
}

// Check 24: InstantiateInlineWorkflowTemplate runs an inline DAG to DONE.
func checkDPInstantiateInlineWorkflowTemplate(ctx context.Context, cfg Config) error {
	client, err := newDataprocWorkflowTemplateClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	tmpl := &dataprocpb.WorkflowTemplate{
		Placement: managedClusterPlacement(cfg.ResourceName("gcpc-grpc-dpwf-inline-cluster")),
		Jobs: []*dataprocpb.OrderedJob{{
			StepId:  "a",
			JobType: &dataprocpb.OrderedJob_PysparkJob{PysparkJob: &dataprocpb.PySparkJob{MainPythonFileUri: "gs://bucket/main.py"}},
		}},
	}
	op, err := client.InstantiateInlineWorkflowTemplate(ctx, &dataprocpb.InstantiateInlineWorkflowTemplateRequest{
		Parent: dataprocWorkflowParent(cfg), Template: tmpl,
	})
	if err != nil {
		return fmt.Errorf("InstantiateInlineWorkflowTemplate: %w", err)
	}
	if op.Done() {
		return fmt.Errorf("instantiate completed inline; want an in-flight LRO")
	}
	for i := 0; i < 40 && !op.Done(); i++ {
		if err := op.Poll(ctx); err != nil {
			return fmt.Errorf("poll workflow: %w", err)
		}
	}
	if !op.Done() {
		return fmt.Errorf("inline workflow did not complete")
	}
	meta, err := op.Metadata()
	if err != nil {
		return fmt.Errorf("workflow metadata: %w", err)
	}
	if meta.GetState() != dataprocpb.WorkflowMetadata_DONE {
		return fmt.Errorf("workflow state = %v, want DONE", meta.GetState())
	}
	nodes := meta.GetGraph().GetNodes()
	if len(nodes) != 1 || nodes[0].GetState() != dataprocpb.WorkflowNode_COMPLETED {
		return fmt.Errorf("workflow nodes = %+v", nodes)
	}
	return nil
}

// Check 25: InstantiateWorkflowTemplate runs a stored template with parameters.
func checkDPInstantiateWorkflowTemplate(ctx context.Context, cfg Config) error {
	client, err := newDataprocWorkflowTemplateClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-dpwf-inst")
	name := dataprocWorkflowParent(cfg) + "/workflowTemplates/" + id
	tmpl := dataprocWorkflowTemplate(id)
	tmpl.Placement = managedClusterPlacement(cfg.ResourceName("gcpc-grpc-dpwf-inst-cluster"))
	tmpl.Parameters = []*dataprocpb.TemplateParameter{{Name: "bucket", Fields: []string{"jobs[*].pysparkJob.mainPythonFileUri"}}}
	tmpl.Jobs[0].JobType = &dataprocpb.OrderedJob_PysparkJob{PysparkJob: &dataprocpb.PySparkJob{MainPythonFileUri: "gs://{bucket}/main.py"}}
	if _, err := client.CreateWorkflowTemplate(ctx, &dataprocpb.CreateWorkflowTemplateRequest{Parent: dataprocWorkflowParent(cfg), Template: tmpl}); err != nil {
		return fmt.Errorf("CreateWorkflowTemplate: %w", err)
	}
	op, err := client.InstantiateWorkflowTemplate(ctx, &dataprocpb.InstantiateWorkflowTemplateRequest{
		Name: name, Parameters: map[string]string{"bucket": "probe-bucket"},
	})
	if err != nil {
		return fmt.Errorf("InstantiateWorkflowTemplate: %w", err)
	}
	for i := 0; i < 40 && !op.Done(); i++ {
		if err := op.Poll(ctx); err != nil {
			return fmt.Errorf("poll workflow: %w", err)
		}
	}
	if !op.Done() {
		return fmt.Errorf("stored workflow did not complete")
	}
	meta, err := op.Metadata()
	if err != nil {
		return fmt.Errorf("workflow metadata: %w", err)
	}
	if meta.GetTemplate() != name {
		return fmt.Errorf("workflow template = %q, want %q", meta.GetTemplate(), name)
	}
	nodes := meta.GetGraph().GetNodes()
	if len(nodes) != 1 || nodes[0].GetState() != dataprocpb.WorkflowNode_COMPLETED {
		return fmt.Errorf("workflow nodes = %+v", nodes)
	}
	return nil
}
