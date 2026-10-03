package grpcconformance

import (
	"context"
	"fmt"

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
		{Service: "dataproc", RPC: "GetCluster", Method: "GetCluster", KeyField: "clusterName round-trip", Run: checkDPGetCluster},
		{Service: "dataproc", RPC: "ListClusters", Method: "ListClusters", KeyField: "created cluster present", Run: checkDPListClusters},
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
		{Service: "dataproc", RPC: "CreateCluster (metastore)", Method: "CreateCluster", KeyField: "auxiliaryServicesConfig.metastoreConfig echo", Run: checkDPMetastoreAttachment},
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

// ensureDataprocCluster creates the run-unique probe cluster, treating
// AlreadyExists as success so repeated probes are idempotent.
func ensureDataprocCluster(ctx context.Context, client *dataproc.ClusterControllerClient, cfg Config) (string, error) {
	id := dataprocClusterID(cfg)
	op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
		ProjectId: cfg.Project,
		Region:    dataprocRegion,
		Cluster:   &dataprocpb.Cluster{ClusterName: id, Labels: map[string]string{"probe": "gcpc"}},
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
		Cluster:   &dataprocpb.Cluster{ClusterName: id, Labels: map[string]string{"probe": "gcpc"}},
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
		ProjectId: cfg.Project, Region: dataprocRegion, Cluster: &dataprocpb.Cluster{ClusterName: id},
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

// dpGkeTarget is the GKE cluster target the probes reference (metadata only; the
// emulator has no GKE control plane).
func dpGkeTarget(cfg Config) string {
	return "projects/" + cfg.Project + "/locations/" + dataprocRegion + "/clusters/gke-1"
}

// Check 15: a GKE-backed cluster keeps its virtualClusterConfig through gRPC and
// never invents a GCE config.
func checkDPVirtualClusterConfig(ctx context.Context, cfg Config) error {
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

// Check 16: a cluster's auxiliaryServicesConfig.metastoreConfig round-trips over
// gRPC. The attachment is validated against the Metastore control plane at
// create, so a run-unique service is created first.
func checkDPMetastoreAttachment(ctx context.Context, cfg Config) error {
	mc, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new metastore client: %w", err)
	}
	defer mc.Close()
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
