package dataproc

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"jaiscloud/internal/clock"
	core "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

func newTestService() *Service {
	return NewService(core.NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore()), "proj")
}

func createClusterReq(id string) *dataprocpb.CreateClusterRequest {
	return &dataprocpb.CreateClusterRequest{
		ProjectId: "proj",
		Region:    "us-central1",
		Cluster:   &dataprocpb.Cluster{ClusterName: id, Labels: map[string]string{"env": "dev"}},
	}
}

func TestCreateCluster_TypedOperation(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	op, err := s.CreateCluster(ctx, createClusterReq("c1"))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	// Cluster create is asynchronous: the returned operation is in flight.
	if op.GetDone() {
		t.Fatal("create operation unexpectedly done")
	}
	var meta dataprocpb.ClusterOperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("metadata UnmarshalTo: %v", err)
	}
	if meta.GetClusterName() != "c1" || meta.GetOperationType() != "CREATE" {
		t.Fatalf("metadata = %+v", &meta)
	}
	// Polling through the longrunning Operations resolver advances the cluster
	// and completes the operation with the typed Cluster response.
	polled, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if !polled.GetDone() {
		t.Fatal("polled operation not done")
	}
	var cl dataprocpb.Cluster
	if err := polled.GetResponse().UnmarshalTo(&cl); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if cl.GetClusterName() != "c1" || cl.GetStatus().GetState() != dataprocpb.ClusterStatus_RUNNING {
		t.Fatalf("cluster = %+v", &cl)
	}
}

func TestGetCluster_RoundTrip(t *testing.T) {
	s := newTestService()
	if _, err := s.CreateCluster(context.Background(), createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	cl, err := s.GetCluster(context.Background(), &dataprocpb.GetClusterRequest{
		ProjectId: "proj", Region: "us-central1", ClusterName: "c1",
	})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if cl.GetProjectId() != "proj" || cl.GetClusterName() != "c1" || cl.GetLabels()["env"] != "dev" {
		t.Fatalf("cluster = %+v", cl)
	}
}

func TestListClusters_IncludesCreated(t *testing.T) {
	s := newTestService()
	if _, err := s.CreateCluster(context.Background(), createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	resp, err := s.ListClusters(context.Background(), &dataprocpb.ListClustersRequest{ProjectId: "proj", Region: "us-central1"})
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(resp.GetClusters()) != 1 || resp.GetClusters()[0].GetClusterName() != "c1" {
		t.Fatalf("clusters = %+v", resp.GetClusters())
	}
}

func TestUpdateCluster_LabelsMask(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	op, err := s.UpdateCluster(ctx, &dataprocpb.UpdateClusterRequest{
		ProjectId:   "proj",
		Region:      "us-central1",
		ClusterName: "c1",
		Cluster:     &dataprocpb.Cluster{ClusterName: "c1", Labels: map[string]string{"env": "prod"}},
		UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateCluster: %v", err)
	}
	// The update is asynchronous: poll to settle the UPDATING transition.
	polled, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	var cl dataprocpb.Cluster
	if err := polled.GetResponse().UnmarshalTo(&cl); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if cl.GetLabels()["env"] != "prod" {
		t.Fatalf("labels = %v", cl.GetLabels())
	}
}

func TestDeleteCluster_EmptyResponse(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	op, err := s.DeleteCluster(ctx, &dataprocpb.DeleteClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "c1"})
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if op.GetDone() {
		t.Fatal("delete operation unexpectedly done")
	}
	// Polling settles the DELETING transition, removes the cluster, and packs
	// the empty response.
	polled, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if !polled.GetDone() {
		t.Fatal("polled delete operation not done")
	}
	if polled.GetResponse().GetTypeUrl() != "type.googleapis.com/google.protobuf.Empty" {
		t.Fatalf("delete response type = %q", polled.GetResponse().GetTypeUrl())
	}
	if _, err := s.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "c1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetCluster after delete = %v, want NotFound", err)
	}
}

func submitReq(jobID string) *dataprocpb.SubmitJobRequest {
	return &dataprocpb.SubmitJobRequest{
		ProjectId: "proj",
		Region:    "us-central1",
		Job: &dataprocpb.Job{
			Reference: &dataprocpb.JobReference{JobId: jobID},
			Placement: &dataprocpb.JobPlacement{ClusterName: "c1"},
			TypeJob: &dataprocpb.Job_PysparkJob{
				PysparkJob: &dataprocpb.PySparkJob{MainPythonFileUri: "gs://b/main.py"},
			},
		},
	}
}

// pollJobTerminal reads a job through the gRPC surface until it reaches a
// terminal state, exercising the lazy job state machine one hop per read.
func pollJobTerminal(t *testing.T, ctx context.Context, s *Service, jobID string) *dataprocpb.Job {
	t.Helper()
	for i := 0; i < 16; i++ {
		j, err := s.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: "proj", Region: "us-central1", JobId: jobID})
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		switch j.GetStatus().GetState() {
		case dataprocpb.JobStatus_DONE, dataprocpb.JobStatus_ERROR, dataprocpb.JobStatus_CANCELLED:
			return j
		}
	}
	t.Fatalf("job %s did not reach a terminal state", jobID)
	return nil
}

func TestSubmitJob_MockProgression(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	j, err := s.SubmitJob(ctx, submitReq("j1"))
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if j.GetStatus().GetState() != dataprocpb.JobStatus_PENDING {
		t.Fatalf("job state = %v, want PENDING", j.GetStatus().GetState())
	}
	if j.GetReference().GetJobId() != "j1" {
		t.Fatalf("job reference = %+v", j.GetReference())
	}
	if got := pollJobTerminal(t, ctx, s, "j1").GetStatus().GetState(); got != dataprocpb.JobStatus_DONE {
		t.Fatalf("job state = %v, want DONE", got)
	}
}

func TestSubmitJobAsOperation_TypedJobResponse(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	op, err := s.SubmitJobAsOperation(ctx, submitReq("j1"))
	if err != nil {
		t.Fatalf("SubmitJobAsOperation: %v", err)
	}
	if op.GetDone() {
		t.Fatal("mock-mode submit operation should be in flight")
	}
	var meta dataprocpb.JobMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("metadata UnmarshalTo: %v", err)
	}
	if meta.GetJobId() != "j1" {
		t.Fatalf("metadata = %+v", &meta)
	}
	// Polling through the longrunning Operations resolver advances the job and
	// completes the operation with the typed Job response.
	for i := 0; i < 16 && !op.GetDone(); i++ {
		polled, handled, pollErr := s.ResolveOperation(ctx, op.GetName())
		if pollErr != nil || !handled {
			t.Fatalf("ResolveOperation: handled=%v err=%v", handled, pollErr)
		}
		op = polled
	}
	if !op.GetDone() {
		t.Fatal("submit operation never completed")
	}
	var jb dataprocpb.Job
	if err := op.GetResponse().UnmarshalTo(&jb); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if jb.GetReference().GetJobId() != "j1" || jb.GetStatus().GetState() != dataprocpb.JobStatus_DONE {
		t.Fatalf("job = %+v", &jb)
	}
}

func TestUpdateJob_Labels(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.SubmitJob(ctx, submitReq("j1")); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	j, err := s.UpdateJob(ctx, &dataprocpb.UpdateJobRequest{
		ProjectId:  "proj",
		Region:     "us-central1",
		JobId:      "j1",
		Job:        &dataprocpb.Job{Reference: &dataprocpb.JobReference{JobId: "j1"}, Labels: map[string]string{"env": "prod"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if j.GetLabels()["env"] != "prod" {
		t.Fatalf("labels = %v", j.GetLabels())
	}
}

func TestDeleteJob_TerminalSucceeds(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.SubmitJob(ctx, submitReq("j1")); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	// A mock-mode job is only deletable once it has reached a terminal state.
	pollJobTerminal(t, ctx, s, "j1")
	if _, err := s.DeleteJob(ctx, &dataprocpb.DeleteJobRequest{ProjectId: "proj", Region: "us-central1", JobId: "j1"}); err != nil {
		t.Fatalf("DeleteJob terminal: %v", err)
	}
	if _, err := s.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: "proj", Region: "us-central1", JobId: "j1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetJob after delete = %v, want NotFound", err)
	}
}

// TestDeleteJob_ActiveFailsPrecondition seeds a non-terminal job directly (the
// mock executor completes jobs synchronously, so SubmitJob cannot produce one)
// and asserts the FAILED_PRECONDITION mapping.
func TestDeleteJob_ActiveFailsPrecondition(t *testing.T) {
	st := dpstore.NewMemoryStore()
	s := NewService(core.NewService(st, store.NewMemoryResourceStore()), "proj")
	ctx := context.Background()
	if err := st.CreateJob(ctx, "proj", "us-central1", dpstore.Job{
		ProjectID: "proj", Region: "us-central1", JobID: "j-active", PlacementClusterName: "c1",
		Type: "pysparkJob", Status: dpstore.JobStatus{State: "RUNNING", StateStartTime: clock.Now().UTC()},
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if _, err := s.DeleteJob(ctx, &dataprocpb.DeleteJobRequest{ProjectId: "proj", Region: "us-central1", JobId: "j-active"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteJob active = %v, want FailedPrecondition", err)
	}
}

// TestListJobs_FilterAndMatcher verifies the gRPC ListJobs cluster_name, filter
// and job_state_matcher fields reach the shared core.
func TestListJobs_FilterAndMatcher(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	for _, c := range []string{"c1", "c2"} {
		if _, err := s.CreateCluster(ctx, createClusterReq(c)); err != nil {
			t.Fatalf("CreateCluster(%s): %v", c, err)
		}
	}
	submit := func(id, cluster string, labels map[string]string) {
		t.Helper()
		jr := submitReq(id)
		jr.Job.Placement.ClusterName = cluster
		jr.Job.Labels = labels
		if _, err := s.SubmitJob(ctx, jr); err != nil {
			t.Fatalf("SubmitJob(%s): %v", id, err)
		}
	}
	submit("j1", "c1", map[string]string{"env": "staging"})
	submit("j2", "c1", map[string]string{"env": "prod"})
	submit("j3", "c2", map[string]string{"env": "staging"})

	list := func(req *dataprocpb.ListJobsRequest) string {
		t.Helper()
		req.ProjectId = "proj"
		req.Region = "us-central1"
		resp, err := s.ListJobs(ctx, req)
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		ids := make([]string, 0, len(resp.GetJobs()))
		for _, j := range resp.GetJobs() {
			ids = append(ids, j.GetReference().GetJobId())
		}
		sort.Strings(ids)
		return strings.Join(ids, ",")
	}

	if got := list(&dataprocpb.ListJobsRequest{JobStateMatcher: dataprocpb.ListJobsRequest_ACTIVE}); got != "j1,j2,j3" {
		t.Fatalf("ACTIVE matcher = %q, want j1,j2,j3", got)
	}
	if got := list(&dataprocpb.ListJobsRequest{ClusterName: "c1"}); got != "j1,j2" {
		t.Fatalf("cluster_name = %q, want j1,j2", got)
	}
	if got := list(&dataprocpb.ListJobsRequest{Filter: `labels.env = staging`}); got != "j1,j3" {
		t.Fatalf("filter = %q, want j1,j3", got)
	}
	if got := list(&dataprocpb.ListJobsRequest{ClusterName: "c1", Filter: `labels.env = staging`}); got != "j1" {
		t.Fatalf("cluster+filter = %q, want j1", got)
	}
	// The three jobs are terminal after the reads above; NON_ACTIVE selects them
	// and a filter still overrides the matcher.
	if got := list(&dataprocpb.ListJobsRequest{JobStateMatcher: dataprocpb.ListJobsRequest_NON_ACTIVE}); got != "j1,j2,j3" {
		t.Fatalf("NON_ACTIVE matcher = %q, want j1,j2,j3", got)
	}
	if got := list(&dataprocpb.ListJobsRequest{JobStateMatcher: dataprocpb.ListJobsRequest_NON_ACTIVE, Filter: `status.state = ACTIVE`}); got != "" {
		t.Fatalf("filter overriding matcher = %q, want none", got)
	}
	if _, err := s.ListJobs(ctx, &dataprocpb.ListJobsRequest{ProjectId: "proj", Region: "us-central1", Filter: "bogus = 1"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed filter = %v, want InvalidArgument", err)
	}
}

// TestJobToProto_SparkHybridPreservesStatusAndJar guards the cross-transport
// case where a REST-created sparkJob carries both mainJarFileUri and mainClass:
// SparkJob.driver is a proto oneof, so the renderer must move the jar into
// jarFileUris (the proto's documented encoding) instead of losing the whole job.
func TestJobToProto_SparkHybridPreservesStatusAndJar(t *testing.T) {
	j := dpstore.Job{
		ProjectID: "proj", Region: "us-central1", JobID: "j1", PlacementClusterName: "c1",
		Type:    "sparkJob",
		TypeJob: []byte(`{"mainJarFileUri":"gs://b/app.jar","mainClass":"com.example.Main"}`),
		Status:  dpstore.JobStatus{State: "RUNNING", StateStartTime: clock.Now().UTC(), Substate: "QUEUED"},
	}
	got := jobToProto(j)
	if got.GetReference().GetJobId() != "j1" {
		t.Fatalf("job reference lost: %+v", got.GetReference())
	}
	if got.GetStatus().GetState() != dataprocpb.JobStatus_RUNNING {
		t.Fatalf("status = %v, want RUNNING", got.GetStatus().GetState())
	}
	sj := got.GetSparkJob()
	if sj == nil {
		t.Fatal("sparkJob type job lost in transcode")
	}
	if sj.GetMainClass() != "com.example.Main" {
		t.Fatalf("mainClass = %q", sj.GetMainClass())
	}
	if uris := sj.GetJarFileUris(); len(uris) != 1 || uris[0] != "gs://b/app.jar" {
		t.Fatalf("jarFileUris = %v, want the hybrid jar", uris)
	}
}

// gkeClusterReq builds a CreateClusterRequest carrying a GKE
// virtualClusterConfig with an existing-cluster target.
func gkeClusterReq(id string) *dataprocpb.CreateClusterRequest {
	return &dataprocpb.CreateClusterRequest{
		ProjectId: "proj",
		Region:    "us-central1",
		Cluster: &dataprocpb.Cluster{
			ClusterName: id,
			VirtualClusterConfig: &dataprocpb.VirtualClusterConfig{
				StagingBucket: "dataproc-staging-proj",
				InfrastructureConfig: &dataprocpb.VirtualClusterConfig_KubernetesClusterConfig{
					KubernetesClusterConfig: &dataprocpb.KubernetesClusterConfig{
						Config: &dataprocpb.KubernetesClusterConfig_GkeClusterConfig{
							GkeClusterConfig: &dataprocpb.GkeClusterConfig{
								GkeClusterTarget: "projects/proj/locations/us-central1/clusters/gke-1",
							},
						},
					},
				},
			},
		},
	}
}

// TestCreateCluster_GKEVirtualClusterConfig verifies the gRPC transport keeps a
// virtualClusterConfig through create/get and never invents a GCE config.
func TestCreateCluster_GKEVirtualClusterConfig(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, gkeClusterReq("gke-1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	cl, err := s.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "gke-1"})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if cl.GetConfig() != nil {
		t.Fatalf("GKE cluster must not carry a GCE config: %+v", cl.GetConfig())
	}
	vcc := cl.GetVirtualClusterConfig()
	if vcc == nil {
		t.Fatal("virtualClusterConfig lost in gRPC transcode")
	}
	if vcc.GetStagingBucket() != "dataproc-staging-proj" {
		t.Fatalf("stagingBucket = %q", vcc.GetStagingBucket())
	}
	if got := vcc.GetKubernetesClusterConfig().GetGkeClusterConfig().GetGkeClusterTarget(); got != "projects/proj/locations/us-central1/clusters/gke-1" {
		t.Fatalf("gkeClusterTarget = %q", got)
	}
}

// TestGetCluster_RESTCreatedGKEVirtualClusterConfig seeds a cluster from a
// Discovery-shaped (REST) body and verifies it is served correctly over gRPC —
// the cross-transport path that shares the core store.
func TestGetCluster_RESTCreatedGKEVirtualClusterConfig(t *testing.T) {
	st := dpstore.NewMemoryStore()
	s := NewService(core.NewService(st, store.NewMemoryResourceStore()), "proj")
	ctx := context.Background()
	restBody := map[string]any{
		"virtualClusterConfig": map[string]any{
			"stagingBucket": "b",
			"kubernetesClusterConfig": map[string]any{
				"gkeClusterConfig": map[string]any{
					"gkeClusterTarget": "projects/proj/locations/us-central1/clusters/gke-1",
				},
			},
		},
	}
	if _, _, err := s.core.CreateCluster(ctx, "proj", "us-central1", "gke-1", core.ClusterInputFromMap(restBody)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	cl, err := s.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "gke-1"})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if cl.GetConfig() != nil {
		t.Fatalf("REST-created GKE cluster surfaced a GCE config over gRPC: %+v", cl.GetConfig())
	}
	if got := cl.GetVirtualClusterConfig().GetKubernetesClusterConfig().GetGkeClusterConfig().GetGkeClusterTarget(); got != "projects/proj/locations/us-central1/clusters/gke-1" {
		t.Fatalf("gkeClusterTarget = %q", got)
	}
}

// TestCreateCluster_MalformedVirtualClusterConfig verifies the gRPC transport
// maps a virtualClusterConfig without a GKE target to InvalidArgument.
func TestCreateCluster_MalformedVirtualClusterConfig(t *testing.T) {
	s := newTestService()
	_, err := s.CreateCluster(context.Background(), &dataprocpb.CreateClusterRequest{
		ProjectId: "proj",
		Region:    "us-central1",
		Cluster: &dataprocpb.Cluster{
			ClusterName: "bad-gke",
			VirtualClusterConfig: &dataprocpb.VirtualClusterConfig{
				InfrastructureConfig: &dataprocpb.VirtualClusterConfig_KubernetesClusterConfig{
					KubernetesClusterConfig: &dataprocpb.KubernetesClusterConfig{
						Config: &dataprocpb.KubernetesClusterConfig_GkeClusterConfig{
							GkeClusterConfig: &dataprocpb.GkeClusterConfig{},
						},
					},
				},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateCluster malformed vcc = %v, want InvalidArgument", err)
	}
}

// TestUpdateCluster_VirtualClusterConfigSnakeMask verifies a gRPC FieldMask's
// snake_case virtual_cluster_config path is normalized and applies the update.
func TestUpdateCluster_VirtualClusterConfigSnakeMask(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, gkeClusterReq("gke-1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.UpdateCluster(ctx, &dataprocpb.UpdateClusterRequest{
		ProjectId:   "proj",
		Region:      "us-central1",
		ClusterName: "gke-1",
		Cluster: &dataprocpb.Cluster{
			ClusterName: "gke-1",
			VirtualClusterConfig: &dataprocpb.VirtualClusterConfig{
				InfrastructureConfig: &dataprocpb.VirtualClusterConfig_KubernetesClusterConfig{
					KubernetesClusterConfig: &dataprocpb.KubernetesClusterConfig{
						Config: &dataprocpb.KubernetesClusterConfig_GkeClusterConfig{
							GkeClusterConfig: &dataprocpb.GkeClusterConfig{
								GkeClusterTarget: "projects/proj/locations/us-central1/clusters/gke-2",
							},
						},
					},
				},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"virtual_cluster_config"}},
	}); err != nil {
		t.Fatalf("UpdateCluster: %v", err)
	}
	cl, err := s.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "gke-1"})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if got := cl.GetVirtualClusterConfig().GetKubernetesClusterConfig().GetGkeClusterConfig().GetGkeClusterTarget(); got != "projects/proj/locations/us-central1/clusters/gke-2" {
		t.Fatalf("gkeClusterTarget = %q, want the updated target", got)
	}
}

// TestSubmitJob_PlacementClusterUUID verifies the gRPC adapter surfaces the
// output-only JobPlacement.clusterUuid captured from the cluster at submit.
func TestSubmitJob_PlacementClusterUUID(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	cl, err := s.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: "proj", Region: "us-central1", ClusterName: "c1"})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if _, err := s.SubmitJob(ctx, submitReq("j1")); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	got, err := s.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: "proj", Region: "us-central1", JobId: "j1"})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if cl.GetClusterUuid() == "" {
		t.Fatal("cluster has no UUID")
	}
	if got.GetPlacement().GetClusterUuid() != cl.GetClusterUuid() {
		t.Fatalf("job placement.clusterUuid = %q, want %q", got.GetPlacement().GetClusterUuid(), cl.GetClusterUuid())
	}
}

// TestClusterOperation_TypedMetadataEveryStatus verifies an in-flight cluster
// operation surfaces typed ClusterOperationMetadata with the operation-status
// enum on every poll (RUNNING while the cluster provisions, DONE once packed),
// so a generated client's Poll/Wait sees incremental status rather than only
// the terminal envelope.
func TestClusterOperation_TypedMetadataEveryStatus(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	s := NewService(core.NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		core.WithClusterReadyDelay(30*time.Second)), "proj")
	ctx := context.Background()

	op, err := s.CreateCluster(ctx, createClusterReq("c1"))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	var meta dataprocpb.ClusterOperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("metadata UnmarshalTo: %v", err)
	}
	if meta.GetStatus().GetState() != dataprocpb.ClusterOperationStatus_RUNNING {
		t.Fatalf("in-flight metadata state = %v, want RUNNING", meta.GetStatus().GetState())
	}
	if meta.GetOperationType() != "CREATE" || meta.GetClusterName() != "c1" {
		t.Fatalf("metadata = %+v", &meta)
	}
	// Still in flight before the delay elapses: the poll returns a typed
	// metadata envelope with done=false.
	polled, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if polled.GetDone() {
		t.Fatal("operation settled before the cluster did")
	}
	// Past the delay the operation completes: metadata is DONE and the response
	// is a typed Cluster.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(31 * time.Second)})
	polled, handled, err = s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if !polled.GetDone() {
		t.Fatal("operation not done after the cluster settled")
	}
	if err := polled.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("terminal metadata UnmarshalTo: %v", err)
	}
	if meta.GetStatus().GetState() != dataprocpb.ClusterOperationStatus_DONE {
		t.Fatalf("terminal metadata state = %v, want DONE", meta.GetStatus().GetState())
	}
	if len(meta.GetStatusHistory()) != 3 {
		t.Fatalf("status history = %d entries, want PENDING,RUNNING,DONE", len(meta.GetStatusHistory()))
	}
	var cl dataprocpb.Cluster
	if err := polled.GetResponse().UnmarshalTo(&cl); err != nil {
		t.Fatalf("response UnmarshalTo: %v", err)
	}
	if cl.GetClusterName() != "c1" || cl.GetStatus().GetState() != dataprocpb.ClusterStatus_RUNNING {
		t.Fatalf("cluster = %+v", &cl)
	}
}

func TestDiagnoseCluster_Unimplemented(t *testing.T) {
	s := newTestService()
	_, err := s.DiagnoseCluster(context.Background(), &dataprocpb.DiagnoseClusterRequest{
		ProjectId: "proj", Region: "us-central1", ClusterName: "c1",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("DiagnoseCluster = %v, want Unimplemented", err)
	}
}

// TestSubmitJob_SchedulingRoundTrip verifies Job.scheduling survives the gRPC
// ingress (proto -> core JobInput) and the egress (core Job -> proto).
func TestSubmitJob_SchedulingRoundTrip(t *testing.T) {
	s := newTestService()
	ctx := context.Background()
	if _, err := s.CreateCluster(ctx, createClusterReq("c1")); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	req := submitReq("j1")
	req.Job.Scheduling = &dataprocpb.JobScheduling{MaxFailuresPerHour: 3, MaxFailuresTotal: 9}
	j, err := s.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if j.GetScheduling().GetMaxFailuresPerHour() != 3 || j.GetScheduling().GetMaxFailuresTotal() != 9 {
		t.Fatalf("scheduling = %+v", j.GetScheduling())
	}
}

// TestJobScalarsToProto_IncludesScheduling pins the scalar fallback path: when
// the full proto transcode fails, the identity/status fields are preserved and
// must include the restart policy.
func TestJobScalarsToProto_IncludesScheduling(t *testing.T) {
	out := jobScalarsToProto(dpstore.Job{
		Scheduling: &dpstore.JobScheduling{MaxFailuresPerHour: 2, MaxFailuresTotal: 4},
	})
	if out.GetScheduling().GetMaxFailuresPerHour() != 2 || out.GetScheduling().GetMaxFailuresTotal() != 4 {
		t.Fatalf("scalar fallback scheduling = %+v", out.GetScheduling())
	}
}
