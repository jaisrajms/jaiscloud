package dataproc

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// runStoreTests exercises a Store against the shared test matrix. Backend tests
// (memory/postgres) call this so both implement the identical contract.
func runStoreTests(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Reset(ctx)

	if _, err := s.GetCluster(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchCluster {
		t.Fatalf("expected ErrNoSuchCluster, got %v", err)
	}

	const vccJSON = `{"kubernetesClusterConfig":{"gkeClusterConfig":{"gkeClusterTarget":"projects/p/locations/us-central1/clusters/gke"}}}`
	c := Cluster{
		Name:                 "my-cluster",
		Config:               []byte(`{"gceClusterConfig":{"zoneUri":"us-central1-a"},"softwareConfig":{"imageVersion":"2.2"}}`),
		VirtualClusterConfig: []byte(vccJSON),
		Labels:               map[string]string{"env": "dev"},
		Status:               ClusterStatus{State: "CREATING"},
		ClusterUUID:          "uuid-1",
	}
	if err := s.CreateCluster(ctx, "proj", "us-central1", c); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	if err := s.CreateCluster(ctx, "proj", "us-central1", c); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	got, err := s.GetCluster(ctx, "proj", "us-central1", "my-cluster")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if got.Status.State != "CREATING" || got.ClusterUUID != "uuid-1" {
		t.Fatalf("cluster fields lost: %+v", got)
	}
	if string(got.Config) != `{"gceClusterConfig":{"zoneUri":"us-central1-a"},"softwareConfig":{"imageVersion":"2.2"}}` {
		t.Fatalf("config not verbatim: %s", got.Config)
	}
	if !got.IsGKEBacked() || string(got.VirtualClusterConfig) != vccJSON {
		t.Fatalf("virtualClusterConfig not verbatim: %s", got.VirtualClusterConfig)
	}

	got.Status.State = "RUNNING"
	if err := s.UpdateCluster(ctx, "proj", "us-central1", got); err != nil {
		t.Fatalf("update cluster: %v", err)
	}
	list, err := s.ListClusters(ctx, "proj", "us-central1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list clusters: %v %d", err, len(list))
	}
	if list[0].Status.State != "RUNNING" {
		t.Fatalf("update not persisted: %+v", list[0])
	}

	// Jobs
	if _, err := s.GetJob(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchJob {
		t.Fatalf("expected ErrNoSuchJob, got %v", err)
	}
	j := Job{
		JobID:                "job-1",
		PlacementClusterName: "my-cluster",
		PlacementClusterUUID: "cuuid-1",
		Type:                 "pysparkJob",
		TypeJob:              []byte(`{"mainPythonFileUri":"gs://b/main.py"}`),
		Status:               JobStatus{State: "DONE"},
	}
	if err := s.CreateJob(ctx, "proj", "us-central1", j); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := s.CreateJob(ctx, "proj", "us-central1", j); err != ErrAlreadyExists {
		t.Fatalf("expected job ErrAlreadyExists, got %v", err)
	}
	gotJob, err := s.GetJob(ctx, "proj", "us-central1", "job-1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if gotJob.Status.State != "DONE" || gotJob.Type != "pysparkJob" {
		t.Fatalf("job fields lost: %+v", gotJob)
	}
	if gotJob.PlacementClusterUUID != "cuuid-1" {
		t.Fatalf("placement.clusterUuid lost: %+v", gotJob)
	}
	jlist, err := s.ListJobs(ctx, "proj", "us-central1")
	if err != nil || len(jlist) != 1 {
		t.Fatalf("list jobs: %v %d", err, len(jlist))
	}
	if jlist[0].PlacementClusterUUID != "cuuid-1" {
		t.Fatalf("placement.clusterUuid lost in list: %+v", jlist[0])
	}
	// The UUID is preserved across the atomic update path (used at terminal).
	if _, err := s.UpdateJobAtomic(ctx, "proj", "us-central1", "job-1", func(cur Job) (Job, error) {
		cur.Status.State = "DONE"
		return cur, nil
	}); err != nil {
		t.Fatalf("UpdateJobAtomic: %v", err)
	}
	if gotJob, err = s.GetJob(ctx, "proj", "us-central1", "job-1"); err != nil || gotJob.PlacementClusterUUID != "cuuid-1" {
		t.Fatalf("placement.clusterUuid lost after atomic update: %+v, %v", gotJob, err)
	}
	if err := s.DeleteJob(ctx, "proj", "us-central1", "job-1"); err != nil {
		t.Fatalf("delete job: %v", err)
	}

	// Operations
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation, got %v", err)
	}
	op := Operation{ID: "op-1", Done: true, Metadata: `{"@type":"x"}`, Response: `{}`, CreateTime: time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)}
	if err := s.CreateOperation(ctx, "proj", "us-central1", op); err != nil {
		t.Fatalf("create op: %v", err)
	}
	gotOp, err := s.GetOperation(ctx, "proj", "us-central1", "op-1")
	if err != nil || gotOp.Metadata != `{"@type":"x"}` {
		t.Fatalf("get op: %v %+v", err, gotOp)
	}

	// UpdateOperationAtomic performs a locked get-mutate-set cycle.
	atomic, err := s.UpdateOperationAtomic(ctx, "proj", "us-central1", "op-1", func(cur Operation) (Operation, error) {
		cur.Metadata = `{"@type":"y"}`
		return cur, nil
	})
	if err != nil || atomic.Metadata != `{"@type":"y"}` {
		t.Fatalf("UpdateOperationAtomic = %+v, %v", atomic, err)
	}
	if _, err := s.UpdateOperationAtomic(ctx, "proj", "us-central1", "nope", func(cur Operation) (Operation, error) {
		return cur, nil
	}); err != ErrNoSuchOperation {
		t.Fatalf("UpdateOperationAtomic(missing) = %v, want ErrNoSuchOperation", err)
	}

	// Operation retention sweep: only operations older than the cutoff go.
	if err := s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op-old", Done: true, CreateTime: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatalf("create old op: %v", err)
	}
	if err := s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op-new", Done: true, CreateTime: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatalf("create new op: %v", err)
	}
	removed, err := s.DeleteStaleOperations(ctx, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || removed != 1 {
		t.Fatalf("DeleteStaleOperations = %d, %v; want 1 removed", removed, err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op-old"); err != ErrNoSuchOperation {
		t.Fatalf("old op should be swept, got %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op-new"); err != nil {
		t.Fatalf("new op should remain, got %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op-1"); err != nil {
		t.Fatalf("fresh op-1 should remain, got %v", err)
	}

	// An in-flight operation is never swept, even with an old CreateTime: its
	// poll still has to advance the cluster state machine.
	if err := s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op-pending", Done: false, CreateTime: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatalf("create in-flight op: %v", err)
	}
	if removed, err := s.DeleteStaleOperations(ctx, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil || removed != 0 {
		t.Fatalf("DeleteStaleOperations = %d, %v; want 0 removed (in-flight retained)", removed, err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op-pending"); err != nil {
		t.Fatalf("in-flight op should be retained, got %v", err)
	}

	// Delete cluster
	if err := s.DeleteCluster(ctx, "proj", "us-central1", "my-cluster"); err != nil {
		t.Fatalf("delete cluster: %v", err)
	}
	if _, err := s.GetCluster(ctx, "proj", "us-central1", "my-cluster"); err != ErrNoSuchCluster {
		t.Fatalf("expected ErrNoSuchCluster after delete, got %v", err)
	}
}

func TestMemoryStore(t *testing.T) {
	runStoreTests(t, NewMemoryStore())
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateCluster(ctx, "p", "r", Cluster{Name: "c"})
	s.Reset(ctx)
	empty, _ := s.IsEmpty(ctx)
	if !empty {
		t.Fatal("expected empty after reset")
	}
}

func TestClusterIsGKEBacked(t *testing.T) {
	if (Cluster{}).IsGKEBacked() {
		t.Fatal("empty cluster must not be GKE-backed")
	}
	if (Cluster{VirtualClusterConfig: []byte(`{}`)}).IsGKEBacked() {
		t.Fatal("empty virtualClusterConfig must not be GKE-backed")
	}
	if !(Cluster{VirtualClusterConfig: []byte(`{"kubernetesClusterConfig":{}}`)}).IsGKEBacked() {
		t.Fatal("cluster with virtualClusterConfig should be GKE-backed")
	}
}

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	const vcc = `{"kubernetesClusterConfig":{"gkeClusterConfig":{"gkeClusterTarget":"projects/p/locations/us-central1/clusters/gke"}}}`
	_ = s.CreateCluster(ctx, "p", "r", Cluster{Name: "c", Labels: map[string]string{"k": "v"}, Status: ClusterStatus{State: "RUNNING"}, VirtualClusterConfig: []byte(vcc)})
	_ = s.CreateCluster(ctx, "p", "r", Cluster{Name: "gce", Status: ClusterStatus{State: "RUNNING"}})
	_ = s.CreateJob(ctx, "p", "r", Job{JobID: "j", Type: "sparkJob", TypeJob: []byte(`{"mainJarFileUri":"gs://b/a.jar"}`)})
	_ = s.CreateOperation(ctx, "p", "r", Operation{ID: "op", Metadata: `{"@type":"m"}`, Response: `{"x":1}`})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := s2.GetCluster(ctx, "p", "r", "c")
	if err != nil || got.Status.State != "RUNNING" {
		t.Fatalf("cluster lost after restore: %v %+v", err, got)
	}
	if !got.IsGKEBacked() || string(got.VirtualClusterConfig) != vcc {
		t.Fatalf("virtualClusterConfig lost after restore: %s", got.VirtualClusterConfig)
	}
	if gce, _ := s2.GetCluster(ctx, "p", "r", "gce"); gce.IsGKEBacked() {
		t.Fatal("GCE cluster became GKE-backed after restore")
	}
	gotJob, err := s2.GetJob(ctx, "p", "r", "j")
	if err != nil || string(gotJob.TypeJob) != `{"mainJarFileUri":"gs://b/a.jar"}` {
		t.Fatalf("job lost after restore: %v %+v", err, gotJob)
	}
	gotOp, err := s2.GetOperation(ctx, "p", "r", "op")
	if err != nil || gotOp.Metadata != `{"@type":"m"}` {
		t.Fatalf("op lost after restore: %v %+v", err, gotOp)
	}
}
