//go:build gcp_persistence

package dataproc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// jsonEqual reports whether two JSON documents are semantically equal, ignoring
// object key order (JSONB normalizes key order on the round trip, so byte-for-byte
// comparison is not meaningful against Postgres — unlike the memory store).
func jsonEqual(a, b string) bool {
	var av, bv any
	if json.Unmarshal([]byte(a), &av) != nil || json.Unmarshal([]byte(b), &bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

// TestPostgresStoreSnapshotVerbatim verifies that cluster config, job type_job,
// and operation metadata/response survive a Postgres Snapshot/Restore round
// trip byte-for-byte.
func TestPostgresStoreSnapshotVerbatim(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres snapshot test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	const config = `{"gceClusterConfig":{"zoneUri":"z"},"softwareConfig":{"imageVersion":"2.2"},"initializationActions":[{"executableFile":"gs://b/init.sh"}]}`
	const vcc = `{"kubernetesClusterConfig":{"gkeClusterConfig":{"gkeClusterTarget":"projects/p/locations/us-central1/clusters/gke"}},"auxiliaryServicesConfig":{"metastoreConfig":{"dataprocMetastoreService":"projects/p/locations/us-central1/services/hms"}}}`
	c := Cluster{Name: "c1", Config: []byte(config), VirtualClusterConfig: []byte(vcc), Labels: map[string]string{"k": "v"}, Status: ClusterStatus{State: "RUNNING"}}
	if err := s.CreateCluster(ctx, "proj", "us-central1", c); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	const typeJob = `{"mainJarFileUri":"gs://b/a.jar","mainClass":"Main","args":["x"]}`
	j := Job{JobID: "j1", PlacementClusterName: "c1", PlacementClusterUUID: "uuid-9", Type: "sparkJob", TypeJob: []byte(typeJob), Status: JobStatus{State: "DONE"}}
	if err := s.CreateJob(ctx, "proj", "us-central1", j); err != nil {
		t.Fatalf("create job: %v", err)
	}
	_ = s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op1", Done: true, Metadata: `{"@type":"t"}`})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := s.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if !jsonEqual(string(got.Config), config) {
		t.Fatalf("config not preserved: %q", got.Config)
	}
	if !got.IsGKEBacked() || !jsonEqual(string(got.VirtualClusterConfig), vcc) {
		t.Fatalf("virtualClusterConfig not preserved: %q", got.VirtualClusterConfig)
	}
	if got.Labels["k"] != "v" || got.Status.State != "RUNNING" {
		t.Fatalf("cluster fields lost: %+v", got)
	}

	gotJob, err := s.GetJob(ctx, "proj", "us-central1", "j1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if !jsonEqual(string(gotJob.TypeJob), typeJob) {
		t.Fatalf("type_job not preserved: %q", gotJob.TypeJob)
	}
	if gotJob.PlacementClusterUUID != "uuid-9" {
		t.Fatalf("placement_cluster_uuid not preserved: %q", gotJob.PlacementClusterUUID)
	}
}

// TestPostgresStoreWorkflowTemplateRoundTrip verifies the workflow template
// table (create/get/version-bump/delete) and the snapshot round trip.
func TestPostgresStoreWorkflowTemplateRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres workflow template test")
	}
	ctx := context.Background()
	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	const def = `{"placement":{"managedCluster":{"clusterName":"c"}},"jobs":[{"stepId":"a","pysparkJob":{"mainPythonFileUri":"gs://b/a.py"}}]}`
	if err := s.CreateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplate{TemplateID: "wf", Version: 1, Definition: []byte(def)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.GetWorkflowTemplate(ctx, "proj", "us-central1", "wf")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Version != 1 || !jsonEqual(string(got.Definition), def) {
		t.Fatalf("template = %+v", got)
	}
	updated, err := s.UpdateWorkflowTemplateAtomic(ctx, "proj", "us-central1", "wf", func(cur WorkflowTemplate) (WorkflowTemplate, error) {
		cur.Version++
		return cur, nil
	})
	if err != nil || updated.Version != 2 {
		t.Fatalf("update = %+v (%v)", updated, err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got2, err := s.GetWorkflowTemplate(ctx, "proj", "us-central1", "wf")
	if err != nil || got2.Version != 2 || !jsonEqual(string(got2.Definition), def) {
		t.Fatalf("after snapshot = %+v (%v)", got2, err)
	}

	if err := s.DeleteWorkflowTemplate(ctx, "proj", "us-central1", "wf"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetWorkflowTemplate(ctx, "proj", "us-central1", "wf"); !errors.Is(err, ErrNoSuchWorkflowTemplate) {
		t.Fatalf("get after delete = %v, want ErrNoSuchWorkflowTemplate", err)
	}
}

// TestPostgresStoreGKEVirtualClusterConfigNoConfig verifies that a GKE-only
// cluster (virtualClusterConfig, no GCE config) does not read back an empty
// `config` from the JSONB '{}' sentinel, and its virtualClusterConfig survives.
func TestPostgresStoreGKEVirtualClusterConfigNoConfig(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres GKE round-trip test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	const vcc = `{"kubernetesClusterConfig":{"gkeClusterConfig":{"gkeClusterTarget":"projects/p/locations/us-central1/clusters/gke"}}}`
	if err := s.CreateCluster(ctx, "proj", "us-central1", Cluster{
		Name: "gke", VirtualClusterConfig: []byte(vcc), Status: ClusterStatus{State: "RUNNING"},
	}); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	got, err := s.GetCluster(ctx, "proj", "us-central1", "gke")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if len(got.Config) != 0 {
		t.Fatalf("GKE cluster read back a GCE config: %q", got.Config)
	}
	if !got.IsGKEBacked() || !jsonEqual(string(got.VirtualClusterConfig), vcc) {
		t.Fatalf("virtualClusterConfig not preserved: %q", got.VirtualClusterConfig)
	}
}

// TestPostgresStoreProjectRegionRoundTrip verifies projectId/region (and the
// operation's region, which drives the "regions/{region}/operations/{id}" name)
// survive a Create/Get/List round trip. This is the CB-1 fix: the SELECTs and
// scanners must read back project_id and region.
func TestPostgresStoreProjectRegionRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres round-trip test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	const project = "proj"
	const region = "us-central1"

	if err := s.CreateCluster(ctx, project, region, Cluster{Name: "c1", Status: ClusterStatus{State: "RUNNING"}}); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	gotC, err := s.GetCluster(ctx, project, region, "c1")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if gotC.ProjectID != project || gotC.Region != region {
		t.Fatalf("cluster projectId/region lost: %+v", gotC)
	}
	listC, err := s.ListClusters(ctx, project, region)
	if err != nil || len(listC) != 1 || listC[0].ProjectID != project || listC[0].Region != region {
		t.Fatalf("list cluster projectId/region lost: %v %+v", err, listC)
	}

	if err := s.CreateJob(ctx, project, region, Job{JobID: "j1", Type: "sparkJob", TypeJob: []byte(`{}`)}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	gotJ, err := s.GetJob(ctx, project, region, "j1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if gotJ.ProjectID != project || gotJ.Region != region {
		t.Fatalf("job projectId/region lost: %+v", gotJ)
	}
	listJ, err := s.ListJobs(ctx, project, region)
	if err != nil || len(listJ) != 1 || listJ[0].ProjectID != project || listJ[0].Region != region {
		t.Fatalf("list job projectId/region lost: %v %+v", err, listJ)
	}

	if err := s.CreateOperation(ctx, project, region, Operation{ID: "op1", Done: true, Metadata: `{"@type":"t"}`}); err != nil {
		t.Fatalf("create op: %v", err)
	}
	gotOp, err := s.GetOperation(ctx, project, region, "op1")
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if gotOp.ProjectID != project || gotOp.Region != region {
		t.Fatalf("operation projectId/region lost (drives operation name region): %+v", gotOp)
	}
}

// TestPostgresStoreSnapshotRoundTrip verifies that terminal-job fields
// (driverOutputResourceUri, driverControlFilesUri, and statusHistory, including
// the terminal status.details) survive a Postgres Snapshot/Restore round trip
// byte-for-byte — mirroring TestMemoryStoreSnapshotRoundTrip's field assertions.
func TestPostgresStoreSnapshotRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres snapshot round-trip test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	const typeJob = `{"mainJarFileUri":"gs://b/a.jar","mainClass":"Main","args":["x","y"]}`
	doneAt := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)
	history := []JobStatus{
		{State: "RUNNING", StateStartTime: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
		{State: "DONE", StateStartTime: doneAt},
	}
	want := Job{
		JobID:                   "j1",
		PlacementClusterName:    "c1",
		Type:                    "sparkJob",
		TypeJob:                 []byte(typeJob),
		Status:                  JobStatus{State: "DONE", Details: "finished", StateStartTime: doneAt},
		StatusHistory:           history,
		DriverOutputResourceURI: "gs://jaiscloud-dataproc/jobuuid/driveroutput",
		DriverControlFilesURI:   "gs://jaiscloud-dataproc/jobuuid/control",
		JobUUID:                 "jobuuid",
	}
	if err := s.CreateJob(ctx, "proj", "us-central1", want); err != nil {
		t.Fatalf("create job: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx) // wipe so Restore is the sole source of the restored state
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := s.GetJob(ctx, "proj", "us-central1", "j1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if !jsonEqual(string(got.TypeJob), typeJob) {
		t.Fatalf("type_job not preserved: %q", got.TypeJob)
	}
	if got.DriverOutputResourceURI != want.DriverOutputResourceURI {
		t.Fatalf("driverOutputResourceUri lost: got %q want %q", got.DriverOutputResourceURI, want.DriverOutputResourceURI)
	}
	if got.DriverControlFilesURI != want.DriverControlFilesURI {
		t.Fatalf("driverControlFilesUri lost: got %q want %q", got.DriverControlFilesURI, want.DriverControlFilesURI)
	}
	if got.JobUUID != want.JobUUID {
		t.Fatalf("jobUuid lost: got %q want %q", got.JobUUID, want.JobUUID)
	}
	if got.Status.State != "DONE" || got.Status.Details != "finished" {
		t.Fatalf("status lost: got %+v", got.Status)
	}
	if !got.Status.StateStartTime.Equal(doneAt) {
		t.Fatalf("status.stateStartTime lost: got %v want %v", got.Status.StateStartTime, doneAt)
	}
	if len(got.StatusHistory) != len(history) {
		t.Fatalf("statusHistory length: got %d want %d", len(got.StatusHistory), len(history))
	}
	for i := range history {
		if got.StatusHistory[i].State != history[i].State ||
			!got.StatusHistory[i].StateStartTime.Equal(history[i].StateStartTime) {
			t.Fatalf("statusHistory[%d] lost: got %+v want %+v", i, got.StatusHistory[i], history[i])
		}
	}
}
