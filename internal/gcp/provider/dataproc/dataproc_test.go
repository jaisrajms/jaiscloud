package dataproc

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/gcp/resource"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func testNR(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Service:    "dataproc",
		Params:     params,
		AccountID:  "proj",
		Region:     "us-central1",
		ResourceID: resource.ResourceID("proj"),
		Cloud:      model.CloudGCP,
	}
}

func newProvider(t *testing.T) *Provider {
	t.Helper()
	return New(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore())
}

func TestCreateCluster_LRO(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	resp, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"projectId":   "proj",
			"clusterName": "c1",
			"config": map[string]any{
				"gceClusterConfig": map[string]any{"zoneUri": "us-central1-a"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	op := resp.Data
	if op["done"] != true {
		t.Fatalf("expected done=true, got %v", op["done"])
	}
	meta, _ := op["metadata"].(map[string]any)
	if meta["@type"] != "type.googleapis.com/google.cloud.dataproc.v1.ClusterOperationMetadata" {
		t.Fatalf("unexpected metadata @type: %v", meta["@type"])
	}
	// name should be projects/{p}/regions/{r}/operations/{id} (id is random).
	if name, _ := op["name"].(string); indexOf(name, "/regions/us-central1/operations/") < 0 {
		t.Fatalf("operation name malformed: %q", name)
	}

	// GetCluster returns the cluster (created synchronously).
	c, err := p.GetCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"}))
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	cluster := c.Data
	if cluster["projectId"] != "proj" || cluster["clusterName"] != "c1" {
		t.Fatalf("cluster identity wrong: %v", cluster)
	}
	status, _ := cluster["status"].(map[string]any)
	if status["state"] != "RUNNING" {
		t.Fatalf("expected RUNNING status, got %v", status["state"])
	}
}

func TestCreateCluster_AlreadyExists(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	body := map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}
	if _, err := p.CreateCluster(ctx, testNR(body)); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := p.CreateCluster(ctx, testNR(body))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 409 {
		t.Fatalf("expected 409 AlreadyExists, got %v", err)
	}
}

func TestGetCluster_NotFound(t *testing.T) {
	p := newProvider(t)
	_, err := p.GetCluster(context.Background(), testNR(map[string]any{"region": "us-central1", "clusterName": "nope"}))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 404 {
		t.Fatalf("expected 404 NotFound, got %v", err)
	}
}

func TestUpdateCluster_UpdateMask(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"projectId":   "proj",
			"clusterName": "c1",
			"config": map[string]any{
				"workerConfig": map[string]any{"numInstances": 2},
			},
			"labels": map[string]any{"env": "dev"},
		},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Update only labels.
	_, err = p.UpdateCluster(ctx, testNR(map[string]any{
		"region":      "us-central1",
		"clusterName": "c1",
		"updateMask":  "labels",
		"body":        map[string]any{"labels": map[string]any{"env": "prod"}},
	}))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	c, _ := p.GetCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"}))
	labels, _ := c.Data["labels"].(map[string]string)
	if labels["env"] != "prod" {
		t.Fatalf("labels not updated: %v", labels)
	}
	// config preserved.
	config, _ := c.Data["config"].(map[string]any)
	if config == nil || config["workerConfig"] == nil {
		t.Fatalf("config was clobbered by label-only update: %v", c.Data)
	}
}

// delayedGetClusterStore wraps a dataprocstore.Store, delaying every
// GetCluster call to widen a TOCTOU race window in tests.
type delayedGetClusterStore struct {
	dataprocstore.Store
	delay time.Duration
}

func (d *delayedGetClusterStore) GetCluster(ctx context.Context, projectID, region, name string) (dataprocstore.Cluster, error) {
	c, err := d.Store.GetCluster(ctx, projectID, region, name)
	time.Sleep(d.delay)
	return c, err
}

// TestUpdateClusterVsStopClusterConcurrent_NoLostUpdate proves UpdateCluster
// and startStopCluster (Start/StopCluster) are atomic with respect to each
// other. Without atomicity, a labels-only PATCH and a concurrent StopCluster
// status transition each do a separate Get-then-Update: both read the same
// base snapshot, and whichever write lands last silently reverts the other's
// already-applied change (labels reverting to their pre-race value, or the
// status transition being lost). A delayed-Get store wrapper widens the
// TOCTOU window reliably (the real in-memory round trip otherwise completes
// in nanoseconds, too fast to overlap deterministically).
func TestUpdateClusterVsStopClusterConcurrent_NoLostUpdate(t *testing.T) {
	p := New(&delayedGetClusterStore{Store: dataprocstore.NewMemoryStore(), delay: 5 * time.Millisecond}, store.NewMemoryResourceStore())
	ctx := context.Background()
	if _, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1", "labels": map[string]any{"env": "dev"}},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}

	var wg sync.WaitGroup
	var updateErr, stopErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, updateErr = p.UpdateCluster(ctx, testNR(map[string]any{
			"region": "us-central1", "clusterName": "c1", "updateMask": "labels",
			"body": map[string]any{"labels": map[string]any{"env": "prod"}},
		}))
	}()
	go func() {
		defer wg.Done()
		_, stopErr = p.StopCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"}))
	}()
	wg.Wait()
	if updateErr != nil {
		t.Fatalf("update: %v", updateErr)
	}
	if stopErr != nil {
		t.Fatalf("stop: %v", stopErr)
	}

	c, err := p.GetCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	labels, _ := c.Data["labels"].(map[string]string)
	if labels["env"] != "prod" {
		t.Errorf("labels PATCH lost: got %v, want env=prod", labels)
	}
	status, _ := c.Data["status"].(map[string]any)
	if status["state"] != "STOPPED" {
		t.Errorf("StopCluster transition lost: got %v, want STOPPED", status["state"])
	}
}

func TestDeleteCluster(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = p.DeleteCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"}))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := p.GetCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"})); err == nil {
		t.Fatal("expected NotFound after delete")
	}
}

func TestStartStopCluster(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))
	if _, err := p.StopCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"})); err != nil {
		t.Fatalf("stop: %v", err)
	}
	c, _ := p.GetCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"}))
	if state := c.Data["status"].(map[string]any)["state"]; state != "STOPPED" {
		t.Fatalf("expected STOPPED, got %v", state)
	}
	if _, err := p.StartCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"})); err != nil {
		t.Fatalf("start: %v", err)
	}
	c, _ = p.GetCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"}))
	if state := c.Data["status"].(map[string]any)["state"]; state != "RUNNING" {
		t.Fatalf("expected RUNNING, got %v", state)
	}
}

func TestDiagnoseCluster_Unimplemented(t *testing.T) {
	p := newProvider(t)
	_, err := p.DiagnoseCluster(context.Background(), testNR(map[string]any{"region": "us-central1", "clusterName": "c1"}))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 501 {
		t.Fatalf("expected 501 Unimplemented, got %v", err)
	}
}

func TestSubmitJob_MockModeDONE(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))
	resp, err := p.SubmitJob(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"job": map[string]any{
				"reference": map[string]any{"projectId": "proj", "jobId": "j1"},
				"placement": map[string]any{"clusterName": "c1"},
				"pysparkJob": map[string]any{
					"mainPythonFileUri": "gs://b/main.py",
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	j := resp.Data
	if j["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("expected DONE in mock mode, got %v", j["status"])
	}
	if j["done"] != true {
		t.Fatalf("expected done=true, got %v", j["done"])
	}

	got, err := p.GetJob(ctx, testNR(map[string]any{"region": "us-central1", "jobId": "j1"}))
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Data["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("GetJob expected DONE, got %v", got.Data)
	}
}

func TestSubmitJob_UnsupportedJobTypeFailsLoud(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))
	resp, err := p.SubmitJob(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"job": map[string]any{
				"reference": map[string]any{"jobId": "j2"},
				"placement": map[string]any{"clusterName": "c1"},
				"hadoopJob": map[string]any{
					"mainJarFileUri": "gs://b/mr.jar",
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	j := resp.Data
	if j["status"].(map[string]any)["state"] != "ERROR" {
		t.Fatalf("expected ERROR for hadoopJob, got %v", j["status"])
	}
	details, _ := j["status"].(map[string]any)["details"].(string)
	if details == "" || details != "job type hadoopJob is not supported by the emulator" {
		t.Fatalf("expected clear statusDetails, got %q", details)
	}
}

func TestSubmitJob_MissingPlacement(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))

	for name, job := range map[string]map[string]any{
		"no placement": {
			"reference":  map[string]any{"jobId": "j-missing-placement"},
			"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
		},
		"empty clusterName": {
			"reference":  map[string]any{"jobId": "j-empty-cluster"},
			"placement":  map[string]any{},
			"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
		},
	} {
		_, err := p.SubmitJob(ctx, testNR(map[string]any{
			"region": "us-central1",
			"body":   map[string]any{"job": job},
		}))
		pe, ok := err.(*model.ProviderError)
		if !ok || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
			t.Fatalf("%s: expected 400 InvalidArgument, got %v", name, err)
		}
	}
}

func TestSubmitJob_UnsupportedSparkSqlAndExtraTypes(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))

	for _, typ := range []string{"sparkSqlJob", "prestoJob", "trinoJob", "flinkJob"} {
		job := map[string]any{
			"reference": map[string]any{"jobId": "j-" + typ},
			"placement": map[string]any{"clusterName": "c1"},
		}
		switch typ {
		case "sparkSqlJob":
			job["sparkSqlJob"] = map[string]any{"queryFileUri": "gs://b/q.sql"}
		case "prestoJob":
			job["prestoJob"] = map[string]any{"queryFileUri": "gs://b/q.sql"}
		case "trinoJob":
			job["trinoJob"] = map[string]any{"queryFileUri": "gs://b/q.sql"}
		case "flinkJob":
			job["flinkJob"] = map[string]any{"mainJarFileUri": "gs://b/a.jar"}
		}
		resp, err := p.SubmitJob(ctx, testNR(map[string]any{
			"region": "us-central1",
			"body":   map[string]any{"job": job},
		}))
		if err != nil {
			t.Fatalf("%s: SubmitJob: %v", typ, err)
		}
		if resp.Data["status"].(map[string]any)["state"] != "ERROR" {
			t.Fatalf("%s: expected ERROR, got %v", typ, resp.Data["status"])
		}
		details, _ := resp.Data["status"].(map[string]any)["details"].(string)
		if details == "" || !strings.Contains(details, typ) {
			t.Fatalf("%s: expected details naming the type, got %q", typ, details)
		}
	}
}

func TestListJobs(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))
	_, _ = p.SubmitJob(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"job": map[string]any{
				"reference": map[string]any{"jobId": "j1"},
				"placement": map[string]any{"clusterName": "c1"},
				"pysparkJob": map[string]any{
					"mainPythonFileUri": "gs://b/main.py",
				},
			},
		},
	}))
	resp, err := p.ListJobs(ctx, testNR(map[string]any{"region": "us-central1"}))
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	jobs, _ := resp.Data["jobs"].([]any)
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
}

func TestSubmitJobAsOperation(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))
	resp, err := p.SubmitJobAsOperation(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"job": map[string]any{
				"reference": map[string]any{"jobId": "j1"},
				"placement": map[string]any{"clusterName": "c1"},
				"pysparkJob": map[string]any{
					"mainPythonFileUri": "gs://b/main.py",
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("SubmitJobAsOperation: %v", err)
	}
	op := resp.Data
	meta, _ := op["metadata"].(map[string]any)
	if meta["@type"] != "type.googleapis.com/google.cloud.dataproc.v1.JobMetadata" {
		t.Fatalf("unexpected metadata @type: %v", meta["@type"])
	}
	// The operation can be fetched by its name.
	opName, _ := op["name"].(string)
	opID := ""
	if i := indexOf(opName, "/operations/"); i >= 0 {
		opID = opName[i+len("/operations/"):]
	}
	got, err := p.GetOperation(ctx, testNR(map[string]any{"region": "us-central1", "operationId": opID}))
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if got.Data["done"] != true {
		t.Fatalf("expected done=true, got %v", got.Data)
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
