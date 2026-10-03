package dataproc

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/resource"
	core "jaiscloud/internal/gcp/service/dataproc"
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
	return NewProvider(core.NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore()), "proj")
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
	// Create is asynchronous: the create response is an in-flight operation.
	if op["done"] != false {
		t.Fatalf("expected done=false on create, got %v", op["done"])
	}
	meta, _ := op["metadata"].(map[string]any)
	if meta["@type"] != "type.googleapis.com/google.cloud.dataproc.v1.ClusterOperationMetadata" {
		t.Fatalf("unexpected metadata @type: %v", meta["@type"])
	}
	if status, _ := meta["status"].(map[string]any); status["state"] != "RUNNING" {
		t.Fatalf("operation status = %v, want RUNNING", meta["status"])
	}
	// name should be projects/{p}/regions/{r}/operations/{id} (id is random).
	name, _ := op["name"].(string)
	if indexOf(name, "/regions/us-central1/operations/") < 0 {
		t.Fatalf("operation name malformed: %q", name)
	}
	opID := name[indexOf(name, "/operations/")+len("/operations/"):]

	// Polling the operation advances the cluster to RUNNING and completes it.
	polled, err := p.GetOperation(ctx, testNR(map[string]any{"region": "us-central1", "operationId": opID}))
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if polled.Data["done"] != true {
		t.Fatalf("expected done=true after poll, got %v", polled.Data)
	}
	response, _ := polled.Data["response"].(map[string]any)
	if response["clusterName"] != "c1" {
		t.Fatalf("operation response = %v, want the cluster", polled.Data["response"])
	}
	if status, _ := response["status"].(map[string]any); status["state"] != "RUNNING" {
		t.Fatalf("polled cluster state = %v, want RUNNING", status["state"])
	}

	// GetCluster returns the settled cluster.
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
	p := NewProvider(core.NewService(&delayedGetClusterStore{Store: dataprocstore.NewMemoryStore(), delay: 5 * time.Millisecond}, store.NewMemoryResourceStore()), "proj")
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

// pollRESTJobTerminal reads a job through the REST surface until it reaches a
// terminal state, exercising the lazy job state machine one hop per read.
func pollRESTJobTerminal(t *testing.T, p *Provider, jobID string) map[string]any {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 16; i++ {
		got, err := p.GetJob(ctx, testNR(map[string]any{"region": "us-central1", "jobId": jobID}))
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		switch got.Data["status"].(map[string]any)["state"] {
		case "DONE", "ERROR", "CANCELLED":
			return got.Data
		}
	}
	t.Fatalf("job %s did not reach a terminal state", jobID)
	return nil
}

func TestSubmitJob_MockProgression(t *testing.T) {
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
	if j["status"].(map[string]any)["state"] != "PENDING" {
		t.Fatalf("expected PENDING on submit, got %v", j["status"])
	}
	if j["done"] != false {
		t.Fatalf("expected done=false on submit, got %v", j["done"])
	}

	got := pollRESTJobTerminal(t, p, "j1")
	if got["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("expected DONE, got %v", got)
	}
	if got["done"] != true {
		t.Fatalf("expected done=true, got %v", got["done"])
	}
}

// TestSubmitJob_SchedulingRoundTrip verifies Job.scheduling is parsed from the
// REST job body and echoed back on the response.
func TestSubmitJob_SchedulingRoundTrip(t *testing.T) {
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
				"reference":  map[string]any{"projectId": "proj", "jobId": "j-sched"},
				"placement":  map[string]any{"clusterName": "c1"},
				"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
				"scheduling": map[string]any{"maxFailuresPerHour": 3, "maxFailuresTotal": 9},
			},
		},
	}))
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	sched, ok := resp.Data["scheduling"].(map[string]any)
	if !ok {
		t.Fatalf("scheduling not rendered: %v", resp.Data["scheduling"])
	}
	if sched["maxFailuresPerHour"] != int32(3) {
		t.Fatalf("maxFailuresPerHour = %v", sched["maxFailuresPerHour"])
	}
	if sched["maxFailuresTotal"] != int32(9) {
		t.Fatalf("maxFailuresTotal = %v", sched["maxFailuresTotal"])
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

func TestSubmitJob_UnsupportedExtraTypes(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))

	for _, typ := range []string{"prestoJob", "trinoJob", "flinkJob"} {
		job := map[string]any{
			"reference": map[string]any{"jobId": "j-" + typ},
			"placement": map[string]any{"clusterName": "c1"},
		}
		switch typ {
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

// TestSubmitJob_SparkSqlJobAccepted pins that sparkSqlJob is no longer
// fail-loud: it is accepted and left non-terminal by the mock executor, and the
// type-job body is echoed back.
func TestSubmitJob_SparkSqlJobAccepted(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))

	resp, err := p.SubmitJob(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{"job": map[string]any{
			"reference":   map[string]any{"jobId": "j-sql"},
			"placement":   map[string]any{"clusterName": "c1"},
			"sparkSqlJob": map[string]any{"queryList": map[string]any{"queries": []any{"SELECT 1"}}},
		}},
	}))
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if got := resp.Data["status"].(map[string]any)["state"]; got == "ERROR" {
		t.Fatalf("sparkSqlJob must not fail loud, got %v", resp.Data["status"])
	}
	if resp.Data["sparkSqlJob"] == nil {
		t.Fatalf("expected sparkSqlJob echoed in response, got %v", resp.Data)
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

// TestListJobs_FilterAndMatcher verifies the REST query params clusterName,
// filter and jobStateMatcher are honored (and an unknown matcher is 400).
func TestListJobs_FilterAndMatcher(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	for _, c := range []string{"c1", "c2"} {
		if _, err := p.CreateCluster(ctx, testNR(map[string]any{
			"region": "us-central1",
			"body":   map[string]any{"projectId": "proj", "clusterName": c},
		})); err != nil {
			t.Fatalf("CreateCluster(%s): %v", c, err)
		}
	}
	submit := func(id, cluster string, labels map[string]any) {
		t.Helper()
		if _, err := p.SubmitJob(ctx, testNR(map[string]any{
			"region": "us-central1",
			"body": map[string]any{"job": map[string]any{
				"reference":  map[string]any{"jobId": id},
				"placement":  map[string]any{"clusterName": cluster},
				"labels":     labels,
				"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
			}},
		})); err != nil {
			t.Fatalf("SubmitJob(%s): %v", id, err)
		}
	}
	submit("j1", "c1", map[string]any{"env": "staging"})
	submit("j2", "c1", map[string]any{"env": "prod"})
	submit("j3", "c2", map[string]any{"env": "staging"})

	list := func(params map[string]any) string {
		t.Helper()
		params["region"] = "us-central1"
		resp, err := p.ListJobs(ctx, testNR(params))
		if err != nil {
			t.Fatalf("ListJobs(%v): %v", params, err)
		}
		jobs, _ := resp.Data["jobs"].([]any)
		ids := make([]string, 0, len(jobs))
		for _, raw := range jobs {
			j, _ := raw.(map[string]any)
			ref, _ := j["reference"].(map[string]any)
			if id, ok := ref["jobId"].(string); ok {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		return strings.Join(ids, ",")
	}

	// Read order matters: jobs start PENDING and settle one hop per ListJobs.
	if got := list(map[string]any{"jobStateMatcher": "ACTIVE"}); got != "j1,j2,j3" {
		t.Fatalf("ACTIVE matcher = %q, want j1,j2,j3", got)
	}
	if got := list(map[string]any{"jobStateMatcher": "NON_ACTIVE"}); got != "" {
		t.Fatalf("NON_ACTIVE matcher = %q, want none", got)
	}
	if got := list(map[string]any{"clusterName": "c1"}); got != "j1,j2" {
		t.Fatalf("clusterName filter = %q, want j1,j2", got)
	}
	if got := list(map[string]any{"filter": `labels.env = staging`}); got != "j1,j3" {
		t.Fatalf("label filter = %q, want j1,j3", got)
	}
	if got := list(map[string]any{"clusterName": "c1", "filter": `labels.env = staging`}); got != "j1" {
		t.Fatalf("cluster+filter = %q, want j1", got)
	}
	if _, err := p.ListJobs(ctx, testNR(map[string]any{"region": "us-central1", "jobStateMatcher": "RUNNING"})); err == nil {
		t.Fatal("unknown jobStateMatcher should be InvalidArgument")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 400 {
		t.Fatalf("unknown jobStateMatcher error = %v, want 400", err)
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
	if op["done"] != false {
		t.Fatalf("expected an in-flight operation, got %v", op["done"])
	}
	// The operation can be fetched by its name; polling advances the job and
	// completes the operation once the job is terminal.
	opName, _ := op["name"].(string)
	opID := ""
	if i := indexOf(opName, "/operations/"); i >= 0 {
		opID = opName[i+len("/operations/"):]
	}
	var got *model.ProviderResponse
	for i := 0; i < 16; i++ {
		g, gerr := p.GetOperation(ctx, testNR(map[string]any{"region": "us-central1", "operationId": opID}))
		if gerr != nil {
			t.Fatalf("GetOperation: %v", gerr)
		}
		got = g
		if got.Data["done"] == true {
			break
		}
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

// TestOperationTTLSweep verifies the lazy operation-retention sweep: minting a
// new operation (via any LRO) deletes operations older than the TTL and keeps
// fresh ones.
func TestOperationTTLSweep(t *testing.T) {
	ctx := context.Background()
	st := dataprocstore.NewMemoryStore()
	p := NewProvider(core.NewService(st, store.NewMemoryResourceStore(), core.WithOperationTTL(time.Hour)), "proj")

	if err := st.CreateOperation(ctx, "proj", "us-central1", dataprocstore.Operation{
		ID: "stale", Done: true, CreateTime: clock.Now().UTC().Add(-2 * time.Hour),
	}); err != nil {
		t.Fatalf("seed stale op: %v", err)
	}
	if err := st.CreateOperation(ctx, "proj", "us-central1", dataprocstore.Operation{
		ID: "fresh", Done: true, CreateTime: clock.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed fresh op: %v", err)
	}

	// Creating a cluster mints an LRO, which triggers the sweep.
	if _, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	if _, err := st.GetOperation(ctx, "proj", "us-central1", "stale"); err != dataprocstore.ErrNoSuchOperation {
		t.Fatalf("stale op should have been swept, got %v", err)
	}
	if _, err := st.GetOperation(ctx, "proj", "us-central1", "fresh"); err != nil {
		t.Fatalf("fresh op should remain, got %v", err)
	}
}

// TestCreateCluster_GKEVirtualClusterConfig verifies the REST transport parses
// a virtualClusterConfig from the create body, renders it on get, and rejects a
// malformed one with 400 InvalidArgument.
func TestCreateCluster_GKEVirtualClusterConfig(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	vcc := map[string]any{
		"stagingBucket": "dataproc-staging-proj",
		"kubernetesClusterConfig": map[string]any{
			"gkeClusterConfig": map[string]any{
				"gkeClusterTarget": "projects/proj/locations/us-central1/clusters/gke-1",
			},
		},
	}
	if _, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"projectId":            "proj",
			"clusterName":          "gke-1",
			"virtualClusterConfig": vcc,
		},
	})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := p.GetCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "gke-1"}))
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	gotVCC, _ := c.Data["virtualClusterConfig"].(map[string]any)
	if gotVCC == nil {
		t.Fatalf("virtualClusterConfig missing: %v", c.Data)
	}
	if _, ok := c.Data["config"]; ok {
		t.Fatalf("GKE cluster must not render a GCE config: %v", c.Data)
	}
	kcc, _ := gotVCC["kubernetesClusterConfig"].(map[string]any)
	gke, _ := kcc["gkeClusterConfig"].(map[string]any)
	if gke["gkeClusterTarget"] != "projects/proj/locations/us-central1/clusters/gke-1" {
		t.Fatalf("gkeClusterTarget lost: %v", gotVCC)
	}

	_, err = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"projectId":            "proj",
			"clusterName":          "bad-gke",
			"virtualClusterConfig": map[string]any{"stagingBucket": "b"},
		},
	}))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
		t.Fatalf("malformed virtualClusterConfig: expected 400 InvalidArgument, got %v", err)
	}
}

// TestJobSubstateTerminalOmitted verifies a terminal job response omits
// substate (every defined dataproc.v1 substate applies only to RUNNING) while
// the RUNNING entry retained in statusHistory keeps QUEUED.
func TestJobSubstateTerminalOmitted(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := p.SubmitJob(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"job": map[string]any{
				"reference":  map[string]any{"projectId": "proj", "jobId": "j-sub"},
				"placement":  map[string]any{"clusterName": "c1"},
				"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
			},
		},
	})); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	data := pollRESTJobTerminal(t, p, "j-sub")
	status, _ := data["status"].(map[string]any)
	if status["state"] != "DONE" {
		t.Fatalf("expected DONE, got %v", status["state"])
	}
	if _, ok := status["substate"]; ok {
		t.Fatalf("terminal job status must omit substate, got %v", status)
	}
	history, _ := data["statusHistory"].([]any)
	var sawRunning bool
	for _, h := range history {
		hm, _ := h.(map[string]any)
		if hm["state"] != "RUNNING" {
			continue
		}
		sawRunning = true
		if hm["substate"] != "QUEUED" {
			t.Fatalf("history RUNNING entry substate = %v, want QUEUED", hm["substate"])
		}
	}
	if !sawRunning {
		t.Fatalf("expected a RUNNING statusHistory entry, got %v", data["statusHistory"])
	}
}
