package dataproc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// gceBody is the minimal create body: a cluster must carry exactly one of
// config or virtualClusterConfig, so a test that is not about the placement
// sends the Compute-Engine shape.
func gceBody(clusterName string, labels map[string]any) map[string]any {
	body := map[string]any{
		"projectId":   "proj",
		"clusterName": clusterName,
		"config":      map[string]any{"gceClusterConfig": map[string]any{"zoneUri": "us-central1-a"}},
	}
	if labels != nil {
		body["labels"] = labels
	}
	return body
}

// decodeIAM builds the NormalizedRequest the adapter would hand the provider for
// a workflowTemplates IAM POST.
func decodeIAM(t *testing.T, verb, resourceID, body string) *model.NormalizedRequest {
	t.Helper()
	path := "/v1/projects/proj/regions/us-central1/workflowTemplates/" + resourceID + ":" + verb
	nr, err := NewCodec().Decode(httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)), []byte(body))
	if err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return nr
}

func TestWorkflowTemplateIAMProviderRoundTrip(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, err := p.core.CreateWorkflowTemplate(ctx, "proj", "us-central1", core.WorkflowTemplateInput{
		ID: "t1",
		Definition: map[string]any{
			"id":        "t1",
			"placement": map[string]any{"managedCluster": map[string]any{"clusterName": "c"}},
			"jobs": []any{
				map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/a.py"}},
			},
		},
	}); err != nil {
		t.Fatalf("CreateWorkflowTemplate: %v", err)
	}

	// An unset policy is the empty default.
	resp, err := p.WorkflowTemplateGetIamPolicy(ctx, decodeIAM(t, "getIamPolicy", "t1", "{}"))
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if resp.Data["version"] != 1 {
		t.Fatalf("default version = %v, want 1", resp.Data["version"])
	}

	body := `{"policy":{"bindings":[{"role":"roles/dataproc.worker","members":["user:a@example.com"],` +
		`"condition":{"title":"expires-soon","expression":"request.time < timestamp(\"2027-01-01T00:00:00Z\")"}}]}}`
	set, err := p.WorkflowTemplateSetIamPolicy(ctx, decodeIAM(t, "setIamPolicy", "t1", body))
	if err != nil {
		t.Fatalf("setIamPolicy: %v", err)
	}
	if set.Data["version"] != 3 {
		t.Fatalf("set version = %v, want 3", set.Data["version"])
	}

	got, err := p.WorkflowTemplateGetIamPolicy(ctx, decodeIAM(t, "getIamPolicy", "t1", "{}"))
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if got.Data["version"] != 3 {
		t.Fatalf("read-back version = %v, want 3", got.Data["version"])
	}
	bindings, _ := got.Data["bindings"].([]any)
	if len(bindings) != 1 {
		t.Fatalf("bindings = %v", got.Data["bindings"])
	}
	bm := bindings[0].(map[string]any)
	cm, _ := bm["condition"].(map[string]any)
	if cm["title"] != "expires-soon" {
		t.Fatalf("condition did not round-trip: %v", bm)
	}

	perms, err := p.WorkflowTemplateTestIamPermissions(ctx, decodeIAM(t, "testIamPermissions", "t1",
		`{"permissions":["dataproc.workflowTemplates.get","dataproc.workflowTemplates.update"]}`))
	if err != nil {
		t.Fatalf("testIamPermissions: %v", err)
	}
	gotPerms, _ := perms.Data["permissions"].([]string)
	if len(gotPerms) != 2 {
		t.Fatalf("permissions = %v", perms.Data["permissions"])
	}

	// A missing template is NotFound for every verb.
	for _, verb := range []string{"getIamPolicy", "setIamPolicy", "testIamPermissions"} {
		body := "{}"
		if verb == "testIamPermissions" {
			body = `{"permissions":["dataproc.workflowTemplates.get"]}`
		}
		nr := decodeIAM(t, verb, "absent", body)
		var err error
		switch verb {
		case "getIamPolicy":
			_, err = p.WorkflowTemplateGetIamPolicy(ctx, nr)
		case "setIamPolicy":
			_, err = p.WorkflowTemplateSetIamPolicy(ctx, nr)
		default:
			_, err = p.WorkflowTemplateTestIamPermissions(ctx, nr)
		}
		pe, ok := err.(*model.ProviderError)
		if !ok || pe.HTTPStatus != 404 {
			t.Fatalf("%s(absent) = %v, want 404", verb, err)
		}
	}
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

// TestCreateCluster_RequiresPlacement verifies Discovery's "Exactly one of
// ClusterConfig or VirtualClusterConfig must be specified": a create carrying
// neither is 400 InvalidArgument and leaves no cluster behind.
func TestCreateCluster_RequiresPlacement(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   map[string]any{"projectId": "proj", "clusterName": "c1"},
	}))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
		t.Fatalf("expected 400 InvalidArgument, got %v", err)
	}
	if _, err := p.GetCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"})); err == nil {
		t.Fatal("a rejected create must not persist a cluster")
	}
}

func TestCreateCluster_AlreadyExists(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	body := map[string]any{
		"region": "us-central1",
		"body":   gceBody("c1", nil),
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

// TestGetCluster_GCEConfigDefaults pins the wire shape a real Dataproc read
// always carries for a Compute-Engine cluster, even when the create request sent
// no config at all: config.softwareConfig.imageVersion plus the documented
// instance-group and zone defaults. The hashicorp/google provider reads
// config.softwareConfig.imageVersion without a nil check on the read-back of a
// google_dataproc_cluster with a plain cluster_config, so leaving it absent
// panics the provider (DPG38).
func TestGetCluster_GCEConfigDefaults(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	// An empty cluster_config is the shape a real client sends when it pins
	// nothing (Terraform omits every computed field, gcloud sends the flags it
	// was given); the service fills in the rest.
	if _, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"projectId":   "proj",
			"clusterName": "c1",
			"config":      map[string]any{},
		},
	})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := p.GetCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "c1"}))
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	// Compare the wire body: the response is JSON-encoded, so this also pins the
	// field names and numeric shapes clients receive.
	raw, err := json.Marshal(c.Data)
	if err != nil {
		t.Fatalf("marshal cluster: %v", err)
	}
	var body struct {
		Config struct {
			SoftwareConfig struct {
				ImageVersion string `json:"imageVersion"`
			} `json:"softwareConfig"`
			GceClusterConfig struct {
				ZoneUri string `json:"zoneUri"`
			} `json:"gceClusterConfig"`
			MasterConfig struct {
				NumInstances   int    `json:"numInstances"`
				MachineTypeUri string `json:"machineTypeUri"`
			} `json:"masterConfig"`
			WorkerConfig struct {
				NumInstances int `json:"numInstances"`
			} `json:"workerConfig"`
		} `json:"config"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("unmarshal cluster: %v", err)
	}
	if body.Config.SoftwareConfig.ImageVersion == "" {
		t.Fatalf("config.softwareConfig.imageVersion missing: %s", raw)
	}
	if body.Config.GceClusterConfig.ZoneUri == "" {
		t.Fatalf("config.gceClusterConfig.zoneUri missing: %s", raw)
	}
	if body.Config.MasterConfig.NumInstances != 1 || body.Config.WorkerConfig.NumInstances != 2 {
		t.Fatalf("instance group counts = %d/%d, want 1/2", body.Config.MasterConfig.NumInstances, body.Config.WorkerConfig.NumInstances)
	}
	wantMachine := "https://www.googleapis.com/compute/v1/projects/proj/zones/us-central1-a/machineTypes/n1-standard-4"
	if body.Config.MasterConfig.MachineTypeUri != wantMachine {
		t.Fatalf("masterConfig.machineTypeUri = %q, want %q", body.Config.MasterConfig.MachineTypeUri, wantMachine)
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
		"body":   gceBody("c1", map[string]any{"env": "dev"}),
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
		"body":   gceBody("c1", nil),
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
		"body":   gceBody("c1", nil),
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
		"body":   gceBody("c1", nil),
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

// TestUpdateJob_Labels covers the REST jobs.patch handler (the body is the Job
// itself and updateMask is a query parameter) and that labels persist.
func TestUpdateJob_Labels(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   gceBody("c1", nil),
	}))
	if _, err := p.SubmitJob(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"job": map[string]any{
				"reference":  map[string]any{"projectId": "proj", "jobId": "j1"},
				"placement":  map[string]any{"clusterName": "c1"},
				"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
			},
		},
	})); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	resp, err := p.UpdateJob(ctx, testNR(map[string]any{
		"region":     "us-central1",
		"jobId":      "j1",
		"updateMask": "labels",
		"body":       map[string]any{"labels": map[string]any{"env": "prod"}},
	}))
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if labels, _ := resp.Data["labels"].(map[string]string); labels["env"] != "prod" {
		t.Fatalf("response labels = %v", resp.Data["labels"])
	}

	got, err := p.GetJob(ctx, testNR(map[string]any{"region": "us-central1", "jobId": "j1"}))
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if labels, _ := got.Data["labels"].(map[string]string); labels["env"] != "prod" {
		t.Fatalf("persisted labels = %v", got.Data["labels"])
	}
}

// TestSubmitJob_SchedulingRoundTrip verifies Job.scheduling is parsed from the
// REST job body and echoed back on the response.
func TestSubmitJob_SchedulingRoundTrip(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   gceBody("c1", nil),
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
		"body":   gceBody("c1", nil),
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
		"body":   gceBody("c1", nil),
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
		"body":   gceBody("c1", nil),
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
		"body":   gceBody("c1", nil),
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
		"body":   gceBody("c1", nil),
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
			"body":   gceBody(c, nil),
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

// TestListClusters_FilterAndPageSize verifies the REST query params filter and
// pageSize are honored (and a negative pageSize is 400).
func TestListClusters_FilterAndPageSize(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	create := func(name string, labels map[string]any) {
		t.Helper()
		if _, err := p.CreateCluster(ctx, testNR(map[string]any{
			"region": "us-central1",
			"body":   gceBody(name, labels),
		})); err != nil {
			t.Fatalf("CreateCluster(%s): %v", name, err)
		}
	}
	create("c1", map[string]any{"env": "staging"})
	create("c2", map[string]any{"env": "prod"})
	create("c3", map[string]any{"env": "staging"})

	names := func(params map[string]any) string {
		t.Helper()
		params["region"] = "us-central1"
		resp, err := p.ListClusters(ctx, testNR(params))
		if err != nil {
			t.Fatalf("ListClusters(%v): %v", params, err)
		}
		clusters, _ := resp.Data["clusters"].([]any)
		out := make([]string, 0, len(clusters))
		for _, raw := range clusters {
			c, _ := raw.(map[string]any)
			if n, ok := c["clusterName"].(string); ok {
				out = append(out, n)
			}
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}

	if got := names(map[string]any{}); got != "c1,c2,c3" {
		t.Fatalf("unfiltered list = %q, want c1,c2,c3", got)
	}
	if got := names(map[string]any{"filter": `labels.env = staging`}); got != "c1,c3" {
		t.Fatalf("label filter = %q, want c1,c3", got)
	}
	if got := names(map[string]any{"filter": `status.state = ACTIVE`}); got != "c1,c2,c3" {
		t.Fatalf("ACTIVE filter = %q, want c1,c2,c3", got)
	}
	if got := names(map[string]any{"filter": `status.state = INACTIVE`}); got != "" {
		t.Fatalf("INACTIVE filter = %q, want none", got)
	}

	if _, err := p.ListClusters(ctx, testNR(map[string]any{"region": "us-central1", "filter": "bogus = 1"})); err == nil {
		t.Fatal("malformed filter should be InvalidArgument")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 400 {
		t.Fatalf("malformed filter error = %v, want 400", err)
	}
	if _, err := p.ListClusters(ctx, testNR(map[string]any{"region": "us-central1", "pageSize": -1})); err == nil {
		t.Fatal("negative pageSize should be InvalidArgument")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 400 {
		t.Fatalf("negative pageSize error = %v, want 400", err)
	}
}

func TestSubmitJobAsOperation(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _ = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body":   gceBody("c1", nil),
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
		"body":   gceBody("c1", nil),
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
		"body":   gceBody("c1", nil),
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

// TestListNonIntegerPageSize pins the int32 query binding: a non-integer
// pageSize is INVALID_ARGUMENT rather than a silent zero.
func TestListNonIntegerPageSize(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	calls := map[string]func(*model.NormalizedRequest) (*model.ProviderResponse, error){
		"clusters": func(n *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return p.ListClusters(ctx, n)
		},
		"jobs": func(n *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return p.ListJobs(ctx, n)
		},
		"workflowTemplates": func(n *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return p.ListWorkflowTemplates(ctx, n)
		},
	}
	for name, call := range calls {
		for _, v := range []any{"abc", "-0.5", "1.5"} {
			_, err := call(testNR(map[string]any{"region": "us-central1", "pageSize": v}))
			perr, ok := err.(*model.ProviderError)
			if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
				t.Errorf("%s pageSize=%#v err = %v, want InvalidArgument/400", name, v, err)
			}
		}
		if _, err := call(testNR(map[string]any{"region": "us-central1", "pageSize": "5"})); err != nil {
			t.Errorf("%s pageSize=5: %v", name, err)
		}
	}
}

// TestCreateCluster_PHSHistoryServer verifies the REST create path parses
// sparkHistoryServerConfig.dataprocCluster, validates it against the referenced
// Dataproc cluster, and returns the documented spark.eventLog.* properties.
func TestCreateCluster_PHSHistoryServer(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"projectId":   "proj",
			"clusterName": "phs",
			"config":      map[string]any{"softwareConfig": map[string]any{"imageVersion": "2.2.0"}},
		},
	})); err != nil {
		t.Fatalf("create PHS source: %v", err)
	}

	ref := "projects/proj/regions/us-central1/clusters/phs"
	vcc := map[string]any{
		"stagingBucket": "dataproc-staging-proj",
		"kubernetesClusterConfig": map[string]any{
			"gkeClusterConfig": map[string]any{
				"gkeClusterTarget": "projects/proj/locations/us-central1/clusters/gke-1",
			},
		},
		"auxiliaryServicesConfig": map[string]any{
			"sparkHistoryServerConfig": map[string]any{"dataprocCluster": ref},
		},
	}
	if _, err := p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"projectId":            "proj",
			"clusterName":          "gke-phs",
			"virtualClusterConfig": vcc,
		},
	})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := p.GetCluster(ctx, testNR(map[string]any{"region": "us-central1", "clusterName": "gke-phs"}))
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	gotVCC, _ := c.Data["virtualClusterConfig"].(map[string]any)
	kcc, _ := gotVCC["kubernetesClusterConfig"].(map[string]any)
	software, _ := kcc["kubernetesSoftwareConfig"].(map[string]any)
	props, _ := software["properties"].(map[string]any)
	if props["spark:spark.eventLog.enabled"] != "true" {
		t.Fatalf("spark.eventLog.enabled = %v", props["spark:spark.eventLog.enabled"])
	}
	if props["spark:spark.eventLog.dir"] != "gs://dataproc-staging-proj/gke-phs/spark-job-history" {
		t.Fatalf("spark.eventLog.dir = %v", props["spark:spark.eventLog.dir"])
	}

	// A missing PHS cluster is a 404 through the REST error mapping.
	vcc["auxiliaryServicesConfig"] = map[string]any{
		"sparkHistoryServerConfig": map[string]any{
			"dataprocCluster": "projects/proj/regions/us-central1/clusters/missing",
		},
	}
	_, err = p.CreateCluster(ctx, testNR(map[string]any{
		"region": "us-central1",
		"body": map[string]any{
			"projectId":            "proj",
			"clusterName":          "gke-missing-phs",
			"virtualClusterConfig": vcc,
		},
	}))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 404 || pe.Code != "NotFound" {
		t.Fatalf("missing PHS cluster: expected 404 NotFound, got %v", err)
	}
}
