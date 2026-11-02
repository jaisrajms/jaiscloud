package dataprocui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	dataproccore "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// mockProvider implements ProviderInterface with canned records, recording the
// arguments the handler resolves.
type mockProvider struct {
	clusters  []dpstore.Cluster
	cluster   dpstore.Cluster
	jobs      []dpstore.Job
	job       dpstore.Job
	templates []dpstore.WorkflowTemplate
	template  dpstore.WorkflowTemplate
	err       error

	gotRegion   string
	gotID       string
	gotInput    dataproccore.ClusterInput
	gotMask     []string
	gotJob      dataproccore.JobInput
	gotTemplate dataproccore.WorkflowTemplateInput
	gotVersion  int32
	gotParams   map[string]string
	gotOp       dpstore.Operation
	deleted     bool
	created     bool
	action      string
}

func (m *mockProvider) ListAllClusters(_ context.Context, _ string) ([]dpstore.Cluster, error) {
	return m.clusters, m.err
}

func (m *mockProvider) GetCluster(_ context.Context, _, region, name string) (dpstore.Cluster, error) {
	m.gotRegion, m.gotID = region, name
	return m.cluster, m.err
}

func (m *mockProvider) StartCluster(_ context.Context, _, region, name string) (dpstore.Cluster, error) {
	return m.mutateCluster("start", region, name)
}

func (m *mockProvider) StopCluster(_ context.Context, _, region, name string) (dpstore.Cluster, error) {
	return m.mutateCluster("stop", region, name)
}

func (m *mockProvider) mutateCluster(action, region, name string) (dpstore.Cluster, error) {
	m.action, m.gotRegion, m.gotID = action, region, name
	return m.cluster, m.err
}

func (m *mockProvider) DeleteCluster(_ context.Context, _, region, name string) error {
	m.gotRegion, m.gotID, m.deleted = region, name, true
	return m.err
}

func (m *mockProvider) CreateCluster(_ context.Context, _, region, name string, in dataproccore.ClusterInput) (dpstore.Cluster, error) {
	m.gotRegion, m.gotID, m.gotInput, m.created = region, name, in, true
	return m.cluster, m.err
}

func (m *mockProvider) ListAllJobs(_ context.Context, _ string) ([]dpstore.Job, error) {
	return m.jobs, m.err
}

func (m *mockProvider) UpdateCluster(_ context.Context, _, region, name string, in dataproccore.ClusterInput, mask []string) (dpstore.Cluster, error) {
	m.action, m.gotRegion, m.gotID, m.gotInput, m.gotMask = "update", region, name, in, mask
	return m.cluster, m.err
}

func (m *mockProvider) GetJob(_ context.Context, _, region, id string) (dpstore.Job, error) {
	m.gotRegion, m.gotID = region, id
	return m.job, m.err
}

func (m *mockProvider) SubmitJob(_ context.Context, _, region string, in dataproccore.JobInput) (dpstore.Job, error) {
	m.action, m.gotRegion, m.gotJob = "submit", region, in
	return m.job, m.err
}

func (m *mockProvider) CancelJob(_ context.Context, _, region, id string) (dpstore.Job, error) {
	m.action, m.gotRegion, m.gotID = "cancel", region, id
	return m.job, m.err
}

func (m *mockProvider) ListAllWorkflowTemplates(_ context.Context, _ string) ([]dpstore.WorkflowTemplate, error) {
	return m.templates, m.err
}

func (m *mockProvider) GetWorkflowTemplate(_ context.Context, _, region, id string) (dpstore.WorkflowTemplate, error) {
	m.gotRegion, m.gotID = region, id
	return m.template, m.err
}

func (m *mockProvider) CreateWorkflowTemplate(_ context.Context, _, region string, in dataproccore.WorkflowTemplateInput) (dpstore.WorkflowTemplate, error) {
	m.action, m.gotRegion, m.gotTemplate = "createTemplate", region, in
	return m.template, m.err
}

func (m *mockProvider) UpdateWorkflowTemplate(_ context.Context, _, region string, in dataproccore.WorkflowTemplateInput) (dpstore.WorkflowTemplate, error) {
	m.action, m.gotRegion, m.gotTemplate = "updateTemplate", region, in
	return m.template, m.err
}

func (m *mockProvider) DeleteWorkflowTemplate(_ context.Context, _, region, id string, version int32) error {
	m.action, m.gotRegion, m.gotID, m.gotVersion, m.deleted = "deleteTemplate", region, id, version, true
	return m.err
}

func (m *mockProvider) InstantiateWorkflowTemplate(_ context.Context, _, region, id string, version int32, params map[string]string) (dpstore.Operation, error) {
	m.action, m.gotRegion, m.gotID, m.gotVersion, m.gotParams = "instantiate", region, id, version, params
	return m.gotOp, m.err
}

func testCfg() *config.Config {
	return &config.Config{
		Port:      8080,
		UIPort:    4567,
		Region:    "global",
		AccountID: "test-project",
		ProjectID: "test-project",
		Clock:     clock.RealClock{},
	}
}

func do(t *testing.T, mock *mockProvider, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	router := BuildRouter(mock, testCfg())
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func doBody(t *testing.T, mock *mockProvider, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	router := BuildRouter(mock, testCfg())
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func cluster(name, region string) dpstore.Cluster {
	return dpstore.Cluster{
		ProjectID:   "test-project",
		Region:      region,
		Name:        name,
		ClusterUUID: "uuid-" + name,
		Labels:      map[string]string{"env": "test"},
		Status:      dpstore.ClusterStatus{State: "RUNNING", StateStartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		Config:      json.RawMessage(`{"gceClusterConfig":{"zoneUri":"z"}}`),
		CreateTime:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func jobStore(id, region, clusterName, typ string) dpstore.Job {
	return dpstore.Job{
		ProjectID:            "test-project",
		Region:               region,
		JobID:                id,
		PlacementClusterName: clusterName,
		Type:                 typ,
		TypeJob:              json.RawMessage(`{"mainClass":"com.example.Main"}`),
		Status:               dpstore.JobStatus{State: "RUNNING"},
		CreateTime:           time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func templateStore(id, region string) dpstore.WorkflowTemplate {
	return dpstore.WorkflowTemplate{
		ProjectID:  "test-project",
		Region:     region,
		TemplateID: id,
		Version:    3,
		Definition: json.RawMessage(`{"jobs":[{"stepId":"a"}]}`),
		CreateTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestListClusters_FlattensAcrossRegions(t *testing.T) {
	mock := &mockProvider{clusters: []dpstore.Cluster{
		cluster("alpha", "europe-west1"),
		cluster("beta", "us-central1"),
	}}
	w := do(t, mock, http.MethodGet, "/clusters")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListClustersResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Clusters) != 2 {
		t.Fatalf("got %+v, want 2 clusters", resp)
	}
	first := resp.Clusters[0]
	if first.ID != "alpha" || first.Region != "europe-west1" || first.Status != "RUNNING" {
		t.Fatalf("unexpected first row: %+v", first)
	}
	if first.Name != "projects/test-project/regions/europe-west1/clusters/alpha" {
		t.Fatalf("name = %q", first.Name)
	}
	if first.Config != nil {
		t.Fatalf("list row must omit config, got %s", first.Config)
	}
}

func TestGetCluster_IncludesConfig(t *testing.T) {
	mock := &mockProvider{cluster: cluster("alpha", "us-central1")}
	w := do(t, mock, http.MethodGet, "/clusters/us-central1/alpha")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotRegion != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("resolved region/cluster = %q/%q", mock.gotRegion, mock.gotID)
	}
	var body Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Config == nil || !strings.Contains(string(body.Config), "gceClusterConfig") {
		t.Fatalf("config not rendered: %s", body.Config)
	}
}

func TestGetCluster_IncludesNamespace(t *testing.T) {
	c := cluster("gke-1", "us-central1")
	c.Config = nil // a GKE virtual cluster carries no GCE ClusterConfig
	c.VirtualClusterConfig = json.RawMessage(`{"kubernetesClusterConfig":{"kubernetesNamespace":"team-a"}}`)
	c.Namespace = "team-a"
	c.NamespaceOwned = true
	mock := &mockProvider{cluster: c}

	w := do(t, mock, http.MethodGet, "/clusters/us-central1/gke-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var body Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.GKEBacked {
		t.Fatalf("cluster should be GKE-backed: %+v", body)
	}
	if body.Namespace != "team-a" {
		t.Fatalf("namespace = %q, want team-a", body.Namespace)
	}
	if !body.NamespaceOwned {
		t.Fatalf("namespaceOwned = false, want true")
	}
	if body.KubernetesNamespace != "team-a" {
		t.Fatalf("requested kubernetesNamespace = %q, want team-a", body.KubernetesNamespace)
	}
}

// TestGetCluster_RendersVirtualClusterLikeTheAPI pins that a console detail read
// renders virtualClusterConfig through the same core helper as config: the
// input-only nodePoolConfig that ClusterJSON strips must not be echoed by the
// console either, and the rendered object must match the API's byte for byte.
func TestGetCluster_RendersVirtualClusterLikeTheAPI(t *testing.T) {
	c := cluster("gke-1", "us-central1")
	c.Config = nil // a GKE virtual cluster carries no GCE ClusterConfig
	c.VirtualClusterConfig = json.RawMessage(`{"kubernetesClusterConfig":{` +
		`"kubernetesNamespace":"team-a","gkeClusterConfig":{` +
		`"gkeClusterTarget":"projects/p/locations/us-central1/clusters/gke-1",` +
		`"nodePoolTarget":[{"nodePool":"projects/p/locations/us-central1/clusters/gke-1/nodePools/pool-a",` +
		`"roles":["DEFAULT"],"nodePoolConfig":{"config":{"machineType":"e2-medium"}}}]}}}`)
	mock := &mockProvider{cluster: c}

	w := do(t, mock, http.MethodGet, "/clusters/us-central1/gke-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var body Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if strings.Contains(string(body.VirtualClusterConfig), "nodePoolConfig") {
		t.Fatalf("input-only nodePoolConfig echoed: %s", body.VirtualClusterConfig)
	}
	if !strings.Contains(string(body.VirtualClusterConfig), "pool-a") {
		t.Fatalf("nodePoolTarget lost: %s", body.VirtualClusterConfig)
	}

	want, err := json.Marshal(dataproccore.ClusterJSON(c)["virtualClusterConfig"])
	if err != nil {
		t.Fatalf("marshal core rendering: %v", err)
	}
	var gotObj, wantObj any
	if err := json.Unmarshal(body.VirtualClusterConfig, &gotObj); err != nil {
		t.Fatalf("decode rendered virtualClusterConfig: %v", err)
	}
	if err := json.Unmarshal(want, &wantObj); err != nil {
		t.Fatalf("decode core rendering: %v", err)
	}
	if !reflect.DeepEqual(gotObj, wantObj) {
		t.Fatalf("console rendering != core rendering:\n got %s\nwant %s", body.VirtualClusterConfig, want)
	}
}

func TestGetCluster_GCEHasNoRequestedNamespace(t *testing.T) {
	c := cluster("gce-1", "us-central1") // GCE config; no virtualClusterConfig
	c.Namespace = "derived-gce-1"
	c.NamespaceOwned = true
	mock := &mockProvider{cluster: c}

	w := do(t, mock, http.MethodGet, "/clusters/us-central1/gce-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var body Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.GKEBacked {
		t.Fatalf("GCE cluster must not be GKE-backed: %+v", body)
	}
	if body.Namespace != "derived-gce-1" {
		t.Fatalf("namespace = %q, want derived-gce-1", body.Namespace)
	}
	if body.KubernetesNamespace != "" {
		t.Fatalf("GCE cluster must have no requested namespace, got %q", body.KubernetesNamespace)
	}
}

func TestGetCluster_RequestedNamespaceWithoutEffective(t *testing.T) {
	c := cluster("gke-1", "us-central1")
	c.Config = nil
	c.VirtualClusterConfig = json.RawMessage(`{"kubernetesClusterConfig":{"kubernetesNamespace":"team-a"}}`)
	// Mock execution / failed provisioning: no effective namespace recorded, so
	// the effective field is omitted (omitempty) while the requested one shows.
	mock := &mockProvider{cluster: c}

	w := do(t, mock, http.MethodGet, "/clusters/us-central1/gke-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var body Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Namespace != "" || body.NamespaceOwned {
		t.Fatalf("no effective namespace expected: %+v", body)
	}
	if body.KubernetesNamespace != "team-a" {
		t.Fatalf("requested kubernetesNamespace = %q, want team-a", body.KubernetesNamespace)
	}
}

func TestListClusters_OmitsNamespace(t *testing.T) {
	c := cluster("gke-1", "us-central1")
	c.VirtualClusterConfig = json.RawMessage(`{"kubernetesClusterConfig":{"kubernetesNamespace":"team-a"}}`)
	c.Namespace = "team-a"
	c.NamespaceOwned = true
	mock := &mockProvider{clusters: []dpstore.Cluster{c}}

	w := do(t, mock, http.MethodGet, "/clusters")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListClustersResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Clusters) != 1 {
		t.Fatalf("got %d clusters, want 1", len(resp.Clusters))
	}
	if got := resp.Clusters[0]; got.Namespace != "" || got.NamespaceOwned || got.KubernetesNamespace != "" {
		t.Fatalf("list row must omit namespace placement: %+v", got)
	}
}

func TestClusterActions_Dispatch(t *testing.T) {
	for _, action := range []string{"start", "stop"} {
		mock := &mockProvider{cluster: cluster("alpha", "us-central1")}
		w := do(t, mock, http.MethodPost, "/clusters/us-central1/alpha/"+action)
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200: %s", action, w.Code, w.Body.String())
		}
		if mock.action != action || mock.gotRegion != "us-central1" || mock.gotID != "alpha" {
			t.Fatalf("%s not dispatched: %+v", action, mock)
		}
	}
}

func TestDeleteCluster_NoContent(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/clusters/us-central1/alpha")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if !mock.deleted || mock.gotRegion != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("delete not dispatched: %+v", mock)
	}
}

func TestCreateCluster_GCEConfig(t *testing.T) {
	mock := &mockProvider{cluster: cluster("alpha", "us-central1")}
	body := `{"region":"us-central1","name":"alpha","config":{"gceClusterConfig":{"zoneUri":"z"}}}`
	w := doBody(t, mock, http.MethodPost, "/clusters", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if !mock.created || mock.gotRegion != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("create not dispatched: %+v", mock)
	}
	if len(mock.gotInput.Config) == 0 || mock.gotInput.VirtualClusterConfig != nil {
		t.Fatalf("config not passed through: %+v", mock.gotInput)
	}
	var got Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Config == nil || !strings.Contains(string(got.Config), "gceClusterConfig") {
		t.Fatalf("config not rendered: %s", got.Config)
	}
}

func TestCreateCluster_VirtualClusterConfig(t *testing.T) {
	mock := &mockProvider{cluster: cluster("gke-1", "us-central1")}
	body := `{"region":"us-central1","name":"gke-1","virtualClusterConfig":{` +
		`"kubernetesClusterConfig":{"kubernetesNamespace":"dataproc","gkeClusterConfig":` +
		`{"gkeClusterTarget":"projects/p/locations/us-central1/clusters/gke-1"}}}}`
	w := doBody(t, mock, http.MethodPost, "/clusters", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if len(mock.gotInput.VirtualClusterConfig) == 0 || mock.gotInput.Config != nil {
		t.Fatalf("virtualClusterConfig not passed through: %+v", mock.gotInput)
	}
	if !strings.Contains(string(mock.gotInput.VirtualClusterConfig), "dataproc") {
		t.Fatalf("kubernetesNamespace lost: %s", mock.gotInput.VirtualClusterConfig)
	}
}

func TestCreateCluster_RequiresRegionAndName(t *testing.T) {
	for _, body := range []string{`{}`, `{"region":"us-central1"}`, `{"name":"alpha"}`} {
		w := doBody(t, &mockProvider{}, http.MethodPost, "/clusters", body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d, want 400", body, w.Code)
		}
	}
}

func TestCreateCluster_InvalidJSON(t *testing.T) {
	w := doBody(t, &mockProvider{}, http.MethodPost, "/clusters", `{`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestCreateCluster_ProviderErrorMapped(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("AlreadyExists", "cluster exists", http.StatusConflict)}
	w := doBody(t, mock, http.MethodPost, "/clusters", `{"region":"us-central1","name":"alpha"}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "cluster exists") {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestListJobs_FlattensAndRenders(t *testing.T) {
	mock := &mockProvider{jobs: []dpstore.Job{
		jobStore("j1", "europe-west1", "alpha", "sparkJob"),
		jobStore("j2", "us-central1", "beta", "pysparkJob"),
	}}
	w := do(t, mock, http.MethodGet, "/jobs")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListJobsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 {
		t.Fatalf("total = %d, want 2", resp.Total)
	}
	first := resp.Jobs[0]
	if first.ID != "j1" || first.Region != "europe-west1" || first.Type != "sparkJob" || first.ClusterName != "alpha" {
		t.Fatalf("unexpected first row: %+v", first)
	}
	if first.Name != "projects/test-project/regions/europe-west1/jobs/j1" {
		t.Fatalf("name = %q", first.Name)
	}
	if first.TypeJob != nil {
		t.Fatalf("list row must omit typeJob, got %s", first.TypeJob)
	}
}

func TestGetJob_IncludesTypeJob(t *testing.T) {
	mock := &mockProvider{job: jobStore("j1", "us-central1", "alpha", "sparkJob")}
	w := do(t, mock, http.MethodGet, "/jobs/us-central1/j1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotRegion != "us-central1" || mock.gotID != "j1" {
		t.Fatalf("resolved region/job = %q/%q", mock.gotRegion, mock.gotID)
	}
	var body Job
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.TypeJob == nil || !strings.Contains(string(body.TypeJob), "mainClass") {
		t.Fatalf("typeJob not rendered: %s", body.TypeJob)
	}
}

func TestCancelJob_Dispatches(t *testing.T) {
	mock := &mockProvider{job: jobStore("j1", "us-central1", "alpha", "sparkJob")}
	w := do(t, mock, http.MethodPost, "/jobs/us-central1/j1/cancel")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.action != "cancel" || mock.gotRegion != "us-central1" || mock.gotID != "j1" {
		t.Fatalf("cancel not dispatched: %+v", mock)
	}
}

func TestListWorkflowTemplates(t *testing.T) {
	mock := &mockProvider{templates: []dpstore.WorkflowTemplate{
		templateStore("wf1", "us-central1"),
		templateStore("wf2", "europe-west1"),
	}}
	w := do(t, mock, http.MethodGet, "/workflow-templates")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListWorkflowTemplatesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 {
		t.Fatalf("total = %d, want 2", resp.Total)
	}
	first := resp.Templates[0]
	if first.ID != "wf1" || first.Version != 3 || first.Region != "us-central1" {
		t.Fatalf("unexpected first row: %+v", first)
	}
	if first.Definition != nil {
		t.Fatalf("list row must omit definition, got %s", first.Definition)
	}
}

func TestGetWorkflowTemplate_IncludesDefinition(t *testing.T) {
	mock := &mockProvider{template: templateStore("wf1", "us-central1")}
	w := do(t, mock, http.MethodGet, "/workflow-templates/us-central1/wf1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotRegion != "us-central1" || mock.gotID != "wf1" {
		t.Fatalf("resolved region/template = %q/%q", mock.gotRegion, mock.gotID)
	}
	var body WorkflowTemplate
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Definition == nil || !strings.Contains(string(body.Definition), "stepId") {
		t.Fatalf("definition not rendered: %s", body.Definition)
	}
}

func TestTarget_RejectsEncodedSlash(t *testing.T) {
	for _, path := range []string{
		"/clusters/us-central1/a%2Fb",
		"/jobs/us-central1/a%2Fb",
		"/workflow-templates/us-central1/a%2Fb",
	} {
		w := do(t, &mockProvider{}, http.MethodGet, path)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", path, w.Code)
		}
	}
}

func TestListClusters_ProviderErrorMapped(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "nope", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/clusters")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "nope") {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestUpdateCluster_PassesMaskVerbatim(t *testing.T) {
	mock := &mockProvider{cluster: cluster("alpha", "us-central1")}
	body := `{"config":{"workerConfig":{"numInstances":3}},"labels":{"env":"prod"},` +
		`"updateMask":["config.workerConfig.numInstances","labels"]}`
	w := doBody(t, mock, http.MethodPatch, "/clusters/us-central1/alpha", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.action != "update" || mock.gotRegion != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("update not dispatched: %+v", mock)
	}
	want := []string{"config.workerConfig.numInstances", "labels"}
	if !reflect.DeepEqual(mock.gotMask, want) {
		t.Fatalf("mask = %v, want %v", mock.gotMask, want)
	}
	if len(mock.gotInput.Config) == 0 || !strings.Contains(string(mock.gotInput.Config), "numInstances") {
		t.Fatalf("config not passed through: %s", mock.gotInput.Config)
	}
	if mock.gotInput.Labels["env"] != "prod" {
		t.Fatalf("labels not passed through: %v", mock.gotInput.Labels)
	}
}

func TestUpdateCluster_RequiresConfigOrLabels(t *testing.T) {
	for _, body := range []string{`{}`, `{"updateMask":["labels"]}`} {
		w := doBody(t, &mockProvider{}, http.MethodPatch, "/clusters/us-central1/alpha", body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d, want 400", body, w.Code)
		}
	}
}

func TestSubmitJob_Dispatches(t *testing.T) {
	mock := &mockProvider{job: jobStore("j1", "us-central1", "alpha", "sparkJob")}
	body := `{"region":"us-central1","job":{"reference":{"jobId":"j1"},` +
		`"placement":{"clusterName":"alpha"},"sparkJob":{"mainClass":"com.example.Main"}}}`
	w := doBody(t, mock, http.MethodPost, "/jobs", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.action != "submit" || mock.gotRegion != "us-central1" {
		t.Fatalf("submit not dispatched: %+v", mock)
	}
	if mock.gotJob.JobID != "j1" || mock.gotJob.PlacementClusterName != "alpha" || mock.gotJob.Type != "sparkJob" {
		t.Fatalf("job input = %+v", mock.gotJob)
	}
	var got Job
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.TypeJob == nil || !strings.Contains(string(got.TypeJob), "mainClass") {
		t.Fatalf("typeJob not rendered: %s", got.TypeJob)
	}
}

func TestSubmitJob_RequiresRegionAndJob(t *testing.T) {
	for _, body := range []string{
		`{}`,
		`{"job":{"sparkJob":{}}}`,
		`{"region":"us-central1"}`,
		`{"region":"us-central1","job":"spark"}`,
	} {
		w := doBody(t, &mockProvider{}, http.MethodPost, "/jobs", body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d, want 400", body, w.Code)
		}
	}
}

func TestCreateWorkflowTemplate(t *testing.T) {
	mock := &mockProvider{template: templateStore("wf1", "us-central1")}
	body := `{"region":"us-central1","id":"wf1","definition":{` +
		`"placement":{"managedCluster":{"clusterName":"alpha"}},"jobs":[{"stepId":"a"}]}}`
	w := doBody(t, mock, http.MethodPost, "/workflow-templates", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.action != "createTemplate" || mock.gotTemplate.ID != "wf1" {
		t.Fatalf("create not dispatched: %+v", mock)
	}
	if mock.gotTemplate.Definition["jobs"] == nil {
		t.Fatalf("definition not passed through: %+v", mock.gotTemplate.Definition)
	}
	var got WorkflowTemplate
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Definition == nil || !strings.Contains(string(got.Definition), "stepId") {
		t.Fatalf("definition not rendered: %s", got.Definition)
	}
}

func TestCreateWorkflowTemplate_RequiresRegionAndID(t *testing.T) {
	for _, body := range []string{`{}`, `{"region":"us-central1"}`, `{"id":"wf1"}`} {
		w := doBody(t, &mockProvider{}, http.MethodPost, "/workflow-templates", body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d, want 400", body, w.Code)
		}
	}
}

func TestUpdateWorkflowTemplate_PassesVersion(t *testing.T) {
	mock := &mockProvider{template: templateStore("wf1", "us-central1")}
	body := `{"version":3,"definition":{"placement":{"managedCluster":{"clusterName":"alpha"}},` +
		`"jobs":[{"stepId":"a"}]}}`
	w := doBody(t, mock, http.MethodPut, "/workflow-templates/us-central1/wf1", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.action != "updateTemplate" || mock.gotRegion != "us-central1" {
		t.Fatalf("update not dispatched: %+v", mock)
	}
	if mock.gotTemplate.ID != "wf1" || mock.gotTemplate.Version != 3 {
		t.Fatalf("template input = %+v", mock.gotTemplate)
	}
}

func TestUpdateWorkflowTemplate_RejectsMismatchedID(t *testing.T) {
	body := `{"id":"other","version":3,"definition":{"placement":{}}}`
	w := doBody(t, &mockProvider{}, http.MethodPut, "/workflow-templates/us-central1/wf1", body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestWorkflowTemplateConflictReSurfaces(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("Aborted", "workflow template version mismatch", http.StatusConflict)}
	body := `{"version":1,"definition":{"placement":{"managedCluster":{"clusterName":"a"}},"jobs":[{"stepId":"a"}]}}`
	w := doBody(t, mock, http.MethodPut, "/workflow-templates/us-central1/wf1", body)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "version mismatch") {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestDeleteWorkflowTemplate_VersionQuery(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/workflow-templates/us-central1/wf1?version=3")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if !mock.deleted || mock.gotID != "wf1" || mock.gotVersion != 3 {
		t.Fatalf("delete not dispatched: %+v", mock)
	}

	w = do(t, &mockProvider{}, http.MethodDelete, "/workflow-templates/us-central1/wf1?version=x")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad version status = %d, want 400", w.Code)
	}
}

func TestInstantiateWorkflowTemplate(t *testing.T) {
	mock := &mockProvider{gotOp: dpstore.Operation{ID: "op-1", ProjectID: "test-project", Region: "us-central1"}}
	body := `{"version":3,"parameters":{"date":"2026-01-01"}}`
	w := doBody(t, mock, http.MethodPost, "/workflow-templates/us-central1/wf1/instantiate", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.action != "instantiate" || mock.gotID != "wf1" || mock.gotVersion != 3 {
		t.Fatalf("instantiate not dispatched: %+v", mock)
	}
	if mock.gotParams["date"] != "2026-01-01" {
		t.Fatalf("parameters = %v", mock.gotParams)
	}
	if !strings.Contains(w.Body.String(), "projects/test-project/regions/us-central1/operations/op-1") {
		t.Fatalf("operation not rendered: %s", w.Body.String())
	}
}

func TestInstantiateWorkflowTemplate_EmptyBody(t *testing.T) {
	mock := &mockProvider{gotOp: dpstore.Operation{ID: "op-1", ProjectID: "test-project", Region: "us-central1"}}
	w := doBody(t, mock, http.MethodPost, "/workflow-templates/us-central1/wf1/instantiate", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotParams != nil {
		t.Fatalf("parameters = %v, want nil", mock.gotParams)
	}
}
