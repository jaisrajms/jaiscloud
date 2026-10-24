package dataprocui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

	gotRegion string
	gotID     string
	gotInput  dataproccore.ClusterInput
	deleted   bool
	created   bool
	action    string
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

func (m *mockProvider) GetJob(_ context.Context, _, region, id string) (dpstore.Job, error) {
	m.gotRegion, m.gotID = region, id
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
