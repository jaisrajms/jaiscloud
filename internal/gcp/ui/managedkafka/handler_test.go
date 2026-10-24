package managedkafkaui

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
	managedkafkacore "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

// mockProvider implements ProviderInterface with canned records, recording the
// arguments the handler resolves.
type mockProvider struct {
	clusters []mkstore.Cluster
	cluster  mkstore.Cluster
	topics   []mkstore.Topic
	topic    mkstore.Topic
	acls     []mkstore.Acl
	acl      mkstore.Acl
	groups   []managedkafkacore.ConsumerGroup
	group    managedkafkacore.ConsumerGroup
	err      error

	// addCreated / removeDeleted / removeAcl drive the ACL entry write results.
	addCreated    bool
	removeDeleted bool
	removeAcl     *mkstore.Acl

	gotLocation string
	gotID       string
	gotCluster  string
	scoped      bool

	gotClusterInput ClusterWriteInput
	gotTopicInput   TopicWriteInput
	gotAclInput     AclWriteInput
	gotCgInput      ConsumerGroupWriteInput
	gotEntry        AclEntry
}

func (m *mockProvider) CreateCluster(_ context.Context, _, location, id string, in ClusterWriteInput) (mkstore.Cluster, error) {
	m.gotLocation, m.gotID, m.gotClusterInput = location, id, in
	return m.cluster, m.err
}

func (m *mockProvider) UpdateCluster(_ context.Context, _, location, id string, in ClusterWriteInput) (mkstore.Cluster, error) {
	m.gotLocation, m.gotID, m.gotClusterInput = location, id, in
	return m.cluster, m.err
}

func (m *mockProvider) CreateTopic(_ context.Context, _, location, cluster, id string, in TopicWriteInput) (mkstore.Topic, error) {
	m.gotLocation, m.gotCluster, m.gotID, m.gotTopicInput = location, cluster, id, in
	return m.topic, m.err
}

func (m *mockProvider) UpdateTopic(_ context.Context, _, location, cluster, id string, in TopicWriteInput) (mkstore.Topic, error) {
	m.gotLocation, m.gotCluster, m.gotID, m.gotTopicInput = location, cluster, id, in
	return m.topic, m.err
}

func (m *mockProvider) DeleteTopic(_ context.Context, _, location, cluster, id string) error {
	m.gotLocation, m.gotCluster, m.gotID = location, cluster, id
	return m.err
}

func (m *mockProvider) CreateAcl(_ context.Context, _, location, cluster, id string, in AclWriteInput) (mkstore.Acl, error) {
	m.gotLocation, m.gotCluster, m.gotID, m.gotAclInput = location, cluster, id, in
	return m.acl, m.err
}

func (m *mockProvider) UpdateAcl(_ context.Context, _, location, cluster, id string, in AclWriteInput) (mkstore.Acl, error) {
	m.gotLocation, m.gotCluster, m.gotID, m.gotAclInput = location, cluster, id, in
	return m.acl, m.err
}

func (m *mockProvider) DeleteAcl(_ context.Context, _, location, cluster, id string) error {
	m.gotLocation, m.gotCluster, m.gotID = location, cluster, id
	return m.err
}

func (m *mockProvider) AddAclEntry(_ context.Context, _, location, cluster, id string, entry AclEntry) (mkstore.Acl, bool, error) {
	m.gotLocation, m.gotCluster, m.gotID, m.gotEntry = location, cluster, id, entry
	return m.acl, m.addCreated, m.err
}

func (m *mockProvider) RemoveAclEntry(_ context.Context, _, location, cluster, id string, entry AclEntry) (*mkstore.Acl, bool, error) {
	m.gotLocation, m.gotCluster, m.gotID, m.gotEntry = location, cluster, id, entry
	return m.removeAcl, m.removeDeleted, m.err
}

func (m *mockProvider) UpdateConsumerGroup(_ context.Context, _, location, cluster, group string, in ConsumerGroupWriteInput) (managedkafkacore.ConsumerGroup, error) {
	m.gotLocation, m.gotCluster, m.gotID, m.gotCgInput = location, cluster, group, in
	return m.group, m.err
}

func (m *mockProvider) DeleteConsumerGroup(_ context.Context, _, location, cluster, group string) error {
	m.gotLocation, m.gotCluster, m.gotID = location, cluster, group
	return m.err
}

func (m *mockProvider) ListAllClusters(_ context.Context, _ string) ([]mkstore.Cluster, error) {
	return m.clusters, m.err
}

func (m *mockProvider) GetCluster(_ context.Context, _, location, cluster string) (mkstore.Cluster, error) {
	m.gotLocation, m.gotID = location, cluster
	return m.cluster, m.err
}

func (m *mockProvider) ListAllTopics(_ context.Context, _ string) ([]mkstore.Topic, error) {
	return m.topics, m.err
}

func (m *mockProvider) ListTopics(_ context.Context, _, location, cluster string) ([]mkstore.Topic, error) {
	m.scoped, m.gotLocation, m.gotID = true, location, cluster
	return m.topics, m.err
}

func (m *mockProvider) GetTopic(_ context.Context, _, location, cluster, topic string) (mkstore.Topic, error) {
	m.gotLocation, m.gotID = location, cluster+"/"+topic
	return m.topic, m.err
}

func (m *mockProvider) ListAcls(_ context.Context, _, location, cluster string) ([]mkstore.Acl, error) {
	m.gotLocation, m.gotID = location, cluster
	return m.acls, m.err
}

func (m *mockProvider) ListConsumerGroups(_ context.Context, _, location, cluster string) ([]managedkafkacore.ConsumerGroup, error) {
	m.gotLocation, m.gotID = location, cluster
	return m.groups, m.err
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
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func cluster(name, location string) mkstore.Cluster {
	return mkstore.Cluster{
		ProjectID:        "test-project",
		Location:         location,
		Name:             name,
		BootstrapAddress: "bootstrap." + name + "." + location + ".managedkafka.test-project.cloud.goog",
		Labels:           map[string]string{"env": "test"},
		Config:           json.RawMessage(`{"capacityConfig":{"vcpuCount":3}}`),
		CreateTime:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func topicStore(name, location, clusterName string) mkstore.Topic {
	return mkstore.Topic{
		ProjectID:         "test-project",
		Location:          location,
		ClusterName:       clusterName,
		Name:              name,
		PartitionCount:    3,
		ReplicationFactor: 2,
		Config:            json.RawMessage(`{"configs":{"retention.ms":"3600000"}}`),
		CreateTime:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestListClusters_FlattensAcrossLocations(t *testing.T) {
	mock := &mockProvider{clusters: []mkstore.Cluster{
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
	if first.ID != "alpha" || first.Location != "europe-west1" || first.State != "ACTIVE" {
		t.Fatalf("unexpected first row: %+v", first)
	}
	if first.Name != "projects/test-project/locations/europe-west1/clusters/alpha" {
		t.Fatalf("name = %q", first.Name)
	}
	if first.BootstrapAddress == "" {
		t.Fatalf("bootstrapAddress not rendered: %+v", first)
	}
	if first.Config != nil {
		t.Fatalf("list row must omit config, got %s", first.Config)
	}
}

func TestListClusters_SynthesizesBootstrapAddress(t *testing.T) {
	c := cluster("alpha", "us-central1")
	c.BootstrapAddress = "" // no live broker (mock topology)
	mock := &mockProvider{clusters: []mkstore.Cluster{c}}
	w := do(t, mock, http.MethodGet, "/clusters")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListClustersResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := "bootstrap.alpha.us-central1.managedkafka.test-project.cloud.goog"
	if resp.Clusters[0].BootstrapAddress != want {
		t.Fatalf("bootstrapAddress = %q, want %q", resp.Clusters[0].BootstrapAddress, want)
	}
}

func TestGetCluster_IncludesConfig(t *testing.T) {
	mock := &mockProvider{cluster: cluster("alpha", "us-central1")}
	w := do(t, mock, http.MethodGet, "/clusters/us-central1/alpha")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("resolved location/cluster = %q/%q", mock.gotLocation, mock.gotID)
	}
	var body Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Config == nil || !strings.Contains(string(body.Config), "capacityConfig") {
		t.Fatalf("config not rendered: %s", body.Config)
	}
}

func TestGetCluster_StripsRequestOnlyConfig(t *testing.T) {
	c := cluster("alpha", "us-central1")
	c.Config = json.RawMessage(`{"labels":{"env":"dev"},"capacityConfig":{"vcpuCount":3}}`)
	mock := &mockProvider{cluster: c}
	w := do(t, mock, http.MethodGet, "/clusters/us-central1/alpha")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var body Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if strings.Contains(string(body.Config), "labels") {
		t.Fatalf("request-only field leaked into config: %s", body.Config)
	}
	if !strings.Contains(string(body.Config), "capacityConfig") {
		t.Fatalf("capacityConfig missing: %s", body.Config)
	}
}

func TestListTopics_FlattensAndRenders(t *testing.T) {
	mock := &mockProvider{topics: []mkstore.Topic{
		topicStore("t1", "europe-west1", "alpha"),
		topicStore("t2", "us-central1", "beta"),
	}}
	w := do(t, mock, http.MethodGet, "/topics")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.scoped {
		t.Fatalf("aggregated list must not scope to a cluster")
	}
	var resp ListTopicsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 {
		t.Fatalf("total = %d, want 2", resp.Total)
	}
	first := resp.Topics[0]
	if first.ID != "t1" || first.Cluster != "alpha" || first.Location != "europe-west1" {
		t.Fatalf("unexpected first row: %+v", first)
	}
	if first.Name != "projects/test-project/locations/europe-west1/clusters/alpha/topics/t1" {
		t.Fatalf("name = %q", first.Name)
	}
	if first.Configs != nil {
		t.Fatalf("list row must omit configs, got %v", first.Configs)
	}
}

func TestListClusterTopics_Scoped(t *testing.T) {
	mock := &mockProvider{topics: []mkstore.Topic{topicStore("t1", "us-central1", "alpha")}}
	w := do(t, mock, http.MethodGet, "/clusters/us-central1/alpha/topics")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if !mock.scoped || mock.gotLocation != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("not scoped: %+v", mock)
	}
}

func TestGetTopic_IncludesConfigs(t *testing.T) {
	mock := &mockProvider{topic: topicStore("t1", "us-central1", "alpha")}
	w := do(t, mock, http.MethodGet, "/clusters/us-central1/alpha/topics/t1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotID != "alpha/t1" {
		t.Fatalf("resolved = %q/%q", mock.gotLocation, mock.gotID)
	}
	var body Topic
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Configs["retention.ms"] != "3600000" {
		t.Fatalf("configs not rendered: %v", body.Configs)
	}
}

func TestListAcls_Renders(t *testing.T) {
	mock := &mockProvider{acls: []mkstore.Acl{{
		ProjectID:    "test-project",
		Location:     "us-central1",
		ClusterName:  "alpha",
		Name:         "topic/my-topic",
		ResourceType: "TOPIC",
		ResourceName: "my-topic",
		PatternType:  "LITERAL",
		Etag:         "etag-1",
		AclEntries:   []mkstore.AclEntry{{Principal: "User:alice", PermissionType: "ALLOW", Operation: "READ", Host: "*"}},
	}}}
	w := do(t, mock, http.MethodGet, "/clusters/us-central1/alpha/acls")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListAclsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || resp.Acls[0].ResourceType != "TOPIC" || len(resp.Acls[0].Entries) != 1 {
		t.Fatalf("unexpected acls: %+v", resp)
	}
	if resp.Acls[0].Name != "projects/test-project/locations/us-central1/clusters/alpha/acls/topic/my-topic" {
		t.Fatalf("name = %q", resp.Acls[0].Name)
	}
}

func TestListConsumerGroups_FlattensOffsets(t *testing.T) {
	mock := &mockProvider{groups: []managedkafkacore.ConsumerGroup{{
		Location: "us-central1",
		Cluster:  "alpha",
		Name:     "group-1",
		Topics: map[string]*managedkafkacore.ConsumerGroupTopic{
			"projects/test-project/locations/us-central1/clusters/alpha/topics/t1": {
				Partitions: map[int32]managedkafkacore.ConsumerGroupPartition{
					1: {Offset: 42},
					0: {Offset: 7, Metadata: "m"},
				},
			},
		},
	}}}
	w := do(t, mock, http.MethodGet, "/clusters/us-central1/alpha/consumer-groups")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListConsumerGroupsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || len(resp.Groups[0].Offsets) != 2 {
		t.Fatalf("unexpected groups: %+v", resp)
	}
	// Offsets are sorted by partition.
	if resp.Groups[0].Offsets[0].Partition != 0 || resp.Groups[0].Offsets[0].Offset != 7 {
		t.Fatalf("offsets not sorted: %+v", resp.Groups[0].Offsets)
	}
	if resp.Groups[0].Name != "projects/test-project/locations/us-central1/clusters/alpha/consumerGroups/group-1" {
		t.Fatalf("name = %q", resp.Groups[0].Name)
	}
}

func TestTarget_RejectsEncodedSlash(t *testing.T) {
	for _, path := range []string{
		"/clusters/us-central1/a%2Fb",
		"/clusters/us-central1/a%2Fb/topics",
		"/clusters/us-central1/a%2Fb/acls",
		"/clusters/us-central1/a%2Fb/consumer-groups",
	} {
		w := do(t, &mockProvider{}, http.MethodGet, path)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", path, w.Code)
		}
	}
	w := do(t, &mockProvider{}, http.MethodGet, "/clusters/us%2Fcentral/alpha")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("encoded location status = %d, want 400", w.Code)
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

func aclStore(name string) mkstore.Acl {
	return mkstore.Acl{
		ProjectID:    "test-project",
		Location:     "us-central1",
		ClusterName:  "alpha",
		Name:         name,
		ResourceType: "TOPIC",
		ResourceName: "t1",
		PatternType:  "LITERAL",
		Etag:         "etag-1",
		AclEntries:   []mkstore.AclEntry{{Principal: "User:alice", PermissionType: "ALLOW", Operation: "READ", Host: "*"}},
	}
}

func groupStore(name string) managedkafkacore.ConsumerGroup {
	return managedkafkacore.ConsumerGroup{
		Location: "us-central1",
		Cluster:  "alpha",
		Name:     name,
		Topics: map[string]*managedkafkacore.ConsumerGroupTopic{
			"projects/test-project/locations/us-central1/clusters/alpha/topics/t1": {
				Partitions: map[int32]managedkafkacore.ConsumerGroupPartition{0: {Offset: 9}},
			},
		},
	}
}

func TestCreateCluster(t *testing.T) {
	mock := &mockProvider{cluster: cluster("gamma", "us-central1")}
	w := doBody(t, mock, http.MethodPost, "/clusters",
		`{"id":"gamma","location":"us-central1","labels":{"env":"dev"},"config":{"capacityConfig":{"vcpuCount":3}}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotID != "gamma" {
		t.Fatalf("resolved = %q/%q", mock.gotLocation, mock.gotID)
	}
	if mock.gotClusterInput.Labels["env"] != "dev" {
		t.Fatalf("labels not decoded: %+v", mock.gotClusterInput)
	}
	var body Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ID != "gamma" || body.Config == nil {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestCreateCluster_RequiresLocationAndID(t *testing.T) {
	mock := &mockProvider{}
	for _, body := range []string{`{"id":"gamma"}`, `{"location":"us-central1"}`} {
		w := doBody(t, mock, http.MethodPost, "/clusters", body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d, want 400", body, w.Code)
		}
	}
}

func TestUpdateCluster(t *testing.T) {
	mock := &mockProvider{cluster: cluster("alpha", "us-central1")}
	w := doBody(t, mock, http.MethodPut, "/clusters/us-central1/alpha", `{"labels":{"env":"prod"}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotID != "alpha" {
		t.Fatalf("resolved = %q/%q", mock.gotLocation, mock.gotID)
	}
	if mock.gotClusterInput.Labels["env"] != "prod" {
		t.Fatalf("labels not decoded: %+v", mock.gotClusterInput)
	}
}

func TestCreateTopic(t *testing.T) {
	mock := &mockProvider{topic: topicStore("t1", "us-central1", "alpha")}
	w := doBody(t, mock, http.MethodPost, "/clusters/us-central1/alpha/topics",
		`{"id":"t1","partitionCount":3,"replicationFactor":2,"configs":{"retention.ms":"3600000"}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotLocation != "us-central1" || mock.gotCluster != "alpha" || mock.gotID != "t1" {
		t.Fatalf("resolved = %q/%q/%q", mock.gotLocation, mock.gotCluster, mock.gotID)
	}
	in := mock.gotTopicInput
	if in.PartitionCount != 3 || in.ReplicationFactor != 2 || in.Configs["retention.ms"] != "3600000" {
		t.Fatalf("input not decoded: %+v", in)
	}
}

func TestCreateTopic_RequiresID(t *testing.T) {
	mock := &mockProvider{}
	w := doBody(t, mock, http.MethodPost, "/clusters/us-central1/alpha/topics", `{"partitionCount":3}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUpdateTopic(t *testing.T) {
	mock := &mockProvider{topic: topicStore("t1", "us-central1", "alpha")}
	w := doBody(t, mock, http.MethodPut, "/clusters/us-central1/alpha/topics/t1",
		`{"partitionCount":3,"replicationFactor":2,"configs":{"retention.ms":"600000"}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotID != "t1" || mock.gotTopicInput.Configs["retention.ms"] != "600000" {
		t.Fatalf("update not decoded: %q %+v", mock.gotID, mock.gotTopicInput)
	}
}

func TestDeleteTopic(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/clusters/us-central1/alpha/topics/t1")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.gotCluster != "alpha" || mock.gotID != "t1" {
		t.Fatalf("resolved = %q/%q", mock.gotCluster, mock.gotID)
	}
}

func TestCreateAcl(t *testing.T) {
	mock := &mockProvider{acl: aclStore("topic/t1")}
	w := doBody(t, mock, http.MethodPost, "/clusters/us-central1/alpha/acls",
		`{"id":"topic/t1","aclEntries":[{"principal":"User:alice","permissionType":"ALLOW","operation":"READ","host":"*"}]}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.gotID != "topic/t1" || len(mock.gotAclInput.Entries) != 1 {
		t.Fatalf("acl create not decoded: %q %+v", mock.gotID, mock.gotAclInput)
	}
	if mock.gotAclInput.Entries[0].Operation != "READ" {
		t.Fatalf("entry not decoded: %+v", mock.gotAclInput.Entries)
	}
}

func TestCreateAcl_RequiresID(t *testing.T) {
	mock := &mockProvider{}
	w := doBody(t, mock, http.MethodPost, "/clusters/us-central1/alpha/acls", `{"aclEntries":[]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUpdateAcl_DecodesSlashInID(t *testing.T) {
	mock := &mockProvider{acl: aclStore("topic/t1")}
	w := doBody(t, mock, http.MethodPut, "/clusters/us-central1/alpha/acls/topic%2Ft1",
		`{"etag":"etag-1","aclEntries":[{"principal":"User:bob","permissionType":"ALLOW","operation":"WRITE"}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotID != "topic/t1" {
		t.Fatalf("acl id = %q, want topic/t1", mock.gotID)
	}
	if mock.gotAclInput.Etag != "etag-1" || mock.gotAclInput.Entries[0].Principal != "User:bob" {
		t.Fatalf("update not decoded: %+v", mock.gotAclInput)
	}
}

func TestDeleteAcl_DecodesSlashInID(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/clusters/us-central1/alpha/acls/topic%2Ft1")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.gotID != "topic/t1" {
		t.Fatalf("acl id = %q, want topic/t1", mock.gotID)
	}
}

func TestAddAclEntry(t *testing.T) {
	mock := &mockProvider{acl: aclStore("cluster"), addCreated: true}
	w := doBody(t, mock, http.MethodPost, "/clusters/us-central1/alpha/acls/cluster/entries",
		`{"principal":"User:alice","permissionType":"ALLOW","operation":"READ"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotID != "cluster" || mock.gotEntry.Operation != "READ" {
		t.Fatalf("entry not decoded: %q %+v", mock.gotID, mock.gotEntry)
	}
	var resp AddAclEntryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.AclCreated || resp.Acl.ID != "cluster" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestRemoveAclEntry_DeletesLastEntry(t *testing.T) {
	mock := &mockProvider{removeDeleted: true}
	w := doBody(t, mock, http.MethodDelete, "/clusters/us-central1/alpha/acls/cluster/entries",
		`{"principal":"User:alice","permissionType":"ALLOW","operation":"READ"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp RemoveAclEntryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.AclDeleted || resp.Acl != nil {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestRemoveAclEntry_KeepsAcl(t *testing.T) {
	a := aclStore("cluster")
	mock := &mockProvider{removeAcl: &a}
	w := doBody(t, mock, http.MethodDelete, "/clusters/us-central1/alpha/acls/cluster/entries",
		`{"principal":"User:alice","permissionType":"ALLOW","operation":"READ"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp RemoveAclEntryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.AclDeleted || resp.Acl == nil || resp.Acl.ID != "cluster" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestUpdateConsumerGroup(t *testing.T) {
	mock := &mockProvider{group: groupStore("g1")}
	w := doBody(t, mock, http.MethodPut, "/clusters/us-central1/alpha/consumer-groups/g1",
		`{"offsets":[{"topic":"t1","partition":0,"offset":9}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotID != "g1" || len(mock.gotCgInput.Offsets) != 1 || mock.gotCgInput.Offsets[0].Offset != 9 {
		t.Fatalf("update not decoded: %q %+v", mock.gotID, mock.gotCgInput)
	}
}

func TestDeleteConsumerGroup(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/clusters/us-central1/alpha/consumer-groups/g1")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", w.Code, w.Body.String())
	}
	if mock.gotID != "g1" {
		t.Fatalf("group = %q, want g1", mock.gotID)
	}
}
