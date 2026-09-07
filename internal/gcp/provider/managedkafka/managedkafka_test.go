package managedkafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/gcp/resource"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func newProvider() *Provider {
	return New(mkstore.NewMemoryStore())
}

func createParams(location, clusterID string, body map[string]any) map[string]any {
	return map[string]any{"location": location, "clusterId": clusterID, "body": body}
}

func TestClusterCRUDAndLRO(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.CreateCluster(ctx, newNR(createParams("us-central1", "c1", map[string]any{
		"labels": map[string]any{"env": "test"},
	})))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if resp.Data["done"] != true {
		t.Fatalf("expected done=true, got %v", resp.Data["done"])
	}
	created, _ := resp.Data["response"].(map[string]any)
	wantName := "projects/proj/locations/us-central1/clusters/c1"
	if created["name"] != wantName {
		t.Errorf("name = %v, want %v", created["name"], wantName)
	}
	if created["state"] != "ACTIVE" {
		t.Errorf("state = %v, want ACTIVE", created["state"])
	}
	labels, _ := created["labels"].(map[string]string)
	if labels["env"] != "test" {
		t.Errorf("labels = %v, want env=test", created["labels"])
	}

	// Get.
	getResp, err := p.GetCluster(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1"}))
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if getResp.Data["name"] != wantName {
		t.Errorf("get name = %v, want %v", getResp.Data["name"], wantName)
	}

	// Update.
	updResp, err := p.UpdateCluster(ctx, newNR(createParams("us-central1", "c1", map[string]any{
		"labels": map[string]any{"env": "prod"},
	})))
	if err != nil {
		t.Fatalf("UpdateCluster: %v", err)
	}
	updated, _ := updResp.Data["response"].(map[string]any)
	if labels, _ := updated["labels"].(map[string]string); labels["env"] != "prod" {
		t.Errorf("updated labels = %v, want env=prod", updated["labels"])
	}

	// List.
	listResp, err := p.ListClusters(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	items, _ := listResp.Data["clusters"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(items))
	}

	// Delete.
	delResp, err := p.DeleteCluster(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1"}))
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if delResp.Data["done"] != true {
		t.Errorf("delete done = %v, want true", delResp.Data["done"])
	}

	if _, err := p.GetCluster(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1"})); err == nil {
		t.Fatal("expected NotFound after delete, got nil error")
	}
}

// delayedGetClusterStore wraps a mkstore.Store, delaying every GetCluster
// call to widen a TOCTOU race window in tests. It only affects the pre-fix
// code path (UpdateCluster calling a standalone GetCluster);
// UpdateClusterAtomic does its own internal locked read and never reaches
// this override.
type delayedGetClusterStore struct {
	mkstore.Store
	delay time.Duration
}

func (d *delayedGetClusterStore) GetCluster(ctx context.Context, projectID, location, name string) (mkstore.Cluster, error) {
	c, err := d.Store.GetCluster(ctx, projectID, location, name)
	time.Sleep(d.delay)
	return c, err
}

// TestUpdateClusterConcurrentDisjointFieldsNoLostUpdate proves UpdateCluster's
// get-merge-write cycle is atomic with respect to other concurrent PATCH
// requests. Without atomicity, a PATCH touching only "labels" reads a stale
// full copy of the cluster (taken before a concurrent "gcpConfig"-only PATCH
// committed), then writes that stale copy back — silently reverting the
// gcpConfig change even though the labels PATCH never touched it.
func TestUpdateClusterConcurrentDisjointFieldsNoLostUpdate(t *testing.T) {
	ctx := context.Background()
	p := New(&delayedGetClusterStore{Store: mkstore.NewMemoryStore(), delay: 5 * time.Millisecond})

	if _, err := p.CreateCluster(ctx, newNR(createParams("us-central1", "c1", map[string]any{
		"labels":    map[string]any{"env": "dev"},
		"gcpConfig": map[string]any{"kmsKey": "orig"},
	}))); err != nil {
		t.Fatalf("create: %v", err)
	}

	const perField = 25
	var wg sync.WaitGroup
	errs := make([]error, 2*perField)
	for i := 0; i < perField; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = p.UpdateCluster(ctx, newNR(createParams("us-central1", "c1", map[string]any{
				"labels": map[string]any{"i": string(rune('a' + i%26))},
			})))
		}(i)
		go func(i int) {
			defer wg.Done()
			_, errs[perField+i] = p.UpdateCluster(ctx, newNR(createParams("us-central1", "c1", map[string]any{
				"gcpConfig": map[string]any{"kmsKey": string(rune('A' + i%26))},
			})))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	got, err := p.GetCluster(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	labels, _ := got.Data["labels"].(map[string]string)
	if _, hasEnv := labels["env"]; hasEnv {
		t.Errorf("labels reverted to the stale pre-race value instead of one of the 25 concurrent writers': got %v", labels)
	}
	gcpConfig, _ := got.Data["gcpConfig"].(map[string]any)
	if gcpConfig["kmsKey"] == "orig" {
		t.Errorf("gcpConfig.kmsKey reverted to the stale pre-race value instead of one of the 25 concurrent writers'")
	}
}

func TestCreateCluster_MissingParams(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	cases := []map[string]any{
		{"location": "", "clusterId": "c1"},
		{"location": "us-central1", "clusterId": ""},
	}
	for _, params := range cases {
		if _, err := p.CreateCluster(ctx, newNR(params)); err == nil {
			t.Errorf("params=%v: expected InvalidArgument, got nil", params)
		} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
			t.Errorf("params=%v: expected InvalidArgument, got %v", params, err)
		}
	}
}

func TestGetCluster_NotFound(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	_, err := p.GetCluster(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "missing"}))
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "NotFound" {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestCreateCluster_AlreadyExists(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	nr := newNR(createParams("us-central1", "c1", nil))
	if _, err := p.CreateCluster(ctx, nr); err != nil {
		t.Fatalf("first CreateCluster: %v", err)
	}
	_, err := p.CreateCluster(ctx, newNR(createParams("us-central1", "c1", nil)))
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "AlreadyExists" {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

func TestTopicCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.CreateCluster(ctx, newNR(createParams("us-central1", "c1", nil))); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	topicParams := map[string]any{
		"location":  "us-central1",
		"clusterId": "c1",
		"topicId":   "t1",
		"body": map[string]any{
			"partitionCount":    float64(3),
			"replicationFactor": float64(2),
		},
	}
	createResp, err := p.CreateTopic(ctx, newNR(topicParams))
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	wantName := "projects/proj/locations/us-central1/clusters/c1/topics/t1"
	if createResp.Data["name"] != wantName {
		t.Errorf("name = %v, want %v", createResp.Data["name"], wantName)
	}
	if createResp.Data["partitionCount"] != 3 {
		t.Errorf("partitionCount = %v, want 3", createResp.Data["partitionCount"])
	}

	// Get.
	getResp, err := p.GetTopic(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1", "topicId": "t1"}))
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if getResp.Data["replicationFactor"] != 2 {
		t.Errorf("replicationFactor = %v, want 2", getResp.Data["replicationFactor"])
	}

	// Update.
	updResp, err := p.UpdateTopic(ctx, newNR(map[string]any{
		"location": "us-central1", "clusterId": "c1", "topicId": "t1",
		"body": map[string]any{"partitionCount": float64(6)},
	}))
	if err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if updResp.Data["partitionCount"] != 6 {
		t.Errorf("updated partitionCount = %v, want 6", updResp.Data["partitionCount"])
	}

	// List.
	listResp, err := p.ListTopics(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1"}))
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	items, _ := listResp.Data["topics"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(items))
	}

	// Delete.
	if _, err := p.DeleteTopic(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1", "topicId": "t1"})); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if _, err := p.GetTopic(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1", "topicId": "t1"})); err == nil {
		t.Fatal("expected NotFound after delete, got nil error")
	}
}

// delayedGetTopicStore wraps a mkstore.Store, delaying every GetTopic call
// to widen a TOCTOU race window in tests. It only affects the pre-fix code
// path (UpdateTopic calling a standalone GetTopic); UpdateTopicAtomic does
// its own internal locked read and never reaches this override.
type delayedGetTopicStore struct {
	mkstore.Store
	delay time.Duration
}

func (d *delayedGetTopicStore) GetTopic(ctx context.Context, projectID, location, clusterName, topicName string) (mkstore.Topic, error) {
	tpc, err := d.Store.GetTopic(ctx, projectID, location, clusterName, topicName)
	time.Sleep(d.delay)
	return tpc, err
}

// TestUpdateTopicConcurrentDisjointFieldsNoLostUpdate proves UpdateTopic's
// get-merge-write cycle is atomic with respect to other concurrent PATCH
// requests. Without atomicity, a PATCH touching only "partitionCount" reads a
// stale full copy of the topic (taken before a concurrent
// "replicationFactor"-only PATCH committed), then writes that stale copy
// back — silently reverting the replicationFactor change even though the
// partitionCount PATCH never touched it.
func TestUpdateTopicConcurrentDisjointFieldsNoLostUpdate(t *testing.T) {
	ctx := context.Background()
	p := New(&delayedGetTopicStore{Store: mkstore.NewMemoryStore(), delay: 5 * time.Millisecond})

	if _, err := p.CreateCluster(ctx, newNR(createParams("us-central1", "c1", nil))); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	if _, err := p.CreateTopic(ctx, newNR(map[string]any{
		"location": "us-central1", "clusterId": "c1", "topicId": "t1",
		"body": map[string]any{"partitionCount": float64(3), "replicationFactor": float64(2)},
	})); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	const perField = 25
	var wg sync.WaitGroup
	errs := make([]error, 2*perField)
	for i := 0; i < perField; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = p.UpdateTopic(ctx, newNR(map[string]any{
				"location": "us-central1", "clusterId": "c1", "topicId": "t1",
				"body": map[string]any{"partitionCount": float64(100 + i)},
			}))
		}(i)
		go func(i int) {
			defer wg.Done()
			_, errs[perField+i] = p.UpdateTopic(ctx, newNR(map[string]any{
				"location": "us-central1", "clusterId": "c1", "topicId": "t1",
				"body": map[string]any{"replicationFactor": float64(200 + i)},
			}))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	got, err := p.GetTopic(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1", "topicId": "t1"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	pc, _ := got.Data["partitionCount"].(int)
	rf, _ := got.Data["replicationFactor"].(int)
	if pc < 100 {
		t.Errorf("partitionCount reverted to a stale value instead of one of the 25 concurrent writers': got %d", pc)
	}
	if rf < 200 {
		t.Errorf("replicationFactor reverted to a stale value instead of one of the 25 concurrent writers': got %d", rf)
	}
}

func TestCreateTopic_ClusterNotFound(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	_, err := p.CreateTopic(ctx, newNR(map[string]any{
		"location": "us-central1", "clusterId": "missing", "topicId": "t1",
	}))
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "NotFound" {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestCreateTopic_MissingParams(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.CreateTopic(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1"})); err == nil {
		t.Fatal("expected InvalidArgument for missing topicId, got nil")
	}
}

func TestListConsumerGroups_AlwaysEmpty(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.ListConsumerGroups(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("ListConsumerGroups: %v", err)
	}
	groups, _ := resp.Data["consumerGroups"].([]string)
	if len(groups) != 0 {
		t.Errorf("expected empty consumer groups, got %v", groups)
	}
	if _, hasNext := resp.Data["nextPageToken"]; hasNext {
		t.Errorf("did not expect nextPageToken for an empty list")
	}
}

func TestConsumerGroupResourceOps_Unimplemented(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	routes := p.Routes()
	for _, name := range []string{
		"ManagedKafka.GetConsumerGroup", "ManagedKafka.UpdateConsumerGroup", "ManagedKafka.DeleteConsumerGroup",
	} {
		_, err := routes[name](ctx, newNR(nil))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "Unimplemented" {
			t.Errorf("%s: expected Unimplemented, got %v", name, err)
		}
	}
}

func TestReset(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.CreateCluster(ctx, newNR(createParams("us-central1", "c1", nil))); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	p.Reset(ctx)
	if _, err := p.GetCluster(ctx, newNR(map[string]any{"location": "us-central1", "clusterId": "c1"})); err == nil {
		t.Fatal("expected NotFound after Reset, got nil error")
	}
}

func TestRoutes_AllHandlersRegistered(t *testing.T) {
	p := newProvider()
	routes := p.Routes()
	want := []string{
		"ManagedKafka.CreateCluster", "ManagedKafka.GetCluster", "ManagedKafka.ListClusters",
		"ManagedKafka.UpdateCluster", "ManagedKafka.DeleteCluster",
		"ManagedKafka.CreateTopic", "ManagedKafka.GetTopic", "ManagedKafka.ListTopics",
		"ManagedKafka.UpdateTopic", "ManagedKafka.DeleteTopic",
		"ManagedKafka.ListConsumerGroups", "ManagedKafka.GetConsumerGroup",
		"ManagedKafka.UpdateConsumerGroup", "ManagedKafka.DeleteConsumerGroup",
	}
	for _, k := range want {
		if routes[k] == nil {
			t.Errorf("missing route %q", k)
		}
	}
	if len(routes) != len(want) {
		t.Errorf("got %d routes, want %d", len(routes), len(want))
	}
}
