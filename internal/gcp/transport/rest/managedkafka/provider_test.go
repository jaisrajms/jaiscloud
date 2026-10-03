package managedkafka

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	core "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

func TestCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		{"POST", "/v1/projects/p/locations/us-central1/clusters?clusterId=c", "CreateCluster"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters", "ListClusters"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c", "GetCluster"},
		{"PATCH", "/v1/projects/p/locations/us-central1/clusters/c", "UpdateCluster"},
		{"DELETE", "/v1/projects/p/locations/us-central1/clusters/c", "DeleteCluster"},
		{"GET", "/v1/projects/p/locations/us-central1/operations", "ListOperations"},
		{"GET", "/v1/projects/p/locations/us-central1/operations/op", "GetOperation"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/topics?topicId=t", "CreateTopic"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/topics", "ListTopics"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/topics/t", "GetTopic"},
		{"PATCH", "/v1/projects/p/locations/us-central1/clusters/c/topics/t", "UpdateTopic"},
		{"DELETE", "/v1/projects/p/locations/us-central1/clusters/c/topics/t", "DeleteTopic"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/consumerGroups", "ListConsumerGroups"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/consumerGroups/g", "GetConsumerGroup"},
		{"PATCH", "/v1/projects/p/locations/us-central1/clusters/c/consumerGroups/g", "UpdateConsumerGroup"},
		{"DELETE", "/v1/projects/p/locations/us-central1/clusters/c/consumerGroups/g", "DeleteConsumerGroup"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/acls?aclId=topic/x", "CreateAcl"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/acls", "ListAcls"},
		{"GET", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x", "GetAcl"},
		{"PATCH", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x", "UpdateAcl"},
		{"DELETE", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x", "DeleteAcl"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x:addAclEntry", "AddAclEntry"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x:removeAclEntry", "RemoveAclEntry"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/acls/cluster:addAclEntry", "AddAclEntry"},
	}
	for _, tc := range cases {
		codec := NewCodec()
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
	}
}

func TestCodecDecodeUnsupported(t *testing.T) {
	codec := NewCodec()
	for _, tc := range []struct{ method, path string }{
		{"PUT", "/v1/projects/p/locations/us-central1/clusters/c"},
		{"POST", "/v1/projects/p/locations/us-central1/clusters/c/acls/topic/x:bogus"},
		{"GET", "/v1/projects/p/locations/us-central1/nonsense"},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		if _, err := codec.Decode(r, nil); err == nil {
			t.Errorf("%s %s: expected error", tc.method, tc.path)
		}
	}
}

func newRESTProvider() (*Provider, *core.Service) {
	c := core.NewService(mkstore.NewMemoryStore())
	return NewProvider(c, "proj"), c
}

func nr(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params}
}

func TestProviderClusterAndTopicFlow(t *testing.T) {
	ctx := context.Background()
	p, _ := newRESTProvider()

	// Create cluster.
	resp, err := p.CreateCluster(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1",
		"body": map[string]any{"labels": map[string]any{"env": "test"}},
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("done = %v, want true", resp.Data["done"])
	}
	created, _ := resp.Data["response"].(map[string]any)
	if created["name"] != "projects/proj/locations/us-central1/clusters/c1" {
		t.Errorf("name = %v", created["name"])
	}

	// Create topic with property overrides; the wire body echoes them back.
	createResp, err := p.CreateTopic(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "topicId": "t1",
		"body": map[string]any{
			"partitionCount":    float64(3),
			"replicationFactor": float64(2),
			"configs":           map[string]any{"cleanup.policy": "compact"},
		},
	}))
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if got, _ := createResp.Data["configs"].(map[string]any); got["cleanup.policy"] != "compact" {
		t.Errorf("created configs = %v, want cleanup.policy=compact", createResp.Data["configs"])
	}

	// An update can change a property override.
	updResp, err := p.UpdateTopic(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "topicId": "t1",
		"body": map[string]any{"configs": map[string]any{"cleanup.policy": "delete"}},
	}))
	if err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if got, _ := updResp.Data["configs"].(map[string]any); got["cleanup.policy"] != "delete" {
		t.Errorf("updated configs = %v, want cleanup.policy=delete", updResp.Data["configs"])
	}

	listResp, err := p.ListTopics(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1"}))
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	items, _ := listResp.Data["topics"].([]any)
	if len(items) != 1 {
		t.Fatalf("topics = %v", items)
	}

	// Consumer groups list is empty; item ops are NotFound.
	cg, err := p.ListConsumerGroups(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1"}))
	if err != nil {
		t.Fatalf("ListConsumerGroups: %v", err)
	}
	groups, ok := cg.Data["consumerGroups"].([]any)
	if !ok {
		t.Fatalf("consumerGroups = %#v, want an empty JSON array (not null)", cg.Data["consumerGroups"])
	}
	if len(groups) != 0 {
		t.Errorf("consumerGroups = %v, want empty", groups)
	}
	if _, err := p.GetConsumerGroup(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1", "consumerGroupId": "g"})); err == nil {
		t.Error("expected NotFound for a consumer group")
	}
}

func TestProviderAclFlow(t *testing.T) {
	ctx := context.Background()
	p, _ := newRESTProvider()
	if _, err := p.CreateCluster(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1"})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	entry := map[string]any{"principal": "User:a@example.com", "permissionType": "ALLOW", "operation": "READ", "host": "*"}
	resp, err := p.CreateAcl(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"aclEntries": []any{entry}},
	}))
	if err != nil {
		t.Fatalf("CreateAcl: %v", err)
	}
	if resp.Data["resourceType"] != "TOPIC" || resp.Data["resourceName"] != "x" {
		t.Errorf("acl = %v", resp.Data)
	}
	etag, _ := resp.Data["etag"].(string)

	// Update requires the etag from the create response.
	if _, err := p.UpdateAcl(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"etag": "stale", "aclEntries": []any{entry}},
	})); err == nil {
		t.Error("expected etag mismatch error")
	}
	if _, err := p.UpdateAcl(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"etag": etag, "aclEntries": []any{entry}},
	})); err != nil {
		t.Fatalf("UpdateAcl: %v", err)
	}

	// Add and remove an entry.
	if _, err := p.AddAclEntry(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"principal": "User:b@example.com", "permissionType": "DENY", "operation": "WRITE", "host": "*"},
	})); err != nil {
		t.Fatalf("AddAclEntry: %v", err)
	}
	removed, err := p.RemoveAclEntry(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"principal": "User:b@example.com", "permissionType": "DENY", "operation": "WRITE", "host": "*"},
	}))
	if err != nil {
		t.Fatalf("RemoveAclEntry: %v", err)
	}
	if _, ok := removed.Data["acl"]; !ok {
		t.Errorf("remove response = %v, want acl", removed.Data)
	}

	// Remove the last entry deletes the acl.
	delResp, err := p.RemoveAclEntry(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "aclId": "topic/x",
		"body": map[string]any{"principal": "User:a@example.com", "permissionType": "ALLOW", "operation": "READ", "host": "*"},
	}))
	if err != nil {
		t.Fatalf("RemoveAclEntry last: %v", err)
	}
	if delResp.Data["aclDeleted"] != true {
		t.Errorf("expected aclDeleted, got %v", delResp.Data)
	}
}

// stubBroker embeds core.Broker so only the methods exercised here need
// implementing; the remaining (unused) methods are never called.
type stubBroker struct {
	core.Broker
	groups    []string
	offsets   map[string][]core.ConsumerGroupOffset
	members   map[string]int
	committed map[string][]core.ConsumerGroupOffset
}

func (b *stubBroker) EnsureCluster(context.Context, string, string, string) (string, error) {
	return "broker:9092", nil
}
func (b *stubBroker) Endpoint(string, string, string) string { return "broker:9092" }
func (b *stubBroker) ListConsumerGroups(context.Context, string, string, string) ([]string, error) {
	return b.groups, nil
}
func (b *stubBroker) ConsumerGroupOffsets(_ context.Context, _, _, _, g string) ([]core.ConsumerGroupOffset, bool, error) {
	o, ok := b.offsets[g]
	return o, ok, nil
}
func (b *stubBroker) ConsumerGroupMembers(_ context.Context, _, _, _, g string) (int, bool, error) {
	_, ok := b.offsets[g]
	return b.members[g], ok, nil
}
func (b *stubBroker) CommitConsumerGroupOffsets(_ context.Context, _, _, _, g string, offs []core.ConsumerGroupOffset) error {
	if b.committed == nil {
		b.committed = map[string][]core.ConsumerGroupOffset{}
	}
	b.committed[g] = offs
	return nil
}
func (b *stubBroker) DeleteConsumerGroup(_ context.Context, _, _, _, g string) (bool, error) {
	_, ok := b.offsets[g]
	delete(b.offsets, g)
	return ok, nil
}

func TestProviderConsumerGroupFlow(t *testing.T) {
	ctx := context.Background()
	t1 := core.TopicName("proj", "us-central1", "c1", "t1")
	fb := &stubBroker{
		groups:  []string{"g1"},
		offsets: map[string][]core.ConsumerGroupOffset{"g1": {{Topic: "t1", Partition: 0, Offset: 5, Metadata: "m"}}},
		members: map[string]int{"g1": 0},
	}
	svc := core.NewService(mkstore.NewMemoryStore(), core.WithBroker(fb))
	p := NewProvider(svc, "proj")
	if _, err := p.CreateCluster(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1"})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	// List (default FULL) expands offsets to resource names.
	listResp, err := p.ListConsumerGroups(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1"}))
	if err != nil {
		t.Fatalf("ListConsumerGroups: %v", err)
	}
	items, _ := listResp.Data["consumerGroups"].([]any)
	if len(items) != 1 {
		t.Fatalf("consumerGroups = %v", items)
	}
	g0, _ := items[0].(map[string]any)
	topics, _ := g0["topics"].(map[string]any)
	topic, _ := topics[t1].(map[string]any)
	partitions, _ := topic["partitions"].(map[string]any)
	if !hasOffset(partitions, "0", 5) {
		t.Errorf("list topics = %#v", topics)
	}

	// BASIC hides offsets.
	basic, err := p.ListConsumerGroups(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1", "view": "CONSUMER_GROUP_VIEW_BASIC"}))
	if err != nil {
		t.Fatalf("ListConsumerGroups BASIC: %v", err)
	}
	bg, _ := basic.Data["consumerGroups"].([]any)[0].(map[string]any)
	if _, ok := bg["topics"]; ok {
		t.Errorf("BASIC group carried topics: %#v", bg)
	}

	// Get round-trips.
	getResp, err := p.GetConsumerGroup(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1", "consumerGroupId": "g1"}))
	if err != nil {
		t.Fatalf("GetConsumerGroup: %v", err)
	}
	if getResp.Data["name"] != core.ConsumerGroupName("proj", "us-central1", "c1", "g1") {
		t.Errorf("name = %v", getResp.Data["name"])
	}
	// The offset is an int64-as-string on the wire; the official REST client
	// decodes it with the ,string struct tag.
	encoded, _ := json.Marshal(getResp.Data)
	if !strings.Contains(string(encoded), `"offset":"5"`) {
		t.Errorf("encoded group missing string offset: %s", encoded)
	}

	// Update commits offsets, normalizing the resource-name key to the bare id.
	// The offset arrives as a JSON string (as the official client sends it).
	updResp, err := p.UpdateConsumerGroup(ctx, nr(map[string]any{
		"location": "us-central1", "clusterId": "c1", "consumerGroupId": "g1",
		"updateMask": "topics",
		"body": map[string]any{"topics": map[string]any{
			t1: map[string]any{"partitions": map[string]any{"0": map[string]any{"offset": "9", "metadata": "reset"}}},
		}},
	}))
	if err != nil {
		t.Fatalf("UpdateConsumerGroup: %v", err)
	}
	if fb.committed["g1"][0].Topic != "t1" || fb.committed["g1"][0].Offset != 9 {
		t.Errorf("committed = %+v", fb.committed["g1"])
	}
	updTopics, _ := updResp.Data["topics"].(map[string]any)
	updTopic, _ := updTopics[t1].(map[string]any)
	if !hasOffset(updTopic["partitions"].(map[string]any), "0", 9) {
		t.Errorf("update topics = %#v", updResp.Data["topics"])
	}

	// Delete removes the group, then reports NotFound.
	if _, err := p.DeleteConsumerGroup(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1", "consumerGroupId": "g1"})); err != nil {
		t.Fatalf("DeleteConsumerGroup: %v", err)
	}
	if _, err := p.GetConsumerGroup(ctx, nr(map[string]any{"location": "us-central1", "clusterId": "c1", "consumerGroupId": "g1"})); err == nil {
		t.Error("expected NotFound after delete")
	}
}

func hasOffset(partitions map[string]any, key string, want int64) bool {
	p, ok := partitions[key].(map[string]any)
	if !ok {
		return false
	}
	got, ok := p["offset"].(string)
	if !ok {
		return false
	}
	n, err := strconv.ParseInt(got, 10, 64)
	return err == nil && n == want
}
