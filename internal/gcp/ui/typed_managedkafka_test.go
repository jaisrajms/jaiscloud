package ui

import (
	"context"
	"encoding/json"
	"testing"

	managedkafkacore "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	restmanagedkafka "jaiscloud/internal/gcp/transport/rest/managedkafka"
	managedkafkaui "jaiscloud/internal/gcp/ui/managedkafka"
	"jaiscloud/internal/provider"
)

func newKafka() (*managedkafkacore.Service, managedkafkaui.ProviderInterface, map[string]provider.HandlerFunc) {
	core := managedkafkacore.NewService(mkstore.NewMemoryStore())
	return core, managedkafkaui.NewProvider(core), restmanagedkafka.NewProvider(core, "proj").Routes()
}

// kafkaSections is the part of a cluster config the REST shape renders.
func kafkaSections(raw json.RawMessage) string {
	var all map[string]any
	_ = json.Unmarshal(raw, &all)
	keep := map[string]any{}
	for _, k := range []string{"capacityConfig", "gcpConfig", "rebalanceConfig", "tlsConfig"} {
		if v, ok := all[k]; ok {
			keep[k] = v
		}
	}
	b, _ := json.Marshal(keep)
	return string(b)
}

// topicConfigs is the Kafka property overrides, the only part of a topic's stored config the REST shape renders.
func topicConfigs(raw json.RawMessage) string {
	var all map[string]any
	_ = json.Unmarshal(raw, &all)
	b, _ := json.Marshal(all["configs"])
	return string(b)
}

func TestManagedKafkaConsolePassthroughMatchesDirect(t *testing.T) {
	core, direct, builtins := newKafka()
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for k := range builtins {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeManagedKafka(typedFixture(builtins, wrap), direct)
	ctx := context.Background()

	cfg := json.RawMessage(`{"capacityConfig":{"vcpuCount":"3","memoryBytes":"3221225472"},"gcpConfig":{"accessConfig":{"networkConfigs":[{"subnet":"projects/proj/regions/us-central1/subnetworks/default"}]}}}`)
	c, err := p.CreateCluster(ctx, "proj", "us-central1", "k1", managedkafkaui.ClusterWriteInput{Labels: map[string]string{"a": "b"}, Config: cfg})
	if err != nil || c.Name != "k1" || c.Location != "us-central1" || c.ProjectID != "proj" {
		t.Fatalf("CreateCluster = %+v, %v", c, err)
	}
	want, err := core.GetCluster(ctx, "proj", "us-central1", "k1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.GetCluster(ctx, "proj", "us-central1", "k1")
	// The REST shape keeps labels at the top level and renders four config sections,
	// so compare the sections it carries (the direct model also keeps labels in Config).
	if err != nil || got.Name != want.Name || len(got.Labels) != len(want.Labels) || !got.CreateTime.Equal(want.CreateTime) ||
		kafkaSections(got.Config) != kafkaSections(want.Config) {
		t.Fatalf("cluster through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, want, err)
	}
	if list, err := p.ListAllClusters(ctx, "proj"); err != nil || len(list) != 1 || list[0].Name != "k1" {
		t.Fatalf("ListAllClusters = %+v, %v", list, err)
	}

	tp, err := p.CreateTopic(ctx, "proj", "us-central1", "k1", "orders", managedkafkaui.TopicWriteInput{PartitionCount: 3, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "1000"}})
	if err != nil || tp.Name != "orders" || tp.PartitionCount != 3 {
		t.Fatalf("CreateTopic = %+v, %v", tp, err)
	}
	wantT, _ := core.GetTopic(ctx, "proj", "us-central1", "k1", "orders")
	if gotT, err := p.GetTopic(ctx, "proj", "us-central1", "k1", "orders"); err != nil || gotT.PartitionCount != wantT.PartitionCount ||
		gotT.ReplicationFactor != wantT.ReplicationFactor || topicConfigs(gotT.Config) != topicConfigs(wantT.Config) {
		t.Fatalf("topic through the chain:\n  %+v\ndirect:\n  %+v\n%v", gotT, wantT, err)
	}
	if list, err := p.ListTopics(ctx, "proj", "us-central1", "k1"); err != nil || len(list) != 1 {
		t.Fatalf("ListTopics = %+v, %v", list, err)
	}
	if list, err := p.ListAllTopics(ctx, "proj"); err != nil || len(list) != 1 || list[0].ClusterName != "k1" {
		t.Fatalf("ListAllTopics = %+v, %v", list, err)
	}

	acl, err := p.CreateAcl(ctx, "proj", "us-central1", "k1", "topic/orders", managedkafkaui.AclWriteInput{Entries: []managedkafkaui.AclEntry{
		{Principal: "User:alice@proj.iam.gserviceaccount.com", PermissionType: "ALLOW", Operation: "READ", Host: "*"}}})
	if err != nil || acl.Name != "topic/orders" || len(acl.AclEntries) != 1 {
		t.Fatalf("CreateAcl = %+v, %v", acl, err)
	}
	if list, err := p.ListAcls(ctx, "proj", "us-central1", "k1"); err != nil || len(list) != 1 {
		t.Fatalf("ListAcls = %+v, %v", list, err)
	}
	if a, created, err := p.AddAclEntry(ctx, "proj", "us-central1", "k1", "topic/orders", managedkafkaui.AclEntry{
		Principal: "User:bob@proj.iam.gserviceaccount.com", PermissionType: "ALLOW", Operation: "WRITE", Host: "*"}); err != nil || created || len(a.AclEntries) != 2 {
		t.Fatalf("AddAclEntry = %+v, %v, %v", a, created, err)
	}
	if err := p.DeleteAcl(ctx, "proj", "us-central1", "k1", "topic/orders"); err != nil {
		t.Fatalf("DeleteAcl: %v", err)
	}
	if groups, err := p.ListConsumerGroups(ctx, "proj", "us-central1", "k1"); err != nil || len(groups) != 0 {
		t.Fatalf("ListConsumerGroups = %+v, %v", groups, err)
	}
	if err := p.DeleteTopic(ctx, "proj", "us-central1", "k1", "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if len(seen) < 13 {
		t.Fatalf("the plugin saw only %d calls (%v)", len(seen), seen)
	}
}

func TestManagedKafkaConsoleShowsWhatThePluginServes(t *testing.T) {
	_, direct, builtins := newKafka()
	p := routeManagedKafka(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"ManagedKafka.ListClusters": replaceWith(map[string]any{"clusters": []any{map[string]any{
			"name": "projects/proj/locations/europe-west1/clusters/from-plugin", "bootstrapAddress": "b.example:9092",
			"capacityConfig": map[string]any{"vcpuCount": "3"}}}}),
	}), direct)
	cs, err := p.ListAllClusters(context.Background(), "proj")
	if err != nil || len(cs) != 1 {
		t.Fatalf("ListAllClusters = %+v, %v", cs, err)
	}
	if c := cs[0]; c.Name != "from-plugin" || c.Location != "europe-west1" || c.BootstrapAddress != "b.example:9092" || canonicalJSON(c.Config) != `{"capacityConfig":{"vcpuCount":"3"}}` {
		t.Fatalf("the console must show the plugin's cluster, got %+v", c)
	}
}
