package ui

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	managedkafkacore "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	managedkafkaui "jaiscloud/internal/gcp/ui/managedkafka"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// typedManagedKafka serves the Managed Service for Apache Kafka console through the
// plugin chain when a plugin covers the key (see typed.go).
type typedManagedKafka struct {
	managedkafkaui.ProviderInterface
	c typedCaller
}

func routeManagedKafka(c typedCaller, p managedkafkaui.ProviderInterface) managedkafkaui.ProviderInterface {
	return typedManagedKafka{ProviderInterface: p, c: c}
}

// ---- decoding ----------------------------------------------------------------

func decodeKafkaCluster(m map[string]any) mkstore.Cluster {
	parts := nameParts(strField(m, "name"))
	cfg := map[string]any{}
	for _, k := range []string{"capacityConfig", "gcpConfig", "rebalanceConfig", "tlsConfig"} {
		if v, ok := m[k]; ok {
			cfg[k] = v
		}
	}
	c := mkstore.Cluster{
		ProjectID: parts["projects"], Location: parts["locations"], Name: parts["clusters"],
		Labels: strMapField(m, "labels"), CreateTime: timeField(m, "createTime"), UpdateTime: timeField(m, "updateTime"),
		BootstrapAddress: strField(m, "bootstrapAddress"),
	}
	if len(cfg) > 0 {
		c.Config, _ = json.Marshal(cfg)
	}
	return c
}

func decodeKafkaTopic(m map[string]any) mkstore.Topic {
	parts := nameParts(strField(m, "name"))
	t := mkstore.Topic{ProjectID: parts["projects"], Location: parts["locations"], ClusterName: parts["clusters"], Name: parts["topics"]}
	t.PartitionCount, t.ReplicationFactor = intField(m, "partitionCount"), intField(m, "replicationFactor")
	if cfg, ok := m["configs"]; ok {
		t.Config, _ = json.Marshal(map[string]any{"configs": cfg})
	}
	return t
}

func decodeKafkaAcl(m map[string]any) mkstore.Acl {
	name := strField(m, "name")
	parts := nameParts(name)
	// An ACL id encodes its resource pattern and can contain slashes ("topic/orders").
	_, aclID, _ := strings.Cut(name, "/acls/")
	a := mkstore.Acl{
		ProjectID: parts["projects"], Location: parts["locations"], ClusterName: parts["clusters"], Name: aclID,
		Etag: strField(m, "etag"), ResourceType: strField(m, "resourceType"), ResourceName: strField(m, "resourceName"),
		PatternType: strField(m, "patternType"),
	}
	for _, e := range items(m, "aclEntries") {
		a.AclEntries = append(a.AclEntries, mkstore.AclEntry{
			Principal: strField(e, "principal"), PermissionType: strField(e, "permissionType"),
			Operation: strField(e, "operation"), Host: strField(e, "host"),
		})
	}
	return a
}

func decodeKafkaGroup(m map[string]any) managedkafkacore.ConsumerGroup {
	parts := nameParts(strField(m, "name"))
	g := managedkafkacore.ConsumerGroup{Location: parts["locations"], Cluster: parts["clusters"], Name: parts["consumerGroups"]}
	topics, _ := m["topics"].(map[string]any)
	if len(topics) > 0 {
		g.Topics = map[string]*managedkafkacore.ConsumerGroupTopic{}
	}
	for topic, raw := range topics {
		tm, _ := raw.(map[string]any)
		pm, _ := tm["partitions"].(map[string]any)
		gt := &managedkafkacore.ConsumerGroupTopic{Partitions: map[int32]managedkafkacore.ConsumerGroupPartition{}}
		for idx, rawP := range pm {
			n, err := strconv.Atoi(idx)
			if err != nil {
				continue
			}
			pe, _ := rawP.(map[string]any)
			off, _ := strconv.ParseInt(strField(pe, "offset"), 10, 64)
			gt.Partitions[int32(n)] = managedkafkacore.ConsumerGroupPartition{Offset: off, Metadata: strField(pe, "metadata")}
		}
		g.Topics[topic] = gt
	}
	return g
}

func kafkaEntry(e managedkafkaui.AclEntry) map[string]any {
	return map[string]any{"principal": e.Principal, "permissionType": e.PermissionType, "operation": e.Operation, "host": e.Host}
}

func kafkaAclBody(in managedkafkaui.AclWriteInput) map[string]any {
	entries := make([]any, 0, len(in.Entries))
	for _, e := range in.Entries {
		entries = append(entries, kafkaEntry(e))
	}
	body := map[string]any{"aclEntries": entries}
	if in.Etag != "" {
		body["etag"] = in.Etag
	}
	return body
}

func (p typedManagedKafka) params(project, location, cluster string, extra map[string]any) map[string]any {
	out := map[string]any{"project": project, "location": location, "clusterId": cluster}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// ---- clusters ----------------------------------------------------------------

func (p typedManagedKafka) ListAllClusters(ctx context.Context, project string) ([]mkstore.Cluster, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		cs, err := p.ProviderInterface.ListAllClusters(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(cs))
		for _, c := range cs {
			out = append(out, managedkafkacore.ClusterJSON(c, project))
		}
		return provider.OK(map[string]any{"clusters": out}), nil
	}
	data, covered, err := p.c.do(ctx, "ManagedKafka.ListClusters", project, "", map[string]any{"project": project, "location": "-"}, all)
	if !covered {
		return p.ProviderInterface.ListAllClusters(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []mkstore.Cluster
	for _, m := range items(data, "clusters") {
		out = append(out, decodeKafkaCluster(m))
	}
	return out, nil
}

func (p typedManagedKafka) GetCluster(ctx context.Context, project, location, cluster string) (mkstore.Cluster, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.GetCluster", project, location, p.params(project, location, cluster, nil), nil)
	if !covered {
		return p.ProviderInterface.GetCluster(ctx, project, location, cluster)
	}
	if err != nil {
		return mkstore.Cluster{}, err
	}
	return decodeKafkaCluster(data), nil
}

func clusterBody(in managedkafkaui.ClusterWriteInput) map[string]any {
	body := map[string]any{}
	if len(in.Config) > 0 {
		_ = json.Unmarshal(in.Config, &body)
	}
	if in.Labels != nil {
		body["labels"] = in.Labels
	}
	return body
}

func (p typedManagedKafka) writeCluster(ctx context.Context, key, project, location, id string, in managedkafkaui.ClusterWriteInput, direct func() (mkstore.Cluster, error)) (mkstore.Cluster, error) {
	data, covered, err := p.c.do(ctx, key, project, location, p.params(project, location, id, map[string]any{"body": clusterBody(in)}), nil)
	if !covered {
		return direct()
	}
	if err != nil {
		return mkstore.Cluster{}, err
	}
	if r, ok := data["response"].(map[string]any); ok {
		return decodeKafkaCluster(r), nil
	}
	return p.GetCluster(ctx, project, location, id)
}

func (p typedManagedKafka) CreateCluster(ctx context.Context, project, location, id string, in managedkafkaui.ClusterWriteInput) (mkstore.Cluster, error) {
	return p.writeCluster(ctx, "ManagedKafka.CreateCluster", project, location, id, in, func() (mkstore.Cluster, error) {
		return p.ProviderInterface.CreateCluster(ctx, project, location, id, in)
	})
}

func (p typedManagedKafka) UpdateCluster(ctx context.Context, project, location, id string, in managedkafkaui.ClusterWriteInput) (mkstore.Cluster, error) {
	return p.writeCluster(ctx, "ManagedKafka.UpdateCluster", project, location, id, in, func() (mkstore.Cluster, error) {
		return p.ProviderInterface.UpdateCluster(ctx, project, location, id, in)
	})
}

// ---- topics ------------------------------------------------------------------

func (p typedManagedKafka) ListAllTopics(ctx context.Context, project string) ([]mkstore.Topic, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		ts, err := p.ProviderInterface.ListAllTopics(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(ts))
		for _, t := range ts {
			out = append(out, managedkafkacore.TopicJSON(t, project))
		}
		return provider.OK(map[string]any{"topics": out}), nil
	}
	data, covered, err := p.c.do(ctx, "ManagedKafka.ListTopics", project, "", p.params(project, "-", "-", nil), all)
	if !covered {
		return p.ProviderInterface.ListAllTopics(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []mkstore.Topic
	for _, m := range items(data, "topics") {
		out = append(out, decodeKafkaTopic(m))
	}
	return out, nil
}

func (p typedManagedKafka) ListTopics(ctx context.Context, project, location, cluster string) ([]mkstore.Topic, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.ListTopics", project, location, p.params(project, location, cluster, nil), nil)
	if !covered {
		return p.ProviderInterface.ListTopics(ctx, project, location, cluster)
	}
	if err != nil {
		return nil, err
	}
	var out []mkstore.Topic
	for _, m := range items(data, "topics") {
		out = append(out, decodeKafkaTopic(m))
	}
	return out, nil
}

func (p typedManagedKafka) GetTopic(ctx context.Context, project, location, cluster, topic string) (mkstore.Topic, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.GetTopic", project, location, p.params(project, location, cluster, map[string]any{"topicId": topic}), nil)
	if !covered {
		return p.ProviderInterface.GetTopic(ctx, project, location, cluster, topic)
	}
	if err != nil {
		return mkstore.Topic{}, err
	}
	return decodeKafkaTopic(data), nil
}

func topicBody(in managedkafkaui.TopicWriteInput) map[string]any {
	body := map[string]any{}
	if in.PartitionCount > 0 {
		body["partitionCount"] = float64(in.PartitionCount)
	}
	if in.ReplicationFactor > 0 {
		body["replicationFactor"] = float64(in.ReplicationFactor)
	}
	if in.Configs != nil {
		body["configs"] = in.Configs
	}
	return body
}

func (p typedManagedKafka) CreateTopic(ctx context.Context, project, location, cluster, id string, in managedkafkaui.TopicWriteInput) (mkstore.Topic, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.CreateTopic", project, location, p.params(project, location, cluster, map[string]any{"topicId": id, "body": topicBody(in)}), nil)
	if !covered {
		return p.ProviderInterface.CreateTopic(ctx, project, location, cluster, id, in)
	}
	if err != nil {
		return mkstore.Topic{}, err
	}
	return decodeKafkaTopic(data), nil
}

func (p typedManagedKafka) UpdateTopic(ctx context.Context, project, location, cluster, id string, in managedkafkaui.TopicWriteInput) (mkstore.Topic, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.UpdateTopic", project, location, p.params(project, location, cluster, map[string]any{"topicId": id, "body": topicBody(in)}), nil)
	if !covered {
		return p.ProviderInterface.UpdateTopic(ctx, project, location, cluster, id, in)
	}
	if err != nil {
		return mkstore.Topic{}, err
	}
	return decodeKafkaTopic(data), nil
}

func (p typedManagedKafka) DeleteTopic(ctx context.Context, project, location, cluster, id string) error {
	_, covered, err := p.c.do(ctx, "ManagedKafka.DeleteTopic", project, location, p.params(project, location, cluster, map[string]any{"topicId": id}), nil)
	if !covered {
		return p.ProviderInterface.DeleteTopic(ctx, project, location, cluster, id)
	}
	return err
}

// ---- ACLs ----------------------------------------------------------------------

func (p typedManagedKafka) ListAcls(ctx context.Context, project, location, cluster string) ([]mkstore.Acl, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.ListAcls", project, location, p.params(project, location, cluster, nil), nil)
	if !covered {
		return p.ProviderInterface.ListAcls(ctx, project, location, cluster)
	}
	if err != nil {
		return nil, err
	}
	var out []mkstore.Acl
	for _, m := range items(data, "acls") {
		out = append(out, decodeKafkaAcl(m))
	}
	return out, nil
}

func (p typedManagedKafka) CreateAcl(ctx context.Context, project, location, cluster, id string, in managedkafkaui.AclWriteInput) (mkstore.Acl, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.CreateAcl", project, location, p.params(project, location, cluster, map[string]any{"aclId": id, "body": kafkaAclBody(in)}), nil)
	if !covered {
		return p.ProviderInterface.CreateAcl(ctx, project, location, cluster, id, in)
	}
	if err != nil {
		return mkstore.Acl{}, err
	}
	return decodeKafkaAcl(data), nil
}

func (p typedManagedKafka) UpdateAcl(ctx context.Context, project, location, cluster, id string, in managedkafkaui.AclWriteInput) (mkstore.Acl, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.UpdateAcl", project, location, p.params(project, location, cluster, map[string]any{"aclId": id, "body": kafkaAclBody(in)}), nil)
	if !covered {
		return p.ProviderInterface.UpdateAcl(ctx, project, location, cluster, id, in)
	}
	if err != nil {
		return mkstore.Acl{}, err
	}
	return decodeKafkaAcl(data), nil
}

func (p typedManagedKafka) DeleteAcl(ctx context.Context, project, location, cluster, id string) error {
	_, covered, err := p.c.do(ctx, "ManagedKafka.DeleteAcl", project, location, p.params(project, location, cluster, map[string]any{"aclId": id}), nil)
	if !covered {
		return p.ProviderInterface.DeleteAcl(ctx, project, location, cluster, id)
	}
	return err
}

func (p typedManagedKafka) AddAclEntry(ctx context.Context, project, location, cluster, id string, entry managedkafkaui.AclEntry) (mkstore.Acl, bool, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.AddAclEntry", project, location, p.params(project, location, cluster, map[string]any{"aclId": id, "body": kafkaEntry(entry)}), nil)
	if !covered {
		return p.ProviderInterface.AddAclEntry(ctx, project, location, cluster, id, entry)
	}
	if err != nil {
		return mkstore.Acl{}, false, err
	}
	created, _ := data["aclCreated"].(bool)
	acl, _ := data["acl"].(map[string]any)
	return decodeKafkaAcl(acl), created, nil
}

func (p typedManagedKafka) RemoveAclEntry(ctx context.Context, project, location, cluster, id string, entry managedkafkaui.AclEntry) (*mkstore.Acl, bool, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.RemoveAclEntry", project, location, p.params(project, location, cluster, map[string]any{"aclId": id, "body": kafkaEntry(entry)}), nil)
	if !covered {
		return p.ProviderInterface.RemoveAclEntry(ctx, project, location, cluster, id, entry)
	}
	if err != nil {
		return nil, false, err
	}
	if deleted, _ := data["aclDeleted"].(bool); deleted {
		return nil, true, nil
	}
	acl, _ := data["acl"].(map[string]any)
	a := decodeKafkaAcl(acl)
	return &a, false, nil
}

// ---- consumer groups ---------------------------------------------------------------

func (p typedManagedKafka) ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]managedkafkacore.ConsumerGroup, error) {
	data, covered, err := p.c.do(ctx, "ManagedKafka.ListConsumerGroups", project, location, p.params(project, location, cluster, map[string]any{"view": "CONSUMER_GROUP_VIEW_FULL"}), nil)
	if !covered {
		return p.ProviderInterface.ListConsumerGroups(ctx, project, location, cluster)
	}
	if err != nil {
		return nil, err
	}
	var out []managedkafkacore.ConsumerGroup
	for _, m := range items(data, "consumerGroups") {
		out = append(out, decodeKafkaGroup(m))
	}
	return out, nil
}

func (p typedManagedKafka) UpdateConsumerGroup(ctx context.Context, project, location, cluster, group string, in managedkafkaui.ConsumerGroupWriteInput) (managedkafkacore.ConsumerGroup, error) {
	topics := map[string]any{}
	for _, o := range in.Offsets {
		t, _ := topics[o.Topic].(map[string]any)
		if t == nil {
			t = map[string]any{"partitions": map[string]any{}}
			topics[o.Topic] = t
		}
		entry := map[string]any{"offset": strconv.FormatInt(o.Offset, 10)}
		if o.Metadata != "" {
			entry["metadata"] = o.Metadata
		}
		t["partitions"].(map[string]any)[strconv.Itoa(int(o.Partition))] = entry
	}
	data, covered, err := p.c.do(ctx, "ManagedKafka.UpdateConsumerGroup", project, location, p.params(project, location, cluster,
		map[string]any{"consumerGroupId": group, "updateMask": "topics", "body": map[string]any{"topics": topics}}), nil)
	if !covered {
		return p.ProviderInterface.UpdateConsumerGroup(ctx, project, location, cluster, group, in)
	}
	if err != nil {
		return managedkafkacore.ConsumerGroup{}, err
	}
	return decodeKafkaGroup(data), nil
}

func (p typedManagedKafka) DeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) error {
	_, covered, err := p.c.do(ctx, "ManagedKafka.DeleteConsumerGroup", project, location, p.params(project, location, cluster, map[string]any{"consumerGroupId": group}), nil)
	if !covered {
		return p.ProviderInterface.DeleteConsumerGroup(ctx, project, location, cluster, group)
	}
	return err
}
