package managedkafkaui

import (
	"context"
	"encoding/json"

	managedkafkacore "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

// ProviderInterface is the subset of *managedkafkacore.Service used by the
// Managed Kafka UI handlers. The core is transport-neutral and addressable
// directly, so the UI reuses its typed API rather than going through a REST
// adapter. It keeps the UI decoupled from the core's concrete type and hides
// the long-running operations and cursor pages the mutations/lists return.
type ProviderInterface interface {
	// ListAllClusters lists every cluster in a project across all locations,
	// sorted by location then name. It backs the location-optional console
	// list.
	ListAllClusters(ctx context.Context, project string) ([]mkstore.Cluster, error)

	// GetCluster returns one cluster.
	GetCluster(ctx context.Context, project, location, cluster string) (mkstore.Cluster, error)

	// ListAllTopics lists every topic in a project across all locations and
	// clusters, sorted by location, cluster, then name.
	ListAllTopics(ctx context.Context, project string) ([]mkstore.Topic, error)

	// ListTopics lists the topics of one cluster.
	ListTopics(ctx context.Context, project, location, cluster string) ([]mkstore.Topic, error)

	// GetTopic returns one topic of a cluster.
	GetTopic(ctx context.Context, project, location, cluster, topic string) (mkstore.Topic, error)

	// ListAcls lists a cluster's ACLs (read-only console surface).
	ListAcls(ctx context.Context, project, location, cluster string) ([]mkstore.Acl, error)

	// ListConsumerGroups lists a cluster's consumer groups from its live
	// broker's group coordinator, including committed offsets. With no live
	// broker (the default mock topology) the list is empty.
	ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]managedkafkacore.ConsumerGroup, error)

	// CreateCluster/UpdateCluster deploy and reconfigure a cluster. The create
	// LRO is discarded (the emulator stores operations done inline).
	CreateCluster(ctx context.Context, project, location, id string, in ClusterWriteInput) (mkstore.Cluster, error)
	UpdateCluster(ctx context.Context, project, location, id string, in ClusterWriteInput) (mkstore.Cluster, error)

	// CreateTopic/UpdateTopic/DeleteTopic manage a cluster's topics.
	CreateTopic(ctx context.Context, project, location, cluster, id string, in TopicWriteInput) (mkstore.Topic, error)
	UpdateTopic(ctx context.Context, project, location, cluster, id string, in TopicWriteInput) (mkstore.Topic, error)
	DeleteTopic(ctx context.Context, project, location, cluster, id string) error

	// CreateAcl/UpdateAcl/DeleteAcl and AddAclEntry/RemoveAclEntry manage a
	// cluster's ACLs and their entries.
	CreateAcl(ctx context.Context, project, location, cluster, id string, in AclWriteInput) (mkstore.Acl, error)
	UpdateAcl(ctx context.Context, project, location, cluster, id string, in AclWriteInput) (mkstore.Acl, error)
	DeleteAcl(ctx context.Context, project, location, cluster, id string) error
	AddAclEntry(ctx context.Context, project, location, cluster, id string, entry AclEntry) (mkstore.Acl, bool, error)
	RemoveAclEntry(ctx context.Context, project, location, cluster, id string, entry AclEntry) (*mkstore.Acl, bool, error)

	// UpdateConsumerGroup sets a group's committed offsets; DeleteConsumerGroup
	// removes it.
	UpdateConsumerGroup(ctx context.Context, project, location, cluster, group string, in ConsumerGroupWriteInput) (managedkafkacore.ConsumerGroup, error)
	DeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) error
}

// Provider adapts the transport-neutral Managed Kafka core onto
// ProviderInterface, discarding cursor pages and long-running operations.
type Provider struct {
	svc *managedkafkacore.Service
}

// NewProvider returns a UI provider over the Managed Kafka core.
func NewProvider(svc *managedkafkacore.Service) *Provider { return &Provider{svc: svc} }

var _ ProviderInterface = (*Provider)(nil)

// ListAllClusters implements ProviderInterface.
func (p *Provider) ListAllClusters(ctx context.Context, project string) ([]mkstore.Cluster, error) {
	return p.svc.ListAllClusters(ctx, project)
}

// GetCluster implements ProviderInterface.
func (p *Provider) GetCluster(ctx context.Context, project, location, cluster string) (mkstore.Cluster, error) {
	return p.svc.GetCluster(ctx, project, location, cluster)
}

// ListAllTopics implements ProviderInterface.
func (p *Provider) ListAllTopics(ctx context.Context, project string) ([]mkstore.Topic, error) {
	return p.svc.ListAllTopics(ctx, project)
}

// ListTopics implements ProviderInterface; every cursor page is followed so
// the console's client-side pagination sees the full cluster (the core pages at
// pageSize<=1000 otherwise).
func (p *Provider) ListTopics(ctx context.Context, project, location, cluster string) ([]mkstore.Topic, error) {
	var out []mkstore.Topic
	token := ""
	for {
		page, next, err := p.svc.ListTopics(ctx, project, location, cluster, uiPageSize, token)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" {
			return out, nil
		}
		token = next
	}
}

// GetTopic implements ProviderInterface.
func (p *Provider) GetTopic(ctx context.Context, project, location, cluster, topic string) (mkstore.Topic, error) {
	return p.svc.GetTopic(ctx, project, location, cluster, topic)
}

// ListAcls implements ProviderInterface; every cursor page is followed so the
// console sees every ACL.
func (p *Provider) ListAcls(ctx context.Context, project, location, cluster string) ([]mkstore.Acl, error) {
	var out []mkstore.Acl
	token := ""
	for {
		page, next, err := p.svc.ListAcls(ctx, project, location, cluster, uiPageSize, token)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" {
			return out, nil
		}
		token = next
	}
}

// ListConsumerGroups implements ProviderInterface; the FULL view is requested
// so committed offsets are included, and every cursor page is followed.
func (p *Provider) ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]managedkafkacore.ConsumerGroup, error) {
	var out []managedkafkacore.ConsumerGroup
	token := ""
	for {
		page, next, err := p.svc.ListConsumerGroups(ctx, project, location, cluster, managedkafkacore.ConsumerGroupViewFull, "", uiPageSize, token)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" {
			return out, nil
		}
		token = next
	}
}

// ─── writes ──────────────────────────────────────────────────────────────────

// CreateCluster implements ProviderInterface; the create LRO is discarded.
func (p *Provider) CreateCluster(ctx context.Context, project, location, id string, in ClusterWriteInput) (mkstore.Cluster, error) {
	c, _, err := p.svc.CreateCluster(ctx, project, location, id, clusterInput(in))
	return c, err
}

// UpdateCluster implements ProviderInterface; the update LRO is discarded.
func (p *Provider) UpdateCluster(ctx context.Context, project, location, id string, in ClusterWriteInput) (mkstore.Cluster, error) {
	c, _, err := p.svc.UpdateCluster(ctx, project, location, id, clusterInput(in))
	return c, err
}

// CreateTopic implements ProviderInterface.
func (p *Provider) CreateTopic(ctx context.Context, project, location, cluster, id string, in TopicWriteInput) (mkstore.Topic, error) {
	return p.svc.CreateTopic(ctx, project, location, cluster, id, topicInput(in))
}

// UpdateTopic implements ProviderInterface.
func (p *Provider) UpdateTopic(ctx context.Context, project, location, cluster, id string, in TopicWriteInput) (mkstore.Topic, error) {
	return p.svc.UpdateTopic(ctx, project, location, cluster, id, topicInput(in))
}

// DeleteTopic implements ProviderInterface.
func (p *Provider) DeleteTopic(ctx context.Context, project, location, cluster, id string) error {
	return p.svc.DeleteTopic(ctx, project, location, cluster, id)
}

// CreateAcl implements ProviderInterface.
func (p *Provider) CreateAcl(ctx context.Context, project, location, cluster, id string, in AclWriteInput) (mkstore.Acl, error) {
	return p.svc.CreateAcl(ctx, project, location, cluster, id, aclInput(in))
}

// UpdateAcl implements ProviderInterface.
func (p *Provider) UpdateAcl(ctx context.Context, project, location, cluster, id string, in AclWriteInput) (mkstore.Acl, error) {
	return p.svc.UpdateAcl(ctx, project, location, cluster, id, aclInput(in))
}

// DeleteAcl implements ProviderInterface.
func (p *Provider) DeleteAcl(ctx context.Context, project, location, cluster, id string) error {
	return p.svc.DeleteAcl(ctx, project, location, cluster, id)
}

// AddAclEntry implements ProviderInterface. created reports whether the entry
// materialized a new ACL (the emulator's AddAclEntry creates a missing ACL).
func (p *Provider) AddAclEntry(ctx context.Context, project, location, cluster, id string, entry AclEntry) (mkstore.Acl, bool, error) {
	return p.svc.AddAclEntry(ctx, project, location, cluster, id, aclEntryInput(entry))
}

// RemoveAclEntry implements ProviderInterface. deleted reports whether removing
// the last entry deleted the ACL (a nil ACL, matching the core).
func (p *Provider) RemoveAclEntry(ctx context.Context, project, location, cluster, id string, entry AclEntry) (*mkstore.Acl, bool, error) {
	return p.svc.RemoveAclEntry(ctx, project, location, cluster, id, aclEntryInput(entry))
}

// UpdateConsumerGroup implements ProviderInterface. The update mask is empty:
// the console PATCHes the full committed-offset set (its only writable field).
func (p *Provider) UpdateConsumerGroup(ctx context.Context, project, location, cluster, group string, in ConsumerGroupWriteInput) (managedkafkacore.ConsumerGroup, error) {
	offsets := make([]managedkafkacore.ConsumerGroupOffset, 0, len(in.Offsets))
	for _, o := range in.Offsets {
		offsets = append(offsets, managedkafkacore.ConsumerGroupOffset{
			Topic:     o.Topic,
			Partition: int32(o.Partition),
			Offset:    o.Offset,
			Metadata:  o.Metadata,
		})
	}
	return p.svc.UpdateConsumerGroup(ctx, project, location, cluster, group, managedkafkacore.ConsumerGroupInput{Offsets: offsets}, nil)
}

// DeleteConsumerGroup implements ProviderInterface.
func (p *Provider) DeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) error {
	return p.svc.DeleteConsumerGroup(ctx, project, location, cluster, group)
}

// clusterInput maps the console body onto the core's cluster input.
func clusterInput(in ClusterWriteInput) managedkafkacore.ClusterInput {
	return managedkafkacore.ClusterInput{Labels: in.Labels, Config: in.Config}
}

// topicInput maps the console body onto the core's topic input. The core reads
// the Kafka property overrides from the verbatim body's "configs" object, so
// they are re-wrapped in that shape.
func topicInput(in TopicWriteInput) managedkafkacore.TopicInput {
	out := managedkafkacore.TopicInput{
		PartitionCount:    in.PartitionCount,
		ReplicationFactor: in.ReplicationFactor,
	}
	if len(in.Configs) > 0 {
		if b, err := json.Marshal(map[string]any{"configs": in.Configs}); err == nil {
			out.Config = b
		}
	}
	return out
}

// aclInput maps the console body onto the core's ACL input.
func aclInput(in AclWriteInput) managedkafkacore.AclInput {
	out := managedkafkacore.AclInput{Etag: in.Etag}
	for _, e := range in.Entries {
		out.AclEntries = append(out.AclEntries, aclEntryInput(e))
	}
	return out
}

// aclEntryInput maps one console entry onto the core's entry input.
func aclEntryInput(e AclEntry) managedkafkacore.AclEntryInput {
	return managedkafkacore.AclEntryInput{
		Principal:      e.Principal,
		PermissionType: e.PermissionType,
		Operation:      e.Operation,
		Host:           e.Host,
	}
}

// uiPageSize is the cursor page size the UI walks. The console paginates
// client-side, so the provider follows every page rather than surfacing a
// next-page token; it mirrors the core's default page cap.
const uiPageSize = 1000
