package managedkafka

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/managedkafka"
)

// Provider handles the Managed Kafka v1 REST data plane. It is a thin adapter:
// every handler resolves the NormalizedRequest params into the core's typed API,
// calls the shared core Service, and encodes the result as Discovery-shaped
// JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Managed Kafka REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "ManagedKafka.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ManagedKafka.CreateCluster":       p.CreateCluster,
		"ManagedKafka.GetCluster":          p.GetCluster,
		"ManagedKafka.ListClusters":        p.ListClusters,
		"ManagedKafka.UpdateCluster":       p.UpdateCluster,
		"ManagedKafka.DeleteCluster":       p.DeleteCluster,
		"ManagedKafka.GetOperation":        p.GetOperation,
		"ManagedKafka.ListOperations":      p.ListOperations,
		"ManagedKafka.CreateTopic":         p.CreateTopic,
		"ManagedKafka.GetTopic":            p.GetTopic,
		"ManagedKafka.ListTopics":          p.ListTopics,
		"ManagedKafka.UpdateTopic":         p.UpdateTopic,
		"ManagedKafka.DeleteTopic":         p.DeleteTopic,
		"ManagedKafka.ListConsumerGroups":  p.ListConsumerGroups,
		"ManagedKafka.GetConsumerGroup":    p.GetConsumerGroup,
		"ManagedKafka.UpdateConsumerGroup": p.UpdateConsumerGroup,
		"ManagedKafka.DeleteConsumerGroup": p.DeleteConsumerGroup,
		"ManagedKafka.CreateAcl":           p.CreateAcl,
		"ManagedKafka.GetAcl":              p.GetAcl,
		"ManagedKafka.ListAcls":            p.ListAcls,
		"ManagedKafka.UpdateAcl":           p.UpdateAcl,
		"ManagedKafka.DeleteAcl":           p.DeleteAcl,
		"ManagedKafka.AddAclEntry":         p.AddAclEntry,
		"ManagedKafka.RemoveAclEntry":      p.RemoveAclEntry,
	}
}

// project resolves the owning project: the path project, else the request's
// account (project) scope, else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// --- Clusters ---

func (p *Provider) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	body := bodyOf(nr)
	_, op, err := p.core.CreateCluster(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), clusterInputFrom(body))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

func (p *Provider) GetCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	c, err := p.core.GetCluster(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.ClusterJSON(c, project)), nil
}

func (p *Provider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListClusters(ctx, project, strParam(nr, "location"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, c := range page {
		items = append(items, core.ClusterJSON(c, project))
	}
	out := map[string]any{"clusters": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) UpdateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	body := bodyOf(nr)
	_, op, err := p.core.UpdateCluster(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), clusterInputFrom(body))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

func (p *Provider) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	op, err := p.core.DeleteCluster(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

// --- Operations ---

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	op, err := p.core.GetOperation(ctx, project, strParam(nr, "location"), strParam(nr, "operationId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

// ResolveOperation resolves a Managed Kafka operation by its location-scoped
// name. handled reports whether Managed Kafka owns the name; a name the store
// does not know — including one belonging to another service that shares the
// locations/{location}/operations/{id} path — returns handled=false so the
// caller falls through. It satisfies the Workflows REST provider's
// cross-service OperationResolver, mirroring the gRPC operations.Resolver.
func (p *Provider) ResolveOperation(ctx context.Context, project, location, opID string) (map[string]any, bool, error) {
	if location == "" || opID == "" {
		return nil, false, nil
	}
	op, err := p.core.GetOperation(ctx, project, location, opID)
	if err != nil {
		if core.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return core.OperationJSON(op, project), true, nil
}

func (p *Provider) ListOperations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListOperations(ctx, project, strParam(nr, "location"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, op := range page {
		items = append(items, core.OperationJSON(op, project))
	}
	out := map[string]any{"operations": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

// --- Topics ---

func (p *Provider) CreateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	t, err := p.core.CreateTopic(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "topicId"), topicInputFrom(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.TopicJSON(t, project)), nil
}

func (p *Provider) GetTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	t, err := p.core.GetTopic(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "topicId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.TopicJSON(t, project)), nil
}

func (p *Provider) ListTopics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListTopics(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, t := range page {
		items = append(items, core.TopicJSON(t, project))
	}
	out := map[string]any{"topics": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) UpdateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	t, err := p.core.UpdateTopic(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "topicId"), topicInputFrom(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.TopicJSON(t, project)), nil
}

func (p *Provider) DeleteTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	if err := p.core.DeleteTopic(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "topicId")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// --- Consumer groups ---

func (p *Provider) ListConsumerGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListConsumerGroups(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"),
		strParam(nr, "view"), strParam(nr, "filter"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, g := range page {
		items = append(items, core.ConsumerGroupJSON(g, project))
	}
	out := map[string]any{"consumerGroups": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) GetConsumerGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	g, err := p.core.GetConsumerGroup(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "consumerGroupId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.ConsumerGroupJSON(g, project)), nil
}

func (p *Provider) UpdateConsumerGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	g, err := p.core.UpdateConsumerGroup(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"),
		strParam(nr, "consumerGroupId"), consumerGroupInputFrom(bodyOf(nr)), fieldMaskPaths(strParam(nr, "updateMask")))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.ConsumerGroupJSON(g, project)), nil
}

func (p *Provider) DeleteConsumerGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteConsumerGroup(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "consumerGroupId")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// --- ACLs ---

func (p *Provider) CreateAcl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	a, err := p.core.CreateAcl(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "aclId"), aclInputFrom(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.AclJSON(a, project)), nil
}

func (p *Provider) GetAcl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	a, err := p.core.GetAcl(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "aclId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.AclJSON(a, project)), nil
}

func (p *Provider) ListAcls(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListAcls(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, a := range page {
		items = append(items, core.AclJSON(a, project))
	}
	out := map[string]any{"acls": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) UpdateAcl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	a, err := p.core.UpdateAcl(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "aclId"), aclInputFrom(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.AclJSON(a, project)), nil
}

func (p *Provider) DeleteAcl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	if err := p.core.DeleteAcl(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "aclId")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) AddAclEntry(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	a, created, err := p.core.AddAclEntry(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "aclId"), aclEntryFrom(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"acl": core.AclJSON(a, project), "aclCreated": created}), nil
}

func (p *Provider) RemoveAclEntry(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	a, deleted, err := p.core.RemoveAclEntry(ctx, project, strParam(nr, "location"), strParam(nr, "clusterId"), strParam(nr, "aclId"), aclEntryFrom(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	if deleted {
		return provider.OK(map[string]any{"aclDeleted": true}), nil
	}
	return provider.OK(map[string]any{"acl": core.AclJSON(*a, project)}), nil
}

// --- body → core input ---

// fieldMaskPaths splits a google-fieldmask value (a comma-separated path list)
// into its paths.
func fieldMaskPaths(mask string) []string {
	if mask == "" {
		return nil
	}
	parts := strings.Split(mask, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func clusterInputFrom(body map[string]any) core.ClusterInput {
	in := core.ClusterInput{Labels: bodyStringMap(body, "labels")}
	if len(body) > 0 {
		if data, err := json.Marshal(body); err == nil {
			in.Config = data
		}
	}
	return in
}

func topicInputFrom(body map[string]any) core.TopicInput {
	in := core.TopicInput{
		PartitionCount:    bodyInt(body, "partitionCount"),
		ReplicationFactor: bodyInt(body, "replicationFactor"),
	}
	if len(body) > 0 {
		if data, err := json.Marshal(body); err == nil {
			in.Config = data
		}
	}
	return in
}

func aclInputFrom(body map[string]any) core.AclInput {
	in := core.AclInput{Etag: strFrom(body["etag"])}
	if list, ok := body["aclEntries"].([]any); ok {
		for _, item := range list {
			if m, ok := item.(map[string]any); ok {
				in.AclEntries = append(in.AclEntries, aclEntryFrom(m))
			}
		}
	}
	return in
}

// consumerGroupInputFrom reads the committed offsets from a consumerGroup PATCH
// body: topics.<topicResourceName>.partitions.<index>.{offset,metadata}.
func consumerGroupInputFrom(body map[string]any) core.ConsumerGroupInput {
	var in core.ConsumerGroupInput
	topics, ok := body["topics"].(map[string]any)
	if !ok {
		return in
	}
	for topic, rawTopic := range topics {
		tm, ok := rawTopic.(map[string]any)
		if !ok {
			continue
		}
		partitions, ok := tm["partitions"].(map[string]any)
		if !ok {
			continue
		}
		for idx, rawPartition := range partitions {
			partition, err := strconv.Atoi(idx)
			if err != nil {
				continue
			}
			pm, ok := rawPartition.(map[string]any)
			if !ok {
				continue
			}
			in.Offsets = append(in.Offsets, core.ConsumerGroupOffset{
				Topic:     topic,
				Partition: int32(partition),
				Offset:    int64(bodyInt(pm, "offset")),
				Metadata:  strFrom(pm["metadata"]),
			})
		}
	}
	return in
}

func aclEntryFrom(m map[string]any) core.AclEntryInput {
	return core.AclEntryInput{
		Principal:      strFrom(m["principal"]),
		PermissionType: strFrom(m["permissionType"]),
		Operation:      strFrom(m["operation"]),
		Host:           strFrom(m["host"]),
	}
}

func strFrom(v any) string {
	s, _ := v.(string)
	return s
}
