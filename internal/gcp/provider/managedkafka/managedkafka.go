// Package managedkafka implements the Apache Kafka for BigQuery (Managed Kafka)
// v1 provider (managedkafka.googleapis.com/v1): Cluster and Topic resources.
//
// A cluster is a logical record only — the emulator never stands up a real
// broker. Cluster create/update/delete return the resource inline wrapped in a
// done long-running operation (matching the test's expectation that the SDK
// reads `done` + `response` from the HTTP body); topic CRUD is fully synchronous
// and returns the resource directly. Consumer groups are not tracked — their
// list endpoint always returns an empty list.
package managedkafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles Managed Kafka v1 cluster and topic resources.
type Provider struct {
	store mkstore.Store
}

// New returns a Provider backed by the given store.
func New(s mkstore.Store) *Provider {
	return &Provider{store: s}
}

// Reset wipes the store.
func (p *Provider) Reset(ctx context.Context) { p.store.Reset(ctx) }

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ManagedKafka.CreateCluster":      p.CreateCluster,
		"ManagedKafka.GetCluster":         p.GetCluster,
		"ManagedKafka.ListClusters":       p.ListClusters,
		"ManagedKafka.UpdateCluster":      p.UpdateCluster,
		"ManagedKafka.DeleteCluster":      p.DeleteCluster,
		"ManagedKafka.CreateTopic":        p.CreateTopic,
		"ManagedKafka.GetTopic":           p.GetTopic,
		"ManagedKafka.ListTopics":         p.ListTopics,
		"ManagedKafka.UpdateTopic":        p.UpdateTopic,
		"ManagedKafka.DeleteTopic":        p.DeleteTopic,
		"ManagedKafka.ListConsumerGroups": p.ListConsumerGroups,
		// Consumer-group resource operations (get/update/delete) are not
		// tracked by the emulator; they route to an Unimplemented response.
		"ManagedKafka.GetConsumerGroup":    p.consumerGroupUnimplemented,
		"ManagedKafka.UpdateConsumerGroup": p.consumerGroupUnimplemented,
		"ManagedKafka.DeleteConsumerGroup": p.consumerGroupUnimplemented,
	}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyMap(body map[string]any, key string) map[string]any {
	if body == nil {
		return nil
	}
	m, _ := body[key].(map[string]any)
	return m
}

func bodyStringMap(body map[string]any, key string) map[string]string {
	m := bodyMap(body, key)
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func bodyInt(body map[string]any, key string) int {
	if body == nil {
		return 0
	}
	switch v := body[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return int(n)
		}
	}
	return 0
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, mkstore.ErrNoSuchCluster):
		return model.NewProviderError("NotFound", "cluster not found", 404)
	case errors.Is(err, mkstore.ErrNoSuchTopic):
		return model.NewProviderError("NotFound", "topic not found", 404)
	case errors.Is(err, mkstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	}
	return err
}

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// --- Wire rendering ---

// clusterMap renders a store Cluster as a managedkafka.v1.Cluster wire map.
// capacityConfig, gcpConfig, rebalanceConfig, and tlsConfig are echoed from the
// stored config verbatim.
func (p *Provider) clusterMap(nr *model.NormalizedRequest, c mkstore.Cluster) map[string]any {
	name := nr.ResourceID("managedkafka-cluster", c.Location+"/"+c.Name)
	out := map[string]any{
		"name":       name,
		"state":      "ACTIVE",
		"createTime": formatTimestamp(c.CreateTime),
		"updateTime": formatTimestamp(c.UpdateTime),
	}
	var cfg map[string]any
	if len(c.Config) > 0 {
		_ = json.Unmarshal(c.Config, &cfg)
	}
	for _, k := range []string{"capacityConfig", "gcpConfig", "rebalanceConfig", "tlsConfig"} {
		if v, ok := cfg[k].(map[string]any); ok {
			out[k] = v
		}
	}
	labels := c.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
	return out
}

// topicMap renders a store Topic as a managedkafka.v1.Topic wire map.
func (p *Provider) topicMap(nr *model.NormalizedRequest, t mkstore.Topic) map[string]any {
	name := nr.ResourceID("managedkafka-topic", t.Location+"/"+t.ClusterName+"/"+t.Name)
	return map[string]any{
		"name":              name,
		"partitionCount":    t.PartitionCount,
		"replicationFactor": t.ReplicationFactor,
	}
}

// --- Clusters ---

// clusterLRO wraps a cluster mutation in the emulator's flattened synchronous
// long-running-operation shape, {"done":true,"response":{...}}. The proto
// returns a google.longrunning.Operation (with name/metadata/@type); this is a
// deliberate simplification so SDKs reading done+response from the body succeed.
func (p *Provider) clusterLRO(nr *model.NormalizedRequest, c mkstore.Cluster) *model.ProviderResponse {
	return provider.OK(map[string]any{"done": true, "response": p.clusterMap(nr, c)})
}

func (p *Provider) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	if location == "" || clusterID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or clusterId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	now := clock.Now().UTC()
	c := mkstore.Cluster{
		Location:   location,
		Name:       clusterID,
		Labels:     bodyStringMap(body, "labels"),
		CreateTime: now,
		UpdateTime: now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			c.Config = data
		}
	}
	if err := p.store.CreateCluster(ctx, nr.AccountID, location, c); err != nil {
		return nil, mapErr(err)
	}
	return p.clusterLRO(nr, c), nil
}

func (p *Provider) GetCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	if location == "" || clusterID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or clusterId", 400)
	}
	c, err := p.store.GetCluster(ctx, nr.AccountID, location, clusterID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.clusterMap(nr, c)), nil
}

func (p *Provider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	clusters, err := p.store.ListClusters(ctx, nr.AccountID, location)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(clusters, func(c mkstore.Cluster) string { return c.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, c := range page {
		items = append(items, p.clusterMap(nr, c))
	}
	resp := map[string]any{"clusters": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	if location == "" || clusterID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or clusterId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	c, err := p.store.UpdateClusterAtomic(ctx, nr.AccountID, location, clusterID, func(c mkstore.Cluster) (mkstore.Cluster, error) {
		if body != nil {
			stored := map[string]any{}
			if len(c.Config) > 0 {
				_ = json.Unmarshal(c.Config, &stored)
			}
			for k, v := range body {
				stored[k] = v
			}
			if labels := bodyStringMap(body, "labels"); labels != nil {
				c.Labels = labels
			}
			if data, err := json.Marshal(stored); err == nil {
				c.Config = data
			}
		}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return p.clusterLRO(nr, c), nil
}

func (p *Provider) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	if location == "" || clusterID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or clusterId", 400)
	}
	if err := p.store.DeleteCluster(ctx, nr.AccountID, location, clusterID); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{"done": true, "response": map[string]any{}}), nil
}

// --- Topics ---

func (p *Provider) CreateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	topicID := strParam(nr, "topicId")
	if location == "" || clusterID == "" || topicID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, clusterId, or topicId", 400)
	}
	if _, err := p.store.GetCluster(ctx, nr.AccountID, location, clusterID); err != nil {
		return nil, mapErr(err)
	}
	body, _ := nr.Params["body"].(map[string]any)
	now := clock.Now().UTC()
	t := mkstore.Topic{
		Location:          location,
		ClusterName:       clusterID,
		Name:              topicID,
		PartitionCount:    bodyInt(body, "partitionCount"),
		ReplicationFactor: bodyInt(body, "replicationFactor"),
		CreateTime:        now,
		UpdateTime:        now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			t.Config = data
		}
	}
	if err := p.store.CreateTopic(ctx, nr.AccountID, location, clusterID, t); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.topicMap(nr, t)), nil
}

func (p *Provider) GetTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	topicID := strParam(nr, "topicId")
	if location == "" || clusterID == "" || topicID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, clusterId, or topicId", 400)
	}
	t, err := p.store.GetTopic(ctx, nr.AccountID, location, clusterID, topicID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.topicMap(nr, t)), nil
}

func (p *Provider) ListTopics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	if location == "" || clusterID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or clusterId", 400)
	}
	topics, err := p.store.ListTopics(ctx, nr.AccountID, location, clusterID)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(topics, func(t mkstore.Topic) string { return t.Name }, nr.Params)
	items := make([]any, 0, len(page))
	for _, t := range page {
		items = append(items, p.topicMap(nr, t))
	}
	resp := map[string]any{"topics": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	topicID := strParam(nr, "topicId")
	if location == "" || clusterID == "" || topicID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, clusterId, or topicId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	t, err := p.store.UpdateTopicAtomic(ctx, nr.AccountID, location, clusterID, topicID, func(t mkstore.Topic) (mkstore.Topic, error) {
		if body != nil {
			if _, ok := body["partitionCount"]; ok {
				t.PartitionCount = bodyInt(body, "partitionCount")
			}
			if _, ok := body["replicationFactor"]; ok {
				t.ReplicationFactor = bodyInt(body, "replicationFactor")
			}
			stored := map[string]any{}
			if len(t.Config) > 0 {
				_ = json.Unmarshal(t.Config, &stored)
			}
			for k, v := range body {
				stored[k] = v
			}
			if data, err := json.Marshal(stored); err == nil {
				t.Config = data
			}
		}
		t.UpdateTime = clock.Now().UTC()
		return t, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.topicMap(nr, t)), nil
}

func (p *Provider) DeleteTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	clusterID := strParam(nr, "clusterId")
	topicID := strParam(nr, "topicId")
	if location == "" || clusterID == "" || topicID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location, clusterId, or topicId", 400)
	}
	if err := p.store.DeleteTopic(ctx, nr.AccountID, location, clusterID, topicID); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

// ListConsumerGroups returns an empty list — the emulator does not track
// consumer-group state. The empty set is still passed through paging.Page so
// the list wire-shape matches the real paginated API.
func (p *Provider) ListConsumerGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var groups []string
	page, next := paging.Page(groups, func(s string) string { return s }, nr.Params)
	resp := map[string]any{"consumerGroups": page}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// consumerGroupUnimplemented returns Unimplemented for the consumer-group
// resource operations (get/update/delete) — the emulator tracks only the
// (empty) consumer-group list, not individual groups.
func (p *Provider) consumerGroupUnimplemented(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("Unimplemented", "consumer group operations are not implemented", 404)
}
