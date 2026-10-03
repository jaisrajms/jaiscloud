// Package managedkafka is the transport-neutral core of the Apache Kafka for
// BigQuery (Managed Kafka) v1 service (managedkafka.googleapis.com). It owns all
// cluster, topic, ACL, operation, and consumer-group business logic over
// internal/gcp/store/managedkafka.
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/managedkafka) and the REST
// transport (internal/gcp/transport/rest/managedkafka) both transcode their wire
// format into this package's typed API and then call the SAME Service instance.
// That is the dual-protocol invariant: one core, one piece of state, so the
// transports cannot drift.
//
// A cluster's metadata is the source of truth for wire read-back; a real
// Kafka-wire broker behind its bootstrapAddress is optional and injected via
// WithBroker (the default mock topology starts none). When a live broker is
// present, topic create/update/delete are mirrored onto it: the metadata write
// is rolled back if the broker rejects it (and the broker topic is removed
// before the metadata on delete), so an API-visible topic is guaranteed to
// exist on the broker. Cluster
// create/update/delete return a done google.longrunning.Operation
// inline so official clients that read done+response from the body succeed
// without polling; the operation is also persisted under
// projects/{project}/locations/{location}/operations/{id}. The LRO timing is
// opt-in via WithLROMode: when enabled, operations are stored done=false and
// settle lazily on read (see settle); the default remains synchronous. Topic and
// ACL CRUD are fully synchronous. Consumer groups are read from the Kafka
// broker's group coordinator: there is no create RPC, so a group exists only
// once a client commits offsets to the cluster and the broker is the source of
// truth. With a live broker the core lists groups, reads committed offsets, and
// resets/removes them through the injected Broker; with no broker (the mock
// topology) the list is empty and get/update/delete report NOT_FOUND, matching
// real GCP for an absent group.
package managedkafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	"jaiscloud/internal/gcp/paging"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

// Broker starts and tracks the real Kafka-wire broker backing a cluster's
// bootstrapAddress. It is optional: when nil (the default) the service is the
// pure metadata control plane and renders the synthesized cloud.goog address.
// The concrete implementation is internal/gcp/broker/kafka; the plain-string
// method set keeps this package free of any Kubernetes dependency.
type Broker interface {
	// EnsureCluster starts (or reuses) the broker and returns its bootstrap
	// endpoint. An empty endpoint with a nil error means no broker (mock); an
	// error means the caller should degrade to the synthesized address.
	EnsureCluster(ctx context.Context, project, location, cluster string) (string, error)
	// Endpoint returns the live endpoint, or "" when no broker is running.
	Endpoint(project, location, cluster string) string
	// StopCluster stops and reaps the broker when the cluster is deleted.
	StopCluster(ctx context.Context, project, location, cluster string) error
	// EnsureTopic provisions the topic on the cluster's live broker with the
	// caller's property overrides (the Kafka topic configs map). With no
	// running broker (mock, or a cluster without a live broker) it is a
	// metadata-only no-op, so hermetic tests need no broker.
	EnsureTopic(ctx context.Context, project, location, cluster, topic string, partitions int, configs map[string]string) error
	// AlterTopicConfigs applies an incremental topic-config change on the
	// cluster's live broker: every key in set is written and every key in
	// remove is cleared. With no running broker it is a metadata-only no-op. A
	// broker failure is returned so the caller can roll the metadata write
	// back; an invalid key/value is reported as ErrInvalidTopicConfig.
	AlterTopicConfigs(ctx context.Context, project, location, cluster, topic string, set map[string]string, remove []string) error
	// AddTopicPartitions raises the topic's broker partition count to
	// totalPartitions. Lowering the count is rejected by the broker.
	AddTopicPartitions(ctx context.Context, project, location, cluster, topic string, totalPartitions int) error
	// DeleteBrokerTopic removes the topic from the broker. It is idempotent and
	// a no-op when no broker is running.
	DeleteBrokerTopic(ctx context.Context, project, location, cluster, topic string) error
	// ListConsumerGroups returns the consumer-group ids known to the cluster's
	// group coordinator. With no running broker it returns an empty set.
	ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]string, error)
	// ConsumerGroupOffsets returns the committed offsets for the group. found
	// is false when no broker is running or the coordinator does not know the
	// group; the topic is the bare Kafka topic id (no resource path).
	ConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string) ([]ConsumerGroupOffset, bool, error)
	// ConsumerGroupMembers returns the number of active members in the group.
	// found is false when no broker is running or the group does not exist.
	ConsumerGroupMembers(ctx context.Context, project, location, cluster, group string) (int, bool, error)
	// DeleteConsumerGroup removes the group and its committed offsets. existed
	// is false when no broker is running or the group was already absent.
	DeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) (bool, error)
	// CommitConsumerGroupOffsets sets the group's committed offsets. The caller
	// checks existence first; a no-broker topology never reaches here.
	CommitConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string, offsets []ConsumerGroupOffset) error
	// ReplaceAcl mirrors an ACL's entry set onto the cluster's live broker,
	// replacing every broker ACL binding for the ACL's resource pattern; an
	// empty entry set removes them. With no running broker it is a
	// metadata-only no-op. A broker failure is returned so the caller can roll
	// the metadata write back.
	ReplaceAcl(ctx context.Context, project, location, cluster, resourceType, resourceName, patternType string, entries []AclBinding) error
	// Reset stops and reaps every broker the manager owns so a
	// /_jaiscloud/reset leaves no broker Pod, Service, or subprocess behind.
	Reset(ctx context.Context) error
}

// Service is the transport-neutral Managed Kafka v1 service.
type Service struct {
	store mkstore.Store
	// lroMode controls operation timing. The zero value is synchronous: every
	// operation is stored done=true inline, matching the v1.1.0 contract. An
	// enabled mode stores operations done=false and settles them lazily on read.
	lroMode lro.Mode
	// broker is the optional real broker manager. Nil means mock-only.
	broker Broker
}

// Option configures Service.
type Option func(*Service)

// WithLROMode sets the long-running-operation timing mode. The zero value is
// synchronous; Mode{Enabled: true, Delay: d} stores create/update/delete
// operations done=false and settles them on read once d has elapsed.
func WithLROMode(m lro.Mode) Option {
	return func(s *Service) { s.lroMode = m }
}

// WithBroker injects the broker manager that backs cluster bootstrapAddresses.
// A nil broker is ignored, leaving the mock topology.
func WithBroker(b Broker) Option {
	return func(s *Service) {
		if b != nil {
			s.broker = b
		}
	}
}

// NewService returns a Managed Kafka core backed by the given store.
func NewService(s mkstore.Store, opts ...Option) *Service {
	svc := &Service{store: s}
	for _, o := range opts {
		o(svc)
	}
	return svc
}

// Reset wipes the store and reaps every broker the injected manager owns, so
// /_jaiscloud/reset leaves no broker Pod, Service, or subprocess behind. Broker
// teardown failures are logged, not surfaced: the metadata wipe has already
// happened and the next cluster create starts a fresh broker.
func (s *Service) Reset(ctx context.Context) {
	if s.broker != nil {
		if err := s.broker.Reset(ctx); err != nil {
			slog.Warn("managedkafka: broker reset failed", "err", err)
		}
	}
	s.store.Reset(ctx)
}

// ensureBroker starts the real broker for a newly created cluster and returns
// its endpoint. A broker failure degrades to the mock topology: the cluster
// still exists and renders the synthesized address, and the failure is logged.
func (s *Service) ensureBroker(ctx context.Context, project, location, cluster string) string {
	if s.broker == nil {
		return ""
	}
	ep, err := s.broker.EnsureCluster(ctx, project, location, cluster)
	if err != nil {
		slog.Warn("managedkafka: broker unavailable; serving synthesized bootstrapAddress", "cluster", cluster, "err", err)
		return ""
	}
	return ep
}

// brokerEndpoint resolves the live endpoint without starting a broker.
func (s *Service) brokerEndpoint(project, location, cluster string) string {
	if s.broker == nil {
		return ""
	}
	return s.broker.Endpoint(project, location, cluster)
}

// brokerReady rehydrates the cluster's broker before a data-plane call. A
// cluster persisted under --dsn (or restored from an import) has no live broker
// until the first call, so this starts one lazily. It is a no-op with no broker
// manager or when a broker is already running; a start failure degrades to the
// metadata-only topology (logged by ensureBroker).
func (s *Service) brokerReady(ctx context.Context, project, location, cluster string) {
	if s.broker == nil || s.broker.Endpoint(project, location, cluster) != "" {
		return
	}
	s.ensureBroker(ctx, project, location, cluster)
}

// stopBroker reaps the broker for a deleted cluster. Failures are logged, not
// surfaced: the metadata delete has already succeeded.
func (s *Service) stopBroker(ctx context.Context, project, location, cluster string) {
	if s.broker == nil {
		return
	}
	if err := s.broker.StopCluster(ctx, project, location, cluster); err != nil {
		slog.Warn("managedkafka: broker stop failed", "cluster", cluster, "err", err)
	}
}

// brokerInternalError wraps a data-plane failure so both transports render it
// as INTERNAL (500): the metadata write was not committed, so the API-visible
// topic and the broker stay consistent.
func brokerInternalError(op string, err error) error {
	return model.NewProviderError("Internal", "managedkafka broker: "+op+": "+err.Error(), 500)
}

// brokerTopicError maps a topic data-plane failure onto the caller-visible
// error: a rejected config key/value is a client error (InvalidArgument, 400),
// while every other broker failure (unreachable, timed out) stays INTERNAL.
func brokerTopicError(op string, err error) error {
	if errors.Is(err, ErrInvalidTopicConfig) {
		return invalidArgument(err.Error())
	}
	return brokerInternalError(op, err)
}

// ErrInvalidTopicConfig reports that the broker rejected a topic's config
// key/value. The broker package wraps it so the core can surface a 400 instead
// of a 500 while still rolling the metadata write back.
var ErrInvalidTopicConfig = errors.New("invalid topic config")

// ensureBrokerTopic provisions the topic on the cluster's broker. It is a no-op
// when the service has no broker manager (mock topology) or the cluster has no
// live broker.
func (s *Service) ensureBrokerTopic(ctx context.Context, project, location, cluster, topic string, partitions int, configs map[string]string) error {
	if s.broker == nil {
		return nil
	}
	s.brokerReady(ctx, project, location, cluster)
	if err := s.broker.EnsureTopic(ctx, project, location, cluster, topic, partitions, configs); err != nil {
		return brokerTopicError("ensure topic", err)
	}
	return nil
}

// alterBrokerTopicConfigs applies an incremental config change to the topic on
// the cluster's broker. It is a no-op when the service has no broker manager
// (mock topology) or the cluster has no live broker.
func (s *Service) alterBrokerTopicConfigs(ctx context.Context, project, location, cluster, topic string, set map[string]string, remove []string) error {
	if s.broker == nil {
		return nil
	}
	s.brokerReady(ctx, project, location, cluster)
	if err := s.broker.AlterTopicConfigs(ctx, project, location, cluster, topic, set, remove); err != nil {
		return brokerTopicError("alter topic configs", err)
	}
	return nil
}

// addBrokerPartitions raises the topic's broker partition count, surfacing a
// data-plane failure as INTERNAL.
func (s *Service) addBrokerPartitions(ctx context.Context, project, location, cluster, topic string, totalPartitions int) error {
	if s.broker == nil {
		return nil
	}
	s.brokerReady(ctx, project, location, cluster)
	if err := s.broker.AddTopicPartitions(ctx, project, location, cluster, topic, totalPartitions); err != nil {
		return brokerInternalError("add topic partitions", err)
	}
	return nil
}

// deleteBrokerTopic removes the topic from the cluster's broker, surfacing a
// data-plane failure as INTERNAL.
func (s *Service) deleteBrokerTopic(ctx context.Context, project, location, cluster, topic string) error {
	if s.broker == nil {
		return nil
	}
	s.brokerReady(ctx, project, location, cluster)
	if err := s.broker.DeleteBrokerTopic(ctx, project, location, cluster, topic); err != nil {
		return brokerInternalError("delete topic", err)
	}
	return nil
}

// replaceBrokerAcl mirrors an ACL's entry set onto the cluster's live broker,
// replacing every binding for the ACL's resource pattern (an empty set removes
// them). It is a no-op when the service has no broker manager or the cluster has
// no live broker; a data-plane failure is surfaced as INTERNAL so the caller can
// roll the metadata write back.
func (s *Service) replaceBrokerAcl(ctx context.Context, project, location, cluster, resourceType, resourceName, patternType string, entries []AclBinding) error {
	if s.broker == nil {
		return nil
	}
	s.brokerReady(ctx, project, location, cluster)
	if err := s.broker.ReplaceAcl(ctx, project, location, cluster, resourceType, resourceName, patternType, entries); err != nil {
		return brokerInternalError("replace acl", err)
	}
	return nil
}

// ClusterInput carries the caller-supplied fields of a cluster create/update.
// Config is the caller's resource body stored verbatim (capacityConfig,
// gcpConfig, ... ); Labels is the extracted labels map.
type ClusterInput struct {
	Labels map[string]string
	Config json.RawMessage
}

// TopicInput carries the caller-supplied fields of a topic create/update.
type TopicInput struct {
	PartitionCount    int
	ReplicationFactor int
	Config            json.RawMessage
}

// parseTopicConfigs extracts the "configs" map (Kafka topic property overrides)
// from a caller-supplied wire body. A missing body or absent configs field is
// nil, nil; a configs value that is not a string map (e.g. a number) is
// rejected as InvalidArgument, matching real GCP.
func parseTopicConfigs(raw json.RawMessage) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var body struct {
		Configs map[string]string `json:"configs"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, invalidArgument("configs must be a map of string to string")
	}
	if len(body.Configs) == 0 {
		return nil, nil
	}
	return body.Configs, nil
}

// topicConfigs is the lenient form of parseTopicConfigs for stored/rendered
// values: a body written by the emulator always parses, and a malformed one
// (e.g. an old record) is treated as no overrides rather than failing a read.
func topicConfigs(raw json.RawMessage) map[string]string {
	c, _ := parseTopicConfigs(raw)
	return c
}

// topicConfigDiff returns the keys to set and the keys to remove when moving
// from old to next. A key present in next with a different value (or newly
// added) is set; a key present only in old is removed.
func topicConfigDiff(old, next map[string]string) (set map[string]string, remove []string) {
	for k, v := range next {
		if ov, ok := old[k]; !ok || ov != v {
			if set == nil {
				set = map[string]string{}
			}
			set[k] = v
		}
	}
	for k := range old {
		if _, ok := next[k]; !ok {
			remove = append(remove, k)
		}
	}
	return set, remove
}

// randomHex returns n random hexadecimal characters.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		b = make([]byte, (n+1)/2)
	}
	return hex.EncodeToString(b)[:n]
}

// pageParams builds the shared cursor-pagination parameter map.
func pageParams(pageSize int, pageToken string) map[string]any {
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	return params
}

// --- Clusters ---

// CreateCluster creates a cluster and returns it with the done operation that
// carries it as the response.
func (s *Service) CreateCluster(ctx context.Context, project, location, clusterID string, in ClusterInput) (mkstore.Cluster, mkstore.Operation, error) {
	if location == "" || clusterID == "" {
		return mkstore.Cluster{}, mkstore.Operation{}, invalidArgument("missing location or clusterId")
	}
	now := clock.Now().UTC()
	c := mkstore.Cluster{
		Location:   location,
		Name:       clusterID,
		Labels:     in.Labels,
		Config:     in.Config,
		CreateTime: now,
		UpdateTime: now,
	}
	if err := s.store.CreateCluster(ctx, project, location, c); err != nil {
		return mkstore.Cluster{}, mkstore.Operation{}, mapStoreError(err)
	}
	c.BootstrapAddress = s.ensureBroker(ctx, project, location, clusterID)
	target := ClusterName(project, c.Location, c.Name)
	op, err := s.storeOperation(ctx, project, location, "create", target, ClusterJSON(c, project))
	if err != nil {
		return mkstore.Cluster{}, mkstore.Operation{}, mapStoreError(err)
	}
	return c, op, nil
}

// GetCluster returns one cluster. The live broker endpoint is resolved with
// EnsureCluster so a cluster persisted under --dsn (or restored from an import)
// rehydrates its broker on first read; the synthesized fallback is rendered
// when no broker manager is configured or the broker cannot start.
func (s *Service) GetCluster(ctx context.Context, project, location, clusterID string) (mkstore.Cluster, error) {
	if location == "" || clusterID == "" {
		return mkstore.Cluster{}, invalidArgument("missing location or clusterId")
	}
	c, err := s.store.GetCluster(ctx, project, location, clusterID)
	if err != nil {
		return mkstore.Cluster{}, mapStoreError(err)
	}
	c.BootstrapAddress = s.ensureBroker(ctx, project, c.Location, c.Name)
	return c, nil
}

// ListClusters returns a cursor page of the clusters in a location.
func (s *Service) ListClusters(ctx context.Context, project, location string, pageSize int, pageToken string) ([]mkstore.Cluster, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	clusters, err := s.store.ListClusters(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(clusters, func(c mkstore.Cluster) string { return c.Name }, pageParams(pageSize, pageToken))
	for i := range page {
		page[i].BootstrapAddress = s.brokerEndpoint(project, page[i].Location, page[i].Name)
	}
	return page, next, nil
}

// UpdateCluster merges the caller's config/labels into the stored cluster and
// returns it with the done update operation.
func (s *Service) UpdateCluster(ctx context.Context, project, location, clusterID string, in ClusterInput) (mkstore.Cluster, mkstore.Operation, error) {
	if location == "" || clusterID == "" {
		return mkstore.Cluster{}, mkstore.Operation{}, invalidArgument("missing location or clusterId")
	}
	c, err := s.store.UpdateClusterAtomic(ctx, project, location, clusterID, func(c mkstore.Cluster) (mkstore.Cluster, error) {
		stored := map[string]any{}
		if len(c.Config) > 0 {
			_ = json.Unmarshal(c.Config, &stored)
		}
		if len(in.Config) > 0 {
			incoming := map[string]any{}
			if json.Unmarshal(in.Config, &incoming) == nil {
				for k, v := range incoming {
					stored[k] = v
				}
			}
		}
		if in.Labels != nil {
			c.Labels = in.Labels
		}
		if data, err := json.Marshal(stored); err == nil {
			c.Config = data
		}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return mkstore.Cluster{}, mkstore.Operation{}, mapStoreError(err)
	}
	c.BootstrapAddress = s.brokerEndpoint(project, c.Location, c.Name)
	target := ClusterName(project, c.Location, c.Name)
	op, err := s.storeOperation(ctx, project, location, "update", target, ClusterJSON(c, project))
	if err != nil {
		return mkstore.Cluster{}, mkstore.Operation{}, mapStoreError(err)
	}
	return c, op, nil
}

// DeleteCluster deletes a cluster (and its topics/ACLs) and returns the done
// delete operation.
func (s *Service) DeleteCluster(ctx context.Context, project, location, clusterID string) (mkstore.Operation, error) {
	if location == "" || clusterID == "" {
		return mkstore.Operation{}, invalidArgument("missing location or clusterId")
	}
	if err := s.store.DeleteCluster(ctx, project, location, clusterID); err != nil {
		return mkstore.Operation{}, mapStoreError(err)
	}
	s.stopBroker(ctx, project, location, clusterID)
	target := ClusterName(project, location, clusterID)
	op, err := s.storeOperation(ctx, project, location, "delete", target, map[string]any{})
	if err != nil {
		return mkstore.Operation{}, mapStoreError(err)
	}
	return op, nil
}

// --- Operations ---

// GetOperation returns a persisted cluster-mutation operation.
func (s *Service) GetOperation(ctx context.Context, project, location, opID string) (mkstore.Operation, error) {
	if location == "" || opID == "" {
		return mkstore.Operation{}, invalidArgument("missing location or operationId")
	}
	op, err := s.store.GetOperation(ctx, project, location, opID)
	if err != nil {
		return mkstore.Operation{}, mapStoreError(err)
	}
	return s.settle(op), nil
}

// ListOperations returns a cursor page of the persisted operations for a
// location.
func (s *Service) ListOperations(ctx context.Context, project, location string, pageSize int, pageToken string) ([]mkstore.Operation, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	ops, err := s.store.ListOperations(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(ops, func(op mkstore.Operation) string { return op.ID }, pageParams(pageSize, pageToken))
	for i := range page {
		page[i] = s.settle(page[i])
	}
	return page, next, nil
}

// --- Topics ---

// CreateTopic creates a topic under an existing cluster.
func (s *Service) CreateTopic(ctx context.Context, project, location, clusterID, topicID string, in TopicInput) (mkstore.Topic, error) {
	if location == "" || clusterID == "" || topicID == "" {
		return mkstore.Topic{}, invalidArgument("missing location, clusterId, or topicId")
	}
	// partition_count and replication_factor are Required in the proto.
	if in.PartitionCount < 1 {
		return mkstore.Topic{}, invalidArgument("partitionCount must be at least 1")
	}
	if in.ReplicationFactor < 1 {
		return mkstore.Topic{}, invalidArgument("replicationFactor must be at least 1")
	}
	configs, err := parseTopicConfigs(in.Config)
	if err != nil {
		return mkstore.Topic{}, err
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return mkstore.Topic{}, mapStoreError(err)
	}
	now := clock.Now().UTC()
	t := mkstore.Topic{
		Location:          location,
		ClusterName:       clusterID,
		Name:              topicID,
		PartitionCount:    in.PartitionCount,
		ReplicationFactor: in.ReplicationFactor,
		Config:            in.Config,
		CreateTime:        now,
		UpdateTime:        now,
	}
	if err := s.store.CreateTopic(ctx, project, location, clusterID, t); err != nil {
		return mkstore.Topic{}, mapStoreError(err)
	}
	// Provision the broker topic only after the metadata write. If the broker
	// rejects it, roll the store write back so an API-visible topic is
	// guaranteed to exist on the broker.
	if err := s.ensureBrokerTopic(ctx, project, location, clusterID, topicID, in.PartitionCount, configs); err != nil {
		s.rollbackCreatedTopic(ctx, project, location, clusterID, topicID, t)
		return mkstore.Topic{}, err
	}
	return t, nil
}

// rollbackCreatedTopic undoes a topic create whose broker provision failed. It
// deletes the metadata only when the stored record still matches the one this
// call wrote (see sameTopicVersion), so a concurrent successful mutation is
// never clobbered. The comparison is semantic because a --dsn store truncates
// timestamps to microseconds and normalizes JSON.
func (s *Service) rollbackCreatedTopic(ctx context.Context, project, location, clusterID, topicID string, created mkstore.Topic) {
	cur, err := s.store.GetTopic(ctx, project, location, clusterID, topicID)
	if err != nil {
		return // already gone
	}
	if !sameTopicVersion(cur, created) {
		slog.Warn("managedkafka: skipping topic create rollback; record was modified concurrently", "topic", topicID)
		return
	}
	if rbErr := s.store.DeleteTopic(ctx, project, location, clusterID, topicID); rbErr != nil {
		slog.Warn("managedkafka: topic create rollback failed", "topic", topicID, "err", rbErr)
	}
}

// GetTopic returns one topic.
func (s *Service) GetTopic(ctx context.Context, project, location, clusterID, topicID string) (mkstore.Topic, error) {
	if location == "" || clusterID == "" || topicID == "" {
		return mkstore.Topic{}, invalidArgument("missing location, clusterId, or topicId")
	}
	t, err := s.store.GetTopic(ctx, project, location, clusterID, topicID)
	if err != nil {
		return mkstore.Topic{}, mapStoreError(err)
	}
	return t, nil
}

// ListTopics returns a cursor page of the topics in a cluster.
func (s *Service) ListTopics(ctx context.Context, project, location, clusterID string, pageSize int, pageToken string) ([]mkstore.Topic, string, error) {
	if location == "" || clusterID == "" {
		return nil, "", invalidArgument("missing location or clusterId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return nil, "", mapStoreError(err)
	}
	topics, err := s.store.ListTopics(ctx, project, location, clusterID)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(topics, func(t mkstore.Topic) string { return t.Name }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateTopic merges the caller's fields into the stored topic. PartitionCount
// is increase-only, and a broker partition or config change must succeed for
// the metadata change to stick (the pre-image record is restored on a broker
// failure).
func (s *Service) UpdateTopic(ctx context.Context, project, location, clusterID, topicID string, in TopicInput) (mkstore.Topic, error) {
	if location == "" || clusterID == "" || topicID == "" {
		return mkstore.Topic{}, invalidArgument("missing location, clusterId, or topicId")
	}
	// Reject a malformed configs map before the metadata write, so a non-string
	// value can never be silently dropped (or, on update, read as "clear all").
	if _, err := parseTopicConfigs(in.Config); err != nil {
		return mkstore.Topic{}, err
	}
	// Captured inside the atomic mutate so the broker calls (and their
	// rollback) can see the pre-image record and config set.
	var prevTopic mkstore.Topic
	var grew bool
	t, err := s.store.UpdateTopicAtomic(ctx, project, location, clusterID, topicID, func(t mkstore.Topic) (mkstore.Topic, error) {
		prevTopic = t
		if in.PartitionCount != 0 {
			// Kafka (and real GCP) allow a topic to grow but never shrink.
			if in.PartitionCount < t.PartitionCount {
				return mkstore.Topic{}, invalidArgument("partitionCount cannot be decreased")
			}
			grew = in.PartitionCount > t.PartitionCount
			t.PartitionCount = in.PartitionCount
		}
		if in.ReplicationFactor != 0 {
			// replication_factor is Immutable in the proto: a change is
			// rejected rather than silently persisted.
			if in.ReplicationFactor != t.ReplicationFactor {
				return mkstore.Topic{}, invalidArgument("replicationFactor is immutable")
			}
			t.ReplicationFactor = in.ReplicationFactor
		}
		if len(in.Config) > 0 {
			stored := map[string]any{}
			if len(t.Config) > 0 {
				_ = json.Unmarshal(t.Config, &stored)
			}
			incoming := map[string]any{}
			if json.Unmarshal(in.Config, &incoming) == nil {
				for k, v := range incoming {
					stored[k] = v
				}
			}
			if data, err := json.Marshal(stored); err == nil {
				t.Config = data
			}
		}
		t.UpdateTime = clock.Now().UTC()
		return t, nil
	})
	if err != nil {
		return mkstore.Topic{}, mapStoreError(err)
	}
	// Mirror the metadata change onto the live broker. The merge above replaces
	// the configs object wholesale, so a key absent from the new set is cleared
	// on the broker too. Any broker failure restores the pre-image metadata,
	// unless a concurrent request has since changed the record. Configs are
	// applied before partition growth: a partition count can never be lowered,
	// so a failure there is the one that cannot be repaired on the broker.
	set, remove := topicConfigDiff(topicConfigs(prevTopic.Config), topicConfigs(t.Config))
	if len(set) > 0 || len(remove) > 0 {
		if err := s.alterBrokerTopicConfigs(ctx, project, location, clusterID, topicID, set, remove); err != nil {
			s.rollbackTopicUpdate(ctx, project, location, clusterID, topicID, t, prevTopic)
			return mkstore.Topic{}, err
		}
	}
	if grew {
		if err := s.addBrokerPartitions(ctx, project, location, clusterID, topicID, t.PartitionCount); err != nil {
			s.rollbackTopicUpdate(ctx, project, location, clusterID, topicID, t, prevTopic)
			// The config alter already succeeded; undo it on the broker too so
			// an override the API no longer reports is not left behind. Growth
			// is applied last because a partition count can never be lowered.
			if len(set) > 0 || len(remove) > 0 {
				back, backRemove := topicConfigDiff(topicConfigs(t.Config), topicConfigs(prevTopic.Config))
				if cErr := s.alterBrokerTopicConfigs(ctx, project, location, clusterID, topicID, back, backRemove); cErr != nil {
					slog.Warn("managedkafka: topic config compensation failed after partition error", "topic", topicID, "err", cErr)
				}
			}
			return mkstore.Topic{}, err
		}
	}
	return t, nil
}

// rollbackTopicUpdate restores prev over the metadata record a failed broker
// call left behind (post). It only applies while the record still matches post,
// so a concurrent successful update is never clobbered.
func (s *Service) rollbackTopicUpdate(ctx context.Context, project, location, clusterID, topicID string, post, prev mkstore.Topic) {
	if _, rbErr := s.store.UpdateTopicAtomic(ctx, project, location, clusterID, topicID, func(cur mkstore.Topic) (mkstore.Topic, error) {
		if sameTopicVersion(cur, post) {
			return prev, nil
		}
		return cur, nil
	}); rbErr != nil {
		slog.Warn("managedkafka: topic update rollback failed", "topic", topicID, "err", rbErr)
	}
}

// sameTopicVersion reports whether cur is the record post wrote, comparing the
// semantic mutable fields the rollback would restore rather than raw bytes or
// timestamps: a --dsn store round-trips UpdateTime at microsecond precision and
// normalizes the Config JSON (sorted keys, spacing), so a byte/timestamp compare
// never matches there and the rollback would silently no-op. The parsed config
// map is precision- and encoding-independent.
func sameTopicVersion(cur, post mkstore.Topic) bool {
	return cur.PartitionCount == post.PartitionCount &&
		cur.ReplicationFactor == post.ReplicationFactor &&
		maps.Equal(topicConfigs(cur.Config), topicConfigs(post.Config))
}

// DeleteTopic removes the topic from the broker first, then the store, so a
// broker failure leaves the metadata (and the API-visible topic) intact.
func (s *Service) DeleteTopic(ctx context.Context, project, location, clusterID, topicID string) error {
	if location == "" || clusterID == "" || topicID == "" {
		return invalidArgument("missing location, clusterId, or topicId")
	}
	if _, err := s.store.GetTopic(ctx, project, location, clusterID, topicID); err != nil {
		return mapStoreError(err)
	}
	if err := s.deleteBrokerTopic(ctx, project, location, clusterID, topicID); err != nil {
		return err
	}
	if err := s.store.DeleteTopic(ctx, project, location, clusterID, topicID); err != nil {
		return mapStoreError(err)
	}
	return nil
}

// --- Consumer groups ---
//
// Consumer-group business logic lives in consumer_group.go: unlike topics, a
// group has no API-side create, so it is read from (and reset on) the cluster's
// live broker rather than from the metadata store.

// IsNotFound reports whether err is the canonical NotFound provider error (as
// returned by GetOperation for an absent operation), so a transport's
// ResolveOperation can decline an unknown name instead of surfacing an error.
func IsNotFound(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.Code == "NotFound"
}

// --- errors ---

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// mapStoreError maps a managedkafka store error onto a canonical provider error.
func mapStoreError(err error) error {
	switch {
	case errors.Is(err, mkstore.ErrNoSuchCluster):
		return model.NewProviderError("NotFound", "cluster not found", 404)
	case errors.Is(err, mkstore.ErrNoSuchTopic):
		return model.NewProviderError("NotFound", "topic not found", 404)
	case errors.Is(err, mkstore.ErrNoSuchAcl):
		return model.NewProviderError("NotFound", "acl not found", 404)
	case errors.Is(err, mkstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, mkstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	default:
		return err
	}
}
