package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	core "jaiscloud/internal/gcp/service/managedkafka"
)

// kafkaAdmin is the subset of Kafka-wire admin operations the broker performs
// on behalf of the Managed Kafka control plane. Isolating it behind an
// interface keeps the (large) Kafka client dependency contained and lets tests
// assert provisioning without a running broker. A future swap of franz-go for
// another client is a change here, not in the core.
type kafkaAdmin interface {
	// EnsureTopic creates the topic if absent, applying the given topic
	// property overrides. It is idempotent: an existing topic with the same
	// name is not an error.
	EnsureTopic(ctx context.Context, topic string, partitions int32, replicationFactor int16, configs map[string]string) error
	// AlterTopicConfigs incrementally writes set and clears remove on the
	// topic. An invalid key/value is reported as core.ErrInvalidTopicConfig.
	AlterTopicConfigs(ctx context.Context, topic string, set map[string]string, remove []string) error
	// AddPartitions raises the topic's partition count to totalPartitions.
	AddPartitions(ctx context.Context, topic string, totalPartitions int32) error
	// DeleteTopic removes the topic. It is idempotent: an absent topic is not
	// an error.
	DeleteTopic(ctx context.Context, topic string) error
	// ListGroups returns the cluster's consumer-group ids, sorted.
	ListGroups(ctx context.Context) ([]string, error)
	// GroupOffsets returns the group's committed offsets. found is false when
	// the coordinator does not know the group.
	GroupOffsets(ctx context.Context, group string) ([]core.ConsumerGroupOffset, bool, error)
	// GroupMembers returns the number of active members and whether the group
	// exists.
	GroupMembers(ctx context.Context, group string) (int, bool, error)
	// DeleteGroup removes the group and its offsets. existed is false when the
	// group was already absent.
	DeleteGroup(ctx context.Context, group string) (bool, error)
	// CommitGroupOffsets sets the group's committed offsets.
	CommitGroupOffsets(ctx context.Context, group string, offsets []core.ConsumerGroupOffset) error
	// ReplaceACLs replaces every binding for spec's resource pattern with
	// spec.Entries; an empty entry set removes them.
	ReplaceACLs(ctx context.Context, spec aclSpec) error
	// Close releases the underlying client.
	Close() error
}

// kafkaAdminFactory builds a kafkaAdmin for one broker endpoint.
type kafkaAdminFactory func(endpoint string) (kafkaAdmin, error)

// adminAPI is the slice of *kadm.Client the franzAdmin uses. It is an interface
// so unit tests can exercise the per-topic/per-group response-error mapping
// (which the top-level error hides) without a running broker.
type adminAPI interface {
	CreateTopic(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topic string) (kadm.CreateTopicResponse, error)
	AlterTopicConfigs(ctx context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error)
	UpdatePartitions(ctx context.Context, set int, topics ...string) (kadm.CreatePartitionsResponses, error)
	DeleteTopic(ctx context.Context, topic string) (kadm.DeleteTopicResponse, error)
	ListGroups(ctx context.Context, filterStates ...string) (kadm.ListedGroups, error)
	DescribeGroups(ctx context.Context, groups ...string) (kadm.DescribedGroups, error)
	FetchOffsets(ctx context.Context, group string) (kadm.OffsetResponses, error)
	DeleteGroups(ctx context.Context, groups ...string) (kadm.DeleteGroupResponses, error)
	CommitOffsets(ctx context.Context, group string, os kadm.Offsets) (kadm.OffsetResponses, error)
	DeleteACLs(ctx context.Context, b *kadm.ACLBuilder) (kadm.DeleteACLsResults, error)
	CreateACLs(ctx context.Context, b *kadm.ACLBuilder) (kadm.CreateACLsResults, error)
	Close()
}

// adminPool caches one admin client per broker endpoint so repeated mutations
// reuse a single Kafka connection instead of dialing per call. It is safe for
// concurrent use.
type adminPool struct {
	newAdmin kafkaAdminFactory
	mu       sync.Mutex
	admins   map[string]kafkaAdmin
}

// newAdminPool returns a pool. A nil factory selects the franz-go client (tests
// inject a fake).
func newAdminPool(factory kafkaAdminFactory) *adminPool {
	if factory == nil {
		factory = newFranzAdmin
	}
	return &adminPool{newAdmin: factory, admins: make(map[string]kafkaAdmin)}
}

// get returns the cached admin for endpoint, creating one on first use.
func (p *adminPool) get(endpoint string) (kafkaAdmin, error) {
	p.mu.Lock()
	if a, ok := p.admins[endpoint]; ok {
		p.mu.Unlock()
		return a, nil
	}
	p.mu.Unlock()

	// Build outside the lock so a factory that dials cannot serialize every
	// caller; re-check for a racing winner before inserting.
	a, err := p.newAdmin(endpoint)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, ok := p.admins[endpoint]; ok {
		_ = a.Close()
		return existing, nil
	}
	p.admins[endpoint] = a
	return a, nil
}

// close drops and closes the admin for endpoint, if any.
func (p *adminPool) close(endpoint string) {
	p.mu.Lock()
	a := p.admins[endpoint]
	delete(p.admins, endpoint)
	p.mu.Unlock()
	if a != nil {
		_ = a.Close()
	}
}

// closeAll drops and closes every pooled admin.
func (p *adminPool) closeAll() {
	p.mu.Lock()
	admins := p.admins
	p.admins = make(map[string]kafkaAdmin)
	p.mu.Unlock()
	for _, a := range admins {
		_ = a.Close()
	}
}

// ensureTopic provisions endpoint's topic through the pool. An empty endpoint
// (no live broker) is a metadata-only no-op.
func ensureTopic(ctx context.Context, pool *adminPool, endpoint, topic string, partitions int, configs map[string]string) error {
	if endpoint == "" {
		return nil
	}
	if partitions < 1 {
		partitions = 1
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return err
	}
	return a.EnsureTopic(ctx, topic, int32(partitions), 1, configs)
}

// alterTopicConfigs applies an incremental config change to endpoint's topic
// through the pool. An empty endpoint (no live broker) is a metadata-only
// no-op.
func alterTopicConfigs(ctx context.Context, pool *adminPool, endpoint, topic string, set map[string]string, remove []string) error {
	if endpoint == "" {
		return nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return err
	}
	return a.AlterTopicConfigs(ctx, topic, set, remove)
}

// addPartitions raises endpoint's topic partition count through the pool. An
// empty endpoint (no live broker) is a metadata-only no-op.
func addPartitions(ctx context.Context, pool *adminPool, endpoint, topic string, totalPartitions int) error {
	if endpoint == "" {
		return nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return err
	}
	return a.AddPartitions(ctx, topic, int32(totalPartitions))
}

// deleteBrokerTopic removes endpoint's topic through the pool. An empty endpoint
// (no live broker) is a metadata-only no-op.
func deleteBrokerTopic(ctx context.Context, pool *adminPool, endpoint, topic string) error {
	if endpoint == "" {
		return nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return err
	}
	return a.DeleteTopic(ctx, topic)
}

// listGroups returns endpoint's consumer groups. An empty endpoint (no live
// broker) yields an empty set.
func listGroups(ctx context.Context, pool *adminPool, endpoint string) ([]string, error) {
	if endpoint == "" {
		return nil, nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return nil, err
	}
	return a.ListGroups(ctx)
}

// groupOffsets returns endpoint's committed offsets for group. An empty endpoint
// (no live broker) reports the group absent.
func groupOffsets(ctx context.Context, pool *adminPool, endpoint, group string) ([]core.ConsumerGroupOffset, bool, error) {
	if endpoint == "" {
		return nil, false, nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return nil, false, err
	}
	return a.GroupOffsets(ctx, group)
}

// groupMembers returns endpoint's active member count for group. An empty
// endpoint (no live broker) reports the group absent.
func groupMembers(ctx context.Context, pool *adminPool, endpoint, group string) (int, bool, error) {
	if endpoint == "" {
		return 0, false, nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return 0, false, err
	}
	return a.GroupMembers(ctx, group)
}

// deleteGroup removes endpoint's group. An empty endpoint (no live broker)
// reports the group absent.
func deleteGroup(ctx context.Context, pool *adminPool, endpoint, group string) (bool, error) {
	if endpoint == "" {
		return false, nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return false, err
	}
	return a.DeleteGroup(ctx, group)
}

// commitGroupOffsets sets endpoint's committed offsets for group.
func commitGroupOffsets(ctx context.Context, pool *adminPool, endpoint, group string, offsets []core.ConsumerGroupOffset) error {
	if endpoint == "" {
		return nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return err
	}
	return a.CommitGroupOffsets(ctx, group, offsets)
}

// franzAdmin is the franz-go/kadm-backed kafkaAdmin.
type franzAdmin struct {
	client adminAPI
}

// newFranzAdmin dials endpoint's Kafka listener and returns an admin client.
func newFranzAdmin(endpoint string) (kafkaAdmin, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(endpoint),
		kgo.ClientID("jaiscloud-managedkafka"),
	)
	if err != nil {
		return nil, fmt.Errorf("managedkafka broker: kafka client: %w", err)
	}
	return &franzAdmin{client: kadm.NewClient(cl)}, nil
}

func (a *franzAdmin) EnsureTopic(ctx context.Context, topic string, partitions int32, replicationFactor int16, configs map[string]string) error {
	if partitions < 1 {
		partitions = 1
	}
	if replicationFactor < 1 {
		replicationFactor = 1
	}
	_, err := a.client.CreateTopic(ctx, partitions, replicationFactor, configValues(configs), topic)
	// Creating a topic that already exists is idempotent: the caller asked for
	// the topic to exist and it does.
	if errors.Is(err, kerr.TopicAlreadyExists) {
		return nil
	}
	return classifyConfigError(err)
}

// AlterTopicConfigs incrementally writes set and clears remove. The
// incremental (not full-state) alter is used so unrelated broker defaults stay
// in place. Keys are sorted so the request is deterministic.
func (a *franzAdmin) AlterTopicConfigs(ctx context.Context, topic string, set map[string]string, remove []string) error {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	alter := make([]kadm.AlterConfig, 0, len(set)+len(remove))
	for _, k := range keys {
		v := set[k]
		alter = append(alter, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: &v})
	}
	del := append([]string(nil), remove...)
	sort.Strings(del)
	for _, k := range del {
		alter = append(alter, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k})
	}
	if len(alter) == 0 {
		return nil
	}
	resps, err := a.client.AlterTopicConfigs(ctx, alter, topic)
	if err != nil {
		return err
	}
	r, err := resps.On(topic, nil)
	if err != nil {
		return err
	}
	return classifyConfigError(r.Err)
}

// configValues converts a plain config map into the pointer map kadm expects.
func configValues(configs map[string]string) map[string]*string {
	if len(configs) == 0 {
		return nil
	}
	out := make(map[string]*string, len(configs))
	for k, v := range configs {
		val := v
		out[k] = &val
	}
	return out
}

// classifyConfigError reports a broker-rejected config as the core's sentinel
// so the API surfaces InvalidArgument (400) rather than Internal (500).
func classifyConfigError(err error) error {
	if errors.Is(err, kerr.InvalidConfig) || errors.Is(err, kerr.InvalidRequest) {
		return fmt.Errorf("%w: %v", core.ErrInvalidTopicConfig, err)
	}
	return err
}

func (a *franzAdmin) AddPartitions(ctx context.Context, topic string, totalPartitions int32) error {
	rs, err := a.client.UpdatePartitions(ctx, int(totalPartitions), topic)
	// err is only a request-level (network) failure; a broker rejection such as
	// InvalidPartitions or UnknownTopicOrPartition is carried per topic in the
	// response, so it must be surfaced explicitly.
	if err != nil {
		return err
	}
	return rs.Error()
}

func (a *franzAdmin) DeleteTopic(ctx context.Context, topic string) error {
	_, err := a.client.DeleteTopic(ctx, topic)
	// Deleting an absent topic is idempotent: the postcondition (no topic)
	// already holds.
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return nil
	}
	return err
}

func (a *franzAdmin) ListGroups(ctx context.Context) ([]string, error) {
	listed, err := a.client.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	return listed.Groups(), nil
}

func (a *franzAdmin) GroupOffsets(ctx context.Context, group string) ([]core.ConsumerGroupOffset, bool, error) {
	described, err := a.client.DescribeGroups(ctx, group)
	if err != nil {
		return nil, false, err
	}
	d, ok := described[group]
	if !ok {
		return nil, false, nil
	}
	if d.Err != nil {
		if errors.Is(d.Err, kerr.GroupIDNotFound) {
			return nil, false, nil
		}
		return nil, false, d.Err
	}
	resps, err := a.client.FetchOffsets(ctx, group)
	if err != nil {
		return nil, false, err
	}
	out := make([]core.ConsumerGroupOffset, 0)
	for topic, parts := range resps {
		for partition, r := range parts {
			if r.Err != nil {
				// A committed offset for a topic that no longer exists is not
				// fatal; skip it rather than failing the whole read.
				if errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
					continue
				}
				return nil, false, r.Err
			}
			out = append(out, core.ConsumerGroupOffset{
				Topic:     topic,
				Partition: partition,
				Offset:    r.At,
				Metadata:  r.Metadata,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out, true, nil
}

func (a *franzAdmin) GroupMembers(ctx context.Context, group string) (int, bool, error) {
	described, err := a.client.DescribeGroups(ctx, group)
	if err != nil {
		return 0, false, err
	}
	d, ok := described[group]
	if !ok {
		return 0, false, nil
	}
	if d.Err != nil {
		if errors.Is(d.Err, kerr.GroupIDNotFound) {
			return 0, false, nil
		}
		return 0, false, d.Err
	}
	return len(d.Members), true, nil
}

func (a *franzAdmin) DeleteGroup(ctx context.Context, group string) (bool, error) {
	resps, err := a.client.DeleteGroups(ctx, group)
	if err != nil {
		return false, err
	}
	r, ok := resps[group]
	if !ok {
		return false, nil
	}
	if r.Err != nil {
		if errors.Is(r.Err, kerr.GroupIDNotFound) {
			return false, nil
		}
		// A group with active members cannot be deleted; surface it as a
		// precondition rather than a raw Kafka error.
		if errors.Is(r.Err, kerr.NonEmptyGroup) {
			return false, core.ErrConsumerGroupNotEmpty
		}
		return false, r.Err
	}
	return true, nil
}

func (a *franzAdmin) CommitGroupOffsets(ctx context.Context, group string, offsets []core.ConsumerGroupOffset) error {
	os := make(kadm.Offsets, len(offsets))
	for _, o := range offsets {
		if os[o.Topic] == nil {
			os[o.Topic] = make(map[int32]kadm.Offset)
		}
		os[o.Topic][o.Partition] = kadm.Offset{
			Topic:     o.Topic,
			Partition: o.Partition,
			At:        o.Offset,
			Metadata:  o.Metadata,
		}
	}
	resps, err := a.client.CommitOffsets(ctx, group, os)
	if err != nil {
		return err
	}
	if err := resps.Error(); err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return core.ErrConsumerGroupTopicNotFound
		}
		return err
	}
	return nil
}

// ReplaceACLs replaces every binding for spec's resource pattern with
// spec.Entries. The broker ACL table has no "replace", so it is a delete of the
// pattern's existing bindings followed by one create per entry (the builder
// multiplies principals by operations, so a per-entry operation/permission
// needs its own request). An empty entry set is a pure delete.
func (a *franzAdmin) ReplaceACLs(ctx context.Context, spec aclSpec) error {
	pattern, err := aclPattern(spec.PatternType)
	if err != nil {
		return err
	}
	del, err := aclDeleteFilter(spec, pattern)
	if err != nil {
		return err
	}
	drs, err := a.client.DeleteACLs(ctx, del)
	if err != nil {
		return err
	}
	if err := firstDeleteACLErr(drs); err != nil {
		return err
	}

	for _, e := range spec.Entries {
		op, err := aclOperation(e.Operation)
		if err != nil {
			return err
		}
		create, err := aclBuilder(spec, pattern)
		if err != nil {
			return err
		}
		create.Operations(op)
		principal := ensureUserPrefix(e.Principal)
		switch {
		case strings.EqualFold(e.PermissionType, "ALLOW"):
			create.Allow(principal).AllowHosts(aclHost(e.Host))
		case strings.EqualFold(e.PermissionType, "DENY"):
			create.Deny(principal).DenyHosts(aclHost(e.Host))
		default:
			return fmt.Errorf("managedkafka broker: unknown acl permission type %q", e.PermissionType)
		}
		crs, err := a.client.CreateACLs(ctx, create)
		if err != nil {
			return err
		}
		if err := firstCreateACLErr(crs); err != nil {
			return err
		}
	}
	return nil
}

func (a *franzAdmin) Close() error {
	a.client.Close()
	return nil
}
