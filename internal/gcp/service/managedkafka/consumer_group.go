package managedkafka

import (
	"context"
	"errors"
	"sort"
	"strings"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
)

// Consumer-group views, mirroring the Discovery ConsumerGroupView enum. The
// zero/unspecified value defaults to FULL, as real GCP documents.
const (
	ConsumerGroupViewUnspecified = "CONSUMER_GROUP_VIEW_UNSPECIFIED"
	ConsumerGroupViewBasic       = "CONSUMER_GROUP_VIEW_BASIC"
	ConsumerGroupViewFull        = "CONSUMER_GROUP_VIEW_FULL"
)

// ErrConsumerGroupNotEmpty reports that a group cannot be deleted while it has
// active members. The broker returns it in place of Kafka's NON_EMPTY_GROUP;
// the core maps it to FailedPrecondition.
var ErrConsumerGroupNotEmpty = errors.New("consumer group not empty")

// ErrConsumerGroupTopicNotFound reports that a committed offset references an
// unknown topic. The broker returns it in place of Kafka's
// UNKNOWN_TOPIC_OR_PARTITION; the core maps it to NotFound.
var ErrConsumerGroupTopicNotFound = errors.New("consumer group topic not found")

// ConsumerGroupOffset is one committed partition offset returned by the
// cluster's group coordinator. Topic is the bare Kafka topic id (the leaf of
// the topic resource name), as the broker knows it.
type ConsumerGroupOffset struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
}

// ConsumerGroup is the state of a Kafka consumer group. Name is the bare group
// id; each transport derives the fully-qualified resource name. Topics maps a
// full topic resource name to the group's committed partitions for it.
type ConsumerGroup struct {
	Location string
	Cluster  string
	Name     string
	Topics   map[string]*ConsumerGroupTopic
}

// ConsumerGroupTopic is one topic's committed partitions for a group.
type ConsumerGroupTopic struct {
	Partitions map[int32]ConsumerGroupPartition
}

// ConsumerGroupPartition is a single committed partition offset.
type ConsumerGroupPartition struct {
	Offset   int64
	Metadata string
}

// ConsumerGroupInput is the caller-supplied state for UpdateConsumerGroup: the
// committed offsets to set. Topic keys may be full resource names or bare ids;
// they are normalized to the bare id before reaching the broker.
type ConsumerGroupInput struct {
	Offsets []ConsumerGroupOffset
}

// ListConsumerGroups lists the cluster's consumer groups from the live broker's
// group coordinator (or, with no live broker, the mock topology's coordinator
// stand-in seeded by the GROUP ACL flow). view (BASIC/FULL) controls whether
// committed offsets are read; filter keeps only groups that hold committed
// metadata for the given topic (matched by full resource name or bare id). A
// missing cluster is NOT_FOUND.
func (s *Service) ListConsumerGroups(ctx context.Context, project, location, clusterID, view, filter string, pageSize int, pageToken string) ([]ConsumerGroup, string, error) {
	if location == "" || clusterID == "" {
		return nil, "", invalidArgument("missing location or clusterId")
	}
	normalizedView, err := normalizeConsumerGroupView(view)
	if err != nil {
		return nil, "", err
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return nil, "", mapStoreError(err)
	}
	ids, err := s.brokerListGroups(ctx, project, location, clusterID)
	if err != nil {
		return nil, "", err
	}
	sort.Strings(ids)

	viewFull := consumerGroupViewIsFull(normalizedView)
	// A filter only matches on the topics map, so it forces the FULL read even
	// when the caller asked for BASIC.
	read := viewFull || filter != ""
	groups := make([]ConsumerGroup, 0, len(ids))
	for _, id := range ids {
		g := ConsumerGroup{Location: location, Cluster: clusterID, Name: id}
		if read {
			offsets, found, err := s.brokerConsumerGroupOffsets(ctx, project, location, clusterID, id)
			if err != nil {
				return nil, "", err
			}
			if found {
				g.Topics = groupTopics(project, location, clusterID, offsets)
			}
		}
		if filter != "" && !groupHasTopic(g, filter) {
			continue
		}
		if !viewFull {
			// BASIC hides the topics map even when a filter forced the read.
			g.Topics = nil
		}
		groups = append(groups, g)
	}
	page, next, err := paging.Page(groups, func(g ConsumerGroup) string { return g.Name }, pageParams(pageSize, pageToken))
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// GetConsumerGroup returns a group's committed offsets from the live broker (or
// the mock topology's registry). A missing cluster or an unknown group is
// NOT_FOUND.
func (s *Service) GetConsumerGroup(ctx context.Context, project, location, clusterID, group string) (ConsumerGroup, error) {
	if location == "" || clusterID == "" || group == "" {
		return ConsumerGroup{}, invalidArgument("missing location, clusterId, or consumerGroupId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return ConsumerGroup{}, mapStoreError(err)
	}
	offsets, found, err := s.brokerConsumerGroupOffsets(ctx, project, location, clusterID, group)
	if err != nil {
		return ConsumerGroup{}, err
	}
	if !found {
		return ConsumerGroup{}, consumerGroupNotFound()
	}
	return ConsumerGroup{
		Location: location,
		Cluster:  clusterID,
		Name:     group,
		Topics:   groupTopics(project, location, clusterID, offsets),
	}, nil
}

// UpdateConsumerGroup resets the group's committed offsets. Matching real GCP,
// the group must already exist (a consumer creates it by committing) and be
// inactive: an offset reset while members are joined is FailedPrecondition.
// updateMask follows the PATCH field mask; only "topics" is mutable, so a mask
// that excludes it (and is not "*") leaves the committed offsets unchanged. An
// empty mask is treated as "*", consistent with the emulator's other PATCHes.
func (s *Service) UpdateConsumerGroup(ctx context.Context, project, location, clusterID, group string, in ConsumerGroupInput, updateMask []string) (ConsumerGroup, error) {
	if location == "" || clusterID == "" || group == "" {
		return ConsumerGroup{}, invalidArgument("missing location, clusterId, or consumerGroupId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return ConsumerGroup{}, mapStoreError(err)
	}
	members, found, err := s.brokerConsumerGroupMembers(ctx, project, location, clusterID, group)
	if err != nil {
		return ConsumerGroup{}, err
	}
	if !found {
		return ConsumerGroup{}, consumerGroupNotFound()
	}
	if members > 0 {
		return ConsumerGroup{}, model.NewProviderError("FailedPrecondition", "consumer group has active members; offsets can only be reset while it is inactive", 400)
	}

	if !maskAppliesToTopics(updateMask) {
		// The mask does not select the writable field: report the unchanged
		// current offsets.
		cur, found, err := s.brokerConsumerGroupOffsets(ctx, project, location, clusterID, group)
		if err != nil {
			return ConsumerGroup{}, err
		}
		if !found {
			return ConsumerGroup{}, consumerGroupNotFound()
		}
		return ConsumerGroup{Location: location, Cluster: clusterID, Name: group,
			Topics: groupTopics(project, location, clusterID, cur)}, nil
	}

	offsets := make([]ConsumerGroupOffset, 0, len(in.Offsets))
	for _, o := range in.Offsets {
		o.Topic = bareTopicName(o.Topic)
		offsets = append(offsets, o)
	}
	if err := s.brokerCommitConsumerGroupOffsets(ctx, project, location, clusterID, group, offsets); err != nil {
		if errors.Is(err, ErrConsumerGroupTopicNotFound) {
			return ConsumerGroup{}, model.NewProviderError("NotFound", "consumer group offset references an unknown topic", 404)
		}
		return ConsumerGroup{}, err
	}
	return ConsumerGroup{
		Location: location,
		Cluster:  clusterID,
		Name:     group,
		Topics:   groupTopics(project, location, clusterID, offsets),
	}, nil
}

// DeleteConsumerGroup removes the group and its committed offsets from the
// broker (or the mock registry). A missing cluster or an already-absent group
// is NOT_FOUND.
func (s *Service) DeleteConsumerGroup(ctx context.Context, project, location, clusterID, group string) error {
	if location == "" || clusterID == "" || group == "" {
		return invalidArgument("missing location, clusterId, or consumerGroupId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return mapStoreError(err)
	}
	existed, err := s.brokerDeleteConsumerGroup(ctx, project, location, clusterID, group)
	if err != nil {
		if errors.Is(err, ErrConsumerGroupNotEmpty) {
			return model.NewProviderError("FailedPrecondition", "consumer group has active members", 400)
		}
		return err
	}
	if !existed {
		return consumerGroupNotFound()
	}
	return nil
}

// --- broker plumbing (no live broker == mock topology) ---

// hasLiveBroker reports whether a real broker is serving the cluster. The
// broker manager itself is non-nil in the default mock topology (it degrades a
// failed/mock start to an empty endpoint), so the live check is the endpoint,
// not the injection.
func (s *Service) hasLiveBroker(project, location, cluster string) bool {
	return s.broker != nil && s.broker.Endpoint(project, location, cluster) != ""
}

func mockGroupKey(project, location, cluster string) string {
	return project + "/" + location + "/" + cluster
}

// seedMockGroup registers a group in the mock topology's coordinator stand-in.
// It is called by the ACL flow for a LITERAL GROUP ACL, which is the only
// control-plane reference that names a group (there is no create RPC).
func (s *Service) seedMockGroup(project, location, cluster, group string) {
	if group == "" {
		return
	}
	s.mockGroupsMu.Lock()
	defer s.mockGroupsMu.Unlock()
	key := mockGroupKey(project, location, cluster)
	if s.mockGroups[key] == nil {
		s.mockGroups[key] = make(map[string]ConsumerGroup)
	}
	if _, ok := s.mockGroups[key][group]; ok {
		return
	}
	s.mockGroups[key][group] = ConsumerGroup{Location: location, Cluster: cluster, Name: group}
}

// dropMockGroups forgets a deleted cluster's mock consumer groups.
func (s *Service) dropMockGroups(project, location, cluster string) {
	s.mockGroupsMu.Lock()
	defer s.mockGroupsMu.Unlock()
	delete(s.mockGroups, mockGroupKey(project, location, cluster))
}

func (s *Service) mockListGroups(project, location, cluster string) []string {
	s.mockGroupsMu.RLock()
	defer s.mockGroupsMu.RUnlock()
	m := s.mockGroups[mockGroupKey(project, location, cluster)]
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}

func (s *Service) mockGroup(project, location, cluster, group string) (ConsumerGroup, bool) {
	s.mockGroupsMu.RLock()
	defer s.mockGroupsMu.RUnlock()
	g, ok := s.mockGroups[mockGroupKey(project, location, cluster)][group]
	return g, ok
}

func (s *Service) brokerListGroups(ctx context.Context, project, location, cluster string) ([]string, error) {
	if !s.hasLiveBroker(project, location, cluster) {
		return s.mockListGroups(project, location, cluster), nil
	}
	s.brokerReady(ctx, project, location, cluster)
	return s.broker.ListConsumerGroups(ctx, project, location, cluster)
}

func (s *Service) brokerConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string) ([]ConsumerGroupOffset, bool, error) {
	if !s.hasLiveBroker(project, location, cluster) {
		g, ok := s.mockGroup(project, location, cluster, group)
		if !ok {
			return nil, false, nil
		}
		return mockGroupOffsets(g), true, nil
	}
	s.brokerReady(ctx, project, location, cluster)
	return s.broker.ConsumerGroupOffsets(ctx, project, location, cluster, group)
}

func (s *Service) brokerConsumerGroupMembers(ctx context.Context, project, location, cluster, group string) (int, bool, error) {
	if !s.hasLiveBroker(project, location, cluster) {
		_, ok := s.mockGroup(project, location, cluster, group)
		return 0, ok, nil
	}
	s.brokerReady(ctx, project, location, cluster)
	return s.broker.ConsumerGroupMembers(ctx, project, location, cluster, group)
}

func (s *Service) brokerDeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) (bool, error) {
	if !s.hasLiveBroker(project, location, cluster) {
		s.mockGroupsMu.Lock()
		defer s.mockGroupsMu.Unlock()
		key := mockGroupKey(project, location, cluster)
		if _, ok := s.mockGroups[key][group]; !ok {
			return false, nil
		}
		delete(s.mockGroups[key], group)
		return true, nil
	}
	s.brokerReady(ctx, project, location, cluster)
	return s.broker.DeleteConsumerGroup(ctx, project, location, cluster, group)
}

func (s *Service) brokerCommitConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string, offsets []ConsumerGroupOffset) error {
	if !s.hasLiveBroker(project, location, cluster) {
		s.mockGroupsMu.Lock()
		defer s.mockGroupsMu.Unlock()
		key := mockGroupKey(project, location, cluster)
		if _, ok := s.mockGroups[key][group]; !ok {
			return consumerGroupNotFound()
		}
		s.mockGroups[key][group] = ConsumerGroup{
			Location: location,
			Cluster:  cluster,
			Name:     group,
			Topics:   groupTopics(project, location, cluster, offsets),
		}
		return nil
	}
	s.brokerReady(ctx, project, location, cluster)
	return s.broker.CommitConsumerGroupOffsets(ctx, project, location, cluster, group, offsets)
}

// mockGroupOffsets flattens a mock group's topic map back into the offsets the
// read path expects.
func mockGroupOffsets(g ConsumerGroup) []ConsumerGroupOffset {
	var out []ConsumerGroupOffset
	for name, t := range g.Topics {
		for partition, p := range t.Partitions {
			out = append(out, ConsumerGroupOffset{
				Topic:     bareTopicName(name),
				Partition: partition,
				Offset:    p.Offset,
				Metadata:  p.Metadata,
			})
		}
	}
	return out
}

// --- helpers ---

// consumerGroupNotFound is the canonical NOT_FOUND both transports map onto
// their wire status.
func consumerGroupNotFound() error {
	return model.NewProviderError("NotFound", "consumer group not found", 404)
}

// consumerGroupViewIsFull reports whether a view includes committed offsets.
// The unspecified value defaults to FULL, matching the Discovery enum.
func consumerGroupViewIsFull(view string) bool {
	return !strings.EqualFold(view, ConsumerGroupViewBasic)
}

// normalizeConsumerGroupView validates view and returns it uppercased. The
// empty/unspecified value is accepted (it means FULL); any other value is
// rejected as InvalidArgument, as real GCP rejects an unknown enum.
func normalizeConsumerGroupView(view string) (string, error) {
	switch v := strings.ToUpper(view); v {
	case "", ConsumerGroupViewUnspecified, ConsumerGroupViewBasic, ConsumerGroupViewFull:
		return v, nil
	default:
		return "", invalidArgument("invalid consumer group view " + view)
	}
}

// maskAppliesToTopics reports whether a PATCH field mask selects the mutable
// topics field. Only "topics" (or the wildcard "*") is writable, so any other
// mask leaves the committed offsets unchanged. An empty mask is treated as "*",
// consistent with the emulator's other PATCH handlers.
func maskAppliesToTopics(mask []string) bool {
	if len(mask) == 0 {
		return true
	}
	for _, path := range mask {
		switch strings.TrimSpace(path) {
		case "topics", "*":
			return true
		}
	}
	return false
}

// groupTopics expands the broker's bare-topic offsets into the resource-keyed
// map the wire shape uses. An empty offset set yields a nil map so the field is
// omitted.
func groupTopics(project, location, cluster string, offsets []ConsumerGroupOffset) map[string]*ConsumerGroupTopic {
	if len(offsets) == 0 {
		return nil
	}
	out := make(map[string]*ConsumerGroupTopic, len(offsets))
	for _, o := range offsets {
		full := TopicName(project, location, cluster, o.Topic)
		t := out[full]
		if t == nil {
			t = &ConsumerGroupTopic{Partitions: map[int32]ConsumerGroupPartition{}}
			out[full] = t
		}
		off := o.Offset
		if off < 0 {
			// Kafka reports -1 for "no committed offset"; the wire contract is
			// 0 when no offset has been committed.
			off = 0
		}
		t.Partitions[o.Partition] = ConsumerGroupPartition{Offset: off, Metadata: o.Metadata}
	}
	return out
}

// groupHasTopic reports whether the group holds committed metadata for filter,
// which may be a full topic resource name or a bare id.
func groupHasTopic(g ConsumerGroup, filter string) bool {
	for topic := range g.Topics {
		if topic == filter || bareTopicName(topic) == filter {
			return true
		}
	}
	return false
}

// bareTopicName returns the leaf of a topic resource name, or the value
// unchanged when it is already a bare id. The broker works in bare ids while
// the API works in resource names.
func bareTopicName(name string) string {
	if i := strings.LastIndex(name, "/topics/"); i >= 0 {
		return name[i+len("/topics/"):]
	}
	return name
}
