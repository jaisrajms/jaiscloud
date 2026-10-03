package managedkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"testing"
	"time"

	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

// fakeBroker records the lifecycle calls the core makes and returns a fixed
// endpoint, so the core↔broker wiring can be asserted without a real broker.
type fakeBroker struct {
	endpoint string
	err      error
	ensured  []string
	stopped  []string
	reset    bool

	// Topic data-plane behaviour + call recording.
	ensureTopicErr    error
	addPartitionsErr  error
	deleteTopicErr    error
	alterConfigsErr   error
	ensuredTopics     []string // "project/location/cluster/topic:partitions"
	ensureConfigs     []map[string]string
	addedPartitions   []string // "project/location/cluster/topic:total"
	deletedTopics     []string // "project/location/cluster/topic"
	alteredConfigs    []fakeConfigAlter
	beforeAlterConfig func()

	// Consumer-group data-plane behaviour + call recording.
	listGroups       []string
	groupState       map[string]fakeGroup
	listGroupsErr    error
	groupOffsetsErr  error
	groupMembersErr  error
	deleteGroupErr   error
	commitOffsetsErr error
	committed        map[string][]ConsumerGroupOffset // group → offsets passed to commit

	// ACL data-plane behaviour + call recording.
	replaceAclErr error
	aclCalls      []fakeAclCall

	// Optional hooks fire while the broker call is "in flight", so tests can
	// simulate a concurrent mutation landing before the call fails.
	beforeEnsureTopic   func()
	beforeAddPartitions func()
}

// fakeGroup is the broker-side state of one consumer group.
type fakeGroup struct {
	offsets []ConsumerGroupOffset
	members int
}

// fakeConfigAlter records one topic-config alter call.
type fakeConfigAlter struct {
	Topic  string // "project/location/cluster/topic"
	Set    map[string]string
	Remove []string
}

// fakeAclCall records one ACL mirror call.
type fakeAclCall struct {
	ResourceType string
	ResourceName string
	PatternType  string
	Entries      []AclBinding
}

func (f *fakeBroker) key(project, location, cluster string) string {
	return project + "/" + location + "/" + cluster
}

func (f *fakeBroker) EnsureCluster(_ context.Context, project, location, cluster string) (string, error) {
	f.ensured = append(f.ensured, f.key(project, location, cluster))
	if f.err != nil {
		return "", f.err
	}
	return f.endpoint, nil
}

func (f *fakeBroker) Endpoint(_, _, _ string) string { return f.endpoint }

func (f *fakeBroker) StopCluster(_ context.Context, project, location, cluster string) error {
	f.stopped = append(f.stopped, f.key(project, location, cluster))
	return nil
}

func (f *fakeBroker) Reset(_ context.Context) error {
	f.reset = true
	return f.err
}

func (f *fakeBroker) EnsureTopic(_ context.Context, project, location, cluster, topic string, partitions int, configs map[string]string) error {
	f.ensuredTopics = append(f.ensuredTopics, fmt.Sprintf("%s/%s:%d", f.key(project, location, cluster), topic, partitions))
	f.ensureConfigs = append(f.ensureConfigs, configs)
	err := f.ensureTopicErr
	if f.beforeEnsureTopic != nil {
		f.beforeEnsureTopic()
	}
	return err
}

func (f *fakeBroker) AlterTopicConfigs(_ context.Context, project, location, cluster, topic string, set map[string]string, remove []string) error {
	f.alteredConfigs = append(f.alteredConfigs, fakeConfigAlter{
		Topic:  f.key(project, location, cluster) + "/" + topic,
		Set:    set,
		Remove: remove,
	})
	err := f.alterConfigsErr
	if f.beforeAlterConfig != nil {
		f.beforeAlterConfig()
	}
	return err
}

func (f *fakeBroker) AddTopicPartitions(_ context.Context, project, location, cluster, topic string, totalPartitions int) error {
	f.addedPartitions = append(f.addedPartitions, fmt.Sprintf("%s/%s/%s/%s:%d", project, location, cluster, topic, totalPartitions))
	err := f.addPartitionsErr
	if f.beforeAddPartitions != nil {
		f.beforeAddPartitions()
	}
	return err
}

func (f *fakeBroker) DeleteBrokerTopic(_ context.Context, project, location, cluster, topic string) error {
	f.deletedTopics = append(f.deletedTopics, f.key(project, location, cluster)+"/"+topic)
	return f.deleteTopicErr
}

func (f *fakeBroker) ListConsumerGroups(context.Context, string, string, string) ([]string, error) {
	return f.listGroups, f.listGroupsErr
}

func (f *fakeBroker) ConsumerGroupOffsets(_ context.Context, _, _, _, group string) ([]ConsumerGroupOffset, bool, error) {
	if f.groupOffsetsErr != nil {
		return nil, false, f.groupOffsetsErr
	}
	st, ok := f.groupState[group]
	if !ok {
		return nil, false, nil
	}
	return st.offsets, true, nil
}

func (f *fakeBroker) ConsumerGroupMembers(_ context.Context, _, _, _, group string) (int, bool, error) {
	if f.groupMembersErr != nil {
		return 0, false, f.groupMembersErr
	}
	st, ok := f.groupState[group]
	if !ok {
		return 0, false, nil
	}
	return st.members, true, nil
}

func (f *fakeBroker) DeleteConsumerGroup(_ context.Context, _, _, _, group string) (bool, error) {
	if f.deleteGroupErr != nil {
		return false, f.deleteGroupErr
	}
	_, ok := f.groupState[group]
	delete(f.groupState, group)
	return ok, nil
}

func (f *fakeBroker) ReplaceAcl(_ context.Context, _, _, _, resourceType, resourceName, patternType string, entries []AclBinding) error {
	f.aclCalls = append(f.aclCalls, fakeAclCall{
		ResourceType: resourceType,
		ResourceName: resourceName,
		PatternType:  patternType,
		Entries:      entries,
	})
	return f.replaceAclErr
}

func (f *fakeBroker) CommitConsumerGroupOffsets(_ context.Context, _, _, _, group string, offsets []ConsumerGroupOffset) error {
	if f.commitOffsetsErr != nil {
		return f.commitOffsetsErr
	}
	if f.committed == nil {
		f.committed = make(map[string][]ConsumerGroupOffset)
	}
	f.committed[group] = offsets
	if f.groupState == nil {
		f.groupState = make(map[string]fakeGroup)
	}
	st := f.groupState[group]
	st.offsets = offsets
	f.groupState[group] = st
	return nil
}

func TestClusterRendersLiveBrokerEndpoint(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker-1.jaiscloud.svc.cluster.local:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))

	c, op, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if c.BootstrapAddress != fb.endpoint {
		t.Errorf("cluster bootstrapAddress = %q, want %q", c.BootstrapAddress, fb.endpoint)
	}
	if len(fb.ensured) != 1 || fb.ensured[0] != "proj/us-central1/c1" {
		t.Fatalf("EnsureCluster calls = %v", fb.ensured)
	}
	opJSON := OperationJSON(op, "proj")
	if resp, _ := opJSON["response"].(map[string]any); resp["bootstrapAddress"] != fb.endpoint {
		t.Errorf("operation response bootstrapAddress = %v, want %q", resp["bootstrapAddress"], fb.endpoint)
	}

	// Reads resolve the live endpoint too.
	got, err := s.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if js := ClusterJSON(got, "proj"); js["bootstrapAddress"] != fb.endpoint {
		t.Errorf("GetCluster bootstrapAddress = %v, want %q", js["bootstrapAddress"], fb.endpoint)
	}
	if js := ClusterJSON(got, "proj"); js["state"] != "ACTIVE" {
		t.Errorf("state = %v, want ACTIVE", js["state"])
	}

	list, _, err := s.ListClusters(ctx, "proj", "us-central1", 0, "")
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(list) != 1 || list[0].BootstrapAddress != fb.endpoint {
		t.Fatalf("ListClusters = %+v, want live endpoint", list)
	}

	// Delete reaps the broker.
	if _, err := s.DeleteCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if len(fb.stopped) != 1 || fb.stopped[0] != "proj/us-central1/c1" {
		t.Fatalf("StopCluster calls = %v", fb.stopped)
	}
}

func TestClusterFallsBackWhenBrokerFails(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{err: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))

	c, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if c.BootstrapAddress != "" {
		t.Errorf("bootstrapAddress = %q, want empty on broker failure", c.BootstrapAddress)
	}
	if js := ClusterJSON(c, "proj"); js["bootstrapAddress"] != BootstrapAddress("proj", "us-central1", "c1") {
		t.Errorf("rendered bootstrapAddress = %v, want synthesized fallback", js["bootstrapAddress"])
	}
}

func TestWithBrokerNilIsMock(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore(), WithBroker(nil))
	c, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if c.BootstrapAddress != "" {
		t.Errorf("bootstrapAddress = %q, want empty for mock topology", c.BootstrapAddress)
	}
}

// TestResetReapsBroker proves Service.Reset reaps the broker as well as wiping
// the store, so /_jaiscloud/reset does not leave a broker running.
func TestResetReapsBroker(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	s.Reset(ctx)
	if !fb.reset {
		t.Error("Service.Reset did not reap the broker")
	}
	if _, err := s.GetCluster(ctx, "proj", "us-central1", "c1"); err == nil {
		t.Fatal("expected NotFound after Reset")
	}
}

// TestGetClusterRehydratesPersistedCluster proves a cluster persisted under
// --dsn (metadata only, no live broker) gets its broker started on first read,
// so a restarted/imported cluster is usable rather than advertising the dead
// synthesized address.
func TestGetClusterRehydratesPersistedCluster(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{} // no live endpoint until EnsureCluster runs
	store := mkstore.NewMemoryStore()
	s := NewService(store, WithBroker(fb))
	if err := store.CreateCluster(ctx, "proj", "us-central1", mkstore.Cluster{Location: "us-central1", Name: "c1"}); err != nil {
		t.Fatalf("seed persisted cluster: %v", err)
	}
	if len(fb.ensured) != 0 {
		t.Fatalf("broker started before first use: %v", fb.ensured)
	}
	if _, err := s.GetCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if len(fb.ensured) != 1 || fb.ensured[0] != "proj/us-central1/c1" {
		t.Fatalf("GetCluster did not rehydrate the broker: %v", fb.ensured)
	}
}

// TestDataPlaneRehydratesPersistedCluster proves the first data-plane call on a
// persisted cluster starts its broker before provisioning.
func TestDataPlaneRehydratesPersistedCluster(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{} // no live endpoint until EnsureCluster runs
	store := mkstore.NewMemoryStore()
	s := NewService(store, WithBroker(fb))
	if err := store.CreateCluster(ctx, "proj", "us-central1", mkstore.Cluster{Location: "us-central1", Name: "c1"}); err != nil {
		t.Fatalf("seed persisted cluster: %v", err)
	}
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(1, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if len(fb.ensured) != 1 || fb.ensured[0] != "proj/us-central1/c1" {
		t.Fatalf("data-plane call did not rehydrate the broker: %v", fb.ensured)
	}
	if len(fb.ensuredTopics) != 1 {
		t.Fatalf("topic not provisioned after rehydrate: %v", fb.ensuredTopics)
	}
}

// --- Topic data plane ---

func withCluster(t *testing.T, s *Service) {
	t.Helper()
	if _, _, err := s.CreateCluster(context.Background(), "proj", "us-central1", "c1", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
}

func TestTopicProvisionsOnBroker(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker-1.jaiscloud.svc.cluster.local:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 2)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if len(fb.ensuredTopics) != 1 || fb.ensuredTopics[0] != "proj/us-central1/c1/t1:3" {
		t.Fatalf("EnsureTopic calls = %v", fb.ensuredTopics)
	}
	// The broker replica count is fixed at 1 (single-node); the caller's
	// replicationFactor stays metadata.
	if got, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); err != nil || got.ReplicationFactor != 2 {
		t.Fatalf("stored replicationFactor = %d, %v; want 2, nil", got.ReplicationFactor, err)
	}

	// Growth is pushed to the broker.
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(6, 0)); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if len(fb.addedPartitions) != 1 || fb.addedPartitions[0] != "proj/us-central1/c1/t1:6" {
		t.Fatalf("AddTopicPartitions calls = %v", fb.addedPartitions)
	}

	// Delete removes the broker topic before the metadata record.
	if err := s.DeleteTopic(ctx, "proj", "us-central1", "c1", "t1"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if len(fb.deletedTopics) != 1 || fb.deletedTopics[0] != "proj/us-central1/c1/t1" {
		t.Fatalf("DeleteBrokerTopic calls = %v", fb.deletedTopics)
	}
}

func TestTopicCreateRollsBackOnBrokerFailure(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", ensureTopicErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	_, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1))
	assertInternal(t, err)
	// The store write was rolled back: an API-visible topic is guaranteed to
	// exist on the broker, and here the broker call failed.
	if _, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); err == nil {
		t.Fatal("topic survived a failed broker provision")
	}
}

func TestTopicUpdateIsIncreaseOnly(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	_, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(2, 0))
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("decrease error = %v, want InvalidArgument", err)
	}
	if len(fb.addedPartitions) != 0 {
		t.Fatalf("broker contacted on a rejected decrease: %v", fb.addedPartitions)
	}
	if got, _ := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); got.PartitionCount != 3 {
		t.Fatalf("partitionCount = %d, want 3 after rejected decrease", got.PartitionCount)
	}
}

func TestTopicUpdateRollsBackOnBrokerFailure(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", addPartitionsErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	_, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(5, 0))
	assertInternal(t, err)
	if got, _ := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); got.PartitionCount != 3 {
		t.Fatalf("partitionCount = %d, want 3 after rollback", got.PartitionCount)
	}
}

// topicConfigsIn builds the wire "configs" body a topic create/update carries.
func topicConfigsIn(configs map[string]string) json.RawMessage {
	if configs == nil {
		return nil
	}
	b, _ := json.Marshal(map[string]any{"configs": configs})
	return b
}

func TestTopicCreatePassesConfigsToBroker(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	in := topicIn(3, 1)
	in.Config = topicConfigsIn(map[string]string{"cleanup.policy": "compact", "retention.ms": "1000"})
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if len(fb.ensureConfigs) != 1 {
		t.Fatalf("EnsureTopic configs = %v, want one call", fb.ensureConfigs)
	}
	if got := fb.ensureConfigs[0]; got["cleanup.policy"] != "compact" || got["retention.ms"] != "1000" {
		t.Fatalf("EnsureTopic configs = %v, want cleanup.policy=compact retention.ms=1000", got)
	}
}

func TestTopicUpdateAltersChangedConfigs(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	in := topicIn(3, 1)
	in.Config = topicConfigsIn(map[string]string{"cleanup.policy": "delete", "retention.ms": "1000"})
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	upd := topicIn(0, 0)
	upd.Config = topicConfigsIn(map[string]string{"cleanup.policy": "compact", "retention.ms": "1000"})
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", upd); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if len(fb.alteredConfigs) != 1 {
		t.Fatalf("AlterTopicConfigs calls = %+v, want one", fb.alteredConfigs)
	}
	got := fb.alteredConfigs[0]
	if got.Set["cleanup.policy"] != "compact" {
		t.Errorf("set = %v, want cleanup.policy=compact", got.Set)
	}
	if _, ok := got.Set["retention.ms"]; ok {
		t.Errorf("unchanged retention.ms re-sent: %v", got.Set)
	}
	if len(got.Remove) != 0 {
		t.Errorf("remove = %v, want none", got.Remove)
	}
}

func TestTopicUpdateClearsRemovedConfigs(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	in := topicIn(3, 1)
	in.Config = topicConfigsIn(map[string]string{"cleanup.policy": "compact", "retention.ms": "1000"})
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// The store replaces the configs object wholesale, so a dropped key must be
	// cleared on the broker too.
	upd := topicIn(0, 0)
	upd.Config = topicConfigsIn(map[string]string{"retention.ms": "1000"})
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", upd); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if len(fb.alteredConfigs) != 1 {
		t.Fatalf("AlterTopicConfigs calls = %+v, want one", fb.alteredConfigs)
	}
	got := fb.alteredConfigs[0]
	if len(got.Set) != 0 {
		t.Errorf("set = %v, want none", got.Set)
	}
	if len(got.Remove) != 1 || got.Remove[0] != "cleanup.policy" {
		t.Errorf("remove = %v, want [cleanup.policy]", got.Remove)
	}
	tp, _ := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1")
	if strings.Contains(string(tp.Config), "cleanup.policy") {
		t.Errorf("dropped key still stored: %s", tp.Config)
	}
}

func TestTopicUpdateSkipsUnchangedConfigs(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	in := topicIn(3, 1)
	in.Config = topicConfigsIn(map[string]string{"cleanup.policy": "compact"})
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	upd := topicIn(0, 0)
	upd.Config = topicConfigsIn(map[string]string{"cleanup.policy": "compact"})
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", upd); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if len(fb.alteredConfigs) != 0 {
		t.Fatalf("broker contacted for an unchanged config set: %+v", fb.alteredConfigs)
	}
}

func TestTopicInvalidConfigIsInvalidArgument(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", ensureTopicErr: fmt.Errorf("%w: unknown key", ErrInvalidTopicConfig)}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	in := topicIn(3, 1)
	in.Config = topicConfigsIn(map[string]string{"not.a.config": "x"})
	_, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in)
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("error = %v, want InvalidArgument(400)", err)
	}
	if _, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); err == nil {
		t.Fatal("topic survived a broker-rejected config")
	}
}

func TestTopicUpdateConfigRollsBackOnBrokerFailure(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", alterConfigsErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	in := topicIn(3, 1)
	in.Config = topicConfigsIn(map[string]string{"cleanup.policy": "delete"})
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	upd := topicIn(0, 0)
	upd.Config = topicConfigsIn(map[string]string{"cleanup.policy": "compact"})
	assertInternal(t, func() error {
		_, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", upd)
		return err
	}())
	got, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1")
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if c := topicConfigs(got.Config); c["cleanup.policy"] != "delete" {
		t.Fatalf("config = %v, want pre-image cleanup.policy=delete after rollback", c)
	}
}

func TestTopicUpdateConfigInvalidIsInvalidArgument(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", alterConfigsErr: fmt.Errorf("%w: bad value", ErrInvalidTopicConfig)}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	upd := topicIn(0, 0)
	upd.Config = topicConfigsIn(map[string]string{"retention.ms": "abc"})
	_, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", upd)
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("error = %v, want InvalidArgument(400)", err)
	}
	got, _ := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1")
	if topicConfigs(got.Config) != nil {
		t.Fatalf("invalid config persisted: %s", got.Config)
	}
}

func TestTopicConfigDiff(t *testing.T) {
	tests := []struct {
		name       string
		old, next  map[string]string
		wantSet    map[string]string
		wantRemove []string
	}{
		{"nil to added", nil, map[string]string{"a": "1"}, map[string]string{"a": "1"}, nil},
		{"added and changed", map[string]string{"a": "1"}, map[string]string{"a": "2", "b": "3"}, map[string]string{"a": "2", "b": "3"}, nil},
		{"removed", map[string]string{"a": "1", "b": "2"}, map[string]string{"a": "1"}, nil, []string{"b"}},
		{"empty clears all", map[string]string{"a": "1"}, nil, nil, []string{"a"}},
		{"unchanged", map[string]string{"a": "1"}, map[string]string{"a": "1"}, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set, remove := topicConfigDiff(tt.old, tt.next)
			if !maps.Equal(set, tt.wantSet) {
				t.Errorf("set = %v, want %v", set, tt.wantSet)
			}
			sort.Strings(remove)
			wantRemove := append([]string(nil), tt.wantRemove...)
			sort.Strings(wantRemove)
			if strings.Join(remove, ",") != strings.Join(wantRemove, ",") {
				t.Errorf("remove = %v, want %v", remove, tt.wantRemove)
			}
		})
	}
}

func TestParseTopicConfigsRejectsNonStringValue(t *testing.T) {
	_, err := parseTopicConfigs(json.RawMessage(`{"configs":{"retention.ms":1000}}`))
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("error = %v, want InvalidArgument(400)", err)
	}
	// An absent/missing configs field is not an error.
	if c, err := parseTopicConfigs(nil); err != nil || c != nil {
		t.Fatalf("nil body = %v, %v; want nil, nil", c, err)
	}
}

// TestSameTopicVersionIgnoresEncodingAndTimePrecision guards the --dsn
// rollback hole: Postgres round-trips UpdateTime at microsecond precision and
// normalizes JSON, so the guard must compare parsed config maps, not bytes.
func TestSameTopicVersionIgnoresEncodingAndTimePrecision(t *testing.T) {
	post := mkstore.Topic{
		PartitionCount:    3,
		ReplicationFactor: 1,
		UpdateTime:        time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC),
		Config:            json.RawMessage(`{"configs":{"b":"2","a":"1"}}`),
	}
	cur := mkstore.Topic{
		PartitionCount:    3,
		ReplicationFactor: 1,
		// Microsecond-truncated, i.e. what a TIMESTAMPTZ read returns.
		UpdateTime: post.UpdateTime.Truncate(time.Microsecond),
		// JSONB-normalized: keys sorted, spacing added.
		Config: json.RawMessage("{\n  \"configs\": {\n    \"a\": \"1\",\n    \"b\": \"2\"\n  }\n}"),
	}
	if !sameTopicVersion(cur, post) {
		t.Fatal("normalized JSON / truncated timestamp should still match")
	}
	cur.PartitionCount = 4
	if sameTopicVersion(cur, post) {
		t.Fatal("a different partition count must not match")
	}
	cur.PartitionCount = 3
	cur.Config = json.RawMessage(`{"configs":{"b":"9","a":"1"}}`)
	if sameTopicVersion(cur, post) {
		t.Fatal("a different config value must not match")
	}
}

// normalizingStore mimics a --dsn store: timestamps are truncated to
// microseconds and the stored JSON is re-encoded, so a byte/timestamp rollback
// guard would never match and the rollback would silently no-op.
type normalizingStore struct{ mkstore.Store }

func normalizeStoredTopic(t mkstore.Topic) mkstore.Topic {
	t.UpdateTime = t.UpdateTime.Truncate(time.Microsecond)
	if len(t.Config) > 0 {
		var m map[string]any
		if json.Unmarshal(t.Config, &m) == nil {
			if b, err := json.MarshalIndent(m, "", "  "); err == nil {
				t.Config = b
			}
		}
	}
	return t
}

func (s normalizingStore) CreateTopic(ctx context.Context, projectID, location, clusterName string, t mkstore.Topic) error {
	return s.Store.CreateTopic(ctx, projectID, location, clusterName, normalizeStoredTopic(t))
}

func (s normalizingStore) GetTopic(ctx context.Context, projectID, location, clusterName, topicName string) (mkstore.Topic, error) {
	t, err := s.Store.GetTopic(ctx, projectID, location, clusterName, topicName)
	return normalizeStoredTopic(t), err
}

// TestTopicCreateRollbackUnderNormalizingStore proves the create rollback still
// removes the metadata when the store round-trips timestamps/JSON the way a
// --dsn (Postgres) store does.
func TestTopicCreateRollbackUnderNormalizingStore(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", ensureTopicErr: fmt.Errorf("%w: unknown key", ErrInvalidTopicConfig)}
	s := NewService(normalizingStore{Store: mkstore.NewMemoryStore()}, WithBroker(fb))
	withCluster(t, s)

	in := topicIn(3, 1)
	in.Config = topicConfigsIn(map[string]string{"bogus": "x"})
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in); err == nil {
		t.Fatal("expected the broker rejection to surface")
	}
	if _, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); err == nil {
		t.Fatal("metadata survived a failed create under a normalizing store")
	}
}

func TestTopicCreateRejectsNonStringConfig(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	in := topicIn(3, 1)
	in.Config = json.RawMessage(`{"configs":{"retention.ms":1000}}`)
	_, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in)
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("error = %v, want InvalidArgument", err)
	}
	if _, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); err == nil {
		t.Fatal("rejected create persisted metadata")
	}
	if len(fb.ensuredTopics) != 0 {
		t.Fatalf("rejected create reached the broker: %v", fb.ensuredTopics)
	}
}

func TestTopicUpdateRejectsNonStringConfig(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	in := topicIn(3, 1)
	in.Config = topicConfigsIn(map[string]string{"cleanup.policy": "compact"})
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	upd := topicIn(0, 0)
	upd.Config = json.RawMessage(`{"configs":{"retention.ms":1000}}`)
	_, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", upd)
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("error = %v, want InvalidArgument", err)
	}
	if len(fb.alteredConfigs) != 0 {
		t.Fatalf("rejected update reached the broker: %+v", fb.alteredConfigs)
	}
	got, _ := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1")
	if c := topicConfigs(got.Config); c["cleanup.policy"] != "compact" {
		t.Fatalf("stored config = %v, want the pre-image unchanged", c)
	}
}

// TestTopicUpdatePartitionFailureCompensatesConfig guards the combined
// config+growth update: when the partition grow fails after the config alter
// succeeded, the broker override must be reverted too.
func TestTopicUpdatePartitionFailureCompensatesConfig(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", addPartitionsErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	in := topicIn(3, 1)
	in.Config = topicConfigsIn(map[string]string{"cleanup.policy": "delete"})
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	upd := topicIn(6, 0)
	upd.Config = topicConfigsIn(map[string]string{"cleanup.policy": "compact"})
	assertInternal(t, func() error {
		_, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", upd)
		return err
	}())

	if len(fb.alteredConfigs) != 2 {
		t.Fatalf("altered configs = %+v, want the alter plus its compensation", fb.alteredConfigs)
	}
	if fb.alteredConfigs[0].Set["cleanup.policy"] != "compact" {
		t.Errorf("first alter = %+v, want cleanup.policy=compact", fb.alteredConfigs[0])
	}
	if fb.alteredConfigs[1].Set["cleanup.policy"] != "delete" {
		t.Errorf("compensation = %+v, want cleanup.policy=delete", fb.alteredConfigs[1])
	}
	got, _ := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1")
	if got.PartitionCount != 3 {
		t.Errorf("partitionCount = %d, want 3 after rollback", got.PartitionCount)
	}
	if c := topicConfigs(got.Config); c["cleanup.policy"] != "delete" {
		t.Errorf("stored config = %v, want cleanup.policy=delete after rollback", c)
	}
}

func TestTopicDeleteBrokerFailureKeepsMetadata(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", deleteTopicErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	assertInternal(t, s.DeleteTopic(ctx, "proj", "us-central1", "c1", "t1"))
	if _, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); err != nil {
		t.Fatalf("metadata removed despite broker delete failure: %v", err)
	}
}

func TestTopicNoBrokerIsMetadataOnly(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())
	withCluster(t, s)

	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(6, 0)); err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if err := s.DeleteTopic(ctx, "proj", "us-central1", "c1", "t1"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
}

func assertInternal(t *testing.T, err error) {
	t.Helper()
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "Internal" || perr.HTTPStatus != 500 {
		t.Fatalf("error = %v, want Internal(500)", err)
	}
}

func TestCreateTopicRequiresPartitionsAndReplication(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())
	withCluster(t, s)

	for name, in := range map[string]TopicInput{
		"zero partitions":  topicIn(0, 1),
		"zero replication": topicIn(1, 0),
	} {
		_, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", in)
		if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
			t.Errorf("%s: error = %v, want InvalidArgument", name, err)
		}
	}
}

func TestUpdateTopicRejectsReplicationFactorChange(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 2)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// replication_factor is Immutable: a change is rejected, an unchanged value
	// is fine.
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(0, 3)); err == nil {
		t.Fatal("changing replicationFactor was accepted")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("error = %v, want InvalidArgument", err)
	}
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(0, 2)); err != nil {
		t.Fatalf("unchanged replicationFactor rejected: %v", err)
	}
}

// TestUpdateRollbackDoesNotClobberConcurrentGrowth proves the partition-count
// rollback only applies when the metadata still holds the count this request
// wrote; a concurrent successful growth is preserved.
func TestUpdateRollbackDoesNotClobberConcurrentGrowth(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", addPartitionsErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	fb.beforeAddPartitions = func() {
		fb.beforeAddPartitions = nil
		fb.addPartitionsErr = nil // the concurrent request succeeds
		if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(7, 0)); err != nil {
			t.Errorf("concurrent UpdateTopic: %v", err)
		}
	}

	// This request grows 3→5; its broker call fails after the concurrent
	// 3→7 committed.
	if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(5, 0)); err == nil {
		t.Fatal("expected the failed broker growth to surface an error")
	} else {
		assertInternal(t, err)
	}
	if got, _ := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); got.PartitionCount != 7 {
		t.Fatalf("partitionCount = %d, want the concurrent 7 preserved", got.PartitionCount)
	}
}

// TestCreateRollbackSkipsConcurrentlyModifiedTopic proves a failed broker
// provision does not delete metadata a concurrent request already updated.
func TestCreateRollbackSkipsConcurrentlyModifiedTopic(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", ensureTopicErr: errors.New("boom")}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	fb.beforeEnsureTopic = func() {
		fb.beforeEnsureTopic = nil
		fb.ensureTopicErr = nil
		if _, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(5, 0)); err != nil {
			t.Errorf("concurrent UpdateTopic: %v", err)
		}
	}

	_, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 1))
	assertInternal(t, err)
	got, gerr := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1")
	if gerr != nil {
		t.Fatalf("concurrently-modified topic was rolled back: %v", gerr)
	}
	if got.PartitionCount != 5 {
		t.Fatalf("partitionCount = %d, want 5", got.PartitionCount)
	}
}
