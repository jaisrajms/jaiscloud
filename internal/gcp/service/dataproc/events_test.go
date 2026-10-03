package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/eventing"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

// recordedMessage captures one publish for assertions.
type recordedMessage struct {
	account string
	topic   string
	data    []byte
	attrs   map[string]string
}

// fakeEventPublisher records every published message; err, when set, makes
// every publish fail (to prove a publish failure never fails a transition).
type fakeEventPublisher struct {
	mu   sync.Mutex
	msgs []recordedMessage
	err  error
}

func (f *fakeEventPublisher) PublishEvent(_ context.Context, account, topic string, data []byte, attrs map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", f.err
	}
	cp := make(map[string]string, len(attrs))
	for k, v := range attrs {
		cp[k] = v
	}
	f.msgs = append(f.msgs, recordedMessage{account: account, topic: topic, data: append([]byte(nil), data...), attrs: cp})
	return "msg-1", nil
}

// byEventType returns the recorded messages of one event type.
func (f *fakeEventPublisher) byEventType(eventType string) []recordedMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recordedMessage
	for _, m := range f.msgs {
		if m.attrs["eventType"] == eventType {
			out = append(out, m)
		}
	}
	return out
}

// fakeDispatcher records every dispatched transport-neutral event.
type fakeDispatcher struct {
	mu     sync.Mutex
	events []eventing.Event
}

func (f *fakeDispatcher) DispatchEvent(_ context.Context, ev eventing.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
}

func (f *fakeDispatcher) byEventType(eventType string) []eventing.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []eventing.Event
	for _, e := range f.events {
		if e.EventType == eventType {
			out = append(out, e)
		}
	}
	return out
}

// submitJobOn submits a minimal pyspark job to an existing cluster.
func submitJobOn(t *testing.T, p *Service, cluster, jobID string) dpstore.Job {
	t.Helper()
	j, err := p.SubmitJob(context.Background(), "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": jobID},
		"placement":  map[string]any{"clusterName": cluster},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	}))
	require.NoError(t, err)
	return j
}

func jobEventStates(msgs []recordedMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.attrs["state"])
	}
	return out
}

// TestJobStateEvents_PublishAndDispatch verifies one CloudEvents message and one
// transport-neutral event are emitted per job state transition, with the
// expected previous/current state, attributes and envelope.
func TestJobStateEvents_PublishAndDispatch(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	pub := &fakeEventPublisher{}
	disp := &fakeDispatcher{}
	p := newJobStateProvider(t, 30*time.Second,
		WithEventPublisher(pub), WithEventsTopic("projects/proj/topics/events"))
	p.SetEventDispatcher(disp)

	submitTestJob(t, p, "j1")
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(31 * time.Second)})
	requireState(t, getJob(t, p, "j1"), "SETUP_DONE")
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(62 * time.Second)})
	requireState(t, getJob(t, p, "j1"), "RUNNING")
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(93 * time.Second)})
	requireState(t, getJob(t, p, "j1"), "DONE")

	msgs := pub.byEventType(eventing.TypeDataprocJobStateChange)
	require.Equal(t, []string{"PENDING", "SETUP_DONE", "RUNNING", "DONE"}, jobEventStates(msgs))
	wantPrev := []string{"", "PENDING", "SETUP_DONE", "RUNNING"}
	for i, m := range msgs {
		require.Equal(t, "proj", m.account)
		require.Equal(t, "projects/proj/topics/events", m.topic)
		require.Equal(t, wantPrev[i], m.attrs["previousState"])
		require.Equal(t, "c1", m.attrs["clusterName"])
		require.Equal(t, "j1", m.attrs["jobId"])
		require.Equal(t, "1", m.attrs["attempt"])

		var ce map[string]any
		require.NoError(t, json.Unmarshal(m.data, &ce))
		require.Equal(t, "1.0", ce["specversion"])
		require.Equal(t, eventing.TypeDataprocJobStateChange, ce["type"])
		require.Equal(t, "//dataproc.googleapis.com/projects/proj/regions/us-central1/jobs/j1", ce["source"])
		require.Equal(t, "application/json", ce["datacontenttype"])
		data, ok := ce["data"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "j1", data["jobId"])
		require.Equal(t, "c1", data["clusterName"])
		require.Equal(t, "proj", data["projectId"])
		require.Equal(t, "us-central1", data["region"])
		require.Equal(t, jobEventStates(msgs)[i], data["state"])
	}

	events := disp.byEventType(eventing.TypeDataprocJobStateChange)
	require.Len(t, events, 4)
	for _, e := range events {
		require.Equal(t, eventing.SourceDataproc, e.Source)
		require.Equal(t, "projects/proj/regions/us-central1/jobs/j1", e.Resource)
	}
}

// TestJobStateEvents_NoTopicNoPublish verifies a configured publisher with no
// topic publishes nothing (and never errors), while direct dispatch still runs.
func TestJobStateEvents_NoTopicNoPublish(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	pub := &fakeEventPublisher{}
	disp := &fakeDispatcher{}
	// No WithEventsTopic: the default is disabled. A zero job-state delay makes
	// each read settle one hop.
	p := newJobStateProvider(t, 0, WithEventPublisher(pub))
	p.SetEventDispatcher(disp)

	submitTestJob(t, p, "j1")
	for _, want := range []string{"SETUP_DONE", "RUNNING", "DONE"} {
		requireState(t, getJob(t, p, "j1"), want)
	}

	require.Empty(t, pub.msgs, "no topic configured must publish nothing")
	require.Len(t, disp.byEventType(eventing.TypeDataprocJobStateChange), 4)
}

// TestJobStateEvents_ClusterLabelOverride verifies a cluster's
// jaiscloud-events-topic label routes both its own and its jobs' events to the
// override topic instead of the default.
func TestJobStateEvents_ClusterLabelOverride(t *testing.T) {
	freezeClock(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	pub := &fakeEventPublisher{}
	p := newJobStateProvider(t, 0, WithEventPublisher(pub),
		WithEventsTopic("projects/proj/topics/default"))

	ctx := context.Background()
	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{
		Labels: map[string]string{"jaiscloud-events-topic": "projects/proj/topics/override"},
	})
	require.NoError(t, err)

	submitJobOn(t, p, "c1", "j1")
	// Advance the whole schedule so several job hops publish.
	for _, want := range []string{"SETUP_DONE", "RUNNING", "DONE"} {
		requireState(t, getJob(t, p, "j1"), want)
	}

	jobEvents := pub.byEventType(eventing.TypeDataprocJobStateChange)
	require.Equal(t, []string{"PENDING", "SETUP_DONE", "RUNNING", "DONE"}, jobEventStates(jobEvents))
	for _, m := range jobEvents {
		require.Equal(t, "projects/proj/topics/override", m.topic)
	}
	clusterEvents := pub.byEventType(eventing.TypeDataprocClusterStateChange)
	require.NotEmpty(t, clusterEvents)
	require.Equal(t, "projects/proj/topics/override", clusterEvents[0].topic)
}

// TestJobStateEvents_AttemptNumber verifies the attempt number on the failure
// path: the ATTEMPT_FAILURE and the terminal ERROR that follows it both report
// attempt 1 (the emulator does not retry).
func TestJobStateEvents_AttemptNumber(t *testing.T) {
	freezeClock(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	pub := &fakeEventPublisher{}
	p := newJobStateProvider(t, 0, WithEventPublisher(pub),
		WithEventsTopic("projects/proj/topics/events"),
		WithJobAttemptFailureHook(func(_, _, _ string) bool { return true }))

	submitTestJob(t, p, "j1")
	for _, want := range []string{"SETUP_DONE", "RUNNING", "ATTEMPT_FAILURE", "ERROR"} {
		requireState(t, getJob(t, p, "j1"), want)
	}

	byState := map[string]string{}
	for _, m := range pub.byEventType(eventing.TypeDataprocJobStateChange) {
		byState[m.attrs["state"]] = m.attrs["attempt"]
	}
	require.Equal(t, "1", byState["ATTEMPT_FAILURE"])
	require.Equal(t, "1", byState["ERROR"])
}

// TestJobStateEvents_PublishErrorSwallowed verifies a failing publisher never
// fails the underlying transition.
func TestJobStateEvents_PublishErrorSwallowed(t *testing.T) {
	freezeClock(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	pub := &fakeEventPublisher{err: errors.New("pubsub down")}
	p := newJobStateProvider(t, 0, WithEventPublisher(pub), WithEventsTopic("t"))

	j := submitTestJob(t, p, "j1")
	require.Equal(t, "PENDING", j.Status.State)
	for _, want := range []string{"SETUP_DONE", "RUNNING", "DONE"} {
		requireState(t, getJob(t, p, "j1"), want)
	}
	require.Empty(t, pub.msgs)
}

// TestClusterStateEvents verifies a cluster's create/settle/stop/delete
// lifecycle publishes CREATING -> RUNNING -> STOPPING -> STOPPED ->
// DELETING -> DELETED, with the delete-completion event carrying the UUID.
func TestClusterStateEvents(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	pub := &fakeEventPublisher{}
	p := newStateProvider(t, 30*time.Second, WithEventPublisher(pub),
		WithEventsTopic("projects/proj/topics/events"))
	ctx := context.Background()

	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{})
	require.NoError(t, err)

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(31 * time.Second)})
	_, err = p.GetCluster(ctx, "proj", "us-central1", "c1")
	require.NoError(t, err)

	_, _, err = p.StopCluster(ctx, "proj", "us-central1", "c1")
	require.NoError(t, err)
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(62 * time.Second)})
	c, err := p.GetCluster(ctx, "proj", "us-central1", "c1")
	require.NoError(t, err)
	require.Equal(t, "STOPPED", c.Status.State)

	_, err = p.DeleteCluster(ctx, "proj", "us-central1", "c1")
	require.NoError(t, err)
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(93 * time.Second)})
	_, err = p.GetCluster(ctx, "proj", "us-central1", "c1")
	require.Error(t, err)

	msgs := pub.byEventType(eventing.TypeDataprocClusterStateChange)
	require.Equal(t, []string{"CREATING", "RUNNING", "STOPPING", "STOPPED", "DELETING", "DELETED"}, jobEventStates(msgs))
	require.Equal(t, "//dataproc.googleapis.com/projects/proj/regions/us-central1/clusters/c1", sourceOf(t, msgs[0]))
	require.Equal(t, "", msgs[0].attrs["previousState"])
	require.Equal(t, "DELETING", msgs[len(msgs)-1].attrs["previousState"])

	var data map[string]any
	require.NoError(t, json.Unmarshal(msgs[len(msgs)-1].data, &data))
	inner, _ := data["data"].(map[string]any)
	require.Equal(t, "c1", inner["clusterName"])
	require.NotEmpty(t, inner["clusterUuid"])
}

// errClusterUpdateStore wraps a store and, when fail is set, runs the mutate of
// UpdateClusterAtomic against the current cluster and then fails the write — the
// shape a Postgres commit failure has. It proves a failed transition does not
// publish an event.
type errClusterUpdateStore struct {
	dpstore.Store
	fail bool
}

func (s *errClusterUpdateStore) UpdateClusterAtomic(ctx context.Context, project, region, name string, mutate func(dpstore.Cluster) (dpstore.Cluster, error)) (dpstore.Cluster, error) {
	if !s.fail {
		return s.Store.UpdateClusterAtomic(ctx, project, region, name, mutate)
	}
	cur, err := s.Store.GetCluster(ctx, project, region, name)
	if err != nil {
		return dpstore.Cluster{}, err
	}
	if _, err := mutate(cur); err != nil {
		return dpstore.Cluster{}, err
	}
	return dpstore.Cluster{}, errors.New("simulated cluster write failure")
}

// TestClusterStateEvents_NoEmitOnFailedWrite verifies a transition whose store
// write fails (but whose mutate ran) publishes nothing.
func TestClusterStateEvents_NoEmitOnFailedWrite(t *testing.T) {
	freezeClock(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	pub := &fakeEventPublisher{}
	st := &errClusterUpdateStore{Store: dpstore.NewMemoryStore(), fail: true}
	p := NewService(st, store.NewMemoryResourceStore(),
		WithClusterReadyDelay(0), WithEventPublisher(pub), WithEventsTopic("t"))
	ctx := context.Background()

	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{})
	require.NoError(t, err)

	_, err = p.GetCluster(ctx, "proj", "us-central1", "c1")
	require.Error(t, err)

	msgs := pub.byEventType(eventing.TypeDataprocClusterStateChange)
	require.Len(t, msgs, 1, "only the CREATING event, not the failed RUNNING transition")
	require.Equal(t, "CREATING", msgs[0].attrs["state"])
}

// requireState asserts a job's state.
func requireState(t *testing.T, j dpstore.Job, want string) {
	t.Helper()
	require.Equal(t, want, j.Status.State)
}

// sourceOf extracts the CloudEvents source from a recorded message.
func sourceOf(t *testing.T, m recordedMessage) string {
	t.Helper()
	var ce map[string]any
	require.NoError(t, json.Unmarshal(m.data, &ce))
	s, _ := ce["source"].(string)
	return s
}
