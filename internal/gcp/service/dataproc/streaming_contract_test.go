package dataproc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/sparkhelpers"
)

// --- Job.scheduling: parse, validate, render ---

func TestJobInputFromMap_Scheduling(t *testing.T) {
	in := JobInputFromMap(map[string]any{
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
		"scheduling": map[string]any{"maxFailuresPerHour": float64(3), "maxFailuresTotal": float64(9)},
	})
	require.NotNil(t, in.Scheduling)
	require.Equal(t, int32(3), in.Scheduling.MaxFailuresPerHour)
	require.Equal(t, int32(9), in.Scheduling.MaxFailuresTotal)

	// Omitted scheduling stays nil (the API default: no restarts).
	in = JobInputFromMap(map[string]any{
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	})
	require.Nil(t, in.Scheduling)
}

func TestSubmitJob_RejectsOutOfRangeScheduling(t *testing.T) {
	ctx := context.Background()
	p := submitTestService(t)

	cases := []*dpstore.JobScheduling{
		{MaxFailuresPerHour: 11},
		{MaxFailuresTotal: 241},
		{MaxFailuresPerHour: -1},
	}
	for _, sched := range cases {
		_, err := p.SubmitJob(ctx, "proj", "us-central1", JobInput{
			JobID:                fmt.Sprintf("bad-%d-%d", sched.MaxFailuresPerHour, sched.MaxFailuresTotal),
			PlacementClusterName: "c1",
			Type:                 "pysparkJob",
			TypeJob:              []byte(`{"mainPythonFileUri":"gs://b/main.py"}`),
			Scheduling:           sched,
		})
		require.Error(t, err, "scheduling %+v must be rejected", sched)
	}

	// 0 (and omitted) is the documented default and is accepted.
	_, err := p.SubmitJob(ctx, "proj", "us-central1", JobInput{
		JobID:                "zero-ok",
		PlacementClusterName: "c1",
		Type:                 "pysparkJob",
		TypeJob:              []byte(`{"mainPythonFileUri":"gs://b/main.py"}`),
		Scheduling:           &dpstore.JobScheduling{},
	})
	require.NoError(t, err)
}

func TestJobInputFromMap_ExplicitZeroSchedulingIsUnset(t *testing.T) {
	for _, sched := range []any{
		map[string]any{},
		map[string]any{"maxFailuresPerHour": float64(0), "maxFailuresTotal": float64(0)},
	} {
		in := JobInputFromMap(map[string]any{
			"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
			"scheduling": sched,
		})
		require.Nil(t, in.Scheduling, "explicit-zero scheduling %v must be unset", sched)
	}
}

// TestSubmitJob_ExplicitZeroSchedulingOmitted pins memory/Postgres parity: an
// explicit but empty policy is stored as unset, so the response omits it (not
// `scheduling: {}` on one backend only).
func TestSubmitJob_ExplicitZeroSchedulingOmitted(t *testing.T) {
	ctx := context.Background()
	p := submitTestService(t)
	j, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "zero-omit"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
		"scheduling": map[string]any{},
	}))
	require.NoError(t, err)
	require.NotContains(t, JobJSON(j), "scheduling")
}

// TestSubmitJob_RejectsNonIntegralOrOverflowScheduling verifies a REST-body
// counter that is not a whole int32 is rejected rather than silently truncated
// or wrapped to the "no restarts" default.
func TestSubmitJob_RejectsNonIntegralOrOverflowScheduling(t *testing.T) {
	ctx := context.Background()
	p := submitTestService(t)
	for i, raw := range []any{float64(4294967296), 10.9} {
		in := JobInputFromMap(map[string]any{
			"placement":  map[string]any{"clusterName": "c1"},
			"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
			"scheduling": map[string]any{"maxFailuresPerHour": raw},
		})
		in.JobID = fmt.Sprintf("bad-sched-%d", i)
		_, err := p.SubmitJob(ctx, "proj", "us-central1", in)
		require.Error(t, err, "raw counter %v must be rejected", raw)
	}
}

func TestJobJSON_RendersScheduling(t *testing.T) {
	j := dpstore.Job{
		ProjectID:  "proj",
		Region:     "us-central1",
		JobID:      "j1",
		Type:       "pysparkJob",
		TypeJob:    []byte(`{"mainPythonFileUri":"gs://b/main.py"}`),
		Scheduling: &dpstore.JobScheduling{MaxFailuresPerHour: 2, MaxFailuresTotal: 5},
		Status:     dpstore.JobStatus{State: jobStateRunning, StateStartTime: clock.Now().UTC()},
	}
	data := JobJSON(j)
	sched, ok := data["scheduling"].(map[string]any)
	require.True(t, ok, "scheduling must be rendered")
	require.Equal(t, int32(2), sched["maxFailuresPerHour"])
	require.Equal(t, int32(5), sched["maxFailuresTotal"])

	// An unset policy is omitted entirely.
	j.Scheduling = nil
	require.NotContains(t, JobJSON(j), "scheduling")
}

// --- jarFileUris → spark-submit --jars ---

func TestJobToEntryPoint_JarFileUris(t *testing.T) {
	ep, _, err := jobToEntryPoint("sparkJob", map[string]any{
		"mainJarFileUri": "gs://b/a.jar",
		"jarFileUris":    []any{"gs://b/connector1.jar", "gs://b/connector2.jar"},
	})
	require.NoError(t, err)
	jar, ok := ep.(sparkhelpers.JarEntryPoint)
	require.True(t, ok)
	require.Equal(t, []string{"gs://b/connector1.jar", "gs://b/connector2.jar"}, jar.JarFileURIs)

	ep, _, err = jobToEntryPoint("pysparkJob", map[string]any{
		"mainPythonFileUri": "gs://b/main.py",
		"jarFileUris":       []any{"gs://b/connector.jar"},
	})
	require.NoError(t, err)
	py, ok := ep.(sparkhelpers.PythonEntryPoint)
	require.True(t, ok)
	require.Equal(t, []string{"gs://b/connector.jar"}, py.JarFileURIs)
}

// --- Long-running / streaming lifecycle ---

func TestIsLongRunningJob(t *testing.T) {
	require.True(t, isLongRunningJob(map[string]any{
		"properties": map[string]any{"spark.sql.streaming.checkpointLocation": "gs://b/ckpt"},
	}))
	require.True(t, isLongRunningJob(map[string]any{
		"properties": map[string]any{"spark.streaming.kafka.maxRatePerPartition": "10"},
	}))
	require.False(t, isLongRunningJob(map[string]any{
		"properties": map[string]any{"spark.executor.memory": "2g"},
	}))
	require.False(t, isLongRunningJob(map[string]any{}))
}

// TestLongRunningJob_MockStaysRunningUntilCancelled verifies a job whose
// properties mark it streaming is not auto-settled to DONE by the mock state
// machine; it stays RUNNING until CancelJob.
func TestLongRunningJob_MockStaysRunningUntilCancelled(t *testing.T) {
	freezeClock(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	p := newJobStateProvider(t, 0)
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	j, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "stream-1"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py", "properties": map[string]any{"spark.sql.streaming.checkpointLocation": "gs://b/ckpt"}},
	}))
	require.NoError(t, err)
	require.True(t, j.LongRunning, "streaming properties must mark the job long-running")

	// PENDING -> SETUP_DONE -> RUNNING, then it stays RUNNING.
	require.Equal(t, jobStateSetupDone, getJob(t, p, "stream-1").Status.State)
	require.Equal(t, jobStateRunning, getJob(t, p, "stream-1").Status.State)
	for i := 0; i < 4; i++ {
		require.Equal(t, jobStateRunning, getJob(t, p, "stream-1").Status.State,
			"a long-running job must not settle to DONE")
	}

	cancelled, err := p.CancelJob(ctx, "proj", "us-central1", "stream-1")
	require.NoError(t, err)
	require.Equal(t, jobStateCancelPending, cancelled.Status.State)
	require.Equal(t, jobStateCancelStarted, getJob(t, p, "stream-1").Status.State)
	require.Equal(t, jobStateCancelled, getJob(t, p, "stream-1").Status.State)
}

// --- Restart policy (unit) ---

func TestRestartPolicyAllowsRestart(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	at := func(offset time.Duration) []time.Time { return []time.Time{now.Add(offset)} }

	// Disabled policy never restarts.
	require.False(t, restartPolicy{}.enabled())
	require.False(t, restartPolicy{}.allowsRestart(at(0), now))

	// Per-hour limit: 1 allows the first failure (restart) but not a second.
	p := restartPolicy{perHour: 1}
	require.True(t, p.allowsRestart(at(0), now))
	require.False(t, p.allowsRestart([]time.Time{now.Add(-time.Minute), now}, now))

	// Total limit: 2 allows two failures, the third exceeds.
	p = restartPolicy{total: 2}
	require.True(t, p.allowsRestart(at(0), now))
	require.True(t, p.allowsRestart([]time.Time{now.Add(-time.Minute), now}, now))
	require.False(t, p.allowsRestart([]time.Time{now.Add(-2 * time.Minute), now.Add(-time.Minute), now}, now))

	// Thrash rule: a 5th non-zero exit inside 10 minutes fails even with a high
	// per-hour/total policy.
	p = restartPolicy{perHour: 10, total: 240}
	thrash := []time.Time{
		now.Add(-4 * time.Minute),
		now.Add(-3 * time.Minute),
		now.Add(-2 * time.Minute),
		now.Add(-time.Minute),
		now,
	}
	require.False(t, p.allowsRestart(thrash, now))
	// Four failures are not thrashing.
	require.True(t, p.allowsRestart(thrash[:4], now))

	// A failure older than the thrash window does not count toward it, but the
	// per-hour counter still sees it.
	p = restartPolicy{perHour: 2, total: 240}
	require.True(t, p.allowsRestart([]time.Time{now.Add(-30 * time.Minute), now}, now))
}

// --- Restart loop (k8s, fake clientset) ---

// driverWatchHub returns a fresh fake watcher per driver-pod watch (keyed by
// job-name) so a multi-attempt restart can be driven deterministically; a
// single shared watcher would be closed by the first WaitTerminal's Stop.
type driverWatchHub struct {
	mu       sync.Mutex
	watchers map[string]*watch.RaceFreeFakeWatcher
}

func newDriverWatchHub(client *fake.Clientset) *driverWatchHub {
	hub := &driverWatchHub{watchers: map[string]*watch.RaceFreeFakeWatcher{}}
	client.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
		wa, ok := action.(k8stesting.WatchAction)
		if !ok {
			return false, nil, nil
		}
		sel := wa.GetWatchRestrictions().Labels.String()
		if !strings.Contains(sel, "job-name") {
			return true, watch.NewRaceFreeFake(), nil
		}
		fw := watch.NewRaceFreeFake()
		name := strings.TrimPrefix(sel, "job-name=")
		hub.mu.Lock()
		hub.watchers[name] = fw
		hub.mu.Unlock()
		return true, fw, nil
	})
	return hub
}

func (h *driverWatchHub) emit(t *testing.T, jobName string, pod *corev1.Pod) {
	t.Helper()
	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		fw := h.watchers[jobName]
		if fw == nil {
			return false
		}
		delete(h.watchers, jobName)
		fw.Add(pod)
		return true
	}, 5*time.Second, 5*time.Millisecond)
}

func terminalPodForExit(jobName string, code int32) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "driver-" + jobName,
			Namespace: "jaiscloud",
			Labels:    map[string]string{"job-name": jobName},
		},
	}
	if code == 0 {
		pod.Status = corev1.PodStatus{Phase: corev1.PodSucceeded}
		return pod
	}
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodFailed,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: "spark-submit",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   code,
					Reason:     "Error",
					StartedAt:  metav1.NewTime(clock.RealNow().Add(-time.Second)),
					FinishedAt: metav1.NewTime(clock.RealNow()),
				},
			},
		}},
	}
	return pod
}

// runRestartJob drives a restartable job through one attempt per exit code:
// each spark-submit Job (jc-spark-cm-<id>, then -r1, -r2, ...) is answered with
// the corresponding terminal pod.
func runRestartJob(t *testing.T, p *Service, client *fake.Clientset, j dpstore.Job, exits []int32) {
	t.Helper()
	hub := newDriverWatchHub(client)
	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runJob(ctx, j.ProjectID, j.Region, j, "")
	}()

	base := "jc-spark-cm-" + strings.ToLower(j.JobID)
	for i, code := range exits {
		name := base
		if i > 0 {
			name = fmt.Sprintf("%s-r%d", base, i)
		}
		hub.emit(t, name, terminalPodForExit(name, code))
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("runJob did not complete")
	}
}

func restartJob(id string, sched *dpstore.JobScheduling) dpstore.Job {
	return dpstore.Job{
		ProjectID:            "proj",
		Region:               "us-central1",
		JobID:                id,
		PlacementClusterName: "c1",
		Type:                 "pysparkJob",
		TypeJob:              []byte(`{"mainPythonFileUri":"gs://b/main.py"}`),
		JobUUID:              "uuid-" + id,
		Scheduling:           sched,
		Status:               dpstore.JobStatus{State: "RUNNING", StateStartTime: clock.Now().UTC()},
		CreateTime:           clock.Now().UTC(),
	}
}

func TestRunJob_Restartable_RestartsThenSucceeds(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	j := restartJob("j-restart-ok", &dpstore.JobScheduling{MaxFailuresPerHour: 3, MaxFailuresTotal: 5})
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))

	runRestartJob(t, p, client, j, []int32{1, 1, 0})

	got, err := p.GetJob(context.Background(), j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.Equal(t, jobStateDone, got.Status.State)
	// Two failed attempts → two ATTEMPT_FAILURE entries, and the third attempt
	// is attempt 3.
	require.Equal(t, 3, jobAttemptNumber(got))
	failures := 0
	for _, h := range got.StatusHistory {
		if h.State == jobStateAttemptFail {
			failures++
		}
	}
	require.Equal(t, 2, failures)

	// Failed attempts are reaped, so only the successful attempt's k8s Job
	// remains.
	jobs, err := client.BatchV1().Jobs("jaiscloud").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, jobs.Items, 1)
}

func TestRunJob_Restartable_TotalLimitExhausted(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	j := restartJob("j-restart-total", &dpstore.JobScheduling{MaxFailuresTotal: 1})
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))

	runRestartJob(t, p, client, j, []int32{1, 1})

	got, err := p.GetJob(context.Background(), j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.Equal(t, jobStateError, got.Status.State)
}

func TestRunJob_Restartable_ThrashFails(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	j := restartJob("j-restart-thrash", &dpstore.JobScheduling{MaxFailuresPerHour: 10, MaxFailuresTotal: 240})
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))

	runRestartJob(t, p, client, j, []int32{1, 1, 1, 1, 1})

	got, err := p.GetJob(context.Background(), j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.Equal(t, jobStateError, got.Status.State)
	// Five attempts were made before the thrash rule stopped the loop; the four
	// restarted attempts were reaped, leaving only the final (failed) Job.
	jobs, err := client.BatchV1().Jobs("jaiscloud").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, jobs.Items, 1)
}

func TestRunJob_NonRestartable_DoesNotRestart(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	j := restartJob("j-no-restart", nil)
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))

	runRestartJob(t, p, client, j, []int32{1})

	got, err := p.GetJob(context.Background(), j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.Equal(t, jobStateError, got.Status.State)
	jobs, err := client.BatchV1().Jobs("jaiscloud").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, jobs.Items, 1)
}
