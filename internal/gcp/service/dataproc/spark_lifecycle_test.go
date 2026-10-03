package dataproc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"jaiscloud/internal/clock"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
	"jaiscloud/internal/store"
)

// newProvider returns a mock-mode service over a memory store.
func newProvider(t *testing.T) *Service {
	t.Helper()
	return NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore())
}

// newK8sProvider returns a service wired with a fake clientset for real Spark
// execution (k8s mode), plus a memory store for jobs and terminal snapshots.
func newK8sProvider(t *testing.T, client *fake.Clientset) *Service {
	t.Helper()
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil),
		WithSparkImage("spark:test"),
	)
	// NewService() starts a background ownership-patcher goroutine; drain it so
	// the test does not leak goroutines/watchers.
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

// advanceJobToTerminal reads a job until it reaches a terminal state,
// exercising the lazy job state machine end-to-end. With the default zero delay
// each read settles one hop.
func advanceJobToTerminal(t *testing.T, p *Service, project, region, jobID string) dataprocstore.Job {
	t.Helper()
	ctx := context.Background()
	var j dataprocstore.Job
	for i := 0; i < 16; i++ {
		got, err := p.GetJob(ctx, project, region, jobID)
		require.NoError(t, err)
		j = got
		if jobTerminal(j.Status.State) {
			return j
		}
	}
	t.Fatalf("job %s did not reach a terminal state (last %q)", jobID, j.Status.State)
	return j
}

func newTestJob() dataprocstore.Job {
	return dataprocstore.Job{
		ProjectID:            "proj",
		Region:               "us-central1",
		JobID:                "j-lc-1",
		PlacementClusterName: "c1",
		Type:                 "pysparkJob",
		TypeJob:              []byte(`{"mainPythonFileUri":"gs://b/main.py"}`),
		JobUUID:              "uuid-lc-1",
		Status:               dataprocstore.JobStatus{State: "RUNNING", StateStartTime: clock.Now().UTC()},
		CreateTime:           clock.Now().UTC(),
	}
}

func succeededDriverPod(name, jobName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "jaiscloud",
			Labels:    map[string]string{"job-name": jobName},
		},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
}

func oomDriverPod(name, jobName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "jaiscloud",
			Labels:    map[string]string{"job-name": jobName},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "spark-submit",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode:   137,
						Reason:     "OOMKilled",
						StartedAt:  metav1.NewTime(clock.RealNow().Add(-5 * time.Second)),
						FinishedAt: metav1.NewTime(clock.RealNow()),
					},
				},
			}},
		},
	}
}

// prependPodWatch installs a buffered fake pod watcher and returns it so the
// test can drive driver-pod terminal events deterministically. Only the
// spark-submit driver watch (label selector "job-name=...") is intercepted;
// other pod watches (e.g. the ownership patcher's "spark-role=executor") get a
// quiet watcher of their own so they don't share/close the driver watcher.
func prependPodWatch(t *testing.T, client *fake.Clientset) *watch.RaceFreeFakeWatcher {
	t.Helper()
	fw := watch.NewRaceFreeFake()
	client.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
		wa, ok := action.(k8stesting.WatchAction)
		if !ok {
			return false, nil, nil
		}
		if strings.Contains(wa.GetWatchRestrictions().Labels.String(), "job-name") {
			return true, fw, nil
		}
		return true, watch.NewRaceFreeFake(), nil
	})
	return fw
}

// runJobWithDriverPod runs the job to completion, emitting the given terminal
// driver pod once the spark-submit k8s Job has been created.
func runJobWithDriverPod(t *testing.T, p *Service, client *fake.Clientset, j dataprocstore.Job, pod *corev1.Pod) {
	t.Helper()
	fw := prependPodWatch(t, client)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runJob(ctx, j.ProjectID, j.Region, j, "")
	}()

	// Wait until the spark-submit Job exists (SubmitClientMode finished), then
	// emit the terminal pod. The buffered fake watcher tolerates the ordering.
	require.Eventually(t, func() bool {
		jobs, err := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
		return err == nil && len(jobs.Items) > 0
	}, 5*time.Second, 10*time.Millisecond)

	fw.Add(pod)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("runJob did not complete")
	}
}

func TestRunJob_DriverSucceeds_TransitionsToDone(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	j := newTestJob()
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))

	runJobWithDriverPod(t, p, client, j, succeededDriverPod("driver-ok", "jc-spark-cm-j-lc-1"))

	// Assert through the wire rendering, not the store field, so a regression in
	// JobJSON (camelCase keys) is caught.
	j2, err := p.GetJob(context.Background(), j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	data := JobJSON(j2)
	status, _ := data["status"].(map[string]any)
	require.Equal(t, "DONE", status["state"])
	require.NotEmpty(t, data["driverOutputResourceUri"])
}

func TestRunJob_DriverOOM_TransitionsToError(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	j := newTestJob()
	j.JobID = "j-lc-oom"
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))

	runJobWithDriverPod(t, p, client, j, oomDriverPod("driver-oom", "jc-spark-cm-j-lc-oom"))

	j2, err := p.GetJob(context.Background(), j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	status, _ := JobJSON(j2)["status"].(map[string]any)
	require.Equal(t, "ERROR", status["state"])
	require.Contains(t, status["details"].(string), "OOMKilled")
}

// TestSparkJobOrphanCleanup mirrors EMR: a pre-existing terminal (failed) Job is
// swept on startup — OnTerminalJob fires and the Job is deleted.
func TestSparkJobOrphanCleanup(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "jc-spark-orphan",
			Namespace: "jaiscloud",
			Labels:    map[string]string{"app.kubernetes.io/managed-by": "jaiscloud"},
		},
		Status: batchv1.JobStatus{
			Failed: 1,
			Conditions: []batchv1.JobCondition{{
				Type:    batchv1.JobFailed,
				Status:  corev1.ConditionTrue,
				Message: "BackoffLimitExceeded",
			}},
		},
	}
	if _, err := client.BatchV1().Jobs("jaiscloud").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create job: %v", err)
	}

	var cbName, cbState string
	if err := k8shelpers.CleanupOrphans(ctx, client, k8shelpers.CleanupConfig{
		Namespace: "jaiscloud",
		OnTerminalJob: func(name, state, reason string) {
			cbName, cbState = name, state
		},
	}); err != nil {
		t.Fatalf("CleanupOrphans: %v", err)
	}

	require.Equal(t, "jc-spark-orphan", cbName)
	require.Equal(t, "FAILED", cbState)
	if _, err := client.BatchV1().Jobs("jaiscloud").Get(ctx, "jc-spark-orphan", metav1.GetOptions{}); err == nil {
		t.Error("expected terminal job to be deleted after CleanupOrphans")
	}
}

// TestRestartReapsSuspended mirrors EMR: a suspended (non-terminal) Job is
// re-adopted (unsuspended) and OnTerminalJob is NOT called for it.
func TestRestartReapsSuspended(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	suspended := true
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "jc-spark-suspended",
			Namespace: "jaiscloud",
			Labels:    map[string]string{"app.kubernetes.io/managed-by": "jaiscloud"},
		},
		Spec: batchv1.JobSpec{Suspend: &suspended},
	}
	if _, err := client.BatchV1().Jobs("jaiscloud").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create suspended job: %v", err)
	}

	terminalCalled := false
	if err := k8shelpers.CleanupOrphans(ctx, client, k8shelpers.CleanupConfig{
		Namespace:     "jaiscloud",
		OnTerminalJob: func(name, state, reason string) { terminalCalled = true },
	}); err != nil {
		t.Fatalf("CleanupOrphans: %v", err)
	}

	require.False(t, terminalCalled, "OnTerminalJob must not fire for a suspended job")
	updated, err := client.BatchV1().Jobs("jaiscloud").Get(ctx, "jc-spark-suspended", metav1.GetOptions{})
	require.NoError(t, err)
	require.False(t, updated.Spec.Suspend != nil && *updated.Spec.Suspend,
		"expected suspended job to be unsuspended (re-adopted)")
}

// TestTerminalSnapshotRoundTrip mirrors EMR's describe-after-TTL: a terminal
// snapshot persisted for a job survives and can be reloaded after the k8s Job
// is GC'd.
func TestTerminalSnapshotRoundTrip(t *testing.T) {
	res := store.NewMemoryResourceStore()
	ctx := context.Background()

	snap := k8shelpers.Snapshot{
		State:     "DONE",
		Reason:    "",
		StartTime: clock.RealNow().Add(-30 * time.Second),
		EndTime:   clock.RealNow(),
		ExitCode:  0,
	}
	require.NoError(t, k8shelpers.PersistTerminalSnapshot(ctx, res, "proj", "us-central1", "dataproc/jobs", "j-snap-1", snap))

	loaded, found, err := k8shelpers.LoadTerminalSnapshot(ctx, res, "proj", "us-central1", "dataproc/jobs", "j-snap-1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "DONE", loaded.State)
}

// TestCancelJob_TransitionsToCancelled cancels an in-flight (blocked) job and
// asserts the store reflects CANCELLED.
func TestCancelJob_TransitionsToCancelled(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	j := newTestJob()
	j.JobID = "j-lc-cancel"
	if err := p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// A watcher that never emits keeps runJob blocked in WaitTerminal.
	fw := prependPodWatch(t, client)
	defer fw.Stop()
	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runJob(ctx, j.ProjectID, j.Region, j, "")
	}()

	require.Eventually(t, func() bool {
		jobs, _ := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
		return len(jobs.Items) > 0
	}, 5*time.Second, 10*time.Millisecond)

	cancelled, err := p.CancelJob(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	status, _ := JobJSON(cancelled)["status"].(map[string]any)
	require.Equal(t, "CANCEL_PENDING", status["state"])

	// Reads settle the cancel progression: CANCEL_STARTED -> CANCELLED.
	final := advanceJobToTerminal(t, p, j.ProjectID, j.Region, j.JobID)
	require.Equal(t, "CANCELLED", final.Status.State)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runJob did not return after cancellation")
	}

	// CancelJob must also stop the executor: the client-mode driver Job (and
	// its pod) is deleted, not left running after the job reports CANCELLED.
	require.Eventually(t, func() bool {
		jobs, _ := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
		return len(jobs.Items) == 0
	}, 5*time.Second, 10*time.Millisecond, "cancelled job's driver k8s Job must be reaped")
}

// TestCancelJob_AlreadyTerminal_NoOp mirrors EMR: cancelling a terminal job is a
// no-op that returns OK without clobbering the terminal state.
func TestCancelJob_AlreadyTerminal_NoOp(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	j := newTestJob()
	j.Status = dataprocstore.JobStatus{State: "DONE", StateStartTime: clock.Now().UTC()}
	require.NoError(t, p.store.CreateJob(ctx, j.ProjectID, j.Region, j))

	cancelled, err := p.CancelJob(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	status, _ := JobJSON(cancelled)["status"].(map[string]any)
	require.Equal(t, "DONE", status["state"])

	got, err := p.store.GetJob(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.Equal(t, "DONE", got.Status.State)
}

// TestCancelJob_NotFound mirrors EMR: cancelling a missing job returns a 404.
func TestCancelJob_NotFound(t *testing.T) {
	p := newProvider(t)
	_, err := p.CancelJob(context.Background(), "proj", "us-central1", "nope")
	require.Error(t, err)
	require.Contains(t, err.Error(), "job not found")
}

// delayedGetJobStore wraps a dataprocstore.Store, delaying every GetJob call
// to widen a TOCTOU race window in tests. It only affects the pre-fix code
// path (CancelJob/finishJob calling a standalone GetJob); UpdateJobAtomic
// does its own internal locked read and never reaches this override, so
// against the fixed code the delay is simply never invoked.
type delayedGetJobStore struct {
	dataprocstore.Store
	delay time.Duration
}

func (d *delayedGetJobStore) GetJob(ctx context.Context, projectID, region, jobID string) (dataprocstore.Job, error) {
	j, err := d.Store.GetJob(ctx, projectID, region, jobID)
	time.Sleep(d.delay)
	return j, err
}

// TestCancelJob_CompletesSubmitOperation proves CancelJob closes out the
// SubmitJobAsOperation LRO on its own terminal-state transition. Previously
// only finishJob ever called completeSubmitOperation, so a job submitted
// asynchronously and then cancelled left its operation stuck at done=false
// forever — a client polling the operation would hang indefinitely even
// though the job itself correctly shows CANCELLED.
func TestCancelJob_CompletesSubmitOperation(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	j := newTestJob()
	j.JobID = "j-cancel-lro"
	require.NoError(t, p.store.CreateJob(ctx, j.ProjectID, j.Region, j))
	require.NoError(t, p.store.CreateOperation(ctx, j.ProjectID, j.Region, dataprocstore.Operation{
		ID: j.JobID, Verb: "submit", Target: j.JobID, CreateTime: clock.Now().UTC(),
	}))

	cancelled, err := p.CancelJob(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.Equal(t, "CANCEL_PENDING", JobJSON(cancelled)["status"].(map[string]any)["state"])

	// The lazy cancel progression reaches CANCELLED and closes the LRO.
	advanceJobToTerminal(t, p, j.ProjectID, j.Region, j.JobID)
	op, err := p.store.GetOperation(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.True(t, op.Done, "operation must be closed once the cancel progression reaches CANCELLED")
}

// TestFinishJobDoesNotResurrectCancelledJob deterministically reproduces the
// TOCTOU race between CancelJob and finishJob: both are launched concurrently
// against a job that a client is racing to cancel just as it finishes
// naturally. A delayed-Get store wrapper widens the window between each
// side's read and write. Without atomicity, whichever write lands last wins
// outright regardless of which one the client was actually told about via
// CancelJob's response — so a client told "CANCELLED" could see the job
// silently resurrect to DONE moments later. With UpdateJobAtomic, CancelJob's
// response is always consistent with the job's final persisted state: if
// CancelJob performed the transition, nothing can un-cancel it afterward.
func TestCancelJobVsFinishJobConcurrent_ResponseMatchesFinalState(t *testing.T) {
	ctx := context.Background()
	p := NewService(&delayedGetJobStore{Store: dataprocstore.NewMemoryStore(), delay: 5 * time.Millisecond}, store.NewMemoryResourceStore())
	j := newTestJob()
	j.JobID = "j-race-1"
	require.NoError(t, p.store.CreateJob(ctx, j.ProjectID, j.Region, j))
	require.NoError(t, p.store.CreateOperation(ctx, j.ProjectID, j.Region, dataprocstore.Operation{
		ID: j.JobID, Verb: "submit", Target: j.JobID, CreateTime: clock.Now().UTC(),
	}))

	var cancelJob dataprocstore.Job
	var cancelErr error
	finishDone := make(chan struct{})
	go func() {
		defer close(finishDone)
		p.finishJob(j.ProjectID, j.Region, j, "DONE", "", nil)
	}()
	cancelJob, cancelErr = p.CancelJob(ctx, j.ProjectID, j.Region, j.JobID)
	<-finishDone
	require.NoError(t, cancelErr)

	got, err := p.store.GetJob(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)

	respState := JobJSON(cancelJob)["status"].(map[string]any)["state"]
	switch respState {
	case "CANCEL_PENDING":
		require.Equal(t, "CANCELLED", got.Status.State,
			"CancelJob started the cancel progression, but the job later resurrected to %q", got.Status.State)
	case "DONE":
		require.Equal(t, "DONE", got.Status.State,
			"finishJob won the race, but the job later became %q", got.Status.State)
	default:
		require.Failf(t, "unexpected CancelJob response state", "state = %q", respState)
	}

	op, err := p.store.GetOperation(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.True(t, op.Done, "operation must be closed regardless of whether CancelJob or finishJob won the race")
}

// TestCancelJob_ConcurrentCancels_SingleTerminal stresses the cancel map under
// -race: N concurrent cancels of one in-flight job all resolve cleanly to a
// single terminal CANCELLED write (no panic/race; first write wins).
func TestCancelJob_ConcurrentCancels_SingleTerminal(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	j := newTestJob()
	j.JobID = "j-lc-concurrent"
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))

	// A watcher that never emits keeps runJob blocked in WaitTerminal, so the
	// cancel func is registered in p.cancels.
	fw := prependPodWatch(t, client)
	defer fw.Stop()
	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runJob(ctx, j.ProjectID, j.Region, j, "")
	}()
	require.Eventually(t, func() bool {
		jobs, _ := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
		return len(jobs.Items) > 0
	}, 5*time.Second, 10*time.Millisecond)

	const n = 10
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := p.CancelJob(ctx, j.ProjectID, j.Region, j.JobID)
			errs <- err
		}()
	}
	for i := 0; i < n; i++ {
		require.NoError(t, <-errs)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runJob did not return after cancellation")
	}

	// The N concurrent cancels collapse to a single CANCEL_PENDING write; reads
	// then settle it to a single terminal CANCELLED.
	final := advanceJobToTerminal(t, p, j.ProjectID, j.Region, j.JobID)
	require.Equal(t, "CANCELLED", final.Status.State)
}

// TestRunJob_WaitError_ReapsDriverAndFails pins STR4: when the terminal wait
// fails with a non-context error while the job context is still live, the
// driver may still be running, so it must be reaped before the job is reported
// ERROR — the same leak class STR3 fixed on the cancel path. The real fallback
// poll never returns a non-context error on its own, so the wait is injected.
func TestRunJob_WaitError_ReapsDriverAndFails(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	j := newTestJob()
	j.JobID = "j-lc-wait-error"
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))

	orig := waitTerminalFn
	waitTerminalFn = func(context.Context, kubernetes.Interface, k8shelpers.JobHandle, sparkhelpers.TerminalOptions) (sparkhelpers.Final, error) {
		return sparkhelpers.Final{}, errors.New("synthetic wait failure")
	}
	defer func() { waitTerminalFn = orig }()

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runJob(ctx, j.ProjectID, j.Region, j, "")
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("runJob did not complete")
	}

	got, err := p.GetJob(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	status, _ := JobJSON(got)["status"].(map[string]any)
	require.Equal(t, "ERROR", status["state"])

	// The driver k8s Job created by SubmitClientMode must not be leaked.
	jobs, err := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, jobs.Items, "driver k8s Job must be reaped on a non-cancel wait error")
}

// TestSubmitJob_RegistersCancelBeforeReturn pins STR5: submitJob used to spawn
// the executor goroutine and return, while runJob registered its cancel func
// inside the goroutine a moment later. A CancelJob landing in that window moved
// the job to CANCEL_PENDING but found no cancel func, so the driver started and
// kept running (STR3's reap never fired because the context was never
// cancelled). Registration must therefore be synchronous with submission.
func TestSubmitJob_RegistersCancelBeforeReturn(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	// A watcher that never emits keeps the executor in its terminal wait, so
	// the job cannot finish before we inspect the cancel map.
	fw := prependPodWatch(t, client)
	defer fw.Stop()

	j, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j-cancel-reg"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	}))
	require.NoError(t, err)

	// Registration must already be visible when SubmitJob returns.
	key := cancelKey("proj", "us-central1", j.JobID)
	p.cancelsMu.Lock()
	_, ok := p.cancels[key]
	p.cancelsMu.Unlock()
	require.True(t, ok, "cancel func must be registered before SubmitJob returns")

	// A cancel issued immediately after submit must reach the executor: the
	// driver k8s Job is reaped (submit -> cancel -> reap), and the job settles
	// CANCELLED.
	_, err = p.CancelJob(ctx, "proj", "us-central1", j.JobID)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		jobs, listErr := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
		return listErr == nil && len(jobs.Items) == 0
	}, 5*time.Second, 10*time.Millisecond, "an immediate CancelJob must stop and reap the driver")

	final := advanceJobToTerminal(t, p, "proj", "us-central1", j.JobID)
	require.Equal(t, "CANCELLED", final.Status.State)
}
