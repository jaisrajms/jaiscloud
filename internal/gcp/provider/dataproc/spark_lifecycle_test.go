package dataproc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"jaiscloud/internal/clock"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newK8sProvider returns a provider wired with a fake clientset for real Spark
// execution (k8s mode), plus a memory store for jobs and terminal snapshots.
func newK8sProvider(t *testing.T, client *fake.Clientset) *Provider {
	t.Helper()
	p := New(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil),
		WithSparkImage("spark:test"),
	)
	// New() starts a background ownership-patcher goroutine; drain it so the
	// test does not leak goroutines/watchers.
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
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
func runJobWithDriverPod(t *testing.T, p *Provider, client *fake.Clientset, j dataprocstore.Job, pod *corev1.Pod) {
	t.Helper()
	fw := prependPodWatch(t, client)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runJob(ctx, j.ProjectID, j.Region, j)
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
	// jobToMap (camelCase keys) is caught.
	resp, err := p.GetJob(context.Background(), testNR(map[string]any{"region": j.Region, "jobId": j.JobID}))
	require.NoError(t, err)
	status, _ := resp.Data["status"].(map[string]any)
	require.Equal(t, "DONE", status["state"])
	require.NotEmpty(t, resp.Data["driverOutputResourceUri"])
}

func TestRunJob_DriverOOM_TransitionsToError(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	j := newTestJob()
	j.JobID = "j-lc-oom"
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))

	runJobWithDriverPod(t, p, client, j, oomDriverPod("driver-oom", "jc-spark-cm-j-lc-oom"))

	resp, err := p.GetJob(context.Background(), testNR(map[string]any{"region": j.Region, "jobId": j.JobID}))
	require.NoError(t, err)
	status, _ := resp.Data["status"].(map[string]any)
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
		p.runJob(ctx, j.ProjectID, j.Region, j)
	}()

	require.Eventually(t, func() bool {
		jobs, _ := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
		return len(jobs.Items) > 0
	}, 5*time.Second, 10*time.Millisecond)

	resp, err := p.CancelJob(ctx, testNR(map[string]any{"region": j.Region, "jobId": j.JobID}))
	require.NoError(t, err)
	status, _ := resp.Data["status"].(map[string]any)
	require.Equal(t, "CANCELLED", status["state"])

	got, err := p.store.GetJob(context.Background(), j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.Equal(t, "CANCELLED", got.Status.State)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runJob did not return after cancellation")
	}
}

// TestCancelJob_AlreadyTerminal_NoOp mirrors EMR: cancelling a terminal job is a
// no-op that returns OK without clobbering the terminal state.
func TestCancelJob_AlreadyTerminal_NoOp(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	j := newTestJob()
	j.Status = dataprocstore.JobStatus{State: "DONE", StateStartTime: clock.Now().UTC()}
	require.NoError(t, p.store.CreateJob(ctx, j.ProjectID, j.Region, j))

	resp, err := p.CancelJob(ctx, testNR(map[string]any{"region": j.Region, "jobId": j.JobID}))
	require.NoError(t, err)
	status, _ := resp.Data["status"].(map[string]any)
	require.Equal(t, "DONE", status["state"])

	got, err := p.store.GetJob(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.Equal(t, "DONE", got.Status.State)
}

// TestCancelJob_NotFound mirrors EMR: cancelling a missing job returns a 404.
func TestCancelJob_NotFound(t *testing.T) {
	p := newProvider(t)
	_, err := p.CancelJob(context.Background(), testNR(map[string]any{"region": "us-central1", "jobId": "nope"}))
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

	resp, err := p.CancelJob(ctx, testNR(map[string]any{"region": j.Region, "jobId": j.JobID}))
	require.NoError(t, err)
	require.Equal(t, "CANCELLED", resp.Data["status"].(map[string]any)["state"])

	op, err := p.store.GetOperation(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.True(t, op.Done, "operation must be closed once CancelJob transitions the job to CANCELLED")
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
	p := New(&delayedGetJobStore{Store: dataprocstore.NewMemoryStore(), delay: 5 * time.Millisecond}, store.NewMemoryResourceStore())
	j := newTestJob()
	j.JobID = "j-race-1"
	require.NoError(t, p.store.CreateJob(ctx, j.ProjectID, j.Region, j))
	require.NoError(t, p.store.CreateOperation(ctx, j.ProjectID, j.Region, dataprocstore.Operation{
		ID: j.JobID, Verb: "submit", Target: j.JobID, CreateTime: clock.Now().UTC(),
	}))

	var cancelResp *model.ProviderResponse
	var cancelErr error
	finishDone := make(chan struct{})
	go func() {
		defer close(finishDone)
		p.finishJob(j.ProjectID, j.Region, j, "DONE", "")
	}()
	cancelResp, cancelErr = p.CancelJob(ctx, testNR(map[string]any{"region": j.Region, "jobId": j.JobID}))
	<-finishDone
	require.NoError(t, cancelErr)

	got, err := p.store.GetJob(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)

	respState := cancelResp.Data["status"].(map[string]any)["state"]
	if respState == "CANCELLED" {
		require.Equal(t, "CANCELLED", got.Status.State,
			"CancelJob told the client CANCELLED, but the job later resurrected to %q", got.Status.State)
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
		p.runJob(ctx, j.ProjectID, j.Region, j)
	}()
	require.Eventually(t, func() bool {
		jobs, _ := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
		return len(jobs.Items) > 0
	}, 5*time.Second, 10*time.Millisecond)

	const n = 10
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := p.CancelJob(ctx, testNR(map[string]any{"region": j.Region, "jobId": j.JobID}))
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

	got, err := p.store.GetJob(ctx, j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.Equal(t, "CANCELLED", got.Status.State)
}
