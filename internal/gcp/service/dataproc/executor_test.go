package dataproc

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
	"jaiscloud/internal/store"
)

// fakeExecutor is a scriptable driverExecutor used to prove the run loop is
// backend-agnostic: it records the submitted jobs and replays a canned terminal
// result, so a Docker-mode submission exercises the same state machine as k8s
// without a real container.
type fakeExecutor struct {
	mu sync.Mutex

	submitted []sparkhelpers.ClientModeJob
	streamed  bool
	final     sparkhelpers.Final
	waitErr   error

	reaped    int
	reset     int
	reapClust []string
}

func (f *fakeExecutor) Submit(_ context.Context, job sparkhelpers.ClientModeJob) (driverHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.submitted = append(f.submitted, job)
	return driverHandle{docker: sparkhelpers.DockerHandle{ID: "c1", Name: "jc-spark-test"}}, nil
}

func (f *fakeExecutor) WaitTerminal(context.Context, driverHandle, sparkhelpers.TerminalOptions) (sparkhelpers.Final, error) {
	return f.final, f.waitErr
}

func (f *fakeExecutor) StreamLogs(_ context.Context, _ driverHandle, sink io.Writer) error {
	f.mu.Lock()
	f.streamed = true
	f.mu.Unlock()
	_, err := io.WriteString(sink, "driver stdout")
	return err
}

func (f *fakeExecutor) Reap(driverHandle) {
	f.mu.Lock()
	f.reaped++
	f.mu.Unlock()
}

func (f *fakeExecutor) Reset(context.Context) {
	f.mu.Lock()
	f.reset++
	f.mu.Unlock()
}

func (f *fakeExecutor) ReapCluster(_ context.Context, _, _, cluster string) {
	f.mu.Lock()
	f.reapClust = append(f.reapClust, cluster)
	f.mu.Unlock()
}

// fakeBlobSink is defined in staging_test.go.

func succeededFinal() sparkhelpers.Final {
	return sparkhelpers.Final{
		Final:          k8shelpers.Final{Succeeded: true, ExitCode: 0},
		SparkSucceeded: true,
		SparkReason:    "exit 0",
	}
}

func TestMockModeWithoutExecutor(t *testing.T) {
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore())
	require.True(t, p.mockMode(), "no executor must be mock mode")

	p2 := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithExecutor(&fakeExecutor{}))
	require.False(t, p2.mockMode(), "an executor must disable mock mode")
}

func TestRunJobWithExecutorDrivesStateAndStagesOutput(t *testing.T) {
	fx := &fakeExecutor{final: succeededFinal()}
	sink := newFakeBlobSink()
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithExecutor(fx),
		WithSparkImage("spark:test"),
		WithBlobSink(sink),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	j := dpstore.Job{
		ProjectID:            "proj",
		Region:               "us-central1",
		JobID:                "j1",
		PlacementClusterName: "c1",
		Type:                 "pysparkJob",
		TypeJob:              []byte(`{"mainPythonFileUri":"gs://b/main.py"}`),
		JobUUID:              "uuid-1",
		Status:               dpstore.JobStatus{State: jobStateRunning, StateStartTime: clock.Now().UTC()},
	}
	require.NoError(t, p.store.CreateJob(context.Background(), "proj", "us-central1", j))

	p.runJob(context.Background(), "proj", "us-central1", j, "", "")

	got, err := p.store.GetJob(context.Background(), "proj", "us-central1", "j1")
	require.NoError(t, err)
	require.Equal(t, jobStateDone, got.Status.State)

	fx.mu.Lock()
	require.Len(t, fx.submitted, 1)
	require.Equal(t, "spark:test", fx.submitted[0].Image)
	require.Equal(t, "pysparkJob", j.Type)
	require.True(t, fx.streamed)
	fx.mu.Unlock()

	// The driver output captured from the executor was staged to the job's GCS
	// driver-output object at terminal state.
	sink.mu.Lock()
	require.NotEmpty(t, sink.objects)
	found := false
	for _, data := range sink.objects {
		if string(data) == "driver stdout" {
			found = true
		}
	}
	require.True(t, found, "driver output was not staged: %v", sink.objects)
	sink.mu.Unlock()
}

func TestRunJobWithExecutorReapsOnRestart(t *testing.T) {
	// A restartable job whose first attempt fails strictly is reaped, restarted,
	// and succeeds on the second attempt; the same seam drives both.
	fx := &scriptedExecutor{
		finals: []sparkhelpers.Final{
			{Final: k8shelpers.Final{Succeeded: false, ExitCode: 1}, SparkReason: "attempt failed"},
			succeededFinal(),
		},
	}
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithExecutor(fx),
		WithSparkImage("spark:test"),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	j := dpstore.Job{
		ProjectID:            "proj",
		Region:               "us-central1",
		JobID:                "j2",
		PlacementClusterName: "c1",
		Type:                 "pysparkJob",
		TypeJob:              []byte(`{"mainPythonFileUri":"gs://b/main.py"}`),
		JobUUID:              "uuid-2",
		Scheduling:           &dpstore.JobScheduling{MaxFailuresTotal: 3},
		Status:               dpstore.JobStatus{State: jobStateRunning, StateStartTime: clock.Now().UTC()},
	}
	require.NoError(t, p.store.CreateJob(context.Background(), "proj", "us-central1", j))

	p.runJob(context.Background(), "proj", "us-central1", j, "", "")

	got, err := p.store.GetJob(context.Background(), "proj", "us-central1", "j2")
	require.NoError(t, err)
	require.Equal(t, jobStateDone, got.Status.State)
	fx.mu.Lock()
	require.Len(t, fx.submitted, 2, "the failed attempt must be restarted")
	require.Equal(t, 1, fx.reaped, "the failed attempt must be reaped")
	fx.mu.Unlock()
}

// scriptedExecutor returns a different terminal result per attempt.
type scriptedExecutor struct {
	mu        sync.Mutex
	submitted []sparkhelpers.ClientModeJob
	finals    []sparkhelpers.Final
	attempt   int
	reaped    int
}

func (f *scriptedExecutor) Submit(_ context.Context, job sparkhelpers.ClientModeJob) (driverHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.submitted = append(f.submitted, job)
	return driverHandle{docker: sparkhelpers.DockerHandle{ID: "c", Name: "n"}}, nil
}

func (f *scriptedExecutor) WaitTerminal(context.Context, driverHandle, sparkhelpers.TerminalOptions) (sparkhelpers.Final, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.attempt
	if idx >= len(f.finals) {
		idx = len(f.finals) - 1
	}
	f.attempt++
	return f.finals[idx], nil
}

func (f *scriptedExecutor) StreamLogs(context.Context, driverHandle, io.Writer) error { return nil }

func (f *scriptedExecutor) Reap(driverHandle) {
	f.mu.Lock()
	f.reaped++
	f.mu.Unlock()
}

func (f *scriptedExecutor) Reset(context.Context) {}

func (f *scriptedExecutor) ReapCluster(context.Context, string, string, string) {}

func TestResetReapsExecutor(t *testing.T) {
	fx := &fakeExecutor{}
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithExecutor(fx))
	p.Reset(context.Background())
	fx.mu.Lock()
	require.Equal(t, 1, fx.reset)
	fx.mu.Unlock()
}

func TestTeardownClusterReapsExecutorContainers(t *testing.T) {
	fx := &fakeExecutor{}
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithExecutor(fx))
	p.teardownClusterNamespace(context.Background(), dpstore.Cluster{ProjectID: "proj", Region: "r", Name: "c1"})
	fx.mu.Lock()
	require.Equal(t, []string{"c1"}, fx.reapClust)
	fx.mu.Unlock()
}
