package dataproc

import (
	"context"
	"io"
	"log/slog"

	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
)

// driverHandle identifies a submitted driver on whichever executor backend owns
// it. Exactly one field is populated; the core only ever passes the handle back
// to the same executor.
type driverHandle struct {
	k8s    k8shelpers.JobHandle
	docker sparkhelpers.DockerHandle
}

// driverExecutor is the driver-submission seam behind the Spark job state
// machine: k8s runs spark-submit as a client-mode Job, docker runs it as a
// one-shot container on the local Docker daemon. The run loop (state
// transitions, restart/Job.scheduling, driver-output staging) is
// backend-agnostic and calls only this seam. A nil executor means mock mode,
// where the job settles lazily in the store.
type driverExecutor interface {
	// Submit starts a driver and returns its handle.
	Submit(ctx context.Context, job sparkhelpers.ClientModeJob) (driverHandle, error)
	// WaitTerminal blocks until the driver exits and returns the classified
	// Spark result.
	WaitTerminal(ctx context.Context, h driverHandle, opts sparkhelpers.TerminalOptions) (sparkhelpers.Final, error)
	// StreamLogs writes the driver's stdout/stderr to sink.
	StreamLogs(ctx context.Context, h driverHandle, sink io.Writer) error
	// Reap force-stops a driver that may still be running (cancel, restart).
	Reap(h driverHandle)
	// Reset reaps every driver this emulator instance owns (/_jaiscloud/reset).
	Reset(ctx context.Context)
	// ReapCluster reaps one cluster's drivers (cluster delete).
	ReapCluster(ctx context.Context, project, region, cluster string)
}

// k8sDriver executes drivers as client-mode k8s Jobs via the shared
// sparkhelpers/k8shelpers engine.
type k8sDriver struct {
	client kubernetes.Interface
}

func (e k8sDriver) Submit(ctx context.Context, job sparkhelpers.ClientModeJob) (driverHandle, error) {
	h, err := sparkhelpers.SubmitClientMode(ctx, e.client, job)
	return driverHandle{k8s: h}, err
}

func (e k8sDriver) WaitTerminal(ctx context.Context, h driverHandle, opts sparkhelpers.TerminalOptions) (sparkhelpers.Final, error) {
	return waitTerminalFn(ctx, e.client, h.k8s, opts)
}

func (e k8sDriver) StreamLogs(ctx context.Context, h driverHandle, sink io.Writer) error {
	return k8shelpers.TailLogs(ctx, e.client, h.k8s, k8shelpers.LogKindMainRaw, sink)
}

func (e k8sDriver) Reap(h driverHandle) {
	if h.k8s.JobName == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), driverReapTimeout)
	defer cancel()
	if err := k8shelpers.Cancel(ctx, e.client, h.k8s); err != nil {
		slog.Warn("dataproc: failed to delete driver job", "job", h.k8s.JobName, "err", err)
	}
}

// Reset is a no-op: the core reaps owned namespaces (which cascade their
// workloads) on /_jaiscloud/reset.
func (k8sDriver) Reset(context.Context) {}

// ReapCluster is a no-op: the core deletes the cluster's k8s Jobs directly on
// cluster delete (deleteClusterJobs).
func (k8sDriver) ReapCluster(context.Context, string, string, string) {}

// dockerDriver executes drivers as one-shot containers on the local Docker
// daemon via sparkhelpers.DockerDriver.
type dockerDriver struct {
	driver *sparkhelpers.DockerDriver
}

func (e dockerDriver) Submit(ctx context.Context, job sparkhelpers.ClientModeJob) (driverHandle, error) {
	h, err := e.driver.Submit(ctx, job)
	return driverHandle{docker: h}, err
}

func (e dockerDriver) WaitTerminal(ctx context.Context, h driverHandle, opts sparkhelpers.TerminalOptions) (sparkhelpers.Final, error) {
	return e.driver.WaitTerminal(ctx, h.docker, opts)
}

func (e dockerDriver) StreamLogs(ctx context.Context, h driverHandle, sink io.Writer) error {
	return e.driver.StreamLogs(ctx, h.docker, sink)
}

func (e dockerDriver) Reap(h driverHandle) { e.driver.Reap(h.docker) }

func (e dockerDriver) Reset(ctx context.Context) { e.driver.Reset(ctx) }

func (e dockerDriver) ReapCluster(ctx context.Context, _, _, cluster string) {
	e.driver.ReapCluster(ctx, "", "", cluster)
}

// WithExecutor overrides the driver-submission backend. A nil executor is
// ignored, leaving mock mode.
func WithExecutor(e driverExecutor) Option {
	return func(s *Service) {
		if e != nil {
			s.executor = e
		}
	}
}

// DockerConfig configures the docker Spark driver executor. It mirrors the
// sparkhelpers Docker knobs and is re-exported so cmd/jaiscloud-gcp can wire
// docker mode without importing internal/sparkhelpers directly.
type DockerConfig = sparkhelpers.DockerConfig

// WithDocker runs jobs through a docker Spark driver on the local Docker daemon.
func WithDocker(cfg DockerConfig) Option {
	return func(s *Service) {
		s.executor = dockerDriver{driver: sparkhelpers.NewDockerDriver(cfg)}
	}
}

// DockerPing reports whether the local Docker daemon is reachable, so startup
// can fall back to mock when it is not.
func DockerPing(ctx context.Context, socket string) error { return sparkhelpers.Ping(ctx, socket) }
