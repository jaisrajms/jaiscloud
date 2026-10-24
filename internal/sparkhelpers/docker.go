package sparkhelpers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	"jaiscloud/internal/docker"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/platform"
)

const (
	defaultDockerPollInterval = 500 * time.Millisecond
	defaultDockerReapTimeout  = 30 * time.Second
	dockerOrphanSweepTimeout  = 30 * time.Second

	// dockerServiceLabelValue scopes this executor's containers to Dataproc.
	dockerServiceLabelValue = "dataproc"
)

// DockerConfig configures a DockerDriver.
type DockerConfig struct {
	Logger *slog.Logger
	// Platform carries the TLS PEM bundle / extra volumes / extra env applied to
	// every driver container; may be nil.
	Platform *platform.PlatformConfig
	// InstanceID scopes container names and labels so two emulator instances
	// sharing one Docker daemon do not reap each other's containers.
	InstanceID string
	// Socket overrides the Docker API unix socket; defaults to the shared
	// default (/var/run/docker.sock).
	Socket string
	// Client overrides the Docker API client (tests). When nil, a client that
	// dials Socket is built.
	Client *http.Client
	// PollInterval bounds the exit-status poll cadence; defaults to 500ms.
	PollInterval time.Duration
	// ReapTimeout bounds a best-effort container teardown; defaults to 30s.
	ReapTimeout time.Duration
	// Addressing is how the emulator reaches the driver containers it launches:
	// host (host.docker.internal maps to the daemon host) or container (the
	// driver reaches the emulator by name on the shared network). The zero value
	// is host mode. The startup wiring resolves it with docker.ResolveAddressing
	// and gates docker mode on docker.Probe.
	Addressing docker.Addressing
}

// DockerHandle identifies a submitted Docker driver container. It is opaque to
// the Dataproc core, which only passes it back to the same DockerDriver.
type DockerHandle struct {
	ID   string
	Name string
}

// DockerDriver runs Spark jobs as one-shot containers on the local Docker
// daemon — the k8s-free sibling of SubmitClientMode/WaitTerminal. The configured
// Spark image runs `spark-submit --master local[*]` in client mode with the same
// GCS connector env/confs the k8s path injects, and the driver's stdout/stderr
// is streamed from the container log. Terminal state is classified by the same
// rules as k8s (sparkhelpers.Classify), so Job.scheduling restarts behave
// identically whichever orchestrator runs the driver. It is built on the shared
// internal/docker client, so its transport matches the Cloud Run docker runtime.
type DockerDriver struct {
	client       *docker.Client
	platform     *platform.PlatformConfig
	instanceID   string
	addr         docker.Addressing
	pollInterval time.Duration
	reapTimeout  time.Duration
	logger       *slog.Logger

	sweepOnce sync.Once
}

// Ping reports whether the Docker daemon at socket answers the Engine API ping
// (empty uses the default local socket). It lets startup fall back to mock.
func Ping(ctx context.Context, socket string) error { return docker.Ping(ctx, socket) }

// NewDockerDriver returns a Docker-backed Spark driver executor.
func NewDockerDriver(cfg DockerConfig) *DockerDriver {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	poll := cfg.PollInterval
	if poll == 0 {
		poll = defaultDockerPollInterval
	}
	reap := cfg.ReapTimeout
	if reap == 0 {
		reap = defaultDockerReapTimeout
	}
	return &DockerDriver{
		client:       docker.New(docker.Config{Logger: logger, Socket: cfg.Socket, Client: cfg.Client}),
		platform:     cfg.Platform,
		instanceID:   cfg.InstanceID,
		addr:         cfg.Addressing,
		pollInterval: poll,
		reapTimeout:  reap,
		logger:       logger,
	}
}

// Submit creates and starts a driver container for the job and returns its
// handle. The driver's exit is observed by WaitTerminal; Submit itself does not
// block on the job.
func (d *DockerDriver) Submit(ctx context.Context, job ClientModeJob) (DockerHandle, error) {
	if job.Image == "" {
		return DockerHandle{}, fmt.Errorf("sparkhelpers: DriverJob.Image is required")
	}
	// Reap containers left by a previous emulator instance before the first job
	// of this process.
	d.sweepOnce.Do(d.sweepOrphans)

	name := dockerDriverContainerName(d.instanceID, job.JobID, job.Attempt)
	// Idempotent re-ensure: a prior attempt may have left the container behind.
	_ = d.client.Remove(ctx, name)

	spec := dockerDriverSpec{
		name:    name,
		image:   job.Image,
		command: []string{DriverCommand(job)},
		args:    BuildDockerArgs(job),
		env:     driverEnvStrings(job.ExtraDriverEnv),
		labels:  d.driverLabels(job),
	}
	id, err := d.startContainer(ctx, spec)
	if err != nil {
		return DockerHandle{}, err
	}
	return DockerHandle{ID: id, Name: name}, nil
}

// WaitTerminal blocks until the driver container exits and returns its
// classified Spark result. Driver logs are collected for classification.
func (d *DockerDriver) WaitTerminal(ctx context.Context, h DockerHandle, opts TerminalOptions) (Final, error) {
	base, err := d.waitExit(ctx, h)
	if err != nil {
		return Final{}, err
	}
	var buf bytes.Buffer
	if logErr := d.StreamLogs(ctx, h, &buf); logErr != nil {
		slog.Warn("sparkhelpers: docker driver logs unavailable for classification", "container", h.Name, "err", logErr)
	}
	return Classify(base, SplitLines(buf.String()), opts), nil
}

// StreamLogs writes the driver's stdout/stderr to sink (non-following; the
// container log is retained until the container is removed).
func (d *DockerDriver) StreamLogs(ctx context.Context, h DockerHandle, sink io.Writer) error {
	id := h.ID
	if id == "" {
		id = h.Name
	}
	return d.client.StreamLogs(ctx, id, sink)
}

// Reap stops and removes a driver container that may still be running. It runs
// on a fresh bounded context (the job context may already be cancelled).
func (d *DockerDriver) Reap(h DockerHandle) {
	id := h.ID
	if id == "" {
		id = h.Name
	}
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.reapTimeout)
	defer cancel()
	if err := d.client.Remove(ctx, id); err != nil {
		d.logger.Warn("sparkhelpers: failed to reap driver container", "container", h.Name, "err", err)
	}
}

// Reset reaps every driver container this emulator instance owns
// (/_jaiscloud/reset).
func (d *DockerDriver) Reset(ctx context.Context) {
	if _, err := d.client.RemoveByFilter(ctx, d.labelFilter(nil)); err != nil {
		d.logger.Warn("dataproc: docker reset sweep incomplete", "err", err)
	}
}

// ReapCluster reaps the driver containers of one cluster (cluster delete).
func (d *DockerDriver) ReapCluster(ctx context.Context, project, region, cluster string) {
	if _, err := d.client.RemoveByFilter(ctx, d.labelFilter([]string{"jaiscloud.io/cluster-name=" + cluster})); err != nil {
		d.logger.Warn("dataproc: docker cluster sweep incomplete", "cluster", cluster, "err", err)
	}
}

// sweepOrphans reaps driver containers left by a previous emulator instance.
// Best-effort: a failure is logged and never blocks a job start.
func (d *DockerDriver) sweepOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), dockerOrphanSweepTimeout)
	defer cancel()
	if n, err := d.client.RemoveByFilter(ctx, d.labelFilter(nil)); err != nil {
		d.logger.Warn("dataproc: docker orphan sweep incomplete", "err", err)
	} else if n > 0 {
		d.logger.Info("dataproc: reaped orphan driver containers", "count", n)
	}
}

// driverLabels merges the job's labels with the executor-owned scoping labels.
func (d *DockerDriver) driverLabels(job ClientModeJob) map[string]string {
	labels := make(map[string]string, len(job.Labels)+2)
	for k, v := range job.Labels {
		labels[k] = v
	}
	labels[docker.LabelService] = dockerServiceLabelValue
	if d.instanceID != "" {
		labels[docker.LabelInstance] = d.instanceID
	}
	return labels
}

// labelFilter scopes a Docker label filter to this emulator instance.
func (d *DockerDriver) labelFilter(extra []string) map[string][]string {
	return docker.LabelFilter(dockerServiceLabelValue, d.instanceID, extra...)
}

// dockerDriverSpec is the resolved input for starting one driver container.
type dockerDriverSpec struct {
	name    string
	image   string
	command []string
	args    []string
	env     []string
	labels  map[string]string
}

// startContainer creates and starts the driver container. It returns the
// container id; the caller observes exit via WaitTerminal.
func (d *DockerDriver) startContainer(ctx context.Context, spec dockerDriverSpec) (string, error) {
	hostCfg := map[string]any{
		"AutoRemove": false,
	}
	if !d.addr.Containerized() {
		// Make the emulator host reachable from the container under the same
		// name the Iceberg/HMS harnesses use, so a GCSEndpoint of
		// http://host.docker.internal:<port> resolves. In container deployment
		// mode the emulator is not on the daemon host — the driver reaches it by
		// name on the shared network instead.
		hostCfg["ExtraHosts"] = []string{"host.docker.internal:host-gateway"}
	}
	if d.addr.Network != "" {
		hostCfg["NetworkMode"] = d.addr.Network
	}
	var env = append([]string{}, spec.env...)
	binds, platformEnv, pErr := docker.BindsAndEnv(d.platform)
	if pErr != nil {
		d.logger.Warn("dataproc docker: platform apply failed", "err", pErr)
	}
	env = append(env, platformEnv...)
	if len(binds) > 0 {
		hostCfg["Binds"] = binds
	}
	createBody := map[string]any{
		"Image":      spec.image,
		"Env":        env,
		"Labels":     spec.labels,
		"HostConfig": hostCfg,
	}
	if len(spec.command) > 0 {
		createBody["Entrypoint"] = spec.command
	}
	if len(spec.args) > 0 {
		createBody["Cmd"] = spec.args
	}
	body, err := json.Marshal(createBody)
	if err != nil {
		return "", fmt.Errorf("docker create: %w", err)
	}

	id, err := d.client.Create(ctx, spec.name, body)
	if err != nil {
		return "", err
	}
	if respBody, status, err := d.client.Call(ctx, "POST", "/containers/"+id+"/start", nil); err != nil {
		_ = d.client.Remove(context.WithoutCancel(ctx), id)
		return "", fmt.Errorf("docker start: %w", err)
	} else if status >= 300 {
		_ = d.client.Remove(context.WithoutCancel(ctx), id)
		return "", fmt.Errorf("docker start: HTTP %d: %s", status, strings.TrimSpace(string(respBody)))
	}
	d.logger.Info("dataproc docker: started driver container", "container", dockerShortName(spec.name), "id", docker.ShortID(id))
	return id, nil
}

// waitExit polls the container until it reaches a terminal state.
func (d *DockerDriver) waitExit(ctx context.Context, h DockerHandle) (k8shelpers.Final, error) {
	id := h.ID
	if id == "" {
		id = h.Name
	}
	for {
		st, err := d.inspect(ctx, id)
		if err != nil {
			return k8shelpers.Final{}, err
		}
		if st.Status == "exited" || st.Status == "dead" {
			return st.final(), nil
		}
		select {
		case <-ctx.Done():
			return k8shelpers.Final{}, ctx.Err()
		case <-time.After(d.pollInterval):
		}
	}
}

// dockerContainerState is the subset of `docker inspect` the driver needs.
type dockerContainerState struct {
	Status     string
	ExitCode   int
	OOMKilled  bool
	Error      string
	StartedAt  time.Time
	FinishedAt time.Time
}

// final converts a container's terminal state to the backend-level Final the
// shared Spark classifier consumes (mirroring k8shelpers' pod-level Final).
func (st dockerContainerState) final() k8shelpers.Final {
	reason := "Error"
	switch {
	case st.OOMKilled:
		reason = "OOMKilled"
	case st.ExitCode == 0:
		reason = "Completed"
	}
	return k8shelpers.Final{
		Succeeded: st.ExitCode == 0,
		ExitCode:  int32(st.ExitCode),
		Reason:    reason,
		Message:   st.Error,
		StartTime: st.StartedAt,
		EndTime:   st.FinishedAt,
	}
}

// inspect reads a container's state.
func (d *DockerDriver) inspect(ctx context.Context, id string) (dockerContainerState, error) {
	body, status, err := d.client.Call(ctx, "GET", "/containers/"+id+"/json", nil)
	if err != nil {
		return dockerContainerState{}, err
	}
	if status >= 300 {
		return dockerContainerState{}, fmt.Errorf("docker inspect: HTTP %d", status)
	}
	var info struct {
		State struct {
			Status     string `json:"Status"`
			ExitCode   int    `json:"ExitCode"`
			OOMKilled  bool   `json:"OOMKilled"`
			Error      string `json:"Error"`
			StartedAt  string `json:"StartedAt"`
			FinishedAt string `json:"FinishedAt"`
		} `json:"State"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return dockerContainerState{}, err
	}
	return dockerContainerState{
		Status:     info.State.Status,
		ExitCode:   info.State.ExitCode,
		OOMKilled:  info.State.OOMKilled,
		Error:      info.State.Error,
		StartedAt:  docker.ParseTime(info.State.StartedAt),
		FinishedAt: docker.ParseTime(info.State.FinishedAt),
	}, nil
}

// driverEnvStrings flattens the driver env (name=value) for the Docker create
// body. ValueFrom is not used on the Spark driver path.
func driverEnvStrings(env []corev1.EnvVar) []string {
	out := make([]string, 0, len(env))
	for _, e := range env {
		out = append(out, e.Name+"="+e.Value)
	}
	return out
}

// dockerDriverContainerName is the deterministic, instance-scoped container name
// for a driver attempt.
func dockerDriverContainerName(instanceID, jobID string, attempt int) string {
	name := "jc-spark-" + docker.ShortInstance(instanceID) + "-" + sanitizeJobID(jobID)
	if attempt > 0 {
		name += fmt.Sprintf("-r%d", attempt)
	}
	return name
}

func dockerShortName(name string) string {
	if len(name) > 40 {
		return name[:40]
	}
	return name
}
