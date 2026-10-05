package sparkhelpers

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"

	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/platform"
)

const (
	// DockerSocketPath is the local Docker API socket the driver container runs
	// on. Like the Lambda/ECS executors the emulator dials a daemon on its own
	// kernel and ignores DOCKER_HOST; the console's runtime health view surfaces
	// that.
	DockerSocketPath = "/var/run/docker.sock"
	dockerAPIVersion = "v1.41"

	dockerLabelService  = "jaiscloud.io/service"
	dockerLabelValue    = "dataproc"
	dockerLabelInstance = "jaiscloud.io/instance-id"

	defaultDockerPollInterval = 500 * time.Millisecond
	defaultDockerReapTimeout  = 30 * time.Second
	dockerOrphanSweepTimeout  = 30 * time.Second
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
	// Socket overrides the Docker API unix socket; defaults to
	// /var/run/docker.sock.
	Socket string
	// Client overrides the Docker API client (tests). When nil, a client that
	// dials Socket is built.
	Client *http.Client
	// PollInterval bounds the exit-status poll cadence; defaults to 500ms.
	PollInterval time.Duration
	// ReapTimeout bounds a best-effort container teardown; defaults to 30s.
	ReapTimeout time.Duration
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
// identically whichever orchestrator runs the driver.
type DockerDriver struct {
	client       *http.Client
	platform     *platform.PlatformConfig
	instanceID   string
	pollInterval time.Duration
	reapTimeout  time.Duration
	logger       *slog.Logger

	sweepOnce sync.Once
}

// Ping reports whether the Docker daemon at socket answers the Engine API ping.
// An empty socket uses the default local socket. It lets startup fall back to
// mock execution when no daemon is reachable, rather than failing every job.
func Ping(ctx context.Context, socket string) error {
	if socket == "" {
		socket = DockerSocketPath
	}
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/_ping", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("docker: ping %s = HTTP %d", socket, resp.StatusCode)
	}
	return nil
}

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
	socket := cfg.Socket
	if socket == "" {
		socket = DockerSocketPath
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			},
		}}
	}
	return &DockerDriver{
		client:       client,
		platform:     cfg.Platform,
		instanceID:   cfg.InstanceID,
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
	_ = d.removeContainer(ctx, name)

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
	resp, err := d.dockerStream(ctx, http.MethodGet, "/containers/"+id+"/logs?stdout=1&stderr=1&follow=false")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("docker logs: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return demuxDockerLogs(resp.Body, sink)
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
	if err := d.removeContainer(ctx, id); err != nil {
		d.logger.Warn("sparkhelpers: failed to reap driver container", "container", h.Name, "err", err)
	}
}

// Reset reaps every driver container this emulator instance owns
// (/_jaiscloud/reset).
func (d *DockerDriver) Reset(ctx context.Context) {
	if _, err := d.removeByFilter(ctx, d.labelFilter(nil)); err != nil {
		d.logger.Warn("dataproc: docker reset sweep incomplete", "err", err)
	}
}

// ReapCluster reaps the driver containers of one cluster (cluster delete).
func (d *DockerDriver) ReapCluster(ctx context.Context, project, region, cluster string) {
	if _, err := d.removeByFilter(ctx, d.labelFilter([]string{"jaiscloud.io/cluster-name=" + cluster})); err != nil {
		d.logger.Warn("dataproc: docker cluster sweep incomplete", "cluster", cluster, "err", err)
	}
}

// sweepOrphans reaps driver containers left by a previous emulator instance.
// Best-effort: a failure is logged and never blocks a job start.
func (d *DockerDriver) sweepOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), dockerOrphanSweepTimeout)
	defer cancel()
	if n, err := d.removeByFilter(ctx, d.labelFilter(nil)); err != nil {
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
	labels[dockerLabelService] = dockerLabelValue
	if d.instanceID != "" {
		labels[dockerLabelInstance] = d.instanceID
	}
	return labels
}

// labelFilter builds a Docker label filter that always scopes to this emulator
// instance, so two emulators sharing a daemon never reap each other's
// containers.
func (d *DockerDriver) labelFilter(extra []string) map[string][]string {
	labels := []string{dockerLabelService + "=" + dockerLabelValue}
	if d.instanceID != "" {
		labels = append(labels, dockerLabelInstance+"="+d.instanceID)
	}
	labels = append(labels, extra...)
	return map[string][]string{"label": labels}
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
		// Make the emulator host reachable from the container under the same
		// name the Iceberg/HMS harnesses use, so a GCSEndpoint of
		// http://host.docker.internal:<port> resolves.
		"ExtraHosts": []string{"host.docker.internal:host-gateway"},
	}
	var env = append([]string{}, spec.env...)
	if d.platform != nil {
		volArgs, envArgs, err := platform.ApplyDocker(d.platform)
		if err != nil {
			d.logger.Warn("dataproc docker: platform apply failed", "err", err)
		}
		var binds []string
		for i := 1; i < len(volArgs); i += 2 {
			binds = append(binds, volArgs[i])
		}
		for i := 1; i < len(envArgs); i += 2 {
			env = append(env, envArgs[i])
		}
		if len(binds) > 0 {
			hostCfg["Binds"] = binds
		}
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

	id, err := d.createContainer(ctx, spec.name, body)
	if err != nil {
		return "", err
	}
	if respBody, status, err := d.dockerCall(ctx, http.MethodPost, "/containers/"+id+"/start", nil); err != nil {
		_ = d.removeContainer(context.WithoutCancel(ctx), id)
		return "", fmt.Errorf("docker start: %w", err)
	} else if status >= 300 {
		_ = d.removeContainer(context.WithoutCancel(ctx), id)
		return "", fmt.Errorf("docker start: HTTP %d: %s", status, strings.TrimSpace(string(respBody)))
	}
	d.logger.Info("dataproc docker: started driver container", "container", dockerShortName(spec.name), "id", dockerShortID(id))
	return id, nil
}

// createContainer creates a container, retrying once after removing a same-named
// leftover when the daemon reports a name conflict (409).
func (d *DockerDriver) createContainer(ctx context.Context, name string, body []byte) (string, error) {
	createURL := "/containers/create?name=" + url.QueryEscape(name)
	respBody, status, err := d.dockerCall(ctx, http.MethodPost, createURL, body)
	if err != nil {
		return "", fmt.Errorf("docker create: %w", err)
	}
	if status == http.StatusConflict {
		if rmErr := d.removeContainer(context.WithoutCancel(ctx), name); rmErr == nil {
			respBody, status, err = d.dockerCall(ctx, http.MethodPost, createURL, body)
			if err != nil {
				return "", fmt.Errorf("docker create: %w", err)
			}
		}
	}
	if status >= 300 {
		return "", fmt.Errorf("docker create: HTTP %d: %s", status, strings.TrimSpace(string(respBody)))
	}
	var createResp struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(respBody, &createResp); err != nil || createResp.ID == "" {
		return "", fmt.Errorf("docker create: malformed response: %s", strings.TrimSpace(string(respBody)))
	}
	return createResp.ID, nil
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
	body, status, err := d.dockerCall(ctx, http.MethodGet, "/containers/"+id+"/json", nil)
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
		StartedAt:  parseDockerTime(info.State.StartedAt),
		FinishedAt: parseDockerTime(info.State.FinishedAt),
	}, nil
}

// removeContainer stops and force-removes a container by id or name. The force
// delete is attempted even when the stop fails, so a wedged or already-exited
// container is still reaped. Removing a missing container is not an error.
func (d *DockerDriver) removeContainer(ctx context.Context, idOrName string) error {
	if idOrName == "" {
		return nil
	}
	if _, status, err := d.dockerCall(ctx, http.MethodPost, "/containers/"+idOrName+"/stop", nil); err != nil {
		d.logger.Debug("dataproc docker: stop failed; forcing remove", "container", idOrName, "err", err)
	} else if status >= 300 && status != http.StatusNotFound {
		d.logger.Debug("dataproc docker: stop returned non-2xx", "status", status, "container", idOrName)
	}
	_, status, err := d.dockerCall(ctx, http.MethodDelete, "/containers/"+idOrName+"?force=true", nil)
	if err != nil {
		return err
	}
	if status >= 300 && status != http.StatusNotFound {
		return fmt.Errorf("docker delete %s: HTTP %d", idOrName, status)
	}
	return nil
}

// removeByFilter stops and removes every container matching the label filter,
// returning how many were reaped.
func (d *DockerDriver) removeByFilter(ctx context.Context, filter map[string][]string) (int, error) {
	rawFilter, err := json.Marshal(filter)
	if err != nil {
		return 0, err
	}
	body, status, err := d.dockerCall(ctx, http.MethodGet, "/containers/json?all=true&filters="+url.QueryEscape(string(rawFilter)), nil)
	if err != nil {
		return 0, err
	}
	if status >= 300 {
		return 0, fmt.Errorf("docker list: HTTP %d", status)
	}
	var items []struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(body, &items); err != nil {
		return 0, err
	}
	var reaped int
	for _, item := range items {
		if err := d.removeContainer(ctx, item.ID); err != nil {
			d.logger.Warn("dataproc docker: failed to reap container", "id", item.ID, "err", err)
			continue
		}
		reaped++
	}
	return reaped, nil
}

// dockerCall issues a Docker Engine API request over the configured socket.
func (d *DockerDriver) dockerCall(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker/"+dockerAPIVersion+path, reader)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return rb, resp.StatusCode, nil
}

// dockerStream issues a GET whose body is streamed rather than buffered (the
// container log endpoint).
func (d *DockerDriver) dockerStream(ctx context.Context, method, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://docker/"+dockerAPIVersion+path, nil)
	if err != nil {
		return nil, err
	}
	return d.client.Do(req)
}

// demuxDockerLogs decodes Docker's multiplexed (stdout/stderr) log stream. The
// driver container is created without a TTY, so the daemon frames stdout and
// stderr as [stream(1)][000][size(4, big-endian)][payload]; the payloads are
// concatenated in order into sink.
func demuxDockerLogs(r io.Reader, sink io.Writer) error {
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		size := binary.BigEndian.Uint32(header[4:8])
		if size == 0 {
			continue
		}
		if _, err := io.CopyN(sink, r, int64(size)); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
	}
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
	name := "jc-spark-" + dockerShortInstance(instanceID) + "-" + sanitizeJobID(jobID)
	if attempt > 0 {
		name += fmt.Sprintf("-r%d", attempt)
	}
	return name
}

func dockerShortInstance(instanceID string) string {
	if len(instanceID) >= 8 {
		return instanceID[:8]
	}
	if instanceID != "" {
		return instanceID
	}
	return "local"
}

func dockerShortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func dockerShortName(name string) string {
	if len(name) > 40 {
		return name[:40]
	}
	return name
}

// parseDockerTime parses a Docker RFC3339Nano timestamp, returning the zero time
// for the daemon's "never" sentinel (0001-01-01T00:00:00Z).
func parseDockerTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}
