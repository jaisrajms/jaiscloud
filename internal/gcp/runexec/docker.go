package runexec

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/docker"
	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
	"jaiscloud/internal/platform"
)

const (
	// dockerServiceValue scopes Cloud Run's containers to the run service in the
	// shared container label set.
	dockerServiceValue = "cloudrun"
	// dockerLabelRunSvc / dockerLabelRevision identify the owning service and
	// revision within the run label namespace.
	dockerLabelRunSvc   = "jaiscloud.io/run-service"
	dockerLabelRevision = "jaiscloud.io/run-revision"

	defaultDockerReadyTimeout = 180 * time.Second
	dockerPollInterval        = 500 * time.Millisecond
)

// DockerConfig configures a DockerManager.
type DockerConfig struct {
	Logger *slog.Logger
	// Platform carries the TLS PEM bundle / extra volumes / extra env applied to
	// every container; may be nil.
	Platform *platform.PlatformConfig
	// InstanceID scopes container names and labels so emulator instances sharing
	// one Docker daemon do not reap each other's containers.
	InstanceID string
	// Proxy is the HTTP client used for the upstream proxy call. Nil builds a
	// client with RequestTimeout as its total timeout.
	Proxy *http.Client
	// RequestTimeout bounds an upstream proxy call; defaults to 300s.
	RequestTimeout time.Duration
	// ReadyTimeout bounds the container-port readiness wait; defaults to 180s.
	ReadyTimeout time.Duration
	// Probe overrides the readiness TCP dial; for tests.
	Probe func(addr string) bool
	// Socket overrides the Docker API unix socket; defaults to
	// the shared default (/var/run/docker.sock).
	Socket string
	// Client overrides the Docker API client (tests). When nil, a client that
	// dials Socket is built.
	Client *http.Client
	// Network sets HostConfig.NetworkMode; empty uses the daemon default.
	Network string
	// Addressing is how the emulator reaches the revision containers it
	// launches: host (published loopback port) or container (by container name
	// on the shared network). The zero value is host mode. The startup wiring
	// resolves it with docker.ResolveAddressing and gates docker mode on
	// docker.Probe.
	Addressing docker.Addressing
}

// DockerManager implements run.RuntimeManager with one container per revision
// and a published loopback host port the proxy dials. It is safe for concurrent
// use. It is built on the shared internal/docker client, so its transport
// matches the Dataproc docker executor.
type DockerManager struct {
	docker       *docker.Client
	proxy        *http.Client
	readyTimeout time.Duration
	probe        func(string) bool
	platform     *platform.PlatformConfig
	instanceID   string
	network      string
	addr         docker.Addressing
	logger       *slog.Logger

	reg *registry

	sweepOnce sync.Once
}

// Ping reports whether the Docker daemon at socket answers the Engine API ping.
// An empty socket uses the default local socket. It lets startup fall back to
// the mock runtime when no daemon is reachable, rather than failing every
// service create.
func Ping(ctx context.Context, socket string) error { return docker.Ping(ctx, socket) }

// httpProbe considers a revision ready when its published port answers an HTTP
// request (any status). Cloud Run revisions are HTTP servers, and Docker's proxy
// binds the published port as soon as the container starts — so a bare TCP dial
// can succeed before the app serves traffic, while a connection-level failure on
// the HTTP request means it is not up yet.
func httpProbe(addr string) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/")
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return true
}

// DockerManager is the docker implementation of the Cloud Run runtime seam.
var _ runcore.RuntimeManager = (*DockerManager)(nil)

// NewDocker returns a docker-backed runtime manager.
func NewDocker(cfg DockerConfig) *DockerManager {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	timeout := cfg.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	proxy := cfg.Proxy
	if proxy == nil {
		proxy = &http.Client{Timeout: timeout}
	}
	probe := cfg.Probe
	if probe == nil {
		probe = httpProbe
	}
	ready := cfg.ReadyTimeout
	if ready == 0 {
		ready = defaultDockerReadyTimeout
	}
	return &DockerManager{
		docker:       docker.New(docker.Config{Logger: logger, Socket: cfg.Socket, Client: cfg.Client, Timeout: timeout}),
		proxy:        proxy,
		readyTimeout: ready,
		probe:        probe,
		platform:     cfg.Platform,
		instanceID:   cfg.InstanceID,
		network:      cfg.Network,
		addr:         cfg.Addressing,
		logger:       logger,
		reg:          newRegistry(),
	}
}

// EnsureRevision starts (or replaces) the revision's container and registers
// the service's invocation authority so data-plane requests resolve to it.
func (m *DockerManager) EnsureRevision(ctx context.Context, svc runstore.Service, rev runstore.Revision) error {
	// Reap containers left by a previous emulator instance before the first
	// revision of this process.
	m.sweepOnce.Do(m.sweepOrphans)

	image, command, args, env, port, err := containerTemplate(svc, rev)
	if err != nil {
		return model.NewProviderError("InvalidArgument", err.Error(), 400)
	}
	name := dockerContainerName(m.instanceID, svc.ProjectID, svc.Location, svc.ID, rev.ID)
	// Idempotent re-ensure: a prior attempt may have left the container behind.
	_ = m.docker.Remove(ctx, name)

	spec := dockerSpec{
		name:    name,
		image:   image,
		command: command,
		args:    args,
		env:     env,
		port:    port,
		memory:  memoryBytesFromResources(firstContainer(rev)["resources"]),
		labels: map[string]string{
			docker.LabelService: dockerServiceValue,
			dockerLabelRunSvc:   svc.ID,
			dockerLabelRevision: rev.ID,
		},
	}
	if m.instanceID != "" {
		spec.labels[docker.LabelInstance] = m.instanceID
	}
	id, hostPort, err := m.startContainer(ctx, spec)
	if err != nil {
		return fmt.Errorf("cloudrun: revision %s runtime: %w", rev.ID, err)
	}

	svcName := runcore.ServiceName(svc.ProjectID, svc.Location, svc.ID)
	host := normalizeHost(serviceHost(svc.ProjectID, svc.Location, svc.ID))
	m.reg.put(svcName, host, &target{
		serviceName: svcName,
		revision:    rev.ID,
		backend:     m.addr.URL(name, int(port), hostPort),
	})
	m.logger.Info("cloudrun: revision ready", "service", svcName, "revision", rev.ID, "container", docker.ShortID(id), "address", m.addr.HostPort(name, int(port), hostPort))
	return nil
}

// RemoveRevision tears down a revision's container. It is called on a
// template-changing update and on service delete.
func (m *DockerManager) RemoveRevision(ctx context.Context, rev runstore.Revision) error {
	name := dockerContainerName(m.instanceID, rev.ProjectID, rev.Location, rev.Service, rev.ID)
	err := m.docker.Remove(ctx, name)
	svcName := runcore.ServiceName(rev.ProjectID, rev.Location, rev.Service)
	host := normalizeHost(serviceHost(rev.ProjectID, rev.Location, rev.Service))
	m.reg.dropRevision(svcName, host, rev.ID)
	if err != nil {
		return fmt.Errorf("cloudrun: remove revision %s: %w", rev.ID, err)
	}
	return nil
}

// RemoveService tears down every container of a service and deregisters it.
func (m *DockerManager) RemoveService(ctx context.Context, svc runstore.Service) error {
	svcName := runcore.ServiceName(svc.ProjectID, svc.Location, svc.ID)
	n, err := m.docker.RemoveByFilter(ctx, m.serviceFilter(svc.ID))
	m.reg.dropService(svcName, normalizeHost(serviceHost(svc.ProjectID, svc.Location, svc.ID)))
	if err != nil {
		return fmt.Errorf("cloudrun: remove service %s: %w", svc.ID, err)
	}
	if n > 0 {
		m.logger.Info("cloudrun: removed service containers", "service", svcName, "count", n)
	}
	return nil
}

// Invoke forwards a data-plane request to the target service's latest ready
// revision and returns its raw HTTP response. Missing service → 404, no ready
// runtime → 503, dial failure → 502, timeout → 504.
func (m *DockerManager) Invoke(ctx context.Context, req runcore.InvocationRequest) (runcore.Invocation, error) {
	return proxyInvoke(ctx, m.proxy, m.reg, req)
}

// Reset tears down every revision container and clears the registry
// (/_jaiscloud/reset).
func (m *DockerManager) Reset(ctx context.Context) {
	m.reg.clear()
	if _, err := m.docker.RemoveByFilter(ctx, m.instanceFilter()); err != nil {
		m.logger.Warn("cloudrun: reset sweep incomplete", "err", err)
	}
}

// sweepOrphans reaps revision containers left by a previous emulator instance.
// Best-effort: a failure is logged and never blocks a revision start.
func (m *DockerManager) sweepOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), orphanSweepTimeout)
	defer cancel()
	if n, err := m.docker.RemoveByFilter(ctx, m.instanceFilter()); err != nil {
		m.logger.Warn("cloudrun: orphan sweep incomplete", "err", err)
	} else if n > 0 {
		m.logger.Info("cloudrun: reaped orphan revision containers", "count", n)
	}
}

// instanceFilter selects this instance's Cloud Run containers.
func (m *DockerManager) instanceFilter() map[string][]string {
	return m.labelFilter(nil)
}

// serviceFilter selects this instance's containers for one service.
func (m *DockerManager) serviceFilter(svcID string) map[string][]string {
	return m.labelFilter([]string{dockerLabelRunSvc + "=" + svcID})
}

// labelFilter builds a Docker label filter that always scopes to this emulator
// instance, so two emulators sharing a daemon never reap each other's containers.
func (m *DockerManager) labelFilter(extra []string) map[string][]string {
	return docker.LabelFilter(dockerServiceValue, m.instanceID, extra...)
}

// dockerSpec is the resolved input for starting one revision container.
type dockerSpec struct {
	name    string
	image   string
	command []string
	args    []string
	env     []envVar
	port    int32
	memory  int64
	labels  map[string]string
}

// startContainer creates and starts the container, then waits for its published
// host port to accept TCP. It returns the container id and the assigned port.
func (m *DockerManager) startContainer(ctx context.Context, spec dockerSpec) (string, int, error) {
	portKey := fmt.Sprintf("%d/tcp", spec.port)
	env := make([]string, 0, len(spec.env))
	for _, e := range spec.env {
		env = append(env, e.name+"="+e.value)
	}
	hostCfg := map[string]any{
		"AutoRemove": false,
	}
	// Host mode: an ephemeral host port bound to loopback, so revisions never
	// race for a fixed port and are not exposed beyond the host. Container mode:
	// nothing is published — the emulator reaches the revision by container name
	// on the shared network.
	if bindings := m.addr.PortBindings(int(spec.port), 0); bindings != nil {
		hostCfg["PortBindings"] = bindings
	}
	if spec.memory > 0 {
		hostCfg["Memory"] = spec.memory
	}
	if network := m.addressNetwork(); network != "" {
		hostCfg["NetworkMode"] = network
	}
	binds, platformEnv, pErr := docker.BindsAndEnv(m.platform)
	if pErr != nil {
		m.logger.Warn("cloudrun docker: platform apply failed", "err", pErr)
	}
	env = append(env, platformEnv...)
	if len(binds) > 0 {
		hostCfg["Binds"] = binds
	}
	createBody := map[string]any{
		"Image":        spec.image,
		"Env":          env,
		"Labels":       spec.labels,
		"ExposedPorts": map[string]any{portKey: map[string]any{}},
		"HostConfig":   hostCfg,
	}
	if len(spec.command) > 0 {
		createBody["Entrypoint"] = spec.command
	}
	if len(spec.args) > 0 {
		createBody["Cmd"] = spec.args
	}
	// Container mode registers the (DNS-safe) network alias so a container name
	// longer than a DNS label is still addressable by name.
	if nc := m.addr.NetworkingConfig(spec.name); nc != nil {
		createBody["NetworkingConfig"] = nc
	}
	body, err := json.Marshal(createBody)
	if err != nil {
		return "", 0, fmt.Errorf("docker create: %w", err)
	}

	id, err := m.docker.Create(ctx, spec.name, body)
	if err != nil {
		return "", 0, err
	}
	// From here the container exists: never leak it on a later failure.
	fail := func(err error) (string, int, error) {
		_ = m.docker.Remove(context.WithoutCancel(ctx), id)
		return "", 0, err
	}

	if _, status, err := m.docker.Call(ctx, http.MethodPost, "/containers/"+id+"/start", nil); err != nil {
		return fail(fmt.Errorf("docker start: %w", err))
	} else if status >= 300 {
		return fail(fmt.Errorf("docker start: HTTP %d", status))
	}

	hostPort, err := m.waitContainerPort(ctx, id, spec.name, spec.port)
	if err != nil {
		return fail(err)
	}
	return id, hostPort, nil
}

// addressNetwork is the NetworkMode a revision joins: the shared network when
// the emulator is containerized (so the revision is resolvable by name), else
// the configured network.
func (m *DockerManager) addressNetwork() string {
	if m.addr.Network != "" {
		return m.addr.Network
	}
	return m.network
}

// waitContainerPort polls the container until its listener accepts a request.
// Host mode waits for the daemon to assign a published host port and probes that
// on loopback; container mode probes the revision by name on the shared network.
func (m *DockerManager) waitContainerPort(ctx context.Context, id, name string, containerPort int32) (int, error) {
	deadline := clock.RealNow().Add(m.readyTimeout)
	portKey := fmt.Sprintf("%d/tcp", containerPort)
	for {
		hostPort := 0
		if !m.addr.Containerized() {
			if p, err := m.inspectHostPort(ctx, id, portKey); err == nil {
				hostPort = p
			} else {
				hostPort = 0
			}
		}
		if m.addr.Containerized() || hostPort > 0 {
			if m.probe(m.addr.HostPort(name, int(containerPort), hostPort)) {
				return hostPort, nil
			}
		}
		if clock.RealNow().After(deadline) {
			return 0, fmt.Errorf("cloudrun: container %s port %d not ready within %s", docker.ShortID(id), containerPort, m.readyTimeout)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(dockerPollInterval):
		}
	}
}

// inspectHostPort reads the host port Docker bound for a container port.
func (m *DockerManager) inspectHostPort(ctx context.Context, id, portKey string) (int, error) {
	body, status, err := m.docker.Call(ctx, http.MethodGet, "/containers/"+id+"/json", nil)
	if err != nil {
		return 0, err
	}
	if status >= 300 {
		return 0, fmt.Errorf("docker inspect: HTTP %d", status)
	}
	var info struct {
		NetworkSettings struct {
			Ports map[string][]struct {
				HostPort string `json:"HostPort"`
			} `json:"Ports"`
		} `json:"NetworkSettings"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return 0, err
	}
	for _, binding := range info.NetworkSettings.Ports[portKey] {
		if binding.HostPort == "" {
			continue
		}
		var p int
		if _, err := fmt.Sscanf(binding.HostPort, "%d", &p); err == nil && p > 0 {
			return p, nil
		}
	}
	return 0, nil
}

// dockerContainerName is the deterministic, instance-scoped container name for a
// revision. Instance scoping keeps two emulators on one daemon from colliding or
// cross-reaping.
func dockerContainerName(instanceID, project, location, service, revision string) string {
	return "jc-cloudrun-" + docker.ShortInstance(instanceID) + "-" + workloadNameFor(project, location, service, revision)
}
