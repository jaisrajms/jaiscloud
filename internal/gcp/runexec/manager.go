// Package runexec implements the Cloud Run runtime managers behind the
// transport-neutral core's RuntimeManager seam. The k8s Manager launches each
// revision's template image as a single-replica Pod plus a ClusterIP Service
// (via internal/k8shelpers), waits for the in-cluster endpoint, and
// reverse-proxies data-plane HTTP requests to it. The docker DockerManager does
// the same with one container per revision and a published loopback host port.
// Unlike the Lambda executor, Cloud Run revisions are always-on: there is no
// idle/keepalive reaper — a revision runtime is torn down only on service
// delete, template-changing update, /_jaiscloud/reset, or the startup orphan
// sweep. Routing and proxying are shared (proxy.go) so the orchestrators cannot
// drift.
package runexec

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/model"
)

const (
	defaultNamespace      = "jaiscloud"
	defaultRequestTimeout = 300 * time.Second
	orphanSweepTimeout    = 30 * time.Second

	labelApp      = "app"
	labelAppValue = "jaiscloud-cloudrun"
	labelInstance = "jaiscloud.io/instance-id"
	labelService  = "jaiscloud.io/run-service"
	labelRevision = "jaiscloud.io/run-revision"

	envPort          = "PORT"
	envService       = "K_SERVICE"
	envRevision      = "K_REVISION"
	envConfiguration = "K_CONFIGURATION"
)

// Config configures a Manager.
type Config struct {
	// Client is the in-cluster Kubernetes client (required).
	Client kubernetes.Interface
	// Namespace is where revision workloads run; defaults to "jaiscloud".
	Namespace string
	Logger    *slog.Logger
	// Proxy is the HTTP client used for the upstream proxy call. Nil builds a
	// client with RequestTimeout as its total timeout.
	Proxy *http.Client
	// RequestTimeout bounds an upstream proxy call; defaults to 300s.
	RequestTimeout time.Duration
	// ReadyTimeout bounds the k8s readiness wait; defaults to the helper's 180s.
	ReadyTimeout time.Duration
	// Probe overrides the readiness TCP dial; for tests.
	Probe func(addr string) bool
}

// Manager implements run.RuntimeManager on a Kubernetes cluster. It is safe for
// concurrent use.
type Manager struct {
	client       kubernetes.Interface
	namespace    string
	logger       *slog.Logger
	proxy        *http.Client
	readyTimeout time.Duration
	probe        func(string) bool

	reg *registry

	sweepOnce sync.Once
}

// Manager is the k8s implementation of the Cloud Run runtime seam.
var _ runcore.RuntimeManager = (*Manager)(nil)

// New returns a k8s-backed runtime manager.
func New(cfg Config) *Manager {
	ns := cfg.Namespace
	if ns == "" {
		ns = defaultNamespace
	}
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
		probe = tcpProbe
	}
	return &Manager{
		client:       cfg.Client,
		namespace:    ns,
		logger:       logger,
		proxy:        proxy,
		readyTimeout: cfg.ReadyTimeout,
		probe:        probe,
		reg:          newRegistry(),
	}
}

// EnsureRevision starts (or reuses) the revision's Pod + Service and registers
// the service's invocation authority so data-plane requests resolve to it.
func (m *Manager) EnsureRevision(ctx context.Context, svc runstore.Service, rev runstore.Revision) error {
	// Reap workloads left by a previous emulator instance before the first
	// revision of this process. Broker/revision liveness is runtime state this
	// process cannot adopt across deterministic names, so every labeled
	// resource is an orphan.
	m.sweepOnce.Do(m.sweepOrphans)

	name := workloadName(svc, rev)
	pod, port, err := buildPod(svc, rev, name, m.namespace)
	if err != nil {
		return model.NewProviderError("InvalidArgument", err.Error(), 400)
	}
	endpoint, err := k8shelpers.EnsureWorkload(ctx, m.client, k8shelpers.WorkloadSpec{
		Namespace: m.namespace,
		Pod:       *pod,
		ServiceLabels: map[string]string{
			labelApp:      labelAppValue,
			labelService:  svc.ID,
			labelRevision: rev.ID,
		},
		Selector:     map[string]string{labelApp: labelAppValue, labelService: svc.ID},
		Ports:        []k8shelpers.ServicePort{{Name: "http", Port: port}},
		ReadyTimeout: m.readyTimeout,
		Probe:        m.probe,
	})
	if err != nil {
		return fmt.Errorf("cloudrun: revision %s runtime: %w", rev.ID, err)
	}

	svcName := runcore.ServiceName(svc.ProjectID, svc.Location, svc.ID)
	host := normalizeHost(serviceHost(svc.ProjectID, svc.Location, svc.ID))
	m.reg.put(svcName, host, &target{
		serviceName: svcName,
		revision:    rev.ID,
		backend:     "http://" + endpoint,
	})
	m.logger.Info("cloudrun: revision ready", "service", svcName, "revision", rev.ID, "endpoint", endpoint)
	return nil
}

// RemoveRevision tears down a revision's runtime. It is called on a
// template-changing update and on service delete.
func (m *Manager) RemoveRevision(ctx context.Context, rev runstore.Revision) error {
	err := k8shelpers.DeleteWorkload(ctx, m.client, m.namespace, workloadNameFor(rev.ProjectID, rev.Location, rev.Service, rev.ID))
	svcName := runcore.ServiceName(rev.ProjectID, rev.Location, rev.Service)
	host := normalizeHost(serviceHost(rev.ProjectID, rev.Location, rev.Service))
	m.reg.dropRevision(svcName, host, rev.ID)
	if err != nil {
		return fmt.Errorf("cloudrun: remove revision %s: %w", rev.ID, err)
	}
	return nil
}

// RemoveService tears down every runtime of a service and deregisters it.
func (m *Manager) RemoveService(ctx context.Context, svc runstore.Service) error {
	svcName := runcore.ServiceName(svc.ProjectID, svc.Location, svc.ID)
	n, err := k8shelpers.SweepWorkloads(ctx, m.client, m.namespace,
		fmt.Sprintf("%s=%s,%s=%s", labelApp, labelAppValue, labelService, svc.ID))
	m.reg.dropService(svcName, normalizeHost(serviceHost(svc.ProjectID, svc.Location, svc.ID)))
	if err != nil {
		return fmt.Errorf("cloudrun: remove service %s: %w", svc.ID, err)
	}
	if n > 0 {
		m.logger.Info("cloudrun: removed service workloads", "service", svcName, "count", n)
	}
	return nil
}

// Invoke forwards a data-plane request to the target service's latest ready
// revision and returns its raw HTTP response. Missing service → 404, no ready
// runtime → 503, dial failure → 502, timeout → 504.
func (m *Manager) Invoke(ctx context.Context, req runcore.InvocationRequest) (runcore.Invocation, error) {
	return proxyInvoke(ctx, m.proxy, m.reg, req)
}

// Reset tears down every revision runtime and clears the registry
// (/_jaiscloud/reset).
func (m *Manager) Reset(ctx context.Context) {
	m.reg.clear()
	if n, err := k8shelpers.SweepWorkloads(ctx, m.client, m.namespace, labelApp+"="+labelAppValue); err != nil {
		m.logger.Warn("cloudrun: reset sweep incomplete", "deleted", n, "err", err)
	}
}

// sweepOrphans reaps revision workloads left by a previous emulator instance.
// Best-effort: a failure is logged and never blocks a revision start.
func (m *Manager) sweepOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), orphanSweepTimeout)
	defer cancel()
	if n, err := k8shelpers.SweepWorkloads(ctx, m.client, m.namespace, labelApp+"="+labelAppValue); err != nil {
		m.logger.Warn("cloudrun: orphan sweep incomplete", "deleted", n, "err", err)
	} else if n > 0 {
		m.logger.Info("cloudrun: reaped orphan revision workloads", "count", n)
	}
}
