package container

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/k8stypes"
	"jaiscloud/internal/platform"
)

// k8sReadyTimeout bounds EnsureWorkload's wait for a warm pod's endpoint.
const k8sReadyTimeout = 90 * time.Second

type warmPod struct {
	name     string // shared Pod + ClusterIP Service name
	endpoint string // http://<name>.<ns>.svc.cluster.local:<port>; empty = sentinel (being created)
	lastUsed time.Time
}

// K8sExecutor manages warm Pods per function using the profile's HTTP invocation
// contract. The Kubernetes control plane (Pod + ClusterIP Service create,
// readiness, delete, sweep) goes through internal/k8shelpers; only the
// invocation path (the HTTP call to the pod) uses a plain HTTP client.
type K8sExecutor struct {
	cfg        Config
	profile    Profile
	platform   *platform.PlatformConfig
	client     kubernetes.Interface // talks to the K8s API server (client-go)
	invoke     *http.Client         // talks to warm pods
	probe      func(string) bool    // endpoint readiness probe; nil = TCP dial (tests inject)
	mu         sync.Mutex
	pods       map[string]*warmPod // functionName -> warm pod
	done       chan struct{}
	wg         sync.WaitGroup
	codeLoader CodeLoader   // optional; nil in tests
	logsAPI    LogsIngestor // optional; nil in tests
}

// K8sExecutorOption customizes a K8sExecutor at construction.
type K8sExecutorOption func(*K8sExecutor)

// WithK8sClient injects the Kubernetes client. Without it, NewK8sExecutor
// builds one from the in-cluster config / JAISCLOUD_K8S_* environment.
func WithK8sClient(client kubernetes.Interface) K8sExecutorOption {
	return func(e *K8sExecutor) { e.client = client }
}

// WithWorkloadProbe overrides the endpoint readiness probe. It exists for
// kubernetes/fake-backed tests, which have no live endpoints to dial.
func WithWorkloadProbe(probe func(string) bool) K8sExecutorOption {
	return func(e *K8sExecutor) { e.probe = probe }
}

// SetCodeLoader injects the code loader for the code-fetch init container.
func (e *K8sExecutor) SetCodeLoader(l CodeLoader) { e.codeLoader = l }

// SetLogsAPI injects the workload log ingestor.
func (e *K8sExecutor) SetLogsAPI(l LogsIngestor) { e.logsAPI = l }

// SetProfile overrides the runtime profile.
func (e *K8sExecutor) SetProfile(p Profile) { e.profile = p }

// SetProfile overrides the runtime profile.
func (e *DockerExecutor) SetProfile(p Profile) { e.profile = p }

// NewK8sExecutor creates a K8sExecutor with warm-pod-per-function architecture.
// profile supplies the per-cloud runtime contract; plat may be nil. A Kubernetes
// client is built from the in-cluster config / JAISCLOUD_K8S_* environment
// unless WithK8sClient injects one.
func NewK8sExecutor(cfg Config, profile Profile, plat *platform.PlatformConfig, opts ...K8sExecutorOption) *K8sExecutor {
	if cfg.Namespace == "" {
		cfg.Namespace = "jaiscloud"
	}

	e := &K8sExecutor{
		cfg:      cfg,
		profile:  profile,
		platform: plat,
		invoke:   &http.Client{Timeout: dockerInvokeTimeout},
		pods:     make(map[string]*warmPod),
		done:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.client == nil {
		client, err := k8shelpers.NewClient()
		if err != nil {
			slog.Warn("executor k8s: cannot build Kubernetes client; k8s execution disabled", "err", err)
		} else {
			e.client = client
		}
	}
	e.cleanupOrphans()
	e.wg.Add(1)
	go e.gcLoop()
	return e
}

// Invoke obtains a warm pod for the function (creating one if needed) and sends
// the profile's invocation request.
func (e *K8sExecutor) Invoke(ctx context.Context, req Request) (Result, error) {
	pod, err := e.getOrCreate(ctx, req)
	if err != nil {
		return Result{}, fmt.Errorf("executor k8s: get/create pod: %w", err)
	}

	inv := e.profile.Invocation(req)
	url := pod.endpoint + inv.Path
	backoff := time.Second
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					go e.removePod(context.Background(), req.FunctionName)
				}
				return Result{}, ctx.Err()
			case <-time.After(backoff):
				if backoff < 4*time.Second {
					backoff += time.Second
				}
			}
		}

		httpReq, err := http.NewRequestWithContext(ctx, inv.Method, url, bytes.NewReader(inv.Body))
		if err != nil {
			return Result{}, fmt.Errorf("executor k8s: build request: %w", err)
		}
		for k, v := range inv.Header {
			httpReq.Header.Set(k, v)
		}

		resp, err := e.invoke.Do(httpReq)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				go e.removePod(context.Background(), req.FunctionName)
				return Result{}, ctx.Err()
			}
			slog.Warn("executor k8s: invoke attempt failed", "attempt", attempt+1, "err", err)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			slog.Warn("executor k8s: read response failed", "attempt", attempt+1, "err", readErr)
			continue
		}
		payload, derr := e.profile.DecodeResponse(resp.StatusCode, body)
		if derr != nil {
			e.removePod(ctx, req.FunctionName)
			return Result{}, fmt.Errorf("executor k8s: %w", derr)
		}

		e.mu.Lock()
		if p, ok := e.pods[req.FunctionName]; ok {
			p.lastUsed = clock.RealNow()
		}
		e.mu.Unlock()
		return Result{Payload: payload}, nil
	}

	e.removePod(ctx, req.FunctionName)
	return Result{}, fmt.Errorf("executor k8s: all invoke attempts failed for %s", req.FunctionName)
}

// DeleteFunction tears down the warm pod for the named function.
func (e *K8sExecutor) DeleteFunction(ctx context.Context, name string) {
	e.removePod(ctx, name)
}

// Reset destroys all warm pods (called on /_jaiscloud/reset).
// After clearing the in-memory map it performs a label-filtered sweep to catch
// any pods that are live on the cluster but missing from the map.
func (e *K8sExecutor) Reset(_ context.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	e.mu.Lock()
	names := make([]string, 0, len(e.pods))
	for name := range e.pods {
		names = append(names, name)
	}
	e.mu.Unlock()
	for _, name := range names {
		e.removePod(ctx, name)
	}

	if e.client == nil || e.cfg.InstanceID == "" {
		return
	}
	e.sweepOrphans(ctx)
}

// Close stops the GC goroutine and destroys all warm pods.
func (e *K8sExecutor) Close() error {
	close(e.done)
	e.wg.Wait()
	e.Reset(context.Background())
	return nil
}

// ─── internal ────────────────────────────────────────────────────────────────

func (e *K8sExecutor) getOrCreate(ctx context.Context, req Request) (*warmPod, error) {
	for {
		e.mu.Lock()
		if p, ok := e.pods[req.FunctionName]; ok {
			if p.endpoint != "" {
				e.mu.Unlock()
				return p, nil
			}
			// Sentinel present — another goroutine is creating the pod.
			e.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		// Insert sentinel.
		e.pods[req.FunctionName] = &warmPod{}
		e.mu.Unlock()

		pod, err := e.createPod(ctx, req)
		if err != nil {
			e.mu.Lock()
			if e.pods[req.FunctionName] != nil && e.pods[req.FunctionName].endpoint == "" {
				delete(e.pods, req.FunctionName)
			}
			e.mu.Unlock()
			return nil, err
		}

		e.mu.Lock()
		e.pods[req.FunctionName] = pod
		e.mu.Unlock()
		return pod, nil
	}
}

// codeArchiveEnvName is the env var the code-fetch init container reads the
// resolved archive URL from. The URL ends in a literal "$LATEST" qualifier;
// passing it through the environment (and quoting the reference) keeps the
// shell from expanding that token to an empty string, which would request
// `.../{key}/` and 404.
const codeArchiveEnvName = "JAISCLOUD_CODE_ARCHIVE_URL"

// applyCodeMount injects the source code-fetch init container into a pod spec
// when both a CodeLoader and a CodeURL base are configured. The init container
// downloads the archive over the admin HTTP API and unpacks it into a shared
// emptyDir mounted (read-only) at mountDir in the runtime container.
//
// The URL is {CodeURL}/lambda/code/{account}/{codeKey}/$LATEST; CodeURL must
// therefore be the admin base including the /_jaiscloud prefix. A nil loader or
// empty CodeURL is a no-op, leaving the mock/mounted-elsewhere behavior
// unchanged.
func applyCodeMount(spec *k8stypes.PodSpec, cfg Config, req Request, loader CodeLoader, mountDir string) {
	if loader == nil || cfg.CodeURL == "" || len(spec.Containers) == 0 || mountDir == "" {
		return
	}
	codeURL := fmt.Sprintf("%s/lambda/code/%s/%s/$LATEST", cfg.CodeURL, req.AccountID, codeKey(req))
	spec.Volumes = append(spec.Volumes, k8stypes.Volume{
		Name:     "code",
		EmptyDir: &k8stypes.EmptyDirVol{},
	})
	// Append rather than replace so any init containers the platform layer
	// already added (e.g. the TLS materializer) are preserved.
	spec.InitContainers = append(spec.InitContainers, k8stypes.Container{
		Name:         "code-fetch",
		Image:        cfg.InitImage,
		Command:      []string{"/bin/sh", "-c"},
		Args:         []string{`wget -qO /tmp/code.zip "$` + codeArchiveEnvName + `" && unzip /tmp/code.zip -d ` + mountDir},
		Env:          []k8stypes.EnvVar{{Name: codeArchiveEnvName, Value: codeURL}},
		VolumeMounts: []k8stypes.VolumeMount{{Name: "code", MountPath: mountDir}},
	})
	spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts,
		k8stypes.VolumeMount{Name: "code", MountPath: mountDir, ReadOnly: true},
	)
}

func (e *K8sExecutor) createPod(ctx context.Context, req Request) (*warmPod, error) {
	if e.client == nil {
		return nil, fmt.Errorf("executor k8s: no Kubernetes client configured")
	}
	ns := e.cfg.Namespace
	image := e.profile.ImageForRuntime(e.cfg, req)
	port := e.profile.InvocationPort()
	label := e.profile.WorkloadLabel()
	sanitized := sanitizePodName(req.FunctionName)
	pfx := e.profile.WorkloadNamePrefix(e.cfg.InstanceID)
	// k8shelpers uses one name for the Pod and its ClusterIP Service, and a
	// Service name is a DNS-1123 label (<=63 chars). Bound the function segment
	// so the unique "-<id>" suffix always fits; the function label keeps the
	// full sanitized name.
	idSuffix := shortID()
	base := sanitized
	if max := 63 - len(pfx) - len(idSuffix) - 1; len(base) > max {
		base = strings.TrimRight(base[:max], "-")
	}
	name := pfx + base + "-" + idSuffix

	env := K8sEnv(e.profile.RuntimeEnv(e.cfg, req))
	args := e.profile.ContainerArgs(req)

	memMB := req.MemoryMB
	if memMB < 128 {
		memMB = 128
	}

	podSpec := k8stypes.PodSpec{
		RestartPolicy:      "Never",
		ServiceAccountName: e.cfg.ServiceAccount,
		Containers: []k8stypes.Container{{
			Name:            "function",
			Image:           image,
			ImagePullPolicy: "IfNotPresent",
			Args:            args,
			Env:             env,
			Ports:           []k8stypes.ContainerPort{{ContainerPort: port}},
			Resources: &k8stypes.Resources{
				Requests: map[string]string{"cpu": "100m", "memory": "64Mi"},
				Limits:   map[string]string{"cpu": "1", "memory": fmt.Sprintf("%dMi", memMB)},
			},
			ReadinessProbe: &k8stypes.Probe{
				TCPSocket:           &k8stypes.TCPSocketAction{Port: port},
				InitialDelaySeconds: 1,
				PeriodSeconds:       1,
				FailureThreshold:    30,
			},
		}},
	}

	// Platform layer: TLS, generic volumes, env — applied before submission.
	if e.platform != nil {
		if err := platform.ApplyK8s(&podSpec, &podSpec.Containers[0], e.platform); err != nil {
			return nil, fmt.Errorf("platform apply: %w", err)
		}
	}

	// Code volume: inject an init container that fetches the archive and unpacks
	// it into the profile's code mount dir via a shared emptyDir.
	applyCodeMount(&podSpec, e.cfg, req, e.codeLoader, e.profile.CodeMountDir())

	coreSpec, err := coreV1PodSpec(podSpec)
	if err != nil {
		return nil, fmt.Errorf("convert pod spec: %w", err)
	}

	podLabels := map[string]string{
		"app":                      label,
		"function":                 sanitized,
		"jaiscloud.io/instance-id": e.cfg.InstanceID,
	}
	endpoint, err := k8shelpers.EnsureWorkload(ctx, e.client, k8shelpers.WorkloadSpec{
		Namespace: ns,
		Pod: corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: podLabels},
			Spec:       coreSpec,
		},
		ServiceLabels: podLabels,
		Selector: map[string]string{
			"function":                 sanitized,
			"jaiscloud.io/instance-id": e.cfg.InstanceID,
		},
		Ports:        []k8shelpers.ServicePort{{Name: "http", Port: int32(port), TargetPort: int32(port)}},
		ReadyTimeout: k8sReadyTimeout,
		Probe:        e.probe,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure workload: %w", err)
	}
	slog.Info("executor k8s: pod created", "pod", name, "function", req.FunctionName)

	return &warmPod{
		name:     name,
		endpoint: "http://" + endpoint,
		lastUsed: clock.RealNow(),
	}, nil
}

// coreV1PodSpec converts a k8stypes.PodSpec (built by the platform layer and
// the code-mount helper) into a client-go corev1.PodSpec. k8stypes mirrors the
// core/v1 wire format exactly (see the package doc), so a JSON round-trip is an
// exact conversion and avoids a second, drift-prone field-by-field mapping.
func coreV1PodSpec(spec k8stypes.PodSpec) (corev1.PodSpec, error) {
	raw, err := json.Marshal(spec)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	var out corev1.PodSpec
	if err := json.Unmarshal(raw, &out); err != nil {
		return corev1.PodSpec{}, err
	}
	return out, nil
}

func (e *K8sExecutor) removePod(ctx context.Context, functionName string) {
	e.mu.Lock()
	pod, ok := e.pods[functionName]
	if ok {
		delete(e.pods, functionName)
	}
	e.mu.Unlock()
	if !ok || pod.name == "" || e.client == nil {
		return
	}
	if err := k8shelpers.DeleteWorkload(ctx, e.client, e.cfg.Namespace, pod.name); err != nil {
		slog.Warn("executor k8s: remove pod/service failed", "function", functionName, "err", err)
		return
	}
	slog.Info("executor k8s: removed pod and service", "function", functionName)
}

func (e *K8sExecutor) gcLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			e.gcOnce()
		}
	}
}

func (e *K8sExecutor) gcOnce() {
	keepalive := time.Duration(e.cfg.KeepaliveSecs) * time.Second
	if keepalive == 0 {
		keepalive = 300 * time.Second
	}
	now := clock.RealNow()
	e.mu.Lock()
	var toRemove []string
	for name, p := range e.pods {
		if p.endpoint != "" && now.Sub(p.lastUsed) > keepalive {
			toRemove = append(toRemove, name)
		}
	}
	e.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, name := range toRemove {
		slog.Info("executor k8s: GC idle pod", "function", name)
		e.removePod(ctx, name)
	}
}

// orphanSelector matches every workload this deployment owns, across functions.
func (e *K8sExecutor) orphanSelector() string {
	return fmt.Sprintf("app=%s,jaiscloud.io/instance-id=%s", e.profile.WorkloadLabel(), e.cfg.InstanceID)
}

// cleanupOrphans deletes pods and services from previous runs on startup.
// Only resources labeled with this instance's ID are touched.
func (e *K8sExecutor) cleanupOrphans() {
	if e.client == nil {
		return
	}
	n, err := k8shelpers.SweepWorkloads(context.Background(), e.client, e.cfg.Namespace, e.orphanSelector())
	if err != nil {
		slog.Warn("executor k8s: cleanupOrphans sweep failed", "err", err)
	}
	if n > 0 {
		slog.Info("executor k8s: cleaned up orphans", "count", n)
	}
}

// sweepOrphans is best-effort cleanup of this instance's untracked workloads.
func (e *K8sExecutor) sweepOrphans(ctx context.Context) {
	if e.client == nil {
		return
	}
	if _, err := k8shelpers.SweepWorkloads(ctx, e.client, e.cfg.Namespace, e.orphanSelector()); err != nil {
		slog.Warn("executor k8s: reset sweep failed", "err", err)
	}
}

// sanitizePodName converts a function name to a valid K8s name segment.
func sanitizePodName(name string) string {
	var b strings.Builder
	for _, ch := range strings.ToLower(name) {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') {
			b.WriteRune(ch)
		} else {
			b.WriteByte('-')
		}
	}
	s := strings.Trim(b.String(), "-")
	if len(s) > 40 {
		s = s[:40]
	}
	return s
}
