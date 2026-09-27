package lambda

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/k8stypes"
	"jaiscloud/internal/platform"
)

const (
	riePort  = 8080
	riePath  = "/2015-03-31/functions/function/invocations"
	labelApp = "jaiscloud-lambda"
)

// instancePrefix returns the instance-scoped pod/service prefix.
// Format: jc-lambda-<instanceID[:8]>-
// Falls back to "jc-lambda-" when InstanceID is empty (dev/test mode).
func instancePrefix(instanceID string) string {
	if len(instanceID) >= 8 {
		return "jc-lambda-" + instanceID[:8] + "-"
	}
	if instanceID != "" {
		return "jc-lambda-" + instanceID + "-"
	}
	return "jc-lambda-"
}

// encodeLabelSelector builds a K8s label selector query param value by
// escaping "=" as %3D and "/" as %2F. Commas are preserved (K8s uses them
// to separate requirements). Never pass the result through url.QueryEscape.
func encodeLabelSelector(raw string) string {
	s := strings.ReplaceAll(raw, "=", "%3D")
	return strings.ReplaceAll(s, "/", "%2F")
}

type warmPod struct {
	podName  string
	svcName  string
	endpoint string // http://<svc>.<ns>:8080; empty = sentinel (being created)
	lastUsed time.Time
}

// K8sExecutor manages warm Pods per Lambda function using the Lambda RIE HTTP protocol.
type K8sExecutor struct {
	cfg        LambdaConfig
	platform   *platform.PlatformConfig
	k8s        *http.Client // talks to K8s API server (30s timeout, custom TLS)
	invoke     *http.Client // talks to Lambda RIE pods (5min timeout)
	token      string
	mu         sync.Mutex
	pods       map[string]*warmPod // functionName -> warm pod
	done       chan struct{}
	wg         sync.WaitGroup
	codeLoader CodeLoader   // optional; nil in tests
	logsAPI    LogsIngestor // optional; nil in tests
}

// SetCodeLoader injects the code loader for /var/task init-container mounting.
func (e *K8sExecutor) SetCodeLoader(l CodeLoader) { e.codeLoader = l }

// SetLogsAPI injects the CloudWatch Logs ingestor for pod log streaming.
func (e *K8sExecutor) SetLogsAPI(l LogsIngestor) { e.logsAPI = l }

// NewK8sExecutor creates a K8sExecutor with warm-pod-per-function architecture.
// plat may be nil.
func NewK8sExecutor(cfg LambdaConfig, plat *platform.PlatformConfig) *K8sExecutor {
	// Read bearer token.
	token := os.Getenv("JAISCLOUD_K8S_TOKEN")
	if token == "" {
		if f := os.Getenv("JAISCLOUD_K8S_TOKEN_FILE"); f != "" {
			b, _ := os.ReadFile(f)
			token = strings.TrimSpace(string(b))
		}
	}
	if token == "" {
		b, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
		token = strings.TrimSpace(string(b))
	}

	if cfg.APIServer == "" {
		cfg.APIServer = "https://kubernetes.default.svc"
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "jaiscloud"
	}

	// Build TLS config.
	tlsCfg := &tls.Config{}
	caFile := os.Getenv("JAISCLOUD_K8S_CA_FILE")
	if caFile == "" {
		caFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	if caBytes, err := os.ReadFile(caFile); err == nil {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caBytes)
		tlsCfg.RootCAs = pool
	} else {
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // dev fallback when no CA
	}
	if certFile := os.Getenv("JAISCLOUD_K8S_CLIENT_CERT_FILE"); certFile != "" {
		keyFile := os.Getenv("JAISCLOUD_K8S_CLIENT_KEY_FILE")
		if cert, err := tls.LoadX509KeyPair(certFile, keyFile); err == nil {
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
	}

	k8sClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
	}
	invokeClient := &http.Client{Timeout: 16 * time.Minute}

	e := &K8sExecutor{
		cfg:      cfg,
		platform: plat,
		k8s:      k8sClient,
		invoke:   invokeClient,
		token:    token,
		pods:     make(map[string]*warmPod),
		done:     make(chan struct{}),
	}
	e.cleanupOrphans()
	e.wg.Add(1)
	go e.gcLoop()
	return e
}

// Invoke obtains a warm pod for the function (creating one if needed) and
// POSTs the payload via the Lambda RIE HTTP protocol.
func (e *K8sExecutor) Invoke(ctx context.Context, req InvokeRequest) (InvokeResult, error) {
	pod, err := e.getOrCreate(ctx, req)
	if err != nil {
		return InvokeResult{}, fmt.Errorf("lambda k8s: get/create pod: %w", err)
	}

	url := pod.endpoint + riePath
	var payload []byte
	backoff := time.Second
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					go e.removePod(context.Background(), req.FunctionName)
				}
				return InvokeResult{}, ctx.Err()
			case <-time.After(backoff):
				if backoff < 4*time.Second {
					backoff += time.Second
				}
			}
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(req.Payload))
		if err != nil {
			return InvokeResult{}, fmt.Errorf("lambda k8s: build request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := e.invoke.Do(httpReq)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				go e.removePod(context.Background(), req.FunctionName)
				return InvokeResult{}, ctx.Err()
			}
			slog.Warn("lambda k8s: invoke attempt failed", "attempt", attempt+1, "err", err)
			continue
		}
		payload, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			slog.Warn("lambda k8s: read response failed", "attempt", attempt+1, "err", err)
			continue
		}

		e.mu.Lock()
		if p, ok := e.pods[req.FunctionName]; ok {
			p.lastUsed = clock.RealNow()
		}
		e.mu.Unlock()
		return InvokeResult{Payload: payload}, nil
	}

	e.removePod(ctx, req.FunctionName)
	return InvokeResult{}, fmt.Errorf("lambda k8s: all invoke attempts failed for %s", req.FunctionName)
}

// DeleteFunction tears down the warm pod for the named function.
func (e *K8sExecutor) DeleteFunction(ctx context.Context, name string) {
	e.removePod(ctx, name)
}

// Reset destroys all warm pods (called on /_jaiscloud/reset).
// After clearing the in-memory map it performs a label-filtered sweep to
// catch any pods that are live on the cluster but missing from the map (LG5).
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

	if e.k8s == nil || e.cfg.InstanceID == "" {
		return
	}
	ns := e.cfg.Namespace
	rawSel := fmt.Sprintf("app=%s,jaiscloud.io/instance-id=%s", labelApp, e.cfg.InstanceID)
	sel := encodeLabelSelector(rawSel)
	e.sweepResources(ns, "pods", sel)
	e.sweepResources(ns, "services", sel)
}

// Close stops the GC goroutine and destroys all warm pods.
func (e *K8sExecutor) Close() error {
	close(e.done)
	e.wg.Wait()
	e.Reset(context.Background())
	return nil
}

// ─── internal ────────────────────────────────────────────────────────────────

func (e *K8sExecutor) getOrCreate(ctx context.Context, req InvokeRequest) (*warmPod, error) {
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
const codeArchiveEnvName = "JAISCLOUD_LAMBDA_CODE_ARCHIVE_URL"

// applyCodeMount injects the source code-fetch init container into a pod spec
// when both a CodeLoader and a CodeURL base are configured. The init container
// downloads the archive over the admin HTTP API and unpacks it into a shared
// emptyDir mounted (read-only) at /var/task in the runtime container.
//
// The URL is {CodeURL}/lambda/code/{account}/{codeKey}/$LATEST; CodeURL must
// therefore be the admin base including the /_jaiscloud prefix (see
// LambdaConfig.CodeURL). A nil loader or empty CodeURL is a no-op, leaving the
// mock/mounted-elsewhere behavior unchanged.
func applyCodeMount(spec *k8stypes.PodSpec, cfg LambdaConfig, req InvokeRequest, loader CodeLoader) {
	if loader == nil || cfg.CodeURL == "" || len(spec.Containers) == 0 {
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
		Args:         []string{`wget -qO /tmp/code.zip "$` + codeArchiveEnvName + `" && unzip /tmp/code.zip -d /var/task`},
		Env:          []k8stypes.EnvVar{{Name: codeArchiveEnvName, Value: codeURL}},
		VolumeMounts: []k8stypes.VolumeMount{{Name: "code", MountPath: "/var/task"}},
	})
	spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts,
		k8stypes.VolumeMount{Name: "code", MountPath: "/var/task", ReadOnly: true},
	)
}

func (e *K8sExecutor) createPod(ctx context.Context, req InvokeRequest) (*warmPod, error) {
	ns := e.cfg.Namespace
	image := ImageForRuntime(req, e.cfg)
	sanitized := sanitizePodName(req.FunctionName)
	pfx := instancePrefix(e.cfg.InstanceID)
	podName := pfx + sanitized + "-" + shortID()
	svcName := pfx + sanitized

	env := k8sRuntimeEnv(e.cfg, req)

	var args []string
	if req.Handler != "" {
		args = []string{req.Handler}
	}

	memMB := req.MemoryMB
	if memMB < 128 {
		memMB = 128
	}

	podSpec := k8stypes.PodSpec{
		RestartPolicy: "Never",
		Containers: []k8stypes.Container{{
			Name:            "lambda",
			Image:           image,
			ImagePullPolicy: "IfNotPresent",
			Args:            args,
			Env:             env,
			Ports:           []k8stypes.ContainerPort{{ContainerPort: riePort}},
			Resources: &k8stypes.Resources{
				Requests: map[string]string{"cpu": "100m", "memory": "64Mi"},
				Limits:   map[string]string{"cpu": "1", "memory": fmt.Sprintf("%dMi", memMB)},
			},
			ReadinessProbe: &k8stypes.Probe{
				TCPSocket:           &k8stypes.TCPSocketAction{Port: riePort},
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

	// Code volume: inject an init container that fetches the zip and unpacks it
	// into /var/task via a shared emptyDir when a code URL base is configured.
	applyCodeMount(&podSpec, e.cfg, req, e.codeLoader)

	podLabels := map[string]string{
		"app":                      labelApp,
		"function":                 sanitized,
		"jaiscloud.io/instance-id": e.cfg.InstanceID,
	}
	podManifest := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      podName,
			"namespace": ns,
			"labels":    podLabels,
		},
		"spec": podSpec,
	}

	podBody, _ := json.Marshal(podManifest)
	podURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods", e.cfg.APIServer, ns)
	if err := e.k8sPost(ctx, podURL, podBody); err != nil {
		return nil, fmt.Errorf("create pod: %w", err)
	}
	slog.Info("lambda k8s: pod created", "pod", podName, "function", req.FunctionName)

	// Create ClusterIP service (idempotent — ignore errors if already exists).
	type svcPort struct {
		Port       int `json:"port"`
		TargetPort int `json:"targetPort"`
	}
	svcLabels := map[string]string{
		"app":                      labelApp,
		"function":                 sanitized,
		"jaiscloud.io/instance-id": e.cfg.InstanceID,
	}
	svcManifest := map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      svcName,
			"namespace": ns,
			"labels":    svcLabels,
		},
		"spec": map[string]any{
			"type": "ClusterIP",
			"selector": map[string]string{
				"function":                 sanitized,
				"jaiscloud.io/instance-id": e.cfg.InstanceID,
			},
			"ports": []svcPort{{Port: riePort, TargetPort: riePort}},
		},
	}
	svcBody, _ := json.Marshal(svcManifest)
	svcURL := fmt.Sprintf("%s/api/v1/namespaces/%s/services", e.cfg.APIServer, ns)
	e.k8sPost(ctx, svcURL, svcBody) //nolint:errcheck // may already exist

	if err := e.waitReady(ctx, ns, podName); err != nil {
		return nil, fmt.Errorf("pod not ready: %w", err)
	}

	endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", svcName, ns, riePort)
	return &warmPod{
		podName:  podName,
		svcName:  svcName,
		endpoint: endpoint,
		lastUsed: clock.RealNow(),
	}, nil
}

func (e *K8sExecutor) waitReady(ctx context.Context, ns, podName string) error {
	deadline := clock.RealNow().Add(90 * time.Second)
	for {
		if clock.RealNow().After(deadline) {
			return fmt.Errorf("timed out waiting for pod %s to be ready", podName)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}

		url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", e.cfg.APIServer, ns, podName)
		body, status, err := e.k8sGet(ctx, url)
		if err != nil || status != 200 {
			continue
		}
		var pod struct {
			Status struct {
				Phase      string `json:"phase"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			} `json:"status"`
		}
		if json.Unmarshal(body, &pod) != nil {
			continue
		}
		if pod.Status.Phase != "Running" {
			continue
		}
		for _, c := range pod.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				return nil
			}
		}
	}
}

func (e *K8sExecutor) removePod(ctx context.Context, functionName string) {
	e.mu.Lock()
	pod, ok := e.pods[functionName]
	if ok {
		delete(e.pods, functionName)
	}
	e.mu.Unlock()
	if !ok || pod.podName == "" {
		return
	}

	podURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", e.cfg.APIServer, e.cfg.Namespace, pod.podName)
	e.k8sDelete(ctx, podURL)

	svcURL := fmt.Sprintf("%s/api/v1/namespaces/%s/services/%s", e.cfg.APIServer, e.cfg.Namespace, pod.svcName)
	e.k8sDelete(ctx, svcURL)
	slog.Info("lambda k8s: removed pod and service", "function", functionName)
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
		slog.Info("lambda k8s: GC idle pod", "function", name)
		e.removePod(ctx, name)
	}
}

// cleanupOrphans deletes pods and services from previous runs on startup.
// Only resources labeled with this instance's ID are touched (LG1 / LR2).
func (e *K8sExecutor) cleanupOrphans() {
	ns := e.cfg.Namespace
	rawSel := fmt.Sprintf("app=%s,jaiscloud.io/instance-id=%s", labelApp, e.cfg.InstanceID)
	sel := encodeLabelSelector(rawSel)

	e.sweepResources(ns, "pods", sel)
	e.sweepResources(ns, "services", sel)
}

// sweepResources lists and deletes all K8s resources of the given kind matching
// the label selector. Paginates via metadata.continue (LG3).
func (e *K8sExecutor) sweepResources(ns, kind, encodedSelector string) {
	const maxItems = 10_000
	continueToken := ""
	deleted := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		baseURL := fmt.Sprintf("%s/api/v1/namespaces/%s/%s?labelSelector=%s&limit=500",
			e.cfg.APIServer, ns, kind, encodedSelector)
		if continueToken != "" {
			baseURL += "&continue=" + continueToken
		}
		body, status, err := e.k8sGet(ctx, baseURL)
		cancel()
		if err != nil || status != 200 {
			slog.Warn("lambda k8s: cleanupOrphans list failed", "kind", kind, "status", status, "err", err)
			return
		}
		var list struct {
			Metadata struct {
				Continue string `json:"continue"`
			} `json:"metadata"`
			Items []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
			} `json:"items"`
		}
		if json.Unmarshal(body, &list) != nil {
			return
		}
		for _, item := range list.Items {
			opCtx, opCancel := context.WithTimeout(context.Background(), 10*time.Second)
			url := fmt.Sprintf("%s/api/v1/namespaces/%s/%s/%s", e.cfg.APIServer, ns, kind, item.Metadata.Name)
			e.k8sDelete(opCtx, url)
			opCancel()
			slog.Info("lambda k8s: cleaned up orphan", "kind", kind, "name", item.Metadata.Name)
			deleted++
		}
		if deleted >= maxItems {
			slog.Warn("lambda k8s: cleanupOrphans safety cap reached", "kind", kind, "count", deleted)
			return
		}
		if list.Metadata.Continue == "" {
			return
		}
		continueToken = list.Metadata.Continue
	}
}

// ─── K8s API helpers ─────────────────────────────────────────────────────────

func (e *K8sExecutor) k8sPost(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}
	resp, err := e.k8s.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		rb, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, rb)
	}
	return nil
}

func (e *K8sExecutor) k8sGet(ctx context.Context, url string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}
	resp, err := e.k8s.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode, nil
}

func (e *K8sExecutor) k8sDelete(ctx context.Context, url string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return
	}
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}
	resp, err := e.k8s.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
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

func shortID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%08x", clock.RealNow().UnixNano()&0xffffffff)
	}
	return hex.EncodeToString(b)
}
