package container

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"jaiscloud/internal/docker"
	"jaiscloud/internal/platform"
)

const dockerInvokeTimeout = 16 * time.Minute

// warmContainer holds the state of a running Docker container for one function.
type warmContainer struct {
	id       string
	hostPort int
	lastUsed time.Time
	codeDir  string // temp dir holding extracted function code; empty = not mounted
	optDir   string // temp dir holding extracted layers; empty = no layers mounted
}

// DockerExecutor manages warm Docker containers per function. Each distinct
// function name gets one container reused across invocations until it has been
// idle for cfg.KeepaliveSecs seconds.
type DockerExecutor struct {
	cfg          Config
	profile      Profile
	platform     *platform.PlatformConfig
	docker       *docker.Client // shared Docker Engine API client (local socket)
	invokeClient *http.Client   // talks to the container's published port over TCP
	mu           sync.Mutex
	containers   map[string]*warmContainer // functionName → container
	nextPort     int
	done         chan struct{}
	wg           sync.WaitGroup
	codeLoader   CodeLoader      // optional; nil in tests
	layerLoader  LayerBlobLoader // optional; nil = no layer mounting
	logsAPI      LogsIngestor    // optional; nil in tests
}

// SetCodeLoader injects the code loader used to mount the function archive.
func (e *DockerExecutor) SetCodeLoader(l CodeLoader) { e.codeLoader = l }

// SetLayerBlobLoader injects the layer blob loader used to mount layers.
func (e *DockerExecutor) SetLayerBlobLoader(l LayerBlobLoader) { e.layerLoader = l }

// SetLogsAPI injects the workload log ingestor. The warm-container path tails
// Docker logs directly; this is retained for callers that wire a log sink.
func (e *DockerExecutor) SetLogsAPI(l LogsIngestor) { e.logsAPI = l }

// NewDockerExecutor creates a DockerExecutor and starts the GC goroutine.
// profile supplies the per-cloud runtime contract; plat may be nil.
func NewDockerExecutor(cfg Config, profile Profile, plat *platform.PlatformConfig) *DockerExecutor {
	e := &DockerExecutor{
		cfg:        cfg,
		profile:    profile,
		platform:   plat,
		docker:     docker.New(docker.Config{}),
		containers: make(map[string]*warmContainer),
		nextPort:   9100,
		done:       make(chan struct{}),
		// The invoke request goes to the container's published port over TCP,
		// NOT through the Docker API socket the `docker` client dials.
		invokeClient: &http.Client{Timeout: dockerInvokeTimeout},
	}
	e.cleanupOrphans()
	e.wg.Add(1)
	go e.gcLoop()
	return e
}

// Invoke obtains a warm container for the function (starting one if needed),
// then sends the profile's invocation request and returns the result (including
// log tail when LogType="Tail").
func (e *DockerExecutor) Invoke(ctx context.Context, req Request) (Result, error) {
	c, err := e.getOrStart(ctx, req)
	if err != nil {
		return Result{}, fmt.Errorf("executor docker: start container: %w", err)
	}

	inv := e.profile.Invocation(req)
	url := fmt.Sprintf("http://localhost:%d%s", c.hostPort, inv.Path)
	httpReq, err := http.NewRequestWithContext(ctx, inv.Method, url, bytes.NewReader(inv.Body))
	if err != nil {
		return Result{}, fmt.Errorf("executor docker: build request: %w", err)
	}
	for k, v := range inv.Header {
		httpReq.Header.Set(k, v)
	}

	resp, err := e.invokeClient.Do(httpReq)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
			e.removeContainer(req.FunctionName)
			return Result{}, ctx.Err()
		default:
			return Result{}, fmt.Errorf("executor docker: invoke: %w", err)
		}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{}, fmt.Errorf("executor docker: read response: %w", err)
	}
	payload, derr := e.profile.DecodeResponse(resp.StatusCode, body)
	if derr != nil {
		e.removeContainer(req.FunctionName)
		return Result{}, fmt.Errorf("executor docker: %w", derr)
	}

	e.mu.Lock()
	if c, ok := e.containers[req.FunctionName]; ok {
		c.lastUsed = clock.RealNow()
	}
	e.mu.Unlock()

	result := Result{Payload: payload}

	// When LogType=Tail, fetch the last 50 log lines of container stdout+stderr.
	if strings.EqualFold(req.LogType, "Tail") {
		e.mu.Lock()
		ctr, ok := e.containers[req.FunctionName]
		e.mu.Unlock()
		if ok && ctr.id != "" {
			logCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			logBody, logStatus, logErr := e.docker.Call(logCtx, http.MethodGet,
				fmt.Sprintf("/containers/%s/logs?stdout=true&stderr=true&tail=50", ctr.id), nil)
			if logErr == nil && logStatus < 300 {
				var buf bytes.Buffer
				if demuxErr := docker.DemuxLogs(bytes.NewReader(logBody), &buf); demuxErr == nil {
					result.LogTail = buf.Bytes()
				}
			}
		}
	}

	return result, nil
}

// Close stops all warm containers and the GC goroutine.
func (e *DockerExecutor) Close() error {
	close(e.done)
	e.wg.Wait()

	e.mu.Lock()
	names := make([]string, 0, len(e.containers))
	for name := range e.containers {
		names = append(names, name)
	}
	e.mu.Unlock()

	for _, name := range names {
		e.removeContainer(name)
	}
	return nil
}

// ─── internal ─────────────────────────────────────────────────────────────────

func (e *DockerExecutor) getOrStart(ctx context.Context, req Request) (*warmContainer, error) {
	e.mu.Lock()
	if c, ok := e.containers[req.FunctionName]; ok {
		e.mu.Unlock()
		return c, nil
	}
	// Reserve a port and insert a sentinel so concurrent callers skip straight
	// to the existing entry rather than racing to start a second container.
	port := e.nextPort
	e.nextPort++
	sentinel := &warmContainer{hostPort: port}
	e.containers[req.FunctionName] = sentinel
	e.mu.Unlock()

	image := e.profile.ImageForRuntime(e.cfg, req)
	id, codeDir, optDir, err := e.startContainer(ctx, req, image, port)
	if err != nil {
		// Remove the sentinel so the next caller can retry.
		e.mu.Lock()
		if e.containers[req.FunctionName] == sentinel {
			delete(e.containers, req.FunctionName)
		}
		e.mu.Unlock()
		return nil, err
	}

	c := &warmContainer{id: id, hostPort: port, lastUsed: clock.RealNow(), codeDir: codeDir, optDir: optDir}
	e.mu.Lock()
	e.containers[req.FunctionName] = c
	e.mu.Unlock()
	return c, nil
}

func (e *DockerExecutor) startContainer(ctx context.Context, req Request, image string, hostPort int) (id string, codeDir string, optDir string, err error) {
	pfx := e.profile.WorkloadNamePrefix(e.cfg.InstanceID)
	name := pfx + sanitizeName(req.FunctionName) + "-" + shortID()

	env := DockerEnv(e.profile.RuntimeEnv(e.cfg, req))

	// Platform layer: TLS PEM bundle + extra env for this container.
	var binds []string
	if e.platform != nil {
		platBinds, platEnv, applyErr := docker.BindsAndEnv(e.platform)
		if applyErr != nil {
			slog.Warn("executor docker: platform apply failed", "err", applyErr)
		}
		binds = append(binds, platBinds...)
		env = append(env, platEnv...)
	}

	mountDir := e.profile.CodeMountDir()

	// Extract and mount function code when a code loader is available.
	if e.codeLoader != nil && req.AccountID != "" && codeKey(req) != "" {
		if zipBytes, loadErr := e.codeLoader.LoadCode(context.Background(), req.AccountID, codeKey(req), "$LATEST"); loadErr == nil && len(zipBytes) > 0 {
			if dir, mkErr := os.MkdirTemp("", "executor-code-*"); mkErr == nil {
				if extErr := ExtractZip(zipBytes, dir); extErr == nil {
					codeDir = dir
					binds = append(binds, dir+":"+mountDir+":ro")
				}
			}
		}
	}

	// Extract and mount each layer when the profile mounts layers and a layer
	// blob loader is available.
	layerDir := e.profile.LayerMountDir()
	if layerDir != "" && e.layerLoader != nil && len(req.Layers) > 0 {
		var mkErr error
		optDir, mkErr = os.MkdirTemp("", "executor-opt-*")
		if mkErr == nil {
			anyLayerMounted := false
			for _, layer := range req.Layers {
				if layer.BlobKey == "" {
					continue
				}
				zipBytes, loadErr := e.layerLoader.GetLayerBlob(context.Background(), layer.BlobKey)
				if loadErr != nil || len(zipBytes) == 0 {
					slog.Warn("executor docker: failed to load layer blob", "arn", layer.ARN, "err", loadErr)
					continue
				}
				if extErr := ExtractZip(zipBytes, optDir); extErr != nil {
					slog.Warn("executor docker: failed to extract layer", "arn", layer.ARN, "err", extErr)
					continue
				}
				anyLayerMounted = true
			}
			if anyLayerMounted {
				binds = append(binds, optDir+":"+layerDir+":ro")
			} else {
				os.RemoveAll(optDir)
				optDir = ""
			}
		} else {
			optDir = ""
		}
	} else if layerDir != "" && len(req.Layers) > 0 {
		slog.Debug("executor docker: layers configured but no layer blob loader set; skipping layer mount", "count", len(req.Layers))
	}

	portKey := fmt.Sprintf("%d/tcp", e.profile.InvocationPort())
	hostConfig := map[string]any{
		"PortBindings": map[string]any{
			portKey: []map[string]any{{"HostPort": fmt.Sprintf("%d", hostPort)}},
		},
		"NetworkMode": e.cfg.Network,
		"Memory":      int64(req.MemoryMB) * 1024 * 1024,
	}
	if len(binds) > 0 {
		hostConfig["Binds"] = binds
	}

	createBody := map[string]any{
		"Image":        image,
		"Env":          env,
		"ExposedPorts": map[string]any{portKey: map[string]any{}},
		"HostConfig":   hostConfig,
		"Labels": map[string]string{
			docker.LabelService:  "functions",
			docker.LabelInstance: e.cfg.InstanceID,
		},
	}
	if args := e.profile.ContainerArgs(req); len(args) > 0 {
		createBody["Cmd"] = args
	}
	body, _ := json.Marshal(createBody)

	containerID, createErr := e.docker.Create(ctx, name, body)
	if createErr != nil {
		return "", codeDir, optDir, createErr
	}

	_, statusCode, startErr := e.docker.Call(ctx, http.MethodPost, "/containers/"+containerID+"/start", nil)
	if startErr != nil {
		return "", codeDir, optDir, fmt.Errorf("docker start: %w", startErr)
	}
	if statusCode >= 300 {
		return "", codeDir, optDir, fmt.Errorf("docker start: HTTP %d", statusCode)
	}

	// Brief readiness wait.
	time.Sleep(500 * time.Millisecond)
	slog.Info("executor docker: started container", "function", req.FunctionName, "port", hostPort)
	return containerID, codeDir, optDir, nil
}

func (e *DockerExecutor) removeContainer(functionName string) {
	e.mu.Lock()
	c, ok := e.containers[functionName]
	if ok {
		delete(e.containers, functionName)
	}
	e.mu.Unlock()
	if !ok {
		return
	}

	_ = e.docker.Remove(context.Background(), c.id)
	if c.codeDir != "" {
		os.RemoveAll(c.codeDir)
	}
	if c.optDir != "" {
		os.RemoveAll(c.optDir)
	}
	slog.Info("executor docker: removed container", "function", functionName)
}

func (e *DockerExecutor) gcLoop() {
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

func (e *DockerExecutor) gcOnce() {
	keepalive := time.Duration(e.cfg.KeepaliveSecs) * time.Second
	now := clock.RealNow()
	e.mu.Lock()
	var toRemove []string
	for name, c := range e.containers {
		if c.id == "" {
			continue // sentinel during cold start; skip
		}
		if now.Sub(c.lastUsed) > keepalive {
			toRemove = append(toRemove, name)
		}
	}
	e.mu.Unlock()
	for _, name := range toRemove {
		slog.Info("executor docker: GC idle container", "function", name)
		e.removeContainer(name)
	}
}

// DeleteFunction tears down the warm container for the named function.
func (e *DockerExecutor) DeleteFunction(_ context.Context, name string) {
	e.removeContainer(name)
}

// Reset tears down all warm containers without stopping the GC goroutine.
// After the map pass it sweeps by instance prefix to catch containers that are
// live on the daemon but missing from the in-memory map.
func (e *DockerExecutor) Reset(_ context.Context) {
	e.mu.Lock()
	names := make([]string, 0, len(e.containers))
	for name := range e.containers {
		names = append(names, name)
	}
	e.mu.Unlock()
	for _, name := range names {
		e.removeContainer(name)
	}
	// Best-effort sweep for escaped containers.
	e.removeContainersByPrefix(e.profile.WorkloadNamePrefix(e.cfg.InstanceID))
}

// cleanupOrphans stops and removes Docker containers from previous runs.
// Only containers whose names start with this instance's prefix are removed,
// so multiple JaisCloud instances on the same Docker daemon don't cross-reap.
func (e *DockerExecutor) cleanupOrphans() {
	e.removeContainersByPrefix(e.profile.WorkloadNamePrefix(e.cfg.InstanceID))
}

// removeContainersByPrefix lists and removes all containers whose names match
// prefix.
func (e *DockerExecutor) removeContainersByPrefix(pfx string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	items, err := e.docker.List(ctx, map[string][]string{"name": {pfx}})
	if err != nil {
		return
	}
	for _, item := range items {
		if err := e.docker.Remove(ctx, item.ID); err != nil {
			slog.Debug("executor docker: orphan remove failed", "id", item.ID, "err", err)
			continue
		}
		slog.Info("executor docker: cleaned up orphan container", "id", docker.ShortID(item.ID))
	}
}

func sanitizeName(name string) string {
	return strings.NewReplacer(":", "-", "/", "-", "_", "-").Replace(name)
}

func shortID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%08x", clock.RealNow().UnixNano()&0xffffffff)
	}
	return hex.EncodeToString(b)
}
