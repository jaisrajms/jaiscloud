package runexec

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/docker"
	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
)

// fakeDocker is a minimal Docker Engine API stub covering the endpoints the
// DockerManager uses: create/start/inspect/list/stop/delete. The list endpoint
// honours the label filter against the containers it created, so the manager's
// instance-scoped sweeps are exercised rather than assumed.
type fakeDocker struct {
	mu       sync.Mutex
	nextID   int
	created  map[string]map[string]any // id -> create body (removed on delete)
	portKey  map[string]string         // id -> "<port>/tcp"
	names    map[string]string         // id -> name
	hostPort int                       // published host port reported for every container
	startErr int                       // non-zero -> start returns this status
	started  []string
	removed  []string
}

func newFakeDocker(hostPort int) *fakeDocker {
	return &fakeDocker{
		created:  map[string]map[string]any{},
		portKey:  map[string]string{},
		names:    map[string]string{},
		hostPort: hostPort,
	}
}

func (f *fakeDocker) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		path := strings.TrimPrefix(r.URL.Path, "/"+docker.APIVersion)
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) < 2 || parts[0] != "containers" {
			http.NotFound(w, r)
			return
		}
		switch {
		case parts[1] == "create" && r.Method == http.MethodPost:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			name := r.URL.Query().Get("name")
			for _, existing := range f.names {
				if existing == name {
					// Simulate the daemon's name-conflict response.
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(`{"message":"Conflict. The container name is already in use"}`))
					return
				}
			}
			f.nextID++
			id := fmt.Sprintf("ctr%04d", f.nextID)
			f.created[id] = body
			f.names[id] = name
			if exp, ok := body["ExposedPorts"].(map[string]any); ok {
				for k := range exp {
					f.portKey[id] = k
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"Id": id})
		case parts[1] == "json" && r.Method == http.MethodGet: // list
			var filter struct {
				Label []string `json:"label"`
			}
			_ = json.Unmarshal([]byte(r.URL.Query().Get("filters")), &filter)
			var out []map[string]any
			for id, body := range f.created {
				if containerHasLabels(body, filter.Label) {
					out = append(out, map[string]any{"Id": id, "Names": []string{"/" + f.names[id]}})
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
		case len(parts) == 2 && r.Method == http.MethodDelete:
			id := parts[1]
			if _, ok := f.created[id]; !ok {
				for cid, name := range f.names {
					if name == parts[1] {
						id = cid
						break
					}
				}
			}
			if _, ok := f.created[id]; ok {
				f.removed = append(f.removed, id)
				delete(f.created, id)
				delete(f.names, id)
			}
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 3 && parts[2] == "start" && r.Method == http.MethodPost:
			if f.startErr != 0 {
				w.WriteHeader(f.startErr)
				_, _ = w.Write([]byte("start failed"))
				return
			}
			f.started = append(f.started, parts[1])
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 3 && parts[2] == "stop" && r.Method == http.MethodPost:
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 3 && parts[2] == "json" && r.Method == http.MethodGet:
			key := f.portKey[parts[1]]
			resp := map[string]any{
				"State": map[string]any{"Status": "running"},
				"NetworkSettings": map[string]any{
					"Ports": map[string]any{key: []map[string]string{{"HostIp": "127.0.0.1", "HostPort": fmt.Sprint(f.hostPort)}}},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		default:
			http.NotFound(w, r)
		}
	})
}

func containerHasLabels(body map[string]any, labels []string) bool {
	got, _ := body["Labels"].(map[string]any)
	for _, kv := range labels {
		k, v, _ := strings.Cut(kv, "=")
		if s, _ := got[k].(string); s != v {
			return false
		}
	}
	return true
}

// dockerTestClient dials the stub regardless of the request URL host.
func dockerTestClient(srv *httptest.Server) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", srv.Listener.Addr().String())
		},
	}}
}

func newDockerManagerOn(t *testing.T, srv *httptest.Server, instanceID string) *DockerManager {
	t.Helper()
	return NewDocker(DockerConfig{
		Client:       dockerTestClient(srv),
		Probe:        func(string) bool { return true },
		InstanceID:   instanceID,
		ReadyTimeout: 3 * time.Second,
	})
}

func newDockerManagerForTest(t *testing.T, f *fakeDocker) (*DockerManager, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return newDockerManagerOn(t, srv, "inst0001"), srv
}

func TestDockerEnsureRevisionRegistersAndInvokes(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "hello "+r.URL.RawQuery)
	}))
	defer upstream.Close()
	upstreamPort := upstream.Listener.Addr().(*net.TCPAddr).Port

	f := newFakeDocker(upstreamPort)
	m, _ := newDockerManagerForTest(t, f)
	m.proxy = &http.Client{}

	svc, rev := revisionWithContainer(map[string]any{
		"image":     "nginx:latest",
		"command":   []any{"/bin/sh"},
		"args":      []any{"-c", "serve"},
		"ports":     []any{map[string]any{"containerPort": float64(80)}},
		"resources": map[string]any{"limits": map[string]any{"memory": "128Mi"}},
	})
	ctx := context.Background()
	if err := m.EnsureRevision(ctx, svc, rev); err != nil {
		t.Fatalf("EnsureRevision: %v", err)
	}

	// The create body carried the injected Cloud Run env, the declared port, a
	// loopback ephemeral host-port binding, the memory limit and the scoping
	// labels.
	if len(f.created) != 1 {
		t.Fatalf("created %d containers, want 1", len(f.created))
	}
	var body map[string]any
	for _, b := range f.created {
		body = b
	}
	env, _ := body["Env"].([]any)
	envSet := map[string]string{}
	for _, e := range env {
		kv := strings.SplitN(e.(string), "=", 2)
		envSet[kv[0]] = kv[1]
	}
	if envSet["PORT"] != "80" || envSet["K_SERVICE"] != "svc" || envSet["K_REVISION"] != "svc-00001" {
		t.Errorf("injected env = %v", envSet)
	}
	if body["Image"] != "nginx:latest" {
		t.Errorf("image = %v", body["Image"])
	}
	if exp, ok := body["ExposedPorts"].(map[string]any); !ok || exp["80/tcp"] == nil {
		t.Errorf("ExposedPorts = %v, want 80/tcp", body["ExposedPorts"])
	}
	hc, _ := body["HostConfig"].(map[string]any)
	if hc["Memory"].(float64) != 128*1024*1024 {
		t.Errorf("Memory = %v, want 128Mi", hc["Memory"])
	}
	pb, _ := hc["PortBindings"].(map[string]any)
	bindings, _ := pb["80/tcp"].([]any)
	if len(bindings) != 1 {
		t.Fatalf("PortBindings = %v, want one 80/tcp binding", pb)
	}
	binding, _ := bindings[0].(map[string]any)
	if hp, _ := binding["HostPort"].(string); hp != "" {
		t.Errorf("HostPort = %q, want empty (Docker-assigned)", hp)
	}
	if hip, _ := binding["HostIp"].(string); hip != "127.0.0.1" {
		t.Errorf("HostIp = %q, want 127.0.0.1 (loopback only)", hip)
	}
	labels, _ := body["Labels"].(map[string]any)
	if labels[docker.LabelService] != dockerServiceValue || labels[dockerLabelRunSvc] != "svc" || labels[dockerLabelRevision] != "svc-00001" || labels[docker.LabelInstance] != "inst0001" {
		t.Errorf("labels = %v", labels)
	}

	// The registry resolves the invocation host to the published loopback port.
	host := runcore.InvocationAuthority("p", "l", "svc")
	svcName := runcore.ServiceName("p", "l", "svc")
	if hostOK, svcOK := m.reg.lookup(svcName, normalizeHost(host)); !hostOK || !svcOK {
		t.Fatalf("registry missing target: host=%v svc=%v", hostOK, svcOK)
	}
	tgt := m.reg.serviceTarget(svcName)
	if want := fmt.Sprintf("http://127.0.0.1:%d", upstreamPort); tgt.backend != want {
		t.Fatalf("backend = %q, want %q", tgt.backend, want)
	}

	inv, err := m.Invoke(ctx, runcore.InvocationRequest{Host: host, Method: http.MethodGet, Path: "/", Query: "a=1"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if inv.Status != http.StatusCreated || string(inv.Body) != "hello a=1" || inv.Headers["X-Upstream"] != "yes" {
		t.Fatalf("Invoke = %d %q %v", inv.Status, inv.Body, inv.Headers)
	}
}

func TestDockerRemoveRevisionOnlyWhenLatest(t *testing.T) {
	f := newFakeDocker(1)
	m, _ := newDockerManagerForTest(t, f)
	svc, rev1 := revisionWithContainer(map[string]any{"image": "nginx:latest"})
	ctx := context.Background()
	if err := m.EnsureRevision(ctx, svc, rev1); err != nil {
		t.Fatalf("EnsureRevision rev1: %v", err)
	}
	f.mu.Lock()
	rev1ID := containerIDForRevision(t, f, rev1.ID)
	f.mu.Unlock()
	rev2 := rev1
	rev2.ID = "svc-00002"
	if err := m.EnsureRevision(ctx, svc, rev2); err != nil {
		t.Fatalf("EnsureRevision rev2: %v", err)
	}

	svcName := runcore.ServiceName("p", "l", "svc")
	if err := m.RemoveRevision(ctx, rev1); err != nil {
		t.Fatalf("RemoveRevision rev1: %v", err)
	}
	f.mu.Lock()
	_, rev1Gone := f.created[rev1ID]
	f.mu.Unlock()
	if rev1Gone {
		t.Error("old revision container survived RemoveRevision")
	}
	if tgt := m.reg.serviceTarget(svcName); tgt == nil || tgt.revision != rev2.ID {
		t.Fatalf("latest target = %+v, want revision %s", tgt, rev2.ID)
	}

	if err := m.RemoveRevision(ctx, rev2); err != nil {
		t.Fatalf("RemoveRevision rev2: %v", err)
	}
	if tgt := m.reg.serviceTarget(svcName); tgt != nil {
		t.Fatalf("target survived removing the latest revision: %+v", tgt)
	}
}

func TestDockerRemoveServiceSweeps(t *testing.T) {
	f := newFakeDocker(1)
	m, _ := newDockerManagerForTest(t, f)
	svc, rev := revisionWithContainer(map[string]any{"image": "nginx:latest"})
	ctx := context.Background()
	if err := m.EnsureRevision(ctx, svc, rev); err != nil {
		t.Fatalf("EnsureRevision: %v", err)
	}
	f.mu.Lock()
	before := len(f.removed)
	f.mu.Unlock()

	if err := m.RemoveService(ctx, svc); err != nil {
		t.Fatalf("RemoveService: %v", err)
	}
	if _, svcOK := m.reg.lookup(runcore.ServiceName("p", "l", "svc"), normalizeHost(runcore.InvocationAuthority("p", "l", "svc"))); svcOK {
		t.Error("service still registered after RemoveService")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.removed) <= before {
		t.Fatalf("RemoveService reaped nothing (removed=%v)", f.removed)
	}
	if len(f.created) != 0 {
		t.Errorf("containers survived RemoveService: %v", f.created)
	}
}

func TestDockerRemoveServiceScopedToInstance(t *testing.T) {
	f := newFakeDocker(1)
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	a := newDockerManagerOn(t, srv, "instAAAA1")
	b := newDockerManagerOn(t, srv, "instBBBB2")

	svc, rev := revisionWithContainer(map[string]any{"image": "nginx:latest"})
	ctx := context.Background()
	if err := a.EnsureRevision(ctx, svc, rev); err != nil {
		t.Fatalf("EnsureRevision A: %v", err)
	}
	if err := b.EnsureRevision(ctx, svc, rev); err != nil {
		t.Fatalf("EnsureRevision B: %v", err)
	}
	if len(f.created) != 2 {
		t.Fatalf("created %d containers, want 2 (one per instance)", len(f.created))
	}

	// Deleting the service on instance A must not reap instance B's container.
	if err := a.RemoveService(ctx, svc); err != nil {
		t.Fatalf("RemoveService A: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.created) != 1 {
		t.Fatalf("instance A's RemoveService reaped across instances: %d containers left", len(f.created))
	}
	for id, body := range f.created {
		labels, _ := body["Labels"].(map[string]any)
		if labels[docker.LabelInstance] != "instBBBB2" {
			t.Errorf("surviving container %s belongs to %v, want instBBBB2", id, labels[docker.LabelInstance])
		}
	}
}

func TestDockerResetClearsAndSweeps(t *testing.T) {
	f := newFakeDocker(1)
	m, _ := newDockerManagerForTest(t, f)
	svc, rev := revisionWithContainer(map[string]any{"image": "nginx:latest"})
	ctx := context.Background()
	if err := m.EnsureRevision(ctx, svc, rev); err != nil {
		t.Fatalf("EnsureRevision: %v", err)
	}
	f.mu.Lock()
	before := len(f.removed)
	f.mu.Unlock()

	m.Reset(ctx)

	if hosts, services := m.reg.size(); hosts != 0 || services != 0 {
		t.Errorf("registry not cleared: %d hosts / %d services", hosts, services)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.removed) <= before {
		t.Fatalf("Reset reaped nothing (removed=%v)", f.removed)
	}
	if len(f.created) != 0 {
		t.Errorf("containers survived Reset: %v", f.created)
	}
}

func TestDockerStartFailureCleansUpContainer(t *testing.T) {
	f := newFakeDocker(1)
	f.startErr = http.StatusInternalServerError
	m, _ := newDockerManagerForTest(t, f)
	svc, rev := revisionWithContainer(map[string]any{"image": "nginx:latest"})
	if err := m.EnsureRevision(context.Background(), svc, rev); err == nil {
		t.Fatal("EnsureRevision: want an error when start fails")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.created) != 0 {
		t.Errorf("container leaked after a failed start: %v", f.created)
	}
	if len(f.removed) == 0 {
		t.Error("failed start did not clean up the created container")
	}
	if _, svcOK := m.reg.lookup(runcore.ServiceName("p", "l", "svc"), normalizeHost(runcore.InvocationAuthority("p", "l", "svc"))); svcOK {
		t.Error("service registered despite a failed start")
	}
}

func TestDockerInvokeNotFoundAndUnavailable(t *testing.T) {
	f := newFakeDocker(1)
	m, _ := newDockerManagerForTest(t, f)
	ctx := context.Background()

	_, err := m.Invoke(ctx, runcore.InvocationRequest{
		Host: "svc-abcdef012345.us-central1.run.app", Method: http.MethodGet, Path: "/",
	})
	assertProviderStatus(t, err, http.StatusNotFound)

	_, err = m.Invoke(ctx, runcore.InvocationRequest{
		Service: runstore.Service{ProjectID: "p", Location: "l", ID: "svc"}, Method: http.MethodGet, Path: "/",
	})
	assertProviderStatus(t, err, http.StatusServiceUnavailable)
}

func TestDockerEnsureRejectsMissingImage(t *testing.T) {
	f := newFakeDocker(1)
	m, _ := newDockerManagerForTest(t, f)
	svc, rev := revisionWithContainer(map[string]any{})
	err := m.EnsureRevision(context.Background(), svc, rev)
	assertProviderStatus(t, err, http.StatusBadRequest)
}

func TestDockerContainerNameIsScopedAndStable(t *testing.T) {
	a := dockerContainerName("inst0001", "p", "l", "svc", "svc-00001")
	if a != dockerContainerName("inst0001", "p", "l", "svc", "svc-00001") {
		t.Error("container name is not stable")
	}
	if b := dockerContainerName("inst0002", "p", "l", "svc", "svc-00001"); b == a {
		t.Error("distinct instances produced the same container name")
	}
}

func TestMemoryBytesFromResources(t *testing.T) {
	cases := []struct {
		name string
		res  any
		want int64
	}{
		{"limit", map[string]any{"limits": map[string]any{"memory": "128Mi"}}, 128 * 1024 * 1024},
		{"request fallback", map[string]any{"requests": map[string]any{"memory": "64Mi"}}, 64 * 1024 * 1024},
		{"limit wins", map[string]any{"limits": map[string]any{"memory": "1Gi"}, "requests": map[string]any{"memory": "64Mi"}}, 1024 * 1024 * 1024},
		{"unset", map[string]any{}, 0},
		{"unparseable", map[string]any{"limits": map[string]any{"memory": "lots"}}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := memoryBytesFromResources(tc.res); got != tc.want {
				t.Errorf("memoryBytesFromResources = %d, want %d", got, tc.want)
			}
		})
	}
}

// containerIDForRevision returns the fake container id whose name matches the
// revision (callers hold f.mu).
func containerIDForRevision(t *testing.T, f *fakeDocker, revision string) string {
	t.Helper()
	for id, name := range f.names {
		if strings.Contains(name, workloadNameFor("p", "l", "svc", revision)) {
			return id
		}
	}
	t.Fatalf("no container for revision %s (names=%v)", revision, f.names)
	return ""
}
