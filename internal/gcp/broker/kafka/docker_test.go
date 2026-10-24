package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/docker"
	"jaiscloud/internal/platform"
)

// fakeDockerAPI is a minimal Docker Engine API stub covering the endpoints the
// dockerBroker uses: create/start/inspect/list/stop/delete. The list endpoint
// honours the label filter against the containers it created, so the broker's
// instance- and cluster-scoped sweeps are exercised rather than assumed.
type fakeDockerAPI struct {
	mu       sync.Mutex
	nextID   int
	created  map[string]map[string]any // id -> create body
	names    map[string]string         // id -> name
	startErr int                       // non-zero -> start returns this status
	removed  []string
}

func newFakeDockerAPI() *fakeDockerAPI {
	return &fakeDockerAPI{created: map[string]map[string]any{}, names: map[string]string{}}
}

func (f *fakeDockerAPI) handler() http.Handler {
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
					w.WriteHeader(http.StatusConflict)
					_, _ = w.Write([]byte(`{"message":"Conflict. The container name is already in use"}`))
					return
				}
			}
			f.nextID++
			id := fmt.Sprintf("ctr%04d", f.nextID)
			f.created[id] = body
			f.names[id] = name
			_ = json.NewEncoder(w).Encode(map[string]string{"Id": id})
		case len(parts) == 2 && parts[1] == "json" && r.Method == http.MethodGet:
			var filter struct {
				Label []string `json:"label"`
			}
			_ = json.Unmarshal([]byte(r.URL.Query().Get("filters")), &filter)
			var out []map[string]any
			for id, body := range f.created {
				if dockerBodyHasLabels(body, filter.Label) {
					out = append(out, map[string]any{"Id": id, "Names": []string{"/" + f.names[id]}})
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
		case len(parts) == 2 && r.Method == http.MethodDelete:
			id := f.resolveID(parts[1])
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
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 3 && parts[2] == "stop" && r.Method == http.MethodPost:
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	})
}

func (f *fakeDockerAPI) resolveID(idOrName string) string {
	if _, ok := f.created[idOrName]; ok {
		return idOrName
	}
	for id, name := range f.names {
		if name == idOrName {
			return id
		}
	}
	return idOrName
}

// seed records a container whose create body carries labels, so a sweep can be
// exercised without going through EnsureCluster.
func (f *fakeDockerAPI) seed(name string, labels map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	id := fmt.Sprintf("ctr%04d", f.nextID)
	lm := make(map[string]any, len(labels))
	for k, v := range labels {
		lm[k] = v
	}
	f.created[id] = map[string]any{"Labels": lm}
	f.names[id] = name
}

func (f *fakeDockerAPI) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.created)
}

func dockerBodyHasLabels(body map[string]any, labels []string) bool {
	got, _ := body["Labels"].(map[string]any)
	for _, kv := range labels {
		k, v, _ := strings.Cut(kv, "=")
		if s, _ := got[k].(string); s != v {
			return false
		}
	}
	return true
}

// kafkaDockerClient dials the stub regardless of the request URL host.
func kafkaDockerClient(srv *httptest.Server) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", srv.Listener.Addr().String())
		},
	}}
}

func newDockerBrokerForTest(t *testing.T, f *fakeDockerAPI, instanceID string) *dockerBroker {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	b := newDockerBroker(Config{
		Mode:         string(ModeDocker),
		Image:        "redpanda:test",
		InstanceID:   instanceID,
		DockerClient: kafkaDockerClient(srv),
		ReadyTimeout: 3 * time.Second,
	}, discardLogger())
	b.ready = func(context.Context, string) error { return nil }
	return b
}

// dockerArgHasValue reports whether args contains name immediately followed by
// value.
func dockerArgHasValue(args []any, name, value string) bool {
	for i, a := range args {
		if a == name && i+1 < len(args) && args[i+1] == value {
			return true
		}
	}
	return false
}

func TestDockerBrokerEnsureAndReap(t *testing.T) {
	f := newFakeDockerAPI()
	b := newDockerBrokerForTest(t, f, "inst0001")
	ctx := context.Background()
	key := ClusterKey{Project: "proj", Location: "us-central1", Cluster: "My_Cluster"}

	if ep := b.Endpoint(key.Project, key.Location, key.Cluster); ep != "" {
		t.Fatalf("Endpoint before ensure = %q, want empty", ep)
	}

	got, err := b.EnsureCluster(ctx, key.Project, key.Location, key.Cluster)
	if err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	if !strings.HasPrefix(got, "127.0.0.1:") {
		t.Fatalf("endpoint = %q, want loopback", got)
	}
	if b.Endpoint(key.Project, key.Location, key.Cluster) != got {
		t.Errorf("Endpoint = %q, want %q", b.Endpoint(key.Project, key.Location, key.Cluster), got)
	}

	f.mu.Lock()
	if len(f.created) != 1 {
		f.mu.Unlock()
		t.Fatalf("created %d containers, want 1", len(f.created))
	}
	var body map[string]any
	for _, bb := range f.created {
		body = bb
	}
	f.mu.Unlock()

	if body["Image"] != "redpanda:test" {
		t.Errorf("image = %v, want redpanda:test", body["Image"])
	}
	entry, _ := body["Entrypoint"].([]any)
	if len(entry) != 2 || entry[0] != "rpk" || entry[1] != "redpanda" {
		t.Errorf("Entrypoint = %v, want [rpk redpanda]", body["Entrypoint"])
	}
	cmd, _ := body["Cmd"].([]any)
	if !dockerArgHasValue(cmd, "--advertise-kafka-addr", "PLAINTEXT://"+got) {
		t.Errorf("Cmd = %v, want --advertise-kafka-addr PLAINTEXT://%s", cmd, got)
	}
	if !dockerArgHasValue(cmd, "--kafka-addr", "PLAINTEXT://0.0.0.0:9092") {
		t.Errorf("Cmd = %v, want --kafka-addr PLAINTEXT://0.0.0.0:9092", cmd)
	}

	labels, _ := body["Labels"].(map[string]any)
	if labels[docker.LabelService] != dockerServiceValue {
		t.Errorf("service label = %v, want %q", labels[docker.LabelService], dockerServiceValue)
	}
	if labels[docker.LabelInstance] != "inst0001" {
		t.Errorf("instance label = %v, want inst0001", labels[docker.LabelInstance])
	}
	if labels[dockerLabelCluster] != key.String() {
		t.Errorf("cluster label = %v, want %q", labels[dockerLabelCluster], key.String())
	}

	hc, _ := body["HostConfig"].(map[string]any)
	pb, _ := hc["PortBindings"].(map[string]any)
	bindings, _ := pb["9092/tcp"].([]any)
	if len(bindings) != 1 {
		t.Fatalf("PortBindings = %v, want one 9092/tcp binding", pb)
	}
	binding, _ := bindings[0].(map[string]any)
	if binding["HostIp"] != "127.0.0.1" {
		t.Errorf("HostIp = %v, want 127.0.0.1 (loopback only)", binding["HostIp"])
	}
	hostPort, _ := binding["HostPort"].(string)
	if !strings.HasSuffix(got, ":"+hostPort) {
		t.Errorf("advertised %q does not match bound host port %q", got, hostPort)
	}

	// A second ensure reuses the running broker (no new container).
	again, err := b.EnsureCluster(ctx, key.Project, key.Location, key.Cluster)
	if err != nil || again != got {
		t.Fatalf("second EnsureCluster = %q, %v; want %q, nil", again, err, got)
	}
	if n := f.count(); n != 1 {
		t.Errorf("second ensure started a new container (have %d)", n)
	}

	if err := b.StopCluster(ctx, key.Project, key.Location, key.Cluster); err != nil {
		t.Fatalf("StopCluster: %v", err)
	}
	if ep := b.Endpoint(key.Project, key.Location, key.Cluster); ep != "" {
		t.Errorf("Endpoint after stop = %q, want empty", ep)
	}
	if n := f.count(); n != 0 {
		t.Errorf("%d containers survived StopCluster", n)
	}
}

// TestDockerBrokerEnsureFailureReaps proves a broker that never becomes ready
// does not leak its container.
func TestDockerBrokerEnsureFailureReaps(t *testing.T) {
	f := newFakeDockerAPI()
	b := newDockerBrokerForTest(t, f, "inst0001")
	b.ready = func(context.Context, string) error { return errors.New("not ready") } // never ready

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := b.EnsureCluster(ctx, "p", "l", "c1"); err == nil {
		t.Fatal("EnsureCluster succeeded with a never-ready broker; want error")
	}
	if ep := b.Endpoint("p", "l", "c1"); ep != "" {
		t.Errorf("Endpoint after failed start = %q, want empty", ep)
	}
	if n := f.count(); n != 0 {
		t.Errorf("%d containers leaked after a failed start", n)
	}
}

// TestDockerBrokerStartHTTPErrorReaps proves a create that starts but whose
// start call returns an error leaves no container behind.
func TestDockerBrokerStartHTTPErrorReaps(t *testing.T) {
	f := newFakeDockerAPI()
	f.startErr = http.StatusInternalServerError
	b := newDockerBrokerForTest(t, f, "inst0001")

	if _, err := b.EnsureCluster(context.Background(), "p", "l", "c1"); err == nil {
		t.Fatal("EnsureCluster succeeded despite a failing start; want error")
	}
	if n := f.count(); n != 0 {
		t.Errorf("%d containers leaked after a failed start", n)
	}
}

// TestDockerBrokerResetReapsAndSweeps proves /_jaiscloud/reset clears tracked
// endpoints and sweeps containers this process never tracked (a failed partial
// create, or a previous instance's leftovers).
func TestDockerBrokerResetReapsAndSweeps(t *testing.T) {
	f := newFakeDockerAPI()
	b := newDockerBrokerForTest(t, f, "inst0001")
	ctx := context.Background()
	if _, err := b.EnsureCluster(ctx, "p", "l", "c1"); err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	f.seed("jc-mkbroker-orphan", map[string]string{
		docker.LabelService:  dockerServiceValue,
		docker.LabelInstance: "inst0001",
	})

	if err := b.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if ep := b.Endpoint("p", "l", "c1"); ep != "" {
		t.Errorf("Endpoint after Reset = %q, want empty", ep)
	}
	if n := f.count(); n != 0 {
		t.Errorf("%d containers survived Reset", n)
	}
}

// TestDockerBrokerSweepIsInstanceScoped proves the sweep only reaps containers
// this emulator instance owns, leaving another instance's broker alone.
func TestDockerBrokerSweepIsInstanceScoped(t *testing.T) {
	f := newFakeDockerAPI()
	mine := "inst0001"
	theirs := "inst0002"
	f.seed("jc-mkbroker-mine", map[string]string{docker.LabelService: dockerServiceValue, docker.LabelInstance: mine})
	f.seed("jc-mkbroker-theirs", map[string]string{docker.LabelService: dockerServiceValue, docker.LabelInstance: theirs})

	b := newDockerBrokerForTest(t, f, mine)
	b.sweepOrphans()

	f.mu.Lock()
	defer f.mu.Unlock()
	foundTheirs := false
	for _, name := range f.names {
		if name == "jc-mkbroker-theirs" {
			foundTheirs = true
		}
		if name == "jc-mkbroker-mine" {
			t.Error("this instance's orphan container was not swept")
		}
	}
	if !foundTheirs {
		t.Error("another instance's broker container was swept")
	}
}

// TestDockerBrokerStopReapsUntrackedContainer proves StopCluster reaps a
// partially-started container that was never tracked (by its cluster label and
// deterministic name), so a failed create cannot leak.
func TestDockerBrokerStopReapsUntrackedContainer(t *testing.T) {
	f := newFakeDockerAPI()
	b := newDockerBrokerForTest(t, f, "inst0001")
	key := ClusterKey{Project: "p", Location: "l", Cluster: "c1"}
	f.seed(dockerBrokerName("inst0001", key), map[string]string{
		docker.LabelService:  dockerServiceValue,
		docker.LabelInstance: "inst0001",
		dockerLabelCluster:   key.String(),
	})

	if err := b.StopCluster(context.Background(), key.Project, key.Location, key.Cluster); err != nil {
		t.Fatalf("StopCluster: %v", err)
	}
	if n := f.count(); n != 0 {
		t.Errorf("%d untracked containers survived StopCluster", n)
	}
}

// TestDockerBrokerAppliesPlatformOverlay proves the platform layer's extra env
// and hostPath bind reach the broker container, matching Cloud Run/Dataproc.
func TestDockerBrokerAppliesPlatformOverlay(t *testing.T) {
	f := newFakeDockerAPI()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	plat := &platform.PlatformConfig{
		TLS: platform.TLSConfig{Enabled: true},
		Env: map[string]string{"JAISCLOUD_TEST_ENV": "on"},
		Volumes: []platform.VolumeSpec{{
			Name:   "extra",
			Source: platform.VolumeSource{Kind: "hostPath", HostPath: &platform.HostPathSource{Path: "/tmp"}},
			Mounts: []platform.MountSpec{{MountPath: "/mnt/extra"}},
		}},
	}
	b := newDockerBroker(Config{
		Mode:         string(ModeDocker),
		Image:        "redpanda:test",
		InstanceID:   "inst0001",
		DockerClient: kafkaDockerClient(srv),
		ReadyTimeout: 3 * time.Second,
		Platform:     plat,
	}, discardLogger())
	b.ready = func(context.Context, string) error { return nil }

	if _, err := b.EnsureCluster(context.Background(), "p", "l", "c1"); err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}

	f.mu.Lock()
	var body map[string]any
	for _, bb := range f.created {
		body = bb
	}
	f.mu.Unlock()

	env, _ := body["Env"].([]any)
	foundEnv := false
	for _, e := range env {
		if e == "JAISCLOUD_TEST_ENV=on" {
			foundEnv = true
		}
	}
	if !foundEnv {
		t.Errorf("Env = %v, want the platform env JAISCLOUD_TEST_ENV=on", env)
	}

	hc, _ := body["HostConfig"].(map[string]any)
	binds, _ := hc["Binds"].([]any)
	foundBind := false
	for _, b := range binds {
		if b == "/tmp:/mnt/extra:ro" {
			foundBind = true
		}
	}
	if !foundBind {
		t.Errorf("Binds = %v, want the platform hostPath bind /tmp:/mnt/extra:ro", binds)
	}
}
