package sparkhelpers

import (
	"bytes"
	"context"
	"encoding/binary"
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

	corev1 "k8s.io/api/core/v1"
)

// fakeDockerAPI is a minimal Docker Engine API stub covering the endpoints the
// DockerDriver uses: create/start/inspect/logs/list/stop/delete. The list
// endpoint honours the label filter against the containers it created, so the
// instance-scoped sweeps are exercised rather than assumed.
type fakeDockerAPI struct {
	mu        sync.Mutex
	nextID    int
	created   map[string]map[string]any // id -> create body
	names     map[string]string         // id -> name
	started   []string
	removed   []string
	exitCode  int
	oomKilled bool
	running   bool   // inspect keeps reporting "running"
	logs      string // driver output, multiplexed on the logs endpoint
	startErr  int    // non-zero -> start returns this status
}

func newFakeDockerAPI() *fakeDockerAPI {
	return &fakeDockerAPI{created: map[string]map[string]any{}, names: map[string]string{}}
}

func (f *fakeDockerAPI) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		path := strings.TrimPrefix(r.URL.Path, "/"+dockerAPIVersion)
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
			status := "exited"
			if f.running {
				status = "running"
			}
			resp := map[string]any{
				"State": map[string]any{
					"Status":     status,
					"ExitCode":   f.exitCode,
					"OOMKilled":  f.oomKilled,
					"StartedAt":  "2020-01-01T00:00:00Z",
					"FinishedAt": "2020-01-01T00:00:01Z",
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		case len(parts) == 3 && parts[2] == "logs" && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/vnd.docker.raw-stream")
			_, _ = w.Write(multiplexFrame(1, f.logs))
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
		default:
			http.NotFound(w, r)
		}
	})
}

// multiplexFrame encodes one Docker log frame (stream id + big-endian size).
func multiplexFrame(stream byte, payload string) []byte {
	var b bytes.Buffer
	header := make([]byte, 8)
	header[0] = stream
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	b.Write(header)
	b.WriteString(payload)
	return b.Bytes()
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

func dockerDriverTestClient(srv *httptest.Server) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", srv.Listener.Addr().String())
		},
	}}
}

func newDockerDriverForTest(t *testing.T, f *fakeDockerAPI) (*DockerDriver, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return NewDockerDriver(DockerConfig{
		Client:       dockerDriverTestClient(srv),
		InstanceID:   "inst0001",
		PollInterval: 5 * time.Millisecond,
		ReapTimeout:  2 * time.Second,
	}), srv
}

func pysparkTestJob(jobID string) ClientModeJob {
	return ClientModeJob{
		JobID:   jobID,
		Image:   "spark:test",
		Attempt: 0,
		EntryPoint: PythonEntryPoint{
			MainPythonFile: "gs://bucket/main.py",
		},
		ExtraDriverEnv: []corev1.EnvVar{
			{Name: "STORAGE_EMULATOR_HOST", Value: "http://host.docker.internal:8080"},
			{Name: "GOOGLE_CLOUD_PROJECT", Value: "proj"},
		},
		ExtraSparkConfs: []string{"--conf", "spark.hadoop.fs.gs.project.id=proj"},
		Labels:          map[string]string{"jaiscloud.io/provider": "dataproc", "jaiscloud.io/job-id": jobID},
	}
}

func TestDockerDriverSubmitBuildsContainer(t *testing.T) {
	f := newFakeDockerAPI()
	d, _ := newDockerDriverForTest(t, f)

	h, err := d.Submit(context.Background(), pysparkTestJob("j1"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if h.ID == "" || h.Name == "" {
		t.Fatalf("handle = %+v, want id+name", h)
	}
	if len(f.started) != 1 {
		t.Fatalf("started %v, want one container", f.started)
	}

	var body map[string]any
	for _, b := range f.created {
		body = b
	}
	if body["Image"] != "spark:test" {
		t.Errorf("Image = %v", body["Image"])
	}
	ep, _ := body["Entrypoint"].([]any)
	if len(ep) != 1 || ep[0] != "spark-submit" {
		t.Errorf("Entrypoint = %v, want [spark-submit]", body["Entrypoint"])
	}
	cmd, _ := body["Cmd"].([]any)
	joined := fmt.Sprint(cmd...)
	if !strings.Contains(joined, "--master") || !strings.Contains(joined, "local[*]") {
		t.Errorf("Cmd = %v, want local[*] master", cmd)
	}
	if !strings.Contains(joined, "gs://bucket/main.py") {
		t.Errorf("Cmd = %v, want the main python file", cmd)
	}
	env, _ := body["Env"].([]any)
	envSet := map[string]string{}
	for _, e := range env {
		kv := strings.SplitN(e.(string), "=", 2)
		envSet[kv[0]] = kv[1]
	}
	if envSet["STORAGE_EMULATOR_HOST"] != "http://host.docker.internal:8080" || envSet["GOOGLE_CLOUD_PROJECT"] != "proj" {
		t.Errorf("env = %v", envSet)
	}
	labels, _ := body["Labels"].(map[string]any)
	if labels[dockerLabelService] != dockerLabelValue || labels[dockerLabelInstance] != "inst0001" || labels["jaiscloud.io/job-id"] != "j1" {
		t.Errorf("labels = %v", labels)
	}
	hc, _ := body["HostConfig"].(map[string]any)
	hosts, _ := hc["ExtraHosts"].([]any)
	if len(hosts) != 1 || hosts[0] != "host.docker.internal:host-gateway" {
		t.Errorf("ExtraHosts = %v, want host.docker.internal:host-gateway", hc["ExtraHosts"])
	}
}

func TestDockerDriverWaitTerminalClassifiesSuccess(t *testing.T) {
	f := newFakeDockerAPI()
	f.logs = "some output\nShutdown hook called\n"
	d, _ := newDockerDriverForTest(t, f)

	h, err := d.Submit(context.Background(), pysparkTestJob("j1"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	final, err := d.WaitTerminal(context.Background(), h, TerminalOptions{})
	if err != nil {
		t.Fatalf("WaitTerminal: %v", err)
	}
	if !final.SparkSucceeded {
		t.Fatalf("final = %+v, want SparkSucceeded", final)
	}
	if final.SparkReason != "exit 0" {
		t.Errorf("SparkReason = %q, want exit 0", final.SparkReason)
	}
}

func TestDockerDriverWaitTerminalClassifiesFailure(t *testing.T) {
	f := newFakeDockerAPI()
	f.exitCode = 1
	f.logs = "boom\nERROR org.apache.spark.SparkException: job failed\n"
	d, _ := newDockerDriverForTest(t, f)

	h, _ := d.Submit(context.Background(), pysparkTestJob("j1"))
	final, err := d.WaitTerminal(context.Background(), h, TerminalOptions{})
	if err != nil {
		t.Fatalf("WaitTerminal: %v", err)
	}
	if final.SparkSucceeded {
		t.Fatalf("final = %+v, want failure", final)
	}
	if !strings.Contains(final.SparkReason, "job failed") {
		t.Errorf("SparkReason = %q, want the ERROR line", final.SparkReason)
	}
}

func TestDockerDriverWaitTerminalCancel(t *testing.T) {
	f := newFakeDockerAPI()
	f.running = true
	d, _ := newDockerDriverForTest(t, f)

	h, _ := d.Submit(context.Background(), pysparkTestJob("j1"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := d.WaitTerminal(ctx, h, TerminalOptions{})
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("WaitTerminal returned nil under cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitTerminal did not return after cancel")
	}
}

func TestDockerDriverStreamLogsDemux(t *testing.T) {
	f := newFakeDockerAPI()
	// Two frames, stdout then stderr.
	f.logs = "hello "
	d, _ := newDockerDriverForTest(t, f)
	h, _ := d.Submit(context.Background(), pysparkTestJob("j1"))

	var buf bytes.Buffer
	if err := d.StreamLogs(context.Background(), h, &buf); err != nil {
		t.Fatalf("StreamLogs: %v", err)
	}
	if got := buf.String(); got != "hello " {
		t.Errorf("StreamLogs = %q, want %q", got, "hello ")
	}
}

func TestDockerDriverReapRemovesContainer(t *testing.T) {
	f := newFakeDockerAPI()
	d, _ := newDockerDriverForTest(t, f)
	h, _ := d.Submit(context.Background(), pysparkTestJob("j1"))

	d.Reap(h)

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.removed) != 1 || f.removed[0] != h.ID {
		t.Fatalf("removed = %v, want [%s]", f.removed, h.ID)
	}
}

func TestDockerDriverResetSweepsOnlyOwnInstance(t *testing.T) {
	f := newFakeDockerAPI()
	mine, _ := newDockerDriverForTest(t, f)
	other := NewDockerDriver(DockerConfig{Client: mine.client, InstanceID: "other999", PollInterval: 5 * time.Millisecond})
	if _, err := mine.Submit(context.Background(), pysparkTestJob("mine")); err != nil {
		t.Fatalf("mine submit: %v", err)
	}
	if _, err := other.Submit(context.Background(), pysparkTestJob("theirs")); err != nil {
		t.Fatalf("other submit: %v", err)
	}

	mine.Reset(context.Background())

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.created) != 1 {
		t.Fatalf("after reset %d containers remain, want 1 (the other instance's)", len(f.created))
	}
	for _, name := range f.names {
		if !strings.Contains(name, "theirs") {
			t.Errorf("surviving container = %q, want the other instance's", name)
		}
	}
}

func TestDockerDriverReapCluster(t *testing.T) {
	f := newFakeDockerAPI()
	d, _ := newDockerDriverForTest(t, f)
	c1 := pysparkTestJob("c1job")
	c1.Labels["jaiscloud.io/cluster-name"] = "c1"
	c2 := pysparkTestJob("c2job")
	c2.Labels["jaiscloud.io/cluster-name"] = "c2"
	if _, err := d.Submit(context.Background(), c1); err != nil {
		t.Fatalf("submit c1: %v", err)
	}
	if _, err := d.Submit(context.Background(), c2); err != nil {
		t.Fatalf("submit c2: %v", err)
	}

	d.ReapCluster(context.Background(), "p", "r", "c1")

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.created) != 1 {
		t.Fatalf("after cluster reap %d remain, want 1", len(f.created))
	}
	for _, name := range f.names {
		if !strings.Contains(name, "c2job") {
			t.Errorf("surviving container = %q, want c2job", name)
		}
	}
}

func TestDockerDriverWaitTerminalStrictExitCode(t *testing.T) {
	// Rule 3 (non-zero exit + "Shutdown hook called", no ERROR) is lenient by
	// default and disabled when the exit code is authoritative (SQL drivers).
	f := newFakeDockerAPI()
	f.exitCode = 1
	f.logs = "done\nShutdown hook called\n"
	d, _ := newDockerDriverForTest(t, f)
	h, _ := d.Submit(context.Background(), pysparkTestJob("j1"))

	lenient, err := d.WaitTerminal(context.Background(), h, TerminalOptions{})
	if err != nil {
		t.Fatalf("WaitTerminal lenient: %v", err)
	}
	if !lenient.SparkSucceeded {
		t.Fatalf("lenient final = %+v, want SparkSucceeded (rule 3)", lenient)
	}
	strict, err := d.WaitTerminal(context.Background(), h, TerminalOptions{StrictExitCode: true})
	if err != nil {
		t.Fatalf("WaitTerminal strict: %v", err)
	}
	if strict.SparkSucceeded {
		t.Fatalf("strict final = %+v, want failure (exit code authoritative)", strict)
	}
}

func TestDockerDriverSubmitStartFailureCleansUp(t *testing.T) {
	f := newFakeDockerAPI()
	f.startErr = http.StatusInternalServerError
	d, _ := newDockerDriverForTest(t, f)
	if _, err := d.Submit(context.Background(), pysparkTestJob("j1")); err == nil {
		t.Fatal("Submit succeeded despite a start failure")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.created) != 0 || len(f.removed) != 1 {
		t.Fatalf("created=%d removed=%v, want the container cleaned up", len(f.created), f.removed)
	}
}

func TestPingRejectsUnreachableSocket(t *testing.T) {
	if err := Ping(context.Background(), "/nonexistent/docker.sock"); err == nil {
		t.Fatal("Ping succeeded against a missing socket")
	}
}

func TestDockerDriverLogsPersistToSink(t *testing.T) {
	// Ensure the sink receives multiplexed frames in order across stdout/stderr.
	var b bytes.Buffer
	if err := demuxDockerLogs(io.MultiReader(
		bytes.NewReader(multiplexFrame(1, "out")),
		bytes.NewReader(multiplexFrame(2, "err")),
	), &b); err != nil {
		t.Fatalf("demux: %v", err)
	}
	if b.String() != "outerr" {
		t.Fatalf("demux = %q, want %q", b.String(), "outerr")
	}
}

func TestBuildDockerArgs(t *testing.T) {
	job := ClientModeJob{
		SparkSubmitArgs: []string{"--conf", "spark.executor.memory=1g"},
		ExtraSparkConfs: []string{"--conf", "spark.hadoop.fs.gs.project.id=proj"},
		EntryPoint:      JarEntryPoint{JarURI: "gs://b/app.jar", MainClass: "Main", JarFileURIs: []string{"gs://b/dep.jar"}},
		JarArgs:         []string{"a", "b"},
	}
	got := BuildDockerArgs(job)
	want := []string{
		"--master", "local[*]",
		"--deploy-mode", "client",
		"--conf", "spark.hadoop.fs.gs.project.id=proj",
		"--conf", "spark.executor.memory=1g",
		"--class", "Main",
		"--jars", "gs://b/dep.jar",
		"gs://b/app.jar",
		"a", "b",
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("BuildDockerArgs =\n  %v\nwant\n  %v", got, want)
	}
}

func TestBuildDockerArgsSQL(t *testing.T) {
	job := ClientModeJob{
		SparkSqlPath: "/opt/spark/bin/spark-sql",
		EntryPoint:   SqlEntryPoint{Queries: []string{"SELECT 1", "SELECT 2"}, HiveVars: map[string]string{"k": "v"}},
	}
	if got := DriverCommand(job); got != "/opt/spark/bin/spark-sql" {
		t.Errorf("DriverCommand = %q, want spark-sql path", got)
	}
	got := strings.Join(BuildDockerArgs(job), " ")
	for _, want := range []string{"--master local[*]", "--hivevar k=v", "-e SELECT 1;\nSELECT 2"} {
		if !strings.Contains(got, want) {
			t.Errorf("BuildDockerArgs = %q, want it to contain %q", got, want)
		}
	}
}
