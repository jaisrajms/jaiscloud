//go:build plugin_e2e && cgo && (linux || darwin)

package plugins

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/jaisrajms/jaiscloud/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// In-process plugins: the real example (sdk/examples/inprocess) is built as a .so in a
// module of its own, the way an author would, and loaded into a cgo build of the real
// emulator. Go loads a plugin only into a binary built with the same toolchain, flags
// and SDK source, so both are built here with the flags `make build-plugins` and
// `make plugin` use (-trimpath, CGO_ENABLED=1).

var (
	variantMu   sync.Mutex
	variantPath = map[string]string{}
	soDir       string
)

// variant builds jaiscloud-<cloud> the way `make build-plugins` does (or reuses
// JAISCLOUD_<CLOUD>_PLUGINS_BIN).
func variant(t *testing.T, cloud string) string {
	t.Helper()
	variantMu.Lock()
	defer variantMu.Unlock()
	if p, ok := variantPath[cloud]; ok {
		return p
	}
	if p := os.Getenv("JAISCLOUD_" + strings.ToUpper(cloud) + "_PLUGINS_BIN"); p != "" {
		variantPath[cloud] = p
		return p
	}
	dir, err := os.MkdirTemp("", "jaiscloud-inprocess-"+cloud)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "jaiscloud-"+cloud+"-plugins")
	cmd := exec.Command("go", "build", "-trimpath", "-o", out, "../../cmd/jaiscloud-"+cloud+"/")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, b)
	}
	variantPath[cloud] = out
	return out
}

// buildSO builds the example plugin in a module of its own that points at this
// checkout's SDK, like an author with a replace or a tag of the same source. extra are
// go build flags (the default is the pinned -trimpath).
func buildSO(t *testing.T, name string, flags ...string) string {
	t.Helper()
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile(filepath.Join(repo, "sdk", "examples", "inprocess", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	gomod := fmt.Sprintf("module example.com/inproc\n\ngo 1.26.8\n\nrequire github.com/jaisrajms/jaiscloud/sdk v0.0.0\n\nreplace github.com/jaisrajms/jaiscloud/sdk => %s\n", filepath.Join(repo, "sdk"))
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), src, 0o644); err != nil {
		t.Fatal(err)
	}
	if flags == nil {
		flags = []string{"-trimpath"}
	}
	out := filepath.Join(dir, name+".so")
	cmd := exec.Command("go", append(append([]string{"build", "-buildmode=plugin"}, flags...), "-o", out, ".")...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the plugin: %v\n%s", err, b)
	}
	return out
}

func startVariant(t *testing.T, cloud string, args ...string) *emulator {
	t.Helper()
	e, _ := commandOf(t, variant(t, cloud), args...)
	return run(t, e)
}

func (e *emulator) rawGET(t *testing.T, path string, headers map[string]string) (int, http.Header, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, e.base+path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.fatal(t, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(b)
}

func (e *emulator) post(t *testing.T, path string, body []byte) (int, []byte) {
	t.Helper()
	resp, err := http.Post(e.base+path, "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		e.fatal(t, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

const s3Auth = "AWS4-HMAC-SHA256 Credential=test/20300102/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"

// AWS: wrap the built-in with Next, answer an S3 read with a stream, and have the plugin's
// own in-memory state follow reset and export/import.
func TestInProcessAWSWrapStreamAndState(t *testing.T) {
	so := buildSO(t, "inprocess")
	e := startVariant(t, "aws", "--plugin", "so:"+so)

	code, _, doctor := e.rawGET(t, "/_jaiscloud/doctor", nil)
	if code != 200 || !strings.Contains(doctor, `"transport":"in-process"`) || !strings.Contains(doctor, `"stateful":true`) || !strings.Contains(doctor, `"pluginBuild"`) {
		t.Fatalf("doctor: %d %s", code, doctor)
	}

	// Wrap: the plugin redacts, then the built-in stores what it was handed.
	st, out := e.sqs(t, "CreateQueue", map[string]any{"QueueName": "q1"})
	if st != 200 {
		t.Fatalf("CreateQueue: %d %v\n%s", st, out, e.logs())
	}
	url := out["QueueUrl"].(string)
	if st, out = e.sqs(t, "SendMessage", map[string]any{"QueueUrl": url, "MessageBody": "login password=hunter2 ok"}); st != 200 {
		t.Fatalf("SendMessage: %d %v\n%s", st, out, e.logs())
	}
	_, out = e.sqs(t, "ReceiveMessage", map[string]any{"QueueUrl": url})
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 || msgs[0].(map[string]any)["Body"] != "login password=*** ok" {
		t.Fatalf("the built-in must store the redacted body, got %v", out)
	}

	// A stream the plugin makes up, which an out-of-process plugin cannot return.
	code, hdr, body := e.rawGET(t, "/anybucket/inprocess/a.txt", map[string]string{"Authorization": s3Auth})
	if code != 200 || body != "served by the in-process plugin: inprocess/a.txt" || hdr.Get("Content-Type") != "text/plain" {
		t.Fatalf("plugin-served object: %d %q %v\n%s", code, body, hdr, e.logs())
	}
	// Clients (aws s3 cp) ask for the object's metadata first; the plugin owns that too.
	headReq, _ := http.NewRequest(http.MethodHead, e.base+"/anybucket/inprocess/a.txt", nil)
	headReq.Header.Set("Authorization", s3Auth)
	headResp, err := http.DefaultClient.Do(headReq)
	if err != nil {
		e.fatal(t, err)
	}
	headResp.Body.Close()
	if headResp.StatusCode != 200 || headResp.Header.Get("Content-Length") != "48" || headResp.Header.Get("Etag") != `"inprocess"` {
		t.Fatalf("HeadObject of a plugin-served object: %d %v\n%s", headResp.StatusCode, headResp.Header, e.logs())
	}
	// Any other key is the built-in's.
	if code, _, _ := e.rawGET(t, "/anybucket/other.txt", map[string]string{"Authorization": s3Auth}); code != 404 {
		t.Fatalf("a key the plugin hands back must reach the built-in (404), got %d", code)
	}

	// State: the count lives in the plugin's memory.
	count := func() float64 {
		st, out := e.sqs(t, "InProcessCount", map[string]any{})
		if st != 200 {
			t.Fatalf("InProcessCount: %d %v\n%s", st, out, e.logs())
		}
		return out["SendMessageCalls"].(float64)
	}
	if n := count(); n != 1 {
		t.Fatalf("count = %v, want 1", n)
	}
	resp, err := http.Get(e.base + "/_jaiscloud/export")
	if err != nil {
		t.Fatal(err)
	}
	tarball, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || len(tarball) == 0 {
		t.Fatalf("export: %d", resp.StatusCode)
	}
	if code, _ := e.post(t, "/_jaiscloud/reset", nil); code != 200 {
		t.Fatalf("reset: %d", code)
	}
	if n := count(); n != 0 {
		t.Fatalf("reset must clear the plugin's own state, count = %v", n)
	}
	if code, b := e.post(t, "/_jaiscloud/import?reset_first=true", tarball); code != 200 {
		t.Fatalf("import: %d %s\n%s", code, b, e.logs())
	}
	if n := count(); n != 1 {
		t.Fatalf("import must restore the plugin's own state, count = %v", n)
	}
}

// GCP: the same .so wraps Pub/Sub publish for REST and for gRPC, and the built-in stores
// what the plugin handed it.
func TestInProcessGCPWrapsPublishOverRESTAndGRPC(t *testing.T) {
	so := buildSO(t, "inprocess")
	grpcAddr, hmsAddr := freeAddr(t), freeAddr(t)
	port := func(a string) string { return a[lastColon(a)+1:] }
	e, _ := commandOf(t, variant(t, "gcp"), "--transports", "rest,grpc", "--grpc-port", port(grpcAddr), "--hms-port", port(hmsAddr), "--plugin", "so:"+so)
	run(t, e)
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if code, out := e.gcpREST(t, http.MethodPut, "/v1/projects/p1/topics/t1", map[string]any{}); code != 200 {
		t.Fatalf("create topic: %d %v\n%s", code, out, e.logs())
	}
	if code, out := e.gcpREST(t, http.MethodPut, "/v1/projects/p1/subscriptions/s1", map[string]any{"topic": "projects/p1/topics/t1"}); code != 200 {
		t.Fatalf("create subscription: %d %v\n%s", code, out, e.logs())
	}
	data := base64.StdEncoding.EncodeToString([]byte("hello"))
	if code, out := e.gcpREST(t, http.MethodPost, "/v1/projects/p1/topics/t1:publish", map[string]any{"messages": []any{map[string]any{"data": data}}}); code != 200 {
		t.Fatalf("REST publish: %d %v\n%s", code, out, e.logs())
	}
	pub := pubsubpb.NewPublisherClient(conn)
	if _, err := pub.Publish(context.Background(), &pubsubpb.PublishRequest{Topic: "projects/p1/topics/t1",
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("hello")}}}); err != nil {
		t.Fatalf("gRPC publish: %v\n%s", err, e.logs())
	}

	deadline := time.Now().Add(10 * time.Second)
	var stamped int
	for time.Now().Before(deadline) && stamped < 2 {
		code, out := e.gcpREST(t, http.MethodPost, "/v1/projects/p1/subscriptions/s1:pull", map[string]any{"maxMessages": 10})
		if code != 200 {
			t.Fatalf("pull: %d %v", code, out)
		}
		stamped = 0
		recv, _ := out["receivedMessages"].([]any)
		for _, r := range recv {
			msg, _ := r.(map[string]any)["message"].(map[string]any)
			if attrs, _ := msg["attributes"].(map[string]any); attrs["via"] == "inprocess-demo" {
				stamped++
			}
		}
		if stamped < 2 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if stamped != 2 {
		t.Fatalf("both the REST and the gRPC message must carry the plugin's attribute, got %d stamped\n%s", stamped, e.logs())
	}
}

// A plugin built with other flags is refused at startup with this emulator's build, not
// loaded and not a crash.
func TestInProcessRefusesAPluginFromAnotherBuild(t *testing.T) {
	so := buildSO(t, "notrim", "-trimpath=false")
	e, _ := commandOf(t, variant(t, "aws"), "--plugin", "so:"+so)
	if err := e.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { e.done <- e.cmd.Wait() }()
	select {
	case err := <-e.done:
		if err == nil {
			t.Fatalf("a plugin from another build must stop startup:\n%s", e.logs())
		}
	case <-time.After(60 * time.Second):
		_ = e.cmd.Process.Kill()
		t.Fatalf("emulator did not exit:\n%s", e.logs())
	}
	if out := e.logs(); !strings.Contains(out, "This emulator is built with") || !strings.Contains(out, "trimpath=true") {
		t.Fatalf("the error must say what this emulator is built with:\n%s", out)
	}
}

// The release build (no cgo) says plainly that it cannot load a .so.
func TestInProcessNeedsACgoBuild(t *testing.T) {
	so := buildSO(t, "inprocess")
	dir := t.TempDir()
	bin := filepath.Join(dir, "jaiscloud-aws")
	cmd := exec.Command("go", "build", "-trimpath", "-o", bin, "../../cmd/jaiscloud-aws/")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, b)
	}
	e, _ := commandOf(t, bin, "--plugin", "so:"+so)
	if err := e.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { e.done <- e.cmd.Wait() }()
	select {
	case err := <-e.done:
		if err == nil {
			t.Fatal("a build without cgo must refuse a so: plugin")
		}
	case <-time.After(60 * time.Second):
		_ = e.cmd.Process.Kill()
		t.Fatalf("emulator did not exit:\n%s", e.logs())
	}
	if out := e.logs(); !strings.Contains(out, "cannot load in-process plugins") || !strings.Contains(out, "make build-plugins") {
		t.Fatalf("the error must say how to get a build that can:\n%s", out)
	}
	out, _ := exec.Command(bin, "version").CombinedOutput()
	if !strings.Contains(string(out), "in-process plugins: not supported") {
		t.Fatalf("version: %s", out)
	}
}

// A plugin built inside the SDK module has the SDK as its main module, and Go treats that
// package as another one than the emulator's dependency. The error says so.
func TestInProcessRefusesAPluginBuiltInsideTheSDKModule(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "inside.so")
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-trimpath", "-o", out, "./examples/inprocess")
	cmd.Dir = filepath.Join(repo, "sdk")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, b)
	}
	e, _ := commandOf(t, variant(t, "aws"), "--plugin", "so:"+out)
	if err := e.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { e.done <- e.cmd.Wait() }()
	select {
	case err := <-e.done:
		if err == nil {
			t.Fatalf("a plugin built inside the SDK module must be refused:\n%s", e.logs())
		}
	case <-time.After(60 * time.Second):
		_ = e.cmd.Process.Kill()
		t.Fatalf("emulator did not exit:\n%s", e.logs())
	}
	if !strings.Contains(e.logs(), "Build the plugin as a module of its own") {
		t.Fatalf("the error must name the cause:\n%s", e.logs())
	}
}

// publishedModuleProxy serves this checkout's SDK as sdk.Version from a file:// module
// proxy, so a build can require it the way it would require a pushed tag.
func publishedModuleProxy(t *testing.T) string {
	t.Helper()
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	proxyRoot := filepath.Join(t.TempDir(), "proxy")
	base := filepath.Join(proxyRoot, "github.com", "jaisrajms", "jaiscloud", "sdk", "@v")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	var zbuf bytes.Buffer
	zw := zip.NewWriter(&zbuf)
	root := filepath.Join(repo, "sdk")
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		w, err := zw.Create("github.com/jaisrajms/jaiscloud/sdk@" + sdk.Version + "/" + filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	gomod, _ := os.ReadFile(filepath.Join(root, "go.mod"))
	for name, data := range map[string][]byte{
		sdk.Version + ".zip":  zbuf.Bytes(),
		sdk.Version + ".mod":  gomod,
		sdk.Version + ".info": []byte(fmt.Sprintf(`{"Version":%q,"Time":"2026-11-01T00:00:00Z"}`, sdk.Version)),
		"list":                []byte(sdk.Version + "\n"),
	} {
		if err := os.WriteFile(filepath.Join(base, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return proxyRoot
}

// The topology a user with only a binary has: a plugin repo that requires the SDK version the
// emulator prints, with no replace, against an emulator built the way a release is (pinned to
// that version). An emulator built through this repository's `replace => ./sdk` holds the same
// source as a different package, and refuses the same plugin.
func TestInProcessPluginRequiringThePublishedSDKVersion(t *testing.T) {
	proxy := publishedModuleProxy(t)
	realCache, err := exec.Command("go", "env", "GOMODCACHE").Output()
	if err != nil {
		t.Fatal(err)
	}
	modcache, err := os.MkdirTemp("", "jaiscloud-modcache")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Go's module cache is read-only; `go clean -modcache` is how it is removed.
		clean := exec.Command("go", "clean", "-modcache")
		clean.Env = append(os.Environ(), "GOMODCACHE="+modcache)
		_ = clean.Run()
		_ = os.RemoveAll(modcache)
	})
	env := append(os.Environ(),
		"GOMODCACHE="+modcache,
		// The proxy serves the SDK; the existing download cache serves every other dependency.
		"GOPROXY=file://"+proxy+",file://"+filepath.Join(strings.TrimSpace(string(realCache)), "cache", "download"),
		"GOSUMDB=off", "GOFLAGS=-mod=mod", "CGO_ENABLED=1",
	)

	// The emulator, pinned to the published SDK, by the Makefile target a release uses.
	out := t.TempDir()
	repo, _ := filepath.Abs("../..")
	mk := exec.Command("make", "-C", repo, "build-plugins-release", "PLUGIN_CLOUDS=aws", "PLUGIN_OUT="+out)
	mk.Env = env
	if b, err := mk.CombinedOutput(); err != nil {
		t.Fatalf("make build-plugins-release: %v\n%s", err, b)
	}
	pinned := filepath.Join(out, "jaiscloud-aws-plugins")
	if v, _ := exec.Command(pinned, "version").CombinedOutput(); !strings.Contains(string(v), "require github.com/jaisrajms/jaiscloud/sdk "+sdk.Version) {
		t.Fatalf("version must name the SDK to require:\n%s", v)
	}

	// The plugin: a repo of its own that requires that version and nothing else.
	src, err := os.ReadFile(filepath.Join(repo, "sdk", "examples", "inprocess", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	gomod := fmt.Sprintf("module example.com/userplugin\n\ngo 1.26.8\n\nrequire github.com/jaisrajms/jaiscloud/sdk %s\n", sdk.Version)
	for name, data := range map[string]string{"go.mod": gomod, "main.go": string(src)} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	so := filepath.Join(dir, "userplugin.so")
	build := exec.Command("go", "build", "-buildmode=plugin", "-trimpath", "-o", so, ".")
	build.Dir, build.Env = dir, env
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the plugin: %v\n%s", err, b)
	}

	// Loads into the pinned emulator and serves.
	e, _ := commandOf(t, pinned, "--plugin", "so:"+so)
	run(t, e)
	if code, _, doctor := e.rawGET(t, "/_jaiscloud/doctor", nil); code != 200 || !strings.Contains(doctor, `"transport":"in-process"`) {
		t.Fatalf("doctor: %d %s\n%s", code, doctor, e.logs())
	}
	if code, _, body := e.rawGET(t, "/b/inprocess/x.txt", map[string]string{"Authorization": s3Auth}); code != 200 || !strings.Contains(body, "served by the in-process plugin") {
		t.Fatalf("plugin-served object: %d %q\n%s", code, body, e.logs())
	}

	// The emulator built from a checkout (replace) refuses it, and says why.
	r, _ := commandOf(t, variant(t, "aws"), "--plugin", "so:"+so)
	if err := r.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { r.done <- r.cmd.Wait() }()
	select {
	case err := <-r.done:
		if err == nil {
			t.Fatalf("an emulator built through the replace must refuse a plugin built against the published SDK:\n%s", r.logs())
		}
	case <-time.After(60 * time.Second):
		_ = r.cmd.Process.Kill()
		t.Fatalf("emulator did not exit:\n%s", r.logs())
	}
	if !strings.Contains(r.logs(), "SDK package differs") {
		t.Fatalf("the refusal must name the SDK:\n%s", r.logs())
	}
}
