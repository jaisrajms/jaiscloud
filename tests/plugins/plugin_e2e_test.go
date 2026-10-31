//go:build plugin_e2e

// Package plugins holds end-to-end tests for the out-of-process plugin layer.
// Each test builds (or reuses, via JAISCLOUD_AWS_BIN) the real jaiscloud-aws
// binary, starts it with --plugin pointing at an in-test sidecar built on /sdk,
// and drives it over HTTP exactly like an SDK client would.
//
//	go test -tags plugin_e2e ./tests/plugins/
package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jaisrajms/jaiscloud/sdk"
)

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

func binary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		if p := os.Getenv("JAISCLOUD_AWS_BIN"); p != "" {
			binPath = p
			return
		}
		dir, err := os.MkdirTemp("", "jaiscloud-plugin-e2e")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "jaiscloud-aws")
		out, err := exec.Command("go", "build", "-o", binPath, "../../cmd/jaiscloud-aws/").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// emulator is a running jaiscloud-aws process.
type emulator struct {
	base string
	out  *bytes.Buffer
	lw   *lockedWriter
	cmd  *exec.Cmd
	done chan error
}

func command(t *testing.T, args ...string) (*emulator, string) {
	t.Helper()
	addr := freeAddr(t)
	port := addr[strings.LastIndex(addr, ":")+1:]
	dir := t.TempDir()
	all := append([]string{"start", "--port", port, "--ephemeral", "--log-level", "warn"}, args...)
	cmd := exec.Command(binary(t), all...)
	cmd.Env = append(os.Environ(), "JAISCLOUD_STATE_DIR="+dir, "JAISCLOUD_DATA_DIR="+dir)
	out := &bytes.Buffer{}
	lw := &lockedWriter{w: out} // one writer (one lock) for both streams
	cmd.Stdout, cmd.Stderr = lw, lw
	e := &emulator{base: "http://" + addr, out: out, lw: lw, cmd: cmd, done: make(chan error, 1)}
	return e, addr
}

type lockedWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// fatal fails the test with the emulator's output, and says so when the process has
// died since it became healthy (otherwise the request error alone hides why).
func (e *emulator) fatal(t *testing.T, err error) {
	t.Helper()
	select {
	case werr := <-e.done:
		t.Fatalf("emulator exited (%v) after it was healthy; request error: %v\n%s", werr, err, e.logs())
	default:
		t.Fatalf("%v\n%s", err, e.logs())
	}
}

func (e *emulator) logs() string {
	e.lw.mu.Lock()
	defer e.lw.mu.Unlock()
	return e.out.String()
}

func startEmulator(t *testing.T, args ...string) *emulator {
	t.Helper()
	e, _ := command(t, args...)
	if err := e.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { e.done <- e.cmd.Wait() }()
	t.Cleanup(func() { _ = e.cmd.Process.Kill() })
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-e.done:
			t.Fatalf("emulator exited during startup: %v\n%s", err, e.logs())
		default:
		}
		if resp, err := http.Get(e.base + "/_jaiscloud/health"); err == nil {
			resp.Body.Close()
			return e
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("emulator did not become healthy:\n%s", e.logs())
	return nil
}

// expectStartupFailure runs the emulator and requires it to exit non-zero.
func expectStartupFailure(t *testing.T, args ...string) string {
	t.Helper()
	e, _ := command(t, args...)
	if err := e.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { e.done <- e.cmd.Wait() }()
	select {
	case err := <-e.done:
		if err == nil {
			t.Fatalf("emulator must fail to start; output:\n%s", e.logs())
		}
		return e.logs()
	case <-time.After(60 * time.Second):
		_ = e.cmd.Process.Kill()
		t.Fatalf("emulator did not exit:\n%s", e.logs())
	}
	return ""
}

func (e *emulator) call(t *testing.T, target string, body map[string]any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, e.base+"/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.fatal(t, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if m == nil {
		m = map[string]any{"_raw": string(raw)}
	}
	return resp.StatusCode, m
}

func (e *emulator) sqs(t *testing.T, action string, body map[string]any) (int, map[string]any) {
	t.Helper()
	return e.call(t, "AmazonSQS."+action, body)
}

func (e *emulator) form(t *testing.T, vals url.Values) (int, string) {
	t.Helper()
	resp, err := http.Post(e.base+"/", "application/x-www-form-urlencoded", strings.NewReader(vals.Encode()))
	if err != nil {
		e.fatal(t, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// formAs posts a Query-protocol request signed for service, for actions the
// router cannot infer from the Action name alone.
func (e *emulator) formAs(t *testing.T, service string, vals url.Values) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.base+"/", strings.NewReader(vals.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/"+service+"/aws4_request, SignedHeaders=host, Signature=00")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.fatal(t, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func (e *emulator) admin(t *testing.T, method, path string, body io.Reader) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, e.base+path, body)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.fatal(t, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// auditPlugin is the whole plugin under test: audit + block + rewrite on
// SendMessage, a few new actions answered from plugin state, and a cache that
// must be dropped on reset.
func auditPlugin() (*sdk.Mux, *cache) {
	c := &cache{}
	m := sdk.NewMux("acme-audit", "0.1.0")

	m.Before("aws", "sqs", "SendMessage", func(ctx context.Context, call *sdk.Call) (sdk.Result, error) {
		row, _ := json.Marshal(map[string]any{"queueUrl": call.Req.Params["QueueUrl"], "at": call.Req.Now, "src": call.Req.SourceArn, "origin": call.Req.Origin})
		if err := call.Store.Put(ctx, "audit", call.Req.CallID, row); err != nil {
			return sdk.Result{}, err
		}
		body, _ := call.Req.Params["MessageBody"].(string)
		switch {
		case strings.Contains(body, "BLOCK"):
			return sdk.Result{Err: &sdk.Error{Code: "InvalidParameterValue", Message: "blocked by acme-audit", Status: 400}}, nil
		case body == "rewrite-me":
			// A patch: only the field being changed; QueueUrl and the rest stay as sent.
			return sdk.Result{Params: map[string]any{"MessageBody": "rewritten"}}, nil
		}
		return sdk.Result{}, nil
	})
	m.After("aws", "sqs", "SendMessage", func(ctx context.Context, call *sdk.Call) (sdk.Result, error) {
		if call.Resp != nil {
			id, _ := call.Resp.Data["MessageId"].(string)
			row, _ := json.Marshal(map[string]any{"at": call.Req.Now})
			if err := call.Store.Put(ctx, "sent", id, row); err != nil {
				return sdk.Result{}, err
			}
		}
		return sdk.Result{}, nil
	})

	m.Replace("aws", "sqs", "*", func(ctx context.Context, call *sdk.Call) (sdk.Result, error) {
		reply := func(d map[string]any) (sdk.Result, error) {
			return sdk.Result{Resp: &sdk.Response{Data: d}}, nil
		}
		switch call.Req.Action {
		case "AcmeAuditCount":
			audits, err := call.Store.List(ctx, "audit", "")
			if err != nil {
				return sdk.Result{}, err
			}
			sent, err := call.Store.List(ctx, "sent", "")
			if err != nil {
				return sdk.Result{}, err
			}
			internal := 0
			for _, a := range audits {
				var r struct{ Origin string }
				_ = json.Unmarshal(a.Data, &r)
				if r.Origin == "internal" {
					internal++
				}
			}
			return reply(map[string]any{"Audits": len(audits), "Sent": len(sent), "Internal": internal,
				"Now": call.Req.Now.Format(time.RFC3339Nano)})
		case "AcmeIncr": // read-modify-write with optimistic concurrency
			for attempt := 0; attempt < 100; attempt++ {
				ent, found, err := call.Store.Get(ctx, "counter", "n")
				if err != nil {
					return sdk.Result{}, err
				}
				n, ver := 0, int64(0)
				if found {
					_ = json.Unmarshal(ent.Data, &n)
					ver = ent.Version
				}
				data, _ := json.Marshal(n + 1)
				switch err := call.Store.PutIf(ctx, "counter", "n", data, ver); err {
				case nil:
					return reply(map[string]any{"N": n + 1})
				case sdk.ErrConflict:
					continue
				default:
					return sdk.Result{}, err
				}
			}
			return sdk.Result{Err: &sdk.Error{Code: "TooMuchContention", Message: "gave up", Status: 503}}, nil
		case "AcmeCounter":
			ent, found, _ := call.Store.Get(ctx, "counter", "n")
			n := 0
			if found {
				_ = json.Unmarshal(ent.Data, &n)
			}
			return reply(map[string]any{"N": n})
		case "AcmeCacheHits":
			return reply(map[string]any{"Hits": c.hit()})
		}
		return sdk.Result{NotHandled: true}, nil
	})
	m.HandleLifecycle(func(_ context.Context, ev string) { c.reset() })
	return m, c
}

// cache stands in for any in-memory state a plugin keeps; it must be dropped
// when the emulator resets or restores.
type cache struct {
	mu   sync.Mutex
	hits int
}

func (c *cache) hit() int { c.mu.Lock(); defer c.mu.Unlock(); c.hits++; return c.hits }
func (c *cache) reset()   { c.mu.Lock(); c.hits = 0; c.mu.Unlock() }

func sidecar(t *testing.T, opts ...sdk.Option) (*httptest.Server, *cache) {
	t.Helper()
	m, c := auditPlugin()
	s := httptest.NewServer(sdk.Serve(m, opts...))
	t.Cleanup(s.Close)
	return s, c
}

func count(t *testing.T, e *emulator) (audits, sent, fromSNS int, now string) {
	t.Helper()
	status, out := e.sqs(t, "AcmeAuditCount", map[string]any{})
	if status != 200 {
		t.Fatalf("AcmeAuditCount: status %d body %v\n%s", status, out, e.logs())
	}
	a, _ := out["Audits"].(float64)
	s, _ := out["Sent"].(float64)
	f, _ := out["Internal"].(float64)
	n, _ := out["Now"].(string)
	return int(a), int(s), int(f), n
}

func receive(t *testing.T, e *emulator, queueURL string) []string {
	t.Helper()
	status, out := e.sqs(t, "ReceiveMessage", map[string]any{"QueueUrl": queueURL, "MaxNumberOfMessages": 10})
	if status != 200 {
		t.Fatalf("ReceiveMessage: %d %v", status, out)
	}
	var bodies []string
	msgs, _ := out["Messages"].([]any)
	for _, m := range msgs {
		bodies = append(bodies, m.(map[string]any)["Body"].(string))
	}
	return bodies
}

func createQueue(t *testing.T, e *emulator, name string) string {
	t.Helper()
	status, out := e.sqs(t, "CreateQueue", map[string]any{"QueueName": name})
	if status != 200 {
		t.Fatalf("CreateQueue: %d %v\n%s", status, out, e.logs())
	}
	u, _ := out["QueueUrl"].(string)
	return u
}

// ---- tests ------------------------------------------------------------------

func TestPluginUC1UC2StateAndAdmin(t *testing.T) {
	s, _ := sidecar(t)
	// A short retry window: the fail-closed check at the end would otherwise wait the default 30 s.
	e := startEmulator(t, "--plugin", s.URL, "--metrics", "--plugin-retry", "1s")
	queueURL := createQueue(t, e, "orders")

	if status, out := e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "hello"}); status != 200 || out["MessageId"] == nil {
		t.Fatalf("SendMessage via plugin chain: %d %v\n%s", status, out, e.logs())
	}
	status, out := e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "BLOCK me"})
	if status != 400 || !strings.Contains(fmt.Sprint(out), "InvalidParameterValue") {
		t.Fatalf("blocked SendMessage: %d %v", status, out)
	}
	if got := receive(t, e, queueURL); len(got) != 1 || got[0] != "hello" {
		t.Fatalf("blocked message must not be enqueued; got %v", got)
	}
	// before-hook rewrites params; the built-in enqueues the rewritten body.
	if status, out = e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "rewrite-me"}); status != 200 {
		t.Fatalf("rewrite SendMessage: %d %v", status, out)
	}
	if got := receive(t, e, queueURL); len(got) != 1 || got[0] != "rewritten" {
		t.Fatalf("params patch not applied: %v", got)
	}

	if a, sent, _, _ := count(t, e); a != 3 || sent != 2 {
		t.Fatalf("plugin state: audits=%d sent=%d, want 3 and 2", a, sent)
	}

	// Observability: doctor lists the plugin, metrics carry the plugin series.
	if st, raw := e.admin(t, http.MethodGet, "/_jaiscloud/doctor", nil); st != 200 || !strings.Contains(string(raw), `"plugins"`) || !strings.Contains(string(raw), "acme-audit") {
		t.Fatalf("doctor = %d %s", st, raw)
	}
	if st, raw := e.admin(t, http.MethodGet, "/metrics", nil); st != 200 || !strings.Contains(string(raw), `jaiscloud_plugin_calls_total{outcome="ok",phase="before",plugin="acme-audit"}`) {
		t.Fatalf("metrics (%d) lack the plugin series", st)
	}

	// Frozen clock reaches the plugin as Request.Now.
	frozen := "2030-01-02T03:04:05Z"
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/clock", strings.NewReader(`{"mode":"fixed","time":"`+frozen+`"}`)); st != 204 {
		t.Fatalf("set clock: %d %s", st, raw)
	}
	if _, _, _, now := count(t, e); now != frozen {
		t.Fatalf("Request.Now = %q, want %q (frozen clock)", now, frozen)
	}

	// Export -> reset -> plugin state is gone -> import -> plugin state is back.
	st, tarball := e.admin(t, http.MethodGet, "/_jaiscloud/export", nil)
	if st != 200 || len(tarball) < 2 {
		t.Fatalf("export: %d (%d bytes)\n%s", st, len(tarball), e.logs())
	}
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/reset", nil); st >= 300 {
		t.Fatalf("reset: %d %s", st, raw)
	}
	a, sent, _, now := count(t, e)
	if a != 0 || sent != 0 {
		t.Fatalf("after reset: audits=%d sent=%d, want 0/0", a, sent)
	}
	// Documented behaviour: reset (and import) put the clock back to real time.
	if strings.HasPrefix(now, "2030") {
		t.Fatalf("reset is expected to un-freeze the clock, plugin still saw %s", now)
	}
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/import", bytes.NewReader(tarball)); st >= 300 {
		t.Fatalf("import: %d %s\n%s", st, raw, e.logs())
	}
	if a, sent, _, _ = count(t, e); a != 3 || sent != 2 {
		t.Fatalf("after import: audits=%d sent=%d, want 3/2", a, sent)
	}

	// Fail-closed: when the plugin is down the built-in is NOT called.
	s.Close()
	status, out = e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "x"})
	if status != 503 || !strings.Contains(fmt.Sprint(out), "PluginUnavailable") {
		t.Fatalf("plugin down: %d %v, want 503 PluginUnavailable", status, out)
	}
}

// Q1: a flow that never touches the wire (SNS publishing into SQS) must still
// go through the plugin that overrides SQS.
func TestInternalSNSDeliveryGoesThroughThePlugin(t *testing.T) {
	s, _ := sidecar(t)
	e := startEmulator(t, "--plugin", s.URL)
	queueURL := createQueue(t, e, "fanout")

	_, attrs := e.sqs(t, "GetQueueAttributes", map[string]any{"QueueUrl": queueURL, "AttributeNames": []string{"QueueArn"}})
	queueArn, _ := attrs["Attributes"].(map[string]any)["QueueArn"].(string)
	if queueArn == "" {
		t.Fatalf("no queue arn: %v", attrs)
	}
	arnRe := regexp.MustCompile(`<TopicArn>([^<]+)</TopicArn>`)
	_, xml := e.form(t, url.Values{"Action": {"CreateTopic"}, "Name": {"alerts"}})
	topic := arnRe.FindStringSubmatch(xml)
	if topic == nil {
		t.Fatalf("CreateTopic: %s", xml)
	}
	if st, body := e.form(t, url.Values{"Action": {"Subscribe"}, "TopicArn": {topic[1]}, "Protocol": {"sqs"}, "Endpoint": {queueArn}}); st != 200 {
		t.Fatalf("Subscribe: %d %s", st, body)
	}

	if st, body := e.form(t, url.Values{"Action": {"Publish"}, "TopicArn": {topic[1]}, "Message": {"from-sns"}}); st != 200 {
		t.Fatalf("Publish: %d %s", st, body)
	}
	var got []string
	for i := 0; i < 30 && len(got) == 0; i++ {
		got = receive(t, e, queueURL)
		time.Sleep(100 * time.Millisecond)
	}
	if len(got) != 1 || !strings.Contains(got[0], "from-sns") {
		t.Fatalf("the built-in delivery must still happen as the innermost step; got %v", got)
	}
	if _, _, fromSNS, _ := count(t, e); fromSNS != 1 {
		t.Fatalf("plugin saw %d SNS-originated SendMessage calls, want 1 — internal flows must go through the plugin", fromSNS)
	}

	// The plugin can also block an internal delivery.
	if st, body := e.form(t, url.Values{"Action": {"Publish"}, "TopicArn": {topic[1]}, "Message": {"BLOCK this"}}); st >= 500 {
		t.Logf("Publish of a blocked message returned %d %s", st, body)
	}
	// The audit row is written before the rejection, so waiting for it proves the
	// plugin really ran on the blocked delivery (and the empty queue is no accident).
	for i := 0; i < 50; i++ {
		if _, _, fromSNS, _ := count(t, e); fromSNS == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, _, fromSNS, _ := count(t, e); fromSNS != 2 {
		t.Fatalf("plugin saw %d SNS-originated calls, want 2 (one delivered, one blocked)", fromSNS)
	}
	for _, b := range receive(t, e, queueURL) {
		if strings.Contains(b, "BLOCK") {
			t.Fatalf("a plugin's before-hook rejection must stop the internal delivery too, but %q was enqueued", b)
		}
	}
}

func TestVersionedPutUnderConcurrency(t *testing.T) {
	s, _ := sidecar(t)
	e := startEmulator(t, "--plugin", s.URL)
	const n = 25
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if st, out := e.sqs(t, "AcmeIncr", map[string]any{}); st != 200 {
				t.Errorf("AcmeIncr: %d %v", st, out)
			}
		}()
	}
	wg.Wait()
	if _, out := e.sqs(t, "AcmeCounter", map[string]any{}); out["N"] != float64(n) {
		t.Fatalf("counter = %v, want %d — lost updates mean PutIf is not atomic", out["N"], n)
	}
}

func TestPluginCachesAreNotifiedOnReset(t *testing.T) {
	s, _ := sidecar(t)
	e := startEmulator(t, "--plugin", s.URL)
	for i := 1; i <= 2; i++ {
		if _, out := e.sqs(t, "AcmeCacheHits", map[string]any{}); out["Hits"] != float64(i) {
			t.Fatalf("hit %d = %v", i, out["Hits"])
		}
	}
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/reset", nil); st >= 300 {
		t.Fatalf("reset: %d %s", st, raw)
	}
	if _, out := e.sqs(t, "AcmeCacheHits", map[string]any{}); out["Hits"] != float64(1) {
		t.Fatalf("after reset the plugin must have been told to drop its cache; hits = %v", out["Hits"])
	}
}

func TestStartupWaitsForALateSidecar(t *testing.T) {
	addr := freeAddr(t)
	go func() {
		time.Sleep(3 * time.Second)
		var l net.Listener
		var err error
		for i := 0; i < 50; i++ { // the reserved port may briefly be taken by another process
			if l, err = net.Listen("tcp", addr); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			return
		}
		m, _ := auditPlugin()
		srv := &http.Server{Handler: sdk.Serve(m)}
		t.Cleanup(func() { srv.Close() })
		_ = srv.Serve(l)
	}()
	e := startEmulator(t, "--plugin", "http://"+addr, "--plugin-wait", "60s")
	if st, _ := e.sqs(t, "AcmeAuditCount", map[string]any{}); st != 200 {
		t.Fatalf("plugin must be live once the emulator is healthy: %d", st)
	}
}

func TestStartupFailsWithDetailsWhenThePluginNeverAppears(t *testing.T) {
	addr := freeAddr(t)
	out := expectStartupFailure(t, "--plugin", "http://"+addr, "--plugin-wait", "1s")
	if !strings.Contains(out, "unavailable after retrying") || !strings.Contains(out, addr) {
		t.Fatalf("the failure must name the plugin address and the retry window, got:\n%s", out)
	}
}

func TestSecret(t *testing.T) {
	s, _ := sidecar(t, sdk.WithSecret("s3cret"))
	out := expectStartupFailure(t, "--plugin", s.URL, "--plugin-wait", "1s")
	if !strings.Contains(out, "plugin") || !strings.Contains(out, "401") {
		t.Fatalf("a plugin that requires a secret must fail startup clearly without one:\n%s", out)
	}
	e := startEmulator(t, "--plugin", s.URL, "--plugin-secret", "s3cret")
	queueURL := createQueue(t, e, "q")
	if st, out := e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "x"}); st != 200 {
		t.Fatalf("with the secret: %d %v", st, out)
	}
	// A browser-style request straight to the sidecar is refused.
	req, _ := http.NewRequest(http.MethodPost, s.URL+"/init", strings.NewReader(`{}`))
	req.Header.Set("Origin", "https://evil.example")
	req.Header.Set("Content-Type", "text/plain")
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode < 400 {
		t.Fatalf("a hostile request to the sidecar must be refused: %v %v", resp, err)
	}
}

func TestZeroPluginParity(t *testing.T) {
	e := startEmulator(t, "--metrics")
	queueURL := createQueue(t, e, "orders")
	if status, out := e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "BLOCK"}); status != 200 {
		t.Fatalf("without a plugin nothing may block: %d %v", status, out)
	}
	if status, _ := e.sqs(t, "AcmeAuditCount", map[string]any{}); status != 400 {
		t.Fatalf("unknown action without a plugin: status %d, want 400 UnknownAction", status)
	}
	if st, _ := e.admin(t, http.MethodPost, "/_jaiscloud/plugin-store/get", strings.NewReader(`{}`)); st == 401 || st == 403 {
		t.Fatalf("store callback route must not exist without plugins (got %d)", st)
	}
	if _, raw := e.admin(t, http.MethodGet, "/_jaiscloud/doctor", nil); strings.Contains(string(raw), "plugins") {
		t.Fatalf("doctor must not mention plugins without any: %s", raw)
	}
	if _, raw := e.admin(t, http.MethodGet, "/metrics", nil); strings.Contains(string(raw), "jaiscloud_plugin") {
		t.Fatal("/metrics must not export plugin series without any plugin")
	}
	// An internal SNS->SQS delivery is untouched too.
	_, attrs := e.sqs(t, "GetQueueAttributes", map[string]any{"QueueUrl": queueURL, "AttributeNames": []string{"QueueArn"}})
	arn, _ := attrs["Attributes"].(map[string]any)["QueueArn"].(string)
	_, xml := e.form(t, url.Values{"Action": {"CreateTopic"}, "Name": {"t"}})
	topic := regexp.MustCompile(`<TopicArn>([^<]+)</TopicArn>`).FindStringSubmatch(xml)
	e.form(t, url.Values{"Action": {"Subscribe"}, "TopicArn": {topic[1]}, "Protocol": {"sqs"}, "Endpoint": {arn}})
	e.form(t, url.Values{"Action": {"Publish"}, "TopicArn": {topic[1]}, "Message": {"plain"}})
	var found bool
	for i := 0; i < 30 && !found; i++ {
		for _, b := range receive(t, e, queueURL) {
			found = found || strings.Contains(b, "plain")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !found {
		t.Fatal("SNS->SQS delivery must work exactly as before without plugins")
	}
}

func TestStoreCallbackRejectsStrangers(t *testing.T) {
	s, _ := sidecar(t)
	e := startEmulator(t, "--plugin", s.URL)
	if st, _ := e.admin(t, http.MethodPost, "/_jaiscloud/plugin-store/list", strings.NewReader(`{"callId":"x","accountId":"a","region":"r","kind":"audit"}`)); st != 401 {
		t.Fatalf("no token: %d, want 401", st)
	}
}

// ---- seam conformance ---------------------------------------------------------

func queueArn(t *testing.T, e *emulator, queueURL string) string {
	t.Helper()
	_, attrs := e.sqs(t, "GetQueueAttributes", map[string]any{"QueueUrl": queueURL, "AttributeNames": []string{"QueueArn"}})
	arn, _ := attrs["Attributes"].(map[string]any)["QueueArn"].(string)
	if arn == "" {
		t.Fatalf("no queue arn: %v", attrs)
	}
	return arn
}

func (e *emulator) s3(t *testing.T, method, path, body string) int {
	t.Helper()
	req, _ := http.NewRequest(method, e.base+path, strings.NewReader(body))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.fatal(t, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// sqsSeams lists every producer that delivers into SQS. Each one must reach a
// plugin that overrides SQS: the registry is the only way into a service. A new
// producer that bypasses the registry fails here once it is added to this table.
var sqsSeams = []struct {
	name    string
	trigger func(t *testing.T, e *emulator, queueURL, queueArn string)
}{
	{"sns", func(t *testing.T, e *emulator, _, arn string) {
		_, xml := e.form(t, url.Values{"Action": {"CreateTopic"}, "Name": {"seam"}})
		topic := regexp.MustCompile(`<TopicArn>([^<]+)</TopicArn>`).FindStringSubmatch(xml)
		if topic == nil {
			t.Fatalf("CreateTopic: %s", xml)
		}
		e.form(t, url.Values{"Action": {"Subscribe"}, "TopicArn": {topic[1]}, "Protocol": {"sqs"}, "Endpoint": {arn}})
		if st, body := e.form(t, url.Values{"Action": {"Publish"}, "TopicArn": {topic[1]}, "Message": {"seam"}}); st != 200 {
			t.Fatalf("Publish: %d %s", st, body)
		}
	}},
	{"s3-notification", func(t *testing.T, e *emulator, _, arn string) {
		if st := e.s3(t, http.MethodPut, "/seam-bucket", ""); st != 200 {
			t.Fatalf("CreateBucket: %d", st)
		}
		cfg := `<NotificationConfiguration><QueueConfiguration><Id>n</Id><Queue>` + arn +
			`</Queue><Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>`
		if st := e.s3(t, http.MethodPut, "/seam-bucket?notification", cfg); st != 200 {
			t.Fatalf("PutBucketNotificationConfiguration: %d", st)
		}
		if st := e.s3(t, http.MethodPut, "/seam-bucket/key.txt", "hello"); st != 200 {
			t.Fatalf("PutObject: %d", st)
		}
	}},
	{"eventbridge", func(t *testing.T, e *emulator, _, arn string) {
		call := func(action string, body map[string]any) {
			if st, out := e.call(t, "AWSEvents."+action, body); st != 200 {
				t.Fatalf("%s: %d %v", action, st, out)
			}
		}
		call("PutRule", map[string]any{"Name": "seam", "EventPattern": `{"source":["acme.seam"]}`})
		call("PutTargets", map[string]any{"Rule": "seam", "Targets": []map[string]any{{"Id": "1", "Arn": arn}}})
		call("PutEvents", map[string]any{"Entries": []map[string]any{{"Source": "acme.seam", "DetailType": "t", "Detail": "{}"}}})
	}},
}

func TestEverySQSSeamGoesThroughThePlugin(t *testing.T) {
	s, _ := sidecar(t)
	e := startEmulator(t, "--plugin", s.URL)
	for _, c := range sqsSeams {
		t.Run(c.name, func(t *testing.T) {
			q := createQueue(t, e, "seam-"+c.name)
			_, _, before, _ := count(t, e)
			c.trigger(t, e, q, queueArn(t, e, q))
			var delivered []string
			var internal int
			for i := 0; i < 50; i++ {
				if delivered = append(delivered, receive(t, e, q)...); len(delivered) > 0 {
					if _, _, internal, _ = count(t, e); internal > before {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
			if len(delivered) == 0 {
				t.Fatalf("%s: the built-in delivery must still happen (it is the plugin chain's last step)", c.name)
			}
			if internal != before+1 {
				t.Fatalf("%s: the plugin saw %d internal SendMessage calls, want %d — this producer bypasses the registry", c.name, internal-before, 1)
			}
		})
	}
}

// ---- internal-call conformance: poller, Lambda, SNS, EventBridge --------------

// observer records every internal call (origin != wire) to the services a
// scenario exercises. It overrides nothing: it only watches, so each scenario's
// built-in behaviour keeps working underneath.
type observer struct {
	mu   sync.Mutex
	seen []string
}

func (o *observer) plugin() *sdk.Mux {
	m := sdk.NewMux("acme-observer", "0.1.0")
	rec := func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		// "internal": another service called us with seam-shaped params;
		// "service": a workflow or CloudFormation called us like a client.
		if c.Req.Origin == "internal" || c.Req.Origin == "service" {
			entry := c.Req.Service + ":" + c.Req.Action
			if c.Req.Origin == "service" {
				entry += "@service"
			}
			o.mu.Lock()
			o.seen = append(o.seen, entry)
			o.mu.Unlock()
		}
		return sdk.Result{}, nil
	}
	for _, svc := range []string{"sqs", "lambda", "sns", "events", "logs", "s3", "secretsmanager", "monitoring", "dynamodbstreams", "states"} {
		m.Before("aws", svc, "*", rec)
	}
	return m
}

func (o *observer) reset() {
	o.mu.Lock()
	o.seen = nil
	o.mu.Unlock()
}

func (o *observer) has(want string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, s := range o.seen {
		if s == want {
			return true
		}
	}
	return false
}

func (o *observer) all() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.seen...)
}

func (e *emulator) rest(t *testing.T, service, method, path, body string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, e.base+path, strings.NewReader(body))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/"+service+"/aws4_request, SignedHeaders=host, Signature=00")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.fatal(t, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func createFunction(t *testing.T, e *emulator, name string) string {
	t.Helper()
	body := `{"FunctionName":"` + name + `","Runtime":"python3.12","Role":"arn:aws:iam::000000000000:role/r","Handler":"index.handler","Code":{"ZipFile":"UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA=="}}`
	if st, raw := e.rest(t, "lambda", http.MethodPost, "/2015-03-31/functions", body); st >= 300 {
		t.Fatalf("CreateFunction: %d %s\n%s", st, raw, e.logs())
	}
	return "arn:aws:lambda:us-east-1:000000000000:function:" + name
}

func createTopic(t *testing.T, e *emulator, name string) string {
	t.Helper()
	_, xml := e.form(t, url.Values{"Action": {"CreateTopic"}, "Name": {name}})
	m := regexp.MustCompile(`<TopicArn>([^<]+)</TopicArn>`).FindStringSubmatch(xml)
	if m == nil {
		t.Fatalf("CreateTopic: %s", xml)
	}
	return m[1]
}

func ebTarget(t *testing.T, e *emulator, rule, source, targetArn string) {
	t.Helper()
	// Ordered on purpose: the target needs its rule to exist.
	steps := []struct {
		action string
		body   map[string]any
	}{
		{"PutRule", map[string]any{"Name": rule, "EventPattern": `{"source":["` + source + `"]}`}},
		{"PutTargets", map[string]any{"Rule": rule, "Targets": []map[string]any{{"Id": "1", "Arn": targetArn}}}},
	}
	for _, st := range steps {
		if code, out := e.call(t, "AWSEvents."+st.action, st.body); code != 200 {
			t.Fatalf("%s: %d %v", st.action, code, out)
		}
	}
	if st, out := e.call(t, "AWSEvents.PutEvents", map[string]any{"Entries": []map[string]any{{"Source": source, "DetailType": "t", "Detail": "{}"}}}); st != 200 {
		t.Fatalf("PutEvents: %d %v", st, out)
	}
}

// internalSeams: each scenario triggers background or cross-service work and
// lists the (service:action) calls the plugin must see with origin=internal.
var internalSeams = []struct {
	name    string
	want    []string
	trigger func(t *testing.T, e *emulator)
}{
	{"lambda-event-source-poller", []string{"sqs:ReceiveMessage", "lambda:InvokeFunction", "sqs:DeleteMessageBatch"}, func(t *testing.T, e *emulator) {
		q := createQueue(t, e, "poll-q")
		fn := createFunction(t, e, "poll-fn")
		body := `{"EventSourceArn":"` + queueArn(t, e, q) + `","FunctionName":"` + fn + `","BatchSize":1}`
		if st, raw := e.rest(t, "lambda", http.MethodPost, "/2015-03-31/event-source-mappings", body); st >= 300 {
			t.Fatalf("CreateEventSourceMapping: %d %s", st, raw)
		}
		if st, out := e.sqs(t, "SendMessage", map[string]any{"QueueUrl": q, "MessageBody": "work"}); st != 200 {
			t.Fatalf("SendMessage: %d %v", st, out)
		}
	}},
	{"s3-notification-to-sns", []string{"sns:Publish"}, func(t *testing.T, e *emulator) {
		topic := createTopic(t, e, "s3-topic")
		e.s3(t, http.MethodPut, "/seam-sns-bucket", "")
		cfg := `<NotificationConfiguration><TopicConfiguration><Id>n</Id><Topic>` + topic +
			`</Topic><Event>s3:ObjectCreated:*</Event></TopicConfiguration></NotificationConfiguration>`
		if st := e.s3(t, http.MethodPut, "/seam-sns-bucket?notification", cfg); st != 200 {
			t.Fatalf("PutBucketNotificationConfiguration: %d", st)
		}
		e.s3(t, http.MethodPut, "/seam-sns-bucket/k", "x")
	}},
	{"eventbridge-to-lambda", []string{"lambda:InvokeFunction"}, func(t *testing.T, e *emulator) {
		ebTarget(t, e, "to-lambda", "acme.lambda", createFunction(t, e, "eb-fn"))
	}},
	{"eventbridge-to-sns", []string{"sns:Publish"}, func(t *testing.T, e *emulator) {
		ebTarget(t, e, "to-sns", "acme.sns", createTopic(t, e, "eb-topic"))
	}},
	{"cloudformation-creates-resources-through-the-registry", []string{"sqs:CreateQueue@service", "sns:CreateTopic@service"}, func(t *testing.T, e *emulator) {
		tpl := `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cfn-seam-q"}},` +
			`"T":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"cfn-seam-t"}}}}`
		if st, out := e.formAs(t, "cloudformation", url.Values{"Action": {"CreateStack"}, "StackName": {"seam-stack"}, "TemplateBody": {tpl}}); st != 200 {
			t.Fatalf("CreateStack: %d %s", st, out)
		}
	}},
	{"step-functions-task-calls-lambda", []string{"lambda:InvokeFunction@service"}, func(t *testing.T, e *emulator) {
		fn := createFunction(t, e, "sfn-fn")
		def, _ := json.Marshal(map[string]any{"StartAt": "Call", "States": map[string]any{"Call": map[string]any{
			"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "End": true,
			"Parameters": map[string]any{"FunctionName": fn, "Payload": map[string]any{"k": "v"}}}}})
		st, out := e.call(t, "AWSStepFunctions.CreateStateMachine", map[string]any{
			"name": "seam-sm", "definition": string(def), "roleArn": "arn:aws:iam::000000000000:role/r"})
		if st != 200 {
			t.Fatalf("CreateStateMachine: %d %v", st, out)
		}
		arn, _ := out["stateMachineArn"].(string)
		if st, out := e.call(t, "AWSStepFunctions.StartExecution", map[string]any{"stateMachineArn": arn, "input": "{}"}); st != 200 {
			t.Fatalf("StartExecution: %d %v", st, out)
		}
	}},
	{"ssm-secret-reference", []string{"secretsmanager:GetSecretValue"}, func(t *testing.T, e *emulator) {
		if st, out := e.call(t, "secretsmanager.CreateSecret", map[string]any{"Name": "seam-secret", "SecretString": "s3cr3t"}); st != 200 {
			t.Fatalf("CreateSecret: %d %v", st, out)
		}
		st, out := e.call(t, "AmazonSSM.GetParameter", map[string]any{"Name": "/aws/reference/secretsmanager/seam-secret"})
		if st != 200 {
			t.Fatalf("GetParameter: %d %v", st, out)
		}
		if v, _ := out["Parameter"].(map[string]any)["Value"].(string); v != "s3cr3t" {
			t.Fatalf("the reference must still resolve through the plugin chain, got %v", out)
		}
	}},
	{"logs-metric-filter-to-cloudwatch", []string{"monitoring:PutMetricData"}, func(t *testing.T, e *emulator) {
		const group = "/seam/metrics"
		for target, body := range map[string]map[string]any{
			"Logs_20140328.CreateLogGroup": {"logGroupName": group},
		} {
			if st, out := e.call(t, target, body); st != 200 {
				t.Fatalf("%s: %d %v", target, st, out)
			}
		}
		steps := []struct {
			target string
			body   map[string]any
		}{
			{"Logs_20140328.PutMetricFilter", map[string]any{"logGroupName": group, "filterName": "errs", "filterPattern": "ERROR",
				"metricTransformations": []map[string]any{{"metricName": "Errs", "metricNamespace": "Seam", "metricValue": "1"}}}},
			{"Logs_20140328.CreateLogStream", map[string]any{"logGroupName": group, "logStreamName": "s"}},
			{"Logs_20140328.PutLogEvents", map[string]any{"logGroupName": group, "logStreamName": "s",
				"logEvents": []map[string]any{{"timestamp": time.Now().UnixMilli(), "message": "ERROR boom"}}}},
		}
		for _, st := range steps {
			if code, out := e.call(t, st.target, st.body); code != 200 {
				t.Fatalf("%s: %d %v", st.target, code, out)
			}
		}
	}},
	{"firehose-delivery-to-s3", []string{"s3:PutObject"}, func(t *testing.T, e *emulator) {
		cfg := map[string]any{"BucketARN": "arn:aws:s3:::seam-firehose", "RoleARN": "arn:aws:iam::000000000000:role/r"}
		if st, out := e.call(t, "Firehose_20150804.CreateDeliveryStream", map[string]any{
			"DeliveryStreamName": "seam-fh", "ExtendedS3DestinationConfiguration": cfg}); st != 200 {
			t.Fatalf("CreateDeliveryStream: %d %v", st, out)
		}
		if st, out := e.call(t, "Firehose_20150804.PutRecord", map[string]any{
			"DeliveryStreamName": "seam-fh", "Record": map[string]any{"Data": "aGVsbG8="}}); st != 200 {
			t.Fatalf("PutRecord: %d %v", st, out)
		}
		if st, out := e.admin(t, http.MethodPost, "/_jaiscloud/firehose/flush", nil); st >= 300 {
			t.Fatalf("flush: %d %s", st, out)
		}
	}},
	{"dynamodb-stream-to-lambda-poller", []string{"dynamodbstreams:DescribeStream", "dynamodbstreams:GetRecords"}, func(t *testing.T, e *emulator) {
		st, out := e.call(t, "DynamoDB_20120810.CreateTable", map[string]any{
			"TableName":            "seam-ddb",
			"AttributeDefinitions": []map[string]any{{"AttributeName": "id", "AttributeType": "S"}},
			"KeySchema":            []map[string]any{{"AttributeName": "id", "KeyType": "HASH"}},
			"BillingMode":          "PAY_PER_REQUEST",
			"StreamSpecification":  map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
		})
		if st != 200 {
			t.Fatalf("CreateTable: %d %v", st, out)
		}
		arn, _ := out["TableDescription"].(map[string]any)["LatestStreamArn"].(string)
		fn := createFunction(t, e, "ddb-fn")
		body := `{"EventSourceArn":"` + arn + `","FunctionName":"` + fn + `","StartingPosition":"TRIM_HORIZON","BatchSize":1}`
		if code, raw := e.rest(t, "lambda", http.MethodPost, "/2015-03-31/event-source-mappings", body); code >= 300 {
			t.Fatalf("CreateEventSourceMapping: %d %s", code, raw)
		}
		if code, out := e.call(t, "DynamoDB_20120810.PutItem", map[string]any{
			"TableName": "seam-ddb", "Item": map[string]any{"id": map[string]any{"S": "1"}}}); code != 200 {
			t.Fatalf("PutItem: %d %v", code, out)
		}
	}},
	{"eventbridge-to-cloudwatch-logs", []string{"logs:CreateLogGroup", "logs:PutLogEvents"}, func(t *testing.T, e *emulator) {
		ebTarget(t, e, "to-logs", "acme.logs", "arn:aws:logs:us-east-1:000000000000:log-group:/seam/eb")
	}},
	{"s3-notification-to-eventbridge", []string{"events:PutEvents"}, func(t *testing.T, e *emulator) {
		e.s3(t, http.MethodPut, "/seam-eb-bucket", "")
		cfg := `<NotificationConfiguration><EventBridgeConfiguration/></NotificationConfiguration>`
		if st := e.s3(t, http.MethodPut, "/seam-eb-bucket?notification", cfg); st != 200 {
			t.Fatalf("PutBucketNotificationConfiguration: %d", st)
		}
		e.s3(t, http.MethodPut, "/seam-eb-bucket/k", "x")
	}},
	{"gateway-cors-preflight", []string{"s3:GetBucketCors"}, func(t *testing.T, e *emulator) {
		e.s3(t, http.MethodPut, "/seam-cors-bucket", "")
		req, _ := http.NewRequest(http.MethodOptions, e.base+"/seam-cors-bucket/k", nil)
		req.Header.Set("Origin", "https://app.example")
		req.Header.Set("Access-Control-Request-Method", "GET")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}},
	{"lambda-code-for-executors", []string{"lambda:GetFunctionCodeZip"}, func(t *testing.T, e *emulator) {
		fn := createFunction(t, e, "code-fn")
		e.admin(t, http.MethodGet, "/_jaiscloud/lambda/code/000000000000/"+fn+"/$LATEST", nil)
	}},
}

func TestEveryInternalSeamGoesThroughThePlugin(t *testing.T) {
	obs := &observer{}
	s := httptest.NewServer(sdk.Serve(obs.plugin()))
	defer s.Close()
	e := startEmulator(t, "--plugin", s.URL)
	for _, c := range internalSeams {
		t.Run(c.name, func(t *testing.T) {
			obs.reset() // an earlier scenario must not satisfy this one
			c.trigger(t, e)
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				ok := true
				for _, w := range c.want {
					ok = ok && obs.has(w)
				}
				if ok {
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
			t.Fatalf("%s: the plugin must see %v as internal calls; saw %v — a caller bypasses the registry\n%s", c.name, c.want, obs.all(), e.logs())
		})
	}
}

// A plugin's answer to an internal call must be what the caller uses, not just
// something the plugin got to see: SSM resolving a Secrets Manager reference
// returns the value the plugin supplied.
func TestInternalCallerUsesThePluginsAnswer(t *testing.T) {
	m := sdk.NewMux("acme-secrets", "0.1.0")
	m.Replace("aws", "secretsmanager", "GetSecretValue", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		if c.Req.Origin != "internal" {
			return sdk.Result{NotHandled: true}, nil
		}
		return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"Name": "x", "SecretString": "from-plugin"}}}, nil
	})
	s := httptest.NewServer(sdk.Serve(m))
	defer s.Close()
	e := startEmulator(t, "--plugin", s.URL)

	st, out := e.call(t, "AmazonSSM.GetParameter", map[string]any{"Name": "/aws/reference/secretsmanager/anything"})
	if st != 200 {
		t.Fatalf("GetParameter: %d %v\n%s", st, out, e.logs())
	}
	if v, _ := out["Parameter"].(map[string]any)["Value"].(string); v != "from-plugin" {
		t.Fatalf("SSM must return the plugin's secret, got %v", out)
	}
}

// A test hook such as eb-tick runs the built-in pass over built-in state only, so
// it also tells the plugins that have a route for that service; a plugin that
// owns the schedule runs its own pass then. A plugin for another service is left
// alone.
func TestTestHooksTellPluginsThatOwnTheService(t *testing.T) {
	var mu sync.Mutex
	got := map[string][]string{}
	plugin := func(name, service string) *httptest.Server {
		m := sdk.NewMux(name, "0.1.0")
		m.Before("aws", service, "*", func(context.Context, *sdk.Call) (sdk.Result, error) { return sdk.Result{}, nil })
		m.HandleLifecycle(func(_ context.Context, ev string) {
			mu.Lock()
			defer mu.Unlock()
			got[name] = append(got[name], ev)
		})
		return httptest.NewServer(sdk.Serve(m))
	}
	events, queues := plugin("acme-events", "events"), plugin("acme-queues", "sqs")
	defer events.Close()
	defer queues.Close()
	e := startEmulator(t, "--plugin", events.URL, "--plugin", queues.URL)

	for path, want := range map[string]string{
		"/_jaiscloud/eb-tick":        "tick:events",
		"/_jaiscloud/firehose/flush": "", // no plugin routes Firehose
	} {
		if st, out := e.admin(t, http.MethodPost, path, nil); st >= 300 {
			t.Fatalf("%s: %d %s", path, st, out)
		}
		if want == "" {
			continue
		}
		mu.Lock()
		seen := append([]string(nil), got["acme-events"]...)
		mu.Unlock()
		if len(seen) == 0 || seen[len(seen)-1] != want {
			t.Fatalf("after %s the events plugin saw %v, want %q last", path, seen, want)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, ev := range got["acme-queues"] {
		if strings.HasPrefix(ev, sdk.EventTickPrefix) {
			t.Fatalf("a plugin that does not own the service was ticked: %v", got["acme-queues"])
		}
	}
	for _, ev := range got["acme-events"] {
		if ev == "tick:firehose" {
			t.Fatalf("no plugin routes firehose, yet the events plugin was ticked for it: %v", got["acme-events"])
		}
	}
}

// ---- review additions (PR 623) -------------------------------------------------

// sqsIn is sqs() signed for another region (the scope comes from the credential scope).
func (e *emulator) sqsIn(t *testing.T, region, action string, body map[string]any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, e.base+"/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "AmazonSQS."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20300102/"+region+"/sqs/aws4_request, SignedHeaders=host, Signature=00")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.fatal(t, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return resp.StatusCode, m
}

func (e *emulator) auditsIn(t *testing.T, region string) int {
	t.Helper()
	st, out := e.sqsIn(t, region, "AcmeAuditCount", map[string]any{})
	if st != 200 {
		t.Fatalf("AcmeAuditCount in %s: %d %v\n%s", region, st, out, e.logs())
	}
	n, _ := out["Audits"].(float64)
	return int(n)
}

// A reset scoped to an account, or to an account and a region, wipes exactly that
// part of the plugin's rows.
func TestScopedResetWipesOnlyThatScopesPluginRows(t *testing.T) {
	s, _ := sidecar(t)
	e := startEmulator(t, "--plugin", s.URL)
	for _, region := range []string{"us-east-1", "eu-west-1"} {
		st, out := e.sqsIn(t, region, "CreateQueue", map[string]any{"QueueName": "q"})
		if st != 200 {
			t.Fatalf("CreateQueue in %s: %d %v", region, st, out)
		}
		if st, out = e.sqsIn(t, region, "SendMessage", map[string]any{"QueueUrl": out["QueueUrl"], "MessageBody": "m"}); st != 200 {
			t.Fatalf("SendMessage in %s: %d %v", region, st, out)
		}
	}
	both := func(wantUS, wantEU int, why string) {
		t.Helper()
		if us, eu := e.auditsIn(t, "us-east-1"), e.auditsIn(t, "eu-west-1"); us != wantUS || eu != wantEU {
			t.Fatalf("%s: audits us-east-1=%d eu-west-1=%d, want %d and %d", why, us, eu, wantUS, wantEU)
		}
	}
	// The count calls themselves are audited too? No: AcmeAuditCount is a Replace, not a SendMessage.
	both(1, 1, "before any reset")
	reset := func(query string) {
		t.Helper()
		if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/reset?"+query, nil); st >= 300 {
			t.Fatalf("reset?%s: %d %s", query, st, raw)
		}
	}
	reset("account=999999999999")
	both(1, 1, "reset of an unrelated account")
	reset("account=000000000000&region=eu-west-1")
	both(1, 0, "reset of one region")
	reset("account=000000000000")
	both(0, 0, "reset of the whole account")
}

// A named snapshot captures the plugin's rows; reverting brings them back.
func TestNamedSnapshotRevertRestoresPluginRows(t *testing.T) {
	s, _ := sidecar(t)
	e := startEmulator(t, "--plugin", s.URL)
	queueURL := createQueue(t, e, "q")
	send := func(body string) {
		t.Helper()
		if st, out := e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": body}); st != 200 {
			t.Fatalf("SendMessage: %d %v", st, out)
		}
	}
	send("one")
	send("two")
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/snapshot", strings.NewReader(`{"name":"before","description":"two audits"}`)); st >= 300 {
		t.Fatalf("snapshot create: %d %s\n%s", st, raw, e.logs())
	}
	send("three")
	if a, _, _, _ := count(t, e); a != 3 {
		t.Fatalf("audits after the third message = %d, want 3", a)
	}
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/snapshot/before/revert?reset_first=true", nil); st >= 300 {
		t.Fatalf("snapshot revert: %d %s\n%s", st, raw, e.logs())
	}
	if a, sent, _, _ := count(t, e); a != 2 || sent != 2 {
		t.Fatalf("after revert: audits=%d sent=%d, want the snapshot's 2 and 2", a, sent)
	}
}

// Two identical runs under a frozen clock give byte-identical answers.
func TestFrozenClockGivesIdenticalRuns(t *testing.T) {
	run := func() string {
		s, _ := sidecar(t)
		e := startEmulator(t, "--plugin", s.URL)
		if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/clock", strings.NewReader(`{"mode":"fixed","time":"2031-05-06T07:08:09Z"}`)); st != 204 {
			t.Fatalf("set clock: %d %s", st, raw)
		}
		queueURL := createQueue(t, e, "q")
		for _, body := range []string{"a", "BLOCK", "rewrite-me"} {
			e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": body})
		}
		_, out := e.sqs(t, "AcmeAuditCount", map[string]any{})
		b, _ := json.Marshal(out)
		_, rows := e.sqs(t, "AcmeCounter", map[string]any{})
		c, _ := json.Marshal(rows)
		return string(b) + string(c)
	}
	first, second := run(), run()
	if first != second || !strings.Contains(first, "2031-05-06T07:08:09Z") {
		t.Fatalf("runs differ or ignore the frozen clock:\n%s\n%s", first, second)
	}
}

// A plugin that restarts while the emulator is running is re-initialised and used again.
func TestPluginRestartWhileTheEmulatorRuns(t *testing.T) {
	addr := freeAddr(t)
	serveOn := func() *http.Server {
		for i := 0; i < 50; i++ {
			l, err := net.Listen("tcp", addr)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			m, _ := auditPlugin()
			srv := &http.Server{Handler: sdk.Serve(m)}
			go func() { _ = srv.Serve(l) }()
			return srv
		}
		t.Fatalf("could not listen on %s", addr)
		return nil
	}
	srv := serveOn()
	t.Cleanup(func() { srv.Close() })
	e := startEmulator(t, "--plugin", "http://"+addr, "--plugin-retry", "20s")
	queueURL := createQueue(t, e, "q")
	if st, out := e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "before"}); st != 200 {
		t.Fatalf("SendMessage: %d %v", st, out)
	}
	srv.Close() // the plugin process dies and comes back with no memory of /init
	srv = serveOn()
	t.Cleanup(func() { srv.Close() })
	if st, out := e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "after"}); st != 200 {
		t.Fatalf("a call after the plugin restarted must be re-initialised and served: %d %v\n%s", st, out, e.logs())
	}
	if a, _, _, _ := count(t, e); a != 2 {
		t.Fatalf("audits = %d; the plugin's rows must survive its own restart (they live in the emulator)", a)
	}
}
