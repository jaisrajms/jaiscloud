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
	"os"
	"os/exec"
	"path/filepath"
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

// emulator is a running jaiscloud-aws process.
type emulator struct {
	base string
	out  *bytes.Buffer
}

func startEmulator(t *testing.T, args ...string) *emulator {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	dir := t.TempDir()
	all := append([]string{"start", "--port", fmt.Sprint(port), "--ephemeral", "--log-level", "warn"}, args...)
	cmd := exec.Command(binary(t), all...)
	cmd.Env = append(os.Environ(), "JAISCLOUD_STATE_DIR="+dir, "JAISCLOUD_DATA_DIR="+dir)
	out := &bytes.Buffer{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })

	e := &emulator{base: fmt.Sprintf("http://127.0.0.1:%d", port), out: out}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(e.base + "/_jaiscloud/health"); err == nil {
			resp.Body.Close()
			return e
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("emulator did not become healthy:\n%s", out.String())
	return nil
}

// sqs sends one SQS JSON-protocol call and returns status + decoded body.
func (e *emulator) sqs(t *testing.T, action string, body map[string]any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, e.base+"/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "AmazonSQS."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	return e.do(t, req)
}

func (e *emulator) do(t *testing.T, req *http.Request) (int, map[string]any) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
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

func (e *emulator) admin(t *testing.T, method, path string, body io.Reader) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, e.base+path, body)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// auditPlugin is the whole plugin: ~30 lines of business logic on top of sdk.Mux.
func auditPlugin() sdk.Plugin {
	m := sdk.NewMux("acme-audit", "0.1.0")

	// UC1 middleware: audit every SendMessage, block bodies containing BLOCK.
	m.Before("aws", "sqs", "SendMessage", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
		row, _ := json.Marshal(map[string]any{"queueUrl": c.Req.Params["QueueUrl"], "at": c.Req.Now})
		if err := c.Store.Put(ctx, "audit", c.Req.RequestID, row); err != nil {
			return sdk.Result{}, err
		}
		if body, _ := c.Req.Params["MessageBody"].(string); strings.Contains(body, "BLOCK") {
			return sdk.Result{Err: &sdk.Error{Code: "InvalidParameterValue", Message: "blocked by acme-audit", Status: 400}}, nil
		}
		return sdk.Result{}, nil
	})
	m.After("aws", "sqs", "SendMessage", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
		if c.Resp != nil {
			id, _ := c.Resp.Data["MessageId"].(string)
			row, _ := json.Marshal(map[string]any{"at": c.Req.Now})
			if err := c.Store.Put(ctx, "sent", id, row); err != nil {
				return sdk.Result{}, err
			}
		}
		return sdk.Result{}, nil
	})

	// UC1 whole-service replace that inherits everything except one new action
	// (UC2-A: AcmeAuditCount has no built-in).
	m.Replace("aws", "sqs", "*", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
		if c.Req.Action != "AcmeAuditCount" {
			return sdk.Result{NotHandled: true}, nil
		}
		audits, err := c.Store.List(ctx, "audit", "")
		if err != nil {
			return sdk.Result{}, err
		}
		sent, err := c.Store.List(ctx, "sent", "")
		if err != nil {
			return sdk.Result{}, err
		}
		return sdk.Result{Resp: &sdk.Response{Data: map[string]any{
			"Audits": len(audits), "Sent": len(sent), "Now": c.Req.Now.Format(time.RFC3339Nano),
		}}}, nil
	})
	return m
}

func count(t *testing.T, e *emulator) (audits, sent int, now string) {
	t.Helper()
	status, out := e.sqs(t, "AcmeAuditCount", map[string]any{})
	if status != 200 {
		t.Fatalf("AcmeAuditCount: status %d body %v\n%s", status, out, e.out.String())
	}
	a, _ := out["Audits"].(float64)
	s, _ := out["Sent"].(float64)
	n, _ := out["Now"].(string)
	return int(a), int(s), n
}

func TestPluginUC1UC2StateAndAdmin(t *testing.T) {
	sidecar := httptest.NewServer(sdk.Serve(auditPlugin()))
	defer sidecar.Close()
	e := startEmulator(t, "--plugin", sidecar.URL)

	// Built-in still works through the interceptor chain (same nr, codec meta intact).
	status, out := e.sqs(t, "CreateQueue", map[string]any{"QueueName": "orders"})
	if status != 200 {
		t.Fatalf("CreateQueue: %d %v\n%s", status, out, e.out.String())
	}
	queueURL, _ := out["QueueUrl"].(string)

	status, out = e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "hello"})
	if status != 200 || out["MessageId"] == nil {
		t.Fatalf("SendMessage via plugin chain: %d %v\n%s", status, out, e.out.String())
	}

	// before-hook short-circuits: the built-in never runs.
	status, out = e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "BLOCK me"})
	if status != 400 || !strings.Contains(fmt.Sprint(out), "InvalidParameterValue") {
		t.Fatalf("blocked SendMessage: %d %v", status, out)
	}
	if status, out = e.sqs(t, "ReceiveMessage", map[string]any{"QueueUrl": queueURL, "MaxNumberOfMessages": 10}); status != 200 {
		t.Fatalf("ReceiveMessage (inherited via NotHandled): %d %v", status, out)
	} else if msgs, _ := out["Messages"].([]any); len(msgs) != 1 {
		t.Fatalf("blocked message must not be enqueued; got %d messages: %v", len(msgs), out)
	}

	// UC2-A: an action the built-in does not have, answered by the plugin from its state.
	if a, s, _ := count(t, e); a != 2 || s != 1 {
		t.Fatalf("plugin state: audits=%d sent=%d, want 2 and 1", a, s)
	}

	// Frozen clock reaches the plugin as Request.Now.
	frozen := "2030-01-02T03:04:05Z"
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/clock", strings.NewReader(`{"mode":"fixed","time":"`+frozen+`"}`)); st != 204 {
		t.Fatalf("set clock: %d %s", st, raw)
	}
	if _, _, now := count(t, e); now != frozen {
		t.Fatalf("Request.Now = %q, want %q (frozen clock)", now, frozen)
	}

	// Export -> reset -> plugin state is gone -> import -> plugin state is back.
	st, tarball := e.admin(t, http.MethodGet, "/_jaiscloud/export", nil)
	if st != 200 || len(tarball) < 2 {
		t.Fatalf("export: %d (%d bytes)\n%s", st, len(tarball), e.out.String())
	}
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/reset", nil); st >= 300 {
		t.Fatalf("reset: %d %s", st, raw)
	}
	if a, s, _ := count(t, e); a != 0 || s != 0 {
		t.Fatalf("after reset: audits=%d sent=%d, want 0/0", a, s)
	}
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/import", bytes.NewReader(tarball)); st >= 300 {
		t.Fatalf("import: %d %s\n%s", st, raw, e.out.String())
	}
	if a, s, _ := count(t, e); a != 2 || s != 1 {
		t.Fatalf("after import: audits=%d sent=%d, want 2/1", a, s)
	}

	// Fail-closed: when the plugin is down the built-in is NOT called.
	sidecar.Close()
	status, out = e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "x"})
	if status != 503 || !strings.Contains(fmt.Sprint(out), "PluginUnavailable") {
		t.Fatalf("plugin down: %d %v, want 503 PluginUnavailable", status, out)
	}
}

func TestZeroPluginParity(t *testing.T) {
	e := startEmulator(t)
	status, out := e.sqs(t, "CreateQueue", map[string]any{"QueueName": "orders"})
	if status != 200 {
		t.Fatalf("CreateQueue: %d %v", status, out)
	}
	queueURL, _ := out["QueueUrl"].(string)
	if status, out = e.sqs(t, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "BLOCK"}); status != 200 {
		t.Fatalf("without a plugin nothing may block: %d %v", status, out)
	}
	if status, _ = e.sqs(t, "AcmeAuditCount", map[string]any{}); status != 400 {
		t.Fatalf("unknown action without a plugin: status %d, want 400 UnknownAction", status)
	}
	if st, _ := e.admin(t, http.MethodPost, "/_jaiscloud/plugin-store/get", strings.NewReader(`{}`)); st == 401 || st == 403 {
		t.Fatalf("store callback route must not exist without plugins (got %d)", st)
	}
}

func TestStoreCallbackRejectsStrangers(t *testing.T) {
	sidecar := httptest.NewServer(sdk.Serve(auditPlugin()))
	defer sidecar.Close()
	e := startEmulator(t, "--plugin", sidecar.URL)
	// No token.
	if st, _ := e.admin(t, http.MethodPost, "/_jaiscloud/plugin-store/list", strings.NewReader(`{"requestId":"x","accountId":"a","region":"r","kind":"audit"}`)); st != 401 {
		t.Fatalf("no token: %d, want 401", st)
	}
}
