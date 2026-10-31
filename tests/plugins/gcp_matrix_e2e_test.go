//go:build plugin_e2e

package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// The cloud-neutral use cases (state, admin, restart, clock, secrets, startup,
// fail-closed) against the real jaiscloud-gcp, mirroring the AWS ones. The plugin
// keeps its state in the emulator's store and reports it by decorating the
// answer of topics.list, so a plain REST client can read it.

type gcpState struct {
	mu     sync.Mutex
	resets int
	ticks  []string
}

// statePlugin audits every topic creation in the plugin store and, after
// topics.list, reports the audit count, the plugin's view of "now" and how many
// resets it was told about.
func statePlugin() (*sdk.Mux, *gcpState) {
	st := &gcpState{}
	m := sdk.NewMux("acme-gcp-state", "0.1.0")
	m.Before("gcp", "pubsub", "TopicCreate", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
		row, _ := json.Marshal(map[string]any{"at": c.Req.Now, "project": c.Req.AccountID})
		return sdk.Result{}, c.Store.Put(ctx, "audit", c.Req.CallID, row)
	})
	m.After("gcp", "pubsub", "TopicList", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
		rows, err := c.Store.List(ctx, "audit", "")
		if err != nil {
			return sdk.Result{}, err
		}
		st.mu.Lock()
		defer st.mu.Unlock()
		data := map[string]any{}
		for k, v := range c.Resp.Data {
			data[k] = v
		}
		data["acmeAudits"] = len(rows)
		data["acmeNow"] = c.Req.Now.Format(time.RFC3339Nano)
		data["acmeResets"] = st.resets
		return sdk.Result{Resp: &sdk.Response{Data: data}}, nil
	})
	m.Before("gcp", "scheduler", "JobsList", func(context.Context, *sdk.Call) (sdk.Result, error) { return sdk.Result{}, nil })
	m.HandleLifecycle(func(_ context.Context, ev string) {
		st.mu.Lock()
		defer st.mu.Unlock()
		if strings.HasPrefix(ev, "tick:") {
			st.ticks = append(st.ticks, ev)
			return
		}
		st.resets++
	})
	return m, st
}

func (e *emulator) topics(t *testing.T) map[string]any {
	t.Helper()
	code, out := e.gcpREST(t, http.MethodGet, "/v1/projects/p1/topics", nil)
	if code != 200 {
		e.fatal(t, fmt.Errorf("list topics: %d %v", code, out))
	}
	return out
}

func (e *emulator) createTopic(t *testing.T, id string) {
	t.Helper()
	if code, out := e.gcpREST(t, http.MethodPut, "/v1/projects/p1/topics/"+id, map[string]any{}); code != 200 {
		e.fatal(t, fmt.Errorf("create topic %s: %d %v", id, code, out))
	}
}

func TestGCPStateAdminClockAndFailClosed(t *testing.T) {
	m, _ := statePlugin()
	s := gcpSidecar(t, m)
	e := startGCP(t, "--plugin", s.URL, "--metrics", "--plugin-retry", "1s")
	e.createTopic(t, "a")
	e.createTopic(t, "b")
	if out := e.topics(t); out["acmeAudits"] != float64(2) {
		t.Fatalf("plugin state: %v", out)
	}

	if st, raw := e.admin(t, http.MethodGet, "/_jaiscloud/doctor", nil); st != 200 || !strings.Contains(string(raw), "acme-gcp-state") {
		t.Fatalf("doctor = %d %s", st, raw)
	}
	if st, raw := e.admin(t, http.MethodGet, "/metrics", nil); st != 200 || !strings.Contains(string(raw), `jaiscloud_plugin_calls_total{outcome="ok",phase="before",plugin="acme-gcp-state"}`) {
		t.Fatalf("metrics (%d) lack the plugin series", st)
	}

	// The business clock reaches the plugin as Request.Now.
	frozen := "2030-01-02T03:04:05Z"
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/clock", strings.NewReader(`{"mode":"fixed","time":"`+frozen+`"}`)); st != 204 {
		t.Fatalf("set clock: %d %s", st, raw)
	}
	if out := e.topics(t); out["acmeNow"] != "2030-01-02T03:04:05Z" {
		t.Fatalf("Request.Now = %v, want the frozen clock", out["acmeNow"])
	}

	// Export, reset (rows gone, plugin told), import (rows back).
	st, tarball := e.admin(t, http.MethodGet, "/_jaiscloud/export", nil)
	if st != 200 || len(tarball) < 2 {
		t.Fatalf("export: %d\n%s", st, e.logs())
	}
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/reset", nil); st >= 300 {
		t.Fatalf("reset: %d %s", st, raw)
	}
	out := e.topics(t)
	if out["acmeAudits"] != float64(0) || out["acmeResets"] != float64(1) {
		t.Fatalf("after reset: %v", out)
	}
	if st, raw := e.admin(t, http.MethodPost, "/_jaiscloud/import?reset_first=true", bytes.NewReader(tarball)); st >= 300 {
		t.Fatalf("import: %d %s\n%s", st, raw, e.logs())
	}
	if out := e.topics(t); out["acmeAudits"] != float64(2) {
		t.Fatalf("after import: %v", out)
	}

	// Fail closed: with the plugin down the built-in is not called.
	s.Close()
	code, body := e.gcpREST(t, http.MethodPut, "/v1/projects/p1/topics/c", map[string]any{})
	if code != 503 || !strings.Contains(fmt.Sprint(body), "PluginUnavailable") && !strings.Contains(fmt.Sprint(body), "UNAVAILABLE") {
		t.Fatalf("plugin down: %d %v, want 503", code, body)
	}
}

func TestGCPStartupWaitsForALateSidecarAndFailsWithDetails(t *testing.T) {
	addr := freeAddr(t)
	go func() {
		time.Sleep(3 * time.Second)
		var l net.Listener
		var err error
		for i := 0; i < 50; i++ {
			if l, err = net.Listen("tcp", addr); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			return
		}
		m, _ := statePlugin()
		srv := &http.Server{Handler: sdk.Serve(m)}
		t.Cleanup(func() { srv.Close() })
		_ = srv.Serve(l)
	}()
	e := startGCP(t, "--plugin", "http://"+addr, "--plugin-wait", "60s")
	e.createTopic(t, "late")
	if out := e.topics(t); out["acmeAudits"] != float64(1) {
		t.Fatalf("the late plugin must be live once the emulator is healthy: %v", out)
	}

	never := freeAddr(t)
	cmd, _ := commandOf(t, gcpBinary(t), "--transports", "rest", "--plugin", "http://"+never, "--plugin-wait", "1s")
	if err := cmd.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { cmd.done <- cmd.cmd.Wait() }()
	select {
	case err := <-cmd.done:
		if err == nil || !strings.Contains(cmd.logs(), "unavailable after retrying") || !strings.Contains(cmd.logs(), never) {
			t.Fatalf("the failure must name the plugin and the retry window (err=%v):\n%s", err, cmd.logs())
		}
	case <-time.After(60 * time.Second):
		_ = cmd.cmd.Process.Kill()
		t.Fatalf("emulator did not exit:\n%s", cmd.logs())
	}
}

func TestGCPSecret(t *testing.T) {
	m, _ := statePlugin()
	s := gcpSidecar(t, m, sdk.WithSecret("s3cret"))
	cmd, _ := commandOf(t, gcpBinary(t), "--transports", "rest", "--plugin", s.URL, "--plugin-wait", "1s")
	if err := cmd.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { cmd.done <- cmd.cmd.Wait() }()
	select {
	case err := <-cmd.done:
		if err == nil || !strings.Contains(cmd.logs(), "401") {
			t.Fatalf("a plugin that needs a secret must fail startup clearly without one (err=%v):\n%s", err, cmd.logs())
		}
	case <-time.After(60 * time.Second):
		_ = cmd.cmd.Process.Kill()
		t.Fatalf("emulator did not exit:\n%s", cmd.logs())
	}
	e := startGCP(t, "--plugin", s.URL, "--plugin-secret", "s3cret")
	e.createTopic(t, "ok")
}

// A plugin that restarts while the emulator runs loses its in-memory init; the
// emulator re-sends it and carries on.
func TestGCPPluginRestartWhileTheEmulatorRuns(t *testing.T) {
	addr := freeAddr(t)
	serve := func() *http.Server {
		var l net.Listener
		var err error
		for i := 0; i < 50; i++ {
			if l, err = net.Listen("tcp", addr); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		m, _ := statePlugin()
		srv := &http.Server{Handler: sdk.Serve(m)}
		go func() { _ = srv.Serve(l) }()
		return srv
	}
	srv := serve()
	t.Cleanup(func() { srv.Close() })
	e := startGCP(t, "--plugin", "http://"+addr, "--plugin-retry", "30s")
	e.createTopic(t, "before-restart")

	srv.Close()
	srv2 := serve() // a fresh process: no /init yet
	t.Cleanup(func() { srv2.Close() })
	e.createTopic(t, "after-restart")
	if out := e.topics(t); out["acmeAudits"] != float64(2) {
		t.Fatalf("rows live in the emulator, so they survive a plugin restart: %v", out)
	}
}

// Without a plugin nothing changes: no plugin section in doctor, no store route.
func TestGCPZeroPluginParity(t *testing.T) {
	e := startGCP(t, "--metrics")
	e.createTopic(t, "plain")
	if out := e.topics(t); out["acmeAudits"] != nil {
		t.Fatalf("no plugin, yet the answer was decorated: %v", out)
	}
	if _, raw := e.admin(t, http.MethodGet, "/_jaiscloud/doctor", nil); strings.Contains(string(raw), "plugins") {
		t.Fatalf("doctor must not mention plugins without any: %s", raw)
	}
	if st, _ := e.admin(t, http.MethodPost, "/_jaiscloud/plugin-store/get", strings.NewReader(`{}`)); st == 401 || st == 403 {
		t.Fatalf("the store callback route must not exist without plugins (got %d)", st)
	}
}

// The test endpoints that run a GCP engine once also tell plugins that own that service.
func TestGCPTestHooksTellPluginsThatOwnTheService(t *testing.T) {
	m, st := statePlugin()
	s := gcpSidecar(t, m)
	e := startGCP(t, "--plugin", s.URL)
	for _, path := range []string{"/_jaiscloud/scheduler-tick", "/_jaiscloud/tasks-tick", "/_jaiscloud/monitoring-tick"} {
		if code, raw := e.admin(t, http.MethodPost, path, nil); code != 204 {
			t.Fatalf("%s: %d %s", path, code, raw)
		}
	}
	// Only the plugin that owns "scheduler" hears the scheduler tick.
	time.Sleep(200 * time.Millisecond)
	st.mu.Lock()
	defer st.mu.Unlock()
	if fmt.Sprint(st.ticks) != "[tick:scheduler]" {
		t.Fatalf("ticks = %v, want [tick:scheduler]", st.ticks)
	}
}
