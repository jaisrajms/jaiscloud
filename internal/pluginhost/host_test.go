package pluginhost

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"

	"github.com/go-chi/chi/v5"
	"github.com/jaisrajms/jaiscloud/sdk"
)

// ---- fixtures ---------------------------------------------------------------

// fakeAdapter maps wire service "sqs" to registry prefix "Queue", like AWS.
type fakeAdapter struct{}

func (fakeAdapter) Cloud() model.Cloud { return model.CloudAWS }
func (fakeAdapter) DetectAndDecode(*http.Request, []byte) (*model.NormalizedRequest, adapter.Codec, error) {
	return nil, nil, nil
}
func (fakeAdapter) ServiceToProvider(s string) string {
	if s == "sqs" {
		return "Queue"
	}
	return s
}
func (fakeAdapter) EnrichRequest(*http.Request, string, string) (string, string, string) {
	return "", "", ""
}
func (fakeAdapter) ResourceIDFor(string, string) func(string, string) string { return nil }

type env struct {
	reg   *provider.Registry
	res   store.ResourceStore
	deps  Deps
	built []string // order in which the fake built-in ran
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{reg: provider.NewRegistry(), res: store.NewMemoryResourceStore()}
	e.reg.RegisterAll(map[string]provider.HandlerFunc{
		"Queue.SendMessage": func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			e.built = append(e.built, "builtin:"+fmtBody(nr))
			return provider.OK(map[string]any{"MessageId": "m-1", "_stream": io.NopCloser(strings.NewReader("x"))}), nil
		},
		"Queue.Boom": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return nil, model.NewProviderError("QueueDoesNotExist", "no such queue", 400)
		},
	})
	e.deps = Deps{Cloud: model.CloudAWS, Registry: e.reg, Adapter: fakeAdapter{}, Resources: e.res,
		CallbackURL: "http://127.0.0.1:0", Timeout: 2 * time.Second, StartWait: 1, CallRetry: 1}
	return e
}

func fmtBody(nr *model.NormalizedRequest) string {
	b, _ := nr.Params["MessageBody"].(string)
	return b
}

func serve(t *testing.T, p sdk.Plugin, opts ...sdk.Option) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(sdk.Serve(p, opts...))
	t.Cleanup(s.Close)
	return s
}

// load wires plugins and mounts the store callback routes on a test server whose
// URL becomes the callback URL (so sidecars can really call back).
func (e *env) load(t *testing.T, urls ...string) (*Host, error) {
	t.Helper()
	cb := httptest.NewUnstartedServer(nil)
	e.deps.CallbackURL = "http://" + cb.Listener.Addr().String()
	h, err := Load(context.Background(), urls, e.deps)
	if h != nil {
		r := chi.NewRouter()
		h.Routes(r)
		cb.Config.Handler = r
		cb.Start()
		t.Cleanup(cb.Close)
	} else {
		cb.Close()
	}
	return h, err
}

func send(e *env, body string, params map[string]any) (*model.ProviderResponse, error) {
	nr := &model.NormalizedRequest{Service: "sqs", Action: "SendMessage", Cloud: model.CloudAWS,
		AccountID: "111111111111", Region: "us-east-1", Params: map[string]any{"MessageBody": body}}
	for k, v := range params {
		nr.Params[k] = v
	}
	return e.reg.Dispatch(context.Background(), "Queue.SendMessage", nr)
}

func muxRoutes(name string, f func(m *sdk.Mux)) *sdk.Mux {
	m := sdk.NewMux(name, "0.0.1")
	f(m)
	return m
}

var noop = func(context.Context, *sdk.Call) (sdk.Result, error) { return sdk.Result{}, nil }

// ---- zero plugin ------------------------------------------------------------

func TestLoadWithoutPluginsIsANoOp(t *testing.T) {
	e := newEnv(t)
	before := e.reg.Keys("Queue")
	h, err := Load(context.Background(), nil, e.deps)
	if h != nil || err != nil {
		t.Fatalf("Load(nil) = %v, %v; want nil, nil", h, err)
	}
	if h, err = Load(context.Background(), []string{"", " , "}, e.deps); h != nil || err != nil {
		t.Fatalf("blank specs must count as none: %v, %v", h, err)
	}
	if got := e.reg.Keys("Queue"); strings.Join(got, ",") != strings.Join(before, ",") {
		t.Fatalf("registry changed: %v -> %v", before, got)
	}
}

// ---- chain semantics ----------------------------------------------------------

func TestOnionOrderAndBuiltinStillRuns(t *testing.T) {
	e := newEnv(t)
	var trail []string
	mk := func(name string) *sdk.Mux {
		return muxRoutes(name, func(m *sdk.Mux) {
			m.Before("aws", "sqs", "SendMessage", func(context.Context, *sdk.Call) (sdk.Result, error) {
				trail = append(trail, name+":before")
				return sdk.Result{}, nil
			})
			m.After("aws", "sqs", "SendMessage", func(context.Context, *sdk.Call) (sdk.Result, error) {
				trail = append(trail, name+":after")
				return sdk.Result{}, nil
			})
		})
	}
	a, b := serve(t, mk("one")), serve(t, mk("two"))
	if _, err := e.load(t, a.URL, b.URL); err != nil {
		t.Fatal(err)
	}
	if _, err := send(e, "hi", nil); err != nil {
		t.Fatal(err)
	}
	want := "one:before,two:before,two:after,one:after"
	if got := strings.Join(trail, ","); got != want {
		t.Fatalf("order = %s, want %s (plugin 1 outermost)", got, want)
	}
	if len(e.built) != 1 {
		t.Fatalf("built-in ran %d times", len(e.built))
	}
}

func TestBeforeParamsPatchKeepsInternalKeysAndShortCircuit(t *testing.T) {
	e := newEnv(t)
	p := muxRoutes("p", func(m *sdk.Mux) {
		m.Before("aws", "sqs", "SendMessage", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			if c.Req.Params["MessageBody"] == "stop" {
				return sdk.Result{Err: &sdk.Error{Code: "Nope", Message: "stopped", Status: 403}}, nil
			}
			if _, leaked := c.Req.Params["_reader"]; leaked {
				t.Error("values that cannot be JSON (streams) must not be visible to plugins")
			}
			// A patch: set MessageBody, remove Extra (null), say nothing about the rest.
			np := map[string]any{"MessageBody": "patched", "Extra": nil}
			return sdk.Result{Params: np}, nil
		})
	})
	if _, err := e.load(t, serve(t, p).URL); err != nil {
		t.Fatal(err)
	}
	rd := io.NopCloser(strings.NewReader("stream"))
	nr := &model.NormalizedRequest{Service: "sqs", Action: "SendMessage", Params: map[string]any{
		"MessageBody": "orig", "Extra": 1, "_reader": rd}, AccountID: "a", Region: "r"}
	if _, err := e.reg.Dispatch(context.Background(), "Queue.SendMessage", nr); err != nil {
		t.Fatal(err)
	}
	if nr.Params["MessageBody"] != "patched" || nr.Params["_reader"] != rd {
		t.Fatalf("params = %v", nr.Params)
	}
	if _, has := nr.Params["Extra"]; has {
		t.Fatalf("a key the plugin set to null must be removed: %v", nr.Params)
	}
	built := len(e.built)
	_, err := send(e, "stop", nil)
	if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "Nope" || pe.HTTPStatus != 403 {
		t.Fatalf("short-circuit error = %v", err)
	}
	if len(e.built) != built {
		t.Fatal("built-in must not run after a before short-circuit")
	}
}

func TestAfterSeesErrorsAndCannotOverrideStreamResponses(t *testing.T) {
	e := newEnv(t)
	var sawErr string
	p := muxRoutes("p", func(m *sdk.Mux) {
		m.After("aws", "sqs", "Boom", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			if c.Err != nil {
				sawErr = c.Err.Code
			}
			return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"recovered": true}}}, nil
		})
		m.After("aws", "sqs", "SendMessage", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			if _, leaked := c.Resp.Data["_stream"]; leaked {
				t.Error("a stream must not reach the plugin in responses")
			}
			return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"x": 1}}}, nil
		})
	})
	if _, err := e.load(t, serve(t, p).URL); err != nil {
		t.Fatal(err)
	}
	resp, err := e.reg.Dispatch(context.Background(), "Queue.Boom", &model.NormalizedRequest{Service: "sqs", Action: "Boom", AccountID: "a", Region: "r"})
	if err != nil || resp.Data["recovered"] != true || sawErr != "QueueDoesNotExist" {
		t.Fatalf("after on error: resp=%v err=%v saw=%q", resp, err, sawErr)
	}
	_, err = send(e, "x", nil)
	if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "PluginUnsupportedPayload" {
		t.Fatalf("overriding a response that carries a stream: %v, want PluginUnsupportedPayload", err)
	}
}

func TestReplaceWildcardInheritsAndCoversPluginAddedActions(t *testing.T) {
	e := newEnv(t)
	seen := map[string]int{}
	// A adds a new action; B observes every action with a wildcard before-hook.
	a := muxRoutes("a", func(m *sdk.Mux) {
		m.Replace("aws", "sqs", "AcmeNew", func(context.Context, *sdk.Call) (sdk.Result, error) {
			return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"new": true}}}, nil
		})
	})
	b := muxRoutes("b", func(m *sdk.Mux) {
		m.Before("aws", "sqs", "*", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			seen[c.Req.Action]++
			return sdk.Result{}, nil
		})
		m.Replace("aws", "sqs", "*", func(context.Context, *sdk.Call) (sdk.Result, error) { return sdk.Result{NotHandled: true}, nil })
	})
	if _, err := e.load(t, serve(t, a).URL, serve(t, b).URL); err != nil {
		t.Fatal(err)
	}
	if resp, err := e.reg.Dispatch(context.Background(), "Queue.AcmeNew", &model.NormalizedRequest{Service: "sqs", Action: "AcmeNew", AccountID: "a", Region: "r"}); err != nil || resp.Data["new"] != true {
		t.Fatalf("AcmeNew = %v, %v", resp, err)
	}
	if _, err := send(e, "hi", nil); err != nil {
		t.Fatal(err)
	}
	if seen["AcmeNew"] != 1 || seen["SendMessage"] != 1 {
		t.Fatalf("wildcard before saw %v; must cover the plugin-added action too", seen)
	}
	// Unknown to everyone: b's wildcard replace says NotHandled -> UnknownAction.
	_, err := e.reg.Dispatch(context.Background(), "Queue.Nope", &model.NormalizedRequest{Service: "sqs", Action: "Nope", AccountID: "a", Region: "r"})
	if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "UnknownAction" {
		t.Fatalf("unknown action = %v", err)
	}
}

// Overriding one action leaves every other action of the service on the
// built-in, and the plugin can still hand individual calls back with NotHandled.
func TestReplaceSingleActionLeavesTheRestOnTheBuiltin(t *testing.T) {
	e := newEnv(t)
	var seen []string
	p := muxRoutes("one-action", func(m *sdk.Mux) {
		m.Replace("aws", "sqs", "SendMessage", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			seen = append(seen, c.Req.Origin+":"+c.Req.Action)
			if body, _ := c.Req.Params["MessageBody"].(string); body == "mine" {
				return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"MessageId": "from-plugin"}}}, nil
			}
			return sdk.Result{NotHandled: true}, nil // inherit the built-in for this call
		})
	})
	if _, err := e.load(t, serve(t, p).URL); err != nil {
		t.Fatal(err)
	}

	resp, err := send(e, "mine", nil)
	if err != nil || resp.Data["MessageId"] != "from-plugin" || len(e.built) != 0 {
		t.Fatalf("plugin-handled send = %v, %v, built-in ran %v", resp, err, e.built)
	}
	if resp, err = send(e, "other", nil); err != nil || resp.Data["MessageId"] != "m-1" || len(e.built) != 1 {
		t.Fatalf("NotHandled send = %v, %v, built-in ran %v", resp, err, e.built)
	}

	// An action the plugin did not claim never reaches it.
	_, err = e.reg.Dispatch(context.Background(), "Queue.Boom", &model.NormalizedRequest{Service: "sqs", Action: "Boom", AccountID: "a", Region: "r"})
	if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "QueueDoesNotExist" {
		t.Fatalf("unclaimed action = %v, want the built-in's error", err)
	}

	// An internal caller (SNS, a poller, ...) reaches the same override.
	internal := &model.NormalizedRequest{Service: "sqs", Action: "SendMessage", Origin: model.OriginInternal,
		AccountID: "a", Region: "r", Params: map[string]any{"MessageBody": "mine"}}
	if resp, err = e.reg.Dispatch(context.Background(), "Queue.SendMessage", internal); err != nil || resp.Data["MessageId"] != "from-plugin" {
		t.Fatalf("internal send = %v, %v", resp, err)
	}
	if got := strings.Join(seen, ","); got != "wire:SendMessage,wire:SendMessage,internal:SendMessage" {
		t.Fatalf("plugin saw %s; Boom must not appear", got)
	}
}

func TestServiceNameIsNormalisedForInternalCallers(t *testing.T) {
	e := newEnv(t)
	var svc, rid1, rid2 string
	p := muxRoutes("p", func(m *sdk.Mux) {
		m.Before("aws", "sqs", "SendMessage", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			svc, rid1 = c.Req.Service, c.Req.RequestID
			return sdk.Result{}, nil
		})
		m.After("aws", "sqs", "SendMessage", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			rid2 = c.Req.RequestID
			return sdk.Result{}, nil
		})
	})
	if _, err := e.load(t, serve(t, p).URL); err != nil {
		t.Fatal(err)
	}
	// Step Functions style: Service carries the registry prefix, no request id in ctx.
	nr := &model.NormalizedRequest{Service: "Queue", Action: "SendMessage", AccountID: "a", Region: "r", Params: map[string]any{"MessageBody": "x"}}
	if _, err := e.reg.Dispatch(context.Background(), "Queue.SendMessage", nr); err != nil {
		t.Fatal(err)
	}
	if svc != "sqs" {
		t.Fatalf("plugin saw service %q, want the wire name sqs", svc)
	}
	if rid1 == "" || rid1 != rid2 {
		t.Fatalf("request ids must be stable across phases: %q vs %q", rid1, rid2)
	}
}

func TestInternalOriginGoesThroughTheSamePluginChain(t *testing.T) {
	e := newEnv(t)
	var origin, src string
	p := muxRoutes("p", func(m *sdk.Mux) {
		m.Before("aws", "sqs", "SendMessage", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			origin, src = c.Req.Origin, c.Req.SourceArn
			return sdk.Result{Params: map[string]any{"MessageBody": "rewritten"}}, nil
		})
	})
	if _, err := e.load(t, serve(t, p).URL); err != nil {
		t.Fatal(err)
	}
	// What a seam does: the same key, Origin=internal.
	nr := &model.NormalizedRequest{Service: "sqs", Action: "SendMessage", Cloud: model.CloudAWS, Origin: model.OriginInternal,
		AccountID: "a", Region: "r", SourceArn: "arn:aws:sns:us-east-1:1:t", Params: map[string]any{"MessageBody": "orig"}}
	if _, err := e.reg.Dispatch(context.Background(), "Queue.SendMessage", nr); err != nil {
		t.Fatal(err)
	}
	if origin != "internal" || src != "arn:aws:sns:us-east-1:1:t" {
		t.Fatalf("plugin saw origin=%q source=%q", origin, src)
	}
	if len(e.built) != 1 || e.built[0] != "builtin:rewritten" {
		t.Fatalf("the single built-in handler must receive the plugin's edit, got %v", e.built)
	}
	// A wire request carries origin "wire".
	if _, err := send(e, "x", nil); err != nil || origin != "wire" {
		t.Fatalf("wire origin = %q (%v)", origin, err)
	}
}

// ---- startup validation -------------------------------------------------------

func TestStartupConflictsAndTypos(t *testing.T) {
	cases := []struct {
		name string
		a, b func(m *sdk.Mux)
		want string
	}{
		{"two replaces", func(m *sdk.Mux) { m.Replace("aws", "sqs", "SendMessage", noop) },
			func(m *sdk.Mux) { m.Replace("aws", "sqs", "SendMessage", noop) }, "both replace Queue.SendMessage"},
		{"two wildcard replaces", func(m *sdk.Mux) { m.Replace("aws", "sqs", "*", noop) },
			func(m *sdk.Mux) { m.Replace("aws", "sqs", "*", noop) }, "both replace every action"},
		{"typo in before", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMesage", noop) }, nil, "unknown action Queue.SendMesage"},
		{"unknown service", func(m *sdk.Mux) { m.Before("aws", "nosuch", "*", noop) }, nil, "no such service"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			urls := []string{serve(t, muxRoutes("a", tc.a)).URL}
			if tc.b != nil {
				urls = append(urls, serve(t, muxRoutes("b", tc.b)).URL)
			}
			if _, err := e.load(t, urls...); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestStartupRejectsBadManifests(t *testing.T) {
	e := newEnv(t)
	bad := func(m sdk.Manifest) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(m)
		}))
	}
	for name, mf := range map[string]sdk.Manifest{
		"abi":  {ABI: 99, Name: "x"},
		"name": {ABI: sdk.ABI, Name: "Bad Name"},
	} {
		s := bad(mf)
		if _, err := e.load(t, s.URL); err == nil {
			t.Errorf("%s: want error", name)
		}
		s.Close()
	}
	// Same name twice.
	p := muxRoutes("dup", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })
	if _, err := e.load(t, serve(t, p).URL, serve(t, p).URL); err == nil || !strings.Contains(err.Error(), "duplicate plugin name") {
		t.Fatalf("duplicate names: %v", err)
	}
	if _, err := e.load(t, "ftp://x"); err == nil {
		t.Fatal("unknown scheme must fail")
	}
	// A file that is not there fails with its name, whether or not this build can load plugins.
	if _, err := e.load(t, "so:/tmp/does-not-exist.so"); err == nil || !strings.Contains(err.Error(), "does-not-exist.so") {
		t.Fatalf("so: spec: %v", err)
	}
}

func TestStartupRetriesAnUnreachablePluginThenFailsWithDetails(t *testing.T) {
	e := newEnv(t)
	// Reserve an address, release it, and bring a plugin up on it 700ms later.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	go func() {
		time.Sleep(700 * time.Millisecond)
		l, err := listenRetry(addr)
		if err != nil {
			return
		}
		p := muxRoutes("late", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })
		srv := &http.Server{Handler: sdk.Serve(p)}
		t.Cleanup(func() { srv.Close() })
		_ = srv.Serve(l)
	}()
	e.deps.StartWait = 10 * time.Second
	if _, err := e.load(t, "http://"+addr); err != nil {
		t.Fatalf("a plugin that comes up within the wait window must load: %v", err)
	}

	e2 := newEnv(t)
	e2.deps.StartWait = 600 * time.Millisecond
	dead, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddr := dead.Addr().String()
	dead.Close()
	_, err := e2.load(t, "http://user:pw@"+deadAddr)
	if err == nil || !strings.Contains(err.Error(), "unavailable after retrying") || !strings.Contains(err.Error(), deadAddr) {
		t.Fatalf("err = %v, want details naming the address and the retry window", err)
	}
	if strings.Contains(err.Error(), "pw") {
		t.Fatalf("credentials leaked into the error: %v", err)
	}
}

// ---- runtime failures ---------------------------------------------------------

func TestFailClosedErrorMapping(t *testing.T) {
	e := newEnv(t)
	var current atomic.Value
	current.Store("ok")
	inner := sdk.Serve(muxRoutes("flaky", func(m *sdk.Mux) {
		m.Before("aws", "sqs", "SendMessage", func(context.Context, *sdk.Call) (sdk.Result, error) { return sdk.Result{}, nil })
	}))
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/handle" {
			switch current.Load().(string) {
			case "slow":
				// Wait until the host gives up and hangs up, not for a fixed time that
				// a loaded machine could make shorter than the host's timeout.
				select {
				case <-r.Context().Done():
				case <-time.After(10 * time.Second):
				}
				return
			case "garbage":
				_, _ = w.Write([]byte("not json"))
				return
			case "redirect":
				http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
				return
			case "both":
				_ = json.NewEncoder(w).Encode(map[string]any{"resp": map[string]any{"data": map[string]any{}}, "err": map[string]any{"code": "X", "status": 400}})
				return
			case "status":
				_ = json.NewEncoder(w).Encode(map[string]any{"err": map[string]any{"code": "X", "status": 42}})
				return
			}
		}
		inner.ServeHTTP(w, r)
	}))
	defer s.Close()
	e.deps.Timeout = 200 * time.Millisecond
	if _, err := e.load(t, s.URL); err != nil {
		t.Fatal(err)
	}
	for m, wantCode := range map[string]string{"slow": "PluginTimeout", "garbage": "PluginProtocolError",
		"redirect": "PluginProtocolError", "both": "PluginProtocolError", "status": "PluginProtocolError"} {
		t.Run(m, func(t *testing.T) {
			e.built = nil
			current.Store(m)
			_, err := send(e, "x", nil)
			pe, ok := err.(*model.ProviderError)
			if !ok || pe.Code != wantCode {
				t.Fatalf("%s: err = %v, want %s", m, err, wantCode)
			}
			if len(e.built) != 0 {
				t.Fatalf("%s: fail-closed means the built-in must not run", m)
			}
		})
	}
}

func TestPluginRestartIsReinitialisedAndRetried(t *testing.T) {
	e := newEnv(t)
	e.deps.CallRetry = 5 * time.Second
	mk := func() sdk.Plugin {
		return muxRoutes("sticky", func(m *sdk.Mux) {
			m.Before("aws", "sqs", "SendMessage", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
				return sdk.Result{}, c.Store.Put(ctx, "k", "id", json.RawMessage(`1`))
			})
		})
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	srv := &http.Server{Handler: sdk.Serve(mk())}
	go srv.Serve(ln)
	if _, err := e.load(t, "http://"+addr); err != nil {
		t.Fatal(err)
	}
	if _, err := send(e, "one", nil); err != nil {
		t.Fatal(err)
	}
	srv.Close() // plugin process dies
	go func() {
		time.Sleep(600 * time.Millisecond) // and comes back later with no memory of /init
		l, err := listenRetry(addr)
		if err != nil {
			return
		}
		s2 := &http.Server{Handler: sdk.Serve(mk())}
		t.Cleanup(func() { s2.Close() })
		_ = s2.Serve(l)
	}()
	if _, err := send(e, "two", nil); err != nil {
		t.Fatalf("a request during a plugin restart must be retried, re-initialised and succeed: %v", err)
	}
}

// ---- store ------------------------------------------------------------------------

func TestStoreVersionsPrefixAndGate(t *testing.T) {
	e := newEnv(t)
	var errs []string
	p := muxRoutes("st", func(m *sdk.Mux) {
		m.Replace("aws", "sqs", "StoreDemo", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
			s := c.Store
			note := func(format string, ok bool) {
				if !ok {
					errs = append(errs, format)
				}
			}
			_ = s.Put(ctx, "k", "order-1", json.RawMessage(`{"a":1}`))
			_ = s.Put(ctx, "k", "my-order-1x", json.RawMessage(`{"a":2}`))
			rows, _ := s.List(ctx, "k", "order-1")
			note("List must be a true prefix match, got "+itoa(len(rows)), len(rows) == 1 && rows[0].ID == "order-1")
			ent, found, _ := s.Get(ctx, "k", "order-1")
			note("first write is version 1", found && ent.Version == 1)
			note("PutIf with the right version succeeds", s.PutIf(ctx, "k", "order-1", json.RawMessage(`{"a":3}`), 1) == nil)
			note("PutIf with a stale version conflicts", s.PutIf(ctx, "k", "order-1", json.RawMessage(`{"a":4}`), 1) == sdk.ErrConflict)
			note("PutIf(0) on an existing row conflicts", s.PutIf(ctx, "k", "order-1", json.RawMessage(`5`), 0) == sdk.ErrConflict)
			note("PutIf(0) creates a new row", s.PutIf(ctx, "k", "fresh", json.RawMessage(`5`), 0) == nil)
			note("NUL escapes are rejected", s.Put(ctx, "k", "nul", json.RawMessage(`"a\u0000b"`)) != nil)
			note("invalid kinds are rejected", s.Put(ctx, "Bad Kind", "x", json.RawMessage(`1`)) != nil)
			ent, _, _ = s.Get(ctx, "k", "order-1")
			note("conditional write bumped the version to 2", ent.Version == 2 && string(ent.Data) == `{"a":3}`)
			return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"ok": true}}}, nil
		})
	})
	h, err := e.load(t, serve(t, p).URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.reg.Dispatch(context.Background(), "Queue.StoreDemo", &model.NormalizedRequest{Service: "sqs", Action: "StoreDemo", AccountID: "a", Region: "r"}); err != nil {
		t.Fatal(err)
	}
	if len(errs) > 0 {
		t.Fatalf("store contract violations: %v", errs)
	}
	// The plugin's rows are real ResourceStore rows under its own namespace.
	rows, _ := e.res.List(context.Background(), "a", "r", "plugin:st:k", "")
	if len(rows) == 0 {
		t.Fatal("rows must be in the ResourceStore under plugin:<name>:<kind>")
	}

	// The callback gate: no token, wrong call id, wrong tenant all fail; a finished call stays dead.
	r := chi.NewRouter()
	h.Routes(r)
	call := func(token, callID, acct string) int {
		body := `{"callId":"` + callID + `","accountId":"` + acct + `","region":"r","kind":"k"}`
		req := httptest.NewRequest(http.MethodPost, storePath+"/list", bytes.NewReader([]byte(body)))
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w.Code
	}
	tok := h.plugins[0].token
	if c := call("wrong", "x", "a"); c != 401 {
		t.Fatalf("bad token = %d", c)
	}
	if c := call(tok, "no-such-call", "a"); c != 403 {
		t.Fatalf("unknown call id = %d", c)
	}
	h.begin("live", h.plugins[0], "a", "r")
	if c := call(tok, "live", "other-account"); c != 403 {
		t.Fatalf("cross-tenant = %d", c)
	}
	if c := call(tok, "live", "a"); c != 200 {
		t.Fatalf("legit callback = %d", c)
	}
	h.end("live")
	if c := call(tok, "live", "a"); c != 403 {
		t.Fatalf("a finished call must be revoked, got %d", c)
	}
}

// listenRetry rebinds an address that was reserved and released; another process
// may briefly hold it, so try for a few seconds before giving up.
func listenRetry(addr string) (net.Listener, error) {
	var err error
	for i := 0; i < 50; i++ {
		var l net.Listener
		if l, err = net.Listen("tcp", addr); err == nil {
			return l, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, err
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func TestEndWaitsForCallbacksInFlight(t *testing.T) {
	h := &Host{calls: map[string]*activeCall{}}
	p := &plugin{}
	h.begin("c", p, "a", "r")
	c, ok := h.acquire("c", p, "a", "r")
	if !ok {
		t.Fatal("acquire")
	}
	done := make(chan struct{})
	go func() { h.end("c"); close(done) }()
	select {
	case <-done:
		t.Fatal("end must wait for the running callback")
	case <-time.After(100 * time.Millisecond):
	}
	if _, again := h.acquire("c", p, "a", "r"); again {
		t.Fatal("no new callback may start once the call is ending")
	}
	c.wg.Done()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("end never returned")
	}
}

// ---- lifecycle, doctor, projection helpers -----------------------------------------

func TestLifecycleNotificationReachesPlugins(t *testing.T) {
	e := newEnv(t)
	var events []string
	p := muxRoutes("lc", func(m *sdk.Mux) {
		m.Before("aws", "sqs", "SendMessage", noop)
		m.HandleLifecycle(func(_ context.Context, ev string) { events = append(events, ev) })
	})
	h, err := e.load(t, serve(t, p).URL)
	if err != nil {
		t.Fatal(err)
	}
	h.Reset(context.Background())
	_ = h.RestoreHook().OnRestore(context.Background())
	if strings.Join(events, ",") != "reset,restore" {
		t.Fatalf("events = %v", events)
	}
	info, _ := json.Marshal(h.DoctorInfo())
	if !strings.Contains(string(info), `"name":"lc"`) {
		t.Fatalf("doctor info = %s", info)
	}
}

// A test hook (eb-tick, ...) reaches only the plugins that have a route for that
// wire service; the plugin runs its own pass when it sees the event.
func TestTickReachesOnlyPluginsThatOwnTheService(t *testing.T) {
	e := newEnv(t)
	e.reg.RegisterAll(map[string]provider.HandlerFunc{"events.PutEvents": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return provider.OK(map[string]any{}), nil
	}})
	var mu sync.Mutex
	got := map[string][]string{}
	record := func(name string) func(*sdk.Mux) {
		return func(m *sdk.Mux) {
			m.HandleLifecycle(func(_ context.Context, ev string) {
				mu.Lock()
				defer mu.Unlock()
				got[name] = append(got[name], ev)
			})
		}
	}
	events := muxRoutes("ev", func(m *sdk.Mux) { m.Replace("aws", "events", "*", noop); record("ev")(m) })
	queue := muxRoutes("q", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop); record("q")(m) })
	h, err := e.load(t, serve(t, events).URL, serve(t, queue).URL)
	if err != nil {
		t.Fatal(err)
	}
	h.Tick(context.Background(), "events")

	mu.Lock()
	defer mu.Unlock()
	if strings.Join(got["ev"], ",") != "tick:events" || len(got["q"]) != 0 {
		t.Fatalf("events = %v; only the plugin routing 'events' may be ticked", got)
	}
	if svc, ok := sdk.TickService(got["ev"][0]); !ok || svc != "events" {
		t.Fatalf("TickService(%q) = %q, %v", got["ev"][0], svc, ok)
	}
	if _, ok := sdk.TickService("reset"); ok {
		t.Fatal("reset is not a tick")
	}
}

func TestProjectionRoundTripsTypes(t *testing.T) {
	rd := io.NopCloser(strings.NewReader("s"))
	orig := map[string]any{
		"A": 1, "B": 2, "_payload": []byte(`{"k":1}`), "_meta": map[string]string{"x": "y"}, "_reader": rd,
	}
	view := project(orig)
	if _, has := view["_reader"]; has {
		t.Fatal("a stream must be hidden")
	}
	if m, ok := view["_payload"].(map[string]any); !ok || m["$base64"] == nil {
		t.Fatalf("bytes must be projected as a $base64 marker, got %v", view["_payload"])
	}

	// Plugin changes A, adds C, removes B with null, and returns _payload/_meta unchanged.
	patch := map[string]any{"A": 9, "B": nil, "_payload": view["_payload"], "_meta": view["_meta"], "C": 3}
	got := mergeParams(orig, patch)
	if got["A"] != 9 || got["C"] != 3 {
		t.Fatalf("changes lost: %v", got)
	}
	if _, has := got["B"]; has {
		t.Fatal("B was set to null by the plugin and must be removed")
	}
	// A patch that names only one key must not delete the others (H1).
	only := mergeParams(orig, map[string]any{"A": 5})
	if only["A"] != 5 || only["B"] != 2 || only["_reader"] != rd {
		t.Fatalf("keys the plugin did not mention must survive: %v", only)
	}
	// The plugin cannot delete a value it never saw.
	if kept := mergeParams(orig, map[string]any{"_reader": nil}); kept["_reader"] != rd {
		t.Fatal("a hidden stream must survive even an explicit null")
	}
	if _, ok := got["_payload"].([]byte); !ok {
		t.Fatalf("an unchanged payload must keep its []byte type, got %T", got["_payload"])
	}
	if _, ok := got["_meta"].(map[string]string); !ok {
		t.Fatalf("an unchanged typed map must keep its type, got %T", got["_meta"])
	}
	if got["_reader"] != rd {
		t.Fatal("a hidden stream must be preserved")
	}

	// Plugin changes the payload: its marker becomes []byte again.
	edited := mergeParams(orig, map[string]any{"A": 1, "B": 2, "_payload": map[string]any{"$base64": "eyJuIjoyfQ=="}, "_meta": view["_meta"]})
	if b, ok := edited["_payload"].([]byte); !ok || string(b) != `{"n":2}` {
		t.Fatalf("edited payload = %v (%T)", edited["_payload"], edited["_payload"])
	}
}

func TestResponseMarkersBecomeBytes(t *testing.T) {
	res, err := fromResult(sdk.Result{Resp: &sdk.Response{Data: map[string]any{"_payload": map[string]any{"$base64": "aGk="}}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if b, ok := res.Data["_payload"].([]byte); !ok || string(b) != "hi" {
		t.Fatalf("a replaced response must carry []byte, got %T %v", res.Data["_payload"], res.Data["_payload"])
	}
}

func TestSpecAndRedactHelpers(t *testing.T) {
	if specs := cleanSpecs([]string{"a, b", "", " c "}); strings.Join(specs, "|") != "a|b|c" {
		t.Fatalf("cleanSpecs = %v", specs)
	}
	if r := redact("http://u:p@h:1/x?token=1"); r != "http://h:1/x" {
		t.Fatalf("redact = %q", r)
	}
}
