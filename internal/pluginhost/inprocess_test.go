package pluginhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"jaiscloud/internal/model"

	"github.com/go-chi/chi/v5"
	"github.com/jaisrajms/jaiscloud/sdk"
)

// loadLinked loads plugins that are already in the test process, the way a .so's New
// result is loaded, so the in-process path is covered without building a plugin.
func (e *env) loadLinked(t *testing.T, plugins ...sdk.Plugin) (*Host, error) {
	t.Helper()
	return load(context.Background(), nil, plugins, e.deps)
}

func inProc(name string, f func(m *sdk.Mux)) *sdk.Mux {
	m := sdk.NewMux(name, "0.0.1").Require(sdk.CapInProcess)
	f(m)
	return m
}

// Next runs the built-in with the parameters the plugin chose, and hands back what it
// answered, stream included.
func TestInProcessReplaceWrapsTheBuiltInWithNext(t *testing.T) {
	e := newEnv(t)
	_, err := e.loadLinked(t, inProc("wrap", func(m *sdk.Mux) {
		m.Replace("aws", "sqs", "SendMessage", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
			params := map[string]any{}
			for k, v := range c.Req.Params {
				params[k] = v
			}
			params["MessageBody"] = strings.ToUpper(params["MessageBody"].(string))
			resp, perr := c.Next(ctx, &sdk.Request{Params: params})
			if perr != nil {
				return sdk.Result{Err: perr}, nil
			}
			resp.Data["wrapped"] = true
			return sdk.Result{Resp: resp}, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := send(e, "hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(e.built, ","); got != "builtin:HELLO" {
		t.Fatalf("the built-in ran as %q, want it to see the plugin's body", got)
	}
	if resp.Data["wrapped"] != true || resp.Data["MessageId"] != "m-1" {
		t.Fatalf("response = %+v", resp.Data)
	}
	if _, ok := resp.Data["_stream"].(io.Reader); !ok {
		t.Fatalf("the built-in's stream must reach the client through Next, got %T", resp.Data["_stream"])
	}
}

// An error from the built-in comes back through Next as an sdk.Error the plugin can return.
func TestInProcessNextCarriesTheBuiltInsError(t *testing.T) {
	e := newEnv(t)
	_, err := e.loadLinked(t, inProc("errs", func(m *sdk.Mux) {
		m.Replace("aws", "sqs", "Boom", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
			_, perr := c.Next(ctx, nil)
			if perr == nil {
				t.Error("Next must report the built-in's error")
				return sdk.Result{}, nil
			}
			perr.Message = "wrapped: " + perr.Message
			return sdk.Result{Err: perr}, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	nr := &model.NormalizedRequest{Service: "sqs", Action: "Boom", AccountID: "1", Region: "r", Params: map[string]any{}}
	_, derr := e.reg.Dispatch(context.Background(), "Queue.Boom", nr)
	var pe *model.ProviderError
	if !errors.As(derr, &pe) || pe.Code != "QueueDoesNotExist" || pe.HTTPStatus != 400 || !strings.HasPrefix(pe.Message, "wrapped: ") {
		t.Fatalf("error = %v", derr)
	}
}

// An in-process answer is used as it is: bytes, streams and a map that happens to look
// like the JSON byte marker are not rewritten.
func TestInProcessAnswersAreNotRewritten(t *testing.T) {
	e := newEnv(t)
	body := io.NopCloser(strings.NewReader("object bytes"))
	_, err := e.loadLinked(t, inProc("raw", func(m *sdk.Mux) {
		m.Replace("aws", "sqs", "SendMessage", func(context.Context, *sdk.Call) (sdk.Result, error) {
			return sdk.Result{Resp: &sdk.Response{Data: map[string]any{
				"_stream": body, "_body": []byte("abc"), "marker": map[string]any{"$base64": "aGk="},
			}}}, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := send(e, "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Data["_stream"] != body {
		t.Errorf("stream = %v", resp.Data["_stream"])
	}
	if b, ok := resp.Data["_body"].([]byte); !ok || string(b) != "abc" {
		t.Errorf("_body = %#v", resp.Data["_body"])
	}
	if m, ok := resp.Data["marker"].(map[string]any); !ok || m["$base64"] != "aGk=" {
		t.Errorf("a map that looks like the byte marker was rewritten: %#v", resp.Data["marker"])
	}
	if len(e.built) != 0 {
		t.Errorf("the built-in must not run when the plugin answers, ran %v", e.built)
	}
}

// before sees the real values, patches them as it likes, and after may replace a
// response that carries a stream (an out-of-process plugin may not).
func TestInProcessBeforeAndAfterSeeTheRealValues(t *testing.T) {
	e := newEnv(t)
	var sawBytes []byte
	_, err := e.loadLinked(t, inProc("around", func(m *sdk.Mux) {
		m.Before("aws", "sqs", "SendMessage", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			sawBytes, _ = c.Req.Params["_payload"].([]byte)
			return sdk.Result{Params: map[string]any{"MessageBody": "patched", "_payload": nil, "extra": []byte("e")}}, nil
		})
		m.After("aws", "sqs", "SendMessage", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			if _, ok := c.Resp.Data["_stream"].(io.Reader); !ok {
				t.Errorf("after must see the stream, got %T", c.Resp.Data["_stream"])
			}
			return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"replaced": true}}}, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := send(e, "orig", map[string]any{"_payload": []byte("raw")})
	if err != nil {
		t.Fatal(err)
	}
	if string(sawBytes) != "raw" {
		t.Errorf("before saw %q for the bytes param", sawBytes)
	}
	if got := strings.Join(e.built, ","); got != "builtin:patched" {
		t.Errorf("the built-in ran as %q", got)
	}
	if resp.Data["replaced"] != true || len(resp.Data) != 1 {
		t.Errorf("after's replacement = %+v", resp.Data)
	}
}

// The plugin's Store works while its call is in flight and stops when the call ends.
func TestInProcessStoreIsGatedToTheCall(t *testing.T) {
	e := newEnv(t)
	var kept sdk.Store
	var versions []int64
	h, err := e.loadLinked(t, inProc("rows", func(m *sdk.Mux) {
		m.Before("aws", "sqs", "SendMessage", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
			kept = c.Store
			if err := c.Store.Put(ctx, "counter", "n", json.RawMessage(`1`)); err != nil {
				return sdk.Result{}, err
			}
			ent, found, err := c.Store.Get(ctx, "counter", "n")
			if err != nil || !found {
				return sdk.Result{}, errors.New("row not found")
			}
			versions = append(versions, ent.Version)
			if err := c.Store.PutIf(ctx, "counter", "n", json.RawMessage(`2`), ent.Version+5); !errors.Is(err, sdk.ErrConflict) {
				return sdk.Result{}, errors.New("a stale PutIf must report sdk.ErrConflict")
			}
			if err := c.Store.PutIf(ctx, "counter", "n", json.RawMessage(`2`), ent.Version); err != nil {
				return sdk.Result{}, err
			}
			rows, err := c.Store.List(ctx, "counter", "")
			if err != nil || len(rows) != 1 || string(rows[0].Data) != "2" {
				return sdk.Result{}, errors.New("list: wrong rows")
			}
			return sdk.Result{}, nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := send(e, "x", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.res.Get(context.Background(), "111111111111", "us-east-1", "plugin:rows:counter", "n"); err != nil {
		t.Fatalf("the row must be in the emulator's resource store under the plugin's type: %v", err)
	}
	if err := kept.Put(context.Background(), "counter", "late", json.RawMessage(`1`)); err == nil {
		t.Fatal("a Store used after its call finished must be refused")
	}

	// An in-process plugin has no HTTP credential: an empty bearer must not authenticate as it.
	r := chi.NewRouter()
	h.Routes(r)
	req := httptest.NewRequest(http.MethodPost, storePath+"/get", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("a callback without a token got %d, want 401", rec.Code)
	}
}

// A plugin that needs the in-process layer is refused when it is loaded as a sidecar,
// and the message says how to load it.
func TestRequiringInProcessFailsOverHTTP(t *testing.T) {
	e := newEnv(t)
	p := inProc("needs-inproc", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })
	_, err := e.load(t, serve(t, p).URL)
	if err == nil || !strings.Contains(err.Error(), "in-process") || !strings.Contains(err.Error(), "so:<path>") {
		t.Fatalf("error = %v", err)
	}
	if _, err := e.loadLinked(t, p); err != nil {
		t.Fatalf("the same plugin must load in-process: %v", err)
	}
	// Anything else it asks for is still unavailable.
	odd := muxRoutes("odd", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop); m.Require("teleport") })
	if _, err := e.loadLinked(t, odd); err == nil || !strings.Contains(err.Error(), "teleport") || !strings.Contains(err.Error(), "in-process") {
		t.Fatalf("unknown capability: %v", err)
	}
}

// A panic, in the plugin or in the built-in it called, is an error for that request.
func TestInProcessPanicsAreContained(t *testing.T) {
	e := newEnv(t)
	_, err := e.loadLinked(t, inProc("boom", func(m *sdk.Mux) {
		m.Replace("aws", "sqs", "SendMessage", func(context.Context, *sdk.Call) (sdk.Result, error) { panic("plugin bug") })
	}))
	if err != nil {
		t.Fatal(err)
	}
	_, derr := send(e, "x", nil)
	var pe *model.ProviderError
	if !errors.As(derr, &pe) || pe.Code != codePluginPanic || pe.HTTPStatus != 500 {
		t.Fatalf("error = %v", derr)
	}
	if _, derr = send(e, "again", nil); derr == nil {
		t.Fatal("the plugin still panics; the process must keep serving")
	}
}

func TestInProcessHandleErrorIsAPluginError(t *testing.T) {
	e := newEnv(t)
	_, err := e.loadLinked(t, inProc("bad", func(m *sdk.Mux) {
		m.Replace("aws", "sqs", "SendMessage", func(context.Context, *sdk.Call) (sdk.Result, error) {
			return sdk.Result{}, errors.New("disk full")
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	_, derr := send(e, "x", nil)
	var pe *model.ProviderError
	if !errors.As(derr, &pe) || pe.Code != "PluginError" || pe.HTTPStatus != 500 || !strings.Contains(pe.Message, "disk full") {
		t.Fatalf("error = %v", derr)
	}
}

// statefulPlugin keeps messages in memory and reports them to reset and snapshots.
type statefulPlugin struct {
	*sdk.Mux
	mu       sync.Mutex
	items    []string
	events   []string
	shutdown *[]string
}

func (s *statefulPlugin) Reset(context.Context) { s.mu.Lock(); s.items = nil; s.mu.Unlock() }
func (s *statefulPlugin) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.NewEncoder(w).Encode(s.items)
}
func (s *statefulPlugin) Restore(_ context.Context, r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.NewDecoder(r).Decode(&s.items)
}
func (s *statefulPlugin) IsEmpty(context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items) == 0, nil
}
func (s *statefulPlugin) OnLifecycle(_ context.Context, ev string) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
}
func (s *statefulPlugin) Shutdown(context.Context) error {
	*s.shutdown = append(*s.shutdown, s.Manifest().Name)
	return nil
}

func TestInProcessStateReachesResetSnapshotsAndShutdown(t *testing.T) {
	e := newEnv(t)
	var order []string
	mk := func(name string) *statefulPlugin {
		m := inProc(name, func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })
		return &statefulPlugin{Mux: m, shutdown: &order, items: []string{"a"}}
	}
	first, second := mk("first"), mk("second")
	h, err := e.loadLinked(t, first, second)
	if err != nil {
		t.Fatal(err)
	}
	snaps := h.Snapshotters()
	if len(snaps) != 2 || snaps["plugin:first"] == nil || snaps["plugin:second"] == nil {
		t.Fatalf("snapshotters = %v", snaps)
	}
	var buf bytes.Buffer
	if err := snaps["plugin:first"].Snapshot(context.Background(), &buf); err != nil || strings.TrimSpace(buf.String()) != `["a"]` {
		t.Fatalf("snapshot = %q, %v", buf.String(), err)
	}

	h.Reset(context.Background())
	if empty, _ := first.IsEmpty(context.Background()); !empty {
		t.Error("reset must clear a Stateful plugin's own state")
	}
	h.RestoreHook().OnRestore(context.Background())
	if got := strings.Join(first.events, ","); got != sdk.EventReset+","+sdk.EventRestore {
		t.Errorf("lifecycle events = %q", got)
	}

	info, _ := json.Marshal(h.DoctorInfo())
	if !strings.Contains(string(info), `"transport":"in-process"`) || !strings.Contains(string(info), `"stateful":true`) {
		t.Errorf("doctor = %s", info)
	}

	h.Shutdown(context.Background())
	if strings.Join(order, ",") != "second,first" {
		t.Errorf("shutdown order = %v, want last loaded first", order)
	}
}

// A plugin that panics outside a request (its lifecycle handler) does not take the host down.
func TestInProcessLifecyclePanicIsContained(t *testing.T) {
	e := newEnv(t)
	m := inProc("lc", func(m *sdk.Mux) {
		m.Before("aws", "sqs", "SendMessage", noop)
		m.HandleLifecycle(func(context.Context, string) { panic("lifecycle bug") })
	})
	h, err := e.loadLinked(t, m)
	if err != nil {
		t.Fatal(err)
	}
	h.Reset(context.Background()) // must return
}

func TestInProcessPanicWhileLoadingIsAnError(t *testing.T) {
	e := newEnv(t)
	_, err := e.loadLinked(t, panicOnInit{inProc("pi", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })})
	if err == nil || !strings.Contains(err.Error(), "panicked while loading") {
		t.Fatalf("error = %v", err)
	}
}

type panicOnInit struct{ *sdk.Mux }

func (panicOnInit) Init(context.Context, sdk.Host) error { panic("init bug") }

func TestInProcessAndHTTPPluginsKeepTheirNamesUnique(t *testing.T) {
	e := newEnv(t)
	p := muxRoutes("same", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })
	if _, err := load(context.Background(), []string{serve(t, p).URL}, []sdk.Plugin{p}, e.deps); err == nil || !strings.Contains(err.Error(), "duplicate plugin name") {
		t.Fatalf("error = %v", err)
	}
}

func TestBuildInfoDescribesThisBuild(t *testing.T) {
	b := Build()
	if b.Go == "" || b.OS == "" || b.Arch == "" {
		t.Fatalf("build = %+v", b)
	}
	if b.InProcess != InProcessSupported() {
		t.Error("InProcess must follow the loader")
	}
	if !strings.Contains(b.Line(), "in-process plugins:") || !strings.Contains(b.String(), "built with go") {
		t.Errorf("line = %q, string = %q", b.Line(), b.String())
	}
}
