package pluginhost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"

	"github.com/go-chi/chi/v5"
	"github.com/jaisrajms/jaiscloud/sdk"
)

// Zero plugins must leave the registry exactly as it was; loading one must not.
func TestLoadWithoutPluginsLeavesEveryHandlerIdentical(t *testing.T) {
	e := newEnv(t)
	e.reg.InterceptFallback("Queue", func(next provider.HandlerFunc) provider.HandlerFunc { return next })
	before := e.reg.Fingerprint()
	for _, specs := range [][]string{nil, {""}, {" , "}} {
		if h, err := Load(context.Background(), specs, e.deps); h != nil || err != nil {
			t.Fatalf("Load(%q) = %v, %v", specs, h, err)
		}
		if got := e.reg.Fingerprint(); got != before {
			t.Fatalf("Load(%q) changed the registry:\n%s\n--- became ---\n%s", specs, before, got)
		}
	}
	// Negative control: the fingerprint does see a wrapped handler.
	p := muxRoutes("p", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })
	if _, err := e.load(t, serve(t, p).URL); err != nil {
		t.Fatal(err)
	}
	if e.reg.Fingerprint() == before {
		t.Fatal("loading a plugin must change the fingerprint, or the test above proves nothing")
	}
}

// A plugin that fails to load must not leave metrics or handlers behind.
func TestRefusedLoadLeavesTheRegistryAlone(t *testing.T) {
	e := newEnv(t)
	before := e.reg.Fingerprint()
	typo := muxRoutes("typo", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMesage", noop) })
	if _, err := e.load(t, serve(t, typo).URL); err == nil {
		t.Fatal("a before hook on an unknown action must fail startup")
	}
	if e.reg.Fingerprint() != before {
		t.Fatal("a refused load changed the registry")
	}
}

func TestPanicInTheSeamBecomesPluginPanic(t *testing.T) {
	e := newEnv(t)
	p := muxRoutes("p", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })
	if _, err := e.load(t, serve(t, p).URL); err != nil {
		t.Fatal(err)
	}
	// A param whose encoder panics makes the host-side projection panic.
	_, err := send(e, "x", map[string]any{"Boom": panicsOnEncode{}})
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "PluginPanic" || pe.HTTPStatus != 500 || !strings.Contains(pe.Message, "plugin p") {
		t.Fatalf("err = %#v, want PluginPanic 500 naming the plugin", err)
	}
	if _, err := send(e, "after", nil); err != nil {
		t.Fatalf("the process must keep serving after a plugin panic: %v", err)
	}
}

type panicsOnEncode struct{}

func (panicsOnEncode) MarshalJSON() ([]byte, error) { panic("boom") }

// A request that cannot fit is refused before it reaches the plugin.
func TestOversizeRequestIsRefusedBeforeReachingThePlugin(t *testing.T) {
	e := newEnv(t)
	var reached atomic.Int32
	p := muxRoutes("p", func(m *sdk.Mux) {
		m.Before("aws", "sqs", "SendMessage", func(context.Context, *sdk.Call) (sdk.Result, error) {
			reached.Add(1)
			return sdk.Result{}, nil
		})
	})
	if _, err := e.load(t, serve(t, p).URL); err != nil {
		t.Fatal(err)
	}
	_, err := send(e, "x", map[string]any{"_body": make([]byte, maxBody)}) // base64 makes it ~4/3 larger
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "PluginUnsupportedPayload" || pe.HTTPStatus != 501 || !strings.Contains(pe.Message, `"_body"`) {
		t.Fatalf("err = %#v, want PluginUnsupportedPayload 501 naming _body", err)
	}
	if reached.Load() != 0 {
		t.Fatal("the plugin must not be called with an oversize request")
	}
	if _, err := send(e, "small", map[string]any{"_body": []byte("ok")}); err != nil || reached.Load() != 1 {
		t.Fatalf("a normal request must still work: %v (reached %d)", err, reached.Load())
	}
}

func TestRequiresIsRefusedWithTheHostCapabilities(t *testing.T) {
	e := newEnv(t)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(sdk.Manifest{ABI: sdk.ABI, Name: "needy", Requires: []string{"atomic-store"}})
	}))
	defer s.Close()
	_, err := e.load(t, s.URL)
	if err == nil || !strings.Contains(err.Error(), "atomic-store") || !strings.Contains(err.Error(), "provides: none") {
		t.Fatalf("err = %v, want the requirement and what the host provides", err)
	}
}

// The host re-sends /init on the sdk.ErrNotInitialized code, never on the wording.
func TestRestartDetectionUsesTheCodeNotTheText(t *testing.T) {
	e := newEnv(t)
	var inits, handles atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/manifest":
			_ = json.NewEncoder(w).Encode(sdk.Manifest{ABI: sdk.ABI, Name: "other-lang", Routes: []sdk.Route{
				{Cloud: "aws", Service: "sqs", Actions: []string{"SendMessage"}, Mode: sdk.Before}}})
		case "/init":
			inits.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/handle":
			if handles.Add(1) == 1 {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"` + sdk.ErrNotInitialized + `","detail":"any wording at all"}`))
				return
			}
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer s.Close()
	if _, err := e.load(t, s.URL); err != nil {
		t.Fatal(err)
	}
	if _, err := send(e, "x", nil); err != nil {
		t.Fatalf("a not_initialized answer must be healed by a new /init and a retry: %v", err)
	}
	if inits.Load() != 2 || handles.Load() != 2 {
		t.Fatalf("inits=%d handles=%d, want 2 and 2", inits.Load(), handles.Load())
	}

	// A different 409, or the old prose with another status, is only a protocol error.
	handles.Store(0)
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/manifest":
			_ = json.NewEncoder(w).Encode(sdk.Manifest{ABI: sdk.ABI, Name: "prose", Routes: []sdk.Route{
				{Cloud: "aws", Service: "sqs", Actions: []string{"Boom"}, Mode: sdk.After}}})
		case "/init":
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"not initialised"}`))
		}
	}))
	defer s2.Close()
	e2 := newEnv(t)
	if _, err := e2.load(t, s2.URL); err != nil {
		t.Fatal(err)
	}
	_, err := e2.reg.Dispatch(context.Background(), "Queue.Boom", &model.NormalizedRequest{Service: "sqs", Action: "Boom", AccountID: "a", Region: "r"})
	if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "PluginProtocolError" {
		t.Fatalf("err = %#v, want PluginProtocolError (the wording is not a contract)", err)
	}
}

// A built-in that wraps a ProviderError must still reach the after hook (N1).
func TestAfterHookSeesAWrappedProviderError(t *testing.T) {
	e := newEnv(t)
	e.reg.RegisterAll(map[string]provider.HandlerFunc{
		"Queue.Wrapped": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return nil, fmt.Errorf("while serving: %w", model.NewProviderError("Inner", "inner failure", 400))
		},
	})
	var saw string
	p := muxRoutes("p", func(m *sdk.Mux) {
		m.After("aws", "sqs", "Wrapped", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			if c.Err != nil {
				saw = c.Err.Code
			}
			return sdk.Result{}, nil
		})
	})
	if _, err := e.load(t, serve(t, p).URL); err != nil {
		t.Fatal(err)
	}
	_, _ = e.reg.Dispatch(context.Background(), "Queue.Wrapped", &model.NormalizedRequest{Service: "sqs", Action: "Wrapped", AccountID: "a", Region: "r"})
	if saw != "Inner" {
		t.Fatalf("after hook saw error code %q, want Inner", saw)
	}
}

func TestDoctorReportsTheManifestABI(t *testing.T) {
	e := newEnv(t)
	p := muxRoutes("p", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })
	h, err := e.load(t, serve(t, p).URL)
	if err != nil {
		t.Fatal(err)
	}
	h.plugins[0].abi = 7 // as if the manifest had declared another supported ABI
	if got := h.DoctorInfo().([]map[string]any)[0]["abi"]; got != 7 {
		t.Fatalf("doctor abi = %v, want the plugin's own", got)
	}
}

// Plugin A and B use the same kind and id; neither can see the other's row.
func TestPluginsCannotSeeEachOthersRows(t *testing.T) {
	e := newEnv(t)
	writer := func(name, action, val string) *sdk.Mux {
		return muxRoutes(name, func(m *sdk.Mux) {
			m.Replace("aws", "sqs", action, func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
				err := c.Store.Put(ctx, "k", "same", json.RawMessage(`"`+val+`"`))
				return sdk.Result{Resp: &sdk.Response{Data: map[string]any{}}}, err
			})
		})
	}
	var bSaw string
	reader := muxRoutes("b", func(m *sdk.Mux) {
		m.Replace("aws", "sqs", "ReadB", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
			ent, found, err := c.Store.Get(ctx, "k", "same")
			if err != nil {
				return sdk.Result{}, err
			}
			if found {
				bSaw = string(ent.Data)
			}
			// b may not name a's namespace through the kind either.
			if c.Store.Put(ctx, "plugin:a:k", "same", json.RawMessage(`"hijack"`)) == nil {
				bSaw += "+hijacked"
			}
			return sdk.Result{Resp: &sdk.Response{Data: map[string]any{}}}, nil
		})
	})
	a := writer("a", "WriteA", "from-a")
	if _, err := e.load(t, serve(t, a).URL, serve(t, reader).URL); err != nil {
		t.Fatal(err)
	}
	dispatch := func(action string) {
		t.Helper()
		_, err := e.reg.Dispatch(context.Background(), "Queue."+action, &model.NormalizedRequest{Service: "sqs", Action: action, AccountID: "a1", Region: "r1"})
		if err != nil {
			t.Fatalf("%s: %v", action, err)
		}
	}
	dispatch("WriteA")
	dispatch("ReadB")
	if bSaw != "" {
		t.Fatalf("plugin b saw %q; it must see nothing of a's rows", bSaw)
	}
	if rows, _ := e.res.List(context.Background(), "a1", "r1", "plugin:a:k", ""); len(rows) != 1 {
		t.Fatalf("a's row must still be a's alone, got %d rows", len(rows))
	}
}

// Ids with NUL or absurd length are a 400, and a list too large to return is a 413.
func TestStoreBoundsAreEnforced(t *testing.T) {
	e := newEnv(t)
	p := muxRoutes("st", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })
	h, err := e.load(t, serve(t, p).URL)
	if err != nil {
		t.Fatal(err)
	}
	r := chi.NewRouter()
	h.Routes(r)
	h.begin("live", h.plugins[0], "a", "r")
	defer h.end("live")
	call := func(op string, req sdk.StoreRequest) int {
		req.CallID, req.AccountID, req.Region, req.Kind = "live", "a", "r", "k"
		b, _ := json.Marshal(req)
		rq := httptest.NewRequest(http.MethodPost, storePath+"/"+op, bytes.NewReader(b))
		rq.Header.Set("Authorization", "Bearer "+h.plugins[0].token)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, rq)
		return w.Code
	}
	for op, req := range map[string]sdk.StoreRequest{
		"put":    {ID: "a\x00b", Data: json.RawMessage(`1`)},
		"get":    {ID: "a\x00b"},
		"delete": {ID: strings.Repeat("x", maxIDLen+1)},
		"list":   {IDPrefix: "p\x00"},
	} {
		if c := call(op, req); c != http.StatusBadRequest {
			t.Errorf("%s with a bad id = %d, want 400", op, c)
		}
	}
	if c := call("put", sdk.StoreRequest{ID: strings.Repeat("x", maxIDLen), Data: json.RawMessage(`1`)}); c != http.StatusOK {
		t.Errorf("an id of exactly %d bytes = %d, want 200", maxIDLen, c)
	}
	for i := 0; i <= maxListRows; i++ {
		data, _ := json.Marshal(envelope{V: 1, D: json.RawMessage(`1`)})
		if _, err := e.res.UpsertAtomic(context.Background(), "a", "r", "plugin:st:big", fmt.Sprintf("row-%05d", i),
			func(store.ResourceEntry, bool) (store.ResourceEntry, error) {
				return store.ResourceEntry{Type: "plugin:st:big", ID: fmt.Sprintf("row-%05d", i), Data: data}, nil
			}); err != nil {
			t.Fatal(err)
		}
	}
	big := sdk.StoreRequest{CallID: "live", AccountID: "a", Region: "r", Kind: "big", IDPrefix: "row-"}
	b, _ := json.Marshal(big)
	rq := httptest.NewRequest(http.MethodPost, storePath+"/list", bytes.NewReader(b))
	rq.Header.Set("Authorization", "Bearer "+h.plugins[0].token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, rq)
	if w.Code != http.StatusRequestEntityTooLarge || !strings.Contains(w.Body.String(), "idPrefix") {
		t.Fatalf("an unbounded list = %d %q, want 413 telling the plugin to narrow it", w.Code, w.Body.String())
	}
}
