package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newMux() *Mux {
	m := NewMux("t", "1")
	m.Before("aws", "sqs", "SendMessage", func(context.Context, *Call) (Result, error) { return Result{}, nil })
	m.Before("aws", "sqs", "ReceiveMessage", func(context.Context, *Call) (Result, error) { return Result{}, nil })
	m.Replace("aws", "sqs", "*", func(_ context.Context, c *Call) (Result, error) {
		return Result{Resp: &Response{Data: map[string]any{"action": c.Req.Action}}}, nil
	})
	return m
}

func TestMuxManifestGroupsActionsPerMode(t *testing.T) {
	mf := newMux().Manifest()
	if mf.ABI != ABI || mf.Name != "t" || len(mf.Routes) != 2 {
		t.Fatalf("manifest = %+v", mf)
	}
	for _, r := range mf.Routes {
		if r.Mode == Before && strings.Join(r.Actions, ",") != "ReceiveMessage,SendMessage" {
			t.Fatalf("before actions = %v (want sorted, grouped)", r.Actions)
		}
	}
}

func TestMuxDispatchFallsBackToWildcardAndNotHandled(t *testing.T) {
	m := newMux()
	res, _ := m.Handle(context.Background(), &Call{Phase: Replace, Req: &Request{Cloud: "aws", Service: "sqs", Action: "Whatever"}})
	if res.Resp == nil || res.Resp.Data["action"] != "Whatever" {
		t.Fatalf("wildcard replace = %+v", res)
	}
	res, _ = m.Handle(context.Background(), &Call{Phase: Replace, Req: &Request{Cloud: "aws", Service: "s3", Action: "X"}})
	if !res.NotHandled {
		t.Fatal("an unregistered replace must be NotHandled")
	}
	res, _ = m.Handle(context.Background(), &Call{Phase: After, Req: &Request{Cloud: "aws", Service: "sqs", Action: "SendMessage"}})
	if res.NotHandled || res.Resp != nil || res.Err != nil {
		t.Fatalf("an unregistered after must be a no-op, got %+v", res)
	}
}

func do(h http.Handler, method, path, ctype, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestServeRefusesBrowsersAndNonJSON(t *testing.T) {
	h := Serve(newMux())
	if w := do(h, "GET", "/manifest", "", "", nil); w.Code != 200 {
		t.Fatalf("manifest = %d", w.Code)
	}
	if w := do(h, "GET", "/manifest", "", "", map[string]string{"Origin": "https://evil.example"}); w.Code != 403 {
		t.Fatalf("a request with an Origin header = %d, want 403", w.Code)
	}
	// text/plain is a CORS "simple" content type: no preflight, so it must be refused by the server.
	if w := do(h, "POST", "/init", "text/plain", `{"abi":1}`, nil); w.Code != 415 {
		t.Fatalf("text/plain POST = %d, want 415", w.Code)
	}
	if w := do(h, "POST", "/init", "application/json", `{"abi":99}`, nil); w.Code != 409 {
		t.Fatalf("ABI mismatch = %d, want 409", w.Code)
	}
}

func TestServeSecret(t *testing.T) {
	h := Serve(newMux(), WithSecret("s3cret"))
	if w := do(h, "GET", "/manifest", "", "", nil); w.Code != 401 {
		t.Fatalf("no secret = %d", w.Code)
	}
	if w := do(h, "GET", "/manifest", "", "", map[string]string{"Authorization": "Bearer nope"}); w.Code != 401 {
		t.Fatalf("wrong secret = %d", w.Code)
	}
	if w := do(h, "GET", "/manifest", "", "", map[string]string{"Authorization": "Bearer s3cret"}); w.Code != 200 {
		t.Fatalf("right secret = %d", w.Code)
	}
}

func TestHandleBeforeInitAsksForReinit(t *testing.T) {
	h := Serve(newMux())
	body, _ := json.Marshal(HandleRequest{Phase: Replace, Req: &Request{Cloud: "aws", Service: "sqs", Action: "X"}})
	w := do(h, "POST", "/handle", "application/json", string(body), nil)
	if w.Code != 409 || !strings.Contains(w.Body.String(), ErrNotInitialized) {
		t.Fatalf("handle before init = %d %s", w.Code, w.Body)
	}
	if w := do(h, "POST", "/init", "application/json", `{"abi":1,"store":{"url":"http://x","token":"t"}}`, nil); w.Code != 204 {
		t.Fatalf("init = %d", w.Code)
	}
	if w := do(h, "POST", "/init", "application/json", `{"abi":1,"store":{"url":"http://y","token":"t2"}}`, nil); w.Code != 204 {
		t.Fatalf("a repeated init (after an emulator restart) must be accepted, got %d", w.Code)
	}
	if w := do(h, "POST", "/handle", "application/json", string(body), nil); w.Code != 200 {
		t.Fatalf("handle after init = %d %s", w.Code, w.Body)
	}
}

func TestLifecycleReachesListener(t *testing.T) {
	m := newMux()
	var got string
	m.HandleLifecycle(func(_ context.Context, ev string) { got = ev })
	h := Serve(m)
	if w := do(h, "POST", "/lifecycle", "application/json", `{"event":"reset"}`, nil); w.Code != 204 || got != EventReset {
		t.Fatalf("lifecycle = %d, event %q", w.Code, got)
	}
}

func TestHTTPStoreMapsConflictAndSendsCredentials(t *testing.T) {
	var gotAuth string
	var gotReq StoreRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		switch {
		case strings.HasSuffix(r.URL.Path, "/put") && gotReq.IfVersion != nil:
			w.WriteHeader(http.StatusConflict)
		case strings.HasSuffix(r.URL.Path, "/get"):
			_ = json.NewEncoder(w).Encode(StoreResponse{Found: true, Entry: &Entry{ID: gotReq.ID, Data: json.RawMessage(`1`), Version: 3}})
		default:
			_ = json.NewEncoder(w).Encode(StoreResponse{})
		}
	}))
	defer srv.Close()
	s := &httpStore{url: srv.URL, token: "tok", callID: "call-1", account: "acct", region: "reg"}
	ctx := context.Background()
	if err := s.PutIf(ctx, "k", "id", json.RawMessage(`1`), 7); err != ErrConflict {
		t.Fatalf("PutIf conflict = %v", err)
	}
	if gotAuth != "Bearer tok" || gotReq.CallID != "call-1" || gotReq.AccountID != "acct" || *gotReq.IfVersion != 7 {
		t.Fatalf("callback credentials/body wrong: %q %+v", gotAuth, gotReq)
	}
	e, found, err := s.Get(ctx, "k", "id")
	if err != nil || !found || e.Version != 3 {
		t.Fatalf("Get = %+v %v %v", e, found, err)
	}
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(Result{})
	if !strings.Contains(buf.String(), `"params":null`) {
		t.Fatalf("an unset Params must encode as null (unchanged), got %s", buf.String())
	}
}

func TestContentTypeIsComparedExactly(t *testing.T) {
	srv := httptest.NewServer(Serve(NewMux("p", "0")))
	defer srv.Close()
	for ct, want := range map[string]int{
		"application/json":                http.StatusConflict, // reaches the handler (not initialised)
		"application/json; charset=utf-8": http.StatusConflict,
		"application/jsonx":               http.StatusUnsupportedMediaType,
		"text/plain":                      http.StatusUnsupportedMediaType,
		"":                                http.StatusUnsupportedMediaType,
	} {
		resp, err := http.Post(srv.URL+"/handle", ct, strings.NewReader(`{"phase":"before","req":{}}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("Content-Type %q = %d, want %d", ct, resp.StatusCode, want)
		}
	}
}

func TestTickService(t *testing.T) {
	for in, want := range map[string]string{"tick:events": "events", "tick:": "", "reset": "", "tick": ""} {
		got, ok := TickService(in)
		if got != want || ok != (want != "") {
			t.Errorf("TickService(%q) = %q, %v", in, got, ok)
		}
	}
}

func TestManifestRequiresIsOmittedWhenEmpty(t *testing.T) {
	b, _ := json.Marshal(Manifest{ABI: ABI, Name: "p"})
	if strings.Contains(string(b), "requires") {
		t.Fatalf("an empty Requires must not appear on the wire: %s", b)
	}
}
