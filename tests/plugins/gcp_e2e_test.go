//go:build plugin_e2e

package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/jaisrajms/jaiscloud/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// The GCP tests drive the real jaiscloud-gcp binary the same way the AWS ones
// drive jaiscloud-aws: an in-test sidecar built on /sdk, the emulator's REST
// surface as the client. JAISCLOUD_GCP_BIN reuses a prebuilt binary.
var (
	gcpOnce sync.Once
	gcpPath string
	gcpErr  error
)

func gcpBinary(t *testing.T) string {
	t.Helper()
	gcpOnce.Do(func() {
		if p := os.Getenv("JAISCLOUD_GCP_BIN"); p != "" {
			gcpPath = p
			return
		}
		dir, err := os.MkdirTemp("", "jaiscloud-plugin-e2e-gcp")
		if err != nil {
			gcpErr = err
			return
		}
		gcpPath = filepath.Join(dir, "jaiscloud-gcp")
		if out, err := exec.Command("go", "build", "-o", gcpPath, "../../cmd/jaiscloud-gcp/").CombinedOutput(); err != nil {
			gcpErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if gcpErr != nil {
		t.Fatal(gcpErr)
	}
	return gcpPath
}

// startGCP starts jaiscloud-gcp on REST only (no gRPC/HMS listeners to collide).
func startGCP(t *testing.T, args ...string) *emulator {
	t.Helper()
	e, _ := commandOf(t, gcpBinary(t), append([]string{"--transports", "rest"}, args...)...)
	return run(t, e)
}

// gcpREST sends a JSON request to the emulator and decodes the JSON answer.
func (e *emulator) gcpREST(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, e.base+path, rd)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.fatal(t, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func gcpSidecar(t *testing.T, m *sdk.Mux, opts ...sdk.Option) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(sdk.Serve(m, opts...))
	t.Cleanup(s.Close)
	return s
}

func TestGCPRESTBeforeAfterReplace(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	note := func(s string) { mu.Lock(); seen = append(seen, s); mu.Unlock() }

	m := sdk.NewMux("gcp-smoke", "0.1.0")
	m.Before("gcp", "pubsub", "TopicCreate", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		note("before:" + c.Req.Action)
		return sdk.Result{}, nil
	})
	m.After("gcp", "pubsub", "TopicCreate", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		note("after:" + c.Req.Action)
		return sdk.Result{}, nil
	})
	m.Replace("gcp", "pubsub", "TopicGet", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"name": "replaced-by-plugin"}}}, nil
	})
	s := gcpSidecar(t, m)

	e := startGCP(t, "--plugin", s.URL)

	if code, out := e.gcpREST(t, http.MethodPut, "/v1/projects/p1/topics/t1", map[string]any{}); code != 200 {
		t.Fatalf("create topic: %d %v\n%s", code, out, e.logs())
	}
	if code, out := e.gcpREST(t, http.MethodGet, "/v1/projects/p1/topics/t1", nil); code != 200 || out["name"] != "replaced-by-plugin" {
		t.Fatalf("get topic must be answered by the plugin: %d %v\n%s", code, out, e.logs())
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(seen) != "[before:TopicCreate after:TopicCreate]" {
		t.Fatalf("hooks seen = %v", seen)
	}
}


// startGCPBoth starts jaiscloud-gcp with REST and gRPC on free ports and
// returns the emulator plus a gRPC connection to it.
func startGCPBoth(t *testing.T, args ...string) (*emulator, *grpc.ClientConn) {
	t.Helper()
	grpcAddr, hmsAddr := freeAddr(t), freeAddr(t)
	port := func(a string) string { return a[len(a)-len(a[lastColon(a)+1:]):] }
	all := append([]string{"--transports", "rest,grpc", "--grpc-port", port(grpcAddr), "--hms-port", port(hmsAddr)}, args...)
	e, _ := commandOf(t, gcpBinary(t), all...)
	run(t, e)
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return e, conn
}

func lastColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}

// pubsubPlugin overrides Pub/Sub keys; one plugin answers both transports and
// branches on Req.Transport, because a gRPC call carries protojson params.
func pubsubPlugin(note func(string)) *sdk.Mux {
	m := sdk.NewMux("gcp-grpc", "0.1.0")
	m.Before("gcp", "pubsub", "TopicCreate", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		note(fmt.Sprintf("before:%s:%s", c.Req.Action, c.Req.Transport))
		if name, _ := c.Req.Params["name"].(string); name == "projects/p1/topics/blocked" {
			return sdk.Result{Err: &sdk.Error{Code: "FailedPrecondition", Message: "blocked by gcp-grpc", Status: 400}}, nil
		}
		// A patch of the request: for gRPC it is the Topic message itself.
		return sdk.Result{Params: map[string]any{"labels": map[string]any{"audited": "yes"}}}, nil
	})
	m.After("gcp", "pubsub", "TopicCreate", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		name, _ := c.Resp.Data["name"].(string)
		note(fmt.Sprintf("after:%s:%s:%s", c.Req.Action, c.Req.Transport, name))
		return sdk.Result{}, nil
	})
	m.Replace("gcp", "pubsub", "TopicGet", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		note(fmt.Sprintf("replace:%s:%s", c.Req.Action, c.Req.Transport))
		return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"name": "replaced-by-plugin"}}}, nil
	})
	m.After("gcp", "pubsub", "TopicDelete", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		note(fmt.Sprintf("after-delete:%s:err=%v", c.Req.Transport, c.Err != nil))
		return sdk.Result{}, nil
	})
	return m
}

func TestGCPGRPCSameKeyAsREST(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	note := func(s string) { mu.Lock(); seen = append(seen, s); mu.Unlock() }
	s := gcpSidecar(t, pubsubPlugin(note))
	e, conn := startGCPBoth(t, "--plugin", s.URL)
	pub := pubsubpb.NewPublisherClient(conn)
	ctx := context.Background()

	// before patches the gRPC request message; after sees the protojson answer.
	topic, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/p1/topics/t1"})
	if err != nil {
		t.Fatalf("CreateTopic: %v\n%s", err, e.logs())
	}
	if topic.Labels["audited"] != "yes" {
		t.Fatalf("the before hook's patch did not reach the built-in: %v", topic)
	}

	// replace answers instead of the built-in, in the RPC's response type.
	got, err := pub.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: "projects/p1/topics/t1"})
	if err != nil || got.Name != "replaced-by-plugin" {
		t.Fatalf("GetTopic must be answered by the plugin: %v %v\n%s", got, err, e.logs())
	}

	// A plugin error becomes the matching gRPC status.
	_, err = pub.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/p1/topics/blocked"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("blocked create = %v, want FailedPrecondition", err)
	}

	// The same key over REST still works, with its own (REST) shape.
	if code, out := e.gcpREST(t, http.MethodGet, "/v1/projects/p1/topics/t1", nil); code != 200 || out["name"] != "replaced-by-plugin" {
		t.Fatalf("REST GetTopic: %d %v", code, out)
	}

	// after sees the built-in's own error, and the client still gets it unchanged.
	_, err = pub.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: "projects/p1/topics/missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("DeleteTopic of a missing topic = %v, want NotFound", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"before:TopicCreate:grpc",
		"after:TopicCreate:grpc:projects/p1/topics/t1",
		"replace:TopicGet:grpc",
		"before:TopicCreate:grpc",
		"replace:TopicGet:",
		"after-delete:grpc:err=true",
	}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("plugin saw\n  %v\nwant\n  %v", seen, want)
	}
}

// The generic google.iam.v1.IAMPolicy RPCs serve every resource type; the key a
// call means (PubSub.TopicGetIamPolicy here) follows the resource it names.
func TestGCPGRPCGenericIAMFollowsTheResource(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	m := sdk.NewMux("gcp-iam", "0.1.0")
	m.After("gcp", "pubsub", "TopicGetIamPolicy", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		mu.Lock()
		seen = append(seen, fmt.Sprintf("%s:%s:%v", c.Req.Action, c.Req.Transport, c.Req.Params["resource"]))
		mu.Unlock()
		return sdk.Result{}, nil
	})
	s := gcpSidecar(t, m)
	e, conn := startGCPBoth(t, "--plugin", s.URL)
	ctx := context.Background()
	if _, err := pubsubpb.NewPublisherClient(conn).CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/p1/topics/iam"}); err != nil {
		t.Fatalf("CreateTopic: %v\n%s", err, e.logs())
	}
	if _, err := iampb.NewIAMPolicyClient(conn).GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: "projects/p1/topics/iam"}); err != nil {
		t.Fatalf("GetIamPolicy: %v\n%s", err, e.logs())
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(seen) != "[TopicGetIamPolicy:grpc:projects/p1/topics/iam]" {
		t.Fatalf("plugin saw %v", seen)
	}
}

// A server-streaming RPC is checked once, when the stream opens: a before hook
// can refuse it, and one that lets it through leaves the stream untouched.
func TestGCPGRPCStreamingIsCheckedAtOpen(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	deny := true
	m := sdk.NewMux("gcp-stream", "0.1.0")
	m.Before("gcp", "firestore", "RunQuery", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, fmt.Sprintf("%s:%s:%v", c.Req.Action, c.Req.Transport, c.Req.Params["parent"]))
		if deny {
			return sdk.Result{Err: &sdk.Error{Code: "PermissionDenied", Message: "no queries today", Status: 403}}, nil
		}
		return sdk.Result{}, nil
	})
	s := gcpSidecar(t, m)
	e, conn := startGCPBoth(t, "--plugin", s.URL)
	fs := firestorepb.NewFirestoreClient(conn)
	req := &firestorepb.RunQueryRequest{
		Parent: "projects/p1/databases/(default)/documents",
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: &firestorepb.StructuredQuery{
			From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "c"}},
		}},
	}
	open := func() error {
		st, err := fs.RunQuery(context.Background(), req)
		if err != nil {
			return err
		}
		for {
			if _, err := st.Recv(); err != nil {
				if err.Error() == "EOF" {
					return nil
				}
				return err
			}
		}
	}
	if err := open(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("denied stream = %v, want PermissionDenied\n%s", err, e.logs())
	}
	mu.Lock()
	deny = false
	mu.Unlock()
	if err := open(); err != nil {
		t.Fatalf("allowed stream failed: %v\n%s", err, e.logs())
	}
	mu.Lock()
	defer mu.Unlock()
	want := "[RunQuery:grpc:projects/p1/databases/(default)/documents RunQuery:grpc:projects/p1/databases/(default)/documents]"
	if fmt.Sprint(seen) != want {
		t.Fatalf("plugin saw %v", seen)
	}
}

// A Cloud Storage notification reaches Pub/Sub through the registry, so a plugin
// that overrides Pub/Sub keys sees it: the topic check when the notification
// config is created, and the publish when an object is written.
func TestGCPInternalPubSubCallsGoThroughThePlugin(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	replacePublish := true
	m := sdk.NewMux("gcp-internal", "0.1.0")
	m.After("gcp", "pubsub", "TopicGet", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		mu.Lock()
		seen = append(seen, "TopicGet:"+c.Req.Origin)
		mu.Unlock()
		return sdk.Result{}, nil
	})
	m.Replace("gcp", "pubsub", "TopicPublish", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, "TopicPublish:"+c.Req.Origin)
		if !replacePublish {
			return sdk.Result{NotHandled: true}, nil
		}
		return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"messageIds": []any{"from-plugin"}}}}, nil
	})
	s := gcpSidecar(t, m)
	e := startGCP(t, "--plugin", s.URL)

	must := func(code int, out map[string]any, what string) {
		t.Helper()
		if code != 200 {
			t.Fatalf("%s: %d %v\n%s", what, code, out, e.logs())
		}
	}
	code, out := e.gcpREST(t, http.MethodPut, "/v1/projects/p1/topics/n1", map[string]any{})
	must(code, out, "create topic")
	code, out = e.gcpREST(t, http.MethodPut, "/v1/projects/p1/subscriptions/s1", map[string]any{"topic": "projects/p1/topics/n1"})
	must(code, out, "create subscription")
	code, out = e.gcpREST(t, http.MethodPost, "/storage/v1/b?project=p1", map[string]any{"name": "b1"})
	must(code, out, "create bucket")
	code, out = e.gcpREST(t, http.MethodPost, "/storage/v1/b/b1/notificationConfigs", map[string]any{
		"topic": "//pubsub.googleapis.com/projects/p1/topics/n1", "payload_format": "JSON_API_V1", "event_types": []string{"OBJECT_FINALIZE"}})
	must(code, out, "create notification config")

	upload := func(name string) {
		t.Helper()
		resp, err := http.Post(e.base+"/upload/storage/v1/b/b1/o?uploadType=media&name="+name, "text/plain", bytes.NewReader([]byte("hi")))
		if err != nil {
			e.fatal(t, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("upload: %d\n%s", resp.StatusCode, e.logs())
		}
	}
	pull := func() int {
		code, out := e.gcpREST(t, http.MethodPost, "/v1/projects/p1/subscriptions/s1:pull", map[string]any{"maxMessages": 10, "returnImmediately": true})
		must(code, out, "pull")
		msgs, _ := out["receivedMessages"].([]any)
		return len(msgs)
	}

	upload("o1") // the plugin replaces the publish: the built-in never stores it
	if n := pull(); n != 0 {
		t.Fatalf("a replaced internal publish must not reach the subscription, got %d messages", n)
	}
	mu.Lock()
	replacePublish = false
	mu.Unlock()
	upload("o2") // the plugin declines: the built-in publishes it
	if n := pull(); n != 1 {
		t.Fatalf("a declined internal publish must reach the subscription, got %d messages", n)
	}

	mu.Lock()
	defer mu.Unlock()
	want := "[TopicGet:internal TopicPublish:internal TopicPublish:internal]"
	if fmt.Sprint(seen) != want {
		t.Fatalf("plugin saw %v, want %s", seen, want)
	}
}
