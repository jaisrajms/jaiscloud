package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeAPI is a minimal Docker Engine API stub for the shared client.
type fakeAPI struct {
	mu       sync.Mutex
	nextID   int
	created  map[string]map[string]any
	names    map[string]string
	conflict bool // next create returns 409 once
	reaped   []string
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{created: map[string]map[string]any{}, names: map[string]string{}}
}

func (f *fakeAPI) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		path := strings.TrimPrefix(r.URL.Path, "/"+APIVersion)
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) < 2 || parts[0] != "containers" {
			http.NotFound(w, r)
			return
		}
		switch {
		case parts[1] == "create" && r.Method == http.MethodPost:
			if f.conflict {
				f.conflict = false
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"message":"Conflict"}`))
				return
			}
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.nextID++
			id := fmt.Sprintf("ctr%04d", f.nextID)
			f.created[id] = body
			f.names[id] = r.URL.Query().Get("name")
			_ = json.NewEncoder(w).Encode(map[string]string{"Id": id})
		case parts[1] == "json" && r.Method == http.MethodGet:
			var filter struct {
				Label []string `json:"label"`
			}
			_ = json.Unmarshal([]byte(r.URL.Query().Get("filters")), &filter)
			var out []map[string]any
			for id, body := range f.created {
				if labelsMatch(body, filter.Label) {
					out = append(out, map[string]any{"Id": id, "Names": []string{"/" + f.names[id]}})
				}
			}
			_ = json.NewEncoder(w).Encode(out)
		case len(parts) == 3 && parts[2] == "json" && r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{"State": map[string]any{"Status": "exited", "ExitCode": 0}})
		case len(parts) == 2 && r.Method == http.MethodDelete:
			id := parts[1]
			if _, ok := f.created[id]; !ok {
				for cid, name := range f.names {
					if name == parts[1] {
						id = cid
					}
				}
			}
			if _, ok := f.created[id]; ok {
				f.reaped = append(f.reaped, id)
				delete(f.created, id)
				delete(f.names, id)
			}
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 3 && parts[2] == "stop" && r.Method == http.MethodPost:
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 3 && parts[2] == "start" && r.Method == http.MethodPost:
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 3 && parts[2] == "logs" && r.Method == http.MethodGet:
			_, _ = w.Write(logFrame(1, "driver output"))
		default:
			http.NotFound(w, r)
		}
	})
}

func labelsMatch(body map[string]any, labels []string) bool {
	got, _ := body["Labels"].(map[string]any)
	for _, kv := range labels {
		k, v, _ := strings.Cut(kv, "=")
		if s, _ := got[k].(string); s != v {
			return false
		}
	}
	return true
}

func logFrame(stream byte, payload string) []byte {
	var b bytes.Buffer
	h := make([]byte, 8)
	h[0] = stream
	binary.BigEndian.PutUint32(h[4:8], uint32(len(payload)))
	b.Write(h)
	b.WriteString(payload)
	return b.Bytes()
}

func testClient(t *testing.T, f *fakeAPI) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return New(Config{Client: &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", srv.Listener.Addr().String())
		},
	}}}), srv
}

func TestClientCreateStartRemove(t *testing.T) {
	f := newFakeAPI()
	c, _ := testClient(t, f)
	body, _ := json.Marshal(map[string]any{"Image": "img"})
	id, err := c.Create(context.Background(), "jc-x", body)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, status, err := c.Call(context.Background(), http.MethodPost, "/containers/"+id+"/start", nil); err != nil || status != http.StatusNoContent {
		t.Fatalf("start: status=%d err=%v", status, err)
	}
	if err := c.Remove(context.Background(), id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reaped) != 1 || f.reaped[0] != id {
		t.Fatalf("reaped = %v, want [%s]", f.reaped, id)
	}
}

func TestClientCreateRetriesOnNameConflict(t *testing.T) {
	f := newFakeAPI()
	f.conflict = true // first create returns 409
	c, _ := testClient(t, f)
	body, _ := json.Marshal(map[string]any{"Image": "img"})
	id, err := c.Create(context.Background(), "jc-dup", body)
	if err != nil {
		t.Fatalf("Create after conflict: %v", err)
	}
	if id == "" {
		t.Fatal("Create returned empty id")
	}
}

func TestClientRemoveByFilterHonoursInstanceScope(t *testing.T) {
	f := newFakeAPI()
	c, _ := testClient(t, f)
	body, _ := json.Marshal(map[string]any{"Labels": map[string]any{LabelService: "cloudrun", LabelInstance: "mine"}})
	if _, err := c.Create(context.Background(), "jc-mine", body); err != nil {
		t.Fatalf("create mine: %v", err)
	}
	other, _ := json.Marshal(map[string]any{"Labels": map[string]any{LabelService: "cloudrun", LabelInstance: "other"}})
	if _, err := c.Create(context.Background(), "jc-other", other); err != nil {
		t.Fatalf("create other: %v", err)
	}

	n, err := c.RemoveByFilter(context.Background(), LabelFilter("cloudrun", "mine"))
	if err != nil {
		t.Fatalf("RemoveByFilter: %v", err)
	}
	if n != 1 {
		t.Fatalf("reaped %d, want 1 (instance-scoped)", n)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.created) != 1 {
		t.Fatalf("after sweep %d containers remain, want 1", len(f.created))
	}
}

func TestLabelFilterScopesLabels(t *testing.T) {
	got := LabelFilter("dataproc", "inst", "jaiscloud.io/cluster-name=c1")
	labels := got["label"]
	for _, want := range []string{LabelService + "=dataproc", LabelInstance + "=inst", "jaiscloud.io/cluster-name=c1"} {
		if !contains(labels, want) {
			t.Errorf("label filter %v missing %q", labels, want)
		}
	}
	if g := LabelFilter("dataproc", ""); contains(g["label"], LabelInstance+"=") {
		t.Errorf("empty instance must not add an instance label: %v", g)
	}
}

func TestStreamLogsDemux(t *testing.T) {
	f := newFakeAPI()
	c, _ := testClient(t, f)
	var buf bytes.Buffer
	if err := c.StreamLogs(context.Background(), "ctr0001", &buf); err != nil {
		t.Fatalf("StreamLogs: %v", err)
	}
	if buf.String() != "driver output" {
		t.Fatalf("StreamLogs = %q", buf.String())
	}
}

func TestDemuxLogsMultipleFrames(t *testing.T) {
	var b bytes.Buffer
	if err := DemuxLogs(bytes.NewReader(append(logFrame(1, "out"), logFrame(2, "err")...)), &b); err != nil {
		t.Fatalf("DemuxLogs: %v", err)
	}
	if b.String() != "outerr" {
		t.Fatalf("DemuxLogs = %q, want outerr", b.String())
	}
}

func TestPingUnreachableSocket(t *testing.T) {
	if err := Ping(context.Background(), "/nonexistent/docker.sock"); err == nil {
		t.Fatal("Ping succeeded against a missing socket")
	}
}

func TestShortInstanceAndParseTime(t *testing.T) {
	if got := ShortInstance(""); got != "local" {
		t.Errorf("ShortInstance(\"\") = %q", got)
	}
	if got := ShortInstance("0123456789"); got != "01234567" {
		t.Errorf("ShortInstance = %q, want 8 chars", got)
	}
	if got := ParseTime("0001-01-01T00:00:00Z"); !got.IsZero() {
		t.Errorf("ParseTime sentinel = %v, want zero", got)
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
