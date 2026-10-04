package eventarc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/gcp/eventing"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/store"
)

// capturedRequest is one HTTP request a test sink received.
type capturedRequest struct {
	method string
	header http.Header
	body   []byte
}

// requestSink is a test HTTP endpoint that records every request.
type requestSink struct {
	*httptest.Server
	mu  sync.Mutex
	got []capturedRequest
}

func newRequestSink(t *testing.T) *requestSink {
	t.Helper()
	s := &requestSink{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.got = append(s.got, capturedRequest{method: r.Method, header: r.Header.Clone(), body: body})
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *requestSink) requests() []capturedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]capturedRequest(nil), s.got...)
}

// newDispatchService returns a service whose outbound client posts to sink.
func newDispatchService(t *testing.T, sink *requestSink) *Service {
	t.Helper()
	resources := store.NewMemoryResourceStore()
	seedTopic(t, resources, "t")
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflowsstore.NewMemoryStore())
	if sink != nil {
		svc.httpClient = sink.Client()
	}
	return svc
}

func createHTTPTrigger(t *testing.T, svc *Service, id, uri, filters string) {
	t.Helper()
	body := json.RawMessage(`{
		"destination":{"httpEndpoint":{"uri":"` + uri + `"}},
		"transport":{"pubsub":{"topic":"projects/proj/topics/t"}},
		"eventFilters":` + filters + `}`)
	if _, _, err := svc.CreateTrigger(context.Background(), "proj", "us-central1", id, body, false); err != nil {
		t.Fatalf("create trigger %s: %v", id, err)
	}
}

func pubsubEvent() eventing.Event {
	return eventing.Event{
		Project:    "proj",
		EventType:  eventing.TypePubSubPublish,
		Resource:   "projects/proj/topics/t",
		EventID:    "msg-123",
		Source:     eventing.SourcePubSub,
		Data:       []byte("hello world"),
		Attributes: map[string]string{"k": "v"},
		OccurredAt: time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC),
	}
}

func TestDispatchEventPostsCloudEventToHTTPEndpoint(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createHTTPTrigger(t, svc, "trig1", sink.URL, `[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]`)

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()

	got := sink.requests()
	if len(got) != 1 {
		t.Fatalf("sink received %d requests, want 1", len(got))
	}
	req := got[0]
	if req.method != http.MethodPost {
		t.Fatalf("method = %s, want POST", req.method)
	}
	headers := map[string]string{
		"ce-id":          "msg-123",
		"ce-source":      "//pubsub.googleapis.com/projects/proj/topics/t",
		"ce-specversion": "1.0",
		"ce-type":        "google.cloud.pubsub.topic.v1.messagePublished",
		"ce-time":        "2026-06-25T12:00:00Z",
		"Content-Type":   "application/json",
	}
	for k, want := range headers {
		if gotV := req.header.Get(k); gotV != want {
			t.Errorf("header %s = %q, want %q", k, gotV, want)
		}
	}

	var payload struct {
		Message struct {
			Data        string            `json:"data"`
			Attributes  map[string]string `json:"attributes"`
			MessageID   string            `json:"messageId"`
			PublishTime string            `json:"publishTime"`
		} `json:"message"`
		Subscription string `json:"subscription"`
	}
	if err := json.Unmarshal(req.body, &payload); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if payload.Message.Data != base64.StdEncoding.EncodeToString([]byte("hello world")) {
		t.Errorf("message.data = %q", payload.Message.Data)
	}
	if payload.Message.MessageID != "msg-123" {
		t.Errorf("message.messageId = %q", payload.Message.MessageID)
	}
	if payload.Message.Attributes["k"] != "v" {
		t.Errorf("message.attributes = %+v", payload.Message.Attributes)
	}
	if want := "projects/proj/subscriptions/eventarc-us-central1-trig1"; payload.Subscription != want {
		t.Errorf("subscription = %q, want %q", payload.Subscription, want)
	}
}

func TestDispatchEventTypeMismatchDoesNotDeliver(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	// A storage-typed filter on a Pub/Sub transport must not match a Pub/Sub
	// event. A Cloud Storage trigger also needs a bucket filter, but that does
	// not make it match a Pub/Sub event.
	createHTTPTrigger(t, svc, "trig1", sink.URL,
		`[{"attribute":"type","value":"google.storage.object.finalize"},{"attribute":"bucket","value":"b"}]`)

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 0 {
		t.Fatalf("sink received %d requests, want 0", len(got))
	}
}

func TestDispatchEventTopicAttributeLastSegmentFallback(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	// Filter uses the short topic id while the event carries the full resource.
	createHTTPTrigger(t, svc, "trig1", sink.URL,
		`[{"attribute":"type","value":"google.pubsub.topic.publish"},{"attribute":"topic","value":"t"}]`)

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 1 {
		t.Fatalf("sink received %d requests, want 1", len(got))
	}

	// A different topic does not match.
	svc2 := newDispatchService(t, sink)
	createHTTPTrigger(t, svc2, "trig2", sink.URL,
		`[{"attribute":"type","value":"google.pubsub.topic.publish"},{"attribute":"topic","value":"other"}]`)
	svc2.DispatchEvent(context.Background(), pubsubEvent())
	svc2.waitDeliveries()
	if got := sink.requests(); len(got) != 1 {
		t.Fatalf("after non-matching topic, sink requests = %d, want still 1", len(got))
	}
}

func TestDispatchEventSkipsCloudFunctionDestination(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	svc.SetFunctionExister(fakeFunctions{exists: map[string]bool{"us-central1/fn": true}})
	body := json.RawMessage(`{
		"destination":{"cloudFunction":"projects/proj/locations/us-central1/functions/fn"},
		"transport":{"pubsub":{"topic":"projects/proj/topics/t"}},
		"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`)
	if _, _, err := svc.CreateTrigger(context.Background(), "proj", "us-central1", "cf", body, false); err != nil {
		t.Fatalf("create cloudFunction trigger: %v", err)
	}

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 0 {
		t.Fatalf("sink received %d requests, want 0 (functions owns cloudFunction)", len(got))
	}
}

// cloudRunCall is one delivery forwarded through the CloudRunInvoker seam.
type cloudRunCall struct {
	project, region, service, path string
	headers                        map[string]string
	body                           []byte
}

// recordingCloudRunInvoker captures forwarded deliveries and can be told to
// fail, so the dispatcher's fire-and-forget error path is exercised.
type recordingCloudRunInvoker struct {
	mu     sync.Mutex
	calls  []cloudRunCall
	status int
	err    error
}

func (r *recordingCloudRunInvoker) Invoke(_ context.Context, project, region, service, path string, headers map[string]string, body []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, cloudRunCall{project, region, service, path, headers, body})
	if r.err != nil {
		return r.status, r.err
	}
	if r.status == 0 {
		return http.StatusOK, nil
	}
	return r.status, nil
}

func (r *recordingCloudRunInvoker) deliveries() []cloudRunCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]cloudRunCall(nil), r.calls...)
}

func createCloudRunTrigger(t *testing.T, svc *Service, id, dest string) {
	t.Helper()
	body := json.RawMessage(`{
		"destination":{"cloudRun":` + dest + `},
		"transport":{"pubsub":{"topic":"projects/proj/topics/t"}},
		"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`)
	if _, _, err := svc.CreateTrigger(context.Background(), "proj", "us-central1", id, body, false); err != nil {
		t.Fatalf("create cloudRun trigger %s: %v", id, err)
	}
}

func TestDispatchEventDeliversCloudRunDestination(t *testing.T) {
	svc := newDispatchService(t, nil)
	inv := &recordingCloudRunInvoker{}
	svc.SetCloudRunInvoker(inv)
	// Full resource name + relative path: id/location are derived and the path
	// is made absolute.
	createCloudRunTrigger(t, svc, "run", `{"service":"projects/proj/locations/us-central1/services/s","path":"hook"}`)

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()

	calls := inv.deliveries()
	if len(calls) != 1 {
		t.Fatalf("invoker got %d calls, want 1", len(calls))
	}
	c := calls[0]
	if c.project != "proj" || c.region != "us-central1" || c.service != "s" || c.path != "/hook" {
		t.Fatalf("call = %+v, want proj/us-central1/s//hook", c)
	}
	for k, want := range map[string]string{
		"ce-id":          "msg-123",
		"ce-type":        eventing.TypePubSubPublishCloudEvent,
		"ce-source":      "//pubsub.googleapis.com/projects/proj/topics/t",
		"ce-specversion": "1.0",
		"ce-time":        "2026-06-25T12:00:00Z",
		"Content-Type":   "application/json",
	} {
		if got := c.headers[k]; got != want {
			t.Errorf("header %s = %q, want %q", k, got, want)
		}
	}
	if !bytes.Contains(c.body, []byte(base64.StdEncoding.EncodeToString([]byte("hello world")))) {
		t.Errorf("body does not carry the base64 message data: %s", c.body)
	}
}

func TestDispatchEventCloudRunShortServiceAndDefaultPath(t *testing.T) {
	svc := newDispatchService(t, nil)
	inv := &recordingCloudRunInvoker{}
	svc.SetCloudRunInvoker(inv)
	// floci's doc shape: short service id + explicit region, no path.
	createCloudRunTrigger(t, svc, "run", `{"service":"hello-run","region":"us-central1"}`)

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()

	calls := inv.deliveries()
	if len(calls) != 1 {
		t.Fatalf("invoker got %d calls, want 1", len(calls))
	}
	if c := calls[0]; c.service != "hello-run" || c.region != "us-central1" || c.path != "/" {
		t.Fatalf("call = %+v", c)
	}
}

func TestDispatchEventDropsCloudRunWithoutInvoker(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createCloudRunTrigger(t, svc, "run", `{"service":"s","region":"us-central1"}`)

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 0 {
		t.Fatalf("sink received %d requests, want 0 (no CloudRun invoker wired)", len(got))
	}
}

func TestDispatchEventCloudRunFailureIsLoggedNotPanicked(t *testing.T) {
	svc := newDispatchService(t, nil)
	inv := &recordingCloudRunInvoker{status: http.StatusServiceUnavailable, err: errors.New("no ready runtime")}
	svc.SetCloudRunInvoker(inv)
	createCloudRunTrigger(t, svc, "run", `{"service":"s","region":"us-central1"}`)

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries() // must return without panicking
	if got := len(inv.deliveries()); got != 1 {
		t.Fatalf("invoker got %d calls, want 1", got)
	}
}

func TestCloudRunDestinationResolution(t *testing.T) {
	tests := []struct {
		name string
		dest string
		want cloudRunTarget
		ok   bool
	}{
		{"full name", `{"cloudRun":{"service":"projects/proj/locations/us-central1/services/svc"}}`,
			cloudRunTarget{region: "us-central1", service: "svc", path: "/"}, true},
		{"full name with explicit region", `{"cloudRun":{"service":"projects/p/locations/europe-west1/services/svc","region":"us-central1"}}`,
			cloudRunTarget{region: "us-central1", service: "svc", path: "/"}, true},
		{"short id and region", `{"cloudRun":{"service":"svc","region":"us-central1"}}`,
			cloudRunTarget{region: "us-central1", service: "svc", path: "/"}, true},
		{"absolute path", `{"cloudRun":{"service":"svc","region":"r","path":"/hook"}}`,
			cloudRunTarget{region: "r", service: "svc", path: "/hook"}, true},
		{"relative path", `{"cloudRun":{"service":"svc","region":"r","path":"hook"}}`,
			cloudRunTarget{region: "r", service: "svc", path: "/hook"}, true},
		{"missing service", `{"cloudRun":{"region":"r"}}`, cloudRunTarget{}, false},
		{"missing region", `{"cloudRun":{"service":"svc"}}`, cloudRunTarget{}, false},
		{"no cloudRun", `{"httpEndpoint":{"uri":"http://x"}}`, cloudRunTarget{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest map[string]any
			if err := json.Unmarshal([]byte(tt.dest), &dest); err != nil {
				t.Fatalf("unmarshal dest: %v", err)
			}
			got, ok := cloudRunDestination(dest)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("cloudRunDestination = (%+v, %v), want (%+v, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

// storageEvent builds a Cloud Storage object event as the storage producer
// raises it: canonical event type, resource projects/_/buckets/{bucket},
// object-metadata JSON body and bucketId/objectId attributes.
func storageEvent(eventType, bucket, object string) eventing.Event {
	return eventing.Event{
		Project:   "proj",
		EventType: eventType,
		Resource:  "projects/_/buckets/" + bucket,
		EventID:   bucket + "/" + object + "/1",
		Source:    eventing.SourceStorage,
		Data: []byte(`{"kind":"storage#object","bucket":"` + bucket +
			`","name":"` + object + `","generation":"1"}`),
		Attributes: map[string]string{"bucketId": bucket, "objectId": object},
		OccurredAt: time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC),
	}
}

// createStorageHTTPTrigger creates a Cloud Storage trigger (no Pub/Sub transport)
// with an httpEndpoint destination and the given eventFilters.
func createStorageHTTPTrigger(t *testing.T, svc *Service, id, uri, filters string) {
	t.Helper()
	body := json.RawMessage(`{
		"destination":{"httpEndpoint":{"uri":"` + uri + `"}},
		"eventFilters":` + filters + `}`)
	if _, _, err := svc.CreateTrigger(context.Background(), "proj", "us-central1", id, body, false); err != nil {
		t.Fatalf("create storage trigger %s: %v", id, err)
	}
}

func TestDispatchEventStorageFinalizeToHTTPEndpoint(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createStorageHTTPTrigger(t, svc, "gcs", sink.URL,
		`[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},{"attribute":"bucket","value":"mybucket"}]`)

	ev := storageEvent(eventing.TypeStorageFinalize, "mybucket", "dir/obj.txt")
	svc.DispatchEvent(context.Background(), ev)
	svc.waitDeliveries()

	got := sink.requests()
	if len(got) != 1 {
		t.Fatalf("sink received %d requests, want 1", len(got))
	}
	req := got[0]
	if req.method != http.MethodPost {
		t.Fatalf("method = %s, want POST", req.method)
	}
	if req.header.Get("ce-id") == "" {
		t.Error("ce-id must be a non-empty unique id")
	}
	for k, want := range map[string]string{
		"ce-source":      "//storage.googleapis.com/projects/_/buckets/mybucket",
		"ce-specversion": "1.0",
		"ce-type":        "google.cloud.storage.object.v1.finalized",
		"ce-time":        "2026-06-25T12:00:00Z",
		"Content-Type":   "application/json",
	} {
		if gotV := req.header.Get(k); gotV != want {
			t.Errorf("header %s = %q, want %q", k, gotV, want)
		}
	}
	if !bytes.Equal(req.body, ev.Data) {
		t.Errorf("body = %s, want object metadata %s", req.body, ev.Data)
	}
}

func TestDispatchEventStorageDeleteToHTTPEndpoint(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createStorageHTTPTrigger(t, svc, "gcs", sink.URL,
		`[{"attribute":"type","value":"google.storage.object.delete"},{"attribute":"bucket","value":"b"}]`)

	svc.DispatchEvent(context.Background(), storageEvent(eventing.TypeStorageDelete, "b", "o"))
	svc.waitDeliveries()

	got := sink.requests()
	if len(got) != 1 {
		t.Fatalf("sink received %d requests, want 1", len(got))
	}
	if ce := got[0].header.Get("ce-type"); ce != "google.cloud.storage.object.v1.deleted" {
		t.Errorf("ce-type = %q, want the deleted CloudEvent spelling", ce)
	}
}

func TestDispatchEventStorageBucketLastSegmentFallback(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	// The filter names the fully-qualified bucket while the event carries the
	// short id; both must match (floci's last-segment comparison).
	createStorageHTTPTrigger(t, svc, "gcs", sink.URL,
		`[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},{"attribute":"bucket","value":"projects/_/buckets/b"}]`)

	svc.DispatchEvent(context.Background(), storageEvent(eventing.TypeStorageFinalize, "b", "o"))
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 1 {
		t.Fatalf("sink received %d requests, want 1", len(got))
	}
}

func TestDispatchEventStorageObjectFilter(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createStorageHTTPTrigger(t, svc, "gcs", sink.URL,
		`[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},{"attribute":"bucket","value":"b"},{"attribute":"object","value":"dir/","operator":"match-path-pattern"}]`)

	// A matching object prefix delivers; a different prefix does not.
	svc.DispatchEvent(context.Background(), storageEvent(eventing.TypeStorageFinalize, "b", "dir/obj.txt"))
	svc.waitDeliveries()
	svc.DispatchEvent(context.Background(), storageEvent(eventing.TypeStorageFinalize, "b", "other/obj.txt"))
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 1 {
		t.Fatalf("sink received %d requests, want 1 (only the dir/ object)", len(got))
	}
}

func TestDispatchEventStorageTypeMismatchDoesNotDeliver(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createStorageHTTPTrigger(t, svc, "gcs", sink.URL,
		`[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},{"attribute":"bucket","value":"b"}]`)

	// A delete event must not match a finalize trigger.
	svc.DispatchEvent(context.Background(), storageEvent(eventing.TypeStorageDelete, "b", "o"))
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 0 {
		t.Fatalf("sink received %d requests, want 0", len(got))
	}
}

func TestStorageCloudEventIDIsUniquePerEvent(t *testing.T) {
	// The producer's EventID (bucket/object/generation) is identical for a
	// finalize and a delete of the same generation, but the CloudEvent id must
	// be unique per event (CloudEvents requires source+id uniqueness).
	_, hFin := buildStorageCloudEvent(storageEvent(eventing.TypeStorageFinalize, "b", "o"))
	if hFin["ce-id"] == "" {
		t.Fatal("finalize ce-id is empty")
	}
	_, hDel := buildStorageCloudEvent(storageEvent(eventing.TypeStorageDelete, "b", "o"))
	if hDel["ce-id"] == hFin["ce-id"] {
		t.Fatalf("finalize and delete share ce-id %q", hFin["ce-id"])
	}
}

func TestEventarcStorageNoFiltersNeverMatches(t *testing.T) {
	body := map[string]any{
		"destination": map[string]any{"httpEndpoint": map[string]any{"uri": "http://x"}},
	}
	if eventarcEventMatches(body, storageEvent(eventing.TypeStorageFinalize, "b", "o")) {
		t.Fatal("a storage trigger with no eventFilters must never match")
	}
}

func TestDispatchEventStorageDoesNotMatchPubSubTrigger(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createHTTPTrigger(t, svc, "trig1", sink.URL,
		`[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]`)

	svc.DispatchEvent(context.Background(), storageEvent(eventing.TypeStorageFinalize, "b", "o"))
	svc.waitDeliveries()
	if got := sink.requests(); len(got) != 0 {
		t.Fatalf("sink received %d requests, want 0 for a storage event on a Pub/Sub trigger", len(got))
	}
}

func TestDispatchEventConnectionFailureIsLoggedNotPanicked(t *testing.T) {
	sink := newRequestSink(t)
	svc := newDispatchService(t, sink)
	createHTTPTrigger(t, svc, "trig1", sink.URL, `[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]`)
	// Close the sink first so the POST fails at connect time.
	sink.Close()

	svc.DispatchEvent(context.Background(), pubsubEvent())
	svc.waitDeliveries() // must return without panicking
}

func TestFiltersMatchNoTypeFallback(t *testing.T) {
	attrs := map[string]string{"type": "google.cloud.pubsub.topic.v1.messagePublished", "topic": "projects/p/topics/t"}
	if !filtersMatch([]any{map[string]any{"attribute": "topic", "value": "t"}}, attrs, "") {
		t.Error("topic filter should match by last segment")
	}
	if filtersMatch([]any{map[string]any{"attribute": "type", "value": "google.storage.object.finalize"}}, attrs, "google.pubsub.topic.publish") {
		t.Error("storage type filter must not match a pubsub event")
	}
	if filtersMatch([]any{map[string]any{"attribute": "missing", "value": "x"}}, attrs, "x") {
		t.Error("filter on an absent attribute must not match")
	}
	// match-path-pattern still applies to non-type attributes.
	pathAttrs := map[string]string{"object": "dir/sub/file.txt"}
	if !filtersMatch([]any{map[string]any{"attribute": "object", "value": "dir/", "operator": "match-path-pattern"}}, pathAttrs, "") {
		t.Error("match-path-pattern prefix should match")
	}
}
