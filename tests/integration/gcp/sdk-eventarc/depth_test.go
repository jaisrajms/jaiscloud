// Package sdk_eventarc_test — behavioural depth (p2–p5) for the Eventarc v1
// surface, driven by the official apiary clients (eventarc and pubsub) against
// the emulator. Where sdk_eventarc_test.go proves the baseline lifecycle (p1),
// this file exercises the behaviour classes docs/gcp/testability.md §10 names:
//
//	p2 concurrency      — trigger create/delete churn on the trigger store while
//	                      events are being delivered, with the client driving
//	                      several goroutines (run under -race), plus the
//	                      delete-stops-delivery boundary.
//	p3 errors           — destination-shape validation (both oneofs set, a
//	                      cloudRun missing service/region, a cross-project
//	                      service, a non-object httpEndpoint), the eventFilters
//	                      contract (no type filter, a storage trigger without a
//	                      bucket), duplicate triggerId 409, get-after-delete 404
//	                      and a stale-etag delete 409.
//	p4 listing & paging — trigger listing by pageSize/pageToken (composed with a
//	                      location) and orderBy (name, name desc, trigger_id,
//	                      createTime desc) and the AIP-160 filter
//	                      (destination:<member>, labels.<k>=v), with an
//	                      unsupported filter/orderBy failing loud rather than
//	                      returning an unfiltered list.
//	p5 scale & boundary — delivery fan-out at volume (many triggers on one topic
//	                      and several on one bucket, one event each, every
//	                      matching trigger delivered exactly once) and the
//	                      documented retry posture (a failing sink is attempted
//	                      once, never retried).
//
// The suite targets the emulator's own delivery engine over an httptest sink: it
// asserts the control plane plus the fire-and-forget httpEndpoint delivery
// contract. The real-Kubernetes delivery e2e (a deployed sink and a Cloud Run
// destination) stays the local `make test-e2e-eventarc-k8s` gate (EV4).
//
// Run with the GCP binary running:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -race ./...
package sdk_eventarc_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	eventarc "google.golang.org/api/eventarc/v1"
	"google.golang.org/api/googleapi"
	pubsub "google.golang.org/api/pubsub/v1"
)

const (
	depthLocation  = "us-central1"
	depthLocation2 = "us-west1"

	pubSubEventType    = "google.cloud.pubsub.topic.v1.messagePublished"
	storageEventType   = "google.cloud.storage.object.v1.finalized"
	deliverySettleTime = 3 * time.Second
	deliveryTimeout    = 25 * time.Second
)

// depthSeq makes the ids of concurrent creates unique without relying on the
// wall clock's resolution (a collision would surface as an unexpected 409).
var depthSeq int64

func depthID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), atomic.AddInt64(&depthSeq, 1))
}

func depthEventarc(t *testing.T) *eventarc.Service {
	t.Helper()
	svc, err := eventarc.NewService(context.Background(), opts()...)
	require.NoError(t, err)
	return svc
}

func depthPubSub(t *testing.T) *pubsub.Service {
	t.Helper()
	svc, err := pubsub.NewService(context.Background(), opts()...)
	require.NoError(t, err)
	return svc
}

func depthParent(location string) string {
	return fmt.Sprintf("projects/%s/locations/%s", projectID(), location)
}

func triggerName(location, id string) string {
	return depthParent(location) + "/triggers/" + id
}

// requireCode asserts err is a *googleapi.Error carrying the given HTTP status.
func requireCode(t *testing.T, err error, code int, what string) {
	t.Helper()
	require.Error(t, err, what)
	var gerr *googleapi.Error
	require.True(t, errors.As(err, &gerr), "%s: expected *googleapi.Error, got %v", what, err)
	require.Equal(t, code, gerr.Code, "%s: %v", what, gerr)
}

// ─── delivery sink ────────────────────────────────────────────────────────────

// sinkRequest is one POST the emulator's delivery engine made to a sink.
type sinkRequest struct {
	path    string
	headers http.Header
	body    []byte
}

// deliverySink is an HTTP server recording every request it receives. Its
// response status is settable so a test can model a failing destination.
type deliverySink struct {
	mu     sync.Mutex
	reqs   []sinkRequest
	status int
}

func newDeliverySink(t *testing.T) (*deliverySink, string) {
	t.Helper()
	sink := &deliverySink{status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sink.mu.Lock()
		sink.reqs = append(sink.reqs, sinkRequest{path: r.URL.Path, headers: r.Header.Clone(), body: body})
		status := sink.status
		sink.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	return sink, srv.URL
}

func (s *deliverySink) setStatus(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
}

// countType returns how many deliveries of a CloudEvent type were recorded.
func (s *deliverySink) countType(eventType string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.reqs {
		if r.headers.Get("ce-type") == eventType {
			n++
		}
	}
	return n
}

// waitForType polls until at least want deliveries of eventType have arrived,
// then settles and asserts the count stayed exactly want: no duplicate and no
// drop.
func (s *deliverySink) waitForType(t *testing.T, eventType string, want int) {
	t.Helper()
	deadline := time.Now().Add(deliveryTimeout)
	for s.countType(eventType) < want && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(deliverySettleTime)
	require.Equal(t, want, s.countType(eventType), "expected exactly %d %s deliveries", want, eventType)
}

// ─── trigger / topic / object helpers ─────────────────────────────────────────

func depthTopic(t *testing.T, ps *pubsub.Service, prefix string) string {
	t.Helper()
	name := fmt.Sprintf("projects/%s/topics/%s", projectID(), depthID(prefix))
	_, err := ps.Projects.Topics.Create(name, &pubsub.Topic{}).Do()
	require.NoError(t, err)
	return name
}

func depthPublish(t *testing.T, ps *pubsub.Service, topic, payload string) {
	t.Helper()
	_, err := ps.Projects.Topics.Publish(topic, &pubsub.PublishRequest{
		Messages: []*pubsub.PubsubMessage{{Data: base64.StdEncoding.EncodeToString([]byte(payload))}},
	}).Do()
	require.NoError(t, err)
}

func pubSubTriggerBody(topic, uri string, labels map[string]string) *eventarc.Trigger {
	return &eventarc.Trigger{
		Destination: &eventarc.Destination{HttpEndpoint: &eventarc.HttpEndpoint{Uri: uri}},
		Transport:   &eventarc.Transport{Pubsub: &eventarc.Pubsub{Topic: topic}},
		EventFilters: []*eventarc.EventFilter{
			{Attribute: "type", Value: pubSubEventType},
		},
		Labels: labels,
	}
}

func storageTriggerBody(bucket, uri string) *eventarc.Trigger {
	return &eventarc.Trigger{
		Destination: &eventarc.Destination{HttpEndpoint: &eventarc.HttpEndpoint{Uri: uri}},
		EventFilters: []*eventarc.EventFilter{
			{Attribute: "type", Value: storageEventType},
			{Attribute: "bucket", Value: bucket},
		},
	}
}

// createTrigger creates a trigger and returns its full resource name.
func createTrigger(t *testing.T, svc *eventarc.Service, location, id string, body *eventarc.Trigger) string {
	t.Helper()
	op, err := svc.Projects.Locations.Triggers.Create(depthParent(location), body).TriggerId(id).Do()
	require.NoError(t, err)
	require.True(t, op.Done, "create operation for %s not done", id)
	return triggerName(location, id)
}

// ─── Cloud Storage (raw REST: this module carries no GCS client) ──────────────

func depthBucket(t *testing.T, prefix string) string {
	t.Helper()
	bucket := strings.ToLower(fmt.Sprintf("%s-%d", prefix, atomic.AddInt64(&depthSeq, 1)))
	req, err := http.NewRequest(http.MethodPost,
		endpoint()+"storage/v1/b?project="+projectID(),
		strings.NewReader(fmt.Sprintf(`{"name":%q}`, bucket)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create bucket: %s", raw)
	return bucket
}

func depthUploadObject(t *testing.T, bucket, object, payload string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost,
		endpoint()+"upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+url.QueryEscape(object),
		strings.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "upload object: %s", raw)
}

// rawTriggerCreate POSTs a trigger body verbatim and returns the HTTP status, so
// a malformed JSON body (which the typed client cannot express) can be probed.
func rawTriggerCreate(t *testing.T, body string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost,
		endpoint()+"v1/"+depthParent(depthLocation)+"/triggers?triggerId="+depthID("raw"),
		strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode
}

// ─── p2 — concurrency ─────────────────────────────────────────────────────────

// TestDepthTriggerChurnDuringDelivery creates and deletes triggers from four
// goroutines while fifteen events are delivered to a stable trigger: the stable
// trigger must receive every event exactly once (no drop, no duplicate) and the
// client must stay race-clean. Deleting it then stops delivery.
func TestDepthTriggerChurnDuringDelivery(t *testing.T) {
	ctx := context.Background()
	svc := depthEventarc(t)
	ps := depthPubSub(t)
	sink, uri := newDeliverySink(t)

	topic := depthTopic(t, ps, "depth-p2")
	// The churn triggers live on their own topic, so they never match the
	// delivered events: the assertion is the stable trigger's delivery count.
	churnTopic := depthTopic(t, ps, "depth-p2-churn")

	stable := createTrigger(t, svc, depthLocation, depthID("stable"), pubSubTriggerBody(topic, uri, nil))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Bounded: the loop exists to churn the store while the events are
			// in flight, not to run forever.
			for i := 0; i < 300; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := depthID(fmt.Sprintf("churn%d", g))
				op, err := svc.Projects.Locations.Triggers.Create(depthParent(depthLocation),
					pubSubTriggerBody(churnTopic, uri, nil)).TriggerId(id).Context(ctx).Do()
				if err != nil {
					t.Errorf("churn create %s: %v", id, err)
					return
				}
				if !op.Done {
					t.Errorf("churn create %s: operation not done", id)
					return
				}
				if _, err := svc.Projects.Locations.Triggers.Delete(triggerName(depthLocation, id)).Context(ctx).Do(); err != nil {
					t.Errorf("churn delete %s: %v", id, err)
					return
				}
			}
		}(g)
	}

	const publishes = 15
	for i := 0; i < publishes; i++ {
		depthPublish(t, ps, topic, fmt.Sprintf("depth-p2-%d", i))
	}
	close(stop)
	wg.Wait()

	sink.waitForType(t, pubSubEventType, publishes)

	// Delete-stops-delivery: a deleted trigger no longer matches a new event.
	_, err := svc.Projects.Locations.Triggers.Delete(stable).Do()
	require.NoError(t, err)
	before := sink.countType(pubSubEventType)
	depthPublish(t, ps, topic, "after-delete")
	time.Sleep(deliverySettleTime)
	require.Equal(t, before, sink.countType(pubSubEventType),
		"a deleted trigger must not receive further deliveries")
}

// ─── p3 — errors, negatives ───────────────────────────────────────────────────

func TestDepthTriggerValidation(t *testing.T) {
	svc := depthEventarc(t)
	ps := depthPubSub(t)
	parent := depthParent(depthLocation)
	topic := depthTopic(t, ps, "depth-p3")

	create := func(id string, body *eventarc.Trigger) error {
		_, err := svc.Projects.Locations.Triggers.Create(parent, body).TriggerId(id).Do()
		return err
	}
	cloudRun := &eventarc.CloudRun{Service: "svc", Region: depthLocation}
	filters := []*eventarc.EventFilter{{Attribute: "type", Value: pubSubEventType}}

	// destination oneof + shape.
	requireCode(t, create(depthID("two"), &eventarc.Trigger{
		Destination:  &eventarc.Destination{Workflow: "projects/p/locations/l/workflows/w", CloudRun: cloudRun},
		EventFilters: filters,
	}), http.StatusBadRequest, "two destination members")
	requireCode(t, create(depthID("nosvc"), &eventarc.Trigger{
		Destination: &eventarc.Destination{CloudRun: &eventarc.CloudRun{Region: depthLocation}}, EventFilters: filters,
	}), http.StatusBadRequest, "cloudRun without service")
	requireCode(t, create(depthID("noregion"), &eventarc.Trigger{
		Destination: &eventarc.Destination{CloudRun: &eventarc.CloudRun{Service: "svc"}}, EventFilters: filters,
	}), http.StatusBadRequest, "cloudRun without region")
	requireCode(t, create(depthID("crossproj"), &eventarc.Trigger{
		Destination: &eventarc.Destination{CloudRun: &eventarc.CloudRun{
			Service: "projects/other/locations/us-central1/services/svc", Region: depthLocation,
		}},
		EventFilters: filters,
	}), http.StatusBadRequest, "cloudRun service in another project")
	requireCode(t, create(depthID("nodest"), &eventarc.Trigger{
		Destination: &eventarc.Destination{}, EventFilters: filters,
	}), http.StatusBadRequest, "no destination member set")
	// A non-object httpEndpoint (a documented schema violation the typed client
	// cannot express) is rejected, not silently treated as absent.
	require.Equal(t, http.StatusBadRequest, rawTriggerCreate(t,
		`{"destination":{"httpEndpoint":"http://127.0.0.1:1/"},"eventFilters":[{"attribute":"type","value":"`+pubSubEventType+`"}]}`),
		"httpEndpoint as a string")

	// eventFilters contract: a type filter is mandatory, and a storage trigger
	// needs its bucket filter.
	requireCode(t, create(depthID("notype"), &eventarc.Trigger{
		Destination:  &eventarc.Destination{CloudRun: cloudRun},
		EventFilters: []*eventarc.EventFilter{{Attribute: "topic", Value: topic}},
	}), http.StatusBadRequest, "missing type filter")
	requireCode(t, create(depthID("nobucket"), &eventarc.Trigger{
		Destination:  &eventarc.Destination{CloudRun: cloudRun},
		EventFilters: []*eventarc.EventFilter{{Attribute: "type", Value: storageEventType}},
	}), http.StatusBadRequest, "storage trigger without a bucket filter")

	// Duplicate triggerId is ALREADY_EXISTS.
	id := depthID("dup")
	body := pubSubTriggerBody(topic, "http://127.0.0.1:1/", nil)
	name := createTrigger(t, svc, depthLocation, id, body)
	requireCode(t, create(id, body), http.StatusConflict, "duplicate triggerId")

	// A stale etag on delete is ABORTED(409); a fresh etag delete succeeds and
	// the subsequent get is 404.
	got, err := svc.Projects.Locations.Triggers.Get(name).Do()
	require.NoError(t, err)
	require.NotEmpty(t, got.Etag)
	_, err = svc.Projects.Locations.Triggers.Patch(name, &eventarc.Trigger{
		Labels: map[string]string{"a": "b"},
	}).UpdateMask("labels").Do()
	require.NoError(t, err)
	_, err = svc.Projects.Locations.Triggers.Delete(name).Etag(got.Etag).Do()
	requireCode(t, err, http.StatusConflict, "stale delete etag")
	_, err = svc.Projects.Locations.Triggers.Delete(name).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Locations.Triggers.Get(name).Do()
	requireCode(t, err, http.StatusNotFound, "get after delete")
}

// ─── p4 — listing, paging, orderBy, filter ────────────────────────────────────

func TestDepthTriggerListing(t *testing.T) {
	svc := depthEventarc(t)
	ps := depthPubSub(t)
	topic := depthTopic(t, ps, "depth-p4")

	// Every trigger this test creates carries a run-unique label, and every
	// listing is scoped by it: the assertions describe this run's triggers only,
	// independent of any trigger left in the emulator by an earlier run.
	run := depthID("p4")
	sel := "labels.run=" + run

	var want []string
	for _, suffix := range []string{"a", "b", "c", "d", "e", "f"} {
		into := map[string]string{"run": run, "env": "prod"}
		if suffix > "d" {
			into["env"] = "test"
		}
		createTrigger(t, svc, depthLocation, run+"-"+suffix, pubSubTriggerBody(topic, "http://127.0.0.1:1/", into))
		want = append(want, run+"-"+suffix)
	}
	other := run + "-other"
	createTrigger(t, svc, depthLocation2, other, pubSubTriggerBody(topic, "http://127.0.0.1:1/", map[string]string{"run": run}))

	// list pages through one location with the selector, an optional extra
	// filter, an order and a page size, returning the ids of every page's items.
	list := func(location, filter, orderBy string, pageSize int64) []string {
		t.Helper()
		var out []string
		token := ""
		prefix := depthParent(location) + "/triggers/"
		for {
			call := svc.Projects.Locations.Triggers.List(depthParent(location)).Filter(filter)
			if orderBy != "" {
				call = call.OrderBy(orderBy)
			}
			if pageSize > 0 {
				call = call.PageSize(pageSize)
			}
			if token != "" {
				call = call.PageToken(token)
			}
			resp, err := call.Do()
			require.NoError(t, err)
			for _, tr := range resp.Triggers {
				out = append(out, strings.TrimPrefix(tr.Name, prefix))
			}
			if resp.NextPageToken == "" {
				return out
			}
			token = resp.NextPageToken
		}
	}
	reverse := make([]string, len(want))
	for i, id := range want {
		reverse[len(want)-1-i] = id
	}
	ids := func(location string, resp *eventarc.ListTriggersResponse) []string {
		t.Helper()
		p := depthParent(location) + "/triggers/"
		out := make([]string, 0, len(resp.Triggers))
		for _, tr := range resp.Triggers {
			out = append(out, strings.TrimPrefix(tr.Name, p))
		}
		return out
	}

	// Default order is ascending name, and the other location is not leaked in.
	require.Equal(t, want, list(depthLocation, sel, "", 0))
	require.Equal(t, []string{other}, list(depthLocation2, sel, "", 0))

	// Page through with pageSize 1: every trigger exactly once, in order.
	require.Equal(t, want, list(depthLocation, sel, "", 1))

	// orderBy.
	require.Equal(t, reverse, list(depthLocation, sel, "name desc", 0))
	require.Equal(t, want, list(depthLocation, sel, "trigger_id", 0))
	require.Equal(t, reverse, list(depthLocation, sel, "createTime desc", 0))

	// AIP-160 filter, composed with the order and the page cursor.
	require.Equal(t, []string{want[4], want[5]}, list(depthLocation, sel+" AND labels.env=test", "", 0))
	require.Empty(t, list(depthLocation, sel+" AND destination:workflow", "", 0))
	// Every documented Destination oneof member is accepted, including the one
	// the emulator cannot create a destination for.
	require.Empty(t, list(depthLocation, sel+" AND destination:networkConfig", "", 0))
	require.Equal(t, want, list(depthLocation, sel+" AND destination:httpEndpoint", "", 0))
	// A filtered, ordered, paged walk returns the whole matching set in order...
	require.Equal(t, []string{want[3], want[2], want[1], want[0]}, list(depthLocation, sel+" AND labels.env=prod", "name desc", 2))
	// ...and one page of it stops early with a next-page token.
	paged, err := svc.Projects.Locations.Triggers.List(depthParent(depthLocation)).
		Filter(sel + " AND labels.env=prod").OrderBy("name desc").PageSize(2).Do()
	require.NoError(t, err)
	require.Equal(t, []string{want[3], want[2]}, ids(depthLocation, paged))
	require.NotEmpty(t, paged.NextPageToken, "the filtered page must report a next page")

	// An unsupported filter or orderBy fails loud (400), never silently
	// returning the unfiltered list.
	_, err = svc.Projects.Locations.Triggers.List(depthParent(depthLocation)).Filter("bogus=1").Do()
	requireCode(t, err, http.StatusBadRequest, "unsupported filter")
	_, err = svc.Projects.Locations.Triggers.List(depthParent(depthLocation)).OrderBy("bogus").Do()
	requireCode(t, err, http.StatusBadRequest, "unsupported orderBy")
	_, err = svc.Projects.Locations.Triggers.List(depthParent(depthLocation)).Filter("name=*").Do()
	requireCode(t, err, http.StatusBadRequest, "wildcard on name")
	// A Trigger has no displayName field, so naming it is an unknown field.
	_, err = svc.Projects.Locations.Triggers.List(depthParent(depthLocation)).Filter("displayName=hello").Do()
	requireCode(t, err, http.StatusBadRequest, "unknown filter field")
	_, err = svc.Projects.Locations.Triggers.List(depthParent(depthLocation)).OrderBy("displayName").Do()
	requireCode(t, err, http.StatusBadRequest, "unknown orderBy field")
}

// ─── p5 — fan-out at volume, retry posture ────────────────────────────────────

// TestDepthDeliveryFanOut points many triggers at one sink and delivers a single
// event: every matching trigger receives it exactly once, and a destination that
// fails is attempted once and never retried (the emulator's documented
// fire-and-forget httpEndpoint posture).
func TestDepthDeliveryFanOut(t *testing.T) {
	svc := depthEventarc(t)
	ps := depthPubSub(t)
	sink, uri := newDeliverySink(t)

	const fanOut = 12
	topic := depthTopic(t, ps, "depth-p5")
	for i := 0; i < fanOut; i++ {
		createTrigger(t, svc, depthLocation, depthID("fan"), pubSubTriggerBody(topic, uri, nil))
	}
	// A trigger on another topic must not receive the event: waitForType asserts
	// the count is exactly fanOut, so any extra delivery fails the test.
	otherTopic := depthTopic(t, ps, "depth-p5-other")
	createTrigger(t, svc, depthLocation, depthID("fan-other"), pubSubTriggerBody(otherTopic, uri, nil))

	depthPublish(t, ps, topic, "fan-out")
	sink.waitForType(t, pubSubEventType, fanOut)

	// Cloud Storage source: three triggers on one bucket, one upload.
	const storageFanOut = 3
	bucket := depthBucket(t, "depth-p5-bucket")
	for i := 0; i < storageFanOut; i++ {
		createTrigger(t, svc, depthLocation, depthID("fan-gcs"), storageTriggerBody(bucket, uri))
	}
	depthUploadObject(t, bucket, "depth/object.txt", "fan-out")
	sink.waitForType(t, storageEventType, storageFanOut)

	// Retry posture: a destination answering 500 is attempted exactly once.
	failing, failingURI := newDeliverySink(t)
	failing.setStatus(http.StatusInternalServerError)
	failTopic := depthTopic(t, ps, "depth-p5-fail")
	createTrigger(t, svc, depthLocation, depthID("fail"), pubSubTriggerBody(failTopic, failingURI, nil))
	depthPublish(t, ps, failTopic, "failing sink")
	deadline := time.Now().Add(deliveryTimeout)
	for failing.countType(pubSubEventType) == 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(deliverySettleTime)
	require.Equal(t, 1, failing.countType(pubSubEventType),
		"a failing httpEndpoint destination must be attempted exactly once (no retry)")
}
