// Package sdk_scheduler_test exercises the jaiscloud-gcp emulator's Cloud
// Scheduler v1 control and data plane through the official Google REST apiary
// client (google.golang.org/api/cloudscheduler/v1). The emulator exposes the
// same v1 REST surface that real GCP serves over gRPC-gateway transcoding, so
// the apiary client is the transport-matching official client here.
//
// It pins the documented contracts: job create/get/list/patch/delete,
// pause/resume, and run forcing an immediate attempt — including a real
// httpTarget delivered to a local HTTP listener (headers, method, body) and a
// pubsubTarget delivered through the emulator's Pub/Sub fan-out. It also drives
// the cron engine deterministically with the emulator clock plus
// POST /_jaiscloud/scheduler-tick.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_scheduler_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	scheduler "google.golang.org/api/cloudscheduler/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func endpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080/"
}

func baseURL() string { return strings.TrimRight(endpoint(), "/") }

func projectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "proj"
}

func locationID() string { return "us-central1" }

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// resetState wipes emulator state (and restores the real clock).
func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(baseURL()+"/_jaiscloud/reset", "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// httpDo performs a raw request against the emulator (used for the admin
// endpoints and the Pub/Sub REST surface the scheduler pubsubTarget fans out
// to). It returns the status code and the response body.
func httpDo(t *testing.T, method, path string, body any) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, baseURL()+path, rd)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

func newService(t *testing.T) *scheduler.Service {
	t.Helper()
	svc, err := scheduler.NewService(context.Background(), opts()...)
	require.NoError(t, err)
	return svc
}

// recorded is one HTTP request the local target received.
type recorded struct {
	method string
	path   string
	header http.Header
	body   []byte
}

// hook is a local HTTP target that records the requests the emulator delivers
// and can be switched between success and 500 (or blocked mid-flight).
type hook struct {
	mu    sync.Mutex
	got   []recorded
	fail  bool
	block chan struct{}
	first chan struct{}
	once  sync.Once
}

func newHook() *hook { return &hook{first: make(chan struct{})} }

func (h *hook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	h.mu.Lock()
	h.got = append(h.got, recorded{method: r.Method, path: r.URL.Path, header: r.Header.Clone(), body: body})
	fail := h.fail
	block := h.block
	h.mu.Unlock()
	h.once.Do(func() { close(h.first) })
	if block != nil {
		<-block
	}
	if fail {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

func (h *hook) setFail(v bool) {
	h.mu.Lock()
	h.fail = v
	h.mu.Unlock()
}

func (h *hook) requests() []recorded {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]recorded, len(h.got))
	copy(out, h.got)
	return out
}

func (h *hook) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.got)
}

func mustCreateJob(t *testing.T, svc *scheduler.Service, parent string, job *scheduler.Job) *scheduler.Job {
	t.Helper()
	created, err := svc.Projects.Locations.Jobs.Create(parent, job).Do()
	require.NoError(t, err)
	return created
}

func apiErr(t *testing.T, err error) *googleapi.Error {
	t.Helper()
	require.Error(t, err)
	var gerr *googleapi.Error
	require.ErrorAs(t, err, &gerr)
	return gerr
}

// TestSDKSchedulerLifecycle covers job CRUD, the update mask, pause/resume, and
// list pagination over the official client.
func TestSDKSchedulerLifecycle(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	jobID := unique("job")
	jobName := parent + "/jobs/" + jobID

	created := mustCreateJob(t, svc, parent, &scheduler.Job{
		Name:        jobName,
		Description: "sdk behavioral job",
		Schedule:    "* * * * *",
		TimeZone:    "UTC",
		HttpTarget:  &scheduler.HttpTarget{Uri: "http://example.com/hook", HttpMethod: "POST"},
	})
	require.Equal(t, jobName, created.Name)
	require.Equal(t, "* * * * *", created.Schedule)
	require.Equal(t, "UTC", created.TimeZone)
	require.Equal(t, "ENABLED", created.State)
	require.Equal(t, "sdk behavioral job", created.Description)
	require.NotEmpty(t, created.ScheduleTime)
	require.NotEmpty(t, created.UserUpdateTime)
	st, err := time.Parse(time.RFC3339Nano, created.ScheduleTime)
	require.NoError(t, err)
	require.True(t, st.After(time.Now().Add(-time.Minute)), "scheduleTime must be in the future: %s", created.ScheduleTime)

	got, err := svc.Projects.Locations.Jobs.Get(jobName).Do()
	require.NoError(t, err)
	require.Equal(t, created.ScheduleTime, got.ScheduleTime, "scheduleTime must be stable across reads")

	// Duplicate create is rejected with AlreadyExists/409.
	_, err = svc.Projects.Locations.Jobs.Create(parent, &scheduler.Job{
		Name:       jobName,
		Schedule:   "* * * * *",
		HttpTarget: &scheduler.HttpTarget{Uri: "http://example.com/hook", HttpMethod: "POST"},
	}).Do()
	require.Equal(t, 409, apiErr(t, err).Code)

	// Patch with an update mask touches only the named fields.
	patched, err := svc.Projects.Locations.Jobs.Patch(jobName, &scheduler.Job{
		Description: "updated",
		Schedule:    "0 */2 * * *",
	}).UpdateMask("description,schedule").Do()
	require.NoError(t, err)
	require.Equal(t, "updated", patched.Description)
	require.Equal(t, "0 */2 * * *", patched.Schedule)
	require.NotNil(t, patched.HttpTarget, "httpTarget must survive a description/schedule patch")
	require.Equal(t, "ENABLED", patched.State)

	// Pause / resume, including the already-in-that-state failures.
	paused, err := svc.Projects.Locations.Jobs.Pause(jobName, &scheduler.PauseJobRequest{}).Do()
	require.NoError(t, err)
	require.Equal(t, "PAUSED", paused.State)
	_, err = svc.Projects.Locations.Jobs.Pause(jobName, &scheduler.PauseJobRequest{}).Do()
	require.Equal(t, 400, apiErr(t, err).Code, "pausing a PAUSED job must fail")

	resumed, err := svc.Projects.Locations.Jobs.Resume(jobName, &scheduler.ResumeJobRequest{}).Do()
	require.NoError(t, err)
	require.Equal(t, "ENABLED", resumed.State)
	_, err = svc.Projects.Locations.Jobs.Resume(jobName, &scheduler.ResumeJobRequest{}).Do()
	require.Equal(t, 400, apiErr(t, err).Code, "resuming an ENABLED job must fail")

	// Pagination: the emulator pageToken is the integer offset into the sorted list.
	fillerA := unique("job")
	fillerB := unique("job")
	mustCreateJob(t, svc, parent, &scheduler.Job{Name: parent + "/jobs/" + fillerA, Schedule: "* * * * *", HttpTarget: &scheduler.HttpTarget{Uri: "http://example.com/a", HttpMethod: "POST"}})
	mustCreateJob(t, svc, parent, &scheduler.Job{Name: parent + "/jobs/" + fillerB, Schedule: "* * * * *", HttpTarget: &scheduler.HttpTarget{Uri: "http://example.com/b", HttpMethod: "POST"}})

	page1, err := svc.Projects.Locations.Jobs.List(parent).PageSize(2).Do()
	require.NoError(t, err)
	require.Len(t, page1.Jobs, 2)
	require.NotEmpty(t, page1.NextPageToken)
	page2, err := svc.Projects.Locations.Jobs.List(parent).PageToken(page1.NextPageToken).Do()
	require.NoError(t, err)
	require.Len(t, page2.Jobs, 1)
	require.Empty(t, page2.NextPageToken)

	// Delete, then the job is gone.
	_, err = svc.Projects.Locations.Jobs.Delete(jobName).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Locations.Jobs.Get(jobName).Do()
	require.Equal(t, 404, apiErr(t, err).Code)
}

// TestSDKSchedulerRunDeliversHTTP forces an attempt with Jobs.run and asserts a
// real HTTP request reaches a local listener — method, body, user headers, and
// the synthetic Cloud Scheduler headers the emulator documents.
func TestSDKSchedulerRunDeliversHTTP(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	h := newHook()
	srv := httptest.NewServer(h)
	defer srv.Close()

	jobID := unique("runjob")
	jobName := parent + "/jobs/" + jobID
	payload := `{"hello":"world"}`
	mustCreateJob(t, svc, parent, &scheduler.Job{
		Name:     jobName,
		Schedule: "* * * * *",
		TimeZone: "UTC",
		HttpTarget: &scheduler.HttpTarget{
			Uri:        srv.URL + "/fire",
			HttpMethod: "POST",
			Headers:    map[string]string{"X-Test": "yes"},
			Body:       base64.StdEncoding.EncodeToString([]byte(payload)),
		},
	})

	// A successful run reports no status and stamps lastAttemptTime.
	ran, err := svc.Projects.Locations.Jobs.Run(jobName, &scheduler.RunJobRequest{}).Do()
	require.NoError(t, err)
	require.Nil(t, ran.Status, "a successful attempt clears the last status")
	require.NotEmpty(t, ran.LastAttemptTime)

	require.Eventually(t, func() bool { return h.count() >= 1 }, 5*time.Second, 20*time.Millisecond)
	reqs := h.requests()
	req := reqs[0]
	require.Equal(t, "POST", req.method)
	require.Equal(t, "/fire", req.path)
	require.Equal(t, payload, string(req.body))
	require.Equal(t, "yes", req.header.Get("X-Test"), "user header must be forwarded")
	require.Equal(t, "Google-Cloud-Scheduler", req.header.Get("User-Agent"))
	require.Equal(t, "true", req.header.Get("X-CloudScheduler"))
	require.Equal(t, jobName, req.header.Get("X-CloudScheduler-JobName"))
	require.NotEmpty(t, req.header.Get("X-CloudScheduler-ScheduleTime"))
	require.Equal(t, "application/octet-stream", req.header.Get("Content-Type"), "a body defaults to octet-stream")

	// A failing target records the response status.
	h.setFail(true)
	failed, err := svc.Projects.Locations.Jobs.Run(jobName, &scheduler.RunJobRequest{}).Do()
	require.NoError(t, err)
	require.NotNil(t, failed.Status)
	require.EqualValues(t, 500, failed.Status.Code)

	// The next successful run clears it again.
	h.setFail(false)
	ok, err := svc.Projects.Locations.Jobs.Run(jobName, &scheduler.RunJobRequest{}).Do()
	require.NoError(t, err)
	require.Nil(t, ok.Status)
}

// TestSDKSchedulerCronEngine drives the cron engine deterministically: a job is
// created, the emulator clock is advanced past its next fire time, and
// POST /_jaiscloud/scheduler-tick fires it at the local listener.
func TestSDKSchedulerCronEngine(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	h := newHook()
	srv := httptest.NewServer(h)
	defer srv.Close()

	jobID := unique("cronjob")
	jobName := parent + "/jobs/" + jobID
	created := mustCreateJob(t, svc, parent, &scheduler.Job{
		Name:       jobName,
		Schedule:   "* * * * *",
		TimeZone:   "UTC",
		HttpTarget: &scheduler.HttpTarget{Uri: srv.URL, HttpMethod: "GET"},
	})
	before, err := time.Parse(time.RFC3339Nano, created.ScheduleTime)
	require.NoError(t, err)

	// Jump the emulator clock two minutes ahead so the job is due.
	code, body := httpDo(t, http.MethodPost, "/_jaiscloud/clock", map[string]any{
		"mode": "offset",
		"time": time.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339Nano),
	})
	require.Equal(t, http.StatusNoContent, code, string(body))
	t.Cleanup(func() {
		_, _ = httpDo(t, http.MethodPost, "/_jaiscloud/clock", map[string]any{"mode": "real"})
	})

	code, body = httpDo(t, http.MethodPost, "/_jaiscloud/scheduler-tick", nil)
	require.Equal(t, http.StatusNoContent, code, string(body))

	require.Eventually(t, func() bool { return h.count() >= 1 }, 5*time.Second, 20*time.Millisecond,
		"the cron engine must fire the due job")

	got, err := svc.Projects.Locations.Jobs.Get(jobName).Do()
	require.NoError(t, err)
	after, err := time.Parse(time.RFC3339Nano, got.ScheduleTime)
	require.NoError(t, err)
	require.True(t, after.After(before), "scheduleTime must advance to the next cron slot (%s -> %s)", before, after)
	require.NotEmpty(t, got.LastAttemptTime)
}

// TestSDKSchedulerPubSubTarget creates a topic + subscription through the
// emulator's Pub/Sub REST surface, runs a pubsubTarget job, and asserts the
// message fans out to the subscription.
func TestSDKSchedulerPubSubTarget(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	topic := unique("sched-topic")
	sub := unique("sched-sub")
	topicFull := "projects/" + project + "/topics/" + topic

	code, body := httpDo(t, http.MethodPut, "/v1/projects/"+project+"/topics/"+topic, map[string]any{})
	require.Equal(t, http.StatusOK, code, "create topic: %s", body)
	code, body = httpDo(t, http.MethodPut, "/v1/projects/"+project+"/subscriptions/"+sub,
		map[string]any{"topic": topicFull})
	require.Equal(t, http.StatusOK, code, "create subscription: %s", body)

	jobID := unique("psjob")
	jobName := parent + "/jobs/" + jobID
	payload := "scheduler payload"
	created := mustCreateJob(t, svc, parent, &scheduler.Job{
		Name:     jobName,
		Schedule: "* * * * *",
		TimeZone: "UTC",
		PubsubTarget: &scheduler.PubsubTarget{
			TopicName:  topicFull,
			Data:       base64.StdEncoding.EncodeToString([]byte(payload)),
			Attributes: map[string]string{"src": "scheduler"},
		},
	})
	require.NotNil(t, created.PubsubTarget)
	require.Equal(t, topicFull, created.PubsubTarget.TopicName)

	ran, err := svc.Projects.Locations.Jobs.Run(jobName, &scheduler.RunJobRequest{}).Do()
	require.NoError(t, err)
	require.Nil(t, ran.Status, "a successful pubsubTarget publish reports no status")

	var pull struct {
		ReceivedMessages []struct {
			Message struct {
				Data       string            `json:"data"`
				Attributes map[string]string `json:"attributes"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		code, body = httpDo(t, http.MethodPost, "/v1/projects/"+project+"/subscriptions/"+sub+":pull",
			map[string]any{"maxMessages": 10, "returnImmediately": true})
		require.Equal(t, http.StatusOK, code, "pull: %s", body)
		require.NoError(t, json.Unmarshal(body, &pull))
		if len(pull.ReceivedMessages) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Len(t, pull.ReceivedMessages, 1, "the pubsubTarget message must be delivered")
	data, err := base64.StdEncoding.DecodeString(pull.ReceivedMessages[0].Message.Data)
	require.NoError(t, err)
	require.Equal(t, payload, string(data))
	require.Equal(t, "scheduler", pull.ReceivedMessages[0].Message.Attributes["src"])
}

// TestSDKSchedulerErrors pins the documented error contracts.
func TestSDKSchedulerErrors(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	// An unparseable cron schedule is InvalidArgument/400.
	_, err := svc.Projects.Locations.Jobs.Create(parent, &scheduler.Job{
		Name:       parent + "/jobs/" + unique("bad"),
		Schedule:   "every 5 minutes",
		HttpTarget: &scheduler.HttpTarget{Uri: "http://example.com", HttpMethod: "POST"},
	}).Do()
	require.Equal(t, 400, apiErr(t, err).Code)

	// A job with no target is InvalidArgument/400.
	_, err = svc.Projects.Locations.Jobs.Create(parent, &scheduler.Job{
		Name:     parent + "/jobs/" + unique("notarget"),
		Schedule: "* * * * *",
	}).Do()
	require.Equal(t, 400, apiErr(t, err).Code)

	// A body on a GET httpTarget is rejected.
	_, err = svc.Projects.Locations.Jobs.Create(parent, &scheduler.Job{
		Name:     parent + "/jobs/" + unique("badmethod"),
		Schedule: "* * * * *",
		HttpTarget: &scheduler.HttpTarget{
			Uri:        "http://example.com",
			HttpMethod: "GET",
			Body:       base64.StdEncoding.EncodeToString([]byte("nope")),
		},
	}).Do()
	require.Equal(t, 400, apiErr(t, err).Code)

	// Missing job reads / runs / deletes are NotFound/404.
	missing := parent + "/jobs/" + unique("ghost")
	_, err = svc.Projects.Locations.Jobs.Get(missing).Do()
	require.Equal(t, 404, apiErr(t, err).Code)
	_, err = svc.Projects.Locations.Jobs.Run(missing, &scheduler.RunJobRequest{}).Do()
	require.Equal(t, 404, apiErr(t, err).Code)
	_, err = svc.Projects.Locations.Jobs.Delete(missing).Do()
	require.Equal(t, 404, apiErr(t, err).Code)
}
