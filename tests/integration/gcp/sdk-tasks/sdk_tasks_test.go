// Package sdk_tasks_test exercises the jaiscloud-gcp emulator's Cloud Tasks v2
// control and data plane through the official Google REST apiary client
// (google.golang.org/api/cloudtasks/v2). The emulator exposes the v2 REST
// surface that real GCP serves over gRPC-gateway transcoding, so the apiary
// client is the transport-matching official client here.
//
// It pins the documented contracts: queue create/get/list/patch/delete,
// pause/resume/purge, task CRUD, batchCreate/batchDelete, run forcing a
// synchronous attempt, and the dispatch engine's per-queue concurrency and
// token-bucket rate limits plus retry accounting. A real httpRequest is
// delivered to a local HTTP listener (headers, method, body, and the Cloud
// Tasks dispatch headers).
//
// Documented emulator deviations this suite pins (real GCP differs):
//   - appEngineHttpRequest tasks are stored and echoed but never delivered (no
//     App Engine router); an attempt is recorded as Unimplemented (code 12).
//   - batchCreate/batchDelete are served synchronously: the emulator performs
//     the work inline and returns a done=true google.longrunning.Operation
//     (real GCP returns an in-flight operation that is polled). The response
//     carries a BatchCreateTasksResponse (the created tasks) or
//     google.protobuf.Empty, so the official client decodes a populated
//     Operation and the suite asserts its response body.
//   - X-CloudTasks-QueueName / X-CloudTasks-TaskName carry the short queue/task
//     id, not the full resource name, and the Authorization token attached for
//     oauth/oidc targets is a synthetic emulator-local token.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_tasks_test

import (
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
	cloudtasks "google.golang.org/api/cloudtasks/v2"
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

// futureRFC returns a scheduleTime far enough ahead that the emulator's
// background dispatch ticker (1 Hz) will not touch a task; tests drive delivery
// explicitly with RunTask or /_jaiscloud/tasks-tick.
func futureRFC() string { return time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano) }

// resetState wipes emulator state (and restores the real clock).
func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(baseURL()+"/_jaiscloud/reset", "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// adminPost POSTs to an admin endpoint and returns the status code.
func adminPost(t *testing.T, path string) int {
	t.Helper()
	resp, err := http.Post(baseURL()+path, "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func newService(t *testing.T) *cloudtasks.Service {
	t.Helper()
	svc, err := cloudtasks.NewService(context.Background(), opts()...)
	require.NoError(t, err)
	return svc
}

type recorded struct {
	method string
	path   string
	header http.Header
	body   []byte
}

// hook is a local HTTP target that records the requests the emulator delivers.
type hook struct {
	mu    sync.Mutex
	got   []recorded
	fail  bool
	block chan struct{}
}

func newHook() *hook { return &hook{} }

func (h *hook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	h.mu.Lock()
	h.got = append(h.got, recorded{method: r.Method, path: r.URL.Path, header: r.Header.Clone(), body: body})
	fail := h.fail
	block := h.block
	h.mu.Unlock()
	if block != nil {
		<-block
	}
	if fail {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *hook) setFail(v bool) {
	h.mu.Lock()
	h.fail = v
	h.mu.Unlock()
}

func (h *hook) setBlock(ch chan struct{}) {
	h.mu.Lock()
	h.block = ch
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

func mustCreateQueue(t *testing.T, svc *cloudtasks.Service, usrParent string, q *cloudtasks.Queue) *cloudtasks.Queue {
	t.Helper()
	created, err := svc.Projects.Locations.Queues.Create(usrParent, q).Do()
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

func httpTask(url string) *cloudtasks.HttpRequest {
	return &cloudtasks.HttpRequest{Url: url, HttpMethod: "POST"}
}

// TestSDKTasksQueueLifecycle covers queue CRUD, list + filter, the update mask,
// pause/resume, and delete.
func TestSDKTasksQueueLifecycle(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	qid := unique("queue")
	name := parent + "/queues/" + qid
	created := mustCreateQueue(t, svc, parent, &cloudtasks.Queue{
		Name: name,
		RateLimits: &cloudtasks.RateLimits{
			MaxDispatchesPerSecond:  10,
			MaxBurstSize:            20,
			MaxConcurrentDispatches: 5,
		},
		RetryConfig: &cloudtasks.RetryConfig{
			MaxAttempts:  3,
			MinBackoff:   "1s",
			MaxBackoff:   "10s",
			MaxDoublings: 2,
		},
	})
	require.Equal(t, name, created.Name)
	require.Equal(t, "RUNNING", created.State)
	require.NotNil(t, created.RateLimits)
	require.Equal(t, 10.0, created.RateLimits.MaxDispatchesPerSecond)
	require.EqualValues(t, 20, created.RateLimits.MaxBurstSize)
	require.EqualValues(t, 5, created.RateLimits.MaxConcurrentDispatches)
	require.NotNil(t, created.RetryConfig)
	require.EqualValues(t, 3, created.RetryConfig.MaxAttempts)
	require.Equal(t, "1s", created.RetryConfig.MinBackoff)
	require.Equal(t, "10s", created.RetryConfig.MaxBackoff)
	require.EqualValues(t, 2, created.RetryConfig.MaxDoublings)

	got, err := svc.Projects.Locations.Queues.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, name, got.Name)

	// Duplicate create is AlreadyExists/409.
	_, err = svc.Projects.Locations.Queues.Create(parent, &cloudtasks.Queue{Name: name}).Do()
	require.Equal(t, 409, apiErr(t, err).Code)

	// List with a filter. The Discovery document defers to Stackdriver's
	// Advanced Logs Filters syntax: "=" is equality on the Queue field (the full
	// resource name), ":" is the contains operator.
	matched, err := svc.Projects.Locations.Queues.List(parent).Filter(`name="` + name + `"`).Do()
	require.NoError(t, err)
	require.Len(t, matched.Queues, 1)
	require.Equal(t, name, matched.Queues[0].Name)
	bare, err := svc.Projects.Locations.Queues.List(parent).Filter(`name="` + qid + `"`).Do()
	require.NoError(t, err)
	require.Empty(t, bare.Queues, "= is equality on the full name, not a substring")
	contains, err := svc.Projects.Locations.Queues.List(parent).Filter(`name:"` + qid + `"`).Do()
	require.NoError(t, err)
	require.Len(t, contains.Queues, 1)
	require.Equal(t, name, contains.Queues[0].Name)
	unmatched, err := svc.Projects.Locations.Queues.List(parent).Filter(`name="no-such-queue-xyz"`).Do()
	require.NoError(t, err)
	require.Empty(t, unmatched.Queues)
	// An out-of-grammar filter is INVALID_ARGUMENT, not silently ignored.
	_, err = svc.Projects.Locations.Queues.List(parent).Filter(`bogus="x"`).Do()
	require.Equal(t, 400, apiErr(t, err).Code)

	// The filter composes with the cursor: a filtered walk visits only the
	// matching queues, page by page.
	var extras []string
	for _, suffix := range []string{"-a", "-b"} {
		id := qid + suffix
		mustCreateQueue(t, svc, parent, &cloudtasks.Queue{Name: parent + "/queues/" + id})
		extras = append(extras, parent+"/queues/"+id)
	}
	var walked []string
	pageToken := ""
	for {
		call := svc.Projects.Locations.Queues.List(parent).Filter(`name:"` + qid + `"`).PageSize(1)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		page, err := call.Do()
		require.NoError(t, err)
		for _, q := range page.Queues {
			walked = append(walked, q.Name)
		}
		pageToken = page.NextPageToken
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, []string{name, extras[0], extras[1]}, walked)
	for _, extra := range extras {
		_, err := svc.Projects.Locations.Queues.Delete(extra).Do()
		require.NoError(t, err)
	}

	// Patch only rateLimits; retryConfig is preserved.
	patched, err := svc.Projects.Locations.Queues.Patch(name, &cloudtasks.Queue{
		RateLimits: &cloudtasks.RateLimits{MaxDispatchesPerSecond: 25, MaxBurstSize: 30, MaxConcurrentDispatches: 7},
	}).UpdateMask("rateLimits").Do()
	require.NoError(t, err)
	require.Equal(t, 25.0, patched.RateLimits.MaxDispatchesPerSecond)
	require.NotNil(t, patched.RetryConfig)
	require.EqualValues(t, 3, patched.RetryConfig.MaxAttempts, "retryConfig must survive a rateLimits-only patch")

	// Pause / resume with the already-in-that-state failures.
	paused, err := svc.Projects.Locations.Queues.Pause(name, &cloudtasks.PauseQueueRequest{}).Do()
	require.NoError(t, err)
	require.Equal(t, "PAUSED", paused.State)
	_, err = svc.Projects.Locations.Queues.Pause(name, &cloudtasks.PauseQueueRequest{}).Do()
	require.Equal(t, 400, apiErr(t, err).Code)
	// The Discovery document's own sample filter, verbatim ("state: PAUSED").
	pausedOnly, err := svc.Projects.Locations.Queues.List(parent).Filter(`state: PAUSED`).Do()
	require.NoError(t, err)
	require.Len(t, pausedOnly.Queues, 1)
	require.Equal(t, name, pausedOnly.Queues[0].Name)
	resumed, err := svc.Projects.Locations.Queues.Resume(name, &cloudtasks.ResumeQueueRequest{}).Do()
	require.NoError(t, err)
	require.Equal(t, "RUNNING", resumed.State)
	_, err = svc.Projects.Locations.Queues.Resume(name, &cloudtasks.ResumeQueueRequest{}).Do()
	require.Equal(t, 400, apiErr(t, err).Code)

	_, err = svc.Projects.Locations.Queues.Delete(name).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Locations.Queues.Get(name).Do()
	require.Equal(t, 404, apiErr(t, err).Code)
}

// TestSDKTasksTaskCRUDAndBatch covers task create/get/list/delete plus the
// batch endpoints. batchCreate/batchDelete return a done google.longrunning
// .Operation whose typed response carries the created tasks / Empty (the
// emulator runs them synchronously — see the package doc).
func TestSDKTasksTaskCRUDAndBatch(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	qid := unique("queue")
	queueName := parent + "/queues/" + qid
	mustCreateQueue(t, svc, parent, &cloudtasks.Queue{Name: queueName})

	tid := unique("task")
	taskName := queueName + "/tasks/" + tid
	payload := `{"n":1}`
	created, err := svc.Projects.Locations.Queues.Tasks.Create(queueName, &cloudtasks.CreateTaskRequest{
		Task: &cloudtasks.Task{
			Name: taskName,
			HttpRequest: &cloudtasks.HttpRequest{
				Url:        srv.URL + "/hook",
				HttpMethod: "POST",
				Headers:    map[string]string{"X-Test": "t"},
				Body:       base64.StdEncoding.EncodeToString([]byte(payload)),
			},
			ScheduleTime: futureRFC(),
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, taskName, created.Name)
	require.NotNil(t, created.HttpRequest)
	require.Equal(t, srv.URL+"/hook", created.HttpRequest.Url)
	require.Equal(t, "POST", created.HttpRequest.HttpMethod)
	require.Equal(t, "600s", created.DispatchDeadline, "unset dispatchDeadline defaults to 10m")
	require.NotEmpty(t, created.ScheduleTime)

	got, err := svc.Projects.Locations.Queues.Tasks.Get(taskName).Do()
	require.NoError(t, err)
	require.Equal(t, taskName, got.Name)

	// batchCreate: the emulator creates synchronously and returns a done
	// google.longrunning.Operation whose BatchCreateTasksResponse carries the
	// created tasks (real GCP returns an in-flight operation that is polled).
	tid2, tid3 := unique("task"), unique("task")
	name2, name3 := queueName+"/tasks/"+tid2, queueName+"/tasks/"+tid3
	batchOp, err := svc.Projects.Locations.Queues.Tasks.BatchCreate(queueName, &cloudtasks.BatchCreateTasksRequest{
		Requests: []*cloudtasks.CreateTaskRequest{
			{Task: &cloudtasks.Task{Name: name2, HttpRequest: httpTask(srv.URL), ScheduleTime: futureRFC()}},
			{Task: &cloudtasks.Task{Name: name3, HttpRequest: httpTask(srv.URL), ScheduleTime: futureRFC()}},
		},
	}).Do()
	require.NoError(t, err)
	require.True(t, batchOp.Done, "batchCreate must return a done operation: %+v", batchOp)
	var batchResp struct {
		Tasks []*cloudtasks.Task `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal(batchOp.Response, &batchResp))
	require.Len(t, batchResp.Tasks, 2)
	require.ElementsMatch(t, []string{name2, name3},
		[]string{batchResp.Tasks[0].Name, batchResp.Tasks[1].Name})

	list, err := svc.Projects.Locations.Queues.Tasks.List(queueName).Do()
	require.NoError(t, err)
	require.Len(t, list.Tasks, 3)

	// batchDelete removes both batch tasks and returns a done operation whose
	// response is google.protobuf.Empty.
	delOp, err := svc.Projects.Locations.Queues.Tasks.BatchDelete(queueName, &cloudtasks.BatchDeleteTasksRequest{
		Names: []string{name2, name3},
	}).Do()
	require.NoError(t, err)
	require.True(t, delOp.Done, "batchDelete must return a done operation: %+v", delOp)
	var deleteResp map[string]any
	require.NoError(t, json.Unmarshal(delOp.Response, &deleteResp))
	require.Equal(t, "type.googleapis.com/google.protobuf.Empty", deleteResp["@type"])
	list, err = svc.Projects.Locations.Queues.Tasks.List(queueName).Do()
	require.NoError(t, err)
	require.Len(t, list.Tasks, 1)
	require.Equal(t, taskName, list.Tasks[0].Name)

	// Delete the survivor.
	_, err = svc.Projects.Locations.Queues.Tasks.Delete(taskName).Do()
	require.NoError(t, err)
	list, err = svc.Projects.Locations.Queues.Tasks.List(queueName).Do()
	require.NoError(t, err)
	require.Empty(t, list.Tasks)
}

// TestSDKTasksRunDeliversHTTPAndRetries forces attempts with Tasks.run, asserts
// the delivered request and the emulator's Cloud Tasks dispatch headers, and
// checks the retry accounting across a failure then a success.
func TestSDKTasksRunDeliversHTTPAndRetries(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	h := newHook()
	srv := httptest.NewServer(h)
	defer srv.Close()

	// A long minBackoff keeps the background engine from retrying the failed
	// task; RunTask drives every attempt explicitly.
	qid := unique("queue")
	queueName := parent + "/queues/" + qid
	mustCreateQueue(t, svc, parent, &cloudtasks.Queue{
		Name:        queueName,
		RateLimits:  &cloudtasks.RateLimits{MaxDispatchesPerSecond: 500, MaxBurstSize: 100, MaxConcurrentDispatches: 100},
		RetryConfig: &cloudtasks.RetryConfig{MaxAttempts: 5, MinBackoff: "1h", MaxBackoff: "1h", MaxDoublings: 1},
	})

	tid := unique("task")
	taskName := queueName + "/tasks/" + tid
	payload := `{"retry":true}`
	_, err := svc.Projects.Locations.Queues.Tasks.Create(queueName, &cloudtasks.CreateTaskRequest{
		Task: &cloudtasks.Task{
			Name: taskName,
			HttpRequest: &cloudtasks.HttpRequest{
				Url:        srv.URL + "/work",
				HttpMethod: "POST",
				Body:       base64.StdEncoding.EncodeToString([]byte(payload)),
			},
			ScheduleTime: futureRFC(),
		},
	}).Do()
	require.NoError(t, err)

	// Attempt 1 fails with 500.
	h.setFail(true)
	failed, err := svc.Projects.Locations.Queues.Tasks.Run(taskName, &cloudtasks.RunTaskRequest{}).Do()
	require.NoError(t, err)
	require.EqualValues(t, 1, failed.DispatchCount)
	require.EqualValues(t, 1, failed.ResponseCount)
	require.NotNil(t, failed.FirstAttempt)
	require.NotEmpty(t, failed.FirstAttempt.DispatchTime)
	require.NotNil(t, failed.LastAttempt)
	require.NotNil(t, failed.LastAttempt.ResponseStatus)
	require.EqualValues(t, 500, failed.LastAttempt.ResponseStatus.Code)
	failedETA, err := time.Parse(time.RFC3339Nano, failed.ScheduleTime)
	require.NoError(t, err)
	require.True(t, failedETA.After(time.Now()), "a failed attempt must reschedule into the future")

	require.Eventually(t, func() bool { return h.count() >= 1 }, 5*time.Second, 20*time.Millisecond)
	req := h.requests()[0]
	require.Equal(t, "POST", req.method)
	require.Equal(t, "/work", req.path)
	require.Equal(t, payload, string(req.body))
	require.Equal(t, "Google-Cloud-Tasks", req.header.Get("User-Agent"))
	require.Equal(t, qid, req.header.Get("X-CloudTasks-QueueName"))
	require.Equal(t, tid, req.header.Get("X-CloudTasks-TaskName"))
	require.Equal(t, "0", req.header.Get("X-CloudTasks-TaskRetryCount"))
	require.Equal(t, "0", req.header.Get("X-CloudTasks-TaskExecutionCount"))
	require.NotEmpty(t, req.header.Get("X-CloudTasks-TaskETA"))
	require.Empty(t, req.header.Get("Content-Type"), "Cloud Tasks does not set Content-Type")

	// Attempt 2 succeeds: the task is deleted and the returned snapshot counts it.
	h.setFail(false)
	ok, err := svc.Projects.Locations.Queues.Tasks.Run(taskName, &cloudtasks.RunTaskRequest{}).Do()
	require.NoError(t, err)
	require.EqualValues(t, 2, ok.DispatchCount)
	require.EqualValues(t, 2, ok.ResponseCount)
	_, err = svc.Projects.Locations.Queues.Tasks.Get(taskName).Do()
	require.Equal(t, 404, apiErr(t, err).Code, "a successfully dispatched task is deleted")
}

// TestSDKTasksRateLimits pins the two documented per-queue dispatch throttles:
// the concurrent-dispatch cap (deterministic with a blocking target) and the
// token-bucket rate (a lower bound on elapsed time for N dispatches).
func TestSDKTasksRateLimits(t *testing.T) {
	t.Run("MaxConcurrentDispatches", func(t *testing.T) {
		resetState(t)
		svc := newService(t)
		project := projectID()
		parent := "projects/" + project + "/locations/" + locationID()

		release := make(chan struct{})
		h := newHook()
		h.setBlock(release)
		srv := httptest.NewServer(h)
		defer srv.Close()

		qid := unique("queue")
		queueName := parent + "/queues/" + qid
		mustCreateQueue(t, svc, parent, &cloudtasks.Queue{
			Name:       queueName,
			RateLimits: &cloudtasks.RateLimits{MaxDispatchesPerSecond: 500, MaxBurstSize: 100, MaxConcurrentDispatches: 1},
		})
		for i := 0; i < 3; i++ {
			_, err := svc.Projects.Locations.Queues.Tasks.Create(queueName, &cloudtasks.CreateTaskRequest{
				Task: &cloudtasks.Task{Name: queueName + "/tasks/" + unique("cap"), HttpRequest: httpTask(srv.URL)},
			}).Do()
			require.NoError(t, err)
		}

		require.Eventually(t, func() bool { return h.count() >= 1 }, 6*time.Second, 20*time.Millisecond,
			"the first task must dispatch")
		time.Sleep(300 * time.Millisecond)
		require.Equal(t, 1, h.count(), "maxConcurrentDispatches=1 must hold the other tasks back")

		close(release)
		require.Eventually(t, func() bool { return h.count() == 3 }, 8*time.Second, 50*time.Millisecond,
			"all tasks must dispatch once the in-flight one completes")
		require.Eventually(t, func() bool {
			list, err := svc.Projects.Locations.Queues.Tasks.List(queueName).Do()
			return err == nil && len(list.Tasks) == 0
		}, 5*time.Second, 50*time.Millisecond, "successful tasks are deleted")
	})

	t.Run("TokenBucket", func(t *testing.T) {
		resetState(t)
		svc := newService(t)
		project := projectID()
		parent := "projects/" + project + "/locations/" + locationID()

		h := newHook()
		srv := httptest.NewServer(h)
		defer srv.Close()

		qid := unique("queue")
		queueName := parent + "/queues/" + qid
		mustCreateQueue(t, svc, parent, &cloudtasks.Queue{
			Name:       queueName,
			RateLimits: &cloudtasks.RateLimits{MaxDispatchesPerSecond: 1, MaxBurstSize: 1, MaxConcurrentDispatches: 100},
		})
		for i := 0; i < 3; i++ {
			_, err := svc.Projects.Locations.Queues.Tasks.Create(queueName, &cloudtasks.CreateTaskRequest{
				Task: &cloudtasks.Task{Name: queueName + "/tasks/" + unique("rate"), HttpRequest: httpTask(srv.URL)},
			}).Do()
			require.NoError(t, err)
		}

		start := time.Now()
		deadline := start.Add(10 * time.Second)
		for {
			require.Equal(t, http.StatusNoContent, adminPost(t, "/_jaiscloud/tasks-tick"))
			list, err := svc.Projects.Locations.Queues.Tasks.List(queueName).Do()
			require.NoError(t, err)
			if len(list.Tasks) == 0 || time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		elapsed := time.Since(start)
		require.Equal(t, 3, h.count(), "all three tasks must eventually dispatch")
		// burst=1 then 1/s: the third dispatch cannot happen before ~2s.
		require.GreaterOrEqual(t, elapsed, 1500*time.Millisecond,
			"maxDispatchesPerSecond=1/burst=1 must rate-limit the dispatches (elapsed %s)", elapsed)
	})
}

// TestSDKTasksAppEngineDeviation pins the documented appEngineHttpRequest
// behavior: the task is stored and echoed, but an attempt is recorded as
// Unimplemented and the task is retained (no App Engine router).
func TestSDKTasksAppEngineDeviation(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	qid := unique("queue")
	queueName := parent + "/queues/" + qid
	mustCreateQueue(t, svc, parent, &cloudtasks.Queue{
		Name:        queueName,
		RetryConfig: &cloudtasks.RetryConfig{MaxAttempts: 3, MinBackoff: "1h", MaxBackoff: "1h"},
	})

	tid := unique("aetask")
	taskName := queueName + "/tasks/" + tid
	created, err := svc.Projects.Locations.Queues.Tasks.Create(queueName, &cloudtasks.CreateTaskRequest{
		Task: &cloudtasks.Task{
			Name:                 taskName,
			AppEngineHttpRequest: &cloudtasks.AppEngineHttpRequest{RelativeUri: "/handler", HttpMethod: "POST"},
			ScheduleTime:         futureRFC(),
		},
	}).Do()
	require.NoError(t, err)
	require.NotNil(t, created.AppEngineHttpRequest)
	require.Equal(t, "/handler", created.AppEngineHttpRequest.RelativeUri)

	ran, err := svc.Projects.Locations.Queues.Tasks.Run(taskName, &cloudtasks.RunTaskRequest{}).Do()
	require.NoError(t, err)
	require.EqualValues(t, 1, ran.DispatchCount)
	require.EqualValues(t, 0, ran.ResponseCount, "an Unimplemented attempt receives no response")
	require.NotNil(t, ran.LastAttempt)
	require.NotNil(t, ran.LastAttempt.ResponseStatus)
	require.EqualValues(t, 12, ran.LastAttempt.ResponseStatus.Code, "gRPC Unimplemented")

	got, err := svc.Projects.Locations.Queues.Tasks.Get(taskName).Do()
	require.NoError(t, err, "the un-deliverable task is retained")
	require.EqualValues(t, 1, got.DispatchCount)
}

// TestSDKTasksErrors pins the documented error contracts.
func TestSDKTasksErrors(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	// Missing queue reads / purges / task creates are NotFound/404.
	missingQueue := parent + "/queues/" + unique("ghost")
	_, err := svc.Projects.Locations.Queues.Get(missingQueue).Do()
	require.Equal(t, 404, apiErr(t, err).Code)
	_, err = svc.Projects.Locations.Queues.Purge(missingQueue, &cloudtasks.PurgeQueueRequest{}).Do()
	require.Equal(t, 404, apiErr(t, err).Code)
	_, err = svc.Projects.Locations.Queues.Tasks.Create(missingQueue, &cloudtasks.CreateTaskRequest{
		Task: &cloudtasks.Task{Name: missingQueue + "/tasks/x", HttpRequest: httpTask("http://example.com")},
	}).Do()
	require.Equal(t, 404, apiErr(t, err).Code)

	// A queue rate over the documented maximum is InvalidArgument/400; a task
	// with an unusable target is InvalidArgument/400.
	_, err = svc.Projects.Locations.Queues.Create(parent, &cloudtasks.Queue{
		Name:       parent + "/queues/" + unique("overrate"),
		RateLimits: &cloudtasks.RateLimits{MaxDispatchesPerSecond: 501},
	}).Do()
	require.Equal(t, 400, apiErr(t, err).Code)

	qid := unique("queue")
	queueName := parent + "/queues/" + qid
	mustCreateQueue(t, svc, parent, &cloudtasks.Queue{Name: queueName})

	_, err = svc.Projects.Locations.Queues.Tasks.Create(queueName, &cloudtasks.CreateTaskRequest{
		Task: &cloudtasks.Task{Name: queueName + "/tasks/" + unique("badurl"), HttpRequest: &cloudtasks.HttpRequest{Url: "not-a-url"}},
	}).Do()
	require.Equal(t, 400, apiErr(t, err).Code)
	_, err = svc.Projects.Locations.Queues.Tasks.Create(queueName, &cloudtasks.CreateTaskRequest{
		Task: &cloudtasks.Task{Name: queueName + "/tasks/" + unique("notarget")},
	}).Do()
	require.Equal(t, 400, apiErr(t, err).Code)

	// Missing task reads / runs are NotFound/404.
	missingTask := queueName + "/tasks/" + unique("ghost")
	_, err = svc.Projects.Locations.Queues.Tasks.Get(missingTask).Do()
	require.Equal(t, 404, apiErr(t, err).Code)
	_, err = svc.Projects.Locations.Queues.Tasks.Run(missingTask, &cloudtasks.RunTaskRequest{}).Do()
	require.Equal(t, 404, apiErr(t, err).Code)

	// A malformed task name in batchDelete is InvalidArgument/400.
	_, err = svc.Projects.Locations.Queues.Tasks.BatchDelete(queueName, &cloudtasks.BatchDeleteTasksRequest{
		Names: []string{"not-a-task-name"},
	}).Do()
	require.Equal(t, 400, apiErr(t, err).Code)
}

// TestSDKTasksPurge covers PurgeQueue: it deletes every task and records
// purgeTime on the queue.
func TestSDKTasksPurge(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	qid := unique("queue")
	queueName := parent + "/queues/" + qid
	mustCreateQueue(t, svc, parent, &cloudtasks.Queue{Name: queueName})
	for i := 0; i < 2; i++ {
		_, err := svc.Projects.Locations.Queues.Tasks.Create(queueName, &cloudtasks.CreateTaskRequest{
			Task: &cloudtasks.Task{Name: queueName + "/tasks/" + unique("purge"), HttpRequest: httpTask("http://example.com"), ScheduleTime: futureRFC()},
		}).Do()
		require.NoError(t, err)
	}

	purged, err := svc.Projects.Locations.Queues.Purge(queueName, &cloudtasks.PurgeQueueRequest{}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, purged.PurgeTime)
	list, err := svc.Projects.Locations.Queues.Tasks.List(queueName).Do()
	require.NoError(t, err)
	require.Empty(t, list.Tasks)
}
