// depth_test.go closes DC6 (plan_docs/gcp-depth-closure-wave-plan.md W2.4) for
// Cloud Tasks: the p4 and p5 cells its record left exempt — queue/task listing
// paging and a many-queues/tasks boundary — asserted through the official Cloud
// Tasks v2 apiary client and the shared depth-test kit
// (tests/integration/gcp/testdepth).
//
// The Discovery document declares `pageSize`/`pageToken` on
// `projects.locations.queues.list` and `...queues.tasks.list`; it documents that
// queues are returned in lexicographical order and that tasks may be in any
// order, so the queue walk asserts OrderAscendingName and the task walk
// OrderUnspecified. The queue list's `filter` is exercised separately
// (sdk_tasks_test.go) after DC16 aligned it with the documented
// Stackdriver/Logging syntax; p5 is the boundary the class
// names: a twenty-five-element collection walked at a small page size, i.e. the
// cursor chain over a large collection rather than one response.
package sdk_tasks_test

import (
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	cloudtasks "google.golang.org/api/cloudtasks/v2"
	testdepth "jaiscloud-gcp-testdepth"
)

// tasksDepthPageSize is deliberately small so a walk crosses many page
// boundaries.
const tasksDepthPageSize = 4

// walkQueues runs projects.locations.queues.list through the kit and returns the
// queues in walk order.
func walkQueues(t *testing.T, svc *cloudtasks.Service, parent string, cfg testdepth.Paging[*cloudtasks.Queue]) []*cloudtasks.Queue {
	t.Helper()
	return testdepth.AssertPaging(t, func() testdepth.Lister[*cloudtasks.Queue] {
		return func(req testdepth.ListRequest) (testdepth.Page[*cloudtasks.Queue], error) {
			call := svc.Projects.Locations.Queues.List(parent)
			if req.PageSize > 0 {
				call = call.PageSize(req.PageSize)
			}
			if req.PageToken != "" {
				call = call.PageToken(req.PageToken)
			}
			resp, err := call.Do()
			if err != nil {
				return testdepth.Page[*cloudtasks.Queue]{}, err
			}
			return testdepth.Page[*cloudtasks.Queue]{Items: resp.Queues, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
}

// walkTasks runs projects.locations.queues.tasks.list through the kit and
// returns the tasks in walk order.
func walkTasks(t *testing.T, svc *cloudtasks.Service, queue string, cfg testdepth.Paging[*cloudtasks.Task]) []*cloudtasks.Task {
	t.Helper()
	return testdepth.AssertPaging(t, func() testdepth.Lister[*cloudtasks.Task] {
		return func(req testdepth.ListRequest) (testdepth.Page[*cloudtasks.Task], error) {
			call := svc.Projects.Locations.Queues.Tasks.List(queue)
			if req.PageSize > 0 {
				call = call.PageSize(req.PageSize)
			}
			if req.PageToken != "" {
				call = call.PageToken(req.PageToken)
			}
			resp, err := call.Do()
			if err != nil {
				return testdepth.Page[*cloudtasks.Task]{}, err
			}
			return testdepth.Page[*cloudtasks.Task]{Items: resp.Tasks, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
}

// fullQueueName returns a queue's full resource name.
func fullQueueName(parent, id string) string { return parent + "/queues/" + id }

// fullTaskName returns a task's full resource name.
func fullTaskName(queue, id string) string { return queue + "/tasks/" + id }

// mustCreateTask creates a task on an existing queue.
func mustCreateTask(t *testing.T, svc *cloudtasks.Service, queue, id string) {
	t.Helper()
	_, err := svc.Projects.Locations.Queues.Tasks.Create(queue, &cloudtasks.CreateTaskRequest{
		Task: &cloudtasks.Task{
			Name:         fullTaskName(queue, id),
			HttpRequest:  httpTask("http://example.com/hook"),
			ScheduleTime: futureRFC(),
		},
	}).Do()
	if err != nil {
		t.Fatalf("CreateTask(%s): %v", id, err)
	}
}

// TestSDKTasksDepthPaging is tasks p4: the list/paging contract over queues
// (asserted in the documented lexicographical order) and over a queue's tasks
// (any order) at a page size that forces several pages.
func TestSDKTasksDepthPaging(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	t.Run("Queues", func(t *testing.T) {
		const queues = 12
		want := make([]string, 0, queues)
		for i := 0; i < queues; i++ {
			name := fullQueueName(parent, unique("pageq")+"-"+strconv.Itoa(i))
			mustCreateQueue(t, svc, parent, &cloudtasks.Queue{Name: name})
			want = append(want, name)
		}
		got := walkQueues(t, svc, parent, testdepth.Paging[*cloudtasks.Queue]{
			Name:                 func(q *cloudtasks.Queue) string { return q.Name },
			PageSize:             tasksDepthPageSize,
			Order:                testdepth.OrderAscendingName,
			RequireNonEmptyPages: true,
			Expect:               want,
		})
		if len(got) != queues {
			t.Fatalf("queue walk returned %d queues, want %d", len(got), queues)
		}
	})

	t.Run("Tasks", func(t *testing.T) {
		queue := fullQueueName(parent, unique("pagetasks"))
		mustCreateQueue(t, svc, parent, &cloudtasks.Queue{Name: queue})

		const tasks = 12
		want := make([]string, 0, tasks)
		for i := 0; i < tasks; i++ {
			id := unique("t") + "-" + strconv.Itoa(i)
			mustCreateTask(t, svc, queue, id)
			want = append(want, fullTaskName(queue, id))
		}
		got := walkTasks(t, svc, queue, testdepth.Paging[*cloudtasks.Task]{
			Name:                 func(task *cloudtasks.Task) string { return task.Name },
			PageSize:             tasksDepthPageSize,
			Order:                testdepth.OrderUnspecified,
			RequireNonEmptyPages: true,
			Expect:               want,
		})
		if len(got) != tasks {
			t.Fatalf("task walk returned %d tasks, want %d", len(got), tasks)
		}
	})
}

// TestSDKTasksDepthScale is tasks p5: a many-queues and many-tasks boundary,
// each collection walked at a small page size so the whole cursor chain over a
// large collection is exercised rather than a single response.
func TestSDKTasksDepthScale(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	const scale = 25

	wantQueues := make([]string, 0, scale)
	for i := 0; i < scale; i++ {
		name := fullQueueName(parent, unique("scaleq")+"-"+strconv.Itoa(i))
		mustCreateQueue(t, svc, parent, &cloudtasks.Queue{Name: name})
		wantQueues = append(wantQueues, name)
	}
	gotQueues := walkQueues(t, svc, parent, testdepth.Paging[*cloudtasks.Queue]{
		Name:                 func(q *cloudtasks.Queue) string { return q.Name },
		PageSize:             tasksDepthPageSize,
		Order:                testdepth.OrderAscendingName,
		RequireNonEmptyPages: true,
		Expect:               wantQueues,
	})
	if len(gotQueues) != scale {
		t.Fatalf("queue scale walk returned %d queues, want %d", len(gotQueues), scale)
	}

	queue := fullQueueName(parent, unique("scaletasks"))
	mustCreateQueue(t, svc, parent, &cloudtasks.Queue{Name: queue})
	wantTasks := make([]string, 0, scale)
	for i := 0; i < scale; i++ {
		id := unique("scalet") + "-" + strconv.Itoa(i)
		mustCreateTask(t, svc, queue, id)
		wantTasks = append(wantTasks, fullTaskName(queue, id))
	}
	gotTasks := walkTasks(t, svc, queue, testdepth.Paging[*cloudtasks.Task]{
		Name:                 func(task *cloudtasks.Task) string { return task.Name },
		PageSize:             tasksDepthPageSize,
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               wantTasks,
	})
	if len(gotTasks) != scale {
		t.Fatalf("task scale walk returned %d tasks, want %d", len(gotTasks), scale)
	}
}

// TestSDKTasksDepthNegativePageSize pins AIP-158 on the REST surface through
// the official apiary client: a negative pageSize is INVALID_ARGUMENT/400 on
// both list methods, rather than a silent fall-back to the documented maximum.
func TestSDKTasksDepthNegativePageSize(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	queue := fullQueueName(parent, unique("negpage"))
	mustCreateQueue(t, svc, parent, &cloudtasks.Queue{Name: queue})

	testdepth.RunNegatives(t, []testdepth.NegativeCase{
		{
			Name: "QueuesListNegativePageSize",
			Run: func() error {
				_, err := svc.Projects.Locations.Queues.List(parent).PageSize(-1).Do()
				return err
			},
			Want: testdepth.SpecInvalidArgument(),
		},
		{
			Name: "TasksListNegativePageSize",
			Run: func() error {
				_, err := svc.Projects.Locations.Queues.Tasks.List(queue).PageSize(-1).Do()
				return err
			},
			Want: testdepth.SpecInvalidArgument(),
		},
	})

	// queues.list documents a 9800 ceiling, not tasks.list's 1000: 9800 is a
	// legal request and returns the queue, not an over-large fall-back.
	list, err := svc.Projects.Locations.Queues.List(parent).PageSize(9800).Do()
	require.NoError(t, err)
	require.Len(t, list.Queues, 1)
	require.Equal(t, queue, list.Queues[0].Name)

	// A non-integer pageSize fails the int32 query binding with 400 — the
	// apiary client cannot emit one (PageSize is an int64), so pin it over raw
	// HTTP against the same REST surface.
	for _, tc := range []struct{ name, path string }{
		{"queues.list", "/v2/" + parent + "/queues"},
		{"tasks.list", "/v2/" + queue + "/tasks"},
	} {
		for _, v := range []string{"abc", "-0.5", "1.5"} {
			resp, err := http.Get(baseURL() + tc.path + "?pageSize=" + v)
			require.NoError(t, err)
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s pageSize=%s body=%s", tc.name, v, body)
			require.Contains(t, string(body), "INVALID_ARGUMENT")
		}
	}
}
