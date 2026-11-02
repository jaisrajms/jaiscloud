package grpcconformance

import (
	"context"
	"fmt"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	cloudtaskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// tasksLocation is a canonical Cloud Tasks location; the emulator does not
// validate the location list.
const tasksLocation = "us-central1"

// tasksChecks covers the Cloud Tasks v2 control plane and dispatch engine
// (google.cloud.tasks.v2.CloudTasks) via the official generated
// cloud.google.com/go/cloudtasks/apiv2 client. RunTask is probed: it forces a
// synchronous delivery to the task's HTTP target, which is the emulator's own
// health endpoint, and a successful (2xx) run deletes the task. The
// location-scoped CMEK config is probed through
// GetCmekConfig/UpdateCmekConfig. The REST-only tasks:buffer and
// tasks:batchCreate / tasks:batchDelete have no gRPC method.
//
// Every probe is self-contained and run-unique (cfg.ResourceName).
func tasksChecks() []Check {
	return []Check{
		{Service: "tasks", RPC: "CreateQueue", Method: "CreateQueue", KeyField: "name + state RUNNING + default rateLimits", Run: checkTasksCreateQueue},
		{Service: "tasks", RPC: "GetQueue", Method: "GetQueue", KeyField: "name round-trip", Run: checkTasksGetQueue},
		{Service: "tasks", RPC: "ListQueues", Method: "ListQueues", KeyField: "created queue present", Run: checkTasksListQueues},
		{Service: "tasks", RPC: "UpdateQueue", Method: "UpdateQueue", KeyField: "rateLimits updated (masked)", Run: checkTasksUpdateQueue},
		{Service: "tasks", RPC: "PauseQueue", Method: "PauseQueue", KeyField: "state PAUSED", Run: checkTasksPauseQueue},
		{Service: "tasks", RPC: "ResumeQueue", Method: "ResumeQueue", KeyField: "state RUNNING", Run: checkTasksResumeQueue},
		{Service: "tasks", RPC: "PurgeQueue", Method: "PurgeQueue", KeyField: "purgeTime set", Run: checkTasksPurgeQueue},
		{Service: "tasks", RPC: "DeleteQueue", Method: "DeleteQueue", KeyField: "subsequent GetQueue NOT_FOUND", Run: checkTasksDeleteQueue},
		{Service: "tasks", RPC: "GetIamPolicy", Method: "GetIamPolicy", KeyField: "etag present", Run: checkTasksGetIamPolicy},
		{Service: "tasks", RPC: "SetIamPolicy", Method: "SetIamPolicy", KeyField: "binding persisted", Run: checkTasksSetIamPolicy},
		{Service: "tasks", RPC: "TestIamPermissions", Method: "TestIamPermissions", KeyField: "requested permission echoed", Run: checkTasksTestIamPermissions},
		{Service: "tasks", RPC: "CreateTask", Method: "CreateTask", KeyField: "name + scheduleTime + httpRequest round-trip", Run: checkTasksCreateTask},
		{Service: "tasks", RPC: "BatchCreateTasks", Method: "BatchCreateTasks", KeyField: "done operation carrying both created tasks", Run: checkTasksBatchCreate},
		{Service: "tasks", RPC: "BatchDeleteTasks", Method: "BatchDeleteTasks", KeyField: "done operation; tasks gone", Run: checkTasksBatchDelete},
		{Service: "tasks", RPC: "GetTask", Method: "GetTask", KeyField: "name round-trip", Run: checkTasksGetTask},
		{Service: "tasks", RPC: "ListTasks", Method: "ListTasks", KeyField: "created task present", Run: checkTasksListTasks},
		{Service: "tasks", RPC: "DeleteTask", Method: "DeleteTask", KeyField: "subsequent GetTask NOT_FOUND", Run: checkTasksDeleteTask},
		{Service: "tasks", RPC: "RunTask", Method: "RunTask", KeyField: "dispatched task returned; success deletes it", Run: checkTasksRunTask},
		{Service: "tasks", RPC: "GetCmekConfig", Method: "GetCmekConfig", KeyField: "default config: name set, kmsKey empty", Run: checkTasksGetCmekConfig},
		{Service: "tasks", RPC: "UpdateCmekConfig", Method: "UpdateCmekConfig", KeyField: "kmsKey round-trip", Run: checkTasksUpdateCmekConfig},
	}
}

// newTasksClient dials the emulator and returns the official generated Cloud
// Tasks client.
func newTasksClient(ctx context.Context, cfg Config) (*cloudtasks.Client, error) {
	return cloudtasks.NewClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func tasksParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, tasksLocation)
}

func tasksQueueName(cfg Config, prefix string) string {
	return tasksParent(cfg) + "/queues/" + cfg.ResourceName(prefix)
}

// createTasksQueue creates a fresh queue for a probe and returns it.
func createTasksQueue(ctx context.Context, client *cloudtasks.Client, cfg Config, prefix string) (*cloudtaskspb.Queue, error) {
	q, err := client.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
		Parent: tasksParent(cfg),
		Queue:  &cloudtaskspb.Queue{Name: tasksQueueName(cfg, prefix)},
	})
	if err != nil {
		return nil, fmt.Errorf("CreateQueue: %w", err)
	}
	return q, nil
}

// createTasksTask creates a fresh HTTP task under queue.
func createTasksTask(ctx context.Context, client *cloudtasks.Client, cfg Config, queue, prefix string) (*cloudtaskspb.Task, error) {
	task, err := client.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
		Parent: queue,
		Task: &cloudtaskspb.Task{MessageType: &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
			Url:        cfg.RESTEndpoint + "/_jaiscloud/health",
			HttpMethod: cloudtaskspb.HttpMethod_GET,
		}}},
	})
	if err != nil {
		return nil, fmt.Errorf("CreateTask: %w", err)
	}
	return task, nil
}

func checkTasksCreateQueue(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	q, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-create")
	if err != nil {
		return err
	}
	if q.GetName() != tasksQueueName(cfg, "gcpc-grpc-tasks-create") {
		return fmt.Errorf("CreateQueue name = %q", q.GetName())
	}
	if q.GetState() != cloudtaskspb.Queue_RUNNING {
		return fmt.Errorf("CreateQueue state = %v, want RUNNING", q.GetState())
	}
	if q.GetRateLimits().GetMaxDispatchesPerSecond() == 0 {
		return fmt.Errorf("CreateQueue default rateLimits missing")
	}
	return nil
}

func checkTasksGetQueue(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-get")
	if err != nil {
		return err
	}
	got, err := client.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("GetQueue: %w", err)
	}
	if got.GetName() != created.GetName() || got.GetState() != cloudtaskspb.Queue_RUNNING {
		return fmt.Errorf("GetQueue = %q/%v, want %q/RUNNING", got.GetName(), got.GetState(), created.GetName())
	}
	return nil
}

func checkTasksListQueues(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-list")
	if err != nil {
		return err
	}
	// AIP-158: a negative page_size is INVALID_ARGUMENT on both list methods,
	// rather than a silent fall-back to the documented maximum.
	if _, err := client.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: tasksParent(cfg), PageSize: -1}).Next(); status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("ListQueues(page_size=-1) code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := client.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{Parent: created.GetName(), PageSize: -1}).Next(); status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("ListTasks(page_size=-1) code = %v, want InvalidArgument", status.Code(err))
	}
	it := client.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: tasksParent(cfg)})
	for {
		q, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListQueues did not include %q", created.GetName())
		}
		if err != nil {
			return fmt.Errorf("ListQueues: %w", err)
		}
		if q.GetName() == created.GetName() {
			return nil
		}
	}
}

func checkTasksUpdateQueue(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-update")
	if err != nil {
		return err
	}
	updated, err := client.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
		Queue:      &cloudtaskspb.Queue{Name: created.GetName(), RateLimits: &cloudtaskspb.RateLimits{MaxDispatchesPerSecond: 37}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rate_limits"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateQueue: %w", err)
	}
	if updated.GetRateLimits().GetMaxDispatchesPerSecond() != 37 {
		return fmt.Errorf("UpdateQueue maxDispatchesPerSecond = %v, want 37", updated.GetRateLimits().GetMaxDispatchesPerSecond())
	}
	if updated.GetRetryConfig().GetMaxAttempts() != created.GetRetryConfig().GetMaxAttempts() {
		return fmt.Errorf("UpdateQueue clobbered retryConfig")
	}
	return nil
}

func checkTasksPauseQueue(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-pause")
	if err != nil {
		return err
	}
	paused, err := client.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("PauseQueue: %w", err)
	}
	if paused.GetState() != cloudtaskspb.Queue_PAUSED {
		return fmt.Errorf("PauseQueue state = %v, want PAUSED", paused.GetState())
	}
	return nil
}

func checkTasksResumeQueue(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-resume")
	if err != nil {
		return err
	}
	if _, err := client.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{Name: created.GetName()}); err != nil {
		return fmt.Errorf("PauseQueue: %w", err)
	}
	resumed, err := client.ResumeQueue(ctx, &cloudtaskspb.ResumeQueueRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("ResumeQueue: %w", err)
	}
	if resumed.GetState() != cloudtaskspb.Queue_RUNNING {
		return fmt.Errorf("ResumeQueue state = %v, want RUNNING", resumed.GetState())
	}
	return nil
}

func checkTasksPurgeQueue(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-purge")
	if err != nil {
		return err
	}
	if _, err := createTasksTask(ctx, client, cfg, created.GetName(), "gcpc-grpc-tasks-purge-task"); err != nil {
		return err
	}
	purged, err := client.PurgeQueue(ctx, &cloudtaskspb.PurgeQueueRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("PurgeQueue: %w", err)
	}
	if purged.GetPurgeTime() == nil {
		return fmt.Errorf("PurgeQueue did not set purgeTime")
	}
	it := client.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{Parent: created.GetName()})
	if _, err := it.Next(); err != iterator.Done {
		return fmt.Errorf("PurgeQueue left tasks (next err = %v)", err)
	}
	return nil
}

func checkTasksDeleteQueue(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-delete")
	if err != nil {
		return err
	}
	if err := client.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{Name: created.GetName()}); err != nil {
		return fmt.Errorf("DeleteQueue: %w", err)
	}
	if _, err := client.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetQueue after delete code = %v, want NotFound", status.Code(err))
	}
	return nil
}

func checkTasksGetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-getiam")
	if err != nil {
		return err
	}
	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: created.GetName()})
	if err != nil {
		return fmt.Errorf("GetIamPolicy: %w", err)
	}
	if len(pol.GetEtag()) == 0 {
		return fmt.Errorf("GetIamPolicy returned no etag")
	}
	return nil
}

func checkTasksSetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-setiam")
	if err != nil {
		return err
	}
	pol, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: created.GetName(),
		Policy: &iampb.Policy{Bindings: []*iampb.Binding{{
			Role:    "roles/owner",
			Members: []string{"user:conformance@example.test"},
		}}},
	})
	if err != nil {
		return fmt.Errorf("SetIamPolicy: %w", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != "roles/owner" {
		return fmt.Errorf("SetIamPolicy bindings = %+v", pol.GetBindings())
	}
	return nil
}

func checkTasksTestIamPermissions(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-testiam")
	if err != nil {
		return err
	}
	resp, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    created.GetName(),
		Permissions: []string{"cloudtasks.queues.get"},
	})
	if err != nil {
		return fmt.Errorf("TestIamPermissions: %w", err)
	}
	if len(resp.GetPermissions()) != 1 {
		return fmt.Errorf("TestIamPermissions = %v", resp.GetPermissions())
	}
	return nil
}

func checkTasksCreateTask(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-ctask")
	if err != nil {
		return err
	}
	task, err := createTasksTask(ctx, client, cfg, created.GetName(), "gcpc-grpc-tasks-ctask")
	if err != nil {
		return err
	}
	if task.GetName() == "" || task.GetScheduleTime() == nil || task.GetDispatchDeadline() == nil {
		return fmt.Errorf("CreateTask missing name/scheduleTime/dispatchDeadline: %+v", task)
	}
	if task.GetHttpRequest().GetUrl() != cfg.RESTEndpoint+"/_jaiscloud/health" {
		return fmt.Errorf("CreateTask httpRequest.url = %q", task.GetHttpRequest().GetUrl())
	}
	return nil
}

func checkTasksGetTask(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-gtask")
	if err != nil {
		return err
	}
	task, err := createTasksTask(ctx, client, cfg, created.GetName(), "gcpc-grpc-tasks-gtask")
	if err != nil {
		return err
	}
	got, err := client.GetTask(ctx, &cloudtaskspb.GetTaskRequest{Name: task.GetName()})
	if err != nil {
		return fmt.Errorf("GetTask: %w", err)
	}
	if got.GetName() != task.GetName() {
		return fmt.Errorf("GetTask name = %q, want %q", got.GetName(), task.GetName())
	}
	return nil
}

func checkTasksListTasks(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-ltask")
	if err != nil {
		return err
	}
	task, err := createTasksTask(ctx, client, cfg, created.GetName(), "gcpc-grpc-tasks-ltask")
	if err != nil {
		return err
	}
	it := client.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{Parent: created.GetName()})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListTasks did not include %q", task.GetName())
		}
		if err != nil {
			return fmt.Errorf("ListTasks: %w", err)
		}
		if got.GetName() == task.GetName() {
			return nil
		}
	}
}

func checkTasksDeleteTask(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-dtask")
	if err != nil {
		return err
	}
	task, err := createTasksTask(ctx, client, cfg, created.GetName(), "gcpc-grpc-tasks-dtask")
	if err != nil {
		return err
	}
	if err := client.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{Name: task.GetName()}); err != nil {
		return fmt.Errorf("DeleteTask: %w", err)
	}
	if _, err := client.GetTask(ctx, &cloudtaskspb.GetTaskRequest{Name: task.GetName()}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetTask after delete code = %v, want NotFound", status.Code(err))
	}
	return nil
}

// checkTasksRunTask forces a synchronous delivery. The task targets the
// emulator's own health endpoint, so the run succeeds and Cloud Tasks deletes
// the task; RunTask must return the dispatched task (not Unimplemented).
func checkTasksRunTask(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-run")
	if err != nil {
		return err
	}
	// Pause the queue so the background dispatch engine cannot deliver the task
	// between CreateTask and RunTask; RunTask must bypass PAUSED.
	if _, err := client.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{Name: created.GetName()}); err != nil {
		return fmt.Errorf("PauseQueue: %w", err)
	}
	task, err := createTasksTask(ctx, client, cfg, created.GetName(), "gcpc-grpc-tasks-run")
	if err != nil {
		return err
	}
	ran, err := client.RunTask(ctx, &cloudtaskspb.RunTaskRequest{Name: task.GetName()})
	if err != nil {
		return fmt.Errorf("RunTask: %w", err)
	}
	if ran.GetName() != task.GetName() {
		return fmt.Errorf("RunTask name = %q, want %q", ran.GetName(), task.GetName())
	}
	if ran.GetDispatchCount() == 0 {
		return fmt.Errorf("RunTask did not record a dispatch attempt")
	}
	if _, err := client.GetTask(ctx, &cloudtaskspb.GetTaskRequest{Name: task.GetName()}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetTask after successful RunTask code = %v, want NotFound", status.Code(err))
	}
	return nil
}

// checkTasksBatchCreate creates two tasks in one batch call and asserts the
// synchronous done operation carries the typed BatchCreateTasksResponse with
// both tasks.
func checkTasksBatchCreate(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	q, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-bcreate")
	if err != nil {
		return err
	}
	newTask := func(path string) *cloudtaskspb.CreateTaskRequest {
		return &cloudtaskspb.CreateTaskRequest{
			Parent: q.GetName(),
			Task: &cloudtaskspb.Task{MessageType: &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
				Url:        cfg.RESTEndpoint + "/_jaiscloud/health" + path,
				HttpMethod: cloudtaskspb.HttpMethod_GET,
			}}},
		}
	}
	op, err := client.BatchCreateTasks(ctx, &cloudtaskspb.BatchCreateTasksRequest{
		Parent:   q.GetName(),
		Requests: []*cloudtaskspb.CreateTaskRequest{newTask("/a"), newTask("/b")},
	})
	if err != nil {
		return fmt.Errorf("BatchCreateTasks: %w", err)
	}
	resp, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("BatchCreateTasks wait: %w", err)
	}
	if len(resp.GetTasks()) != 2 {
		return fmt.Errorf("BatchCreateTasks returned %d tasks, want 2", len(resp.GetTasks()))
	}
	for _, t := range resp.GetTasks() {
		if _, err := client.GetTask(ctx, &cloudtaskspb.GetTaskRequest{Name: t.GetName()}); err != nil {
			return fmt.Errorf("GetTask(%s): %w", t.GetName(), err)
		}
	}
	// A request parent that differs from the batch parent is INVALID_ARGUMENT.
	other := *newTask("/c")
	other.Parent = q.GetName() + "x"
	if _, err := client.BatchCreateTasks(ctx, &cloudtaskspb.BatchCreateTasksRequest{
		Parent: q.GetName(), Requests: []*cloudtaskspb.CreateTaskRequest{&other},
	}); status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("BatchCreateTasks(mismatched parent) code = %v, want InvalidArgument", status.Code(err))
	}
	return client.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{Name: q.GetName()})
}

// checkTasksBatchDelete deletes two tasks in one batch call and asserts the done
// operation plus the subsequent NOT_FOUND reads.
func checkTasksBatchDelete(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	q, err := createTasksQueue(ctx, client, cfg, "gcpc-grpc-tasks-bdelete")
	if err != nil {
		return err
	}
	names := make([]string, 0, 2)
	for _, suffix := range []string{"a", "b"} {
		t, err := createTasksTask(ctx, client, cfg, q.GetName(), "gcpc-grpc-tasks-bdelete-"+suffix)
		if err != nil {
			return err
		}
		names = append(names, t.GetName())
	}
	op, err := client.BatchDeleteTasks(ctx, &cloudtaskspb.BatchDeleteTasksRequest{
		Parent: q.GetName(),
		Names:  names,
	})
	if err != nil {
		return fmt.Errorf("BatchDeleteTasks: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("BatchDeleteTasks wait: %w", err)
	}
	for _, name := range names {
		if _, err := client.GetTask(ctx, &cloudtaskspb.GetTaskRequest{Name: name}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("GetTask(%s) after batch delete code = %v, want NotFound", name, status.Code(err))
		}
	}
	return client.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{Name: q.GetName()})
}

// tasksCmekName is the location-scoped CMEK config resource name; the emulator
// does not validate the location list.
func tasksCmekName(cfg Config) string { return tasksParent(cfg) + "/cmekConfig" }

// checkTasksGetCmekConfig reads the location's CMEK config. An unconfigured
// location returns the documented default (a config with an empty kmsKey), not
// NOT_FOUND.
func checkTasksGetCmekConfig(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	got, err := client.GetCmekConfig(ctx, &cloudtaskspb.GetCmekConfigRequest{Name: tasksCmekName(cfg)})
	if err != nil {
		return fmt.Errorf("GetCmekConfig: %w", err)
	}
	if got.GetName() != tasksCmekName(cfg) {
		return fmt.Errorf("GetCmekConfig name = %q, want %q", got.GetName(), tasksCmekName(cfg))
	}
	return nil
}

// checkTasksUpdateCmekConfig sets the location's CMEK key, reads it back and
// clears it again (an empty kmsKey turns CMEK off).
func checkTasksUpdateCmekConfig(ctx context.Context, cfg Config) error {
	client, err := newTasksClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	key := fmt.Sprintf("projects/%s/locations/global/keyRings/gcpc/cryptoKeys/tasks", cfg.Project)
	upd, err := client.UpdateCmekConfig(ctx, &cloudtaskspb.UpdateCmekConfigRequest{
		CmekConfig: &cloudtaskspb.CmekConfig{Name: tasksCmekName(cfg), KmsKey: key},
	})
	if err != nil {
		return fmt.Errorf("UpdateCmekConfig: %w", err)
	}
	if upd.GetKmsKey() != key || upd.GetName() != tasksCmekName(cfg) {
		return fmt.Errorf("UpdateCmekConfig = %+v, want kmsKey %q", upd, key)
	}
	got, err := client.GetCmekConfig(ctx, &cloudtaskspb.GetCmekConfigRequest{Name: tasksCmekName(cfg)})
	if err != nil {
		return fmt.Errorf("GetCmekConfig: %w", err)
	}
	if got.GetKmsKey() != key {
		return fmt.Errorf("GetCmekConfig kmsKey = %q, want %q", got.GetKmsKey(), key)
	}
	if _, err := client.UpdateCmekConfig(ctx, &cloudtaskspb.UpdateCmekConfigRequest{
		CmekConfig: &cloudtaskspb.CmekConfig{Name: tasksCmekName(cfg)},
	}); err != nil {
		return fmt.Errorf("UpdateCmekConfig(clear): %w", err)
	}
	if got, err := client.GetCmekConfig(ctx, &cloudtaskspb.GetCmekConfigRequest{Name: tasksCmekName(cfg)}); err != nil {
		return fmt.Errorf("GetCmekConfig after clear: %w", err)
	} else if got.GetKmsKey() != "" {
		return fmt.Errorf("GetCmekConfig after clear kmsKey = %q, want empty", got.GetKmsKey())
	}
	return nil
}
