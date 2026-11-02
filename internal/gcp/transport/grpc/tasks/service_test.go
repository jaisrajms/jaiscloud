package tasks

import (
	"context"
	"strings"
	"testing"
	"time"

	cloudtaskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	core "jaiscloud/internal/gcp/service/tasks"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	"jaiscloud/internal/store"
)

func newServer() *Service {
	c := core.NewService(tasksstore.NewMemoryStore(), store.NewMemoryResourceStore())
	return NewService(c, "p")
}

const parent = "projects/p/locations/l"

func TestGRPCQueueLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newServer()

	q, err := s.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
		Parent: parent,
		Queue:  &cloudtaskspb.Queue{Name: parent + "/queues/q1", RateLimits: &cloudtaskspb.RateLimits{MaxDispatchesPerSecond: 5}},
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if !strings.HasPrefix(q.GetName(), parent+"/queues/") || q.GetState() != cloudtaskspb.Queue_RUNNING {
		t.Fatalf("queue = %+v", q)
	}
	name := q.GetName()

	if _, err := s.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: name}); err != nil {
		t.Fatalf("GetQueue: %v", err)
	}
	list, err := s.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: parent})
	if err != nil || len(list.GetQueues()) != 1 {
		t.Fatalf("ListQueues = %+v, %v", list, err)
	}
	// The list filter is the core's (DC16): "=" is equality on the full name and
	// ":" is contains, on both transports; an unknown field is InvalidArgument.
	if got, err := s.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: parent, Filter: `name:"` + name + `"`}); err != nil || len(got.GetQueues()) != 1 {
		t.Fatalf("ListQueues(contains) = %+v, %v", got, err)
	}
	if got, err := s.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: parent, Filter: `name="q1"`}); err != nil || len(got.GetQueues()) != 0 {
		t.Fatalf("ListQueues(bare id equality) = %+v, %v", got, err)
	}
	if _, err := s.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: parent, Filter: `bogus="x"`}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListQueues(bad filter) code = %v", status.Code(err))
	}
	if _, err := s.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
		Queue:      &cloudtaskspb.Queue{Name: name, RateLimits: &cloudtaskspb.RateLimits{MaxDispatchesPerSecond: 9}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rate_limits"}},
	}); err != nil {
		t.Fatalf("UpdateQueue: %v", err)
	}
	if _, err := s.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{Name: name}); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}
	if _, err := s.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{Name: name}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("double pause code = %v", status.Code(err))
	}
	if _, err := s.ResumeQueue(ctx, &cloudtaskspb.ResumeQueueRequest{Name: name}); err != nil {
		t.Fatalf("ResumeQueue: %v", err)
	}
	if _, err := s.PurgeQueue(ctx, &cloudtaskspb.PurgeQueueRequest{Name: name}); err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}
	if _, err := s.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{Name: name}); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	if _, err := s.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: name}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetQueue after delete code = %v", status.Code(err))
	}
}

func TestGRPCTaskLifecycleAndRun(t *testing.T) {
	ctx := context.Background()
	s := newServer()
	q, err := s.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{Parent: parent, Queue: &cloudtaskspb.Queue{Name: parent + "/queues/q1"}})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	tk, err := s.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
		Parent: q.GetName(),
		Task: &cloudtaskspb.Task{MessageType: &cloudtaskspb.Task_HttpRequest{
			HttpRequest: &cloudtaskspb.HttpRequest{Url: "http://example.test/hook", HttpMethod: cloudtaskspb.HttpMethod_POST},
		}},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	name := tk.GetName()
	if _, err := s.GetTask(ctx, &cloudtaskspb.GetTaskRequest{Name: name}); err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	list, err := s.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{Parent: q.GetName()})
	if err != nil || len(list.GetTasks()) != 1 {
		t.Fatalf("ListTasks = %+v, %v", list, err)
	}
	if ran, err := s.RunTask(ctx, &cloudtaskspb.RunTaskRequest{Name: name}); err != nil || ran.GetName() != name {
		t.Fatalf("RunTask = %+v, %v", ran, err)
	}
	if _, err := s.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{Name: name}); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
}

func TestGRPCQueueIAM(t *testing.T) {
	ctx := context.Background()
	s := newServer()
	q, err := s.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{Parent: parent, Queue: &cloudtaskspb.Queue{Name: parent + "/queues/q1"}})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := s.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: q.GetName()}); err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	pol, err := s.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: q.GetName(),
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/owner", Members: []string{"user:a@b"}}}},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if len(pol.GetBindings()) != 1 {
		t.Fatalf("policy = %+v", pol)
	}
	resp, err := s.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: q.GetName(), Permissions: []string{"cloudtasks.queues.get"}})
	if err != nil || len(resp.GetPermissions()) != 1 {
		t.Fatalf("TestIamPermissions = %+v, %v", resp, err)
	}
}

// TestGRPCCmekConfig covers locations.getCmekConfig/updateCmekConfig over gRPC,
// the same core the REST transport uses.
func TestGRPCCmekConfig(t *testing.T) {
	ctx := context.Background()
	s := newServer()
	name := parent + "/cmekConfig"

	got, err := s.GetCmekConfig(ctx, &cloudtaskspb.GetCmekConfigRequest{Name: name})
	if err != nil {
		t.Fatalf("GetCmekConfig: %v", err)
	}
	if got.GetName() != name || got.GetKmsKey() != "" {
		t.Fatalf("GetCmekConfig(default) = %+v", got)
	}

	key := "projects/p/locations/global/keyRings/ring/cryptoKeys/key"
	upd, err := s.UpdateCmekConfig(ctx, &cloudtaskspb.UpdateCmekConfigRequest{
		CmekConfig: &cloudtaskspb.CmekConfig{Name: name, KmsKey: key},
	})
	if err != nil {
		t.Fatalf("UpdateCmekConfig: %v", err)
	}
	if upd.GetKmsKey() != key || upd.GetName() != name {
		t.Fatalf("UpdateCmekConfig = %+v", upd)
	}

	if _, err := s.UpdateCmekConfig(ctx, &cloudtaskspb.UpdateCmekConfigRequest{
		CmekConfig: &cloudtaskspb.CmekConfig{Name: name, KmsKey: "bad"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateCmekConfig(bad key) code = %v, want InvalidArgument", status.Code(err))
	}

	// Empty kmsKey clears the config.
	if _, err := s.UpdateCmekConfig(ctx, &cloudtaskspb.UpdateCmekConfigRequest{
		CmekConfig: &cloudtaskspb.CmekConfig{Name: name},
	}); err != nil {
		t.Fatalf("UpdateCmekConfig(clear): %v", err)
	}
	if got, err := s.GetCmekConfig(ctx, &cloudtaskspb.GetCmekConfigRequest{Name: name}); err != nil || got.GetKmsKey() != "" {
		t.Fatalf("GetCmekConfig after clear = %+v, %v", got, err)
	}

	if _, err := s.GetCmekConfig(ctx, &cloudtaskspb.GetCmekConfigRequest{Name: "projects/p/locations/l/queues/q1"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetCmekConfig(bad name) code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestGRPCTaskRetryConfigRoundTrip pins the task-level retryConfig on the gRPC
// wire: it is stored and echoed, and an absent config stays absent.
func TestGRPCTaskRetryConfigRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newServer()
	q, err := s.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{Parent: parent, Queue: &cloudtaskspb.Queue{Name: parent + "/queues/q1"}})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	tk, err := s.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
		Parent: q.GetName(),
		Task: &cloudtaskspb.Task{
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{Url: "http://example.test/hook", HttpMethod: cloudtaskspb.HttpMethod_POST},
			},
			RetryConfig: &cloudtaskspb.RetryConfig{
				MaxAttempts: 3,
				MinBackoff:  durationpb.New(2 * time.Second),
				MaxBackoff:  durationpb.New(time.Minute),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if rc := tk.GetRetryConfig(); rc.GetMaxAttempts() != 3 || rc.GetMinBackoff().AsDuration() != 2*time.Second || rc.GetMaxBackoff().AsDuration() != time.Minute {
		t.Fatalf("created task retryConfig = %+v", rc)
	}
	got, err := s.GetTask(ctx, &cloudtaskspb.GetTaskRequest{Name: tk.GetName()})
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if rc := got.GetRetryConfig(); rc.GetMaxAttempts() != 3 {
		t.Fatalf("GetTask retryConfig = %+v", rc)
	}

	// An absent task-level config means "inherit the queue", so it is omitted
	// rather than defaulted to the queue's values.
	plain, err := s.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
		Parent: q.GetName(),
		Task: &cloudtaskspb.Task{MessageType: &cloudtaskspb.Task_HttpRequest{
			HttpRequest: &cloudtaskspb.HttpRequest{Url: "http://example.test/plain", HttpMethod: cloudtaskspb.HttpMethod_POST},
		}},
	})
	if err != nil {
		t.Fatalf("CreateTask(plain): %v", err)
	}
	if plain.GetRetryConfig() != nil {
		t.Fatalf("plain task retryConfig = %+v, want nil (inherit the queue)", plain.GetRetryConfig())
	}

	// A task-level maxAttempts below -1 is INVALID_ARGUMENT.
	if _, err := s.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
		Parent: q.GetName(),
		Task: &cloudtaskspb.Task{
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{Url: "http://example.test/bad", HttpMethod: cloudtaskspb.HttpMethod_POST},
			},
			RetryConfig: &cloudtaskspb.RetryConfig{MaxAttempts: -5},
		},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateTask(maxAttempts=-5) code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestGRPCListNegativePageSize pins AIP-158 on the gRPC surface: a negative
// page_size is INVALID_ARGUMENT on both list methods, and queues.list accepts
// its documented 9800 ceiling.
func TestGRPCListNegativePageSize(t *testing.T) {
	ctx := context.Background()
	s := newServer()
	q, err := s.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{Parent: parent, Queue: &cloudtaskspb.Queue{Name: parent + "/queues/q1"}})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := s.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: parent, PageSize: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListQueues(page_size=-1) err = %v, want InvalidArgument", err)
	}
	if _, err := s.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{Parent: q.GetName(), PageSize: -1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListTasks(page_size=-1) err = %v, want InvalidArgument", err)
	}
	if _, err := s.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{Parent: parent, PageSize: core.MaxQueuesPageSize}); err != nil {
		t.Fatalf("ListQueues(page_size=9800): %v", err)
	}
}
