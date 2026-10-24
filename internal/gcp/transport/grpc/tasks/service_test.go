package tasks

import (
	"context"
	"strings"
	"testing"

	cloudtaskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
