package tasks

import (
	"context"
	"encoding/json"
	"strconv"

	cloudtaskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/tasks"
	"jaiscloud/internal/model"
)

// Service implements cloudtaskspb.CloudTasksServer over the shared core.
type Service struct {
	cloudtaskspb.UnimplementedCloudTasksServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Cloud Tasks gRPC service wrapping the core.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func invalid(msg string) error { return model.NewProviderError("InvalidArgument", msg, 400) }

// projectFor resolves the owning project: the name's project when present, else
// the gRPC metadata routing header, else the configured default.
func (s *Service) projectFor(ctx context.Context, project string) string {
	if project != "" {
		return project
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

func (s *Service) ListQueues(ctx context.Context, req *cloudtaskspb.ListQueuesRequest) (*cloudtaskspb.ListQueuesResponse, error) {
	project, location, ok := core.ParseQueueParent(req.GetParent())
	if !ok {
		return nil, mapError(invalid("invalid parent"))
	}
	queues, err := s.core.ListQueues(ctx, s.projectFor(ctx, project), location, req.GetFilter())
	if err != nil {
		return nil, mapError(err)
	}
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	offset := pageOffset(req.GetPageToken(), len(queues))
	end := offset + pageSize
	if end > len(queues) {
		end = len(queues)
	}
	out := &cloudtaskspb.ListQueuesResponse{}
	for _, q := range queues[offset:end] {
		out.Queues = append(out.Queues, queueToProto(q))
	}
	if end < len(queues) {
		out.NextPageToken = strconv.Itoa(end)
	}
	return out, nil
}

func (s *Service) GetQueue(ctx context.Context, req *cloudtaskspb.GetQueueRequest) (*cloudtaskspb.Queue, error) {
	project, location, name, ok := core.ParseQueueName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid queue name"))
	}
	q, err := s.core.GetQueue(ctx, s.projectFor(ctx, project), location, name)
	if err != nil {
		return nil, mapError(err)
	}
	return queueToProto(q), nil
}

func (s *Service) CreateQueue(ctx context.Context, req *cloudtaskspb.CreateQueueRequest) (*cloudtaskspb.Queue, error) {
	project, location, ok := core.ParseQueueParent(req.GetParent())
	if !ok {
		return nil, mapError(invalid("invalid parent"))
	}
	q, err := s.core.CreateQueue(ctx, s.projectFor(ctx, project), location, queueFromProto(req.GetQueue()))
	if err != nil {
		return nil, mapError(err)
	}
	return queueToProto(q), nil
}

func (s *Service) UpdateQueue(ctx context.Context, req *cloudtaskspb.UpdateQueueRequest) (*cloudtaskspb.Queue, error) {
	project, location, name, ok := core.ParseQueueName(req.GetQueue().GetName())
	if !ok {
		return nil, mapError(invalid("invalid queue name"))
	}
	q, err := s.core.UpdateQueue(ctx, s.projectFor(ctx, project), location, name, queueFromProto(req.GetQueue()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return queueToProto(q), nil
}

func (s *Service) DeleteQueue(ctx context.Context, req *cloudtaskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	project, location, name, ok := core.ParseQueueName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid queue name"))
	}
	if err := s.core.DeleteQueue(ctx, s.projectFor(ctx, project), location, name); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) PurgeQueue(ctx context.Context, req *cloudtaskspb.PurgeQueueRequest) (*cloudtaskspb.Queue, error) {
	project, location, name, ok := core.ParseQueueName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid queue name"))
	}
	q, err := s.core.PurgeQueue(ctx, s.projectFor(ctx, project), location, name)
	if err != nil {
		return nil, mapError(err)
	}
	return queueToProto(q), nil
}

func (s *Service) PauseQueue(ctx context.Context, req *cloudtaskspb.PauseQueueRequest) (*cloudtaskspb.Queue, error) {
	project, location, name, ok := core.ParseQueueName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid queue name"))
	}
	q, err := s.core.PauseQueue(ctx, s.projectFor(ctx, project), location, name)
	if err != nil {
		return nil, mapError(err)
	}
	return queueToProto(q), nil
}

func (s *Service) ResumeQueue(ctx context.Context, req *cloudtaskspb.ResumeQueueRequest) (*cloudtaskspb.Queue, error) {
	project, location, name, ok := core.ParseQueueName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid queue name"))
	}
	q, err := s.core.ResumeQueue(ctx, s.projectFor(ctx, project), location, name)
	if err != nil {
		return nil, mapError(err)
	}
	return queueToProto(q), nil
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project, location, name, ok := core.ParseQueueName(req.GetResource())
	if !ok {
		return nil, mapError(invalid("invalid queue name"))
	}
	pol, err := s.core.QueueGetIamPolicy(ctx, s.projectFor(ctx, project), location, name)
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol)
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project, location, name, ok := core.ParseQueueName(req.GetResource())
	if !ok {
		return nil, mapError(invalid("invalid queue name"))
	}
	body, err := policyBody(req.GetPolicy())
	if err != nil {
		return nil, mapError(invalid("invalid policy"))
	}
	pol, err := s.core.QueueSetIamPolicy(ctx, s.projectFor(ctx, project), location, name, body)
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol)
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	project, location, name, ok := core.ParseQueueName(req.GetResource())
	if !ok {
		return nil, mapError(invalid("invalid queue name"))
	}
	granted, err := s.core.QueueTestIamPermissions(ctx, s.projectFor(ctx, project), location, name, req.GetPermissions())
	if err != nil {
		return nil, mapError(err)
	}
	return &iampb.TestIamPermissionsResponse{Permissions: granted}, nil
}

func (s *Service) ListTasks(ctx context.Context, req *cloudtaskspb.ListTasksRequest) (*cloudtaskspb.ListTasksResponse, error) {
	project, location, queue, ok := core.ParseTaskParent(req.GetParent())
	if !ok {
		return nil, mapError(invalid("invalid parent"))
	}
	items, err := s.core.ListTasks(ctx, s.projectFor(ctx, project), location, queue)
	if err != nil {
		return nil, mapError(err)
	}
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	offset := pageOffset(req.GetPageToken(), len(items))
	end := offset + pageSize
	if end > len(items) {
		end = len(items)
	}
	out := &cloudtaskspb.ListTasksResponse{}
	for _, t := range items[offset:end] {
		out.Tasks = append(out.Tasks, taskToProto(t))
	}
	if end < len(items) {
		out.NextPageToken = strconv.Itoa(end)
	}
	return out, nil
}

func (s *Service) GetTask(ctx context.Context, req *cloudtaskspb.GetTaskRequest) (*cloudtaskspb.Task, error) {
	project, location, queue, name, ok := core.ParseTaskName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid task name"))
	}
	t, err := s.core.GetTask(ctx, s.projectFor(ctx, project), location, queue, name)
	if err != nil {
		return nil, mapError(err)
	}
	return taskToProto(t), nil
}

func (s *Service) CreateTask(ctx context.Context, req *cloudtaskspb.CreateTaskRequest) (*cloudtaskspb.Task, error) {
	project, location, queue, ok := core.ParseTaskParent(req.GetParent())
	if !ok {
		return nil, mapError(invalid("invalid parent"))
	}
	t, err := s.core.CreateTask(ctx, s.projectFor(ctx, project), location, queue, taskFromProto(req.GetTask()))
	if err != nil {
		return nil, mapError(err)
	}
	return taskToProto(t), nil
}

func (s *Service) DeleteTask(ctx context.Context, req *cloudtaskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	project, location, queue, name, ok := core.ParseTaskName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid task name"))
	}
	if err := s.core.DeleteTask(ctx, s.projectFor(ctx, project), location, queue, name); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) RunTask(ctx context.Context, req *cloudtaskspb.RunTaskRequest) (*cloudtaskspb.Task, error) {
	project, location, queue, name, ok := core.ParseTaskName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid task name"))
	}
	t, err := s.core.RunTask(ctx, s.projectFor(ctx, project), location, queue, name)
	if err != nil {
		return nil, mapError(err)
	}
	return taskToProto(t), nil
}

// --- helpers ---

func pageOffset(token string, n int) int {
	offset := 0
	if token != "" {
		offset, _ = strconv.Atoi(token)
	}
	if offset < 0 {
		offset = 0
	}
	if offset > n {
		offset = n
	}
	return offset
}

// policyToProto converts a stored policy into an iampb.Policy.
func policyToProto(pol policy.Policy) (*iampb.Policy, error) {
	data, err := json.Marshal(policy.ToMap(pol))
	if err != nil {
		return nil, mapError(err)
	}
	out := &iampb.Policy{}
	if err := protojson.Unmarshal(data, out); err != nil {
		return nil, mapError(err)
	}
	return out, nil
}

// policyBody converts an iampb.Policy into the map policy.Set expects.
func policyBody(pb *iampb.Policy) (map[string]any, error) {
	if pb == nil {
		return map[string]any{}, nil
	}
	data, err := protojson.Marshal(pb)
	if err != nil {
		return nil, err
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, err
	}
	return body, nil
}

// compile-time assertion.
var _ cloudtaskspb.CloudTasksServer = (*Service)(nil)
