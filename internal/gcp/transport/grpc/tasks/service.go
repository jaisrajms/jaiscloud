package tasks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strconv"

	cloudtaskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/paging"
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
	pageSize, err := paging.Size(int(req.GetPageSize()), core.MaxQueuesPageSize)
	if err != nil {
		return nil, mapError(err)
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
	pageSize, err := paging.Size(int(req.GetPageSize()), core.MaxTasksPageSize)
	if err != nil {
		return nil, mapError(err)
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

// BatchCreateTasks creates several tasks under one queue and returns a done
// google.longrunning.Operation carrying the typed BatchCreateTasksResponse (the
// emulator performs the batch synchronously). The proto requires every request's
// parent to equal the batch parent.
func (s *Service) BatchCreateTasks(ctx context.Context, req *cloudtaskspb.BatchCreateTasksRequest) (*longrunningpb.Operation, error) {
	project, location, queue, ok := core.ParseTaskParent(req.GetParent())
	if !ok {
		return nil, mapError(invalid("invalid parent"))
	}
	if len(req.GetRequests()) == 0 {
		return nil, mapError(invalid("requests is required"))
	}
	resp := &cloudtaskspb.BatchCreateTasksResponse{}
	for _, r := range req.GetRequests() {
		rp, rl, rq, ok := core.ParseTaskParent(r.GetParent())
		if !ok || rp != project || rl != location || rq != queue {
			return nil, mapError(invalid("every request parent must equal the batch parent"))
		}
		t, err := s.core.CreateTask(ctx, s.projectFor(ctx, project), location, queue, taskFromProto(r.GetTask()))
		if err != nil {
			return nil, mapError(err)
		}
		resp.Tasks = append(resp.Tasks, taskToProto(t))
	}
	return batchOperation(project, location, &cloudtaskspb.BatchCreateTasksMetadata{}, resp)
}

// BatchDeleteTasks deletes several tasks and returns a done
// google.longrunning.Operation with a google.protobuf.Empty response, as the
// proto declares. It is non-atomic like the real method: the first failing name
// decides the error, names already deleted stay deleted.
func (s *Service) BatchDeleteTasks(ctx context.Context, req *cloudtaskspb.BatchDeleteTasksRequest) (*longrunningpb.Operation, error) {
	project, location, queue, ok := core.ParseTaskParent(req.GetParent())
	if !ok {
		return nil, mapError(invalid("invalid parent"))
	}
	project = s.projectFor(ctx, project)
	for _, name := range req.GetNames() {
		p, l, q, t, ok := core.ParseTaskName(name)
		if !ok {
			return nil, mapError(invalid("invalid task name: " + name))
		}
		if p != project || l != location || q != queue {
			return nil, mapError(invalid("every task name must be under the batch parent"))
		}
		if err := s.core.DeleteTask(ctx, project, location, queue, t); err != nil {
			return nil, mapError(err)
		}
	}
	return batchOperation(project, location, &cloudtaskspb.BatchDeleteTasksMetadata{}, &emptypb.Empty{})
}

func (s *Service) GetCmekConfig(ctx context.Context, req *cloudtaskspb.GetCmekConfigRequest) (*cloudtaskspb.CmekConfig, error) {
	project, location, ok := core.ParseCmekConfigName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid cmekConfig name"))
	}
	cfg, err := s.core.GetCmekConfig(ctx, s.projectFor(ctx, project), location)
	if err != nil {
		return nil, mapError(err)
	}
	return cmekToProto(cfg), nil
}

func (s *Service) UpdateCmekConfig(ctx context.Context, req *cloudtaskspb.UpdateCmekConfigRequest) (*cloudtaskspb.CmekConfig, error) {
	project, location, ok := core.ParseCmekConfigName(req.GetCmekConfig().GetName())
	if !ok {
		return nil, mapError(invalid("invalid cmekConfig name"))
	}
	cfg, err := s.core.UpdateCmekConfig(ctx, s.projectFor(ctx, project), location, req.GetCmekConfig().GetKmsKey())
	if err != nil {
		return nil, mapError(err)
	}
	return cmekToProto(cfg), nil
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

// batchOperation renders a completed google.longrunning.Operation for a
// synchronous Cloud Tasks batch mutation: done=true, a location-scoped name, the
// packed metadata and the packed typed response.
func batchOperation(project, location string, metadata, response proto.Message) (*longrunningpb.Operation, error) {
	md, err := anypb.New(metadata)
	if err != nil {
		return nil, mapError(err)
	}
	resp, err := anypb.New(response)
	if err != nil {
		return nil, mapError(err)
	}
	return &longrunningpb.Operation{
		Name:     core.OperationName(project, location, taskOperationID()),
		Metadata: md,
		Done:     true,
		Result:   &longrunningpb.Operation_Response{Response: resp},
	}, nil
}

// taskOperationID returns an ephemeral operation id for a synchronously
// completed batch operation.
func taskOperationID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
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
