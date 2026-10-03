// Package scheduler is the gRPC transport for Cloud Scheduler v1
// (google.cloud.scheduler.v1.CloudScheduler). It is a thin proto adapter over
// the transport-neutral core in internal/gcp/service/scheduler: it transcodes
// between the generated protobuf messages and the core's typed API, and maps
// core errors to gRPC status codes. It owns no business logic and no state
// beyond its default project.
package scheduler

import (
	"context"
	"strconv"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/protobuf/types/known/emptypb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/scheduler"
	"jaiscloud/internal/model"
)

// Service implements schedulerpb.CloudSchedulerServer over the shared core.
type Service struct {
	schedulerpb.UnimplementedCloudSchedulerServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Cloud Scheduler gRPC service wrapping the core.
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

func (s *Service) ListJobs(ctx context.Context, req *schedulerpb.ListJobsRequest) (*schedulerpb.ListJobsResponse, error) {
	project, location, ok := core.ParseParent(req.GetParent())
	if !ok {
		return nil, mapError(invalid("invalid parent"))
	}
	jobs, err := s.core.ListJobs(ctx, s.projectFor(ctx, project), location)
	if err != nil {
		return nil, mapError(err)
	}
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 || pageSize > 500 {
		pageSize = 500
	}
	offset := 0
	if t := req.GetPageToken(); t != "" {
		offset, _ = strconv.Atoi(t)
	}
	if offset < 0 {
		offset = 0
	}
	if offset > len(jobs) {
		offset = len(jobs)
	}
	end := offset + pageSize
	if end > len(jobs) {
		end = len(jobs)
	}
	out := &schedulerpb.ListJobsResponse{}
	for _, j := range jobs[offset:end] {
		out.Jobs = append(out.Jobs, jobToProto(j))
	}
	if end < len(jobs) {
		out.NextPageToken = strconv.Itoa(end)
	}
	return out, nil
}

func (s *Service) GetJob(ctx context.Context, req *schedulerpb.GetJobRequest) (*schedulerpb.Job, error) {
	project, location, name, ok := core.ParseJobName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid job name"))
	}
	j, err := s.core.GetJob(ctx, s.projectFor(ctx, project), location, name)
	if err != nil {
		return nil, mapError(err)
	}
	return jobToProto(j), nil
}

func (s *Service) CreateJob(ctx context.Context, req *schedulerpb.CreateJobRequest) (*schedulerpb.Job, error) {
	project, location, ok := core.ParseParent(req.GetParent())
	if !ok {
		return nil, mapError(invalid("invalid parent"))
	}
	j, err := s.core.CreateJob(ctx, s.projectFor(ctx, project), location, jobFromProto(req.GetJob()))
	if err != nil {
		return nil, mapError(err)
	}
	return jobToProto(j), nil
}

func (s *Service) UpdateJob(ctx context.Context, req *schedulerpb.UpdateJobRequest) (*schedulerpb.Job, error) {
	project, location, name, ok := core.ParseJobName(req.GetJob().GetName())
	if !ok {
		return nil, mapError(invalid("invalid job name"))
	}
	j, err := s.core.UpdateJob(ctx, s.projectFor(ctx, project), location, name, jobFromProto(req.GetJob()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return jobToProto(j), nil
}

func (s *Service) DeleteJob(ctx context.Context, req *schedulerpb.DeleteJobRequest) (*emptypb.Empty, error) {
	project, location, name, ok := core.ParseJobName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid job name"))
	}
	if err := s.core.DeleteJob(ctx, s.projectFor(ctx, project), location, name); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) PauseJob(ctx context.Context, req *schedulerpb.PauseJobRequest) (*schedulerpb.Job, error) {
	project, location, name, ok := core.ParseJobName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid job name"))
	}
	j, err := s.core.PauseJob(ctx, s.projectFor(ctx, project), location, name)
	if err != nil {
		return nil, mapError(err)
	}
	return jobToProto(j), nil
}

func (s *Service) ResumeJob(ctx context.Context, req *schedulerpb.ResumeJobRequest) (*schedulerpb.Job, error) {
	project, location, name, ok := core.ParseJobName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid job name"))
	}
	j, err := s.core.ResumeJob(ctx, s.projectFor(ctx, project), location, name)
	if err != nil {
		return nil, mapError(err)
	}
	return jobToProto(j), nil
}

func (s *Service) RunJob(ctx context.Context, req *schedulerpb.RunJobRequest) (*schedulerpb.Job, error) {
	project, location, name, ok := core.ParseJobName(req.GetName())
	if !ok {
		return nil, mapError(invalid("invalid job name"))
	}
	j, err := s.core.RunJob(ctx, s.projectFor(ctx, project), location, name)
	if err != nil {
		return nil, mapError(err)
	}
	return jobToProto(j), nil
}

// compile-time assertion.
var _ schedulerpb.CloudSchedulerServer = (*Service)(nil)
