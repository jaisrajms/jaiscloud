package eventarc

// gRPC surface for the Eventarc advanced control plane (message buses,
// enrollments, pipelines, Google API sources, channel connections and the
// Google channel config singleton). Each RPC is a thin adapter over the shared
// core, mirroring the trigger/channel methods: create/update/delete return a
// done operation with the typed resource packed inline; get/list return the
// resource(s) inline; the singleton's update returns the config directly.
//
// IAM for the new families is served through the shared google.iam.v1.IAMPolicy
// router (see iam.go) — Eventarc's own proto carries no IAM RPCs.

import (
	"context"

	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/eventarc"
)

// ─── Message buses ────────────────────────────────────────────────────────────

func (s *Service) GetMessageBus(ctx context.Context, req *eventarcpb.GetMessageBusRequest) (*eventarcpb.MessageBus, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, err := s.core.GetAdvanced(ctx, project, core.MessageBusKind, rn.Location, rn.MessageBus)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return advancedToProto(project, core.MessageBusKind, r).(*eventarcpb.MessageBus), nil
}

func (s *Service) ListMessageBuses(ctx context.Context, req *eventarcpb.ListMessageBusesRequest) (*eventarcpb.ListMessageBusesResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListAdvanced(ctx, project, core.MessageBusKind, rn.Location, req.GetFilter(), req.GetOrderBy(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	out := &eventarcpb.ListMessageBusesResponse{NextPageToken: next}
	for _, r := range page {
		out.MessageBuses = append(out.MessageBuses, advancedToProto(project, core.MessageBusKind, r).(*eventarcpb.MessageBus))
	}
	return out, nil
}

func (s *Service) CreateMessageBus(ctx context.Context, req *eventarcpb.CreateMessageBusRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	r, op, err := s.core.CreateAdvanced(ctx, project, core.MessageBusKind, rn.Location, req.GetMessageBusId(), protoConfig(req.GetMessageBus()), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.MessageBusKind, r))
}

func (s *Service) UpdateMessageBus(ctx context.Context, req *eventarcpb.UpdateMessageBusRequest) (*longrunningpb.Operation, error) {
	name := req.GetMessageBus().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	r, op, err := s.core.UpdateAdvanced(ctx, project, core.MessageBusKind, rn.Location, rn.MessageBus,
		protoConfig(req.GetMessageBus()), maskString(req.GetUpdateMask().GetPaths()), req.GetMessageBus().GetEtag(), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.MessageBusKind, r))
}

func (s *Service) DeleteMessageBus(ctx context.Context, req *eventarcpb.DeleteMessageBusRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, op, err := s.core.DeleteAdvanced(ctx, project, core.MessageBusKind, rn.Location, rn.MessageBus, req.GetEtag(), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.MessageBusKind, r))
}

func (s *Service) ListMessageBusEnrollments(ctx context.Context, req *eventarcpb.ListMessageBusEnrollmentsRequest) (*eventarcpb.ListMessageBusEnrollmentsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	names, next, err := s.core.ListMessageBusEnrollments(ctx, project, rn.Location, rn.MessageBus, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return &eventarcpb.ListMessageBusEnrollmentsResponse{Enrollments: names, NextPageToken: next}, nil
}

// ─── Enrollments ──────────────────────────────────────────────────────────────

func (s *Service) GetEnrollment(ctx context.Context, req *eventarcpb.GetEnrollmentRequest) (*eventarcpb.Enrollment, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, err := s.core.GetAdvanced(ctx, project, core.EnrollmentKind, rn.Location, rn.Enrollment)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return advancedToProto(project, core.EnrollmentKind, r).(*eventarcpb.Enrollment), nil
}

func (s *Service) ListEnrollments(ctx context.Context, req *eventarcpb.ListEnrollmentsRequest) (*eventarcpb.ListEnrollmentsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListAdvanced(ctx, project, core.EnrollmentKind, rn.Location, req.GetFilter(), req.GetOrderBy(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	out := &eventarcpb.ListEnrollmentsResponse{NextPageToken: next}
	for _, r := range page {
		out.Enrollments = append(out.Enrollments, advancedToProto(project, core.EnrollmentKind, r).(*eventarcpb.Enrollment))
	}
	return out, nil
}

func (s *Service) CreateEnrollment(ctx context.Context, req *eventarcpb.CreateEnrollmentRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	r, op, err := s.core.CreateAdvanced(ctx, project, core.EnrollmentKind, rn.Location, req.GetEnrollmentId(), protoConfig(req.GetEnrollment()), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.EnrollmentKind, r))
}

func (s *Service) UpdateEnrollment(ctx context.Context, req *eventarcpb.UpdateEnrollmentRequest) (*longrunningpb.Operation, error) {
	name := req.GetEnrollment().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	r, op, err := s.core.UpdateAdvanced(ctx, project, core.EnrollmentKind, rn.Location, rn.Enrollment,
		protoConfig(req.GetEnrollment()), maskString(req.GetUpdateMask().GetPaths()), req.GetEnrollment().GetEtag(), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.EnrollmentKind, r))
}

func (s *Service) DeleteEnrollment(ctx context.Context, req *eventarcpb.DeleteEnrollmentRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, op, err := s.core.DeleteAdvanced(ctx, project, core.EnrollmentKind, rn.Location, rn.Enrollment, req.GetEtag(), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.EnrollmentKind, r))
}

// ─── Pipelines ────────────────────────────────────────────────────────────────

func (s *Service) GetPipeline(ctx context.Context, req *eventarcpb.GetPipelineRequest) (*eventarcpb.Pipeline, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, err := s.core.GetAdvanced(ctx, project, core.PipelineKind, rn.Location, rn.Pipeline)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return advancedToProto(project, core.PipelineKind, r).(*eventarcpb.Pipeline), nil
}

func (s *Service) ListPipelines(ctx context.Context, req *eventarcpb.ListPipelinesRequest) (*eventarcpb.ListPipelinesResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListAdvanced(ctx, project, core.PipelineKind, rn.Location, req.GetFilter(), req.GetOrderBy(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	out := &eventarcpb.ListPipelinesResponse{NextPageToken: next}
	for _, r := range page {
		out.Pipelines = append(out.Pipelines, advancedToProto(project, core.PipelineKind, r).(*eventarcpb.Pipeline))
	}
	return out, nil
}

func (s *Service) CreatePipeline(ctx context.Context, req *eventarcpb.CreatePipelineRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	r, op, err := s.core.CreateAdvanced(ctx, project, core.PipelineKind, rn.Location, req.GetPipelineId(), protoConfig(req.GetPipeline()), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.PipelineKind, r))
}

func (s *Service) UpdatePipeline(ctx context.Context, req *eventarcpb.UpdatePipelineRequest) (*longrunningpb.Operation, error) {
	name := req.GetPipeline().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	r, op, err := s.core.UpdateAdvanced(ctx, project, core.PipelineKind, rn.Location, rn.Pipeline,
		protoConfig(req.GetPipeline()), maskString(req.GetUpdateMask().GetPaths()), req.GetPipeline().GetEtag(), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.PipelineKind, r))
}

func (s *Service) DeletePipeline(ctx context.Context, req *eventarcpb.DeletePipelineRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, op, err := s.core.DeleteAdvanced(ctx, project, core.PipelineKind, rn.Location, rn.Pipeline, req.GetEtag(), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.PipelineKind, r))
}

// ─── Google API sources ───────────────────────────────────────────────────────

func (s *Service) GetGoogleApiSource(ctx context.Context, req *eventarcpb.GetGoogleApiSourceRequest) (*eventarcpb.GoogleApiSource, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, err := s.core.GetAdvanced(ctx, project, core.GoogleApiSourceKind, rn.Location, rn.GoogleApiSource)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return advancedToProto(project, core.GoogleApiSourceKind, r).(*eventarcpb.GoogleApiSource), nil
}

func (s *Service) ListGoogleApiSources(ctx context.Context, req *eventarcpb.ListGoogleApiSourcesRequest) (*eventarcpb.ListGoogleApiSourcesResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListAdvanced(ctx, project, core.GoogleApiSourceKind, rn.Location, req.GetFilter(), req.GetOrderBy(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	out := &eventarcpb.ListGoogleApiSourcesResponse{NextPageToken: next}
	for _, r := range page {
		out.GoogleApiSources = append(out.GoogleApiSources, advancedToProto(project, core.GoogleApiSourceKind, r).(*eventarcpb.GoogleApiSource))
	}
	return out, nil
}

func (s *Service) CreateGoogleApiSource(ctx context.Context, req *eventarcpb.CreateGoogleApiSourceRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	r, op, err := s.core.CreateAdvanced(ctx, project, core.GoogleApiSourceKind, rn.Location, req.GetGoogleApiSourceId(), protoConfig(req.GetGoogleApiSource()), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.GoogleApiSourceKind, r))
}

func (s *Service) UpdateGoogleApiSource(ctx context.Context, req *eventarcpb.UpdateGoogleApiSourceRequest) (*longrunningpb.Operation, error) {
	name := req.GetGoogleApiSource().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	r, op, err := s.core.UpdateAdvanced(ctx, project, core.GoogleApiSourceKind, rn.Location, rn.GoogleApiSource,
		protoConfig(req.GetGoogleApiSource()), maskString(req.GetUpdateMask().GetPaths()), req.GetGoogleApiSource().GetEtag(), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.GoogleApiSourceKind, r))
}

func (s *Service) DeleteGoogleApiSource(ctx context.Context, req *eventarcpb.DeleteGoogleApiSourceRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, op, err := s.core.DeleteAdvanced(ctx, project, core.GoogleApiSourceKind, rn.Location, rn.GoogleApiSource, req.GetEtag(), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.GoogleApiSourceKind, r))
}

// ─── Channel connections ──────────────────────────────────────────────────────

func (s *Service) GetChannelConnection(ctx context.Context, req *eventarcpb.GetChannelConnectionRequest) (*eventarcpb.ChannelConnection, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, err := s.core.GetAdvanced(ctx, project, core.ChannelConnectionKind, rn.Location, rn.ChannelConnection)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return advancedToProto(project, core.ChannelConnectionKind, r).(*eventarcpb.ChannelConnection), nil
}

func (s *Service) ListChannelConnections(ctx context.Context, req *eventarcpb.ListChannelConnectionsRequest) (*eventarcpb.ListChannelConnectionsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	// channelConnections.list documents neither filter nor orderBy, so both are
	// passed empty (the request message carries no such fields anyway).
	page, next, err := s.core.ListAdvanced(ctx, project, core.ChannelConnectionKind, rn.Location, "", "", int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	out := &eventarcpb.ListChannelConnectionsResponse{NextPageToken: next}
	for _, r := range page {
		out.ChannelConnections = append(out.ChannelConnections, advancedToProto(project, core.ChannelConnectionKind, r).(*eventarcpb.ChannelConnection))
	}
	return out, nil
}

func (s *Service) CreateChannelConnection(ctx context.Context, req *eventarcpb.CreateChannelConnectionRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	r, op, err := s.core.CreateAdvanced(ctx, project, core.ChannelConnectionKind, rn.Location, req.GetChannelConnectionId(), protoConfig(req.GetChannelConnection()), false)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.ChannelConnectionKind, r))
}

func (s *Service) DeleteChannelConnection(ctx context.Context, req *eventarcpb.DeleteChannelConnectionRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, op, err := s.core.DeleteAdvanced(ctx, project, core.ChannelConnectionKind, rn.Location, rn.ChannelConnection, "", false)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, advancedToProto(project, core.ChannelConnectionKind, r))
}

// ─── Google channel config (per-location singleton) ───────────────────────────

func (s *Service) GetGoogleChannelConfig(ctx context.Context, req *eventarcpb.GetGoogleChannelConfigRequest) (*eventarcpb.GoogleChannelConfig, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	r, err := s.core.GetAdvanced(ctx, project, core.GoogleChannelConfigKind, rn.Location, "")
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return advancedToProto(project, core.GoogleChannelConfigKind, r).(*eventarcpb.GoogleChannelConfig), nil
}

func (s *Service) UpdateGoogleChannelConfig(ctx context.Context, req *eventarcpb.UpdateGoogleChannelConfigRequest) (*eventarcpb.GoogleChannelConfig, error) {
	name := req.GetGoogleChannelConfig().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	r, _, err := s.core.UpdateAdvanced(ctx, project, core.GoogleChannelConfigKind, rn.Location, "",
		protoConfig(req.GetGoogleChannelConfig()), maskString(req.GetUpdateMask().GetPaths()), "", false)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return advancedToProto(project, core.GoogleChannelConfigKind, r).(*eventarcpb.GoogleChannelConfig), nil
}
