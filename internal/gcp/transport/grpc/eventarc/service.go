package eventarc

import (
	"context"

	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/eventarc"
)

// Service implements eventarcpb.EventarcServer over the shared core, and the
// google.iam.v1.IAMPolicy surface for triggers and channels (see iam.go).
type Service struct {
	eventarcpb.UnimplementedEventarcServer

	core        *core.Service
	defaultProj string
}

// NewService returns an Eventarc gRPC service wrapping the core. defaultProj is
// the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// projectFor resolves the owning project for a name/parent, falling back to the
// gRPC metadata routing header then the configured default.
func (s *Service) projectFor(ctx context.Context, name string) string {
	if p := core.ProjectFromName(name); p != "" {
		return p
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// ─── Triggers ─────────────────────────────────────────────────────────────────

func (s *Service) GetTrigger(ctx context.Context, req *eventarcpb.GetTriggerRequest) (*eventarcpb.Trigger, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	t, err := s.core.GetTrigger(ctx, project, rn.Location, rn.Trigger)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return triggerToProto(project, t), nil
}

func (s *Service) ListTriggers(ctx context.Context, req *eventarcpb.ListTriggersRequest) (*eventarcpb.ListTriggersResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListTriggers(ctx, project, rn.Location, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	out := &eventarcpb.ListTriggersResponse{NextPageToken: next}
	for _, t := range page {
		out.Triggers = append(out.Triggers, triggerToProto(project, t))
	}
	return out, nil
}

func (s *Service) CreateTrigger(ctx context.Context, req *eventarcpb.CreateTriggerRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	t, op, err := s.core.CreateTrigger(ctx, project, rn.Location, req.GetTriggerId(), protoConfig(req.GetTrigger()), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, triggerToProto(project, t))
}

func (s *Service) UpdateTrigger(ctx context.Context, req *eventarcpb.UpdateTriggerRequest) (*longrunningpb.Operation, error) {
	name := req.GetTrigger().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	t, op, err := s.core.UpdateTrigger(ctx, project, rn.Location, rn.Trigger,
		protoConfig(req.GetTrigger()), maskString(req.GetUpdateMask().GetPaths()), req.GetTrigger().GetEtag(), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, triggerToProto(project, t))
}

func (s *Service) DeleteTrigger(ctx context.Context, req *eventarcpb.DeleteTriggerRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	t, op, err := s.core.DeleteTrigger(ctx, project, rn.Location, rn.Trigger, req.GetEtag(), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, triggerToProto(project, t))
}

// ─── Channels ─────────────────────────────────────────────────────────────────

func (s *Service) GetChannel(ctx context.Context, req *eventarcpb.GetChannelRequest) (*eventarcpb.Channel, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	c, err := s.core.GetChannel(ctx, project, rn.Location, rn.Channel)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return channelToProto(project, c), nil
}

func (s *Service) ListChannels(ctx context.Context, req *eventarcpb.ListChannelsRequest) (*eventarcpb.ListChannelsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListChannels(ctx, project, rn.Location, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	out := &eventarcpb.ListChannelsResponse{NextPageToken: next}
	for _, c := range page {
		out.Channels = append(out.Channels, channelToProto(project, c))
	}
	return out, nil
}

func (s *Service) CreateChannel(ctx context.Context, req *eventarcpb.CreateChannelRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	c, op, err := s.core.CreateChannel(ctx, project, rn.Location, req.GetChannelId(), protoConfig(req.GetChannel()), req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, channelToProto(project, c))
}

func (s *Service) UpdateChannel(ctx context.Context, req *eventarcpb.UpdateChannelRequest) (*longrunningpb.Operation, error) {
	name := req.GetChannel().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	c, op, err := s.core.UpdateChannel(ctx, project, rn.Location, rn.Channel,
		protoConfig(req.GetChannel()), maskString(req.GetUpdateMask().GetPaths()), "", req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, channelToProto(project, c))
}

func (s *Service) DeleteChannel(ctx context.Context, req *eventarcpb.DeleteChannelRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	c, op, err := s.core.DeleteChannel(ctx, project, rn.Location, rn.Channel, "", req.GetValidateOnly())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return s.operationToProto(project, op, channelToProto(project, c))
}

// ─── Providers (read-only discovery) ──────────────────────────────────────────

func (s *Service) GetProvider(ctx context.Context, req *eventarcpb.GetProviderRequest) (*eventarcpb.Provider, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	d, err := s.core.GetProvider(ctx, project, rn.Location, rn.Provider)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return providerToProto(project, rn.Location, d), nil
}

func (s *Service) ListProviders(ctx context.Context, req *eventarcpb.ListProvidersRequest) (*eventarcpb.ListProvidersResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListProviders(ctx, project, rn.Location, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	out := &eventarcpb.ListProvidersResponse{NextPageToken: next}
	for _, d := range page {
		out.Providers = append(out.Providers, providerToProto(project, rn.Location, d))
	}
	return out, nil
}

// compile-time assertion that Service implements the generated server (trigger,
// channel and provider RPCs; the other RPCs fall through to the embedded
// UnimplementedEventarcServer).
var _ eventarcpb.EventarcServer = (*Service)(nil)
