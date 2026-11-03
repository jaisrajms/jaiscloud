package monitoring

import (
	"context"
	"encoding/json"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"

	core "jaiscloud/internal/gcp/service/monitoring"
	"jaiscloud/internal/model"
)

// UptimeCheckService: the declarative uptimeCheckConfigs surface, shared with
// the REST transport. The configuration document travels as protojson JSON
// because the core stores the Discovery-shaped document verbatim; the probe-
// derived ListUptimeCheckIps stays UNIMPLEMENTED, exactly as the REST
// /v3/uptimeCheckIps path stays a 501.

// uptimeCheckConfigJSON renders a proto configuration as the core's stored
// document. protojson's default (camelCase JSON names, unset fields omitted) is
// the shape the Discovery document binds.
func uptimeCheckConfigJSON(p *monitoringpb.UptimeCheckConfig) (json.RawMessage, error) {
	if p == nil {
		return nil, mapError(invalidArgument("uptime check config is required"))
	}
	data, err := protojson.Marshal(p)
	if err != nil {
		return nil, mapError(invalidArgument("invalid uptime check config: " + err.Error()))
	}
	return data, nil
}

// uptimeCheckConfigProto decodes a stored document into a proto message.
// Unknown fields are discarded: the Discovery document declares fields the
// generated proto of a given client version may not know yet (e.g. `disabled`),
// and a client must not see fields it cannot represent.
func uptimeCheckConfigProto(doc json.RawMessage) (*monitoringpb.UptimeCheckConfig, error) {
	out := &monitoringpb.UptimeCheckConfig{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(doc, out); err != nil {
		return nil, model.NewProviderError("Internal", "stored uptime check config is not a valid UptimeCheckConfig: "+err.Error(), 500)
	}
	return out, nil
}

func (s *Service) ListUptimeCheckConfigs(ctx context.Context, req *monitoringpb.ListUptimeCheckConfigsRequest) (*monitoringpb.ListUptimeCheckConfigsResponse, error) {
	project, ok := core.SplitUptimeCheckConfigParent(req.GetParent())
	if !ok {
		return nil, mapError(invalidArgument("invalid uptimeCheckConfigs parent: " + req.GetParent()))
	}
	page, next, err := s.core.ListUptimeCheckConfigs(ctx, project, req.GetFilter(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &monitoringpb.ListUptimeCheckConfigsResponse{NextPageToken: next}
	for _, c := range page {
		p, err := uptimeCheckConfigProto(c.Document)
		if err != nil {
			return nil, mapError(err)
		}
		out.UptimeCheckConfigs = append(out.UptimeCheckConfigs, p)
	}
	return out, nil
}

func (s *Service) GetUptimeCheckConfig(ctx context.Context, req *monitoringpb.GetUptimeCheckConfigRequest) (*monitoringpb.UptimeCheckConfig, error) {
	project, id, ok := core.SplitUptimeCheckConfigName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid uptime check config name: " + req.GetName()))
	}
	c, err := s.core.GetUptimeCheckConfig(ctx, project, id)
	if err != nil {
		return nil, mapError(err)
	}
	return uptimeCheckConfigProto(c.Document)
}

func (s *Service) CreateUptimeCheckConfig(ctx context.Context, req *monitoringpb.CreateUptimeCheckConfigRequest) (*monitoringpb.UptimeCheckConfig, error) {
	project, ok := core.SplitUptimeCheckConfigParent(req.GetParent())
	if !ok {
		return nil, mapError(invalidArgument("invalid uptimeCheckConfigs parent: " + req.GetParent()))
	}
	body, err := uptimeCheckConfigJSON(req.GetUptimeCheckConfig())
	if err != nil {
		return nil, err
	}
	c, err := s.core.CreateUptimeCheckConfig(ctx, project, body)
	if err != nil {
		return nil, mapError(err)
	}
	return uptimeCheckConfigProto(c.Document)
}

func (s *Service) UpdateUptimeCheckConfig(ctx context.Context, req *monitoringpb.UpdateUptimeCheckConfigRequest) (*monitoringpb.UptimeCheckConfig, error) {
	project, id, ok := core.SplitUptimeCheckConfigName(req.GetUptimeCheckConfig().GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid uptime check config name: " + req.GetUptimeCheckConfig().GetName()))
	}
	body, err := uptimeCheckConfigJSON(req.GetUptimeCheckConfig())
	if err != nil {
		return nil, err
	}
	c, err := s.core.UpdateUptimeCheckConfig(ctx, project, id, body, req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return uptimeCheckConfigProto(c.Document)
}

func (s *Service) DeleteUptimeCheckConfig(ctx context.Context, req *monitoringpb.DeleteUptimeCheckConfigRequest) (*emptypb.Empty, error) {
	project, id, ok := core.SplitUptimeCheckConfigName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid uptime check config name: " + req.GetName()))
	}
	if err := s.core.DeleteUptimeCheckConfig(ctx, project, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ListUptimeCheckIps stays loud. The static checker address list is the egress
// plane this emulator does not have: returning a fabricated list would let a
// caller build firewall rules around addresses that never probe anything.
func (s *Service) ListUptimeCheckIps(ctx context.Context, req *monitoringpb.ListUptimeCheckIpsRequest) (*monitoringpb.ListUptimeCheckIpsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "uptimeCheckIps is probe-derived and is not served by the emulator")
}
