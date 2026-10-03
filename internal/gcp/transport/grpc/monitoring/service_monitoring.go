// This file is the gRPC transport for the Service Monitoring API
// (google.monitoring.v3.ServiceMonitoringService): Services and
// ServiceLevelObjectives. Like the rest of the package it is a thin proto
// adapter over the shared core in internal/gcp/service/monitoring; the
// identifier/telemetry/indicator oneofs are stored as opaque canonical JSON.
package monitoring

import (
	"context"
	"encoding/json"
	"strings"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	calendarperiod "google.golang.org/genproto/googleapis/type/calendarperiod"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

// ─── transcoding ──────────────────────────────────────────────────────────────

// serviceFromProto converts the identifier and telemetry oneofs to opaque
// canonical JSON (each fragment is a Service JSON object carrying exactly one
// field, so it round-trips through protojson).
func serviceFromProto(p *monitoringpb.Service) monitoringstore.Service {
	if p == nil {
		return monitoringstore.Service{}
	}
	svc := monitoringstore.Service{
		DisplayName: p.GetDisplayName(),
		UserLabels:  p.GetUserLabels(),
	}
	if p.GetIdentifier() != nil {
		svc.Identifier = marshalProtoJSON(&monitoringpb.Service{Identifier: p.GetIdentifier()})
	}
	if p.GetBasicService() != nil {
		svc.BasicService = marshalProtoJSON(&monitoringpb.Service{BasicService: p.GetBasicService()})
	}
	if p.GetTelemetry() != nil {
		svc.Telemetry = marshalProtoJSON(&monitoringpb.Service{Telemetry: p.GetTelemetry()})
	}
	return svc
}

func serviceToProto(svc monitoringstore.Service, project string) *monitoringpb.Service {
	out := &monitoringpb.Service{
		Name:        core.ServiceName(project, svc.ID),
		DisplayName: svc.DisplayName,
		UserLabels:  svc.UserLabels,
	}
	if !jsonEmpty(svc.Identifier) {
		tmp := &monitoringpb.Service{}
		if protojson.Unmarshal(svc.Identifier, tmp) == nil {
			out.Identifier = tmp.GetIdentifier()
		}
	}
	if !jsonEmpty(svc.BasicService) {
		tmp := &monitoringpb.Service{}
		if protojson.Unmarshal(svc.BasicService, tmp) == nil {
			out.BasicService = tmp.GetBasicService()
		}
	}
	if !jsonEmpty(svc.Telemetry) {
		tmp := &monitoringpb.Service{}
		if protojson.Unmarshal(svc.Telemetry, tmp) == nil {
			out.Telemetry = tmp.GetTelemetry()
		}
	}
	return out
}

func serviceLevelObjectiveFromProto(p *monitoringpb.ServiceLevelObjective) monitoringstore.ServiceLevelObjective {
	if p == nil {
		return monitoringstore.ServiceLevelObjective{}
	}
	slo := monitoringstore.ServiceLevelObjective{
		DisplayName: p.GetDisplayName(),
		Goal:        p.GetGoal(),
		UserLabels:  p.GetUserLabels(),
	}
	if p.GetServiceLevelIndicator() != nil {
		slo.ServiceLevelIndicator = marshalProtoJSON(p.GetServiceLevelIndicator())
	}
	switch per := p.GetPeriod().(type) {
	case *monitoringpb.ServiceLevelObjective_RollingPeriod:
		slo.RollingPeriod = per.RollingPeriod.AsDuration()
	case *monitoringpb.ServiceLevelObjective_CalendarPeriod:
		slo.CalendarPeriod = int32(per.CalendarPeriod)
	}
	return slo
}

func serviceLevelObjectiveToProto(slo monitoringstore.ServiceLevelObjective, project string) *monitoringpb.ServiceLevelObjective {
	out := &monitoringpb.ServiceLevelObjective{
		Name:        core.ServiceLevelObjectiveName(project, slo.ServiceID, slo.ID),
		DisplayName: slo.DisplayName,
		Goal:        slo.Goal,
		UserLabels:  slo.UserLabels,
	}
	if !jsonEmpty(slo.ServiceLevelIndicator) {
		sli := &monitoringpb.ServiceLevelIndicator{}
		if protojson.Unmarshal(slo.ServiceLevelIndicator, sli) == nil {
			out.ServiceLevelIndicator = sli
		}
	}
	switch {
	case slo.RollingPeriod != 0:
		out.Period = &monitoringpb.ServiceLevelObjective_RollingPeriod{
			RollingPeriod: durationpb.New(slo.RollingPeriod),
		}
	case slo.CalendarPeriod != 0:
		out.Period = &monitoringpb.ServiceLevelObjective_CalendarPeriod{
			CalendarPeriod: calendarperiod.CalendarPeriod(slo.CalendarPeriod),
		}
	}
	return out
}

// marshalProtoJSON marshals a proto message to canonical JSON (proto field
// names), returning nil for a message with no set fields.
func marshalProtoJSON(m proto.Message) json.RawMessage {
	b, err := protojson.Marshal(m)
	if err != nil || string(b) == "{}" {
		return nil
	}
	return json.RawMessage(b)
}

// jsonEmpty reports whether an opaque JSON payload is absent or an empty object.
func jsonEmpty(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s == "" || s == "null" || s == "{}"
}

// parentProject extracts the project from a "projects/{p}" parent name.
func parentProject(parent string) (string, bool) {
	p := core.ProjectFromResourceName(parent)
	return p, p != ""
}

// ─── ServiceMonitoringService: services ───────────────────────────────────────

func (s *Service) CreateService(ctx context.Context, req *monitoringpb.CreateServiceRequest) (*monitoringpb.Service, error) {
	project, ok := parentProject(req.GetParent())
	if !ok {
		return nil, mapError(invalidArgument("invalid parent: " + req.GetParent()))
	}
	svc, err := s.core.CreateService(ctx, project, req.GetServiceId(), serviceFromProto(req.GetService()))
	if err != nil {
		return nil, mapError(err)
	}
	return serviceToProto(svc, project), nil
}

func (s *Service) GetService(ctx context.Context, req *monitoringpb.GetServiceRequest) (*monitoringpb.Service, error) {
	project, id, ok := core.SplitServiceName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid service name: " + req.GetName()))
	}
	svc, err := s.core.GetService(ctx, project, id)
	if err != nil {
		return nil, mapError(err)
	}
	return serviceToProto(svc, project), nil
}

func (s *Service) ListServices(ctx context.Context, req *monitoringpb.ListServicesRequest) (*monitoringpb.ListServicesResponse, error) {
	project, ok := parentProject(req.GetParent())
	if !ok {
		return nil, mapError(invalidArgument("invalid parent: " + req.GetParent()))
	}
	page, _, next, err := s.core.ListServices(ctx, project, req.GetFilter(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*monitoringpb.Service, 0, len(page))
	for _, svc := range page {
		out = append(out, serviceToProto(svc, project))
	}
	return &monitoringpb.ListServicesResponse{
		Services:      out,
		NextPageToken: next,
	}, nil
}

func (s *Service) UpdateService(ctx context.Context, req *monitoringpb.UpdateServiceRequest) (*monitoringpb.Service, error) {
	project, id, ok := core.SplitServiceName(req.GetService().GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid service name: " + req.GetService().GetName()))
	}
	svc, err := s.core.UpdateService(ctx, project, id, serviceFromProto(req.GetService()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return serviceToProto(svc, project), nil
}

func (s *Service) DeleteService(ctx context.Context, req *monitoringpb.DeleteServiceRequest) (*emptypb.Empty, error) {
	project, id, ok := core.SplitServiceName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid service name: " + req.GetName()))
	}
	if err := s.core.DeleteService(ctx, project, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── ServiceMonitoringService: service level objectives ───────────────────────

func (s *Service) CreateServiceLevelObjective(ctx context.Context, req *monitoringpb.CreateServiceLevelObjectiveRequest) (*monitoringpb.ServiceLevelObjective, error) {
	project, serviceID, ok := core.SplitServiceName(req.GetParent())
	if !ok {
		return nil, mapError(invalidArgument("invalid parent: " + req.GetParent()))
	}
	slo, err := s.core.CreateServiceLevelObjective(ctx, project, serviceID, req.GetServiceLevelObjectiveId(), serviceLevelObjectiveFromProto(req.GetServiceLevelObjective()))
	if err != nil {
		return nil, mapError(err)
	}
	return serviceLevelObjectiveToProto(slo, project), nil
}

func (s *Service) GetServiceLevelObjective(ctx context.Context, req *monitoringpb.GetServiceLevelObjectiveRequest) (*monitoringpb.ServiceLevelObjective, error) {
	project, serviceID, id, ok := core.SplitServiceLevelObjectiveName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid service level objective name: " + req.GetName()))
	}
	slo, err := s.core.GetServiceLevelObjective(ctx, project, serviceID, id)
	if err != nil {
		return nil, mapError(err)
	}
	return serviceLevelObjectiveToProto(slo, project), nil
}

func (s *Service) ListServiceLevelObjectives(ctx context.Context, req *monitoringpb.ListServiceLevelObjectivesRequest) (*monitoringpb.ListServiceLevelObjectivesResponse, error) {
	project, serviceID, ok := core.SplitServiceName(req.GetParent())
	if !ok {
		return nil, mapError(invalidArgument("invalid parent: " + req.GetParent()))
	}
	page, _, next, err := s.core.ListServiceLevelObjectives(ctx, project, serviceID, req.GetFilter(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*monitoringpb.ServiceLevelObjective, 0, len(page))
	for _, slo := range page {
		out = append(out, serviceLevelObjectiveToProto(slo, project))
	}
	return &monitoringpb.ListServiceLevelObjectivesResponse{
		ServiceLevelObjectives: out,
		NextPageToken:          next,
	}, nil
}

func (s *Service) UpdateServiceLevelObjective(ctx context.Context, req *monitoringpb.UpdateServiceLevelObjectiveRequest) (*monitoringpb.ServiceLevelObjective, error) {
	project, serviceID, id, ok := core.SplitServiceLevelObjectiveName(req.GetServiceLevelObjective().GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid service level objective name: " + req.GetServiceLevelObjective().GetName()))
	}
	slo, err := s.core.UpdateServiceLevelObjective(ctx, project, serviceID, id, serviceLevelObjectiveFromProto(req.GetServiceLevelObjective()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return serviceLevelObjectiveToProto(slo, project), nil
}

func (s *Service) DeleteServiceLevelObjective(ctx context.Context, req *monitoringpb.DeleteServiceLevelObjectiveRequest) (*emptypb.Empty, error) {
	project, serviceID, id, ok := core.SplitServiceLevelObjectiveName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid service level objective name: " + req.GetName()))
	}
	if err := s.core.DeleteServiceLevelObjective(ctx, project, serviceID, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}
