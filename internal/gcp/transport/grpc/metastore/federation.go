package metastore

import (
	"context"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"

	core "jaiscloud/internal/gcp/service/metastore"
)

// This file implements the separate
// google.cloud.metastore.v1.DataprocMetastoreFederation gRPC service
// (ListFederations/GetFederation/CreateFederation/UpdateFederation/
// DeleteFederation). A federation is location-scoped
// (projects/{p}/locations/{l}/federations/{f}), not nested under a service.
// Create/Update/Delete return a done LRO with the typed Federation response, so
// the generated client's Wait observes it without polling.

func (s *Service) ListFederations(ctx context.Context, req *metastorepb.ListFederationsRequest) (*metastorepb.ListFederationsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListFederations(ctx, project, rn.Location, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &metastorepb.ListFederationsResponse{NextPageToken: next}
	for _, f := range page {
		out.Federations = append(out.Federations, federationToProto(f, project))
	}
	return out, nil
}

func (s *Service) GetFederation(ctx context.Context, req *metastorepb.GetFederationRequest) (*metastorepb.Federation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	f, err := s.core.GetFederation(ctx, project, rn.Location, rn.Federation)
	if err != nil {
		return nil, mapError(err)
	}
	return federationToProto(f, project), nil
}

func (s *Service) CreateFederation(ctx context.Context, req *metastorepb.CreateFederationRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	f, op, err := s.core.CreateFederation(ctx, project, rn.Location, req.GetFederationId(), protoConfig(req.GetFederation()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, federationToProto(f, project))
}

func (s *Service) UpdateFederation(ctx context.Context, req *metastorepb.UpdateFederationRequest) (*longrunningpb.Operation, error) {
	name := req.GetFederation().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	f, op, err := s.core.UpdateFederation(ctx, project, rn.Location, rn.Federation, protoConfig(req.GetFederation()), maskString(req.GetUpdateMask().GetPaths()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, federationToProto(f, project))
}

func (s *Service) DeleteFederation(ctx context.Context, req *metastorepb.DeleteFederationRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	op, err := s.core.DeleteFederation(ctx, project, rn.Location, rn.Federation)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, emptyResponse())
}

// compile-time assertion that Service implements the federation server.
var _ metastorepb.DataprocMetastoreFederationServer = (*Service)(nil)
