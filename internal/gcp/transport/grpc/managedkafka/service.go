// Package managedkafka is the gRPC transport for Apache Kafka for BigQuery
// (Managed Kafka) v1 (google.cloud.managedkafka.v1.ManagedKafka). It is a thin
// proto adapter over the transport-neutral core in
// internal/gcp/service/managedkafka: it transcodes between the generated
// protobuf messages and the core's typed API, and maps core errors to gRPC
// status codes. It owns no business logic and no state beyond its default
// project.
//
// Cluster create/update/delete return a done google.longrunning.Operation with
// the response (a Cluster, or Empty for delete) packed inline, so the generated
// client's Wait observes it without polling. In the opt-in async mode the
// operation is returned in flight and the client's poll resolves it through
// ResolveOperation (registered with the shared google.longrunning.Operations
// service in main.go). The separate
// google.cloud.managedkafka.v1.ManagedKafkaConnect service is not registered.
package managedkafka

import (
	"context"
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	grpcoperations "jaiscloud/internal/gcp/grpc/operations"
	core "jaiscloud/internal/gcp/service/managedkafka"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements managedkafkapb.ManagedKafkaServer over the shared core.
type Service struct {
	managedkafkapb.UnimplementedManagedKafkaServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Managed Kafka gRPC service wrapping the core. defaultProj
// is the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// ─── Clusters ─────────────────────────────────────────────────────────────────

func (s *Service) ListClusters(ctx context.Context, req *managedkafkapb.ListClustersRequest) (*managedkafkapb.ListClustersResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListClusters(ctx, project, rn.Location, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &managedkafkapb.ListClustersResponse{NextPageToken: next}
	for _, c := range page {
		out.Clusters = append(out.Clusters, clusterToProto(c, project))
	}
	return out, nil
}

func (s *Service) GetCluster(ctx context.Context, req *managedkafkapb.GetClusterRequest) (*managedkafkapb.Cluster, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	c, err := s.core.GetCluster(ctx, project, rn.Location, rn.Cluster)
	if err != nil {
		return nil, mapError(err)
	}
	return clusterToProto(c, project), nil
}

func (s *Service) CreateCluster(ctx context.Context, req *managedkafkapb.CreateClusterRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	c, op, err := s.core.CreateCluster(ctx, project, rn.Location, req.GetClusterId(), clusterInputFromProto(req.GetCluster()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, clusterToProto(c, project))
}

func (s *Service) UpdateCluster(ctx context.Context, req *managedkafkapb.UpdateClusterRequest) (*longrunningpb.Operation, error) {
	name := req.GetCluster().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	c, op, err := s.core.UpdateCluster(ctx, project, rn.Location, rn.Cluster, clusterInputFromProto(req.GetCluster()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, clusterToProto(c, project))
}

func (s *Service) DeleteCluster(ctx context.Context, req *managedkafkapb.DeleteClusterRequest) (*longrunningpb.Operation, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	op, err := s.core.DeleteCluster(ctx, project, rn.Location, rn.Cluster)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op, project, emptyResponse())
}

// ─── Topics ───────────────────────────────────────────────────────────────────

func (s *Service) ListTopics(ctx context.Context, req *managedkafkapb.ListTopicsRequest) (*managedkafkapb.ListTopicsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListTopics(ctx, project, rn.Location, rn.Cluster, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &managedkafkapb.ListTopicsResponse{NextPageToken: next}
	for _, t := range page {
		out.Topics = append(out.Topics, topicToProto(t, project))
	}
	return out, nil
}

func (s *Service) GetTopic(ctx context.Context, req *managedkafkapb.GetTopicRequest) (*managedkafkapb.Topic, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	t, err := s.core.GetTopic(ctx, project, rn.Location, rn.Cluster, rn.Topic)
	if err != nil {
		return nil, mapError(err)
	}
	return topicToProto(t, project), nil
}

func (s *Service) CreateTopic(ctx context.Context, req *managedkafkapb.CreateTopicRequest) (*managedkafkapb.Topic, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	t, err := s.core.CreateTopic(ctx, project, rn.Location, rn.Cluster, req.GetTopicId(), topicInputFromProto(req.GetTopic()))
	if err != nil {
		return nil, mapError(err)
	}
	return topicToProto(t, project), nil
}

func (s *Service) UpdateTopic(ctx context.Context, req *managedkafkapb.UpdateTopicRequest) (*managedkafkapb.Topic, error) {
	name := req.GetTopic().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	t, err := s.core.UpdateTopic(ctx, project, rn.Location, rn.Cluster, rn.Topic, topicInputFromProto(req.GetTopic()))
	if err != nil {
		return nil, mapError(err)
	}
	return topicToProto(t, project), nil
}

func (s *Service) DeleteTopic(ctx context.Context, req *managedkafkapb.DeleteTopicRequest) (*emptypb.Empty, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	if err := s.core.DeleteTopic(ctx, project, rn.Location, rn.Cluster, rn.Topic); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── Consumer groups ──────────────────────────────────────────────────────────

func (s *Service) ListConsumerGroups(ctx context.Context, req *managedkafkapb.ListConsumerGroupsRequest) (*managedkafkapb.ListConsumerGroupsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	// The pinned proto has no view/filter; the documented default view is FULL,
	// so the core is asked for committed offsets.
	page, next, err := s.core.ListConsumerGroups(ctx, project, rn.Location, rn.Cluster, core.ConsumerGroupViewFull, "", int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &managedkafkapb.ListConsumerGroupsResponse{NextPageToken: next}
	for _, g := range page {
		out.ConsumerGroups = append(out.ConsumerGroups, consumerGroupToProto(g, project))
	}
	return out, nil
}

func (s *Service) GetConsumerGroup(ctx context.Context, req *managedkafkapb.GetConsumerGroupRequest) (*managedkafkapb.ConsumerGroup, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	g, err := s.core.GetConsumerGroup(ctx, project, rn.Location, rn.Cluster, rn.ConsumerGroup)
	if err != nil {
		return nil, mapError(err)
	}
	return consumerGroupToProto(g, project), nil
}

func (s *Service) UpdateConsumerGroup(ctx context.Context, req *managedkafkapb.UpdateConsumerGroupRequest) (*managedkafkapb.ConsumerGroup, error) {
	name := req.GetConsumerGroup().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	g, err := s.core.UpdateConsumerGroup(ctx, project, rn.Location, rn.Cluster, rn.ConsumerGroup,
		consumerGroupInputFromProto(req.GetConsumerGroup()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return consumerGroupToProto(g, project), nil
}

func (s *Service) DeleteConsumerGroup(ctx context.Context, req *managedkafkapb.DeleteConsumerGroupRequest) (*emptypb.Empty, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	if err := s.core.DeleteConsumerGroup(ctx, project, rn.Location, rn.Cluster, rn.ConsumerGroup); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── ACLs ─────────────────────────────────────────────────────────────────────

func (s *Service) ListAcls(ctx context.Context, req *managedkafkapb.ListAclsRequest) (*managedkafkapb.ListAclsResponse, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	page, next, err := s.core.ListAcls(ctx, project, rn.Location, rn.Cluster, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &managedkafkapb.ListAclsResponse{NextPageToken: next}
	for _, a := range page {
		out.Acls = append(out.Acls, aclToProto(a, project))
	}
	return out, nil
}

func (s *Service) GetAcl(ctx context.Context, req *managedkafkapb.GetAclRequest) (*managedkafkapb.Acl, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	a, err := s.core.GetAcl(ctx, project, rn.Location, rn.Cluster, rn.AclID)
	if err != nil {
		return nil, mapError(err)
	}
	return aclToProto(a, project), nil
}

func (s *Service) CreateAcl(ctx context.Context, req *managedkafkapb.CreateAclRequest) (*managedkafkapb.Acl, error) {
	project := s.projectFor(ctx, req.GetParent())
	rn := core.ParseName(req.GetParent())
	a, err := s.core.CreateAcl(ctx, project, rn.Location, rn.Cluster, req.GetAclId(), aclInputFromProto(req.GetAcl()))
	if err != nil {
		return nil, mapError(err)
	}
	return aclToProto(a, project), nil
}

func (s *Service) UpdateAcl(ctx context.Context, req *managedkafkapb.UpdateAclRequest) (*managedkafkapb.Acl, error) {
	name := req.GetAcl().GetName()
	project := s.projectFor(ctx, name)
	rn := core.ParseName(name)
	a, err := s.core.UpdateAcl(ctx, project, rn.Location, rn.Cluster, rn.AclID, aclInputFromProto(req.GetAcl()))
	if err != nil {
		return nil, mapError(err)
	}
	return aclToProto(a, project), nil
}

func (s *Service) DeleteAcl(ctx context.Context, req *managedkafkapb.DeleteAclRequest) (*emptypb.Empty, error) {
	project := s.projectFor(ctx, req.GetName())
	rn := core.ParseName(req.GetName())
	if err := s.core.DeleteAcl(ctx, project, rn.Location, rn.Cluster, rn.AclID); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) AddAclEntry(ctx context.Context, req *managedkafkapb.AddAclEntryRequest) (*managedkafkapb.AddAclEntryResponse, error) {
	project := s.projectFor(ctx, req.GetAcl())
	rn := core.ParseName(req.GetAcl())
	a, created, err := s.core.AddAclEntry(ctx, project, rn.Location, rn.Cluster, rn.AclID, aclEntryInputFromProto(req.GetAclEntry()))
	if err != nil {
		return nil, mapError(err)
	}
	return &managedkafkapb.AddAclEntryResponse{Acl: aclToProto(a, project), AclCreated: created}, nil
}

func (s *Service) RemoveAclEntry(ctx context.Context, req *managedkafkapb.RemoveAclEntryRequest) (*managedkafkapb.RemoveAclEntryResponse, error) {
	project := s.projectFor(ctx, req.GetAcl())
	rn := core.ParseName(req.GetAcl())
	a, deleted, err := s.core.RemoveAclEntry(ctx, project, rn.Location, rn.Cluster, rn.AclID, aclEntryInputFromProto(req.GetAclEntry()))
	if err != nil {
		return nil, mapError(err)
	}
	if deleted {
		return &managedkafkapb.RemoveAclEntryResponse{
			Result: &managedkafkapb.RemoveAclEntryResponse_AclDeleted{AclDeleted: true},
		}, nil
	}
	return &managedkafkapb.RemoveAclEntryResponse{
		Result: &managedkafkapb.RemoveAclEntryResponse_Acl{Acl: aclToProto(*a, project)},
	}, nil
}

// ResolveOperation implements the google.longrunning.Operations resolver for
// Managed Kafka. Names are location-scoped
// (projects/{p}/locations/{l}/operations/{id}); a name outside that shape — or
// an id the managedkafka store does not know, since a location-scoped name is
// shared with other services — is not ours (handled=false) so the generic
// terminal stub keeps serving it done=true.
func (s *Service) ResolveOperation(ctx context.Context, name string) (*longrunningpb.Operation, bool, error) {
	project, location, id, ok := parseOperationName(name)
	if !ok {
		return nil, false, nil
	}
	op, err := s.core.GetOperation(ctx, project, location, id)
	if err != nil {
		if core.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, true, mapError(err)
	}
	out, err := operationToProto(op, project, operationResponse(op))
	if err != nil {
		return nil, true, err
	}
	return out, true, nil
}

// parseOperationName parses a Managed Kafka location operation name
// projects/{project}/locations/{location}/operations/{id}.
func parseOperationName(name string) (project, location, id string, ok bool) {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "locations" || parts[4] != "operations" {
		return "", "", "", false
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

// ListOperations implements the shared google.longrunning.Operations
// ListRegistry surface for Managed Kafka. The parent must be a location parent;
// a location that holds no Managed Kafka operations is declined so the shared
// stub (or another service) answers when the request is not endpoint-scoped.
// When the request is addressed to the Managed Kafka endpoint (the shared
// service's SetEndpointResolvers) this page is authoritative, so an empty page
// is returned rather than falling through. The standard filter parameter is not
// modelled; a non-empty filter fails loud rather than silently returning
// unfiltered results.
func (s *Service) ListOperations(ctx context.Context, parent string, pageSize int32, pageToken, filter string) (*longrunningpb.ListOperationsResponse, bool, error) {
	_, location, ok := core.ParseParent(parent)
	if !ok {
		return nil, false, nil
	}
	project := s.projectFor(ctx, parent)
	page, next, err := s.core.ListOperations(ctx, project, location, int(pageSize), pageToken)
	if err != nil {
		return nil, true, mapError(err)
	}
	if len(page) == 0 {
		return nil, false, nil
	}
	if filter != "" {
		return nil, true, status.Error(codes.InvalidArgument, "operations filter is not supported")
	}
	out := &longrunningpb.ListOperationsResponse{NextPageToken: next}
	for _, op := range page {
		pb, err := operationToProto(op, project, operationResponse(op))
		if err != nil {
			return nil, true, err
		}
		out.Operations = append(out.Operations, pb)
	}
	return out, true, nil
}

// compile-time assertions that Service implements the generated server and the
// operations resolver/List surface.
var (
	_ managedkafkapb.ManagedKafkaServer = (*Service)(nil)
	_ grpcoperations.Resolver           = (*Service)(nil)
	_ grpcoperations.ListRegistry       = (*Service)(nil)
)
