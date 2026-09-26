package managedkafka

import (
	"context"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

// protojsonOpts tolerates unknown fields so a REST-created resource body (which
// may carry fields the proto snapshot does not yet have) still transcodes.
var protojsonOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// clusterToProto renders a stored cluster as the proto Cluster.
//
// It cannot populate bootstrapAddress: the pinned
// cloud.google.com/go/managedkafka proto does not define
// Cluster.bootstrap_address, so a gRPC caller does not see the field the REST
// transport renders in core.ClusterJSON. That is an upstream-proto gap, not a
// state divergence — both transports share the same core.
func clusterToProto(c mkstore.Cluster, project string) *managedkafkapb.Cluster {
	out := &managedkafkapb.Cluster{}
	if len(c.Config) > 0 {
		_ = protojsonOpts.Unmarshal(c.Config, out)
	}
	out.Name = core.ClusterName(project, c.Location, c.Name)
	out.State = managedkafkapb.Cluster_ACTIVE
	out.Labels = c.Labels
	out.CreateTime = timestamppb.New(c.CreateTime)
	out.UpdateTime = timestamppb.New(c.UpdateTime)
	return out
}

// topicToProto renders a stored topic as the proto Topic.
func topicToProto(t mkstore.Topic, project string) *managedkafkapb.Topic {
	out := &managedkafkapb.Topic{}
	if len(t.Config) > 0 {
		_ = protojsonOpts.Unmarshal(t.Config, out)
	}
	out.Name = core.TopicName(project, t.Location, t.ClusterName, t.Name)
	out.PartitionCount = int32(t.PartitionCount)
	out.ReplicationFactor = int32(t.ReplicationFactor)
	return out
}

// aclEntryToProto renders a stored ACL entry as the proto AclEntry.
func aclEntryToProto(e mkstore.AclEntry) *managedkafkapb.AclEntry {
	return &managedkafkapb.AclEntry{
		Principal:      e.Principal,
		PermissionType: e.PermissionType,
		Operation:      e.Operation,
		Host:           e.Host,
	}
}

// aclToProto renders a stored ACL as the proto Acl.
func aclToProto(a mkstore.Acl, project string) *managedkafkapb.Acl {
	out := &managedkafkapb.Acl{
		Name:         core.AclName(project, a.Location, a.ClusterName, a.Name),
		Etag:         a.Etag,
		ResourceType: a.ResourceType,
		ResourceName: a.ResourceName,
		PatternType:  a.PatternType,
	}
	for _, e := range a.AclEntries {
		out.AclEntries = append(out.AclEntries, aclEntryToProto(e))
	}
	return out
}

// operationToProto renders a stored operation as the proto
// google.longrunning.Operation, packing the already-rendered metadata and the
// caller-supplied response message (a Cluster, or Empty for delete).
func operationToProto(op mkstore.Operation, project string, response proto.Message) (*longrunningpb.Operation, error) {
	metadata, err := anypb.New(&managedkafkapb.OperationMetadata{
		CreateTime: timestamppb.New(op.CreateTime),
		EndTime:    timestamppb.New(op.EndTime),
		Target:     op.Target,
		Verb:       op.Verb,
		ApiVersion: "v1",
	})
	if err != nil {
		return nil, err
	}
	out := &longrunningpb.Operation{
		Name:     core.OperationName(project, op.Location, op.ID),
		Metadata: metadata,
		Done:     true,
	}
	if response != nil {
		resp, err := anypb.New(response)
		if err != nil {
			return nil, err
		}
		out.Result = &longrunningpb.Operation_Response{Response: resp}
	}
	return out, nil
}

// emptyResponse builds the google.protobuf.Empty result a delete operation
// carries.
func emptyResponse() proto.Message { return &emptypb.Empty{} }

// clusterInputFromProto builds a core ClusterInput from a request Cluster.
func clusterInputFromProto(c *managedkafkapb.Cluster) core.ClusterInput {
	in := core.ClusterInput{Labels: c.GetLabels()}
	if c != nil {
		if data, err := protojson.Marshal(c); err == nil {
			in.Config = data
		}
	}
	return in
}

// topicInputFromProto builds a core TopicInput from a request Topic.
func topicInputFromProto(t *managedkafkapb.Topic) core.TopicInput {
	in := core.TopicInput{
		PartitionCount:    int(t.GetPartitionCount()),
		ReplicationFactor: int(t.GetReplicationFactor()),
	}
	if t != nil {
		if data, err := protojson.Marshal(t); err == nil {
			in.Config = data
		}
	}
	return in
}

// aclInputFromProto builds a core AclInput from a request Acl.
func aclInputFromProto(a *managedkafkapb.Acl) core.AclInput {
	in := core.AclInput{Etag: a.GetEtag()}
	for _, e := range a.GetAclEntries() {
		in.AclEntries = append(in.AclEntries, aclEntryInputFromProto(e))
	}
	return in
}

// aclEntryInputFromProto builds a core AclEntryInput from a request AclEntry.
func aclEntryInputFromProto(e *managedkafkapb.AclEntry) core.AclEntryInput {
	return core.AclEntryInput{
		Principal:      e.GetPrincipal(),
		PermissionType: e.GetPermissionType(),
		Operation:      e.GetOperation(),
		Host:           e.GetHost(),
	}
}

// projectFor resolves the owning project for a name/parent, falling back to the
// gRPC metadata routing header then the configured default.
func (s *Service) projectFor(ctx context.Context, name string) string {
	if p := core.ProjectFromName(name); p != "" {
		return p
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}
