package serviceusage

import (
	"context"
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	serviceusagepb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/serviceusage"
)

// parseName extracts the project and (optional) service id from a Service Usage
// resource name: projects/{project}/services/{service}, or the projects/{project}
// parent when only listing.
func parseName(name string) (project, service string) {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		switch p {
		case "projects":
			if i+1 < len(parts) {
				project = parts[i+1]
			}
		case "services":
			if i+1 < len(parts) {
				service = parts[i+1]
			}
		}
	}
	return project, service
}

// projectFor resolves the owning project: the name's project when present, else
// the gRPC metadata routing header, else the configured default.
func (s *Service) projectFor(ctx context.Context, project string) string {
	if project != "" {
		return project
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// stateToProto maps a core State to the proto enum.
func stateToProto(st core.State) serviceusagepb.State {
	switch st {
	case core.StateEnabled:
		return serviceusagepb.State_ENABLED
	case core.StateDisabled:
		return serviceusagepb.State_DISABLED
	default:
		return serviceusagepb.State_STATE_UNSPECIFIED
	}
}

// apiToProto renders a core API as the proto Service.
func apiToProto(a core.API) *serviceusagepb.Service {
	return &serviceusagepb.Service{
		Name:   a.Name,
		Parent: a.Parent,
		Config: &serviceusagepb.ServiceConfig{Name: a.ConfigName},
		State:  stateToProto(a.State),
	}
}

// operationToProto renders a core Operation as the proto
// google.longrunning.Operation, packing the metadata and the caller-supplied
// response message (EnableServiceResponse, DisableServiceResponse, or
// BatchEnableServicesResponse) as typed Any values.
func operationToProto(op core.Operation, response proto.Message) (*longrunningpb.Operation, error) {
	metadata, err := anypb.New(&serviceusagepb.OperationMetadata{ResourceNames: op.ResourceNames})
	if err != nil {
		return nil, err
	}
	out := &longrunningpb.Operation{Name: op.Name, Metadata: metadata, Done: op.Done}
	if response != nil {
		resp, err := anypb.New(response)
		if err != nil {
			return nil, err
		}
		out.Result = &longrunningpb.Operation_Response{Response: resp}
	}
	return out, nil
}

// operationResponseProto reconstructs the typed response for a polled operation
// from its persisted service snapshot. It returns nil while the operation is in
// flight, so operationToProto omits the response until the operation is done
// (real GCP omits an in-flight result).
func operationResponseProto(op core.Operation) proto.Message {
	if !op.Done {
		return nil
	}
	switch op.Verb {
	case "enable":
		if len(op.Services) == 1 {
			return &serviceusagepb.EnableServiceResponse{Service: apiToProto(op.Services[0])}
		}
	case "disable":
		if len(op.Services) == 1 {
			return &serviceusagepb.DisableServiceResponse{Service: apiToProto(op.Services[0])}
		}
	case "batchEnable":
		resp := &serviceusagepb.BatchEnableServicesResponse{}
		for _, a := range op.Services {
			resp.Services = append(resp.Services, apiToProto(a))
		}
		return resp
	}
	return nil
}
