// Package eventarc is the gRPC transport for the Eventarc v1 control plane
// (google.cloud.eventarc.v1.Eventarc). It is a thin proto adapter over the
// transport-neutral core in internal/gcp/service/eventarc: it transcodes
// between the generated protobuf messages and the core's typed API, and maps
// core errors to gRPC status codes. It owns no business logic and no state
// beyond its default project.
//
// Create/Update/Delete return a done google.longrunning.Operation with the
// typed response packed inline, so the generated client's Wait observes it
// without polling. Trigger/Channel IAM is served through the shared
// google.iam.v1.IAMPolicy service (registered by the caller via the IAM
// router), because Eventarc's own proto carries no IAM RPCs. Every other
// Eventarc RPC (ChannelConnection, GoogleChannelConfig, MessageBus, Enrollment,
// Pipeline, GoogleApiSource) is an explicit Unimplemented stub, and the
// emulator deliberately models no event-delivery engine.
package eventarc

import (
	"encoding/json"
	"strings"

	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "jaiscloud/internal/gcp/service/eventarc"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
)

// protojsonOpts tolerates unknown fields so a REST-created resource body (which
// may carry fields the proto snapshot does not yet have) still transcodes.
var protojsonOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// unmarshalProtoJSON decodes a rendered wire map into a proto message, ignoring
// an (impossible for these plain shapes) error so a malformed stored body
// degrades to the typed zero value.
func unmarshalProtoJSON(m map[string]any, out proto.Message) {
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	_ = protojsonOpts.Unmarshal(data, out)
}

// triggerToProto renders a stored trigger as the proto Trigger. It is driven by
// the core's TriggerJSON map (via protojson), so the REST and gRPC renders of
// the same stored record cannot drift.
func triggerToProto(project string, t eventarcstore.Trigger) *eventarcpb.Trigger {
	out := &eventarcpb.Trigger{}
	unmarshalProtoJSON(core.TriggerJSON(project, t), out)
	return out
}

// channelToProto renders a stored channel as the proto Channel.
func channelToProto(project string, c eventarcstore.Channel) *eventarcpb.Channel {
	out := &eventarcpb.Channel{}
	unmarshalProtoJSON(core.ChannelJSON(project, c), out)
	return out
}

// providerToProto renders a catalogued provider as the proto Provider.
func providerToProto(project, location string, d core.Provider) *eventarcpb.Provider {
	out := &eventarcpb.Provider{}
	unmarshalProtoJSON(core.ProviderJSON(project, location, d), out)
	return out
}

// protoConfig marshals a request message to the camelCase JSON body the core
// stores verbatim. A nil message yields a nil body.
func protoConfig(m proto.Message) json.RawMessage {
	if m == nil {
		return nil
	}
	data, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}
	return data
}

// maskString joins a proto field mask's paths into the comma-separated form the
// core's updateMask handling expects.
func maskString(paths []string) string { return strings.Join(paths, ",") }

// operationToProto renders the core's operation as the proto
// google.longrunning.Operation, packing the typed metadata and the
// caller-supplied response message (a Trigger/Channel, or Empty for delete).
func (s *Service) operationToProto(project string, op core.Operation, response proto.Message) (*longrunningpb.Operation, error) {
	metadata, err := anypb.New(&eventarcpb.OperationMetadata{
		CreateTime: timestamppb.New(op.CreateTime),
		EndTime:    timestamppb.New(op.CreateTime),
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
