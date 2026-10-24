package functions

import (
	"encoding/json"
	"time"

	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"jaiscloud/internal/clock"
	core "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/gcp/timestamp"
)

// protojsonOpts tolerates unknown fields so a REST-created resource body (whose
// Discovery shape may carry fields the proto snapshot does not yet have) still
// transcodes.
var protojsonOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// protojsonToMap renders a proto message to a Discovery-shaped map (camelCase
// field names), the inverse of mapToProto.
func protojsonToMap(m proto.Message) map[string]any {
	if m == nil {
		return nil
	}
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil
	}
	return out
}

// mapToProto renders a Discovery-shaped map into a proto message.
func mapToProto(m map[string]any, out proto.Message) error {
	if m == nil {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return protojsonOpts.Unmarshal(b, out)
}

// functionToProtoV1 renders a stored function as the v1 proto CloudFunction via
// the shared Discovery renderer, so REST and gRPC cannot drift.
func functionToProtoV1(project string, f functionsstore.Function) *functionspb.CloudFunction {
	out := &functionspb.CloudFunction{}
	if err := mapToProto(core.FunctionJSON(core.V1, project, f), out); err != nil {
		return &functionspb.CloudFunction{Name: core.FunctionJSON(core.V1, project, f)["name"].(string)}
	}
	return out
}

// functionToProtoV2 renders a stored function as the v2 proto Function.
func functionToProtoV2(project string, f functionsstore.Function) *apiv2functionspb.Function {
	out := &apiv2functionspb.Function{}
	if err := mapToProto(core.FunctionJSON(core.V2, project, f), out); err != nil {
		return &apiv2functionspb.Function{Name: core.FunctionJSON(core.V2, project, f)["name"].(string)}
	}
	return out
}

// runtimeToProto renders a core runtime as the v2 proto Runtime via the shared
// Discovery renderer (RuntimeJSON), so the REST and gRPC transports cannot
// drift on the field names/enums.
func runtimeToProto(rt core.Runtime) *apiv2functionspb.ListRuntimesResponse_Runtime {
	out := &apiv2functionspb.ListRuntimesResponse_Runtime{}
	if err := mapToProto(core.RuntimeJSON(rt), out); err != nil {
		return &apiv2functionspb.ListRuntimesResponse_Runtime{Name: rt.Name, DisplayName: rt.DisplayName}
	}
	return out
}

// operationToProtoV1 packs a v1 operation with typed Any metadata
// (OperationMetadataV1) and — only once it is done — a typed Any response
// (CloudFunction, or Empty for a delete). While an async operation is in flight
// the response and the metadata completion timestamp are omitted, matching real
// GCP; a done operation keeps the original output exactly.
func operationToProtoV1(project string, op core.Operation) (*longrunningpb.Operation, error) {
	meta := &functionspb.OperationMetadataV1{
		Target: op.Target,
		Type:   operationTypeV1(op.Verb),
	}
	if op.Done {
		meta.UpdateTime = timestamp.Proto(operationEndTime(op))
	}
	metaAny, err := anypb.New(meta)
	if err != nil {
		return nil, err
	}
	out := &longrunningpb.Operation{Name: core.OperationName(core.V1, project, op), Metadata: metaAny, Done: op.Done}
	if op.Done {
		respAny, err := operationResponseProtoV1(project, op)
		if err != nil {
			return nil, err
		}
		out.Result = &longrunningpb.Operation_Response{Response: respAny}
	}
	return out, nil
}

// operationToProtoV2 packs a v2 operation with typed Any metadata
// (OperationMetadata) and — only once it is done — a typed Any response
// (Function, or Empty for a delete). While an async operation is in flight the
// response and metadata.endTime are omitted, matching real GCP; a done operation
// keeps the original output exactly.
func operationToProtoV2(project string, op core.Operation) (*longrunningpb.Operation, error) {
	meta := &apiv2functionspb.OperationMetadata{
		Target:        op.Target,
		Verb:          op.Verb,
		ApiVersion:    "v2",
		CreateTime:    timestamp.Proto(operationStartTime(op)),
		OperationType: operationTypeV2(op.Verb),
	}
	if op.Done {
		meta.EndTime = timestamp.Proto(operationEndTime(op))
	}
	metaAny, err := anypb.New(meta)
	if err != nil {
		return nil, err
	}
	out := &longrunningpb.Operation{Name: core.OperationName(core.V2, project, op), Metadata: metaAny, Done: op.Done}
	if op.Done {
		respAny, err := operationResponseProtoV2(project, op)
		if err != nil {
			return nil, err
		}
		out.Result = &longrunningpb.Operation_Response{Response: respAny}
	}
	return out, nil
}

// operationStartTime and operationEndTime mirror the REST core renderer
// (service/functions operationMetadataMap): a zero CreateTime falls back to the
// business clock and a zero EndTime to the start, so both transports report the
// same operation instants.
func operationStartTime(op core.Operation) time.Time {
	if op.CreateTime.IsZero() {
		return clock.Now()
	}
	return op.CreateTime
}

func operationEndTime(op core.Operation) time.Time {
	if op.EndTime.IsZero() {
		return operationStartTime(op)
	}
	return op.EndTime
}

func operationResponseProtoV1(project string, op core.Operation) (*anypb.Any, error) {
	if op.Function != nil {
		return anypb.New(functionToProtoV1(project, *op.Function))
	}
	return anypb.New(&emptypb.Empty{})
}

func operationResponseProtoV2(project string, op core.Operation) (*anypb.Any, error) {
	if op.Function != nil {
		return anypb.New(functionToProtoV2(project, *op.Function))
	}
	return anypb.New(&emptypb.Empty{})
}

func operationTypeV1(verb string) functionspb.OperationType {
	switch verb {
	case "create":
		return functionspb.OperationType_CREATE_FUNCTION
	case "update":
		return functionspb.OperationType_UPDATE_FUNCTION
	case "delete":
		return functionspb.OperationType_DELETE_FUNCTION
	}
	return functionspb.OperationType_OPERATION_UNSPECIFIED
}

func operationTypeV2(verb string) apiv2functionspb.OperationType {
	switch verb {
	case "create":
		return apiv2functionspb.OperationType_CREATE_FUNCTION
	case "update":
		return apiv2functionspb.OperationType_UPDATE_FUNCTION
	case "delete":
		return apiv2functionspb.OperationType_DELETE_FUNCTION
	}
	return apiv2functionspb.OperationType_OPERATIONTYPE_UNSPECIFIED
}
