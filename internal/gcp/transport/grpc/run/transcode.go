package run

import (
	"encoding/json"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	runpb "cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
)

// protojsonOpts tolerates unknown fields so a resource created over REST (whose
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

// serviceToProto renders a stored service as the proto Service via the shared
// Discovery renderer, so REST and gRPC cannot drift.
func serviceToProto(s runstore.Service) *runpb.Service {
	body := core.ServiceJSON(s)
	out := &runpb.Service{}
	if err := mapToProto(body, out); err != nil {
		return &runpb.Service{Name: stringField(body, "name")}
	}
	return out
}

// revisionToProto renders a stored revision as the proto Revision.
func revisionToProto(r runstore.Revision) *runpb.Revision {
	body := core.RevisionJSON(r)
	out := &runpb.Revision{}
	if err := mapToProto(body, out); err != nil {
		return &runpb.Revision{Name: stringField(body, "name")}
	}
	return out
}

// operationToProto renders a stored operation as the proto
// google.longrunning.Operation via the shared Discovery renderer. The response
// and metadata are typed Any protos (Service for service mutations, Revision
// for revision deletes), resolved from the global proto registry.
func operationToProto(op runstore.Operation) *longrunningpb.Operation {
	body := core.OperationJSON(op)
	out := &longrunningpb.Operation{}
	if err := mapToProto(body, out); err != nil {
		return &longrunningpb.Operation{Name: stringField(body, "name")}
	}
	return out
}

// policyToProto renders an internal policy.Policy as an iampb.Policy.
func policyToProto(p policy.Policy) *iampb.Policy {
	var out iampb.Policy
	if err := mapToProto(policy.ToMap(p), &out); err != nil {
		return &iampb.Policy{}
	}
	return &out
}

func stringField(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}
