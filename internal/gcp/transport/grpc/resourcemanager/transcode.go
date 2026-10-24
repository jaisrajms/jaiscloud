package resourcemanager

import (
	"context"
	"errors"
	"strings"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/resourcemanager"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
)

// projectFor resolves the owning project from a resource name/parent, falling
// back to the gRPC metadata routing header then the configured default. It
// returns ok=false only when the name is present but malformed.
func (s *Service) projectFor(ctx context.Context, name string) (string, bool) {
	if name == "" {
		return grpcutil.ProjectFromMetadata(ctx, s.defaultProj), true
	}
	return core.ParseProjectName(name)
}

// projectToProto renders the core project as the v3 Project. Zero lifecycle
// times are omitted rather than encoded as the Unix epoch.
func projectToProto(p core.Project) *resourcemanagerpb.Project {
	out := &resourcemanagerpb.Project{
		Name:        core.ProjectName(p.ProjectID),
		Parent:      p.Parent,
		ProjectId:   p.ProjectID,
		State:       stateToProto(p.State),
		DisplayName: p.DisplayName,
		Etag:        p.Etag,
		Labels:      p.Labels,
	}
	if !p.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(p.CreateTime)
	}
	if !p.UpdateTime.IsZero() {
		out.UpdateTime = timestamp.Proto(p.UpdateTime)
	}
	if !p.DeleteTime.IsZero() {
		out.DeleteTime = timestamp.Proto(p.DeleteTime)
	}
	return out
}

func stateToProto(state string) resourcemanagerpb.Project_State {
	switch state {
	case core.StateActive:
		return resourcemanagerpb.Project_ACTIVE
	case core.StateDeleteRequested:
		return resourcemanagerpb.Project_DELETE_REQUESTED
	}
	return resourcemanagerpb.Project_STATE_UNSPECIFIED
}

// policyFromProto converts a request Policy into the core's typed policy input.
func policyFromProto(p *iampb.Policy) core.PolicyInput {
	in := core.PolicyInput{Version: int(p.GetVersion())}
	if et := p.GetEtag(); len(et) > 0 {
		in.Etag = string(et)
	}
	for _, b := range p.GetBindings() {
		members := make([]any, 0, len(b.GetMembers()))
		for _, m := range b.GetMembers() {
			members = append(members, m)
		}
		in.Bindings = append(in.Bindings, map[string]any{"role": b.GetRole(), "members": members})
	}
	return in
}

// policyToProto renders a stored policy as the proto Policy.
func policyToProto(p policy.Policy) *iampb.Policy {
	out := &iampb.Policy{Version: int32(p.Version)}
	if p.Etag != "" {
		out.Etag = []byte(p.Etag)
	}
	for _, b := range p.Bindings {
		m, ok := b.(map[string]any)
		if !ok {
			continue
		}
		binding := &iampb.Binding{}
		binding.Role, _ = m["role"].(string)
		for _, v := range toStrings(m["members"]) {
			binding.Members = append(binding.Members, v)
		}
		out.Bindings = append(out.Bindings, binding)
	}
	return out
}

func toStrings(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// ─── long-running operations ─────────────────────────────────────────────────

// operationToProto renders a core operation as the v3 google.longrunning
// Operation: the verb-specific typed metadata plus, once done, the resulting
// Project as the Any response. An in-flight operation carries no result.
func operationToProto(op core.Operation) (*longrunningpb.Operation, error) {
	out := &longrunningpb.Operation{Name: op.Name, Done: op.Done}
	metadata, err := operationMetadataProto(op)
	if err != nil {
		return nil, err
	}
	out.Metadata = metadata
	if op.Done {
		resp, err := anypb.New(projectToProto(op.Project))
		if err != nil {
			return nil, err
		}
		out.Result = &longrunningpb.Operation_Response{Response: resp}
	}
	return out, nil
}

// operationMetadataProto packs the v3 typed metadata message for a project
// mutation verb. Update/Move/Delete/Undelete metadata are empty marker
// messages; Create carries the workflow timing/gettable/ready fields.
func operationMetadataProto(op core.Operation) (*anypb.Any, error) {
	switch op.Verb {
	case "create":
		md := &resourcemanagerpb.CreateProjectMetadata{Gettable: op.Done, Ready: op.Done}
		if !op.CreateTime.IsZero() {
			md.CreateTime = timestamp.Proto(op.CreateTime)
		}
		return anypb.New(md)
	case "update":
		return anypb.New(&resourcemanagerpb.UpdateProjectMetadata{})
	case "move":
		return anypb.New(&resourcemanagerpb.MoveProjectMetadata{})
	case "delete":
		return anypb.New(&resourcemanagerpb.DeleteProjectMetadata{})
	case "undelete":
		return anypb.New(&resourcemanagerpb.UndeleteProjectMetadata{})
	}
	return nil, nil
}

// updateMaskPaths returns the field-mask paths of an UpdateProjectRequest, or
// nil when no mask was supplied.
func updateMaskPaths(m *fieldmaskpb.FieldMask) []string {
	if m == nil {
		return nil
	}
	return append([]string(nil), m.GetPaths()...)
}

// isTopLevelOperationName reports whether name is a top-level operations/{id}
// resource name (the shape project mutations publish).
func isTopLevelOperationName(name string) bool {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	return len(parts) == 2 && parts[0] == "operations" && parts[1] != ""
}

// isNotFound reports whether err is the canonical NotFound provider error (as
// returned by GetOperation for an absent operation).
func isNotFound(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.Code == "NotFound"
}
