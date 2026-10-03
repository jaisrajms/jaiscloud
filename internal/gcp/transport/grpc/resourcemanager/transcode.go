package resourcemanager

import (
	"context"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/resourcemanager"
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
		out.CreateTime = timestamppb.New(p.CreateTime)
	}
	if !p.UpdateTime.IsZero() {
		out.UpdateTime = timestamppb.New(p.UpdateTime)
	}
	if !p.DeleteTime.IsZero() {
		out.DeleteTime = timestamppb.New(p.DeleteTime)
	}
	return out
}

func stateToProto(state string) resourcemanagerpb.Project_State {
	switch state {
	case core.StateActive:
		return resourcemanagerpb.Project_ACTIVE
	case "DELETE_REQUESTED":
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
