package eventarc

import (
	"context"

	iampb "cloud.google.com/go/iam/apiv1/iampb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/eventarc"
)

// Owns reports whether the Eventarc service handles IAM for this resource name
// (a trigger or a channel). It lets the shared google.iam.v1.IAMPolicy router
// dispatch Eventarc IAM alongside Pub/Sub and KMS.
func (s *Service) Owns(resource string) bool {
	rn := core.ParseName(resource)
	return rn.Trigger != "" || rn.Channel != ""
}

// iamTarget resolves an IAM resource name to its owning project, location and
// resource id, reporting whether it is a trigger (channel=false) or a channel.
func (s *Service) iamTarget(resource string) (project, location, id string, channel bool, ok bool) {
	rn := core.ParseName(resource)
	project = rn.Project
	if rn.Trigger != "" {
		return project, rn.Location, rn.Trigger, false, true
	}
	if rn.Channel != "" {
		return project, rn.Location, rn.Channel, true, true
	}
	return "", "", "", false, false
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project, location, id, channel, ok := s.iamTarget(req.GetResource())
	if !ok {
		return nil, grpcutil.GRPCStatus(core.InvalidIAMResource())
	}
	var (
		pol policy.Policy
		err error
	)
	if channel {
		pol, err = s.core.ChannelGetIamPolicy(ctx, project, location, id)
	} else {
		pol, err = s.core.TriggerGetIamPolicy(ctx, project, location, id)
	}
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project, location, id, channel, ok := s.iamTarget(req.GetResource())
	if !ok {
		return nil, grpcutil.GRPCStatus(core.InvalidIAMResource())
	}
	body := protoPolicyToBody(req.GetPolicy())
	var (
		pol policy.Policy
		err error
	)
	if channel {
		pol, err = s.core.ChannelSetIamPolicy(ctx, project, location, id, body)
	} else {
		pol, err = s.core.TriggerSetIamPolicy(ctx, project, location, id, body)
	}
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	project, location, id, channel, ok := s.iamTarget(req.GetResource())
	if !ok {
		return nil, grpcutil.GRPCStatus(core.InvalidIAMResource())
	}
	var (
		perms []string
		err   error
	)
	if channel {
		perms, err = s.core.ChannelTestIamPermissions(ctx, project, location, id, req.GetPermissions())
	} else {
		perms, err = s.core.TriggerTestIamPermissions(ctx, project, location, id, req.GetPermissions())
	}
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return &iampb.TestIamPermissionsResponse{Permissions: perms}, nil
}

// protoPolicyToBody renders an iampb.Policy as the JSON body the shared policy
// package stores (the same shape the REST :setIamPolicy body carries).
func protoPolicyToBody(p *iampb.Policy) map[string]any {
	body := map[string]any{}
	if p == nil {
		return body
	}
	bindings := make([]any, 0, len(p.GetBindings()))
	for _, b := range p.GetBindings() {
		members := make([]any, 0, len(b.GetMembers()))
		for _, m := range b.GetMembers() {
			members = append(members, m)
		}
		bindings = append(bindings, map[string]any{"role": b.GetRole(), "members": members})
	}
	body["bindings"] = bindings
	if et := p.GetEtag(); len(et) > 0 {
		body["etag"] = string(et)
	}
	if p.GetVersion() != 0 {
		body["version"] = int(p.GetVersion())
	}
	return body
}

// policyToProto renders a stored policy as an iampb.Policy.
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
