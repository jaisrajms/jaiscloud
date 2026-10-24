package metastore

import (
	"context"

	iampb "cloud.google.com/go/iam/apiv1/iampb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/metastore"
)

// This file implements the google.iam.v1.IAMPolicy mixin for Dataproc Metastore
// resources. The pinned metastore proto defines no IAM RPCs, so real GCP serves
// the mixin through the standalone google.iam.v1.IAMPolicy interface; the
// emulator registers this Service with the shared IAMPolicy router
// (internal/gcp/grpc/iam.go) alongside Pub/Sub, KMS and Eventarc. Owns keys the
// dispatch on the resource name, so one handler covers the
// service/backup/database/table/federation levels.

// Owns reports whether this service handles IAM for the resource name. It
// recognizes the metastore resource collections (services and the resources
// nested under them, plus federations), letting the shared IAMPolicy router
// dispatch metastore IAM alongside Pub/Sub, KMS and Eventarc.
func (s *Service) Owns(resource string) bool {
	_, ok := core.ParseIAMResource(resource)
	return ok
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	pol, err := s.core.GetIamPolicy(ctx, req.GetResource())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	pol, err := s.core.SetIamPolicy(ctx, req.GetResource(), protoPolicyToBody(req.GetPolicy()))
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	perms, err := s.core.TestIamPermissions(ctx, req.GetResource(), req.GetPermissions())
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

// compile-time assertion that Service participates in the IAM router.
var _ interface {
	GetIamPolicy(context.Context, *iampb.GetIamPolicyRequest) (*iampb.Policy, error)
} = (*Service)(nil)
