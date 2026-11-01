package dataproc

import (
	"context"
	"encoding/json"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/encoding/protojson"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/dataproc"
)

// This file implements the google.iam.v1.IAMPolicy mixin for Dataproc workflow
// templates. The pinned dataproc proto defines no IAM RPCs, so real GCP serves
// the mixin through the standalone google.iam.v1.IAMPolicy interface; the
// emulator registers this Service with the shared IAMPolicy router
// (internal/gcp/grpc/iam.go) alongside Pub/Sub, KMS, Eventarc and Metastore.
//
// Unlike the older per-service converters, the policy is transcoded with
// protojson in both directions so an iampb.Binding.condition survives a
// set/get round-trip (the hand-written converters drop it).

// Owns reports whether this service handles IAM for the resource name. Only
// dataproc workflow-template names are claimed; clusters and jobs have no IAM
// surface in the emulator.
func (s *Service) Owns(resource string) bool {
	_, ok := core.ParseIAMResource(resource)
	return ok
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	pol, err := s.core.GetIamPolicy(ctx, req.GetResource())
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol)
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	body, err := policyBody(req.GetPolicy())
	if err != nil {
		return nil, mapError(err)
	}
	pol, err := s.core.SetIamPolicy(ctx, req.GetResource(), body)
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol)
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	perms, err := s.core.TestIamPermissions(ctx, req.GetResource(), req.GetPermissions())
	if err != nil {
		return nil, mapError(err)
	}
	return &iampb.TestIamPermissionsResponse{Permissions: perms}, nil
}

// policyToProto renders a stored policy as an iampb.Policy. policy.ToMap emits
// the wire JSON shape (including each binding's condition object), so protojson
// is the faithful converter.
func policyToProto(p policy.Policy) (*iampb.Policy, error) {
	data, err := json.Marshal(policy.ToMap(p))
	if err != nil {
		return nil, mapError(err)
	}
	out := &iampb.Policy{}
	if err := protojson.Unmarshal(data, out); err != nil {
		return nil, mapError(err)
	}
	return out, nil
}

// policyBody renders an iampb.Policy as the JSON map policy.Set accepts. The
// condition survives because protojson emits google.type.Expr as its JSON form.
func policyBody(p *iampb.Policy) (map[string]any, error) {
	if p == nil {
		return map[string]any{}, nil
	}
	data, err := protojson.Marshal(p)
	if err != nil {
		return nil, err
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, err
	}
	return body, nil
}

// compile-time assertion that Service participates in the IAM router.
var _ grpcutil.IAMResourceServer = (*Service)(nil)
