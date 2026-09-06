package grpc

import (
	"context"

	iampb "cloud.google.com/go/iam/apiv1/iampb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IAMResourceServer implements the google.iam.v1.IAMPolicy surface for one
// class of resources (Pub/Sub topics+subscriptions, KMS keyrings/keys/versions).
// Owns reports whether the handler recognizes a resource name.
type IAMResourceServer interface {
	iampb.IAMPolicyServer
	Owns(resource string) bool
}

// IAMRouter implements iampb.IAMPolicyServer by delegating each call to the
// first handler that owns the resource name. gRPC permits only a single
// registration per service, so this unifies the Pub/Sub and KMS IAM surfaces
// behind one google.iam.v1.IAMPolicy server.
type IAMRouter struct {
	iampb.UnimplementedIAMPolicyServer
	handlers []IAMResourceServer
}

// NewIAMRouter returns a router that dispatches to handlers in registration
// order.
func NewIAMRouter(handlers ...IAMResourceServer) *IAMRouter {
	return &IAMRouter{handlers: handlers}
}

func (r *IAMRouter) route(resource string) iampb.IAMPolicyServer {
	for _, h := range r.handlers {
		if h.Owns(resource) {
			return h
		}
	}
	return nil
}

func (r *IAMRouter) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	h := r.route(req.GetResource())
	if h == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid resource name")
	}
	return h.GetIamPolicy(ctx, req)
}

func (r *IAMRouter) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	h := r.route(req.GetResource())
	if h == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid resource name")
	}
	return h.SetIamPolicy(ctx, req)
}

func (r *IAMRouter) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	h := r.route(req.GetResource())
	if h == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid resource name")
	}
	return h.TestIamPermissions(ctx, req)
}
