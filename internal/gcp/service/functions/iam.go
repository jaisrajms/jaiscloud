package functions

import (
	"context"

	"jaiscloud/internal/gcp/policy"
)

// GetIamPolicy returns the function's IAM policy. The function must exist.
func (s *Service) GetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error) {
	if err := s.requireFunction(ctx, project, location, id); err != nil {
		return policy.Policy{}, err
	}
	return s.loadPolicy(ctx, project, location, id), nil
}

// SetIamPolicy stores a policy for the function (etag OCC) and returns it. The
// function must exist.
func (s *Service) SetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	if err := s.requireFunction(ctx, project, location, id); err != nil {
		return policy.Policy{}, err
	}
	return policy.Set(ctx, s.resources, project, rtFunctionPolicy, iamID(location, id), body)
}

// TestIamPermissions echoes the requested permissions for an existing function.
func (s *Service) TestIamPermissions(ctx context.Context, project, location, id string, permissions []string) ([]string, error) {
	if err := s.requireFunction(ctx, project, location, id); err != nil {
		return nil, err
	}
	return policy.TestPermissions(permissions), nil
}
