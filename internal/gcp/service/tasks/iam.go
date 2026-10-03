package tasks

import (
	"context"

	"jaiscloud/internal/gcp/policy"
)

// QueueGetIamPolicy returns the queue's IAM policy. The queue must exist.
func (s *Service) QueueGetIamPolicy(ctx context.Context, project, location, queue string) (policy.Policy, error) {
	if _, err := s.store.GetQueue(ctx, project, location, queue); err != nil {
		return policy.Policy{}, mapStoreErr(err)
	}
	return s.loadPolicy(ctx, project, location, queue), nil
}

// QueueSetIamPolicy stores a policy for the queue (etag OCC) and returns it.
// The queue must exist.
func (s *Service) QueueSetIamPolicy(ctx context.Context, project, location, queue string, body map[string]any) (policy.Policy, error) {
	if _, err := s.store.GetQueue(ctx, project, location, queue); err != nil {
		return policy.Policy{}, mapStoreErr(err)
	}
	return policy.Set(ctx, s.resources, project, rtQueuePolicy, iamID(location, queue), body)
}

// QueueTestIamPermissions echoes the requested permissions for an existing
// queue.
func (s *Service) QueueTestIamPermissions(ctx context.Context, project, location, queue string, permissions []string) ([]string, error) {
	if _, err := s.store.GetQueue(ctx, project, location, queue); err != nil {
		return nil, mapStoreErr(err)
	}
	return policy.TestPermissions(permissions), nil
}
