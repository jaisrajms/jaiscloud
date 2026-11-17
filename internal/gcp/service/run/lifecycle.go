package run

import "context"

// Name identifies the Cloud Run hook in POST /_jaiscloud/park|resume.
func (s *Service) Name() string { return "cloudrun" }

// Park tears down every revision runtime for every service, keeping the
// service and revision records intact. A parked service has no container to
// serve traffic until Resume recreates its latest ready revision.
func (s *Service) Park(ctx context.Context, project string) (map[string]any, error) {
	svcs, err := s.ListAllServices(ctx, project)
	if err != nil {
		return nil, err
	}
	removed := 0
	for _, svc := range svcs {
		revs, err := s.ListRevisions(ctx, svc.ProjectID, svc.Location, svc.ID)
		if err != nil {
			return map[string]any{"revisionsRemoved": removed}, err
		}
		for _, rev := range revs {
			if err := s.runtime.RemoveRevision(ctx, rev); err != nil {
				return map[string]any{"revisionsRemoved": removed}, err
			}
			removed++
		}
	}
	return map[string]any{"revisionsRemoved": removed}, nil
}

// Resume recreates the runtime for each service's latest ready revision. A
// service with no latest ready revision (never deployed, or still rolling out)
// is skipped; the runtime manager's EnsureRevision is idempotent.
func (s *Service) Resume(ctx context.Context, project string) error {
	svcs, err := s.ListAllServices(ctx, project)
	if err != nil {
		return err
	}
	for _, svc := range svcs {
		if svc.LatestReadyRevision == "" {
			continue
		}
		revs, err := s.ListRevisions(ctx, svc.ProjectID, svc.Location, svc.ID)
		if err != nil {
			return err
		}
		for _, rev := range revs {
			if RevisionName(rev.ProjectID, rev.Location, rev.Service, rev.ID) != svc.LatestReadyRevision {
				continue
			}
			if err := s.runtime.EnsureRevision(ctx, svc, rev); err != nil {
				return err
			}
			break
		}
	}
	return nil
}
