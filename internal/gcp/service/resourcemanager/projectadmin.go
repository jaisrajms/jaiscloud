package resourcemanager

import (
	"context"
	"fmt"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/model"
)

// errProjectEtagMismatch is the ABORTED/409 sentinel a stale project etag
// produces. It mirrors the shared IAM-policy etag-OCC error (internal/gcp/policy)
// so the gRPC mapping is intentional (Aborted) rather than incidental via
// httpToCode(409), and the REST envelope reports "ABORTED".
var errProjectEtagMismatch = &model.ProviderError{
	Code:       "Aborted",
	Message:    "etag mismatch: optimistic concurrency control failed",
	HTTPStatus: 409,
	Status:     "ABORTED",
}

// UpdateProjectInput carries the caller-supplied fields of a project metadata
// update. UpdateMask is the set of mutable field paths to apply (the proto
// field-mask paths "display_name" and/or "labels"); an empty mask applies the
// populated mutable fields. Etag, when non-empty, is enforced as optimistic
// concurrency control.
type UpdateProjectInput struct {
	DisplayName string
	Labels      map[string]string
	UpdateMask  []string
	Etag        string
}

// UpdateProject applies a metadata update to a project and returns it with the
// mutation operation. Only display name and labels are mutable; any other mask
// path is InvalidArgument. An unknown id that is neither created nor configured
// is NotFound; a configured id with no registry entry is materialized so the
// new state persists (matching DeleteProject). A non-empty request etag that
// does not match the current one is rejected with ABORTED/409. The etag rotates
// on every successful update.
func (s *Service) UpdateProject(ctx context.Context, project string, in UpdateProjectInput) (Project, Operation, error) {
	if project == "" {
		return Project{}, Operation{}, invalidArgument("project is required")
	}
	if err := validateUpdateMask(in.UpdateMask); err != nil {
		return Project{}, Operation{}, err
	}
	p, err := s.transitionProject(ctx, project, func(cur Project, exists bool) (Project, error) {
		if !exists {
			if !s.isKnown(project) {
				return Project{}, notFoundProject(project)
			}
			cur = synthesizeProject(project)
			cur.CreateTime = clock.Now().UTC()
		}
		if in.Etag != "" && in.Etag != cur.Etag {
			return Project{}, errProjectEtagMismatch
		}
		cur = applyProjectUpdate(cur, in)
		cur.Etag = freshProjectEtag(project)
		cur.UpdateTime = clock.Now().UTC()
		return cur, nil
	})
	if err != nil {
		return Project{}, Operation{}, err
	}
	op, err := s.newOperation(ctx, project, "update", p)
	if err != nil {
		return Project{}, Operation{}, err
	}
	return p, op, nil
}

// applyProjectUpdate returns cur with the masked (or, for an empty mask, the
// populated) mutable fields replaced. Masked labels replace the whole label map
// (a nil map clears it); a masked empty display name clears it.
func applyProjectUpdate(cur Project, in UpdateProjectInput) Project {
	if len(in.UpdateMask) == 0 {
		if in.DisplayName != "" {
			cur.DisplayName = in.DisplayName
		}
		if in.Labels != nil {
			cur.Labels = cloneLabels(in.Labels)
		}
		return cur
	}
	for _, path := range in.UpdateMask {
		switch normalizeUpdatePath(path) {
		case "display_name":
			cur.DisplayName = in.DisplayName
		case "labels":
			cur.Labels = cloneLabels(in.Labels)
		case "*":
			// AIP-134 full replacement: apply every mutable field.
			cur.DisplayName = in.DisplayName
			cur.Labels = cloneLabels(in.Labels)
		}
	}
	return cur
}

// validateUpdateMask rejects a field-mask path outside the mutable set. Both the
// proto field name (display_name) and the JSON name (displayName) are accepted,
// as is the AIP-134 `*` full-replacement path.
func validateUpdateMask(mask []string) error {
	for _, path := range mask {
		if normalizeUpdatePath(path) == "" {
			return invalidArgument(fmt.Sprintf(
				"Invalid update mask path %q. Only display_name and labels are mutable.", path))
		}
	}
	return nil
}

// normalizeUpdatePath maps an accepted field-mask path to its canonical form,
// or "" when unsupported.
func normalizeUpdatePath(path string) string {
	switch path {
	case "display_name", "displayName":
		return "display_name"
	case "labels":
		return "labels"
	case "*":
		return "*"
	}
	return ""
}

// cloneLabels returns an isolated copy of m (nil stays nil), so a stored or
// returned project never aliases a caller-supplied map.
func cloneLabels(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// MoveProject reparents a project under destinationParent and returns it with
// the mutation operation. destinationParent must be "organizations/{id}" or
// "folders/{id}" (InvalidArgument otherwise). An unknown id is NotFound, a
// non-ACTIVE project is FailedPrecondition, and moving to the current parent is
// an idempotent no-op. The etag rotates on a real move.
func (s *Service) MoveProject(ctx context.Context, project, destinationParent string) (Project, Operation, error) {
	if project == "" {
		return Project{}, Operation{}, invalidArgument("project is required")
	}
	dest := strings.TrimSpace(destinationParent)
	if !isProjectParent(dest) {
		return Project{}, Operation{}, invalidArgument(fmt.Sprintf(
			"Invalid destination parent %q. It must be \"organizations/{id}\" or \"folders/{id}\".", destinationParent))
	}
	p, err := s.transitionProject(ctx, project, func(cur Project, exists bool) (Project, error) {
		if !exists {
			if !s.isKnown(project) {
				return Project{}, notFoundProject(project)
			}
			cur = synthesizeProject(project)
			cur.CreateTime = clock.Now().UTC()
		}
		if cur.State != StateActive {
			return Project{}, failedPrecondition(fmt.Sprintf("Project %s is not ACTIVE.", project))
		}
		if cur.Parent == dest {
			return cur, nil
		}
		cur.Parent = dest
		cur.Etag = freshProjectEtag(project)
		cur.UpdateTime = clock.Now().UTC()
		return cur, nil
	})
	if err != nil {
		return Project{}, Operation{}, err
	}
	op, err := s.newOperation(ctx, project, "move", p)
	if err != nil {
		return Project{}, Operation{}, err
	}
	return p, op, nil
}

// isProjectParent reports whether parent is a well-formed org/folder reference
// ("organizations/{numeric-id}" or "folders/{numeric-id}"). Real GCP requires a
// numeric parent id; an extra path segment or a non-numeric id is rejected.
func isProjectParent(parent string) bool {
	parts := strings.Split(parent, "/")
	if len(parts) != 2 || (parts[0] != "organizations" && parts[0] != "folders") {
		return false
	}
	if parts[1] == "" {
		return false
	}
	for i := 0; i < len(parts[1]); i++ {
		if parts[1][i] < '0' || parts[1][i] > '9' {
			return false
		}
	}
	return true
}

// SearchProjects returns a cursor page of projects matching the v3 search query
// expression (the same grammar ListProjects accepts). Unlike ListProjects it
// has no showDeleted switch, so DELETE_REQUESTED projects are returned unless a
// state filter excludes them; the query is applied before pagination.
func (s *Service) SearchProjects(ctx context.Context, pageSize int, pageToken, query string) ([]Project, string, error) {
	pred, err := compileProjectFilter(query)
	if err != nil {
		return nil, "", err
	}
	byID, err := s.collectProjects(ctx)
	if err != nil {
		return nil, "", err
	}
	out := make([]Project, 0, len(byID))
	for _, p := range byID {
		if pred.match(p) {
			out = append(out, p)
		}
	}
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	page, next, err := paging.Page(out, func(p Project) string { return p.ProjectID }, params)
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// freshProjectEtag derives a fresh, non-deterministic project etag so it rotates
// on every successful mutation while staying an opaque SHA-1/Base64 checksum.
func freshProjectEtag(project string) string {
	return policy.Etag(ProjectName(project) + ":" + randomHex(16))
}
