package resourcemanager

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"jaiscloud/internal/store"
)

// OrganizationNumberForProject reports the numeric id of the organization a
// project belongs to, derived from the parent reference the project registry
// stores ("organizations/{id}"). It is what IAM Credentials reports in the
// documented `google.organization_number` ID-token claim.
//
// nil means the project is not associated with an organization, the documented
// value of that claim for such a service account. That covers a project with no
// registry entry (the emulator's synthesized projects), a project under a folder
// (the folder hierarchy is not modelled: only the parent reference is kept), and
// an unset or non-numeric parent.
func OrganizationNumberForProject(ctx context.Context, resources store.ResourceStore, project string) (*int64, error) {
	if project == "" {
		return nil, nil
	}
	e, err := resources.Get(ctx, project, store.GlobalRegion, rtProject, project)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p Project
	if err := json.Unmarshal(e.Data, &p); err != nil {
		return nil, err
	}
	return organizationNumber(p.Parent), nil
}

// organizationNumber parses the numeric id out of an "organizations/{id}" parent
// reference and returns nil for any other parent shape.
func organizationNumber(parent string) *int64 {
	const prefix = "organizations/" //gcplint:ignore resource-prefix parent reference parsed, not built
	if !strings.HasPrefix(parent, prefix) {
		return nil
	}
	n, err := strconv.ParseInt(parent[len(prefix):], 10, 64)
	if err != nil {
		return nil
	}
	return &n
}
