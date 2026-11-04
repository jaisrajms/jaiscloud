// Package cloudbilling is the transport-neutral core of Cloud Billing v1
// (cloudbilling.googleapis.com/v1).
//
// Only the project↔billing-account association is modelled: the
// ProjectBillingInfo singleton at projects/{project}/billingInfo. The
// billing-account catalog (billingAccounts.*, subAccounts.*, organizations.*,
// services.*, skus.*) is deliberately not implemented — the emulator has no
// account lifecycle and no SKU catalog to serve (plan CB5). The association is
// stored in the shared ResourceStore, so memory and --dsn behave identically,
// the resources snapshotter covers it, and no new store package or Reset
// registration is needed.
//
// There is no protobuf or NormalizedRequest dependency: the REST transport
// (internal/gcp/transport/rest/cloudbilling) transcodes its wire format into
// this package's typed API. The service is REST-only: no client in scope speaks
// google.cloud.billing.v1 over gRPC.
package cloudbilling

import (
	"context"
	"encoding/json"
	"errors"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/store"
)

// rtBillingInfo is the resource type for a project's billing association in the
// shared ResourceStore. The entry id and the owning account are both the project
// id, and the region is store.GlobalRegion: billing is a global,
// project-scoped singleton, so a Get/Delete is naturally project-scoped.
const rtBillingInfo = "billingInfo"

// BillingInfo is a project's billing-account association (the ProjectBillingInfo
// schema minus the server-derived name and billingEnabled fields, which the REST
// transport renders). A project with no stored record is not associated:
// BillingAccountName is empty and billing is disabled.
type BillingInfo struct {
	// ProjectID is the project the association belongs to (output-only on the
	// wire; it is always the resolved request project).
	ProjectID string `json:"projectId"`
	// BillingAccountName is the canonical account resource name
	// ("billingAccounts/{account_id}") the project is linked to, or "" when the
	// project is not associated.
	BillingAccountName string `json:"billingAccountName"`
}

// Service is the transport-neutral Cloud Billing core.
type Service struct {
	resources store.ResourceStore
}

// NewService returns a Cloud Billing core backed by the shared ResourceStore.
func NewService(resources store.ResourceStore) *Service {
	return &Service{resources: resources}
}

// BillingInfoName formats the project-billing singleton's canonical resource
// name ("projects/{project}/billingInfo" — ProjectBillingInfo.name) from the
// raw project id.
func BillingInfoName(project string) string {
	return resource.ResourceID(project)("cloudbilling-project-info", "")
}

// GetProjectBillingInfo returns the project's billing association. A project
// with no stored record is simply not associated — the zero association, not an
// error — matching a newly created real project and the Terraform data source's
// "no billing account" read. Any project id is answered (the resourcemanager
// stance of synthesizing unknown projects).
func (s *Service) GetProjectBillingInfo(ctx context.Context, project string) (BillingInfo, error) {
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtBillingInfo, project)
	if errors.Is(err, store.ErrNotFound) {
		return BillingInfo{ProjectID: project}, nil
	}
	if err != nil {
		return BillingInfo{}, err
	}
	var info BillingInfo
	if err := json.Unmarshal(e.Data, &info); err != nil {
		return BillingInfo{}, err
	}
	if info.ProjectID == "" {
		info.ProjectID = e.ID
	}
	return info, nil
}
