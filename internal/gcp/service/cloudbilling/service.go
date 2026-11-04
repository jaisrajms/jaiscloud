// Package cloudbilling is the transport-neutral core of Cloud Billing v1
// (cloudbilling.googleapis.com/v1).
//
// Only the project↔billing-account association is modelled: the
// ProjectBillingInfo singleton at projects/{project}/billingInfo (read and
// update). The billing-account catalog (billingAccounts.*, subAccounts.*,
// organizations.*, services.*, skus.*) is deliberately not implemented — the
// emulator has no account lifecycle and no SKU catalog to serve (plan CB5); the
// one exception is the permissive billing-account
// testIamPermissions echo, which the transport serves directly because it has
// no business logic. The association is
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
	"strings"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
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

// billingAccountPrefix is the resource-name prefix a project association must
// carry (billingAccounts/{account_id} — the ProjectBillingInfo.billingAccountName
// form). updateBillingInfo receives the full resource name (the Terraform
// provider prepends the prefix to the bare account id), so a bare id or a
// foreign resource name is rejected rather than stored.
const billingAccountPrefix = "billingAccounts/"

// SetProjectBillingInfo associates the project with billingAccountName (the full
// "billingAccounts/{account_id}" resource name) or clears the association when
// billingAccountName is empty. A non-empty value without the billingAccounts/
// prefix is InvalidArgument: storing it would make every read render an account
// no client could resolve.
//
// The stored record is the same shape GetProjectBillingInfo reads, so a write is
// immediately visible to the next read on either transport. Clearing stores the
// empty association (a cleared record, not a deleted one) so a subsequent Get
// returns the zero association from the store rather than from the not-found
// default — the wire answer is identical either way.
func (s *Service) SetProjectBillingInfo(ctx context.Context, project, billingAccountName string) (BillingInfo, error) {
	if billingAccountName != "" &&
		(!strings.HasPrefix(billingAccountName, billingAccountPrefix) ||
			len(billingAccountName) == len(billingAccountPrefix) ||
			strings.TrimSpace(billingAccountName) != billingAccountName) {
		return BillingInfo{}, model.NewProviderError("InvalidArgument",
			"billingAccountName must be a billing account resource name: billingAccounts/{account_id}", 400)
	}
	info := BillingInfo{ProjectID: project, BillingAccountName: billingAccountName}
	data, err := json.Marshal(info)
	if err != nil {
		return BillingInfo{}, err
	}
	if err := s.resources.Upsert(ctx, project, store.GlobalRegion, store.ResourceEntry{
		Type: rtBillingInfo, ID: project, Data: data,
	}); err != nil {
		return BillingInfo{}, err
	}
	return info, nil
}

// BillingAccount is a billing-account catalog entry (the Discovery BillingAccount
// resource, minus the organization link the emulator has no notion of). There is
// no account lifecycle behind it, so it is never instantiated: the type pins the
// shape the transport would render once the catalog is modelled (plan CB5).
type BillingAccount struct {
	// Name is the canonical account resource name: billingAccounts/{account_id}.
	Name string `json:"name"`
	// DisplayName is the human-readable account name.
	DisplayName string `json:"displayName,omitempty"`
	// Open reports whether the account is open for business.
	Open bool `json:"open"`
}

// ListBillingAccounts returns the billing-account catalog. The emulator models no
// account lifecycle and no SKU catalog, so the catalog is deliberately empty
// (plan CB5): an empty list is the honest answer, and the transport renders it
// as the schema-valid {"billingAccounts": []} rather than a fabricated account.
func (s *Service) ListBillingAccounts(context.Context) []BillingAccount {
	return nil
}
