package cloudbilling

import (
	"context"
	"fmt"

	core "jaiscloud/internal/gcp/service/cloudbilling"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles the Cloud Billing v1 REST surface. It is a thin adapter:
// every handler resolves the NormalizedRequest params into the core's typed API,
// calls the shared core Service, and encodes the result as Discovery-shaped
// JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Cloud Billing REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "CloudBilling.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"CloudBilling.ProjectGetBillingInfo":            p.GetProjectBillingInfo,
		"CloudBilling.ProjectUpdateBillingInfo":         p.UpdateProjectBillingInfo,
		"CloudBilling.BillingAccountsList":              p.ListBillingAccounts,
		"CloudBilling.BillingAccountsGet":               p.GetBillingAccount,
		"CloudBilling.BillingAccountTestIamPermissions": p.TestBillingAccountIamPermissions,
	}
}

// project resolves the owning project: the path project, else the request's
// account (project) scope, else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// GetProjectBillingInfo returns the ProjectBillingInfo singleton for the
// project. billingEnabled is derived from the stored account name (the field is
// output-only in the Discovery document), so a project with no association reads
// back the four-field default with an empty billingAccountName.
func (p *Provider) GetProjectBillingInfo(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	info, err := p.core.GetProjectBillingInfo(ctx, p.project(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(billingInfoJSON(info)), nil
}

// UpdateProjectBillingInfo applies projects.updateBillingInfo: it stores the
// body's billingAccountName (the full "billingAccounts/{account_id}" resource
// name; an empty string clears the association) and returns the resulting
// ProjectBillingInfo, so the write and the read-back use one schema. A body
// billingEnabled is ignored — the field is output-only.
func (p *Provider) UpdateProjectBillingInfo(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	info, err := p.core.SetProjectBillingInfo(ctx, p.project(nr), mapStr(body, "billingAccountName"))
	if err != nil {
		return nil, err
	}
	return provider.OK(billingInfoJSON(info)), nil
}

// ListBillingAccounts returns the billing-account catalog. It is always empty:
// the emulator models no account lifecycle (plan CB5), so the honest answer is
// the schema-valid empty list rather than a fabricated account. Real GCP omits
// the field when empty; an explicit [] is equivalent for every client.
func (p *Provider) ListBillingAccounts(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	accounts := p.core.ListBillingAccounts(ctx)
	items := make([]any, 0, len(accounts))
	for _, a := range accounts {
		items = append(items, a)
	}
	return provider.OK(map[string]any{"billingAccounts": items}), nil
}

// GetBillingAccount answers billingAccounts.get. No account catalog exists, so
// every id is NotFound — never a synthesized account.
func (p *Provider) GetBillingAccount(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("NotFound", fmt.Sprintf(
		"billing account %q not found: the emulator models no billing-account catalog (plan CB5)",
		strParam(nr, "billingAccount")), 404)
}

// TestBillingAccountIamPermissions answers billingAccounts.testIamPermissions
// with a default-permissive echo: it returns exactly the requested permission
// strings and enforces nothing, the same stance as the rest of the emulator's
// IAM. An absent or empty request echoes an empty list.
func (p *Provider) TestBillingAccountIamPermissions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	perms := stringSlice(bodyOf(nr)["permissions"])
	granted := make([]any, 0, len(perms))
	for _, perm := range perms {
		granted = append(granted, perm)
	}
	return provider.OK(map[string]any{"permissions": granted}), nil
}

// billingInfoJSON renders the core association as the Discovery document's
// ProjectBillingInfo: name, projectId, billingAccountName and the derived
// billingEnabled. Those are the whole schema — no emulator-only fields.
func billingInfoJSON(info core.BillingInfo) map[string]any {
	return map[string]any{
		"name":               core.BillingInfoName(info.ProjectID),
		"projectId":          info.ProjectID,
		"billingAccountName": info.BillingAccountName,
		"billingEnabled":     info.BillingAccountName != "",
	}
}
