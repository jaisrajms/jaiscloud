package cloudbilling

import (
	"context"

	core "jaiscloud/internal/gcp/service/cloudbilling"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles the Cloud Billing v1 REST project billing surface. It is a
// thin adapter: every handler resolves the NormalizedRequest params into the
// core's typed API, calls the shared core Service, and encodes the result as
// Discovery-shaped JSON. No business logic lives here.
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
		"CloudBilling.ProjectGetBillingInfo": p.GetProjectBillingInfo,
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
