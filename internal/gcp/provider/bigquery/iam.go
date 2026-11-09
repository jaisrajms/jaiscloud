package bigquery

import (
	"context"
	"errors"

	"jaiscloud/internal/gcp/policy"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// TablePolicyType is the shared ResourceStore type that holds a table's IAM
// policy. One type for every table keeps these rows distinct from the other
// policy planes; the dataset-level plane is the legacy `access` ACL carried in
// the dataset resource itself (real BigQuery has no datasets.getIamPolicy).
const TablePolicyType = "gcp_bigquery_table_policy"

// tablePolicyKey is the project-relative ResourceStore id of a table's policy.
// The dataset segment keeps two datasets' same-named tables apart.
func tablePolicyKey(datasetID, tableID string) string { return datasetID + "/" + tableID }

// tableIamTarget resolves the dataset/table pair an IAM request names and
// verifies the table exists. The existence check runs before the policy plane
// is read or written, so a missing table is NotFound for get/set and never
// creates a policy row. testIamPermissions deliberately does not call this: the
// Discovery contract answers an empty permission set for an absent resource.
func (p *Provider) tableIamTarget(ctx context.Context, nr *model.NormalizedRequest) (string, string, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return "", "", invalidArgument("datasetId and tableId are required")
	}
	if _, err := p.store.GetTable(ctx, projectOf(nr), datasetID, tableID); err != nil {
		return "", "", mapErr(err)
	}
	return datasetID, tableID, nil
}

// policyStore returns the IAM policy plane. A provider constructed without the
// seam (unit tests, or a transport that never serves IAM) answers the explicit
// Unimplemented rather than fabricating a policy.
func (p *Provider) policyStore() (store.ResourceStore, error) {
	if p.resources == nil {
		return nil, unimplementedResource("table IAM")
	}
	return p.resources, nil
}

// GetTableIamPolicy answers tables.getIamPolicy. The response is the Policy
// object itself (the Discovery response is `$ref: Policy`, not a wrapper), and
// an existing table without a stored policy yields the empty default policy.
func (p *Provider) GetTableIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resources, err := p.policyStore()
	if err != nil {
		return nil, err
	}
	datasetID, tableID, err := p.tableIamTarget(ctx, nr)
	if err != nil {
		return nil, err
	}
	pol := policy.Load(ctx, resources, projectOf(nr), TablePolicyType, tablePolicyKey(datasetID, tableID))
	return provider.OK(policy.ToMap(pol)), nil
}

// SetTableIamPolicy answers tables.setIamPolicy. The body is a
// SetIamPolicyRequest ({"policy": {...}}), which policy.Set unwraps; etag OCC
// and the AIP-149 version rules are shared with every other policy plane.
func (p *Provider) SetTableIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resources, err := p.policyStore()
	if err != nil {
		return nil, err
	}
	datasetID, tableID, err := p.tableIamTarget(ctx, nr)
	if err != nil {
		return nil, err
	}
	body := bodyMap(nr)
	if body == nil {
		body = map[string]any{}
	}
	pol, err := policy.Set(ctx, resources, projectOf(nr), TablePolicyType, tablePolicyKey(datasetID, tableID), body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

// TestTableIamPermissions answers tables.testIamPermissions. The emulator does
// not enforce IAM, so every requested permission is granted. Discovery
// documents that an absent resource returns an empty set rather than NOT_FOUND,
// so a missing table is not an error here (unlike get/set).
func (p *Provider) TestTableIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, err := p.policyStore(); err != nil {
		return nil, err
	}
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	perms := policy.TestPermissions(policy.Permissions(bodyMap(nr)))
	if _, err := p.store.GetTable(ctx, projectOf(nr), datasetID, tableID); err != nil {
		if !errors.Is(err, bqstore.ErrNoSuchTable) {
			return nil, mapErr(err)
		}
		perms = nil
	}
	if perms == nil {
		// The Discovery response declares permissions as an array; an absent
		// table (or an empty request) must render [] rather than null.
		perms = []string{}
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}

// WithResourceStore wires the shared policy plane the table IAM methods store
// their policy in.
func WithResourceStore(resources store.ResourceStore) Option {
	return func(p *Provider) { p.resources = resources }
}

// deleteTablePolicy drops a table's stored policy. It is best-effort: the table
// store is the source of truth, and a policy-plane failure must not turn a
// successful delete into an error.
func (p *Provider) deleteTablePolicy(ctx context.Context, project, datasetID, tableID string) {
	if p.resources == nil {
		return
	}
	_ = p.resources.Delete(ctx, project, store.GlobalRegion, TablePolicyType, tablePolicyKey(datasetID, tableID))
}
