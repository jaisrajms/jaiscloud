package sdkrest_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/cloudbilling/v1"
	"google.golang.org/api/googleapi"
)

// The billing account the write-path tests associate a project with. The
// emulator models no account catalog, so any well-formed
// "billingAccounts/{account_id}" resource name is stored verbatim; this is the
// same id the Terraform suite and the wave plan use.
const testBillingAccount = "0X0X0X-0X0X0X-0X0X0X"

func billingAccountName(account string) string { return "billingAccounts/" + account }

// billingInfo fetches the projects/{p}/billingInfo singleton through the
// official client (cloudbilling.projects.getBillingInfo).
func billingInfo(ctx context.Context, t *testing.T, svc *cloudbilling.APIService, project string) *cloudbilling.ProjectBillingInfo {
	t.Helper()
	info, err := svc.Projects.GetBillingInfo("projects/" + project).Context(ctx).Do()
	require.NoError(t, err)
	return info
}

// TestSDKCloudBillingProjectBillingInfo covers the project↔billing-account
// association through the official apiary client: the untouched-project default
// (the read that used to be an UnknownService 404 and blocked Terraform's
// data "google_project"), the update→read round-trip, and the disable path.
func TestSDKCloudBillingProjectBillingInfo(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudbilling.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := unique("cbfresh")

	// A project nobody touched is not associated: the four-field default, with
	// billingAccountName empty and billingEnabled false.
	got := billingInfo(ctx, t, svc, project)
	require.Equal(t, "projects/"+project+"/billingInfo", got.Name)
	require.Equal(t, project, got.ProjectId)
	require.Empty(t, got.BillingAccountName)
	require.False(t, got.BillingEnabled)

	// updateBillingInfo stores the full resource name; the read echoes it and
	// derives billingEnabled (output-only on the wire) from it.
	acct := billingAccountName(testBillingAccount)
	wrote, err := svc.Projects.UpdateBillingInfo("projects/"+project,
		&cloudbilling.ProjectBillingInfo{BillingAccountName: acct}).Context(ctx).Do()
	require.NoError(t, err)
	require.Equal(t, acct, wrote.BillingAccountName)
	require.True(t, wrote.BillingEnabled)

	got = billingInfo(ctx, t, svc, project)
	require.Equal(t, "projects/"+project+"/billingInfo", got.Name)
	require.Equal(t, project, got.ProjectId)
	require.Equal(t, acct, got.BillingAccountName)
	require.True(t, got.BillingEnabled)

	// An empty billingAccountName disables billing again.
	wrote, err = svc.Projects.UpdateBillingInfo("projects/"+project,
		&cloudbilling.ProjectBillingInfo{}).Context(ctx).Do()
	require.NoError(t, err)
	require.Empty(t, wrote.BillingAccountName)
	require.False(t, wrote.BillingEnabled)

	got = billingInfo(ctx, t, svc, project)
	require.Empty(t, got.BillingAccountName)
	require.False(t, got.BillingEnabled)
}

// TestSDKCloudBillingProjectIDBoundary is the p5 boundary: a project id at the
// 30-character limit real GCP allows round-trips through the association
// without truncation or re-derivation of the name.
func TestSDKCloudBillingProjectIDBoundary(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudbilling.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := unique("cb")
	for len(project) < 30 {
		project += "0"
	}
	project = project[:30]
	require.Len(t, project, 30)

	acct := billingAccountName(testBillingAccount)
	_, err = svc.Projects.UpdateBillingInfo("projects/"+project,
		&cloudbilling.ProjectBillingInfo{BillingAccountName: acct}).Context(ctx).Do()
	require.NoError(t, err)

	got := billingInfo(ctx, t, svc, project)
	require.Equal(t, "projects/"+project+"/billingInfo", got.Name)
	require.Equal(t, project, got.ProjectId)
	require.Equal(t, acct, got.BillingAccountName)
	require.True(t, got.BillingEnabled)
}

// TestSDKCloudBillingErrors covers the negative surface: a billingAccountName
// that is not a billing-account resource name is InvalidArgument 400 (never
// stored), and a billing account that does not exist is NotFound 404 (the
// emulator fabricates no catalog).
func TestSDKCloudBillingErrors(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudbilling.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := unique("cbbad")
	_, err = svc.Projects.UpdateBillingInfo("projects/"+project,
		&cloudbilling.ProjectBillingInfo{BillingAccountName: "projects/x"}).Context(ctx).Do()
	var bad *googleapi.Error
	require.ErrorAs(t, err, &bad)
	require.Equal(t, 400, bad.Code, "a non-billingAccounts resource name must be InvalidArgument")

	// The rejected write left nothing behind: the project is still unassociated.
	got := billingInfo(ctx, t, svc, project)
	require.Empty(t, got.BillingAccountName)
	require.False(t, got.BillingEnabled)

	_, err = svc.BillingAccounts.Get(billingAccountName(testBillingAccount)).Context(ctx).Do()
	var missing *googleapi.Error
	require.ErrorAs(t, err, &missing)
	require.Equal(t, 404, missing.Code, "no billing-account catalog is modelled, so any account is NotFound")
}

// TestSDKCloudBillingAccounts covers the billing-account collection: the list is
// the schema-valid empty answer (honest, not a fabricated account) and ignores
// pageSize/pageToken, which is the whole paging surface an always-empty catalog
// has; testIamPermissions is a default-permissive echo of exactly the requested
// permissions (this is the call google_project's billing_account pre-requisite
// makes).
func TestSDKCloudBillingAccounts(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudbilling.NewService(ctx, opts()...)
	require.NoError(t, err)

	list, err := svc.BillingAccounts.List().PageSize(10).Context(ctx).Do()
	require.NoError(t, err)
	require.Empty(t, list.BillingAccounts)
	require.Empty(t, list.NextPageToken, "an empty catalog has no next page")

	perms := []string{
		"billing.resourceAssociations.create",
		"billing.resourceAssociations.delete",
	}
	got, err := svc.BillingAccounts.TestIamPermissions(billingAccountName(testBillingAccount),
		&cloudbilling.TestIamPermissionsRequest{Permissions: perms}).Context(ctx).Do()
	require.NoError(t, err)
	require.ElementsMatch(t, perms, got.Permissions)

	// An empty request echoes an empty set (the echo enforces nothing, so it
	// never invents a permission).
	empty, err := svc.BillingAccounts.TestIamPermissions(billingAccountName(testBillingAccount),
		&cloudbilling.TestIamPermissionsRequest{}).Context(ctx).Do()
	require.NoError(t, err)
	require.Empty(t, empty.Permissions)
}

// TestSDKCloudBillingConcurrentUpdate is the p2 concurrency class: two
// updateBillingInfo calls race on one project's singleton under -race, and the
// final read must be exactly one of the two written values (last writer wins;
// the store never leaves a torn or merged association behind).
func TestSDKCloudBillingConcurrentUpdate(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudbilling.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := unique("cbconc")
	accounts := []string{
		billingAccountName("0X0X0X-0X0X0X-0X0X0X"),
		billingAccountName("1Y1Y1Y-1Y1Y1Y-1Y1Y1Y"),
	}

	var wg sync.WaitGroup
	errs := make([]error, len(accounts))
	for i, acct := range accounts {
		wg.Add(1)
		go func(i int, acct string) {
			defer wg.Done()
			_, errs[i] = svc.Projects.UpdateBillingInfo("projects/"+project,
				&cloudbilling.ProjectBillingInfo{BillingAccountName: acct}).Context(ctx).Do()
		}(i, acct)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoErrorf(t, err, "concurrent update with %s", accounts[i])
	}

	got := billingInfo(ctx, t, svc, project)
	require.Contains(t, accounts, got.BillingAccountName, "the final association must be one whole written value")
	require.True(t, got.BillingEnabled)
	require.Equal(t, "projects/"+project+"/billingInfo", got.Name)
}
