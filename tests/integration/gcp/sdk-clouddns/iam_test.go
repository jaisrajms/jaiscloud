package sdk_clouddns_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	dns "google.golang.org/api/dns/v1"
	"google.golang.org/api/googleapi"
)

// TestSDKCloudDNSZoneIAM drives the managedZones google.iam.v1.IAMPolicy mixin
// through the official apiary client, the way the provider's
// google_dns_managed_zone_iam_* resources do: the empty default policy, the
// read-modify-write the provider performs (taking the etag from the read), the
// permission echo, the stale-etag ABORTED/409, the absent-zone 404 on every
// verb, and the policy being dropped with the zone.
func TestSDKCloudDNSZoneIAM(t *testing.T) {
	ctx := context.Background()
	svc, err := dns.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	zoneName := unique("iam-zone")
	zone, err := svc.ManagedZones.Create(project, &dns.ManagedZone{
		Name:    zoneName,
		DnsName: zoneName + ".example.com.",
	}).Context(ctx).Do()
	require.NoError(t, err)
	require.Equal(t, zoneName, zone.Name)

	resource := fmt.Sprintf("projects/%s/managedZones/%s", project, zoneName)

	// getIamPolicy on a fresh zone is the empty default policy.
	pol, err := svc.ManagedZones.GetIamPolicy(resource, &dns.GoogleIamV1GetIamPolicyRequest{}).Context(ctx).Do()
	require.NoError(t, err)
	require.Equal(t, int64(1), pol.Version)
	require.Equal(t, "ACAB", pol.Etag)
	require.Empty(t, pol.Bindings)

	// The provider's member-add path: read, append the member, write the policy
	// back with the etag it just read.
	pol.Bindings = append(pol.Bindings, &dns.GoogleIamV1Binding{
		Role:    "roles/dns.admin",
		Members: []string{"user:tf-dns@example.com"},
	})
	updated, err := svc.ManagedZones.SetIamPolicy(resource, &dns.GoogleIamV1SetIamPolicyRequest{Policy: pol}).Context(ctx).Do()
	require.NoError(t, err)
	require.NotEqual(t, "ACAB", updated.Etag)
	require.Len(t, updated.Bindings, 1)
	require.Equal(t, "roles/dns.admin", updated.Bindings[0].Role)
	require.Equal(t, []string{"user:tf-dns@example.com"}, updated.Bindings[0].Members)

	// A second read-modify-write with the stored etag converges: same policy in,
	// same etag out (this is what makes a Terraform re-plan report no changes).
	again, err := svc.ManagedZones.SetIamPolicy(resource, &dns.GoogleIamV1SetIamPolicyRequest{Policy: updated}).Context(ctx).Do()
	require.NoError(t, err)
	require.Equal(t, updated.Etag, again.Etag)

	// testIamPermissions grants the request (the emulator enforces no authz).
	perms, err := svc.ManagedZones.TestIamPermissions(resource, &dns.GoogleIamV1TestIamPermissionsRequest{
		Permissions: []string{"dns.managedZones.get", "dns.managedZones.update"},
	}).Context(ctx).Do()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"dns.managedZones.get", "dns.managedZones.update"}, perms.Permissions)

	// A stale etag is ABORTED on the wire (HTTP 409) and leaves the policy alone.
	_, err = svc.ManagedZones.SetIamPolicy(resource, &dns.GoogleIamV1SetIamPolicyRequest{
		Policy: &dns.GoogleIamV1Policy{
			Etag:     "stale",
			Bindings: []*dns.GoogleIamV1Binding{{Role: "roles/viewer", Members: []string{"user:stale@example.com"}}},
		},
	}).Context(ctx).Do()
	var gerr *googleapi.Error
	require.ErrorAs(t, err, &gerr)
	require.Equal(t, 409, gerr.Code)
	require.Contains(t, gerr.Body, "ABORTED")

	stored, err := svc.ManagedZones.GetIamPolicy(resource, &dns.GoogleIamV1GetIamPolicyRequest{}).Context(ctx).Do()
	require.NoError(t, err)
	require.Equal(t, updated.Etag, stored.Etag)
	require.Len(t, stored.Bindings, 1)

	// The zone must exist: every verb is NotFound for an absent zone, and no
	// policy row is created for it.
	missing := fmt.Sprintf("projects/%s/managedZones/missing-%s", project, zoneName)
	var missingErr *googleapi.Error
	for name, call := range map[string]func() error{
		"get": func() error {
			_, err := svc.ManagedZones.GetIamPolicy(missing, &dns.GoogleIamV1GetIamPolicyRequest{}).Context(ctx).Do()
			return err
		},
		"set": func() error {
			_, err := svc.ManagedZones.SetIamPolicy(missing, &dns.GoogleIamV1SetIamPolicyRequest{
				Policy: &dns.GoogleIamV1Policy{Bindings: []*dns.GoogleIamV1Binding{{Role: "roles/viewer", Members: []string{"user:x@example.com"}}}},
			}).Context(ctx).Do()
			return err
		},
		"test": func() error {
			_, err := svc.ManagedZones.TestIamPermissions(missing, &dns.GoogleIamV1TestIamPermissionsRequest{
				Permissions: []string{"dns.managedZones.get"},
			}).Context(ctx).Do()
			return err
		},
	} {
		missingErr = nil
		require.ErrorAs(t, call(), &missingErr, "%s on an absent zone", name)
		require.Equal(t, 404, missingErr.Code, "%s on an absent zone", name)
	}

	// The policy is bound to the zone's lifetime: after the delete it is gone.
	require.NoError(t, svc.ManagedZones.Delete(project, zoneName).Context(ctx).Do())
	_, err = svc.ManagedZones.GetIamPolicy(resource, &dns.GoogleIamV1GetIamPolicyRequest{}).Context(ctx).Do()
	var goneErr *googleapi.Error
	require.ErrorAs(t, err, &goneErr)
	require.Equal(t, 404, goneErr.Code)
}
