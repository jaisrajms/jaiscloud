// This file deepens the Cloud DNS behavioral coverage: managed-zone
// patch/update, private-visibility zones, resource-record-set get/patch,
// additions-only and deletions-only changes, list pagination, and the
// NotFound/InvalidArgument contracts.
package sdk_clouddns_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	dns "google.golang.org/api/dns/v1"
	"google.golang.org/api/googleapi"
)

func baseURL() string {
	return strings.TrimRight(endpoint(), "/")
}

// resetState wipes emulator state between tests (the suite shares one server).
func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(baseURL()+"/_jaiscloud/reset", "", nil) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func requireAPIError(t *testing.T, err error, code int) {
	t.Helper()
	require.Error(t, err)
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.Code)
}

// TestSDKCloudDNSManagedZoneMutations covers zone patch/update merge semantics,
// private visibility, and the zone-not-found contract.
func TestSDKCloudDNSManagedZoneMutations(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	svc, err := dns.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	zone := unique("zone")
	dnsName := zone + ".example.com."

	created, err := svc.ManagedZones.Create(project, &dns.ManagedZone{
		Name:        zone,
		DnsName:     dnsName,
		Description: "original",
		Labels:      map[string]string{"env": "test"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "public", created.Visibility)

	// Patch merges the mutable fields. Cloud DNS v1's managedZones.patch is
	// declared to return a dns#operation; the emulator returns the updated zone
	// instead — a known deferred fidelity gap, not accepted behavior. The merged
	// state is therefore asserted by reading the zone back.
	if _, err := svc.ManagedZones.Patch(project, zone, &dns.ManagedZone{
		Description: "patched",
		Visibility:  "private",
		Labels:      map[string]string{"env": "prod", "team": "dns"},
	}).Do(); err != nil {
		require.NoError(t, err)
	}

	got, err := svc.ManagedZones.Get(project, zone).Do()
	require.NoError(t, err)
	require.Equal(t, "patched", got.Description)
	require.Equal(t, "private", got.Visibility)
	require.Equal(t, map[string]string{"env": "prod", "team": "dns"}, got.Labels)
	require.Equal(t, created.Id, got.Id, "id must be stable across a patch")

	// Update (PUT) shares the merge semantics for the mutable fields.
	_, err = svc.ManagedZones.Update(project, zone, &dns.ManagedZone{
		Description: "updated",
	}).Do()
	require.NoError(t, err)
	got, err = svc.ManagedZones.Get(project, zone).Do()
	require.NoError(t, err)
	require.Equal(t, "updated", got.Description)
	require.Equal(t, "private", got.Visibility, "unset PUT fields must be retained")

	// An unknown zone is NotFound for get/patch/delete.
	_, err = svc.ManagedZones.Get(project, "zone-missing").Do()
	requireAPIError(t, err, 404)
	_, err = svc.ManagedZones.Patch(project, "zone-missing", &dns.ManagedZone{Description: "x"}).Do()
	requireAPIError(t, err, 404)
	err = svc.ManagedZones.Delete(project, "zone-missing").Do()
	requireAPIError(t, err, 404)
}

// TestSDKCloudDNSRecordSetMutations covers the record-set get/patch paths and
// the additions-only / deletions-only change forms, which the base suite does
// not exercise (it only drives a combined change).
func TestSDKCloudDNSRecordSetMutations(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	svc, err := dns.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	zone := unique("zone")
	dnsName := zone + ".example.com."
	_, err = svc.ManagedZones.Create(project, &dns.ManagedZone{Name: zone, DnsName: dnsName}).Do()
	require.NoError(t, err)

	// additions-only change: apply two record sets.
	addChange, err := svc.Changes.Create(project, zone, &dns.Change{
		Additions: []*dns.ResourceRecordSet{
			{Name: "a." + dnsName, Type: "A", Ttl: 300, Rrdatas: []string{"1.2.3.4"}},
			{Name: "txt." + dnsName, Type: "TXT", Ttl: 60, Rrdatas: []string{"\"v=spf1\""}},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "done", addChange.Status)
	require.True(t, addChange.IsServing)
	require.Empty(t, addChange.Deletions)

	rrs, err := svc.ResourceRecordSets.List(project, zone).Do()
	require.NoError(t, err)
	require.Len(t, rrs.Rrsets, 2)

	// resourceRecordSets.get resolves the {name}/{type} path.
	one, err := svc.ResourceRecordSets.Get(project, zone, "a."+dnsName, "A").Do()
	require.NoError(t, err)
	require.Equal(t, "a."+dnsName, one.Name)
	require.Equal(t, int64(300), one.Ttl)
	require.Equal(t, []string{"1.2.3.4"}, one.Rrdatas)

	// resourceRecordSets.patch updates ttl/rrdatas in place.
	patched, err := svc.ResourceRecordSets.Patch(project, zone, "a."+dnsName, "A", &dns.ResourceRecordSet{
		Name:    "a." + dnsName,
		Type:    "A",
		Ttl:     600,
		Rrdatas: []string{"5.6.7.8", "9.10.11.12"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, int64(600), patched.Ttl)
	require.Equal(t, []string{"5.6.7.8", "9.10.11.12"}, patched.Rrdatas)
	reGot, err := svc.ResourceRecordSets.Get(project, zone, "a."+dnsName, "A").Do()
	require.NoError(t, err)
	require.Equal(t, int64(600), reGot.Ttl)

	// deletions-only change: remove the TXT set.
	delChange, err := svc.Changes.Create(project, zone, &dns.Change{
		Deletions: []*dns.ResourceRecordSet{
			{Name: "txt." + dnsName, Type: "TXT", Ttl: 60, Rrdatas: []string{"\"v=spf1\""}},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "done", delChange.Status)
	require.Empty(t, delChange.Additions)

	rrs, err = svc.ResourceRecordSets.List(project, zone).Do()
	require.NoError(t, err)
	require.Len(t, rrs.Rrsets, 1)
	require.Equal(t, "a."+dnsName, rrs.Rrsets[0].Name)

	// The change history now holds both changes.
	changes, err := svc.Changes.List(project, zone).Do()
	require.NoError(t, err)
	require.Len(t, changes.Changes, 2)

	// Missing record set and change are NotFound; a record set in a missing zone
	// is NotFound too.
	_, err = svc.ResourceRecordSets.Get(project, zone, "nope."+dnsName, "A").Do()
	requireAPIError(t, err, 404)
	_, err = svc.Changes.Get(project, zone, "no-such-change").Do()
	requireAPIError(t, err, 404)
	_, err = svc.ResourceRecordSets.Create(project, "zone-missing", &dns.ResourceRecordSet{
		Name: "x.example.com.", Type: "A", Ttl: 1, Rrdatas: []string{"1.1.1.1"},
	}).Do()
	requireAPIError(t, err, 404)

	// A missing record set on delete is NotFound.
	_, err = svc.ResourceRecordSets.Delete(project, zone, "nope."+dnsName, "A").Do()
	requireAPIError(t, err, 404)
}

// TestSDKCloudDNSListPagination covers maxResults/pageToken cursor pagination
// on managed-zone listing.
func TestSDKCloudDNSListPagination(t *testing.T) {
	resetState(t)
	ctx := context.Background()
	svc, err := dns.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	names := []string{unique("zonea"), unique("zoneb"), unique("zonec")}
	for _, name := range names {
		_, err := svc.ManagedZones.Create(project, &dns.ManagedZone{
			Name:    name,
			DnsName: fmt.Sprintf("%s.example.com.", name),
		}).Do()
		require.NoError(t, err)
	}

	first, err := svc.ManagedZones.List(project).MaxResults(2).Do()
	require.NoError(t, err)
	require.Len(t, first.ManagedZones, 2)
	require.NotEmpty(t, first.NextPageToken)

	second, err := svc.ManagedZones.List(project).MaxResults(2).PageToken(first.NextPageToken).Do()
	require.NoError(t, err)
	require.Len(t, second.ManagedZones, 1)
	require.Empty(t, second.NextPageToken)

	seen := map[string]bool{}
	for _, z := range append(first.ManagedZones, second.ManagedZones...) {
		seen[z.Name] = true
	}
	for _, name := range names {
		require.True(t, seen[name], "paginated listing must cover %s", name)
	}
}
