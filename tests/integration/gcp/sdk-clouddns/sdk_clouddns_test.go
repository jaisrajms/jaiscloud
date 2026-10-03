// Package sdk_clouddns_test exercises the jaiscloud-gcp emulator's Cloud DNS
// control plane (dns.googleapis.com/dns/v1) through the official Google REST
// apiary client. This validates wire-level parity with the real SDK: managed
// zone CRUD, resource record set create/list/delete, the change records that
// apply rrset additions/deletions, and the synthesized project/quota record.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_clouddns_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dns "google.golang.org/api/dns/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func endpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080/"
}

func projectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "proj"
}

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestSDKCloudDNS(t *testing.T) {
	ctx := context.Background()
	svc, err := dns.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	zoneName := unique("zone")
	dnsName := zoneName + ".example.com."

	// --- project + quota ---
	proj, err := svc.Projects.Get(project).Do()
	require.NoError(t, err)
	require.Equal(t, "dns#project", proj.Kind)
	require.Equal(t, project, proj.Id)
	require.NotZero(t, proj.Number)
	require.NotNil(t, proj.Quota)
	require.NotZero(t, proj.Quota.ManagedZones)

	// --- managed zone create/get/list ---
	created, err := svc.ManagedZones.Create(project, &dns.ManagedZone{
		Name:        zoneName,
		DnsName:     dnsName,
		Description: "sdk test zone",
		Labels:      map[string]string{"env": "test"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "dns#managedZone", created.Kind)
	require.Equal(t, zoneName, created.Name)
	require.Equal(t, dnsName, created.DnsName)
	require.Equal(t, "public", created.Visibility)
	require.NotZero(t, created.Id)
	require.NotEmpty(t, created.NameServers)
	require.NotEmpty(t, created.CreationTime)

	got, err := svc.ManagedZones.Get(project, zoneName).Do()
	require.NoError(t, err)
	require.Equal(t, created.Id, got.Id, "id must be stable across reads")

	list, err := svc.ManagedZones.List(project).Do()
	require.NoError(t, err)
	require.Equal(t, "dns#managedZonesListResponse", list.Kind)
	require.NotEmpty(t, list.ManagedZones)

	// A duplicate zone is rejected.
	_, err = svc.ManagedZones.Create(project, &dns.ManagedZone{
		Name:    zoneName,
		DnsName: dnsName,
	}).Do()
	require.Error(t, err, "duplicate managed zone must be rejected")
	var dupErr *googleapi.Error
	require.ErrorAs(t, err, &dupErr)
	require.Equal(t, 409, dupErr.Code)

	// --- resource record sets ---
	rrName := "www." + dnsName
	rr, err := svc.ResourceRecordSets.Create(project, zoneName, &dns.ResourceRecordSet{
		Name:    rrName,
		Type:    "A",
		Ttl:     300,
		Rrdatas: []string{"1.2.3.4"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "dns#resourceRecordSet", rr.Kind)
	require.Equal(t, rrName, rr.Name)
	require.Equal(t, int64(300), rr.Ttl)

	rrs, err := svc.ResourceRecordSets.List(project, zoneName).Do()
	require.NoError(t, err)
	require.Equal(t, "dns#resourceRecordSetsListResponse", rrs.Kind)
	require.Len(t, rrs.Rrsets, 1)

	// --- change applies additions + deletions ---
	change, err := svc.Changes.Create(project, zoneName, &dns.Change{
		Additions: []*dns.ResourceRecordSet{
			{Name: "mail." + dnsName, Type: "MX", Ttl: 120, Rrdatas: []string{"10 mail." + dnsName}},
		},
		Deletions: []*dns.ResourceRecordSet{
			{Name: rrName, Type: "A", Ttl: 300, Rrdatas: []string{"1.2.3.4"}},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "dns#change", change.Kind)
	require.Equal(t, "done", change.Status)
	require.NotEmpty(t, change.Id)

	gotChange, err := svc.Changes.Get(project, zoneName, change.Id).Do()
	require.NoError(t, err)
	require.Equal(t, change.Id, gotChange.Id)

	changes, err := svc.Changes.List(project, zoneName).Do()
	require.NoError(t, err)
	require.Equal(t, "dns#changesListResponse", changes.Kind)
	require.Len(t, changes.Changes, 1)

	after, err := svc.ResourceRecordSets.List(project, zoneName).Do()
	require.NoError(t, err)
	require.Len(t, after.Rrsets, 1)
	require.Equal(t, "mail."+dnsName, after.Rrsets[0].Name)

	// Delete the surviving rrset via the name/type path form.
	_, err = svc.ResourceRecordSets.Delete(project, zoneName, "mail."+dnsName, "MX").Do()
	require.NoError(t, err)
	empty, err := svc.ResourceRecordSets.List(project, zoneName).Do()
	require.NoError(t, err)
	require.Empty(t, empty.Rrsets)

	// --- zone delete ---
	err = svc.ManagedZones.Delete(project, zoneName).Do()
	require.NoError(t, err)
	_, err = svc.ManagedZones.Get(project, zoneName).Do()
	require.Error(t, err, "a deleted zone must not be readable")
	require.ErrorAs(t, err, &dupErr)
	require.Equal(t, 404, dupErr.Code)
}
