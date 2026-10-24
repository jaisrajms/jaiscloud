// Package sdk_compute_test exercises the jaiscloud-gcp emulator's Compute
// Engine control plane (compute.googleapis.com/compute/v1) through the official
// Google compute/v1 client. This validates wire-level parity with the real SDK:
// zonal instances (insert/get/list/stop/start/delete), disks, global
// networks/firewalls, regional subnetworks, the synthesized zones/regions/
// machineTypes discovery surface, the persisted compute#operation envelope with
// status DONE, list envelopes, scope isolation, and the AlreadyExists/NotFound
// error codes.
//
// The compute client's BasePath includes the service path, so the endpoint is
// the emulator base plus "/compute/v1/". The GCP_EMULATOR_ENDPOINT env var is
// honored and the "/compute/v1/" suffix appended when absent.
//
// Run with the GCP binary running:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_compute_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func endpoint() string {
	e := os.Getenv("GCP_EMULATOR_ENDPOINT")
	if e == "" {
		e = "http://localhost:8080/"
	}
	if !strings.HasSuffix(e, "/compute/v1/") {
		e = strings.TrimRight(e, "/") + "/compute/v1/"
	}
	return e
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

func TestSDKCompute(t *testing.T) {
	ctx := context.Background()
	svc, err := compute.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	zone := "us-central1-a"
	otherZone := "us-central1-b"
	instance := unique("vm")

	// --- instance insert -> get -> list ---
	insertOp, err := svc.Instances.Insert(project, zone, &compute.Instance{
		Name:        instance,
		MachineType: "e2-micro",
		Disks: []*compute.AttachedDisk{{
			Boot: true,
			InitializeParams: &compute.AttachedDiskInitializeParams{
				SourceImage: "projects/debian-cloud/global/images/debian-12-bookworm-v20240101",
			},
		}},
		NetworkInterfaces: []*compute.NetworkInterface{{Network: "global/networks/default"}},
		Labels:            map[string]string{"env": "test"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#operation", insertOp.Kind)
	require.Equal(t, "DONE", insertOp.Status)
	require.Equal(t, "insert", insertOp.OperationType)
	require.NotEmpty(t, insertOp.Name)
	require.NotEmpty(t, insertOp.TargetLink)

	got, err := svc.Instances.Get(project, zone, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#instance", got.Kind)
	require.Equal(t, "RUNNING", got.Status)
	require.Equal(t, instance, got.Name)
	require.Contains(t, got.Zone, "zones/"+zone)
	require.Contains(t, got.MachineType, "machineTypes/e2-micro")
	require.NotEmpty(t, got.SelfLink)
	require.NotEmpty(t, got.CpuPlatform)
	require.NotEmpty(t, got.NetworkInterfaces)
	require.NotEmpty(t, got.Disks)

	list, err := svc.Instances.List(project, zone).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#instanceList", list.Kind)
	require.NotEmpty(t, list.Items)

	// A duplicate instance is rejected.
	_, err = svc.Instances.Insert(project, zone, &compute.Instance{Name: instance}).Do()
	require.Error(t, err, "duplicate instance must be rejected")
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 409, apiErr.Code)

	// --- stop -> start -> reset ---
	stopOp, err := svc.Instances.Stop(project, zone, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "stop", stopOp.OperationType)
	require.Equal(t, "DONE", stopOp.Status)
	stopped, err := svc.Instances.Get(project, zone, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "TERMINATED", stopped.Status)

	startOp, err := svc.Instances.Start(project, zone, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "start", startOp.OperationType)
	running, err := svc.Instances.Get(project, zone, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "RUNNING", running.Status)

	resetOp, err := svc.Instances.Reset(project, zone, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "reset", resetOp.OperationType)
	require.Equal(t, "DONE", resetOp.Status)

	// --- scoping: the same name in another zone is a distinct instance ---
	_, err = svc.Instances.Get(project, otherZone, instance).Do()
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)

	otherInsertOp, err := svc.Instances.Insert(project, otherZone, &compute.Instance{Name: instance}).Do()
	require.NoError(t, err)
	require.Equal(t, "DONE", otherInsertOp.Status)
	inOther, err := svc.Instances.Get(project, otherZone, instance).Do()
	require.NoError(t, err)
	require.Contains(t, inOther.Zone, "zones/"+otherZone)

	// --- operations get/list (zonal) ---
	fetchedOp, err := svc.ZoneOperations.Get(project, zone, insertOp.Name).Do()
	require.NoError(t, err)
	require.Equal(t, insertOp.Name, fetchedOp.Name)
	require.Equal(t, "DONE", fetchedOp.Status)
	require.Equal(t, int64(100), fetchedOp.Progress)
	require.NotEmpty(t, fetchedOp.InsertTime)

	zoneOps, err := svc.ZoneOperations.List(project, zone).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#operationList", zoneOps.Kind)
	require.NotEmpty(t, zoneOps.Items)

	// --- aggregated instance list ---
	agg, err := svc.Instances.AggregatedList(project).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#instanceAggregatedList", agg.Kind)
	require.Contains(t, agg.Items, "zones/"+zone)
	require.Contains(t, agg.Items, "zones/"+otherZone)

	// --- disks ---
	disk := unique("disk")
	diskOp, err := svc.Disks.Insert(project, zone, &compute.Disk{
		Name:   disk,
		SizeGb: 20,
		Type:   "pd-ssd",
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "insert", diskOp.OperationType)
	gotDisk, err := svc.Disks.Get(project, zone, disk).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#disk", gotDisk.Kind)
	require.Equal(t, "READY", gotDisk.Status)
	require.Equal(t, int64(20), gotDisk.SizeGb)
	disks, err := svc.Disks.List(project, zone).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#diskList", disks.Kind)
	require.NotEmpty(t, disks.Items)
	_, err = svc.Disks.Delete(project, zone, disk).Do()
	require.NoError(t, err)
	_, err = svc.Disks.Get(project, zone, disk).Do()
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)

	// --- networks (global) ---
	network := unique("net")
	netOp, err := svc.Networks.Insert(project, &compute.Network{
		Name:                  network,
		AutoCreateSubnetworks: false,
		ForceSendFields:       []string{"AutoCreateSubnetworks"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#operation", netOp.Kind)
	require.Contains(t, netOp.SelfLink, "global/operations/")
	gotNet, err := svc.Networks.Get(project, network).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#network", gotNet.Kind)
	require.False(t, gotNet.AutoCreateSubnetworks)
	nets, err := svc.Networks.List(project).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#networkList", nets.Kind)
	require.NotEmpty(t, nets.Items)
	_, err = svc.Networks.Delete(project, network).Do()
	require.NoError(t, err)
	_, err = svc.Networks.Get(project, network).Do()
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)

	// --- firewalls (global) ---
	firewall := unique("fw")
	fwOp, err := svc.Firewalls.Insert(project, &compute.Firewall{
		Name:         firewall,
		Network:      "default",
		SourceRanges: []string{"0.0.0.0/0"},
		Allowed:      []*compute.FirewallAllowed{{IPProtocol: "tcp", Ports: []string{"22"}}},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "insert", fwOp.OperationType)
	gotFW, err := svc.Firewalls.Get(project, firewall).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#firewall", gotFW.Kind)
	require.Contains(t, gotFW.Network, "global/networks/default")
	require.NotEmpty(t, gotFW.Allowed)
	fws, err := svc.Firewalls.List(project).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#firewallList", fws.Kind)
	require.NotEmpty(t, fws.Items)
	_, err = svc.Firewalls.Delete(project, firewall).Do()
	require.NoError(t, err)
	_, err = svc.Firewalls.Get(project, firewall).Do()
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)

	// --- subnetworks (regional) ---
	subnet := unique("sub")
	region := "us-central1"
	subOp, err := svc.Subnetworks.Insert(project, region, &compute.Subnetwork{
		Name:        subnet,
		Network:     "default",
		IpCidrRange: "10.1.0.0/20",
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "insert", subOp.OperationType)
	gotSub, err := svc.Subnetworks.Get(project, region, subnet).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#subnetwork", gotSub.Kind)
	require.Equal(t, "10.1.0.0/20", gotSub.IpCidrRange)
	require.Contains(t, gotSub.Region, "regions/"+region)
	subs, err := svc.Subnetworks.List(project, region).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#subnetworkList", subs.Kind)
	require.NotEmpty(t, subs.Items)
	_, err = svc.Subnetworks.Delete(project, region, subnet).Do()
	require.NoError(t, err)
	_, err = svc.Subnetworks.Get(project, region, subnet).Do()
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)

	// --- machineTypes / zones / regions discovery ---
	mts, err := svc.MachineTypes.List(project, zone).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#machineTypeList", mts.Kind)
	require.NotEmpty(t, mts.Items)
	mt, err := svc.MachineTypes.Get(project, zone, "e2-micro").Do()
	require.NoError(t, err)
	require.Equal(t, "compute#machineType", mt.Kind)
	require.Equal(t, int64(2), mt.GuestCpus)

	zones, err := svc.Zones.List(project).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#zoneList", zones.Kind)
	require.NotEmpty(t, zones.Items)
	gotZone, err := svc.Zones.Get(project, zone).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#zone", gotZone.Kind)
	require.Equal(t, "UP", gotZone.Status)

	regions, err := svc.Regions.List(project).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#regionList", regions.Kind)
	require.NotEmpty(t, regions.Items)
	gotRegion, err := svc.Regions.Get(project, region).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#region", gotRegion.Kind)
	require.NotEmpty(t, gotRegion.Zones)

	// --- deferred surfaces fail loud with 501, not 404 ---
	_, err = svc.GlobalAddresses.List(project).Do()
	require.Error(t, err, "deferred surface must fail")
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 501, apiErr.Code)

	// --- instance delete ---
	delOp, err := svc.Instances.Delete(project, zone, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "delete", delOp.OperationType)
	require.Equal(t, "DONE", delOp.Status)
	_, err = svc.Instances.Get(project, zone, instance).Do()
	require.Error(t, err, "a deleted instance must not be readable")
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)
}

// requireAPICode asserts err is a googleapi.Error with the given HTTP status.
func requireAPICode(t *testing.T, err error, code int) {
	t.Helper()
	require.Error(t, err)
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.Code, "unexpected API error: %v", err)
}

// requireErrorEnvelope also asserts the GCP error envelope body carries the
// matching google.rpc status name (e.g. INVALID_ARGUMENT, NOT_FOUND).
func requireErrorEnvelope(t *testing.T, err error, status string, code int) {
	t.Helper()
	requireAPICode(t, err, code)
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.NotEmpty(t, apiErr.Body, "error body must carry the GCP envelope")
	var env struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(apiErr.Body), &env))
	require.Equal(t, code, env.Error.Code)
	require.Equal(t, status, env.Error.Status)
	require.NotEmpty(t, env.Error.Message)
}

func instanceTemplate(name string) *compute.Instance {
	return &compute.Instance{Name: name, MachineType: "e2-micro"}
}

// randomUUID mints a v4 UUID for a requestId token, so repeated runs never
// replay an earlier run's operation.
func randomUUID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	_, err := rand.Read(b[:])
	require.NoError(t, err)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// TestSDKComputeConcurrency is the p2 class: concurrent writers. The test client
// runs under -race; the emulator is a separate process, so the assertions here
// are the conflict semantics, not server-side race detection. A same-name insert
// race must produce exactly one success and N-1 conflicts, and the same is true
// of a delete race; distinct-name churn must lose no writer.
func TestSDKComputeConcurrency(t *testing.T) {
	ctx := context.Background()
	svc, err := compute.NewService(ctx, opts()...)
	require.NoError(t, err)
	project, zone := projectID(), "us-east1-c"

	const racers = 8

	// Same-name concurrent insert -> exactly one winner, the rest 409.
	name := unique("race-vm")
	var wg sync.WaitGroup
	errs := make([]error, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = svc.Instances.Insert(project, zone, instanceTemplate(name)).Do()
		}(i)
	}
	wg.Wait()
	_, conflicts := tallyResults(t, errs, 409)
	require.Equal(t, racers-1, conflicts, "a same-name insert race must conflict the losers")

	// Exactly one instance was created.
	after, err := svc.Instances.List(project, zone).Filter("name = " + name).Do()
	require.NoError(t, err)
	require.Len(t, after.Items, 1)

	// Same-name concurrent delete -> exactly one winner, the rest 404.
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = svc.Instances.Delete(project, zone, name).Do()
		}(i)
	}
	wg.Wait()
	_, notFound := tallyResults(t, errs, 404)
	require.Equal(t, racers-1, notFound, "a same-name delete race must 404 the losers")

	// Same-name concurrent disk and firewall inserts conflict the same way.
	disk := unique("race-disk")
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = svc.Disks.Insert(project, zone, &compute.Disk{Name: disk, SizeGb: 10}).Do()
		}(i)
	}
	wg.Wait()
	successes, conflicts := tallyResults(t, errs, 409)
	require.Equal(t, 1, successes, "exactly one disk insert must win")
	require.Equal(t, racers-1, conflicts)

	fw := unique("race-fw")
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = svc.Firewalls.Insert(project, &compute.Firewall{
				Name: fw, Network: "default", SourceRanges: []string{"0.0.0.0/0"},
				Allowed: []*compute.FirewallAllowed{{IPProtocol: "tcp", Ports: []string{"22"}}},
			}).Do()
		}(i)
	}
	wg.Wait()
	successes, conflicts = tallyResults(t, errs, 409)
	require.Equal(t, 1, successes, "exactly one firewall insert must win")
	require.Equal(t, racers-1, conflicts)

	// Distinct-name churn: every writer must win, with no data race.
	distinct := make([]string, racers)
	for i := 0; i < racers; i++ {
		distinct[i] = fmt.Sprintf("%s-%d", unique("churn"), i)
	}
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = svc.Instances.Insert(project, zone, instanceTemplate(distinct[i])).Do()
		}(i)
	}
	wg.Wait()
	successes, _ = tallyResults(t, errs, 0)
	require.Equal(t, racers, successes, "distinct-name inserts must all succeed")

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = svc.Instances.Delete(project, zone, distinct[i]).Do()
		}(i)
	}
	wg.Wait()
	successes, _ = tallyResults(t, errs, 0)
	require.Equal(t, racers, successes, "distinct-name deletes must all succeed")
}

// tallyResults counts successes (err == nil) and errors carrying wantCode.
func tallyResults(t *testing.T, errs []error, wantCode int) (successes, coded int) {
	t.Helper()
	for _, e := range errs {
		if e == nil {
			successes++
			continue
		}
		requireAPICode(t, e, wantCode)
		coded++
	}
	return successes, coded
}

// TestSDKComputeErrorSemantics is the p3 class: real error envelopes, 400/404/409.
func TestSDKComputeErrorSemantics(t *testing.T) {
	ctx := context.Background()
	svc, err := compute.NewService(ctx, opts()...)
	require.NoError(t, err)
	project, zone := projectID(), "us-east4-a"

	// An unknown machine type is rejected with 400 (not silently accepted).
	_, err = svc.Instances.Insert(project, zone, &compute.Instance{
		Name: unique("vm"), MachineType: "bogus-type",
	}).Do()
	requireErrorEnvelope(t, err, "INVALID_ARGUMENT", 400)

	// A malformed resource name is rejected with 400.
	_, err = svc.Instances.Insert(project, zone, &compute.Instance{
		Name: "Bad_Name", MachineType: "e2-micro",
	}).Do()
	requireErrorEnvelope(t, err, "INVALID_ARGUMENT", 400)

	// A machine type outside the advertised catalog but real (family/size or
	// custom) is accepted.
	_, err = svc.Instances.Insert(project, zone, &compute.Instance{
		Name: unique("vm"), MachineType: "e2-standard-4",
	}).Do()
	require.NoError(t, err)

	// Duplicate insert -> 409.
	dup := unique("vm")
	_, err = svc.Instances.Insert(project, zone, instanceTemplate(dup)).Do()
	require.NoError(t, err)
	_, err = svc.Instances.Insert(project, zone, instanceTemplate(dup)).Do()
	requireErrorEnvelope(t, err, "ALREADY_EXISTS", 409)

	// start/stop on a missing instance -> 404.
	_, err = svc.Instances.Start(project, zone, unique("absent")).Do()
	requireErrorEnvelope(t, err, "NOT_FOUND", 404)
	_, err = svc.Instances.Stop(project, zone, unique("absent")).Do()
	requireAPICode(t, err, 404)

	// get-after-delete -> 404 for every resource kind.
	for _, tc := range []struct {
		label string
		do    func(name string) error
	}{
		{"instance", func(n string) error {
			if _, err := svc.Instances.Insert(project, zone, instanceTemplate(n)).Do(); err != nil {
				return err
			}
			if _, err := svc.Instances.Delete(project, zone, n).Do(); err != nil {
				return err
			}
			_, err := svc.Instances.Get(project, zone, n).Do()
			return err
		}},
		{"disk", func(n string) error {
			if _, err := svc.Disks.Insert(project, zone, &compute.Disk{Name: n, SizeGb: 10}).Do(); err != nil {
				return err
			}
			if _, err := svc.Disks.Delete(project, zone, n).Do(); err != nil {
				return err
			}
			_, err := svc.Disks.Get(project, zone, n).Do()
			return err
		}},
		{"network", func(n string) error {
			if _, err := svc.Networks.Insert(project, &compute.Network{Name: n, AutoCreateSubnetworks: false}).Do(); err != nil {
				return err
			}
			if _, err := svc.Networks.Delete(project, n).Do(); err != nil {
				return err
			}
			_, err := svc.Networks.Get(project, n).Do()
			return err
		}},
		{"firewall", func(n string) error {
			if _, err := svc.Firewalls.Insert(project, &compute.Firewall{Name: n, Network: "default"}).Do(); err != nil {
				return err
			}
			if _, err := svc.Firewalls.Delete(project, n).Do(); err != nil {
				return err
			}
			_, err := svc.Firewalls.Get(project, n).Do()
			return err
		}},
		{"subnetwork", func(n string) error {
			if _, err := svc.Subnetworks.Insert(project, "us-east4", &compute.Subnetwork{Name: n, Network: "default", IpCidrRange: "10.4.0.0/20"}).Do(); err != nil {
				return err
			}
			if _, err := svc.Subnetworks.Delete(project, "us-east4", n).Do(); err != nil {
				return err
			}
			_, err := svc.Subnetworks.Get(project, "us-east4", n).Do()
			return err
		}},
	} {
		requireAPICode(t, tc.do(unique("e-"+tc.label)), 404)
	}
}

// TestSDKComputePagingAndFilter is the p4 class: maxResults/pageToken walks
// composed with the documented filter subset.
func TestSDKComputePagingAndFilter(t *testing.T) {
	ctx := context.Background()
	svc, err := compute.NewService(ctx, opts()...)
	require.NoError(t, err)
	project, zone := projectID(), "us-west1-b"

	prefix := unique("page-vm")
	want := map[string]bool{}
	for i := 0; i < 5; i++ {
		n := fmt.Sprintf("%s-%d", prefix, i)
		_, err := svc.Instances.Insert(project, zone, instanceTemplate(n)).Do()
		require.NoError(t, err)
		want[n] = true
	}

	// Walk the zone listing two at a time; no duplicates, every instance seen,
	// pages continue until the token is empty.
	seen := map[string]bool{}
	token, pages := "", 0
	for {
		call := svc.Instances.List(project, zone).MaxResults(2)
		if token != "" {
			call = call.PageToken(token)
		}
		page, err := call.Do()
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Items), 2)
		for _, it := range page.Items {
			require.False(t, seen[it.Name], "instance %q returned twice", it.Name)
			seen[it.Name] = true
		}
		pages++
		token = page.NextPageToken
		if token == "" {
			break
		}
		require.Less(t, pages, 100, "paging did not terminate")
	}
	for n := range want {
		require.True(t, seen[n], "instance %q missing from the paged listing", n)
	}
	require.Greater(t, pages, 1, "5 instances at maxResults=2 must span more than one page")

	// Filter equality is composed with paging.
	first := fmt.Sprintf("%s-0", prefix)
	list, err := svc.Instances.List(project, zone).Filter("name = " + first).Do()
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	require.Equal(t, first, list.Items[0].Name)

	// Filter negates too: none of the returned items is the excluded one.
	list, err = svc.Instances.List(project, zone).Filter("name != " + first).Do()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(list.Items), 4)
	for _, it := range list.Items {
		require.NotEqual(t, first, it.Name)
	}

	// A filter outside the documented subset fails loud.
	_, err = svc.Instances.List(project, zone).Filter("name > x").Do()
	requireAPICode(t, err, 400)

	// The aggregated listing pages the flattened set and re-groups by zone.
	aggSeen := map[string]bool{}
	token = ""
	for {
		call := svc.Instances.AggregatedList(project).MaxResults(3)
		if token != "" {
			call = call.PageToken(token)
		}
		page, err := call.Do()
		require.NoError(t, err)
		count := 0
		for _, group := range page.Items {
			for _, it := range group.Instances {
				count++
				require.False(t, aggSeen[it.Name], "instance %q returned twice across pages", it.Name)
				aggSeen[it.Name] = true
			}
		}
		require.LessOrEqual(t, count, 3)
		pages++
		token = page.NextPageToken
		if token == "" {
			break
		}
		require.Less(t, pages, 200, "aggregated paging did not terminate")
	}
	for n := range want {
		require.True(t, aggSeen[n], "instance %q missing from the aggregated listing", n)
	}
}

// TestSDKComputeMaxResultsBindingMessage pins the negative maxResults binding an
// official client can emit (the generated MaxResults field is an int64): a 400
// INVALID_ARGUMENT whose message names the `header` message's field, which is
// where Compute's frontend binds its query parameters (DC68).
func TestSDKComputeMaxResultsBindingMessage(t *testing.T) {
	ctx := context.Background()
	svc, err := compute.NewService(ctx, opts()...)
	require.NoError(t, err)
	project, zone := projectID(), "us-west1-b"

	_, err = svc.Instances.List(project, zone).MaxResults(-1).Do()
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 400, apiErr.Code)
	require.Equal(t, `Invalid value at 'header.max_results' (TYPE_UINT32), "-1"`, apiErr.Message)
}

// TestSDKComputeOperationsLifecycle is the p5 class: the operation-polling
// surface (zonal + global) and a many-resource listing.
func TestSDKComputeOperationsLifecycle(t *testing.T) {
	ctx := context.Background()
	svc, err := compute.NewService(ctx, opts()...)
	require.NoError(t, err)
	project := projectID()

	// A zonal mutation's operation is retrievable by name and listed at scope.
	zone := "us-west1-a"
	insOp, err := svc.Instances.Insert(project, zone, instanceTemplate(unique("op-vm"))).Do()
	require.NoError(t, err)
	require.Equal(t, "compute#operation", insOp.Kind)
	require.Equal(t, "DONE", insOp.Status)
	require.Equal(t, int64(100), insOp.Progress)
	require.NotEmpty(t, insOp.InsertTime)
	require.NotEmpty(t, insOp.EndTime)

	got, err := svc.ZoneOperations.Get(project, zone, insOp.Name).Do()
	require.NoError(t, err)
	require.Equal(t, insOp.Name, got.Name)
	require.Equal(t, "DONE", got.Status)
	require.Contains(t, got.SelfLink, "zones/"+zone+"/operations/")

	listed, err := svc.ZoneOperations.List(project, zone).Filter("name = " + insOp.Name).Do()
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)

	// A global mutation's operation lives at global scope.
	netOp, err := svc.Networks.Insert(project, &compute.Network{
		Name: unique("op-net"), AutoCreateSubnetworks: false, ForceSendFields: []string{"AutoCreateSubnetworks"},
	}).Do()
	require.NoError(t, err)
	globalGot, err := svc.GlobalOperations.Get(project, netOp.Name).Do()
	require.NoError(t, err)
	require.Equal(t, "DONE", globalGot.Status)
	require.Contains(t, globalGot.SelfLink, "global/operations/")

	globalList, err := svc.GlobalOperations.List(project).Filter("name = " + netOp.Name).Do()
	require.NoError(t, err)
	require.Len(t, globalList.Items, 1)

	// Many-resource listing: several instances across two zones all show up in
	// the aggregated listing, grouped by zone.
	zoneB := "europe-west1-b"
	names := map[string]bool{}
	for i := 0; i < 3; i++ {
		n := fmt.Sprintf("%s-%d", unique("many"), i)
		_, err := svc.Instances.Insert(project, zoneB, instanceTemplate(n)).Do()
		require.NoError(t, err)
		names[n] = true
	}
	agg, err := svc.Instances.AggregatedList(project).MaxResults(500).Do()
	require.NoError(t, err)
	require.Contains(t, agg.Items, "zones/"+zoneB)
	for _, it := range agg.Items["zones/"+zoneB].Instances {
		delete(names, it.Name)
	}
	require.Empty(t, names, "every created instance must appear in the aggregated listing")
}

// TestSDKComputeRequestIdIdempotency is the TD18 case: the documented
// requestId idempotency token on instances.insert/disks.insert. The first
// insert echoes the token back as the operation's clientOperationId; a retry
// carrying the same token is ignored and replays the original operation (no
// duplicate resource, no 409); a malformed or zero-UUID token fails loud 400.
func TestSDKComputeRequestIdIdempotency(t *testing.T) {
	ctx := context.Background()
	svc, err := compute.NewService(ctx, opts()...)
	require.NoError(t, err)
	project, zone := projectID(), "us-central1-c"

	// instances.insert: the token is echoed, and a retry replays the operation.
	token := randomUUID(t)
	name := unique("vm-idem")
	first, err := svc.Instances.Insert(project, zone, instanceTemplate(name)).RequestId(token).Do()
	require.NoError(t, err)
	require.Equal(t, token, first.ClientOperationId, "operation must echo the requestId")

	retry, err := svc.Instances.Insert(project, zone, instanceTemplate(name)).RequestId(token).Do()
	require.NoError(t, err, "a retry with the same requestId must be ignored, not 409")
	require.Equal(t, first.Name, retry.Name, "retry must replay the original operation")
	require.Equal(t, token, retry.ClientOperationId)

	// The retry created no second instance.
	list, err := svc.Instances.List(project, zone).Filter("name = " + name).Do()
	require.NoError(t, err)
	require.Len(t, list.Items, 1, "a retried insert must not duplicate the instance")

	// operations.get resolves the replayed operation with the token echoed.
	op, err := svc.ZoneOperations.Get(project, zone, first.Name).Do()
	require.NoError(t, err)
	require.Equal(t, token, op.ClientOperationId)

	// disks.insert has the same contract.
	dtoken := randomUUID(t)
	dname := unique("disk-idem")
	dfirst, err := svc.Disks.Insert(project, zone, &compute.Disk{Name: dname, SizeGb: 10}).RequestId(dtoken).Do()
	require.NoError(t, err)
	require.Equal(t, dtoken, dfirst.ClientOperationId)
	dretry, err := svc.Disks.Insert(project, zone, &compute.Disk{Name: dname, SizeGb: 10}).RequestId(dtoken).Do()
	require.NoError(t, err)
	require.Equal(t, dfirst.Name, dretry.Name)

	// instances.delete: a token retry replays the delete operation instead of
	// 404-ing on the already-deleted instance.
	delToken := randomUUID(t)
	delOp, err := svc.Instances.Delete(project, zone, name).RequestId(delToken).Do()
	require.NoError(t, err)
	require.Equal(t, delToken, delOp.ClientOperationId)
	delRetry, err := svc.Instances.Delete(project, zone, name).RequestId(delToken).Do()
	require.NoError(t, err, "a delete retry with the same token must replay, not 404")
	require.Equal(t, delOp.Name, delRetry.Name)

	// A malformed token and the unsupported zero UUID are rejected 400.
	_, err = svc.Instances.Insert(project, zone, instanceTemplate(unique("vm-bad"))).RequestId("not-a-uuid").Do()
	requireErrorEnvelope(t, err, "INVALID_ARGUMENT", 400)
	_, err = svc.Instances.Insert(project, zone, instanceTemplate(unique("vm-zero"))).RequestId("00000000-0000-0000-0000-000000000000").Do()
	requireErrorEnvelope(t, err, "INVALID_ARGUMENT", 400)
}

// TestSDKComputeRequestIdConcurrentRetryReplays is the in-flight case the token
// exists for: several retries arriving at once must all replay the one original
// operation — none may be answered with a 409 conflict — and exactly one
// instance must exist.
func TestSDKComputeRequestIdConcurrentRetryReplays(t *testing.T) {
	ctx := context.Background()
	svc, err := compute.NewService(ctx, opts()...)
	require.NoError(t, err)
	project, zone := projectID(), "us-west1-c"

	const workers = 8
	token := randomUUID(t)
	name := unique("vm-idem-race")
	ops := make([]string, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			op, err := svc.Instances.Insert(project, zone, instanceTemplate(name)).RequestId(token).Do()
			errs[i] = err
			if err == nil {
				ops[i] = op.Name
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "same-token insert %d must replay, not conflict", i)
		require.Equal(t, ops[0], ops[i], "every retry must return the original operation")
	}

	list, err := svc.Instances.List(project, zone).Filter("name = " + name).Do()
	require.NoError(t, err)
	require.Len(t, list.Items, 1, "concurrent same-token inserts must not duplicate the instance")
}
