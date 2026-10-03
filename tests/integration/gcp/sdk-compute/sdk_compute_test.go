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
	"fmt"
	"os"
	"strings"
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
