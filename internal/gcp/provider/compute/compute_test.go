package compute

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     params,
		ResourceID: resource.ResourceID("proj"),
	}
}

func newProvider() *Provider { return New(store.NewMemoryResourceStore()) }

func instanceBody(name string) map[string]any {
	return map[string]any{
		"name":        name,
		"machineType": "e2-micro",
		"disks": []any{map[string]any{
			"boot": true,
			"initializeParams": map[string]any{
				"sourceImage": "projects/debian-cloud/global/images/debian-12-bookworm-v20240101",
			},
		}},
		"networkInterfaces": []any{map[string]any{
			"network": "global/networks/default",
		}},
		"labels": map[string]any{"env": "test"},
	}
}

func zonal(zone string, extra map[string]any) map[string]any {
	p := map[string]any{"scope": zone, "scopeType": "zones", "zone": zone}
	for k, v := range extra {
		p[k] = v
	}
	return p
}

func regional(region string, extra map[string]any) map[string]any {
	p := map[string]any{"scope": region, "scopeType": "regions", "region": region}
	for k, v := range extra {
		p[k] = v
	}
	return p
}

func mustCreateInstance(t *testing.T, p *Provider, zone, name string) map[string]any {
	t.Helper()
	resp, err := p.InstancesInsert(context.Background(), newNR(zonal(zone, map[string]any{"body": instanceBody(name)})))
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	return resp.Data
}

func TestInstanceRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	op := mustCreateInstance(t, p, "us-central1-a", "vm-a")
	if op["kind"] != kindOperation {
		t.Fatalf("insert must return a %s operation, got %v", kindOperation, op["kind"])
	}
	if op["status"] != "DONE" {
		t.Fatalf("operation status = %v, want DONE", op["status"])
	}
	if op["operationType"] != "insert" {
		t.Errorf("operationType = %v, want insert", op["operationType"])
	}
	if op["progress"] != int64(100) {
		t.Errorf("progress = %v, want 100", op["progress"])
	}
	if _, ok := op["targetId"].(string); !ok {
		t.Errorf("targetId must be a numeric string, got %T", op["targetId"])
	}

	got, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if got.Data["status"] != "RUNNING" {
		t.Errorf("status = %v, want RUNNING", got.Data["status"])
	}
	if got.Data["kind"] != kindInstance {
		t.Errorf("kind = %v, want %s", got.Data["kind"], kindInstance)
	}
	if !strings.Contains(str(got.Data["zone"]), "zones/us-central1-a") {
		t.Errorf("zone = %v", got.Data["zone"])
	}
	if !strings.Contains(str(got.Data["machineType"]), "machineTypes/e2-micro") {
		t.Errorf("machineType = %v", got.Data["machineType"])
	}
	if _, ok := got.Data["id"].(string); !ok {
		t.Errorf("id must be a numeric string, got %T", got.Data["id"])
	}

	list, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if list.Data["kind"] != kindInstanceList {
		t.Errorf("list kind = %v", list.Data["kind"])
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(items))
	}

	// stop -> TERMINATED
	stopOp, err := p.InstancesStop(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if stopOp.Data["operationType"] != "stop" || stopOp.Data["status"] != "DONE" {
		t.Errorf("unexpected stop operation: %v", stopOp.Data)
	}
	stopped, _ := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if stopped.Data["status"] != "TERMINATED" {
		t.Errorf("status after stop = %v, want TERMINATED", stopped.Data["status"])
	}

	// start -> RUNNING
	startOp, err := p.InstancesStart(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if startOp.Data["operationType"] != "start" {
		t.Errorf("unexpected start operation: %v", startOp.Data)
	}
	started, _ := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if started.Data["status"] != "RUNNING" {
		t.Errorf("status after start = %v, want RUNNING", started.Data["status"])
	}

	if _, err := p.InstancesReset(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"}))); err != nil {
		t.Fatalf("reset: %v", err)
	}

	delOp, err := p.InstancesDelete(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if delOp.Data["operationType"] != "delete" {
		t.Errorf("unexpected delete operation: %v", delOp.Data)
	}
	if _, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

// TestInstanceMetadataFingerprintIsBase64 pins the wire format: Discovery
// declares Metadata.fingerprint as format:byte, so the emulator must emit
// base64-encoded bytes, not a variable-length hex digest.
func TestInstanceMetadataFingerprintIsBase64(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm-fp")

	got, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-fp"})))
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	md, ok := got.Data["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata = %#v, want an object", got.Data["metadata"])
	}
	fp, _ := md["fingerprint"].(string)
	if fp == "" {
		t.Fatal("metadata.fingerprint is empty")
	}
	if _, err := base64.StdEncoding.DecodeString(fp); err != nil {
		t.Fatalf("metadata.fingerprint %q is not valid base64 (format:byte): %v", fp, err)
	}
}

func TestZonalScopingDoesNotCollide(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm")
	mustCreateInstance(t, p, "us-central1-b", "vm")

	a, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm"})))
	if err != nil {
		t.Fatalf("get zone a: %v", err)
	}
	if !strings.Contains(str(a.Data["zone"]), "us-central1-a") {
		t.Errorf("zone a instance zone = %v", a.Data["zone"])
	}
	b, err := p.InstancesGet(ctx, newNR(zonal("us-central1-b", map[string]any{"instance": "vm"})))
	if err != nil {
		t.Fatalf("get zone b: %v", err)
	}
	if !strings.Contains(str(b.Data["zone"]), "us-central1-b") {
		t.Errorf("zone b instance zone = %v", b.Data["zone"])
	}

	listA, _ := p.InstancesList(ctx, newNR(zonal("us-central1-a", nil)))
	if items, _ := listA.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("zone a list = %d, want 1", len(items))
	}
	listB, _ := p.InstancesList(ctx, newNR(zonal("us-central1-b", nil)))
	if items, _ := listB.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("zone b list = %d, want 1", len(items))
	}
	listC, _ := p.InstancesList(ctx, newNR(zonal("us-central1-c", nil)))
	if items, _ := listC.Data["items"].([]any); len(items) != 0 {
		t.Fatalf("zone c list = %d, want 0", len(items))
	}
}

func TestInstanceAlreadyExistsAndNotFound(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm")

	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": instanceBody("vm")}))); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	if _, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "nope"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound, got %v", err)
	}
	if _, err := p.InstancesDelete(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "nope"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound on delete, got %v", err)
	}
	if _, err := p.InstancesStop(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "nope"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound on stop, got %v", err)
	}
	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{"machineType": "e2-micro"}}))); !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument for missing name, got %v", err)
	}
}

func TestAggregatedList(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm-a")
	mustCreateInstance(t, p, "us-east1-b", "vm-b")

	resp, err := p.InstancesAggregatedList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("aggregated list: %v", err)
	}
	if resp.Data["kind"] != kindInstanceAggregatedList {
		t.Errorf("kind = %v", resp.Data["kind"])
	}
	items, _ := resp.Data["items"].(map[string]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 scoped groups, got %d: %v", len(items), items)
	}
	if _, ok := items["zones/us-central1-a"]; !ok {
		t.Errorf("missing zone a group: %v", items)
	}
}

func TestDiskCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	op, err := p.DisksInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{
		"name": "disk-a", "sizeGb": 20, "type": "pd-ssd",
	}})))
	if err != nil {
		t.Fatalf("create disk: %v", err)
	}
	if op.Data["operationType"] != "insert" || op.Data["status"] != "DONE" {
		t.Errorf("unexpected disk operation: %v", op.Data)
	}

	if _, err := p.DisksInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{"name": "disk-a"}}))); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}

	got, err := p.DisksGet(ctx, newNR(zonal("us-central1-a", map[string]any{"disk": "disk-a"})))
	if err != nil {
		t.Fatalf("get disk: %v", err)
	}
	if got.Data["kind"] != kindDisk || got.Data["status"] != "READY" {
		t.Errorf("disk = %v", got.Data)
	}
	if got.Data["sizeGb"] != "20" {
		t.Errorf("sizeGb = %v (%T), want \"20\"", got.Data["sizeGb"], got.Data["sizeGb"])
	}
	if !strings.Contains(str(got.Data["type"]), "diskTypes/pd-ssd") {
		t.Errorf("type = %v", got.Data["type"])
	}

	list, err := p.DisksList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("list disks: %v", err)
	}
	if list.Data["kind"] != kindDiskList {
		t.Errorf("disk list kind = %v", list.Data["kind"])
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 disk, got %d", len(items))
	}

	// Disks are zonal: another zone must not see it.
	if _, err := p.DisksGet(ctx, newNR(zonal("us-central1-b", map[string]any{"disk": "disk-a"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound in other zone, got %v", err)
	}

	if _, err := p.DisksDelete(ctx, newNR(zonal("us-central1-a", map[string]any{"disk": "disk-a"}))); err != nil {
		t.Fatalf("delete disk: %v", err)
	}
	if _, err := p.DisksGet(ctx, newNR(zonal("us-central1-a", map[string]any{"disk": "disk-a"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

func TestNetworkAndFirewallCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	netOp, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name": "vpc-a", "autoCreateSubnetworks": false,
	}}))
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	if netOp.Data["operationType"] != "insert" || netOp.Data["status"] != "DONE" {
		t.Errorf("unexpected network operation: %v", netOp.Data)
	}
	if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": "vpc-a"}})); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	net, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-a"}))
	if err != nil {
		t.Fatalf("get network: %v", err)
	}
	if net.Data["kind"] != kindNetwork || net.Data["autoCreateSubnetworks"] != false {
		t.Errorf("network = %v", net.Data)
	}
	nets, err := p.NetworksList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list networks: %v", err)
	}
	if nets.Data["kind"] != kindNetworkList {
		t.Errorf("network list kind = %v", nets.Data["kind"])
	}
	if _, err := p.NetworksDelete(ctx, newNR(map[string]any{"network": "vpc-a"})); err != nil {
		t.Fatalf("delete network: %v", err)
	}
	if _, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-a"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}

	fwOp, err := p.FirewallsInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name":         "allow-ssh",
		"network":      "vpc-a",
		"sourceRanges": []any{"0.0.0.0/0"},
		"allowed":      []any{map[string]any{"IPProtocol": "tcp", "ports": []any{"22"}}},
	}}))
	if err != nil {
		t.Fatalf("create firewall: %v", err)
	}
	if fwOp.Data["operationType"] != "insert" {
		t.Errorf("unexpected firewall operation: %v", fwOp.Data)
	}
	if _, err := p.FirewallsInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": "allow-ssh"}})); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	fw, err := p.FirewallsGet(ctx, newNR(map[string]any{"firewall": "allow-ssh"}))
	if err != nil {
		t.Fatalf("get firewall: %v", err)
	}
	if fw.Data["kind"] != kindFirewall || fw.Data["direction"] != "INGRESS" {
		t.Errorf("firewall = %v", fw.Data)
	}
	if !strings.Contains(str(fw.Data["network"]), "global/networks/vpc-a") {
		t.Errorf("firewall network = %v", fw.Data["network"])
	}
	fws, err := p.FirewallsList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list firewalls: %v", err)
	}
	if fws.Data["kind"] != kindFirewallList {
		t.Errorf("firewall list kind = %v", fws.Data["kind"])
	}
	if _, err := p.FirewallsDelete(ctx, newNR(map[string]any{"firewall": "allow-ssh"})); err != nil {
		t.Fatalf("delete firewall: %v", err)
	}
	if _, err := p.FirewallsGet(ctx, newNR(map[string]any{"firewall": "allow-ssh"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

func TestSubnetworkCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	op, err := p.SubnetworksInsert(ctx, newNR(regional("us-central1", map[string]any{"body": map[string]any{
		"name": "sub-a", "network": "vpc-a", "ipCidrRange": "10.1.0.0/20",
	}})))
	if err != nil {
		t.Fatalf("create subnetwork: %v", err)
	}
	if op.Data["operationType"] != "insert" {
		t.Errorf("unexpected subnetwork operation: %v", op.Data)
	}
	if _, err := p.SubnetworksInsert(ctx, newNR(regional("us-central1", map[string]any{"body": map[string]any{"name": "sub-a"}}))); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	sub, err := p.SubnetworksGet(ctx, newNR(regional("us-central1", map[string]any{"subnetwork": "sub-a"})))
	if err != nil {
		t.Fatalf("get subnetwork: %v", err)
	}
	if sub.Data["kind"] != kindSubnetwork || sub.Data["ipCidrRange"] != "10.1.0.0/20" {
		t.Errorf("subnetwork = %v", sub.Data)
	}
	if !strings.Contains(str(sub.Data["region"]), "regions/us-central1") {
		t.Errorf("subnetwork region = %v", sub.Data["region"])
	}
	// Regional scoping: a different region must not see it.
	if _, err := p.SubnetworksGet(ctx, newNR(regional("us-east1", map[string]any{"subnetwork": "sub-a"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound in other region, got %v", err)
	}
	list, err := p.SubnetworksList(ctx, newNR(regional("us-central1", nil)))
	if err != nil {
		t.Fatalf("list subnetworks: %v", err)
	}
	if list.Data["kind"] != kindSubnetworkList {
		t.Errorf("subnetwork list kind = %v", list.Data["kind"])
	}
	if _, err := p.SubnetworksDelete(ctx, newNR(regional("us-central1", map[string]any{"subnetwork": "sub-a"}))); err != nil {
		t.Fatalf("delete subnetwork: %v", err)
	}
}

func TestOperationsPersistedAndLookup(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	op := mustCreateInstance(t, p, "us-central1-a", "vm-a")
	opName, _ := op["name"].(string)
	if opName == "" {
		t.Fatal("operation name is empty")
	}
	if op["targetId"] == "" || op["targetLink"] == "" || op["selfLink"] == "" {
		t.Errorf("operation links must be populated: %v", op)
	}
	if op["insertTime"] == "" || op["endTime"] == "" {
		t.Errorf("operation times must be populated: %v", op)
	}

	got, err := p.OperationsGet(ctx, newNR(zonal("us-central1-a", map[string]any{"operation": opName})))
	if err != nil {
		t.Fatalf("operations.get: %v", err)
	}
	if got.Data["name"] != opName || got.Data["status"] != "DONE" {
		t.Errorf("operation readback = %v", got.Data)
	}
	if !strings.Contains(str(got.Data["selfLink"]), "zones/us-central1-a/operations/") {
		t.Errorf("operation selfLink = %v", got.Data["selfLink"])
	}

	list, err := p.OperationsList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("operations.list: %v", err)
	}
	if list.Data["kind"] != kindOperationList {
		t.Errorf("operations list kind = %v", list.Data["kind"])
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(items))
	}

	// An operation is scoped: another zone must not see it.
	if _, err := p.OperationsGet(ctx, newNR(zonal("us-central1-b", map[string]any{"operation": opName}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound in other zone, got %v", err)
	}
	if _, err := p.OperationsGet(ctx, newNR(zonal("us-central1-a", map[string]any{"operation": "missing"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound, got %v", err)
	}

	// A global mutation persists its operation at global scope.
	netOp, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": "global-net"}}))
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	globalName, _ := netOp.Data["name"].(string)
	globalGot, err := p.OperationsGet(ctx, newNR(map[string]any{"operation": globalName}))
	if err != nil {
		t.Fatalf("global operations.get: %v", err)
	}
	if !strings.Contains(str(globalGot.Data["selfLink"]), "global/operations/") {
		t.Errorf("global operation selfLink = %v", globalGot.Data["selfLink"])
	}
}

func TestSynthesizedDiscovery(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	zones, err := p.ZonesList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("zones list: %v", err)
	}
	if zones.Data["kind"] != kindZoneList {
		t.Errorf("zones kind = %v", zones.Data["kind"])
	}
	if items, _ := zones.Data["items"].([]any); len(items) == 0 {
		t.Fatal("expected zones")
	}
	zone, err := p.ZonesGet(ctx, newNR(map[string]any{"zone": "us-central1-a"}))
	if err != nil {
		t.Fatalf("zone get: %v", err)
	}
	if zone.Data["kind"] != kindZone || zone.Data["status"] != "UP" {
		t.Errorf("zone = %v", zone.Data)
	}
	if _, err := p.ZonesGet(ctx, newNR(map[string]any{"zone": "us-moon-1a"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound for unknown zone, got %v", err)
	}

	regions, err := p.RegionsList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("regions list: %v", err)
	}
	if regions.Data["kind"] != kindRegionList {
		t.Errorf("regions kind = %v", regions.Data["kind"])
	}
	region, err := p.RegionsGet(ctx, newNR(map[string]any{"region": "us-central1"}))
	if err != nil {
		t.Fatalf("region get: %v", err)
	}
	if zoneURLs, _ := region.Data["zones"].([]any); len(zoneURLs) == 0 {
		t.Error("region must list its zones")
	}
	if _, err := p.RegionsGet(ctx, newNR(map[string]any{"region": "mars-central1"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound for unknown region, got %v", err)
	}

	mts, err := p.MachineTypesList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("machine types list: %v", err)
	}
	if mts.Data["kind"] != kindMachineTypeList {
		t.Errorf("machine types kind = %v", mts.Data["kind"])
	}
	if items, _ := mts.Data["items"].([]any); len(items) == 0 {
		t.Fatal("expected machine types")
	}
	mt, err := p.MachineTypesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"machineType": "e2-micro"})))
	if err != nil {
		t.Fatalf("machine type get: %v", err)
	}
	if mt.Data["kind"] != kindMachineType || mt.Data["guestCpus"] != int64(2) {
		t.Errorf("machine type = %v", mt.Data)
	}
	if _, err := p.MachineTypesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"machineType": "nope"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestUnimplemented(t *testing.T) {
	if _, err := newProvider().Unimplemented(context.Background(), newNR(nil)); !isCode(err, "Unimplemented") {
		t.Fatalf("expected Unimplemented, got %v", err)
	}
}

// TestResourceNameValidation pins Compute Engine's documented name grammar
// (Discovery, Instance.name): 1-63 chars matching [a-z]([-a-z0-9]*[a-z0-9])?.
func TestResourceNameValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	bad := []string{"Upper", "under_score", "-leading", "trailing-", "with space", strings.Repeat("a", 64)}

	for _, name := range bad {
		if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": instanceBody(name)}))); !isCode(err, "InvalidArgument") {
			t.Errorf("instance name %q: expected InvalidArgument, got %v", name, err)
		}
		if _, err := p.DisksInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{"name": name}}))); !isCode(err, "InvalidArgument") {
			t.Errorf("disk name %q: expected InvalidArgument, got %v", name, err)
		}
		if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": name}})); !isCode(err, "InvalidArgument") {
			t.Errorf("network name %q: expected InvalidArgument, got %v", name, err)
		}
		if _, err := p.FirewallsInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": name}})); !isCode(err, "InvalidArgument") {
			t.Errorf("firewall name %q: expected InvalidArgument, got %v", name, err)
		}
		if _, err := p.SubnetworksInsert(ctx, newNR(regional("us-central1", map[string]any{"body": map[string]any{"name": name}}))); !isCode(err, "InvalidArgument") {
			t.Errorf("subnetwork name %q: expected InvalidArgument, got %v", name, err)
		}
	}

	// A single-character name and an all-digit tail are valid.
	for _, name := range []string{"a", "vm-1", "web-01"} {
		if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": instanceBody(name)}))); err != nil {
			t.Errorf("instance name %q: expected success, got %v", name, err)
		}
	}
}

// TestMachineTypeValidationOnInsert pins that an unknown machine type is
// rejected with 400 (real GCP) instead of silently accepted.
func TestMachineTypeValidationOnInsert(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	// Unknown machine type.
	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{
		"name": "vm-bogus", "machineType": "bogus-type",
	}}))); !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument for unknown machine type, got %v", err)
	}

	// A full/partial machine-type URL resolves to its last segment.
	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{
		"name": "vm-url", "machineType": "projects/proj/zones/us-central1-a/machineTypes/e2-micro",
	}}))); err != nil {
		t.Fatalf("full machine-type URL must be accepted, got %v", err)
	}

	// Real machine types missing from the advertised catalog must still be
	// accepted (the catalog is a subset, not the universe).
	for i, mt := range []string{"e2-standard-4", "n2-highmem-8", "c3-highcpu-4", "custom-2-4096", "n2-custom-2-4096", "n2-custom-2-4096-ext"} {
		if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{
			"name": fmt.Sprintf("vm-real-%d", i), "machineType": mt,
		}}))); err != nil {
			t.Errorf("real machine type %q must be accepted, got %v", mt, err)
		}
	}

	// An absent machine type defaults to e2-micro.
	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{
		"name": "vm-default",
	}}))); err != nil {
		t.Fatalf("insert without machineType: %v", err)
	}
	inst, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-default"})))
	if err != nil {
		t.Fatalf("get default instance: %v", err)
	}
	if !strings.Contains(str(inst.Data["machineType"]), "machineTypes/"+defaultMachineType) {
		t.Errorf("default machineType = %v, want %s", inst.Data["machineType"], defaultMachineType)
	}
}

// TestListFilterThroughHandlers exercises the documented filter subset through
// the provider handlers (not just the parser).
func TestListFilterThroughHandlers(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, n := range []string{"web-a", "web-b", "db-a"} {
		mustCreateInstance(t, p, "us-central1-a", n)
	}

	// Filter by name equality.
	resp, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", map[string]any{"filter": "name = web-a"})))
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 1 || stringField(items[0].(map[string]any), "name") != "web-a" {
		t.Fatalf("name filter = %v", items)
	}

	// Filter by status and by labels (instances are RUNNING with env=test).
	for _, expr := range []string{"status = RUNNING", "labels.env = test"} {
		resp, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", map[string]any{"filter": expr})))
		if err != nil {
			t.Fatalf("filter %q: %v", expr, err)
		}
		if items, _ := resp.Data["items"].([]any); len(items) != 3 {
			t.Errorf("filter %q = %d items, want 3", expr, len(items))
		}
	}

	// Negation.
	resp, err = p.InstancesList(ctx, newNR(zonal("us-central1-a", map[string]any{"filter": "name != web-a"})))
	if err != nil {
		t.Fatalf("negated filter: %v", err)
	}
	if items, _ := resp.Data["items"].([]any); len(items) != 2 {
		t.Errorf("name != web-a = %d items, want 2", len(items))
	}

	// A filter outside the subset fails loud rather than returning everything.
	if _, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", map[string]any{"filter": "cpuPlatform eq .*"}))); !isCode(err, "InvalidArgument") {
		t.Fatalf("unsupported filter must be InvalidArgument, got %v", err)
	}
}

// TestAggregatedListPaging covers the documented maxResults/pageToken cursor
// over the flattened instance set, re-grouped by zone.
func TestAggregatedListPaging(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm-1")
	mustCreateInstance(t, p, "us-central1-a", "vm-2")
	mustCreateInstance(t, p, "us-east1-b", "vm-3")

	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		params := map[string]any{"maxResults": 1}
		if token != "" {
			params["pageToken"] = token
		}
		resp, err := p.InstancesAggregatedList(ctx, newNR(params))
		if err != nil {
			t.Fatalf("aggregated page: %v", err)
		}
		if resp.Data["kind"] != kindInstanceAggregatedList {
			t.Errorf("kind = %v", resp.Data["kind"])
		}
		items, _ := resp.Data["items"].(map[string]any)
		for _, g := range items {
			group, _ := g.(map[string]any)
			insts, _ := group["instances"].([]any)
			for _, i := range insts {
				name := stringField(i.(map[string]any), "name")
				if seen[name] {
					t.Errorf("instance %q returned on more than one page", name)
				}
				seen[name] = true
			}
		}
		token, _ = resp.Data["nextPageToken"].(string)
		pages++
		if token == "" {
			break
		}
		if pages > 10 {
			t.Fatal("paging did not terminate")
		}
	}
	if len(seen) != 3 {
		t.Fatalf("paged aggregated list returned %d instances, want 3: %v", len(seen), seen)
	}
	if pages != 3 {
		t.Errorf("pages = %d, want 3", pages)
	}

	// The aggregated list also honours the filter.
	resp, err := p.InstancesAggregatedList(ctx, newNR(map[string]any{"filter": "name = vm-3"}))
	if err != nil {
		t.Fatalf("filtered aggregated list: %v", err)
	}
	items, _ := resp.Data["items"].(map[string]any)
	if _, ok := items["zones/us-east1-b"]; !ok {
		t.Errorf("filtered aggregated list = %v", items)
	}
	if _, ok := items["zones/us-central1-a"]; ok {
		t.Errorf("zone a must be filtered out: %v", items)
	}
}

// TestListParamsPageSizeDefaults pins Compute Engine's documented 500-result
// default and 0-500 range (the shared paging helper defaults to 1000).
func TestListParamsPageSizeDefaults(t *testing.T) {
	cases := []struct {
		label  string
		params map[string]any
		want   int
	}{
		{"absent", nil, defaultComputePageSize},
		{"maxResults int", map[string]any{"maxResults": 2}, 2},
		{"maxResults string", map[string]any{"maxResults": "3"}, 3},
		{"zero means default", map[string]any{"maxResults": 0}, defaultComputePageSize},
		{"clamped to max", map[string]any{"maxResults": 501}, maxComputePageSize},
		{"at max", map[string]any{"maxResults": 500}, 500},
		// maxResults is a uint32 in Compute's Discovery document, so a value
		// above int32 is a legal binding; the documented 0-500 range clamps it.
		{"above int32 but valid uint32", map[string]any{"maxResults": "2147483648"}, maxComputePageSize},
		// Compute Engine has no pageSize parameter; it must not override maxResults.
		{"pageSize ignored", map[string]any{"pageSize": 7}, defaultComputePageSize},
	}
	for _, tc := range cases {
		params, err := listParams(newNR(tc.params))
		if err != nil {
			t.Errorf("%s: listParams error = %v", tc.label, err)
			continue
		}
		if got := params["pageSize"]; got != tc.want {
			t.Errorf("%s: pageSize = %v, want %d", tc.label, got, tc.want)
		}
	}
}

// TestListParamsRejectsBadMaxResults pins the uint32 query binding Compute's
// Discovery document declares for maxResults: an unparseable value, a fraction,
// an explicitly empty one (DC63) or a negative one is INVALID_ARGUMENT, never a
// silent fall back to the 500 default page — the same answer real GCP's binding
// gives. Compute's frontend binds its query parameters on the `header` message,
// so the message names 'header.max_results' (DC68).
func TestListParamsRejectsBadMaxResults(t *testing.T) {
	for _, v := range []any{"", "abc", "1.5", "-1", "-0.5", "4294967296", float64(1.5), int64(-1)} {
		_, err := listParams(newNR(map[string]any{"maxResults": v}))
		var perr *model.ProviderError
		if !errors.As(err, &perr) {
			t.Errorf("listParams(maxResults=%#v) error = %v, want a *model.ProviderError", v, err)
			continue
		}
		if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("listParams(maxResults=%#v) = %+v, want InvalidArgument/400", v, perr)
		}
		if want := `Invalid value at 'header.max_results' (TYPE_UINT32), ` + boundValue(v); perr.Message != want {
			t.Errorf("listParams(maxResults=%#v) message = %q, want %q", v, perr.Message, want)
		}
	}
}

// ─── requestId idempotency (TD18) ─────────────────────────────────────────────

// boundValue renders the offending query value the way the frontend's binding
// error echoes it: a string value is re-quoted verbatim.
func boundValue(v any) string {
	if s, ok := v.(string); ok {
		return strconv.Quote(s)
	}
	return fmt.Sprint(v)
}

// testRequestID is a well-formed UUID used by the requestId tests.
const testRequestID = "3b241101-e2bb-4255-8caf-4136c566a962"

// TestInstanceInsertRequestIDIsIdempotent covers the documented contract on
// instances.insert: a retry carrying the same requestId is ignored once the
// first attempt finished and replays the original operation, so no duplicate
// instance is created.
func TestInstanceInsertRequestIDIsIdempotent(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	params := zonal("us-central1-a", map[string]any{"body": instanceBody("vm-idem"), "requestId": testRequestID})

	first, err := p.InstancesInsert(ctx, newNR(params))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := str(first.Data["clientOperationId"]); got != testRequestID {
		t.Errorf("clientOperationId = %q, want %q", got, testRequestID)
	}

	// A retry with the same token replays the original operation.
	second, err := p.InstancesInsert(ctx, newNR(params))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if second.Data["name"] != first.Data["name"] {
		t.Errorf("retry replayed operation %v, want %v", second.Data["name"], first.Data["name"])
	}
	if second.Data["selfLink"] != first.Data["selfLink"] {
		t.Errorf("retry selfLink = %v, want %v", second.Data["selfLink"], first.Data["selfLink"])
	}
	if second.Data["clientOperationId"] != testRequestID {
		t.Errorf("retry clientOperationId = %v, want %s", second.Data["clientOperationId"], testRequestID)
	}

	// The retry must not have created a second instance.
	list, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 instance after retry, got %d", len(items))
	}

	// operations.get resolves the replayed operation, token echoed.
	opName := str(first.Data["name"])
	got, err := p.OperationsGet(ctx, newNR(zonal("us-central1-a", map[string]any{"operation": opName})))
	if err != nil {
		t.Fatalf("operations.get: %v", err)
	}
	if got.Data["clientOperationId"] != testRequestID {
		t.Errorf("persisted clientOperationId = %v, want %s", got.Data["clientOperationId"], testRequestID)
	}

	// A fresh token on the same name is a distinct request: the name is taken.
	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{
		"body": instanceBody("vm-idem"), "requestId": "0f8fad5b-d9cb-469f-a165-70867728950e"}))); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists for a new token on an existing name, got %v", err)
	}
}

// TestRequestIDReuseAcrossResourcesReplaysOriginal pins the documented
// "if original operation with the same request ID was received ... ignore the
// second request": the token, not the body, decides — a different body with
// the same token replays the original operation and creates nothing.
func TestRequestIDReuseAcrossResourcesReplaysOriginal(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	first, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{
		"body": instanceBody("vm-first"), "requestId": testRequestID})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	second, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{
		"body": instanceBody("vm-second"), "requestId": testRequestID})))
	if err != nil {
		t.Fatalf("reuse: %v", err)
	}
	if second.Data["name"] != first.Data["name"] {
		t.Errorf("reused token replayed operation %v, want %v", second.Data["name"], first.Data["name"])
	}
	if _, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-second"}))); !isCode(err, "NotFound") {
		t.Fatalf("vm-second must not have been created, got %v", err)
	}
}

// TestDiskInsertRequestIDIsIdempotent covers the same contract on disks.insert.
func TestDiskInsertRequestIDIsIdempotent(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	body := map[string]any{"name": "disk-idem", "sizeGb": 20, "type": "pd-ssd"}
	params := zonal("us-central1-a", map[string]any{"body": body, "requestId": testRequestID})

	first, err := p.DisksInsert(ctx, newNR(params))
	if err != nil {
		t.Fatalf("insert disk: %v", err)
	}
	second, err := p.DisksInsert(ctx, newNR(params))
	if err != nil {
		t.Fatalf("retry disk: %v", err)
	}
	if second.Data["name"] != first.Data["name"] {
		t.Errorf("retry replayed operation %v, want %v", second.Data["name"], first.Data["name"])
	}
	if second.Data["clientOperationId"] != testRequestID {
		t.Errorf("clientOperationId = %v, want %s", second.Data["clientOperationId"], testRequestID)
	}
	list, err := p.DisksList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("list disks: %v", err)
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 disk after retry, got %d", len(items))
	}
}

// TestDeleteRequestIDIsIdempotent covers the delete side: retrying a completed
// delete with the same token replays the original operation rather than
// 404-ing on the already-deleted instance.
func TestDeleteRequestIDIsIdempotent(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm-del")
	params := zonal("us-central1-a", map[string]any{"instance": "vm-del", "requestId": testRequestID})

	first, err := p.InstancesDelete(ctx, newNR(params))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	second, err := p.InstancesDelete(ctx, newNR(params))
	if err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	if second.Data["name"] != first.Data["name"] {
		t.Errorf("retry replayed operation %v, want %v", second.Data["name"], first.Data["name"])
	}
	if second.Data["operationType"] != "delete" {
		t.Errorf("operationType = %v, want delete", second.Data["operationType"])
	}
	// Without a token the resource is gone, so a plain retry is 404.
	if _, err := p.InstancesDelete(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-del"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound for an untokened retry, got %v", err)
	}
}

// TestRequestIDValidation asserts the documented UUID contract: a malformed
// token or the unsupported zero UUID fails loud with 400, and an absent token
// leaves clientOperationId unset.
func TestRequestIDValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, bad := range []string{
		"not-a-uuid",
		"3b241101e2bb42558caf4136c566a962",
		"00000000-0000-0000-0000-000000000000",
	} {
		_, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{
			"body": instanceBody("vm-bad"), "requestId": bad})))
		if !isCode(err, "InvalidArgument") {
			t.Errorf("requestId %q: expected InvalidArgument, got %v", bad, err)
		}
	}

	op := mustCreateInstance(t, p, "us-central1-a", "vm-plain")
	if _, ok := op["clientOperationId"]; ok {
		t.Errorf("clientOperationId must be absent without a requestId: %v", op)
	}
}

// TestRequestIDSameTokenConcurrentInsertsReplayOneOperation asserts that a
// burst of same-token inserts (including ones that overlap an in-flight
// original) all replay the single original operation, so no duplicate resource
// is created and no retry is answered with a conflict. Run under -race it also
// exercises the idempotency mapping and its per-token lock.
func TestRequestIDSameTokenConcurrentInsertsReplayOneOperation(t *testing.T) {
	const workers = 8
	ctx := context.Background()
	p := newProvider()
	names := make([]string, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{
				"body": instanceBody("vm-race"), "requestId": testRequestID})))
			errs[i] = err
			if err == nil {
				names[i] = str(resp.Data["name"])
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("same-token insert %d failed: %v (a retry must replay, not conflict)", i, err)
		}
		if names[i] != names[0] {
			t.Fatalf("worker %d replayed operation %q, want %q", i, names[i], names[0])
		}
	}

	list, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected exactly 1 instance from concurrent same-token inserts, got %d", len(items))
	}
}

// TestRequestIDIdempotentForGlobalScopeAndTransitions covers the global-scope
// (network) and instance-transition paths: a retried start replays the original
// operation and leaves the instance RUNNING.
func TestRequestIDIdempotentForGlobalScopeAndTransitions(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	// Global scope: a network insert retry replays.
	nparams := map[string]any{"body": map[string]any{"name": "net-idem"}, "requestId": testRequestID}
	firstNet, err := p.NetworksInsert(ctx, newNR(nparams))
	if err != nil {
		t.Fatalf("network insert: %v", err)
	}
	secondNet, err := p.NetworksInsert(ctx, newNR(nparams))
	if err != nil {
		t.Fatalf("network retry: %v", err)
	}
	if secondNet.Data["name"] != firstNet.Data["name"] {
		t.Errorf("network retry replayed %v, want %v", secondNet.Data["name"], firstNet.Data["name"])
	}
	if secondNet.Data["clientOperationId"] != testRequestID {
		t.Errorf("network clientOperationId = %v, want %s", secondNet.Data["clientOperationId"], testRequestID)
	}
	nets, err := p.NetworksList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list networks: %v", err)
	}
	if items, _ := nets.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 network after retry, got %d", len(items))
	}

	// A state transition: a retried start replays its operation.
	mustCreateInstance(t, p, "us-central1-a", "vm-start")
	sparams := zonal("us-central1-a", map[string]any{
		"instance": "vm-start", "requestId": "0f8fad5b-d9cb-469f-a165-70867728950e"})
	firstStart, err := p.InstancesStart(ctx, newNR(sparams))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	secondStart, err := p.InstancesStart(ctx, newNR(sparams))
	if err != nil {
		t.Fatalf("start retry: %v", err)
	}
	if secondStart.Data["name"] != firstStart.Data["name"] {
		t.Errorf("start retry replayed %v, want %v", secondStart.Data["name"], firstStart.Data["name"])
	}
	if secondStart.Data["operationType"] != "start" {
		t.Errorf("operationType = %v, want start", secondStart.Data["operationType"])
	}
	got, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-start"})))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["status"] != "RUNNING" {
		t.Errorf("status = %v, want RUNNING", got.Data["status"])
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func isCode(err error, code string) bool {
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		return false
	}
	return perr.Code == code
}

// TestListHandlersRejectBadMaxResults pins the uint32 query binding on
// Compute's list surfaces: a negative, fractional or unparseable maxResults is
// INVALID_ARGUMENT (the documented range is 0 to 500 inclusive and the
// parameter is unsigned), never a silent fall-back to the 500 default. Every
// list handler that pages through listParams is exercised so the shared reject
// is covered on each cursor walk.
func TestListHandlersRejectBadMaxResults(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	handlers := []struct {
		name string
		call func(*model.NormalizedRequest) (*model.ProviderResponse, error)
	}{
		{"InstancesList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.InstancesList(ctx, nr) }},
		{"InstancesAggregatedList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return p.InstancesAggregatedList(ctx, nr)
		}},
		{"DisksList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.DisksList(ctx, nr) }},
		{"NetworksList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.NetworksList(ctx, nr) }},
		{"FirewallsList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.FirewallsList(ctx, nr) }},
		{"SubnetworksList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.SubnetworksList(ctx, nr) }},
		{"MachineTypesList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.MachineTypesList(ctx, nr) }},
		{"ZonesList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.ZonesList(ctx, nr) }},
		{"RegionsList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.RegionsList(ctx, nr) }},
		{"OperationsList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.OperationsList(ctx, nr) }},
	}
	for _, h := range handlers {
		for _, v := range []any{"-1", "abc", "1.5", "4294967296"} {
			_, err := h.call(newNR(map[string]any{"maxResults": v}))
			if !isCode(err, "InvalidArgument") {
				t.Errorf("%s(maxResults=%v) = %v, want InvalidArgument", h.name, v, err)
			}
		}
	}
}
