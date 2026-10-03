package compute

import (
	"context"
	"errors"
	"strings"
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
