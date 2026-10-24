// Package compute implements the Compute Engine v1 control-plane provider
// (compute.googleapis.com/compute/v1): instances, disks, networks, firewalls,
// subnetworks, and the zonal/regional/global operations that mutate them, plus
// the synthesized zones, regions, and machineTypes discovery surface.
//
// This is metadata-only, mirroring the Cloud DNS, Memorystore, and Cloud SQL
// providers and the AWS EC2 provider: every resource is a record in the shared
// ResourceStore keyed by the project (AccountID) and the request scope — the
// zone for zonal resources, the region for regional resources, and
// store.GlobalRegion for global resources. The emulator never boots a VM or
// attaches a real disk, so an instance is born in status RUNNING with
// synthesized ids, selfLinks, timestamps, a cosmetic network interface that
// nothing listens on, and a stable cpuPlatform. There is no data plane and no
// AuthN/AuthZ.
//
// Every mutation returns Compute Engine's own Operation envelope
// (kind "compute#operation") inline with status "DONE" and progress 100 — NOT
// a google.longrunning.Operation — and the same operation is persisted at its
// scope so operations.get/list can read it back.
//
// Deferred surfaces (addresses, images, snapshots, instance groups/templates,
// routers, backend services, and every custom method such as setMetadata,
// attachDisk, or reset) resolve to the Unimplemented action and fail loud with
// 501 rather than 404-ing.
package compute

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource types in the shared ResourceStore.
const (
	rtInstance   = "gcp_compute_instance"
	rtDisk       = "gcp_compute_disk"
	rtNetwork    = "gcp_compute_network"
	rtFirewall   = "gcp_compute_firewall"
	rtSubnetwork = "gcp_compute_subnetwork"
	rtOperation  = "gcp_compute_operation"
	// rtRequestID holds the requestId → operation idempotency mapping for
	// mutating methods, keyed by the requestId at the request's scope.
	rtRequestID = "gcp_compute_request_id"
)

// Kind constants for the GCP wire envelope.
const (
	kindInstance               = "compute#instance"
	kindInstanceList           = "compute#instanceList"
	kindInstanceAggregatedList = "compute#instanceAggregatedList"
	kindDisk                   = "compute#disk"
	kindDiskList               = "compute#diskList"
	kindNetwork                = "compute#network"
	kindNetworkList            = "compute#networkList"
	kindFirewall               = "compute#firewall"
	kindFirewallList           = "compute#firewallList"
	kindSubnetwork             = "compute#subnetwork"
	kindSubnetworkList         = "compute#subnetworkList"
	kindMachineType            = "compute#machineType"
	kindMachineTypeList        = "compute#machineTypeList"
	kindZone                   = "compute#zone"
	kindZoneList               = "compute#zoneList"
	kindRegion                 = "compute#region"
	kindRegionList             = "compute#regionList"
	kindOperation              = "compute#operation"
	kindOperationList          = "compute#operationList"
)

// computeBasePath is the public Compute Engine REST root. selfLink/targetLink
// are absolute URLs rooted here.
const computeBasePath = "https://www.googleapis.com/compute/v1/"

// defaultDiskSizeGb is used when a disk create request omits sizeGb.
const defaultDiskSizeGb = "10"

// defaultMachineType is used when an instance create request omits machineType.
const defaultMachineType = "e2-micro"

// defaultComputePageSize is Compute Engine's documented maxResults default
// (Discovery): "Acceptable values are 0 to 500, inclusive. (Default: 500)".
// The shared paging helper defaults to 1000, so compute lists set it explicitly.
const defaultComputePageSize = 500

// maxComputePageSize is Compute Engine's documented maxResults upper bound.
const maxComputePageSize = 500

// resourceNameRE is Compute Engine's documented resource-name grammar
// (Discovery, Instance.name): 1-63 characters matching [a-z]([-a-z0-9]*[a-z0-9])?.
var resourceNameRE = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)

// machineTypeRE matches the wire form of a predefined machine type that is not
// in machineTypeCatalog: <series>-<family>-<size> (e.g. e2-standard-4,
// n2-highmem-8, c3-highcpu-4, a2-highgpu-1g). The catalog is a small stable
// subset, so a name matching this shape is a real Compute Engine machine type
// the emulator simply does not advertise — it must not be rejected as invalid.
var machineTypeRE = regexp.MustCompile(`^[a-z][a-z0-9]*-[a-z][a-z0-9]*-[a-z0-9]+$`)

// customMachineTypeRE matches a custom machine type. Real GCP names them
// <series>-custom-<vCPUs>-<memMB> (e.g. n2-custom-2-4096, e2-custom-4-8192) or,
// for N1, bare custom-<vCPUs>-<memMB>, optionally with an -ext extended-memory
// suffix.
var customMachineTypeRE = regexp.MustCompile(`^(?:[a-z0-9]+-)?custom-[0-9]+-[0-9]+(?:-ext)?$`)

// zeroRequestID is the all-zero UUID the Compute Engine API documents as
// unsupported for the requestId parameter.
const zeroRequestID = "00000000-0000-0000-0000-000000000000"

// requestIDRE matches the UUID form the Compute Engine API documents for the
// requestId parameter.
var requestIDRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// isKnownMachineType reports whether name could be a real Compute Engine
// machine type: an advertised catalog entry, a predefined family/size, or a
// custom type. A name that matches none of these is not a machine type at all
// and is rejected with 400 (real GCP).
func isKnownMachineType(name string) bool {
	if _, ok := machineTypeSpecFor(name); ok {
		return true
	}
	return machineTypeRE.MatchString(name) || customMachineTypeRE.MatchString(name)
}

// Provider handles Compute Engine instances, disks, networks, firewalls,
// subnetworks, machine types, zones, regions, and operations.
type Provider struct {
	resources store.ResourceStore
	// mutations serializes same-requestId mutations so a retry replays the
	// original operation even when it overlaps an in-flight original.
	mutations *keyedMutex
}

// New returns a Provider backed by the shared ResourceStore.
func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources, mutations: newKeyedMutex()}
}

// Routes maps "Compute.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Compute.InstancesInsert":         p.InstancesInsert,
		"Compute.InstancesGet":            p.InstancesGet,
		"Compute.InstancesList":           p.InstancesList,
		"Compute.InstancesDelete":         p.InstancesDelete,
		"Compute.InstancesStart":          p.InstancesStart,
		"Compute.InstancesStop":           p.InstancesStop,
		"Compute.InstancesReset":          p.InstancesReset,
		"Compute.InstancesAggregatedList": p.InstancesAggregatedList,

		"Compute.DisksInsert": p.DisksInsert,
		"Compute.DisksGet":    p.DisksGet,
		"Compute.DisksList":   p.DisksList,
		"Compute.DisksDelete": p.DisksDelete,

		"Compute.NetworksInsert": p.NetworksInsert,
		"Compute.NetworksGet":    p.NetworksGet,
		"Compute.NetworksList":   p.NetworksList,
		"Compute.NetworksDelete": p.NetworksDelete,

		"Compute.FirewallsInsert": p.FirewallsInsert,
		"Compute.FirewallsGet":    p.FirewallsGet,
		"Compute.FirewallsList":   p.FirewallsList,
		"Compute.FirewallsDelete": p.FirewallsDelete,

		"Compute.SubnetworksInsert": p.SubnetworksInsert,
		"Compute.SubnetworksGet":    p.SubnetworksGet,
		"Compute.SubnetworksList":   p.SubnetworksList,
		"Compute.SubnetworksDelete": p.SubnetworksDelete,

		"Compute.MachineTypesGet":  p.MachineTypesGet,
		"Compute.MachineTypesList": p.MachineTypesList,

		"Compute.ZonesGet":  p.ZonesGet,
		"Compute.ZonesList": p.ZonesList,

		"Compute.RegionsGet":  p.RegionsGet,
		"Compute.RegionsList": p.RegionsList,

		"Compute.OperationsGet":  p.OperationsGet,
		"Compute.OperationsList": p.OperationsList,

		"Compute.Unimplemented": p.Unimplemented,
	}
}

// ─── Instance operations ──────────────────────────────────────────────────────

func (p *Provider) InstancesInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := accountID(nr)
	zone := scopeOf(nr)
	if zone == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing zone", 400)
	}
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, zone)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	body := bodyMap(nr)
	name := stringField(body, "name")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "instance name is required", 400)
	}
	if err := validateResourceName(name); err != nil {
		return nil, err
	}

	inst, err := p.buildInstance(nr, project, zone, name, body)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(inst)
	if err != nil {
		return nil, err
	}
	err = p.resources.Create(ctx, project, zone, store.ResourceEntry{Type: rtInstance, ID: name, Data: data})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, model.NewProviderError("AlreadyExists", fmt.Sprintf("instance %q already exists", name), 409)
	}
	if err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(nr, project, zone, "zones", "insert", name, stringField(inst, "targetLink"), requestID)), nil
}

func (p *Provider) buildInstance(nr *model.NormalizedRequest, project, zone, name string, body map[string]any) (map[string]any, error) {
	mtName := lastSegment(stringField(body, "machineType"))
	if mtName == "" {
		mtName = defaultMachineType
	} else if !isKnownMachineType(mtName) {
		return nil, model.NewProviderError("InvalidArgument",
			fmt.Sprintf("Invalid value for field 'resource.machineType': 'machineTypes/%s'. Machine type is not offered in zone %q", mtName, zone), 400)
	}
	inst := map[string]any{
		"kind":              kindInstance,
		"id":                numericID(project + "/zones/" + zone + "/instances/" + name),
		"name":              name,
		"machineType":       absLink(nr, "compute-machine-type", zone+"/"+mtName),
		"status":            "RUNNING",
		"zone":              absLink(nr, "compute-zone", zone),
		"region":            absLink(nr, "compute-region", regionFromZone(zone)),
		"creationTimestamp": formatTimestamp(clock.Now()),
		"selfLink":          absLink(nr, "compute-instance", zone+"/"+name),
		"targetLink":        absLink(nr, "compute-instance", zone+"/"+name),
		"cpuPlatform":       cpuPlatform(mtName),
		"networkInterfaces": normalizeNetworkInterfaces(nr, project, zone, name, body["networkInterfaces"]),
	}
	if v, ok := body["disks"]; ok {
		inst["disks"] = v
	}
	if v, ok := body["metadata"]; ok && v != nil {
		inst["metadata"] = v
	} else {
		inst["metadata"] = map[string]any{"kind": "compute#metadata", "fingerprint": contentEtag(name)}
	}
	if v, ok := body["labels"]; ok && v != nil {
		inst["labels"] = v
	}
	if v, ok := body["tags"]; ok && v != nil {
		inst["tags"] = v
	}
	if v, ok := body["description"]; ok && v != nil {
		inst["description"] = v
	}
	return inst, nil
}

func (p *Provider) InstancesGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "instance")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	inst, err := p.loadMap(ctx, accountID(nr), scopeOf(nr), rtInstance, name, "instance")
	if err != nil {
		return nil, err
	}
	return provider.OK(inst), nil
}

func (p *Provider) InstancesList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, scope := accountID(nr), scopeOf(nr)
	entries, err := p.resources.List(ctx, project, scope, rtInstance, "")
	if err != nil {
		return nil, err
	}
	items := mapsFromEntries(entries)
	items, err = applyFilterMaps(nr, items)
	if err != nil {
		return nil, err
	}
	params, err := listParams(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(items, func(m map[string]any) string { return stringField(m, "name") }, params)
	if err != nil {
		return nil, err
	}
	out := toAnySlice(page)
	resp := map[string]any{"kind": kindInstanceList, "items": out}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// InstancesAggregatedList lists instances across every zone for the project,
// honouring the documented maxResults/pageToken cursor and filter. The store's
// cross-scope scan requires account and region to be empty, so the result is
// filtered by the owning project. Paging walks the flattened instance set
// (ordered by "zone/name"); the returned page is re-grouped by zone, so — as in
// real GCP — only scopes with a matching instance on the current page appear.
func (p *Provider) InstancesAggregatedList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := accountID(nr)
	entries, err := p.resources.List(ctx, "", "", rtInstance, "")
	if err != nil {
		return nil, err
	}
	f, err := compileFilter(nr)
	if err != nil {
		return nil, err
	}
	type scopedInstance struct {
		zone string
		m    map[string]any
	}
	flat := make([]scopedInstance, 0, len(entries))
	for _, e := range entries {
		if e.Account != project {
			continue
		}
		var m map[string]any
		if json.Unmarshal(e.Data, &m) != nil {
			continue
		}
		if !f.match(m) {
			continue
		}
		flat = append(flat, scopedInstance{zone: e.Region, m: m})
	}
	params, err := listParams(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(flat, func(s scopedInstance) string {
		return s.zone + "/" + stringField(s.m, "name")
	}, params)
	if err != nil {
		return nil, err
	}
	items := map[string]any{}
	for _, s := range page {
		key := "zones/" + s.zone
		group, _ := items[key].(map[string]any)
		if group == nil {
			group = map[string]any{}
			items[key] = group
		}
		list, _ := group["instances"].([]any)
		group["instances"] = append(list, s.m)
	}
	resp := map[string]any{"kind": kindInstanceAggregatedList, "items": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) InstancesDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "instance")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	project, scope := accountID(nr), scopeOf(nr)
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, scope)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	inst, err := p.loadMap(ctx, project, scope, rtInstance, name, "instance")
	if err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, project, scope, rtInstance, name); err != nil {
		return nil, storeError(err, "instance")
	}
	return provider.OK(p.recordOperation(nr, project, scope, "zones", "delete", name, stringField(inst, "targetLink"), requestID)), nil
}

func (p *Provider) InstancesStart(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setInstanceState(ctx, nr, "start", "RUNNING")
}

func (p *Provider) InstancesStop(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setInstanceState(ctx, nr, "stop", "TERMINATED")
}

func (p *Provider) InstancesReset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setInstanceState(ctx, nr, "reset", "RUNNING")
}

// setInstanceState transitions an instance to status and records an operation
// of the given type. A stopped instance's status is TERMINATED; start/reset
// return it to RUNNING.
func (p *Provider) setInstanceState(ctx context.Context, nr *model.NormalizedRequest, opType, status string) (*model.ProviderResponse, error) {
	name := strParam(nr, "instance")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	project, scope := accountID(nr), scopeOf(nr)
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, scope)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	updated, err := p.resources.UpsertAtomic(ctx, project, scope, rtInstance, name, func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
		if !exists {
			return store.ResourceEntry{}, store.ErrNotFound
		}
		var inst map[string]any
		if err := json.Unmarshal(current.Data, &inst); err != nil {
			return store.ResourceEntry{}, err
		}
		inst["status"] = status
		data, err := json.Marshal(inst)
		if err != nil {
			return store.ResourceEntry{}, err
		}
		current.Data = data
		return current, nil
	})
	if err != nil {
		return nil, storeError(err, "instance")
	}
	var inst map[string]any
	if err := json.Unmarshal(updated.Data, &inst); err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(nr, project, scope, "zones", opType, name, stringField(inst, "targetLink"), requestID)), nil
}

// ─── Disk operations ──────────────────────────────────────────────────────────

func (p *Provider) DisksInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := accountID(nr)
	zone := scopeOf(nr)
	if zone == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing zone", 400)
	}
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, zone)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	body := bodyMap(nr)
	name := stringField(body, "name")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "disk name is required", 400)
	}
	if err := validateResourceName(name); err != nil {
		return nil, err
	}
	diskType := lastSegment(stringField(body, "type"))
	if diskType == "" {
		diskType = "pd-standard"
	}
	sizeGb := stringifyInt(body["sizeGb"])
	if sizeGb == "" {
		sizeGb = defaultDiskSizeGb
	}
	disk := map[string]any{
		"kind":              kindDisk,
		"id":                numericID(project + "/zones/" + zone + "/disks/" + name),
		"name":              name,
		"sizeGb":            sizeGb,
		"type":              absLink(nr, "compute-disk-type", zone+"/"+diskType),
		"zone":              absLink(nr, "compute-zone", zone),
		"status":            "READY",
		"creationTimestamp": formatTimestamp(clock.Now()),
		"selfLink":          absLink(nr, "compute-disk", zone+"/"+name),
		"targetLink":        absLink(nr, "compute-disk", zone+"/"+name),
	}
	if v, ok := body["description"]; ok && v != nil {
		disk["description"] = v
	}
	if v, ok := body["labels"]; ok && v != nil {
		disk["labels"] = v
	}
	data, err := json.Marshal(disk)
	if err != nil {
		return nil, err
	}
	err = p.resources.Create(ctx, project, zone, store.ResourceEntry{Type: rtDisk, ID: name, Data: data})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, model.NewProviderError("AlreadyExists", fmt.Sprintf("disk %q already exists", name), 409)
	}
	if err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(nr, project, zone, "zones", "insert", name, stringField(disk, "targetLink"), requestID)), nil
}

func (p *Provider) DisksGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "disk")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing disk", 400)
	}
	disk, err := p.loadMap(ctx, accountID(nr), scopeOf(nr), rtDisk, name, "disk")
	if err != nil {
		return nil, err
	}
	return provider.OK(disk), nil
}

func (p *Provider) DisksList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, accountID(nr), scopeOf(nr), rtDisk, "")
	if err != nil {
		return nil, err
	}
	items := mapsFromEntries(entries)
	items, err = applyFilterMaps(nr, items)
	if err != nil {
		return nil, err
	}
	params, err := listParams(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(items, func(m map[string]any) string { return stringField(m, "name") }, params)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"kind": kindDiskList, "items": toAnySlice(page)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) DisksDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "disk")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing disk", 400)
	}
	project, scope := accountID(nr), scopeOf(nr)
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, scope)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	disk, err := p.loadMap(ctx, project, scope, rtDisk, name, "disk")
	if err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, project, scope, rtDisk, name); err != nil {
		return nil, storeError(err, "disk")
	}
	return provider.OK(p.recordOperation(nr, project, scope, "zones", "delete", name, stringField(disk, "targetLink"), requestID)), nil
}

// ─── Network operations ───────────────────────────────────────────────────────

func (p *Provider) NetworksInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := accountID(nr)
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, store.GlobalRegion)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	body := bodyMap(nr)
	name := stringField(body, "name")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "network name is required", 400)
	}
	if err := validateResourceName(name); err != nil {
		return nil, err
	}
	auto := true
	if v, ok := body["autoCreateSubnetworks"].(bool); ok {
		auto = v
	}
	network := map[string]any{
		"kind":                  kindNetwork,
		"id":                    numericID(project + "/global/networks/" + name),
		"name":                  name,
		"autoCreateSubnetworks": auto,
		"creationTimestamp":     formatTimestamp(clock.Now()),
		"selfLink":              absLink(nr, "compute-network", name),
		"targetLink":            absLink(nr, "compute-network", name),
	}
	if v, ok := body["description"]; ok && v != nil {
		network["description"] = v
	}
	data, err := json.Marshal(network)
	if err != nil {
		return nil, err
	}
	err = p.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtNetwork, ID: name, Data: data})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, model.NewProviderError("AlreadyExists", fmt.Sprintf("network %q already exists", name), 409)
	}
	if err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(nr, project, store.GlobalRegion, "global", "insert", name, stringField(network, "targetLink"), requestID)), nil
}

func (p *Provider) NetworksGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "network")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing network", 400)
	}
	network, err := p.loadMap(ctx, accountID(nr), store.GlobalRegion, rtNetwork, name, "network")
	if err != nil {
		return nil, err
	}
	return provider.OK(network), nil
}

func (p *Provider) NetworksList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, accountID(nr), store.GlobalRegion, rtNetwork, "")
	if err != nil {
		return nil, err
	}
	items := mapsFromEntries(entries)
	items, err = applyFilterMaps(nr, items)
	if err != nil {
		return nil, err
	}
	params, err := listParams(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(items, func(m map[string]any) string { return stringField(m, "name") }, params)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"kind": kindNetworkList, "items": toAnySlice(page)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) NetworksDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "network")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing network", 400)
	}
	project := accountID(nr)
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, store.GlobalRegion)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	network, err := p.loadMap(ctx, project, store.GlobalRegion, rtNetwork, name, "network")
	if err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, project, store.GlobalRegion, rtNetwork, name); err != nil {
		return nil, storeError(err, "network")
	}
	return provider.OK(p.recordOperation(nr, project, store.GlobalRegion, "global", "delete", name, stringField(network, "targetLink"), requestID)), nil
}

// ─── Firewall operations ──────────────────────────────────────────────────────

func (p *Provider) FirewallsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := accountID(nr)
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, store.GlobalRegion)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	body := bodyMap(nr)
	name := stringField(body, "name")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "firewall name is required", 400)
	}
	if err := validateResourceName(name); err != nil {
		return nil, err
	}
	network := stringField(body, "network")
	if network == "" {
		network = "default"
	}
	fw := map[string]any{
		"kind":              kindFirewall,
		"id":                numericID(project + "/global/firewalls/" + name),
		"name":              name,
		"network":           normalizeNetwork(nr, network),
		"creationTimestamp": formatTimestamp(clock.Now()),
		"selfLink":          absLink(nr, "compute-firewall", name),
		"targetLink":        absLink(nr, "compute-firewall", name),
		"direction":         "INGRESS",
		"priority":          int64(1000),
		"disabled":          false,
	}
	if v, ok := body["allowed"]; ok && v != nil {
		fw["allowed"] = v
	}
	if v, ok := body["denied"]; ok && v != nil {
		fw["denied"] = v
	}
	if v, ok := body["sourceRanges"]; ok && v != nil {
		fw["sourceRanges"] = v
	}
	if v, ok := body["targetTags"]; ok && v != nil {
		fw["targetTags"] = v
	}
	if v, ok := body["description"]; ok && v != nil {
		fw["description"] = v
	}
	if v := stringField(body, "direction"); v != "" {
		fw["direction"] = v
	}
	data, err := json.Marshal(fw)
	if err != nil {
		return nil, err
	}
	err = p.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtFirewall, ID: name, Data: data})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, model.NewProviderError("AlreadyExists", fmt.Sprintf("firewall %q already exists", name), 409)
	}
	if err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(nr, project, store.GlobalRegion, "global", "insert", name, stringField(fw, "targetLink"), requestID)), nil
}

func (p *Provider) FirewallsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "firewall")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing firewall", 400)
	}
	fw, err := p.loadMap(ctx, accountID(nr), store.GlobalRegion, rtFirewall, name, "firewall")
	if err != nil {
		return nil, err
	}
	return provider.OK(fw), nil
}

func (p *Provider) FirewallsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, accountID(nr), store.GlobalRegion, rtFirewall, "")
	if err != nil {
		return nil, err
	}
	items := mapsFromEntries(entries)
	items, err = applyFilterMaps(nr, items)
	if err != nil {
		return nil, err
	}
	params, err := listParams(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(items, func(m map[string]any) string { return stringField(m, "name") }, params)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"kind": kindFirewallList, "items": toAnySlice(page)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) FirewallsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "firewall")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing firewall", 400)
	}
	project := accountID(nr)
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, store.GlobalRegion)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	fw, err := p.loadMap(ctx, project, store.GlobalRegion, rtFirewall, name, "firewall")
	if err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, project, store.GlobalRegion, rtFirewall, name); err != nil {
		return nil, storeError(err, "firewall")
	}
	return provider.OK(p.recordOperation(nr, project, store.GlobalRegion, "global", "delete", name, stringField(fw, "targetLink"), requestID)), nil
}

// ─── Subnetwork operations ────────────────────────────────────────────────────

func (p *Provider) SubnetworksInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := accountID(nr)
	region := scopeOf(nr)
	if region == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region", 400)
	}
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, region)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	body := bodyMap(nr)
	name := stringField(body, "name")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "subnetwork name is required", 400)
	}
	if err := validateResourceName(name); err != nil {
		return nil, err
	}
	network := stringField(body, "network")
	if network == "" {
		network = "default"
	}
	cidr := stringField(body, "ipCidrRange")
	if cidr == "" {
		cidr = "10.0.0.0/20"
	}
	sub := map[string]any{
		"kind":              kindSubnetwork,
		"id":                numericID(project + "/regions/" + region + "/subnetworks/" + name),
		"name":              name,
		"network":           normalizeNetwork(nr, network),
		"ipCidrRange":       cidr,
		"region":            absLink(nr, "compute-region", region),
		"gatewayAddress":    "10.0.0.1",
		"creationTimestamp": formatTimestamp(clock.Now()),
		"selfLink":          absLink(nr, "compute-subnetwork", region+"/"+name),
		"targetLink":        absLink(nr, "compute-subnetwork", region+"/"+name),
		"state":             "READY",
	}
	if v, ok := body["privateIpGoogleAccess"].(bool); ok {
		sub["privateIpGoogleAccess"] = v
	}
	if v, ok := body["description"]; ok && v != nil {
		sub["description"] = v
	}
	data, err := json.Marshal(sub)
	if err != nil {
		return nil, err
	}
	err = p.resources.Create(ctx, project, region, store.ResourceEntry{Type: rtSubnetwork, ID: name, Data: data})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, model.NewProviderError("AlreadyExists", fmt.Sprintf("subnetwork %q already exists", name), 409)
	}
	if err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(nr, project, region, "regions", "insert", name, stringField(sub, "targetLink"), requestID)), nil
}

func (p *Provider) SubnetworksGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "subnetwork")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing subnetwork", 400)
	}
	sub, err := p.loadMap(ctx, accountID(nr), scopeOf(nr), rtSubnetwork, name, "subnetwork")
	if err != nil {
		return nil, err
	}
	return provider.OK(sub), nil
}

func (p *Provider) SubnetworksList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, accountID(nr), scopeOf(nr), rtSubnetwork, "")
	if err != nil {
		return nil, err
	}
	items := mapsFromEntries(entries)
	items, err = applyFilterMaps(nr, items)
	if err != nil {
		return nil, err
	}
	params, err := listParams(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(items, func(m map[string]any) string { return stringField(m, "name") }, params)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"kind": kindSubnetworkList, "items": toAnySlice(page)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) SubnetworksDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "subnetwork")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing subnetwork", 400)
	}
	project, scope := accountID(nr), scopeOf(nr)
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, scope)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	sub, err := p.loadMap(ctx, project, scope, rtSubnetwork, name, "subnetwork")
	if err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, project, scope, rtSubnetwork, name); err != nil {
		return nil, storeError(err, "subnetwork")
	}
	return provider.OK(p.recordOperation(nr, project, scope, "regions", "delete", name, stringField(sub, "targetLink"), requestID)), nil
}

// ─── Machine type, zone, and region discovery ─────────────────────────────────

func (p *Provider) MachineTypesGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "machineType")
	zone := scopeOf(nr)
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing machineType", 400)
	}
	spec, ok := machineTypeSpecFor(name)
	if !ok {
		return nil, model.NewProviderError("NotFound", fmt.Sprintf("machine type %q not found", name), 404)
	}
	return provider.OK(machineTypeObject(nr, zone, spec)), nil
}

func (p *Provider) MachineTypesList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zone := scopeOf(nr)
	objs := make([]map[string]any, 0, len(machineTypeCatalog))
	for _, spec := range machineTypeCatalog {
		objs = append(objs, machineTypeObject(nr, zone, spec))
	}
	objs, err := applyFilterMaps(nr, objs)
	if err != nil {
		return nil, err
	}
	params, err := listParams(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(objs, func(m map[string]any) string { return stringField(m, "name") }, params)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"kind": kindMachineTypeList, "items": toAnySlice(page)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) ZonesGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "zone")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing zone", 400)
	}
	if !isKnownZone(name) {
		return nil, model.NewProviderError("NotFound", fmt.Sprintf("zone %q not found", name), 404)
	}
	return provider.OK(zoneObject(nr, name)), nil
}

func (p *Provider) ZonesList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	objs := make([]map[string]any, 0, len(zoneNames))
	for _, z := range zoneNames {
		objs = append(objs, zoneObject(nr, z))
	}
	objs, err := applyFilterMaps(nr, objs)
	if err != nil {
		return nil, err
	}
	params, err := listParams(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(objs, func(m map[string]any) string { return stringField(m, "name") }, params)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"kind": kindZoneList, "items": toAnySlice(page)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) RegionsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "region")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region", 400)
	}
	if !isKnownRegion(name) {
		return nil, model.NewProviderError("NotFound", fmt.Sprintf("region %q not found", name), 404)
	}
	return provider.OK(regionObject(nr, name)), nil
}

func (p *Provider) RegionsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	objs := make([]map[string]any, 0, len(regionNames))
	for _, r := range regionNames {
		objs = append(objs, regionObject(nr, r))
	}
	objs, err := applyFilterMaps(nr, objs)
	if err != nil {
		return nil, err
	}
	params, err := listParams(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(objs, func(m map[string]any) string { return stringField(m, "name") }, params)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"kind": kindRegionList, "items": toAnySlice(page)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// ─── Operation operations ─────────────────────────────────────────────────────

func (p *Provider) OperationsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	opID := strParam(nr, "operation")
	if opID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing operation", 400)
	}
	op, err := p.loadMap(ctx, accountID(nr), scopeOf(nr), rtOperation, opID, "operation")
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) OperationsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, accountID(nr), scopeOf(nr), rtOperation, "")
	if err != nil {
		return nil, err
	}
	items := mapsFromEntries(entries)
	items, err = applyFilterMaps(nr, items)
	if err != nil {
		return nil, err
	}
	params, err := listParams(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(items, func(m map[string]any) string { return stringField(m, "name") }, params)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"kind": kindOperationList, "items": toAnySlice(page)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// recordOperation builds Compute Engine's own Operation envelope
// (compute#operation) with status DONE and progress 100, persists it at scope
// so operations.get/list can read it back, and returns it for the caller to
// embed in the mutation response. When requestID is non-empty the operation
// carries Compute Engine's clientOperationId field (the requestId echoed back
// on the operation) and is remembered so a retry with the same token replays
// it.
func (p *Provider) recordOperation(nr *model.NormalizedRequest, project, scope, scopeType, opType, targetID, targetLink, requestID string) map[string]any {
	opID := randomHex(16)
	now := formatTimestamp(clock.Now())
	op := map[string]any{
		"kind":          kindOperation,
		"id":            numericID(opID),
		"name":          opID,
		"status":        "DONE",
		"operationType": opType,
		"targetId":      numericID(targetID),
		"targetLink":    targetLink,
		"targetProject": project,
		"selfLink":      absLink(nr, operationFormatter(scopeType), operationName(scope, scopeType, opID)),
		"insertTime":    now,
		"startTime":     now,
		"endTime":       now,
		"progress":      int64(100),
		"user":          serviceAccountEmail(project),
	}
	if requestID != "" {
		op["clientOperationId"] = requestID
	}
	switch scopeType {
	case "zones":
		op["zone"] = absLink(nr, "compute-zone", scope)
	case "regions":
		op["region"] = absLink(nr, "compute-region", scope)
	}
	data, err := json.Marshal(op)
	if err == nil {
		_ = p.resources.Upsert(context.Background(), project, scope, store.ResourceEntry{Type: rtOperation, ID: opID, Data: data})
	}
	p.rememberOperation(project, scope, requestID, op)
	return op
}

// beginMutation validates the requestId query parameter, takes the per-token
// mutation lock, and replays the operation recorded by an earlier mutation
// carrying the same token at the same scope. A non-nil op means the retried
// request is ignored and must be returned unchanged; the caller proceeds only
// when op is nil, and must `defer unlock()` (a no-op when no token was
// supplied). Holding the token lock across the mutation is what makes the
// documented replay guarantee hold even when a retry overlaps an in-flight
// original: the retry waits, then observes the recorded operation instead of
// racing the resource write. The returned requestID ("" when absent) is passed
// back to recordOperation so a fresh mutation is remembered. A malformed token
// fails loud with 400, matching the Compute Engine contract.
func (p *Provider) beginMutation(ctx context.Context, nr *model.NormalizedRequest, project, scope string) (map[string]any, string, func(), error) {
	requestID := strParam(nr, "requestId")
	if requestID == "" {
		return nil, "", func() {}, nil
	}
	if requestID == zeroRequestID || !requestIDRE.MatchString(requestID) {
		return nil, "", func() {}, model.NewProviderError("InvalidArgument",
			fmt.Sprintf("Invalid value for field 'requestId': '%s'. Must be a valid UUID and not the zero UUID", requestID), 400)
	}
	unlock := p.mutations.lock(project + "\x00" + scope + "\x00" + requestID)
	op, ok, err := p.replayOperation(ctx, project, scope, requestID)
	if err != nil {
		unlock()
		return nil, requestID, func() {}, err
	}
	if ok {
		unlock()
		return op, requestID, func() {}, nil
	}
	return nil, requestID, unlock, nil
}

// replayOperation returns the operation recorded by an earlier mutation that
// carried the same requestId at the same scope — the documented "ignore the
// request if it has already been completed" contract. ok is false when nothing
// was recorded yet. A store failure other than ErrNotFound is returned so a
// genuine retry is never mistaken for a fresh request.
func (p *Provider) replayOperation(ctx context.Context, project, scope, requestID string) (map[string]any, bool, error) {
	if requestID == "" {
		return nil, false, nil
	}
	e, err := p.resources.Get(ctx, project, scope, rtRequestID, requestID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var op map[string]any
	if json.Unmarshal(e.Data, &op) != nil || op == nil {
		return nil, false, nil
	}
	return op, true, nil
}

// rememberOperation records op as the operation for requestID at scope so a
// retry with the same token replays it. The first writer wins: a token that
// already has an operation is left untouched, so concurrent retries converge
// on the original operation rather than clobbering it.
func (p *Provider) rememberOperation(project, scope, requestID string, op map[string]any) {
	if requestID == "" {
		return
	}
	data, err := json.Marshal(op)
	if err != nil {
		return
	}
	_, _ = p.resources.UpsertAtomic(context.Background(), project, scope, rtRequestID, requestID,
		func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			if exists {
				return current, nil
			}
			return store.ResourceEntry{Type: rtRequestID, ID: requestID, Data: data}, nil
		})
}

// keyedMutex serializes work on a string key. The compute provider uses it to
// process same-requestId mutations one at a time, so a retry observes the
// original operation (and its recorded result) instead of racing the token
// reservation. Entries are reference-counted and dropped once unlocked.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

func newKeyedMutex() *keyedMutex { return &keyedMutex{locks: map[string]*keyedLock{}} }

// lock acquires the mutex for key and returns the release function.
func (k *keyedMutex) lock(key string) func() {
	if k == nil {
		return func() {}
	}
	k.mu.Lock()
	l, ok := k.locks[key]
	if !ok {
		l = &keyedLock{}
		k.locks[key] = l
	}
	l.refs++
	k.mu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		k.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}

// Unimplemented is the fail-loud handler for deferred Compute Engine surfaces.
// The codec routes every unclaimed /compute/v1/ path here so unknown methods
// return 501 rather than 404.
func (p *Provider) Unimplemented(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("Unimplemented", "operation is not supported by the Compute Engine emulator", 501)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// strParam reads a string path/query parameter.
func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// validateResourceName enforces Compute Engine's documented resource-name
// grammar (1-63 characters, [a-z]([-a-z0-9]*[a-z0-9])?). Real GCP rejects a
// malformed name with 400 before doing any work; the emulator did not, so an
// invalid name was silently accepted.
func validateResourceName(name string) error {
	if len(name) < 1 || len(name) > 63 || !resourceNameRE.MatchString(name) {
		return model.NewProviderError("InvalidArgument",
			fmt.Sprintf("Invalid value for field 'resource.name': '%s'. Must be 1-63 characters long and match the regular expression '[a-z]([-a-z0-9]*[a-z0-9])?'", name), 400)
	}
	return nil
}

func bodyMap(nr *model.NormalizedRequest) map[string]any {
	m, _ := nr.Params["body"].(map[string]any)
	return m
}

func stringField(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	s, _ := m[key].(string)
	return s
}

func accountID(nr *model.NormalizedRequest) string {
	if nr.AccountID != "" {
		return nr.AccountID
	}
	if p := strParam(nr, "project"); p != "" {
		return p
	}
	return "jaiscloud-project"
}

// scopeOf returns the store scope for the request: the zone for zonal
// resources, the region for regional resources, and store.GlobalRegion for
// global resources. The codec records it in Params["scope"]; the gateway's
// per-request Region ("global") is only a fallback.
func scopeOf(nr *model.NormalizedRequest) string {
	if s, _ := nr.Params["scope"].(string); s != "" {
		return s
	}
	if nr.Region != "" {
		return nr.Region
	}
	return store.GlobalRegion
}

// listParams maps Compute Engine's maxResults query parameter onto the shared
// paging helper's pageSize cursor, passing pageToken through unchanged. An
// absent maxResults takes Compute Engine's documented default of 500 (the
// shared helper's own default is 1000), and the documented 0-500 range is
// enforced. Compute Engine has no pageSize parameter, so a stray one is
// ignored. A value the Discovery document's uint32 binding cannot represent
// ("abc", "1.5", a negative, a number above uint32) is INVALID_ARGUMENT instead
// of a silently absent parameter.
func listParams(nr *model.NormalizedRequest) (map[string]any, error) {
	size, err := computePageSize(nr.Params["maxResults"])
	if err != nil {
		return nil, err
	}
	p := make(map[string]any, len(nr.Params)+1)
	for k, v := range nr.Params {
		p[k] = v
	}
	p["pageSize"] = size
	return p, nil
}

// computePageSize clamps Compute Engine's maxResults to its documented 0-500
// range; an absent or zero value takes the documented 500 default. A negative
// value is rejected by the uint32 binding the Discovery document declares
// (`maxResults` is an unsigned integer there), as is an unparseable or
// out-of-uint32 one, rather than being folded into the default page.
func computePageSize(v any) (int, error) {
	// Compute's query parameters hang off the frontend's `header` message, so
	// its binding errors name 'header.max_results' where the shared resolver
	// names 'max_results' (DC68); the field path is the resolver's name.
	n, err := paging.ParseUintSizeNamed(v, "header.maxResults")
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return defaultComputePageSize, nil
	}
	if n > maxComputePageSize {
		return maxComputePageSize, nil
	}
	return n, nil
}

// absLink builds an absolute selfLink/targetLink rooted at computeBasePath,
// formatting the relative resource name through the injected formatter.
func absLink(nr *model.NormalizedRequest, rt, name string) string {
	rel := name
	if nr.ResourceID != nil {
		rel = nr.ResourceID(rt, name)
	}
	return computeBasePath + rel
}

// operationFormatter selects the relative-name formatter for an operation's
// scope.
func operationFormatter(scopeType string) string {
	switch scopeType {
	case "zones":
		return "compute-zone-operation"
	case "regions":
		return "compute-region-operation"
	default:
		return "compute-global-operation"
	}
}

// operationName renders the "scope/op" (or bare op) argument consumed by the
// operation formatters.
func operationName(scope, scopeType, opID string) string {
	if scopeType == "global" {
		return opID
	}
	return scope + "/" + opID
}

// normalizeNetwork resolves a bare network name or partial path into the
// canonical global-networks URL.
func normalizeNetwork(nr *model.NormalizedRequest, network string) string {
	if strings.HasPrefix(network, "http://") || strings.HasPrefix(network, "https://") {
		return network
	}
	name := lastSegment(network)
	if name == "" {
		name = "default"
	}
	return absLink(nr, "compute-network", name)
}

// normalizeNetworkInterfaces keeps caller-supplied interfaces verbatim and
// synthesizes a single cosmetic nic0 otherwise.
func normalizeNetworkInterfaces(nr *model.NormalizedRequest, project, zone, name string, v any) any {
	if list, ok := v.([]any); ok && len(list) > 0 {
		return v
	}
	n := project + "/" + zone + "/" + name
	return []any{map[string]any{
		"kind":      "compute#networkInterface",
		"name":      "nic0",
		"network":   normalizeNetwork(nr, "default"),
		"networkIP": synthIP(n + "/nic0"),
		"accessConfigs": []any{map[string]any{
			"kind":  "compute#accessConfig",
			"name":  "external-nat",
			"type":  "ONE_TO_ONE_NAT",
			"natIP": synthIP(n + "/nat"),
		}},
	}}
}

func mapsFromEntries(entries []store.ResourceEntry) []map[string]any {
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil {
			out = append(out, m)
		}
	}
	return out
}

func toAnySlice(maps []map[string]any) []any {
	out := make([]any, 0, len(maps))
	for _, m := range maps {
		out = append(out, m)
	}
	return out
}

func (p *Provider) loadMap(ctx context.Context, account, scope, rt, id, what string) (map[string]any, error) {
	e, err := p.resources.Get(ctx, account, scope, rt, id)
	if err != nil {
		return nil, storeError(err, what)
	}
	var m map[string]any
	if err := json.Unmarshal(e.Data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// storeError translates store sentinels into GCP ProviderErrors.
func storeError(err error, what string) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return model.NewProviderError("NotFound", what+" not found", 404)
	case errors.Is(err, store.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", what+" already exists", 409)
	}
	return err
}

// ─── Synthesized discovery catalogs ───────────────────────────────────────────

type machineTypeSpec struct {
	Name  string
	CPUs  int64
	MemMB int64
}

// machineTypeCatalog is a small, stable subset of real Compute Engine machine
// types.
var machineTypeCatalog = []machineTypeSpec{
	{"e2-micro", 2, 1024},
	{"e2-small", 2, 2048},
	{"e2-medium", 2, 4096},
	{"f1-micro", 1, 614},
	{"g1-small", 1, 1740},
	{"n1-standard-1", 1, 3840},
	{"n1-standard-2", 2, 7680},
	{"n2-standard-2", 2, 8192},
	{"n2-standard-4", 4, 16384},
	{"c3-standard-4", 4, 16384},
}

func machineTypeSpecFor(name string) (machineTypeSpec, bool) {
	for _, s := range machineTypeCatalog {
		if s.Name == name {
			return s, true
		}
	}
	return machineTypeSpec{}, false
}

// zoneNames is the synthesized set of zones the emulator advertises. Real
// Compute Engine is available in more zones; this is a stable subset whose
// regions are all present in regionNames.
var zoneNames = []string{
	"us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f",
	"us-east1-b", "us-east1-c", "us-east1-d",
	"us-east4-a", "us-east4-b", "us-east4-c",
	"us-west1-a", "us-west1-b", "us-west1-c",
	"europe-west1-b", "europe-west1-c", "europe-west1-d",
	"europe-west4-a", "europe-west4-b", "europe-west4-c",
	"asia-east1-a", "asia-east1-b", "asia-east1-c",
	"asia-southeast1-a", "asia-southeast1-b", "asia-southeast1-c",
}

// regionNames is the sorted set of regions derived from zoneNames.
var regionNames = deriveRegions()

func deriveRegions() []string {
	seen := map[string]bool{}
	var out []string
	for _, z := range zoneNames {
		r := regionFromZone(z)
		if r != "" && !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	sort.Strings(out)
	return out
}

func regionFromZone(zone string) string {
	if i := strings.LastIndexByte(zone, '-'); i > 0 {
		return zone[:i]
	}
	return zone
}

func isKnownZone(name string) bool {
	for _, z := range zoneNames {
		if z == name {
			return true
		}
	}
	return false
}

func isKnownRegion(name string) bool {
	for _, r := range regionNames {
		if r == name {
			return true
		}
	}
	return false
}

func zoneObject(nr *model.NormalizedRequest, name string) map[string]any {
	return map[string]any{
		"kind":              kindZone,
		"id":                numericID("zone/" + name),
		"name":              name,
		"status":            "UP",
		"region":            absLink(nr, "compute-region", regionFromZone(name)),
		"selfLink":          absLink(nr, "compute-zone", name),
		"creationTimestamp": "2014-01-01T00:00:00.000-07:00",
		"availableCpuPlatforms": []string{
			"Intel Skylake", "Intel Broadwell", "Intel Haswell", "AMD Rome",
		},
	}
}

func regionObject(nr *model.NormalizedRequest, name string) map[string]any {
	zones := make([]any, 0, 4)
	for _, z := range zonesInRegion(name) {
		zones = append(zones, absLink(nr, "compute-zone", z))
	}
	return map[string]any{
		"kind":              kindRegion,
		"id":                numericID("region/" + name),
		"name":              name,
		"status":            "UP",
		"selfLink":          absLink(nr, "compute-region", name),
		"creationTimestamp": "2014-01-01T00:00:00.000-07:00",
		"zones":             zones,
		"quotas": []any{
			map[string]any{"metric": "CPUS", "limit": float64(24), "usage": float64(0)},
			map[string]any{"metric": "DISKS_TOTAL_GB", "limit": float64(4096), "usage": float64(0)},
			map[string]any{"metric": "STATIC_ADDRESSES", "limit": float64(8), "usage": float64(0)},
			map[string]any{"metric": "IN_USE_ADDRESSES", "limit": float64(8), "usage": float64(0)},
		},
	}
}

func zonesInRegion(region string) []string {
	var out []string
	for _, z := range zoneNames {
		if regionFromZone(z) == region {
			out = append(out, z)
		}
	}
	return out
}

func machineTypeObject(nr *model.NormalizedRequest, zone string, spec machineTypeSpec) map[string]any {
	return map[string]any{
		"kind":                         kindMachineType,
		"id":                           numericID("machineType/" + zone + "/" + spec.Name),
		"name":                         spec.Name,
		"zone":                         absLink(nr, "compute-zone", zone),
		"selfLink":                     absLink(nr, "compute-machine-type", zone+"/"+spec.Name),
		"creationTimestamp":            "2014-01-01T00:00:00.000-07:00",
		"description":                  spec.Name + " machine type",
		"guestCpus":                    spec.CPUs,
		"memoryMb":                     spec.MemMB,
		"imageSpaceGb":                 int64(10),
		"maximumPersistentDisks":       int64(128),
		"maximumPersistentDisksSizeGb": "263168",
	}
}

// cpuPlatform derives a stable CPU platform label from the machine type family.
func cpuPlatform(machineType string) string {
	family := machineType
	if i := strings.IndexAny(family, "-"); i > 0 {
		family = family[:i]
	}
	switch family {
	case "e2":
		return "Intel Broadwell"
	case "n2", "c3":
		return "Intel Ice Lake"
	case "n1":
		return "Intel Haswell"
	default:
		return "Intel Broadwell"
	}
}

// lastSegment returns the final path segment of a URL or relative path.
func lastSegment(s string) string {
	if s == "" {
		return ""
	}
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[i+1:]
	}
	return s
}

func stringifyInt(v any) string {
	switch n := v.(type) {
	case string:
		return n
	case float64:
		return strconv.FormatInt(int64(n), 10)
	case int:
		return strconv.Itoa(n)
	case int64:
		return strconv.FormatInt(n, 10)
	case json.Number:
		return n.String()
	}
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// contentEtag derives a stable fingerprint from a resource name. Discovery
// declares the fields it feeds (Metadata.fingerprint, Instance.fingerprint)
// as format:byte, i.e. base64-encoded bytes, so the digest is base64-encoded
// rather than rendered as hex.
func contentEtag(seed string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// numericID derives a stable decimal id from a string.
func numericID(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	n := h.Sum64()
	if n == 0 {
		n = 1
	}
	return strconv.FormatUint(n, 10)
}

// serviceAccountEmail synthesizes the project's Compute Engine service account.
func serviceAccountEmail(project string) string {
	return numericID(project) + "-compute@developer.gserviceaccount.com"
}

// synthIP derives a stable cosmetic IPv4 address from a seed. Nothing listens
// on it; a real instance address is allocated from the network's range.
func synthIP(seed string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))
	n := h.Sum32()
	return fmt.Sprintf("10.%d.%d.%d", (n>>16)&0xff, (n>>8)&0xff, n&0xff)
}

func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}
