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
// The synthesized discovery catalogs are zones, regions, machine types, the
// project resource, and public images (images.get / images.getFromFamily, which
// answer a derived READY image for the projects Google publishes public images
// from — there is no image registry and no image bytes).
//
// Deferred surfaces (addresses, snapshots, instance groups/templates, routers,
// backend services, images.insert/list/delete and the image write methods, and
// every custom method such as setMetadata, attachDisk, or reset) resolve to the
// Unimplemented action and fail loud with 501 rather than 404-ing.
package compute

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
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
	"jaiscloud/internal/gcp/reference"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource types in the shared ResourceStore.
const (
	rtInstance   = "gcp_compute_instance"
	rtDisk       = "gcp_compute_disk"
	rtNetwork    = reference.ComputeNetworkType
	rtFirewall   = "gcp_compute_firewall"
	rtSubnetwork = "gcp_compute_subnetwork"
	rtOperation  = "gcp_compute_operation"
	// rtProjectMetadata holds a project's common instance metadata: the items
	// projects.setCommonInstanceMetadata wrote plus the fingerprint and the
	// write generation that the optimistic-concurrency check compares against.
	// It is keyed by the project id at store.GlobalRegion.
	rtProjectMetadata = "gcp_compute_project_metadata"
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
	kindImage                  = "compute#image"
	kindMachineType            = "compute#machineType"
	kindMachineTypeList        = "compute#machineTypeList"
	kindZone                   = "compute#zone"
	kindZoneList               = "compute#zoneList"
	kindRegion                 = "compute#region"
	kindRegionList             = "compute#regionList"
	kindOperation              = "compute#operation"
	kindOperationList          = "compute#operationList"
	kindProject                = "compute#project"
	kindMetadata               = "compute#metadata"
)

// catalogCreationTimestamp is the synthesized creation time the emulator stamps
// on the discovery-catalog objects it derives rather than stores (zones,
// regions, machine types, and the project itself): they have no creation event,
// so the wire carries one stable origin.
const catalogCreationTimestamp = "2014-01-01T00:00:00.000-07:00"

// computeBasePath is the public Compute Engine REST root. selfLink/targetLink
// are absolute URLs rooted here.
const computeBasePath = "https://www.googleapis.com/compute/v1/"

// defaultDiskSizeGb is used when a disk create request omits sizeGb.
const defaultDiskSizeGb = "10"

// defaultDiskPhysicalBlockSizeBytes is the physical block size a disk reports
// when the create request omits it: Discovery (Disk.physicalBlockSizeBytes)
// documents a server-side default, and 4096 is the value real Compute reports
// for the pd-* types. It is Optional+Computed in the hashicorp/google provider,
// so a disk created without it must read the default back rather than nothing
// (an absent value reads as 0 and permadiffs the ForceNew field).
const defaultDiskPhysicalBlockSizeBytes = "4096"

// defaultNetworkMtu is the MTU a network reports when the create request omits
// it: Discovery (Network.mtu) documents "If unspecified, the value defaults to
// 1460". The hashicorp/google provider reads mtu back (Optional+Computed) and
// records an absent response as 0, so a network created with an explicit mtu
// re-plans as `mtu = 0 -> <configured>` on every plan (TF2-17).
const defaultNetworkMtu = 1460

// minNetworkMtu/maxNetworkMtu bound a caller-supplied mtu. Discovery
// (Network.mtu): "The minimum value for this field is 1300 and the maximum value
// is 8896 bytes (jumbo frames)."
const (
	minNetworkMtu = 1300
	maxNetworkMtu = 8896
)

// defaultNetworkFirewallPolicyEnforcementOrder is the order a network reports
// when the create request omits it: Discovery
// (Network.networkFirewallPolicyEnforcementOrder) documents "Defaults to
// AFTER_CLASSIC_FIREWALL if the field is not specified", and the
// hashicorp/google provider gives the attribute a Default of the same value. A
// network that never sets it must therefore read the value back rather than
// nothing (an absent value re-plans as
// `+ network_firewall_policy_enforcement_order` on every plan).
const defaultNetworkFirewallPolicyEnforcementOrder = "AFTER_CLASSIC_FIREWALL"

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

// maxMetadataValueBytes is the documented size ceiling of one metadata entry's
// value (Discovery, Metadata.items[].value: "its size must be less than or equal
// to 262144 bytes (256 KiB)").
const maxMetadataValueBytes = 262144

// maxProjectMetadataBytes is the documented total size of a project metadata
// envelope (Discovery, Metadata.items: "The total size of all keys and values
// must be less than 512 KB").
const maxProjectMetadataBytes = 512 * 1024

// metadataKeyRE is the documented metadata-key grammar (Discovery,
// Metadata.items[].key: pattern [a-zA-Z0-9-_]{1,128}).
var metadataKeyRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

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
		"Compute.ProjectsGet":                       p.ProjectsGet,
		"Compute.ProjectsSetCommonInstanceMetadata": p.ProjectsSetCommonInstanceMetadata,

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
		"Compute.NetworksPatch":  p.NetworksPatch,
		"Compute.NetworksDelete": p.NetworksDelete,

		"Compute.FirewallsInsert": p.FirewallsInsert,
		"Compute.FirewallsGet":    p.FirewallsGet,
		"Compute.FirewallsList":   p.FirewallsList,
		"Compute.FirewallsDelete": p.FirewallsDelete,

		"Compute.SubnetworksInsert": p.SubnetworksInsert,
		"Compute.SubnetworksGet":    p.SubnetworksGet,
		"Compute.SubnetworksList":   p.SubnetworksList,
		"Compute.SubnetworksDelete": p.SubnetworksDelete,

		"Compute.ImagesGet":           p.ImagesGet,
		"Compute.ImagesGetFromFamily": p.ImagesGetFromFamily,

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

// ─── Project operations ───────────────────────────────────────────────────────

// ProjectsGet renders the synthesized compute#project (compute.projects.get,
// GET /compute/v1/projects/{project}). Compute is a metadata-only surface with
// no project registry: like Resource Manager's projects.get the emulator answers
// an ACTIVE project for any id it is asked about, and the domain-wide identity
// is the synthesized resource.ProjectNumber every other surface shares. Two
// fields are load-bearing for real consumers:
//
//   - defaultServiceAccount is the project's Compute Engine default service
//     account. The hashicorp/google `google_compute_default_service_account`
//     data source reads exactly this field, and the Dataproc module and the
//     scheduler examples resolve their IAM grant from it (TF2-1).
//   - commonInstanceMetadata carries the project-wide metadata envelope. It is
//     the record projects.setCommonInstanceMetadata writes (items plus the
//     server fingerprint), or the empty default with a stable fingerprint when
//     nothing has been written yet.
func (p *Provider) ProjectsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// The response describes the project named in the request path. Path and
	// account agree for a wire request (the gateway derives AccountID from the
	// same segment), but the resource *is* the path project, so it wins.
	project := strParam(nr, "project")
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "project is required", 400)
	}
	rec, err := p.projectMetadataRecord(ctx, project)
	if err != nil {
		return nil, err
	}
	return provider.OK(projectObject(nr, project, rec.metadata())), nil
}

// ProjectsSetCommonInstanceMetadata applies compute.projects.setCommonInstanceMetadata
// (POST /compute/v1/projects/{project}/setCommonInstanceMetadata), the method
// the hashicorp/google `google_compute_project_metadata` and
// `google_compute_project_metadata_item` resources use to write the
// project-wide metadata (TF1-5).
//
// The request body is Discovery's Metadata envelope. `items` replaces the
// project's whole metadata set — the provider reads the project first, merges
// its own keys in and sends the result — so an absent (or null) `items` clears
// it, which is also how the provider's destroy path removes the metadata. Each
// entry is validated against the documented key grammar and size ceilings; a
// malformed entry is 400 instead of being stored.
//
// `fingerprint` is the optimistic-concurrency token Discovery describes: it is
// optional (the schema carries no required annotation for it), and when present
// it must be the project's current fingerprint — anything else is the
// documented 412 (Metadata.fingerprint: "otherwise the request will fail with
// error412 conditionNotMet"). The provider always sends the fingerprint it read
// from projects.get. A fingerprint that is not a base64 byte string cannot be
// the API's `format: byte` value at all, so it is 400 INVALID_ARGUMENT.
//
// The response is Compute Engine's own DONE Operation envelope (the provider
// polls it through ComputeOperationWaitTime) and the stored metadata is
// readable through projects.get, with a fingerprint that changes on every
// write (Discovery: "changes after every request to modify or update metadata")
// even when the items are byte-identical.
func (p *Provider) ProjectsSetCommonInstanceMetadata(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := strParam(nr, "project")
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "project is required", 400)
	}
	replay, requestID, unlock, err := p.beginMutation(ctx, nr, project, store.GlobalRegion)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if replay != nil {
		return provider.OK(replay), nil
	}
	body := bodyMap(nr)
	items, err := projectMetadataItems(body["items"])
	if err != nil {
		return nil, err
	}
	fingerprint := stringField(body, "fingerprint")
	if fingerprint != "" {
		if _, err := base64.StdEncoding.DecodeString(fingerprint); err != nil {
			return nil, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("Invalid value for field 'resource.fingerprint': '%s'. A fingerprint is a base64-encoded byte string; call projects.get to retrieve the latest one", fingerprint), 400)
		}
	}
	// The read-modify-write is atomic: a concurrent write with the same
	// fingerprint must lose the precondition rather than silently clobber the
	// winner's items.
	_, err = p.resources.UpsertAtomic(ctx, project, store.GlobalRegion, rtProjectMetadata, project,
		func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			rec := newProjectMetadataRecord(project)
			if exists {
				stored, perr := parseProjectMetadataRecord(project, current.Data)
				if perr != nil {
					return store.ResourceEntry{}, perr
				}
				rec = stored
			}
			if fingerprint != "" && fingerprint != rec.Fingerprint {
				return store.ResourceEntry{}, model.NewProviderError("PreconditionFailed",
					fmt.Sprintf("Invalid fingerprint: the supplied fingerprint '%s' does not match the project's current fingerprint '%s'; call projects.get to retrieve the latest one", fingerprint, rec.Fingerprint), 412)
			}
			rec.Items = items
			rec.Generation++
			rec.Fingerprint = projectMetadataFingerprint(project, rec.Generation)
			data, err := json.Marshal(rec)
			if err != nil {
				return store.ResourceEntry{}, err
			}
			return store.ResourceEntry{Type: rtProjectMetadata, ID: project, Data: data}, nil
		})
	if err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(nr, project, store.GlobalRegion, "global", "setCommonInstanceMetadata", project, absLink(nr, "compute-project", project), requestID)), nil
}

// projectMetadataRecord is the persisted common-instance metadata of one
// project. Fingerprint is derived from Generation rather than from the items so
// Discovery's "changes after every request to modify or update metadata" holds
// even for a write that repeats the previous items (a provider re-plan).
type projectMetadataRecord struct {
	Items       []any  `json:"items"`
	Generation  int64  `json:"generation"`
	Fingerprint string `json:"fingerprint"`
}

// newProjectMetadataRecord is the record a project that has never had its
// metadata written reports: no items and the stable default fingerprint.
func newProjectMetadataRecord(project string) projectMetadataRecord {
	return projectMetadataRecord{Items: []any{}, Fingerprint: defaultProjectMetadataFingerprint(project)}
}

// metadata renders the record as Discovery's compute#metadata envelope.
func (r projectMetadataRecord) metadata() map[string]any {
	items := r.Items
	if items == nil {
		items = []any{}
	}
	return map[string]any{"kind": kindMetadata, "fingerprint": r.Fingerprint, "items": items}
}

// projectMetadataRecord loads a project's stored metadata, falling back to the
// never-written default.
func (p *Provider) projectMetadataRecord(ctx context.Context, project string) (projectMetadataRecord, error) {
	e, err := p.resources.Get(ctx, project, store.GlobalRegion, rtProjectMetadata, project)
	if errors.Is(err, store.ErrNotFound) {
		return newProjectMetadataRecord(project), nil
	}
	if err != nil {
		return projectMetadataRecord{}, err
	}
	return parseProjectMetadataRecord(project, e.Data)
}

// parseProjectMetadataRecord decodes a stored record, filling in the never-written
// defaults a record written by an older build (or a hand-written one) may lack.
func parseProjectMetadataRecord(project string, data []byte) (projectMetadataRecord, error) {
	rec := newProjectMetadataRecord(project)
	if err := json.Unmarshal(data, &rec); err != nil {
		return projectMetadataRecord{}, err
	}
	if rec.Items == nil {
		rec.Items = []any{}
	}
	if rec.Fingerprint == "" {
		rec.Fingerprint = defaultProjectMetadataFingerprint(project)
	}
	return rec, nil
}

// defaultProjectMetadataFingerprint is the fingerprint a project that has never
// had its common instance metadata written reports. It stays byte-identical to
// the value projects.get reported before TF1-5 so an untouched project does not
// churn between reads.
func defaultProjectMetadataFingerprint(project string) string {
	return contentEtag("project/" + project + "/commonInstanceMetadata")
}

// projectMetadataFingerprint derives the fingerprint a project reports after its
// generation-th write. The generation is part of the seed because Discovery
// documents that the fingerprint changes after every write, including one that
// repeats the previous items.
func projectMetadataFingerprint(project string, generation int64) string {
	return contentEtag(fmt.Sprintf("project/%s/commonInstanceMetadata/%d", project, generation))
}

// projectMetadataItems validates and normalizes a setCommonInstanceMetadata
// request body's items. Discovery marks an entry's key and value required and
// gives the key its grammar and both a per-value and a total size ceiling, so a
// body that violates them is 400 before anything is stored. An absent (or null)
// items list is the empty set, which clears the metadata.
func projectMetadataItems(v any) ([]any, error) {
	if v == nil {
		return []any{}, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, model.NewProviderError("InvalidArgument",
			"Invalid value for field 'resource.items': must be a list of key/value entries", 400)
	}
	out := make([]any, 0, len(list))
	total := 0
	for i, raw := range list {
		item, ok := raw.(map[string]any)
		if !ok {
			return nil, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("Invalid value for field 'resource.items[%d]': must be an object with a key and a value", i), 400)
		}
		key, _ := item["key"].(string)
		if key == "" {
			return nil, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("Invalid value for field 'resource.items[%d].key': a metadata key is required", i), 400)
		}
		if !metadataKeyRE.MatchString(key) {
			return nil, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("Invalid value for field 'resource.items[%d].key': '%s'. Must be 1-128 characters long and match the regular expression '[a-zA-Z0-9-_]{1,128}'", i, key), 400)
		}
		value, ok := item["value"].(string)
		if !ok {
			return nil, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("Invalid value for field 'resource.items[%d].value': a metadata value is required", i), 400)
		}
		if len(value) > maxMetadataValueBytes {
			return nil, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("Invalid value for field 'resource.items[%d].value': the value of key '%s' is %d bytes; metadata values must be at most %d bytes (256 KiB)", i, key, len(value), maxMetadataValueBytes), 400)
		}
		total += len(key) + len(value)
		out = append(out, map[string]any{"key": key, "value": value})
	}
	if total >= maxProjectMetadataBytes {
		return nil, model.NewProviderError("InvalidArgument",
			fmt.Sprintf("Invalid value for field 'resource.items': the total size of all keys and values is %d bytes; project metadata must be less than %d bytes (512 KiB)", total, maxProjectMetadataBytes), 400)
	}
	return out, nil
}

// projectNumber returns the number a project is addressed by: the synthesized
// resource.ProjectNumber for a project id, and the reference itself when the
// caller addressed the project by number (Discovery's project parameter accepts
// both). The synthesized number is not invertible, so a number is taken at face
// value — which is what real Compute reports back as the Project's id.
func projectNumber(project string) string {
	return resource.ProjectNumberFor(project)
}

// projectObject builds the compute#project payload.
func projectObject(nr *model.NormalizedRequest, name string, commonInstanceMetadata map[string]any) map[string]any {
	number := projectNumber(name)
	return map[string]any{
		"kind":                   kindProject,
		"id":                     number,
		"name":                   name,
		"selfLink":               absLink(nr, "compute-project", name),
		"creationTimestamp":      catalogCreationTimestamp,
		"commonInstanceMetadata": commonInstanceMetadata,
		"defaultServiceAccount":  defaultServiceAccountForNumber(number),
		// Real Compute reports PREMIUM as the default network tier and
		// ZONAL_ONLY as the default internal-DNS setting for a project created
		// with the current defaults (the project schema marks both output-only).
		"defaultNetworkTier": "PREMIUM",
		"vmDnsSetting":       "ZONAL_ONLY",
		// A project that is not the host of a shared VPC reports the
		// unspecified member of the output-only xpnProjectStatus enum.
		"xpnProjectStatus": "UNSPECIFIED_XPN_PROJECT_STATUS",
		"quotas": []any{
			map[string]any{"metric": "CPUS", "limit": float64(24), "usage": float64(0)},
			map[string]any{"metric": "DISKS_TOTAL_GB", "limit": float64(4096), "usage": float64(0)},
			map[string]any{"metric": "STATIC_ADDRESSES", "limit": float64(8), "usage": float64(0)},
			map[string]any{"metric": "IN_USE_ADDRESSES", "limit": float64(8), "usage": float64(0)},
		},
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
		inst["metadata"] = map[string]any{"kind": kindMetadata, "fingerprint": contentEtag(name)}
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
	blockSizeBytes := stringifyInt(body["physicalBlockSizeBytes"])
	if blockSizeBytes == "" {
		blockSizeBytes = defaultDiskPhysicalBlockSizeBytes
	}
	disk := map[string]any{
		"kind":                   kindDisk,
		"id":                     numericID(project + "/zones/" + zone + "/disks/" + name),
		"name":                   name,
		"sizeGb":                 sizeGb,
		"physicalBlockSizeBytes": blockSizeBytes,
		"type":                   absLink(nr, "compute-disk-type", zone+"/"+diskType),
		"zone":                   absLink(nr, "compute-zone", zone),
		"status":                 "READY",
		"creationTimestamp":      formatTimestamp(clock.Now()),
		"selfLink":               absLink(nr, "compute-disk", zone+"/"+name),
		"targetLink":             absLink(nr, "compute-disk", zone+"/"+name),
	}
	if v, ok := body["description"]; ok && v != nil {
		disk["description"] = v
	}
	if v, ok := body["labels"]; ok && v != nil {
		disk["labels"] = v
	}
	// Persisted caller inputs are echoed on read. Each is part of the published
	// Disk schema and the hashicorp/google provider reads it back; most are
	// ForceNew, so a value the create carried but the record dropped re-plans as
	// a replacement on every plan (TF2-11-3). The decision per field is caller
	// value vs documented server default: only physicalBlockSizeBytes documents
	// a server default (above), so every field below is echoed only when the
	// request carried it. A synthesized value would permadiff a config that
	// omits it (architecture and storage_pool are not Computed in the provider)
	// and invent state the caller never sent.
	//
	// Deliberately not echoed: params (Discovery: "input only … not persisted as
	// part of resource payload"). The three CustomerEncryptionKey blocks are
	// echoed in their server-visible shape only — see below.
	for _, field := range []string{
		"accessMode",
		"architecture",
		"asyncPrimaryDisk",
		"enableConfidentialCompute",
		"guestOsFeatures",
		"licenses",
		"replicaZones",
		"resourcePolicies",
		"sourceDisk",
		"sourceInstantSnapshot",
		"sourceSnapshot",
		"sourceStorageObject",
		"storagePool",
	} {
		if v, ok := body[field]; ok && v != nil {
			disk[field] = v
		}
	}
	// These are int64 on the wire (the Go client encodes them as JSON strings
	// via `,string`), so normalize a numeric body to the decimal string the rest
	// of the disk record uses.
	for _, field := range []string{"provisionedIops", "provisionedThroughput"} {
		if v := stringifyInt(body[field]); v != "" {
			disk[field] = v
		}
	}
	// The three CustomerEncryptionKey blocks are persisted in the shape real
	// Compute returns, never verbatim (TF2-11-4): a CMEK block echoes the key
	// name and service account, a CSEK block only the documented sha256. Raw and
	// RSA-wrapped key material is request-only and must never be stored or
	// echoed. hashicorp/google reads the block back into its ForceNew
	// disk_encryption_key / source_*_encryption_key blocks, so without the echo
	// a disk created with a CMEK key re-plans "forces replacement" on every
	// plan.
	for _, field := range []string{"diskEncryptionKey", "sourceImageEncryptionKey", "sourceSnapshotEncryptionKey"} {
		v, ok := body[field]
		if !ok || v == nil {
			continue
		}
		key, err := encryptionKeyResponse(field, v)
		if err != nil {
			return nil, err
		}
		if key != nil {
			disk[field] = key
		}
	}
	// A disk created from an image records the image it resolved to, exactly as
	// real Compute does: a family reference resolves to the family's concrete
	// build and every spelling is stored as the image's absolute selfLink, so a
	// read reports the canonical link. The provider compares that link back
	// against the configured `image` (DiskImageDiffSuppress understands every
	// documented spelling but only when the read is the absolute selfLink), so a
	// disk created from a family would otherwise permadiff and be replaced on
	// every plan.
	if v, ok := body["sourceImage"]; ok && v != nil {
		ref, isString := v.(string)
		if !isString {
			return nil, model.NewProviderError("InvalidArgument",
				"Invalid value for field 'resource.sourceImage': must be a string", 400)
		}
		if ref != "" {
			link, id, err := resolveImageReference(project, ref)
			if err != nil {
				return nil, err
			}
			disk["sourceImage"] = link
			disk["sourceImageId"] = id
		}
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
	firewallOrder, err := networkFirewallPolicyEnforcementOrder(body, defaultNetworkFirewallPolicyEnforcementOrder)
	if err != nil {
		return nil, err
	}
	mtu, err := networkMtu(body, defaultNetworkMtu)
	if err != nil {
		return nil, err
	}
	network := map[string]any{
		"kind":                                  kindNetwork,
		"id":                                    numericID(project + "/global/networks/" + name),
		"name":                                  name,
		"autoCreateSubnetworks":                 auto,
		"mtu":                                   mtu,
		"networkFirewallPolicyEnforcementOrder": firewallOrder,
		"creationTimestamp":                     formatTimestamp(clock.Now()),
		"selfLink":                              absLink(nr, "compute-network", name),
		"targetLink":                            absLink(nr, "compute-network", name),
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

// NetworksPatch applies a partial update to a network (compute.networks.patch,
// PATCH /compute/v1/projects/{project}/global/networks/{network}) and returns
// the operation envelope Discovery declares for it. This is the mutation path
// Terraform's google_compute_network update uses: the provider PATCHes the
// network whenever routing_mode, a bgp_* field, the MTU or the firewall-policy
// enforcement order changes, then polls the returned operation (TF2-17-1).
// Discovery's own method description names only routingConfig, but the field set
// `gcloud compute networks update` and the provider's update path actually send
// is mtu, networkFirewallPolicyEnforcementOrder, routingConfig and the ULA IPv6
// pair — the first two are the ones this record stores.
//
// The body is a merge patch, not a replacement: an absent (or JSON-null) field
// keeps its stored value, so a PATCH that carries only mtu cannot reset the
// enforcement order. The fields the record does not keep (routingConfig/BGP,
// networkProfile, the ULA IPv6 pair) are accepted and dropped, the same boundary
// the create path has (TF2-17-2). Identity fields — name, id, description,
// autoCreateSubnetworks — are not patchable by any published client (the provider
// marks name, description and auto mode ForceNew and recreates instead), so a
// body that carries one is ignored rather than modeling a change real Compute
// does not offer through this method.
func (p *Provider) NetworksPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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
	body := bodyMap(nr)
	// A merge patch keeps an absent field: the fallback is the stored value, not
	// the create-time default, so a PATCH that names only the enforcement order
	// cannot reset mtu. A JSON null is treated like an absent field.
	currentMtu := defaultNetworkMtu
	if n, ok := network["mtu"].(float64); ok {
		currentMtu = int(n)
	}
	mtu, err := networkMtu(body, currentMtu)
	if err != nil {
		return nil, err
	}
	network["mtu"] = mtu
	currentOrder := stringField(network, "networkFirewallPolicyEnforcementOrder")
	if currentOrder == "" {
		currentOrder = defaultNetworkFirewallPolicyEnforcementOrder
	}
	order, err := networkFirewallPolicyEnforcementOrder(body, currentOrder)
	if err != nil {
		return nil, err
	}
	network["networkFirewallPolicyEnforcementOrder"] = order
	data, err := json.Marshal(network)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Update(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtNetwork, ID: name, Data: data}); err != nil {
		return nil, storeError(err, "network")
	}
	return provider.OK(p.recordOperation(nr, project, store.GlobalRegion, "global", "patch", name, stringField(network, "targetLink"), requestID)), nil
}

// networkMtu reads a request body's mtu, falling back to fallback when the field
// is absent or null, and enforces the range Discovery documents for it. Both the
// create and the patch path share it: real Compute validates mtu the same way
// whether it arrives in an insert or a patch.
func networkMtu(body map[string]any, fallback int) (int, error) {
	v, ok := body["mtu"]
	if !ok || v == nil {
		return fallback, nil
	}
	n, err := strconv.Atoi(stringifyInt(v))
	if err != nil || n < minNetworkMtu || n > maxNetworkMtu {
		return 0, model.NewProviderError("InvalidArgument",
			fmt.Sprintf("Invalid value for field 'resource.mtu': '%v'. Must be between %d and %d", v, minNetworkMtu, maxNetworkMtu), 400)
	}
	return n, nil
}

// networkFirewallPolicyEnforcementOrder reads a request body's order, falling
// back to fallback when the field is absent or null, and enforces the enum
// Discovery documents for it.
func networkFirewallPolicyEnforcementOrder(body map[string]any, fallback string) (string, error) {
	v, ok := body["networkFirewallPolicyEnforcementOrder"]
	if !ok || v == nil {
		return fallback, nil
	}
	order, _ := v.(string)
	switch order {
	case "AFTER_CLASSIC_FIREWALL", "BEFORE_CLASSIC_FIREWALL":
		return order, nil
	}
	return "", model.NewProviderError("InvalidArgument",
		fmt.Sprintf("Invalid value for field 'resource.networkFirewallPolicyEnforcementOrder': '%v'. Must be one of AFTER_CLASSIC_FIREWALL or BEFORE_CLASSIC_FIREWALL", v), 400)
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

// ─── Image catalog ────────────────────────────────────────────────────────────

// Compute images are a *derived* catalog here, like zone/region/machine-type
// discovery: the emulator keeps no image registry (images.insert/list/delete are
// not served and no image bytes exist), so a read answers a synthesized READY
// public image instead of a stored record.
//
// Only the projects real GCP publishes public images from resolve. That
// restriction is load-bearing for the hashicorp/google provider, which resolves
// an `image` reference by *probing*: it asks for a concrete image first and only
// falls back to reading the same name as a *family* when that probe answers 404
// ("debian-cloud/debian-12" is a family, not an image, so a permissive 200 for
// every name would skip the family lookup and change the sourceImage it
// records). An image read in any other project is therefore a 404, exactly as
// it is in a project that has no images.
//
// A name is served as a concrete image when it carries a real public-build
// label:
//   - "-vYYYYMMDD" (debian-12-bookworm-v20241009, windows-server-2019-dc-v20241008,
//     sles-15-sp5-v20240109-x86-64), or
//   - Cloud COS's milestone/build/patch/revision form
//     (cos-stable-109-17800-147-54).
//
// images.getFromFamily accepts any RFC1035 family name in a public image
// project: the real families cannot be enumerated offline, and a family read has
// nothing stored behind it either.

// publicImageProjects are the image projects real GCP serves public images
// from: the hashicorp/google provider's own ImageMap (centos, coreos, debian,
// fedora-coreos, opensuse, rhel, rocky-linux, sles, ubuntu, windows,
// windows-sql) plus the COS, Pro/Ubuntu-OS, CentOS Stream, SAP, deep-learning
// and ML image projects Terraform configurations reference directly.
var publicImageProjects = map[string]bool{
	"centos-cloud":                  true,
	"centos-stream-cloud":           true,
	"coreos-cloud":                  true,
	"cos-cloud":                     true,
	"debian-cloud":                  true,
	"deeplearning-platform-release": true,
	"fedora-coreos-cloud":           true,
	"ml-images":                     true,
	"opensuse-cloud":                true,
	"rhel-cloud":                    true,
	"rhel-sap-cloud":                true,
	"rocky-linux-cloud":             true,
	"suse-cloud":                    true,
	"suse-sap-cloud":                true,
	"ubuntu-os-cloud":               true,
	"ubuntu-os-pro-cloud":           true,
	"windows-cloud":                 true,
	"windows-sql-cloud":             true,
}

// familyImageBuild is the build label a synthesized family image carries. A
// family read has no real "latest image" to report (that changes on every
// Google publish), so every family answers one stable build. Images created
// with it parse back to the family name (imageFamilyFor), so a get of the name
// getFromFamily returned is self-consistent.
const familyImageBuild = "v20240101"

// imageDiskSizeGB is the disk size a synthesized public image restores to (the
// documented default boot-disk size the public Linux images use).
const imageDiskSizeGB = "10"

// publicImageBuildRE matches the "-vYYYYMMDD" build label inside a concrete
// public image name (an sles image carries an "-x86-64" suffix after it, which
// is why the label is matched underived rather than anchored).
var publicImageBuildRE = regexp.MustCompile(`-v[0-9]{8}`)

// cosImageBuildRE matches Cloud COS's milestone/build/patch/revision suffix
// (cos-stable-109-17800-147-54).
var cosImageBuildRE = regexp.MustCompile(`-[0-9]+-[0-9]+-[0-9]+-[0-9]+$`)

// imageBuildDateRE captures the date inside a "-vYYYYMMDD" build label.
var imageBuildDateRE = regexp.MustCompile(`-v([0-9]{4})([0-9]{2})([0-9]{2})`)

// ImagesGet answers compute.images.get
// (GET /compute/v1/projects/{project}/global/images/{image}).
func (p *Provider) ImagesGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, name := accountID(nr), strParam(nr, "image")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing image", 400)
	}
	if err := validateImageName(name, "image"); err != nil {
		return nil, err
	}
	if !publicImageProjects[project] || !isConcreteImageName(name) {
		return nil, imageNotFound("images", project, name)
	}
	return provider.OK(publicImageObject(nr, project, name, imageFamilyFor(name))), nil
}

// ImagesGetFromFamily answers compute.images.getFromFamily
// (GET /compute/v1/projects/{project}/global/images/family/{family}) with the
// family's synthesized build.
func (p *Provider) ImagesGetFromFamily(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, family := accountID(nr), strParam(nr, "family")
	if family == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing family", 400)
	}
	if err := validateImageName(family, "family"); err != nil {
		return nil, err
	}
	if !publicImageProjects[project] {
		return nil, imageNotFound("family", project, family)
	}
	return provider.OK(publicImageObject(nr, project, family+"-"+familyImageBuild, family)), nil
}

// validateImageName enforces Compute Engine's documented resource-name grammar
// (Discovery, Image.name: 1-63 characters, [a-z]([-a-z0-9]*[a-z0-9])?). Real
// GCP rejects a malformed name with 400 before looking anything up.
func validateImageName(name, field string) error {
	if resourceNameRE.MatchString(name) {
		return nil
	}
	return model.NewProviderError("InvalidArgument",
		fmt.Sprintf("Invalid value for field 'resource.%s': '%s'. Must be 1-63 characters long and match the regular expression '[a-z]([-a-z0-9]*[a-z0-9])?'", field, name), 400)
}

// imageNotFound renders Compute Engine's not-found error for an images.get or
// images.getFromFamily lookup: the exact shape real GCP answers, with the
// "family" collection segment when a family was requested.
func imageNotFound(collection, project, name string) error {
	if collection == "family" {
		return model.NewProviderError("NotFound",
			fmt.Sprintf("The resource 'projects/%s/global/images/family/%s' was not found", project, name), 404)
	}
	return model.NewProviderError("NotFound",
		fmt.Sprintf("The resource 'projects/%s/global/images/%s' was not found", project, name), 404)
}

// isConcreteImageName reports whether name is a concrete public image build
// rather than a family name.
func isConcreteImageName(name string) bool {
	return publicImageBuildRE.MatchString(name) || cosImageBuildRE.MatchString(name)
}

// imageFamilyFor derives the family a synthesized concrete image reports: the
// part of the name before its build label, so
// "debian-12-bookworm-v20241009" reports "debian-12-bookworm" and
// "sles-15-sp5-v20240109-x86-64" reports "sles-15-sp5". A name with no
// recognizable build label has no family.
func imageFamilyFor(name string) string {
	if loc := publicImageBuildRE.FindStringIndex(name); loc != nil && loc[0] > 0 {
		return name[:loc[0]]
	}
	if loc := cosImageBuildRE.FindStringIndex(name); loc != nil && loc[0] > 0 {
		return name[:loc[0]]
	}
	return ""
}

// imageCreationTimestamp dates a synthesized image from its build label, so a
// concrete build reports the day it was built and a family build reports the
// stable catalog origin.
func imageCreationTimestamp(name string) string {
	m := imageBuildDateRE.FindStringSubmatch(name)
	if m == nil {
		return catalogCreationTimestamp
	}
	return fmt.Sprintf("%s-%s-%sT00:00:00.000-07:00", m[1], m[2], m[3])
}

// imageArchiveSize is a stable cosmetic archive size for a synthesized image
// (real public images are roughly 1.0-1.5 GB compressed).
func imageArchiveSize(name string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return strconv.FormatInt(int64(1_000_000_000)+int64(h.Sum32()%500_000_000), 10)
}

// resolveImageReference normalizes the image reference a create request
// carries (`sourceImage` on a disk insert) the way real Compute does when it
// records the resource: a family reference resolves to the family's concrete
// build, and a project-relative or full-URL spelling becomes the image's
// absolute selfLink plus its numeric id. The documented forms are
// projects/{p}/global/images/{image}, projects/{p}/global/images/family/{family},
// global/images/{image}, global/images/family/{family}, and the absolute
// self-link of any of those under /compute/{version}/.
func resolveImageReference(project, ref string) (link, id string, err error) {
	imageProject, name, family, ok := parseImageReference(project, ref)
	if !ok || !resourceNameRE.MatchString(name) {
		return "", "", model.NewProviderError("InvalidArgument",
			fmt.Sprintf("Invalid value for field 'resource.sourceImage': '%s'. Must be a global image or image-family reference", ref), 400)
	}
	if !publicImageProjects[imageProject] {
		return "", "", imageNotFound("images", imageProject, name)
	}
	if family {
		name += "-" + familyImageBuild
	} else if !isConcreteImageName(name) {
		return "", "", imageNotFound("images", imageProject, name)
	}
	return computeBasePath + resource.ResourceID(imageProject)("compute-image", name),
		numericID("image/" + imageProject + "/" + name), nil
}

// parseImageReference splits an image reference into its project, name and
// family flag. defaultProject is the project the reference inherits when it
// names none (global/images/...). An unrecognized spelling reports ok=false.
func parseImageReference(defaultProject, ref string) (project, name string, family, ok bool) {
	segs := imageReferenceSegments(ref)
	switch {
	case len(segs) == 5 && segs[0] == "projects" && segs[2] == "global" && segs[3] == "images":
		return segs[1], segs[4], false, true
	case len(segs) == 6 && segs[0] == "projects" && segs[2] == "global" && segs[3] == "images" && segs[4] == "family":
		return segs[1], segs[5], true, true
	case len(segs) == 3 && segs[0] == "global" && segs[1] == "images":
		return defaultProject, segs[2], false, true
	case len(segs) == 4 && segs[0] == "global" && segs[1] == "images" && segs[2] == "family":
		return defaultProject, segs[3], true, true
	}
	return "", "", false, false
}

// imageReferenceSegments turns an image reference into path segments, dropping
// the REST root and the API version of an absolute self-link so the standard
// and universe-domain spellings (https://www.googleapis.com/compute/v1/...,
// https://compute.example/compute/beta/...) parse alike.
func imageReferenceSegments(ref string) []string {
	ref = strings.TrimSpace(ref)
	if i := strings.Index(ref, "/compute/"); i >= 0 && strings.HasPrefix(ref, "http") {
		ref = ref[i+len("/compute/"):]
		if j := strings.IndexByte(ref, '/'); j >= 0 {
			ref = ref[j+1:]
		}
	}
	return strings.Split(strings.Trim(ref, "/"), "/")
}

// publicImageObject renders the compute#image a read of a synthesized public
// image answers. Every value is derived from the request path, so two reads of
// the same image are byte-identical and a read after a restart reports the same
// resource (nothing is stored).
func publicImageObject(nr *model.NormalizedRequest, project, name, family string) map[string]any {
	obj := map[string]any{
		"kind":              kindImage,
		"id":                numericID("image/" + project + "/" + name),
		"name":              name,
		"status":            "READY",
		"sourceType":        "RAW",
		"architecture":      "X86_64",
		"diskSizeGb":        imageDiskSizeGB,
		"archiveSizeBytes":  imageArchiveSize(name),
		"creationTimestamp": imageCreationTimestamp(name),
		"selfLink":          absLink(nr, "compute-image", name),
		"labelFingerprint":  contentEtag("image/" + project + "/" + name),
		"storageLocations":  []string{"us"},
		"guestOsFeatures": []any{
			map[string]any{"type": "UEFI_COMPATIBLE"},
			map[string]any{"type": "VIRTIO_SCSI_MULTIQUEUE"},
			map[string]any{"type": "GVNIC"},
		},
	}
	label := family
	if label == "" {
		label = name
	}
	obj["description"] = label + " public image, x86_64"
	if family != "" {
		obj["family"] = family
		obj["licenses"] = []string{absLink(nr, "compute-license", family)}
	}
	return obj
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
		"creationTimestamp": catalogCreationTimestamp,
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
		"creationTimestamp": catalogCreationTimestamp,
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
		"creationTimestamp":            catalogCreationTimestamp,
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

// encryptionKeyResponse renders a request's CustomerEncryptionKey block in the
// shape real Compute returns on a disk GET (TF2-11-4). Discovery documents
// rawKey and rsaEncryptedKey as request material and marks sha256
// "[Output only] The RFC 4648 base64 encoded SHA-256 hash of the
// customer-supplied encryption key that protects this resource", so only
// kmsKeyName, kmsKeyServiceAccount and a synthesized sha256 are ever persisted:
// the raw and RSA-wrapped key material is neither stored nor echoed.
//
// The digest is over the decoded 32-byte key, the same value GCS reports for a
// CSEK object (see the storage provider's CSEK read path). A CSEK block whose
// rawKey is not a base64-encoded 256-bit key is rejected the way real Compute
// rejects it, and a sha256 the caller sent is ignored because the field is
// output-only. A block with nothing server-visible to report (no key material
// and no kmsKeyName, e.g. only rsaEncryptedKey) yields nil, so no empty block is
// rendered.
func encryptionKeyResponse(field string, v any) (map[string]any, error) {
	block, ok := v.(map[string]any)
	if !ok {
		return nil, model.NewProviderError("InvalidArgument",
			fmt.Sprintf("Invalid value for field 'resource.%s': must be a CustomerEncryptionKey object", field), 400)
	}
	out := map[string]any{}
	for _, name := range []string{"kmsKeyName", "kmsKeyServiceAccount"} {
		if s, _ := block[name].(string); s != "" {
			out[name] = s
		}
	}
	if raw, _ := block["rawKey"].(string); raw != "" {
		key, err := base64.StdEncoding.DecodeString(raw)
		if err != nil || len(key) != 32 {
			return nil, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("Invalid value for field 'resource.%s.rawKey': a 256-bit customer-supplied encryption key encoded in RFC 4648 base64 is required", field), 400)
		}
		sum := sha256.Sum256(key)
		out["sha256"] = base64.StdEncoding.EncodeToString(sum[:])
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
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

// serviceAccountEmail is the project's Compute Engine default service account,
// the identity compute.projects.get reports as defaultServiceAccount. Compute
// operations are not attributed to a caller, so the operation envelope's `user`
// carries that same account (real GCP stamps the requesting principal there).
func serviceAccountEmail(project string) string {
	return defaultServiceAccountForNumber(projectNumber(project))
}

// defaultServiceAccountForNumber renders the Compute Engine default service
// account of a project number: {number}-compute@developer.gserviceaccount.com.
func defaultServiceAccountForNumber(number string) string {
	return number + "-compute@developer.gserviceaccount.com"
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
