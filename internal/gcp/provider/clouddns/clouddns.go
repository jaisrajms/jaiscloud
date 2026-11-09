// Package clouddns implements the Cloud DNS v1 control-plane provider
// (dns.googleapis.com/dns/v1): managed zones and their resource record sets,
// plus the change records that mutate them.
//
// This is metadata-only, mirroring the AWS Route53 provider: zones, rrsets, and
// changes are stored as records in the shared ResourceStore, keyed by the
// project (AccountID) with store.GlobalRegion. The emulator never stands up an
// authoritative DNS server, so nothing is ever served over port 53: name
// servers are synthesized placeholders, record sets are stored verbatim, and a
// "change" completes synchronously with status "done". Create/patch/delete of
// zones, rrsets, and changes are the supported surface; managed-zone IAM is
// served over the shared policy plane; every DNSSEC and policy method is
// deferred and fails loud with Unimplemented.
package clouddns

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource types in the shared ResourceStore.
const (
	rtManagedZone       = "gcp_dns_managed_zone"
	rtManagedZonePolicy = "gcp_dns_managed_zone_policy"
	rtRRSet             = "gcp_dns_rrset"
	rtChange            = "gcp_dns_change"
	rtOperation         = "gcp_dns_operation"
)

// Kind constants for the GCP wire envelope.
const (
	kindManagedZone        = "dns#managedZone"
	kindManagedZoneList    = "dns#managedZonesListResponse"
	kindResourceRecordSet  = "dns#resourceRecordSet"
	kindResourceRecordList = "dns#resourceRecordSetsListResponse"
	kindChange             = "dns#change"
	kindChangeList         = "dns#changesListResponse"
	kindOperation          = "dns#operation"
	kindOperationList      = "dns#managedZoneOperationsListResponse"
)

// Provider handles Cloud DNS managed zones, resource record sets, and changes.
type Provider struct {
	resources store.ResourceStore
}

// New returns a Provider backed by the shared ResourceStore.
func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

// Routes maps "CloudDNS.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"CloudDNS.ManagedZoneCreate": p.CreateManagedZone,
		"CloudDNS.ManagedZoneGet":    p.GetManagedZone,
		"CloudDNS.ManagedZoneList":   p.ListManagedZones,
		"CloudDNS.ManagedZonePatch":  p.PatchManagedZone,
		"CloudDNS.ManagedZoneUpdate": p.UpdateManagedZone,
		"CloudDNS.ManagedZoneDelete": p.DeleteManagedZone,

		"CloudDNS.ResourceRecordSetGet":    p.GetResourceRecordSet,
		"CloudDNS.ResourceRecordSetList":   p.ListResourceRecordSets,
		"CloudDNS.ResourceRecordSetCreate": p.CreateResourceRecordSet,
		"CloudDNS.ResourceRecordSetPatch":  p.PatchResourceRecordSet,
		"CloudDNS.ResourceRecordSetDelete": p.DeleteResourceRecordSet,

		"CloudDNS.ChangeCreate": p.CreateChange,
		"CloudDNS.ChangeGet":    p.GetChange,
		"CloudDNS.ChangeList":   p.ListChanges,

		"CloudDNS.ManagedZoneOperationGet":  p.GetManagedZoneOperation,
		"CloudDNS.ManagedZoneOperationList": p.ListManagedZoneOperations,

		// The google.iam.v1.IAMPolicy mixin on managedZones.
		"CloudDNS.ManagedZoneGetIamPolicy":       p.GetManagedZoneIamPolicy,
		"CloudDNS.ManagedZoneSetIamPolicy":       p.SetManagedZoneIamPolicy,
		"CloudDNS.ManagedZoneTestIamPermissions": p.TestManagedZoneIamPermissions,

		"CloudDNS.ProjectGet":    p.GetProject,
		"CloudDNS.Unimplemented": p.Unimplemented,
	}
}

// ─── Wire types ───────────────────────────────────────────────────────────────

// ManagedZone is the dns#managedZone representation. id/nameServers/creationTime
// are output-only and synthesized on the server; the mutable fields are echoed
// from the request.
//
// The configuration blocks (dnssecConfig, forwardingConfig, peeringConfig,
// privateVisibilityConfig) are kept as the decoded request objects rather than
// re-typed field by field: the Terraform provider sends the whole block on
// create and again in every PATCH, and the official modules round-trip
// sub-fields the Discovery document declares (forwarding ipv6Address/domainName,
// GKE clusters, default key specs). Echoing the accepted object verbatim means a
// GET returns exactly what the client sent, so a second plan reports no changes.
// Nothing is synthesized here: a block the client omitted stays omitted.
type ManagedZone struct {
	Kind         string            `json:"kind,omitempty"`
	Name         string            `json:"name,omitempty"`
	DNSName      string            `json:"dnsName,omitempty"`
	Description  string            `json:"description,omitempty"`
	ID           string            `json:"id,omitempty"`
	NameServers  []string          `json:"nameServers,omitempty"`
	Visibility   string            `json:"visibility,omitempty"`
	CreationTime string            `json:"creationTime,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	// CloudLoggingConfig is always reported by real GCP (an empty config
	// carrying only the kind), and the provider also sends it when
	// enable_logging is set. Modeled so both the wire response and the update
	// path match.
	CloudLoggingConfig map[string]any `json:"cloudLoggingConfig,omitempty"`

	DNSSECConfig            map[string]any `json:"dnssecConfig,omitempty"`
	ForwardingConfig        map[string]any `json:"forwardingConfig,omitempty"`
	PeeringConfig           map[string]any `json:"peeringConfig,omitempty"`
	PrivateVisibilityConfig map[string]any `json:"privateVisibilityConfig,omitempty"`
}

// withZoneDefaults fills the output-only managed-zone fields real GCP always
// reports, so zones created before these fields existed still render fully. A
// privateVisibilityConfig that binds neither a network nor a GKE cluster is not
// rendered: `hashicorp/google` always sends `{"networks": []}` (the API's
// documented "remove the binding" form) even on a public zone, and real GCP does
// not report an empty private visibility block back.
func withZoneDefaults(mz ManagedZone) ManagedZone {
	if mz.Visibility == "" {
		mz.Visibility = "public"
	}
	if mz.CloudLoggingConfig == nil {
		mz.CloudLoggingConfig = map[string]any{"kind": "dns#managedZoneCloudLoggingConfig"}
	}
	if !privateVisibilityBinds(mz.PrivateVisibilityConfig) {
		mz.PrivateVisibilityConfig = nil
	}
	return mz
}

// privateVisibilityBinds reports whether a privateVisibilityConfig actually
// binds VPC networks or GKE clusters. An absent block, an empty object and the
// provider's `{"networks": []}` clearing form all bind nothing.
func privateVisibilityBinds(cfg map[string]any) bool {
	if len(cfg) == 0 {
		return false
	}
	var pvc struct {
		Networks    []any `json:"networks"`
		GKEClusters []any `json:"gkeClusters"`
	}
	if err := decodeBody(cfg, &pvc); err != nil {
		// A malformed block is not silently discarded; let the write path
		// decide what to do with it.
		return true
	}
	return len(pvc.Networks) > 0 || len(pvc.GKEClusters) > 0
}

// ResourceRecordSet is the dns#resourceRecordSet representation.
type ResourceRecordSet struct {
	Kind    string   `json:"kind,omitempty"`
	Name    string   `json:"name,omitempty"`
	Type    string   `json:"type,omitempty"`
	TTL     int64    `json:"ttl,omitempty"`
	RRDatas []string `json:"rrdatas,omitempty"`
}

// Change is the dns#change representation. Changes complete synchronously, so
// Status is always "done".
type Change struct {
	Kind      string              `json:"kind,omitempty"`
	ID        string              `json:"id,omitempty"`
	Status    string              `json:"status,omitempty"`
	StartTime string              `json:"startTime,omitempty"`
	IsServing bool                `json:"isServing,omitempty"`
	Additions []ResourceRecordSet `json:"additions,omitempty"`
	Deletions []ResourceRecordSet `json:"deletions,omitempty"`
}

// storedRRSet scopes a record set to its managed zone without leaking an
// emulator-only field onto the wire.
type storedRRSet struct {
	Zone string            `json:"zone"`
	RR   ResourceRecordSet `json:"rr"`
}

// storedChange scopes a change record to its managed zone.
type storedChange struct {
	Zone   string `json:"zone"`
	Change Change `json:"change"`
}

// Operation is the dns#operation representation: the audit record a successful
// managed-zone mutation (managedZones.patch/update) produces. The Discovery
// document declares the status enum pending|done and the type insert|update|
// delete; the emulator applies the mutation and records the operation done, so
// an official client's poll of operations.get settles on the first call. The
// zoneContext carries the pre- and post-operation ManagedZone, which is where
// real GCP exposes what the operation changed.
type Operation struct {
	Kind        string                       `json:"kind,omitempty"`
	ID          string                       `json:"id,omitempty"`
	Type        string                       `json:"type,omitempty"`
	Status      string                       `json:"status,omitempty"`
	StartTime   string                       `json:"startTime,omitempty"`
	User        string                       `json:"user,omitempty"`
	ZoneContext *OperationManagedZoneContext `json:"zoneContext,omitempty"`
}

// OperationManagedZoneContext is the dns#operationManagedZoneContext shape: the
// managed zone before and after the operation.
type OperationManagedZoneContext struct {
	OldValue *ManagedZone `json:"oldValue,omitempty"`
	NewValue *ManagedZone `json:"newValue,omitempty"`
}

// storedOperation scopes an operation record to its managed zone, which is the
// collection operations.get/list address it through.
type storedOperation struct {
	Zone      string    `json:"zone"`
	Operation Operation `json:"operation"`
}

// ─── ManagedZone operations ───────────────────────────────────────────────────

func (p *Provider) CreateManagedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body, _ := nr.Params["body"].(map[string]any)
	mz := ManagedZone{}
	_ = decodeBody(body, &mz)
	if mz.Name == "" {
		return nil, model.NewProviderError("InvalidArgument", "managedZone.name is required", 400)
	}
	if mz.DNSName == "" {
		return nil, model.NewProviderError("InvalidArgument", "managedZone.dnsName is required", 400)
	}
	mz.DNSName = fqdn(mz.DNSName)
	mz.Kind = kindManagedZone
	if mz.Visibility == "" {
		mz.Visibility = "public"
	}
	if mz.Visibility != "public" && mz.Visibility != "private" {
		return nil, model.NewProviderError("InvalidArgument", "managedZone.visibility must be public or private", 400)
	}
	if err := validateManagedZoneConfig(mz); err != nil {
		return nil, err
	}
	mz.ID = numericID(nr.AccountID + "/" + mz.Name)
	mz.NameServers = nameServers()
	mz.CreationTime = formatTimestamp(clock.Now())
	mz = withZoneDefaults(mz)

	data, err := json.Marshal(mz)
	if err != nil {
		return nil, err
	}
	err = p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtManagedZone, ID: mz.Name, Data: data})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, model.NewProviderError("AlreadyExists", fmt.Sprintf("managed zone %q already exists", mz.Name), 409)
	}
	if err != nil {
		return nil, err
	}
	return provider.OK(toMap(withZoneDefaults(mz))), nil
}

func (p *Provider) GetManagedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "managedZone")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	mz, err := p.loadManagedZone(ctx, nr, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(toMap(withZoneDefaults(mz))), nil
}

func (p *Provider) ListManagedZones(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bound, err := bindPageSize(nr.Params)
	if err != nil {
		return nil, err
	}
	size, err := resolvePageSize(bound)
	if err != nil {
		return nil, err
	}
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtManagedZone, "")
	if err != nil {
		return nil, err
	}
	zones := make([]ManagedZone, 0, len(entries))
	for _, e := range entries {
		var mz ManagedZone
		if json.Unmarshal(e.Data, &mz) == nil {
			zones = append(zones, mz)
		}
	}
	return provider.OK(p.pageManagedZones(nr, zones, size)), nil
}

// PatchManagedZone applies a partial update and returns the dns#operation the
// Discovery document declares for managedZones.patch.
func (p *Provider) PatchManagedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.applyManagedZoneUpdate(ctx, nr)
}

// UpdateManagedZone applies a full update and returns the dns#operation the
// Discovery document declares for managedZones.update.
func (p *Provider) UpdateManagedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.applyManagedZoneUpdate(ctx, nr)
}

// applyManagedZoneUpdate merges the mutable managed-zone fields (description,
// visibility, labels and the dnssec/forwarding/peering/private-visibility/
// cloud-logging configuration blocks) from the request body. Both the PATCH and
// PUT wire methods share these merge semantics; dnsName is immutable and
// ignored. The mutation is applied and its operation recorded done in one call,
// so the returned dns#operation settles immediately and the next
// managedZones.get shows the new values — the sequence an official client's
// patch-then-poll-operations.get performs.
//
// The Terraform provider sends the whole configuration block in every PATCH
// (its update path expands the full schema object), so a block present in the
// body replaces the stored one. A JSON null or an empty object is not a block:
// the provider emits `"privateVisibilityConfig": null` when the caller has no
// private_visibility_config, and real GCP treats that as "unset", not "clear".
func (p *Provider) applyManagedZoneUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "managedZone")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	old, err := p.loadManagedZone(ctx, nr, name)
	if err != nil {
		return nil, err
	}
	mz := old
	body, _ := nr.Params["body"].(map[string]any)
	if body != nil {
		if v, ok := body["description"].(string); ok {
			mz.Description = v
		}
		if v, ok := body["visibility"].(string); ok && v != "" {
			if v != "public" && v != "private" {
				return nil, model.NewProviderError("InvalidArgument", "managedZone.visibility must be public or private", 400)
			}
			mz.Visibility = v
		}
		if v, ok := body["labels"]; ok {
			mz.Labels = stringMap(v)
		}
		for key, dst := range map[string]*map[string]any{
			"dnssecConfig":            &mz.DNSSECConfig,
			"forwardingConfig":        &mz.ForwardingConfig,
			"peeringConfig":           &mz.PeeringConfig,
			"privateVisibilityConfig": &mz.PrivateVisibilityConfig,
			"cloudLoggingConfig":      &mz.CloudLoggingConfig,
		} {
			if v, ok := body[key].(map[string]any); ok && len(v) > 0 {
				*dst = v
			}
		}
	}
	if err := validateManagedZoneConfig(mz); err != nil {
		return nil, err
	}
	data, err := json.Marshal(mz)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtManagedZone, ID: mz.Name, Data: data}); err != nil {
		return nil, storeError(err, "managed zone")
	}
	op, err := p.newManagedZoneOperation(ctx, nr, mz.Name, old, mz)
	if err != nil {
		return nil, err
	}
	return provider.OK(toMap(op)), nil
}

// validateManagedZoneConfig enforces the combinations the Discovery document
// and the Cloud DNS API declare for a managed zone's configuration blocks:
// forwarding, peering and private visibility all live on private zones, a zone
// cannot both forward and peer, and a forwarding target must name the IPv4
// address it forwards to. dnssecConfig is stored verbatim and is not restricted
// by these combination rules (its own fields belong to the DNSSEC surface).
func validateManagedZoneConfig(mz ManagedZone) error {
	private := mz.Visibility == "private"
	if !private && privateVisibilityBinds(mz.PrivateVisibilityConfig) {
		return model.NewProviderError("InvalidArgument",
			"managedZone.privateVisibilityConfig requires visibility \"private\"", 400)
	}
	if len(mz.ForwardingConfig) > 0 {
		if !private {
			return model.NewProviderError("InvalidArgument",
				"managedZone.forwardingConfig requires visibility \"private\"", 400)
		}
		if len(mz.PeeringConfig) > 0 {
			return model.NewProviderError("InvalidArgument",
				"managedZone.forwardingConfig and managedZone.peeringConfig cannot be combined", 400)
		}
		var fc struct {
			TargetNameServers []struct {
				IPv4Address string `json:"ipv4Address"`
			} `json:"targetNameServers"`
		}
		if err := decodeBody(mz.ForwardingConfig, &fc); err != nil {
			return model.NewProviderError("InvalidArgument", "managedZone.forwardingConfig is malformed", 400)
		}
		for i, target := range fc.TargetNameServers {
			if target.IPv4Address == "" {
				return model.NewProviderError("InvalidArgument",
					fmt.Sprintf("managedZone.forwardingConfig.targetNameServers[%d].ipv4Address is required", i), 400)
			}
		}
	}
	if len(mz.PeeringConfig) > 0 {
		if !private {
			return model.NewProviderError("InvalidArgument",
				"managedZone.peeringConfig requires visibility \"private\"", 400)
		}
		var pc struct {
			TargetNetwork struct {
				NetworkURL string `json:"networkUrl"`
			} `json:"targetNetwork"`
		}
		if err := decodeBody(mz.PeeringConfig, &pc); err != nil {
			return model.NewProviderError("InvalidArgument", "managedZone.peeringConfig is malformed", 400)
		}
		if pc.TargetNetwork.NetworkURL == "" {
			return model.NewProviderError("InvalidArgument",
				"managedZone.peeringConfig.targetNetwork.networkUrl is required", 400)
		}
	}
	return nil
}

// ─── ManagedZone operation operations ─────────────────────────────────────────

// newManagedZoneOperation records the dns#operation for a committed managed-zone
// mutation: type "update", status "done" (the mutation has already been
// applied), with the pre- and post-operation zone in zoneContext. The id is the
// caller's clientOperationId when supplied (it must be unique within the zone's
// operations collection), otherwise a generated one.
func (p *Provider) newManagedZoneOperation(ctx context.Context, nr *model.NormalizedRequest, zone string, old, updated ManagedZone) (Operation, error) {
	id := strParam(nr, "clientOperationId")
	if id != "" {
		if len(id) > 63 || strings.ContainsAny(id, "/") {
			return Operation{}, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("clientOperationId %q must be at most 63 characters and must not contain '/'", id), 400)
		}
		if _, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtOperation, operationID(zone, id)); err == nil {
			return Operation{}, model.NewProviderError("AlreadyExists",
				fmt.Sprintf("operation %q already exists", id), 409)
		} else if !errors.Is(err, store.ErrNotFound) {
			return Operation{}, err
		}
	} else {
		id = newChangeID()
	}

	oldZone := withZoneDefaults(old)
	newZone := withZoneDefaults(updated)
	op := Operation{
		Kind:      kindOperation,
		ID:        id,
		Type:      "update",
		Status:    "done",
		StartTime: formatTimestamp(clock.Now()),
		ZoneContext: &OperationManagedZoneContext{
			OldValue: &oldZone,
			NewValue: &newZone,
		},
	}
	data, err := json.Marshal(storedOperation{Zone: zone, Operation: op})
	if err != nil {
		return Operation{}, err
	}
	entry := store.ResourceEntry{Type: rtOperation, ID: operationID(zone, id), Data: data}
	if err := p.resources.Upsert(ctx, nr.AccountID, store.GlobalRegion, entry); err != nil {
		return Operation{}, err
	}
	return op, nil
}

// GetManagedZoneOperation fetches a recorded managed-zone operation
// (managedZoneOperations.get). An unknown id in the zone is NotFound.
func (p *Provider) GetManagedZoneOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zone := strParam(nr, "managedZone")
	id := strParam(nr, "operation")
	if zone == "" || id == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone or operation", 400)
	}
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtOperation, operationID(zone, id))
	if err != nil {
		return nil, storeError(err, "operation")
	}
	var s storedOperation
	if err := json.Unmarshal(e.Data, &s); err != nil {
		return nil, err
	}
	return provider.OK(toMap(s.Operation)), nil
}

// ListManagedZoneOperations enumerates a zone's recorded operations
// (managedZoneOperations.list), sorted by the documented sortBy criterion
// (startTime, the default, or id).
func (p *Provider) ListManagedZoneOperations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bound, err := bindPageSize(nr.Params)
	if err != nil {
		return nil, err
	}
	zone := strParam(nr, "managedZone")
	if zone == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	sortBy, err := resolveOperationSortBy(nr.Params)
	if err != nil {
		return nil, err
	}
	if _, err := p.loadManagedZone(ctx, nr, zone); err != nil {
		return nil, err
	}
	size, err := resolvePageSize(bound)
	if err != nil {
		return nil, err
	}
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtOperation, zone)
	if err != nil {
		return nil, err
	}
	ops := make([]Operation, 0, len(entries))
	for _, e := range entries {
		var s storedOperation
		if json.Unmarshal(e.Data, &s) == nil && s.Zone == zone {
			ops = append(ops, s.Operation)
		}
	}
	key := func(op Operation) string { return op.StartTime + "\x00" + op.ID }
	if sortBy == "id" {
		key = func(op Operation) string { return op.ID }
	}
	page, nextPageToken := paginate(ops, key, nr.Params, size)
	items := make([]any, 0, len(page))
	for _, op := range page {
		items = append(items, toMap(op))
	}
	resp := map[string]any{"kind": kindOperationList, "operations": items}
	if nextPageToken != "" {
		resp["nextPageToken"] = nextPageToken
	}
	return provider.OK(resp), nil
}

// resolveOperationSortBy validates managedZoneOperations.list's sortBy binding:
// the Discovery enum's startTime (the default) or id. Values are matched
// case-insensitively so the enum spelling and the upper-case form both work;
// anything else is InvalidArgument rather than a silently mis-sorted page.
func resolveOperationSortBy(params map[string]any) (string, error) {
	raw, _ := params["sortBy"].(string)
	if raw == "" {
		return "startTime", nil
	}
	switch strings.ToLower(raw) {
	case "starttime":
		return "startTime", nil
	case "id":
		return "id", nil
	}
	return "", model.NewProviderError("InvalidArgument",
		fmt.Sprintf("Invalid value for 'sortBy': %q. Supported values are startTime and id.", raw), 400)
}

func (p *Provider) DeleteManagedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "managedZone")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtManagedZone, name); err != nil {
		return nil, storeError(err, "managed zone")
	}
	// Zone children are not addressable once the zone is gone; drop them so a
	// subsequent create of the same name starts clean.
	p.purgeZoneChildren(ctx, nr.AccountID, name)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) loadManagedZone(ctx context.Context, nr *model.NormalizedRequest, name string) (ManagedZone, error) {
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtManagedZone, name)
	if err != nil {
		return ManagedZone{}, storeError(err, "managed zone")
	}
	var mz ManagedZone
	if err := json.Unmarshal(e.Data, &mz); err != nil {
		return ManagedZone{}, err
	}
	return mz, nil
}

func (p *Provider) pageManagedZones(nr *model.NormalizedRequest, zones []ManagedZone, size int) map[string]any {
	page, nextPageToken := paginate(zones, func(z ManagedZone) string { return z.Name }, nr.Params, size)
	items := make([]any, 0, len(page))
	for _, z := range page {
		items = append(items, toMap(withZoneDefaults(z)))
	}
	resp := map[string]any{"kind": kindManagedZoneList, "managedZones": items}
	if nextPageToken != "" {
		resp["nextPageToken"] = nextPageToken
	}
	return resp
}

// purgeZoneChildren removes the record sets and change records owned by a zone.
func (p *Provider) purgeZoneChildren(ctx context.Context, account, zone string) {
	if entries, err := p.resources.List(ctx, account, store.GlobalRegion, rtRRSet, zone); err == nil {
		for _, e := range entries {
			var s storedRRSet
			if json.Unmarshal(e.Data, &s) == nil && s.Zone == zone {
				_ = p.resources.Delete(ctx, account, store.GlobalRegion, rtRRSet, e.ID)
			}
		}
	}
	if entries, err := p.resources.List(ctx, account, store.GlobalRegion, rtChange, zone); err == nil {
		for _, e := range entries {
			var s storedChange
			if json.Unmarshal(e.Data, &s) == nil && s.Zone == zone {
				_ = p.resources.Delete(ctx, account, store.GlobalRegion, rtChange, e.ID)
			}
		}
	}
	if entries, err := p.resources.List(ctx, account, store.GlobalRegion, rtOperation, zone); err == nil {
		for _, e := range entries {
			var s storedOperation
			if json.Unmarshal(e.Data, &s) == nil && s.Zone == zone {
				_ = p.resources.Delete(ctx, account, store.GlobalRegion, rtOperation, e.ID)
			}
		}
	}
	// The zone's IAM policy is bound to the resource's lifetime (real Cloud DNS
	// drops it with the zone), so a zone re-created under the same name starts
	// with the empty default policy instead of inheriting the old grants.
	_ = p.resources.Delete(ctx, account, store.GlobalRegion, rtManagedZonePolicy, zone)
}

// ─── Managed-zone IAM ────────────────────────────────────────────────────────

// Cloud DNS publishes the google.iam.v1.IAMPolicy mixin on managedZones
// (dns/v1/{+resource}:getIamPolicy, :setIamPolicy, :testIamPermissions with
// resource projects/{project}/managedZones/{zone}). It is the same stored
// policy plane as the other resource-IAM surfaces: etag OCC, AIP-149 versions,
// and no enforcement. The zone must exist before the policy is read or written,
// so a miss is NotFound on every verb and never creates a policy row.

// iamZone resolves the managed zone named by an IAM request and verifies it
// exists. It reads the store directly (not through the mutation paths) so an
// IAM read has no side effect on the zone.
func (p *Provider) iamZone(ctx context.Context, nr *model.NormalizedRequest) (string, error) {
	zone := strParam(nr, "managedZone")
	if zone == "" {
		return "", model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	if _, err := p.loadManagedZone(ctx, nr, zone); err != nil {
		return "", err
	}
	return zone, nil
}

// GetManagedZoneIamPolicy returns the zone's stored policy, or the empty
// default policy when none was set.
func (p *Provider) GetManagedZoneIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zone, err := p.iamZone(ctx, nr)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(policy.Load(ctx, p.resources, nr.AccountID, rtManagedZonePolicy, zone))), nil
}

// SetManagedZoneIamPolicy stores the zone's policy (etag OCC).
func (p *Provider) SetManagedZoneIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zone, err := p.iamZone(ctx, nr)
	if err != nil {
		return nil, err
	}
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtManagedZonePolicy, zone, bodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

// TestManagedZoneIamPermissions reports the requested permissions the caller
// holds. The emulator does not enforce IAM, so every permission is granted.
func (p *Provider) TestManagedZoneIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, err := p.iamZone(ctx, nr); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(bodyOf(nr)))}), nil
}

// ─── ResourceRecordSet operations ─────────────────────────────────────────────

func (p *Provider) CreateResourceRecordSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zone := strParam(nr, "managedZone")
	if zone == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	if _, err := p.loadManagedZone(ctx, nr, zone); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	rr := ResourceRecordSet{}
	_ = decodeBody(body, &rr)
	rr.Name = fqdn(rr.Name)
	if rr.Name == "" || rr.Type == "" {
		return nil, model.NewProviderError("InvalidArgument", "resourceRecordSet.name and type are required", 400)
	}
	rr.Kind = kindResourceRecordSet
	if rr.RRDatas == nil {
		rr.RRDatas = []string{}
	}
	if err := p.saveRRSet(ctx, nr.AccountID, zone, rr, true); err != nil {
		return nil, err
	}
	return provider.OK(rrToMap(rr)), nil
}

func (p *Provider) GetResourceRecordSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zone, name, typ := rrsetParams(nr)
	if zone == "" || name == "" || typ == "" {
		return nil, model.NewProviderError("InvalidArgument", "managedZone, name, and type are required", 400)
	}
	rr, err := p.loadRRSet(ctx, nr.AccountID, zone, fqdn(name), typ)
	if err != nil {
		return nil, err
	}
	return provider.OK(rrToMap(rr)), nil
}

func (p *Provider) ListResourceRecordSets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bound, err := bindPageSize(nr.Params)
	if err != nil {
		return nil, err
	}
	zone := strParam(nr, "managedZone")
	if zone == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	if _, err := p.loadManagedZone(ctx, nr, zone); err != nil {
		return nil, err
	}
	size, err := resolvePageSize(bound)
	if err != nil {
		return nil, err
	}
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtRRSet, zone)
	if err != nil {
		return nil, err
	}
	sets := make([]ResourceRecordSet, 0, len(entries))
	for _, e := range entries {
		var s storedRRSet
		if json.Unmarshal(e.Data, &s) == nil && s.Zone == zone {
			sets = append(sets, s.RR)
		}
	}
	return provider.OK(p.pageRRSets(nr, sets, size)), nil
}

func (p *Provider) PatchResourceRecordSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zone := strParam(nr, "managedZone")
	if zone == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	if _, err := p.loadManagedZone(ctx, nr, zone); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	rr := ResourceRecordSet{}
	_ = decodeBody(body, &rr)
	if rr.Name == "" {
		rr.Name, _ = nr.Params["rrsetName"].(string)
	}
	if rr.Type == "" {
		rr.Type, _ = nr.Params["rrsetType"].(string)
	}
	rr.Name = fqdn(rr.Name)
	if rr.Name == "" || rr.Type == "" {
		return nil, model.NewProviderError("InvalidArgument", "resourceRecordSet.name and type are required", 400)
	}
	rr.Kind = kindResourceRecordSet
	if rr.RRDatas == nil {
		rr.RRDatas = []string{}
	}
	if err := p.saveRRSet(ctx, nr.AccountID, zone, rr, false); err != nil {
		return nil, err
	}
	return provider.OK(rrToMap(rr)), nil
}

func (p *Provider) DeleteResourceRecordSet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zone, name, typ := rrsetParams(nr)
	if zone == "" || name == "" || typ == "" {
		return nil, model.NewProviderError("InvalidArgument", "managedZone, name, and type are required", 400)
	}
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtRRSet, rrsetID(zone, fqdn(name), typ)); err != nil {
		return nil, storeError(err, "resource record set")
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) saveRRSet(ctx context.Context, account, zone string, rr ResourceRecordSet, createOnly bool) error {
	data, err := json.Marshal(storedRRSet{Zone: zone, RR: rr})
	if err != nil {
		return err
	}
	entry := store.ResourceEntry{Type: rtRRSet, ID: rrsetID(zone, rr.Name, rr.Type), Data: data}
	if createOnly {
		if err := p.resources.Create(ctx, account, store.GlobalRegion, entry); err != nil {
			if errors.Is(err, store.ErrAlreadyExists) {
				return model.NewProviderError("AlreadyExists", fmt.Sprintf("resource record set %s/%s already exists", rr.Name, rr.Type), 409)
			}
			return err
		}
		return nil
	}
	return p.resources.Upsert(ctx, account, store.GlobalRegion, entry)
}

func (p *Provider) loadRRSet(ctx context.Context, account, zone, name, typ string) (ResourceRecordSet, error) {
	e, err := p.resources.Get(ctx, account, store.GlobalRegion, rtRRSet, rrsetID(zone, name, typ))
	if err != nil {
		return ResourceRecordSet{}, storeError(err, "resource record set")
	}
	var s storedRRSet
	if err := json.Unmarshal(e.Data, &s); err != nil {
		return ResourceRecordSet{}, err
	}
	return s.RR, nil
}

func (p *Provider) pageRRSets(nr *model.NormalizedRequest, sets []ResourceRecordSet, size int) map[string]any {
	page, nextPageToken := paginate(sets, func(r ResourceRecordSet) string { return r.Name + "/" + r.Type }, nr.Params, size)
	items := make([]any, 0, len(page))
	for _, r := range page {
		items = append(items, rrToMap(r))
	}
	resp := map[string]any{"kind": kindResourceRecordList, "rrsets": items}
	if nextPageToken != "" {
		resp["nextPageToken"] = nextPageToken
	}
	return resp
}

// ─── Change operations ────────────────────────────────────────────────────────

func (p *Provider) CreateChange(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zone := strParam(nr, "managedZone")
	if zone == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	if _, err := p.loadManagedZone(ctx, nr, zone); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	change := Change{}
	_ = decodeBody(body, &change)

	for _, rr := range change.Additions {
		rr.Name = fqdn(rr.Name)
		rr.Kind = kindResourceRecordSet
		if rr.Name == "" || rr.Type == "" {
			return nil, model.NewProviderError("InvalidArgument", "change.additions entries require name and type", 400)
		}
		if rr.RRDatas == nil {
			rr.RRDatas = []string{}
		}
		if err := p.saveRRSet(ctx, nr.AccountID, zone, rr, false); err != nil {
			return nil, err
		}
	}
	for _, rr := range change.Deletions {
		rr.Name = fqdn(rr.Name)
		if rr.Name == "" || rr.Type == "" {
			return nil, model.NewProviderError("InvalidArgument", "change.deletions entries require name and type", 400)
		}
		if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtRRSet, rrsetID(zone, rr.Name, rr.Type)); err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	}

	change.Kind = kindChange
	change.ID = newChangeID()
	change.Status = "done"
	change.IsServing = true
	change.StartTime = formatTimestamp(clock.Now())

	data, err := json.Marshal(storedChange{Zone: zone, Change: change})
	if err != nil {
		return nil, err
	}
	entry := store.ResourceEntry{Type: rtChange, ID: changeID(zone, change.ID), Data: data}
	if err := p.resources.Upsert(ctx, nr.AccountID, store.GlobalRegion, entry); err != nil {
		return nil, err
	}
	return provider.OK(changeToMap(change)), nil
}

func (p *Provider) GetChange(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	zone := strParam(nr, "managedZone")
	id := strParam(nr, "changeId")
	if zone == "" || id == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone or changeId", 400)
	}
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtChange, changeID(zone, id))
	if err != nil {
		return nil, storeError(err, "change")
	}
	var s storedChange
	if err := json.Unmarshal(e.Data, &s); err != nil {
		return nil, err
	}
	return provider.OK(changeToMap(s.Change)), nil
}

func (p *Provider) ListChanges(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bound, err := bindPageSize(nr.Params)
	if err != nil {
		return nil, err
	}
	zone := strParam(nr, "managedZone")
	if zone == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	if _, err := p.loadManagedZone(ctx, nr, zone); err != nil {
		return nil, err
	}
	size, err := resolvePageSize(bound)
	if err != nil {
		return nil, err
	}
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtChange, zone)
	if err != nil {
		return nil, err
	}
	changes := make([]Change, 0, len(entries))
	for _, e := range entries {
		var s storedChange
		if json.Unmarshal(e.Data, &s) == nil && s.Zone == zone {
			changes = append(changes, s.Change)
		}
	}
	return provider.OK(p.pageChanges(nr, changes, size)), nil
}

func (p *Provider) pageChanges(nr *model.NormalizedRequest, changes []Change, size int) map[string]any {
	page, nextPageToken := paginate(changes, func(c Change) string { return c.ID }, nr.Params, size)
	items := make([]any, 0, len(page))
	for _, c := range page {
		items = append(items, changeToMap(c))
	}
	resp := map[string]any{"kind": kindChangeList, "changes": items}
	if nextPageToken != "" {
		resp["nextPageToken"] = nextPageToken
	}
	return resp
}

// ─── Project ──────────────────────────────────────────────────────────────────

// GetProject synthesizes a dns#project record with a quota block.
func (p *Provider) GetProject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"kind":   "dns#project",
		"id":     nr.AccountID,
		"number": numericID(nr.AccountID),
		"quota": map[string]any{
			"kind":                               "dns#quota",
			"managedZones":                       10000,
			"rrsetsPerManagedZone":               10000,
			"rrsetAdditionsPerChange":            100,
			"rrsetDeletionsPerChange":            100,
			"totalRrdataSizePerChange":           10000,
			"dnsKeysPerManagedZone":              2,
			"resourceRecordsPerRrset":            100,
			"nameserversPerDelegation":           6,
			"networksPerManagedZone":             100,
			"managedZonesPerNetwork":             1,
			"gkeClustersPerManagedZone":          100,
			"gkeClustersPerPolicy":               100,
			"internetHealthChecksPerManagedZone": 100,
		},
	}), nil
}

// Unimplemented is the fail-loud handler for deferred Cloud DNS surfaces
// (DNSSEC keys, policies, and response policies).
func (p *Provider) Unimplemented(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("Unimplemented", "operation is not supported by the Cloud DNS emulator", 501)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyOf(nr *model.NormalizedRequest) map[string]any {
	m, _ := nr.Params["body"].(map[string]any)
	return m
}

// rrsetParams returns (zone, name, type) for an rrset request. The type is
// upper-cased so lookups are case-insensitive.
func rrsetParams(nr *model.NormalizedRequest) (zone, name, typ string) {
	zone = strParam(nr, "managedZone")
	name = strParam(nr, "rrsetName")
	typ = strings.ToUpper(strParam(nr, "rrsetType"))
	if name == "" {
		name = strParam(nr, "name")
	}
	if typ == "" {
		typ = strings.ToUpper(strParam(nr, "type"))
	}
	return zone, name, typ
}

func rrsetID(zone, name, typ string) string {
	return zone + "/" + name + "/" + typ
}

func changeID(zone, id string) string {
	return zone + "/" + id
}

// operationID scopes an operation id to its managed zone: operations.get/list
// address the operation through the zone's operations collection, so the same
// operation id can exist in two zones.
func operationID(zone, id string) string {
	return zone + "/" + id
}

// decodeBody round-trips a request body map through JSON into a typed struct so
// clients can send any subset of fields without a hand-written decoder.
func decodeBody(body map[string]any, v any) error {
	if body == nil {
		return nil
	}
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// toMap converts a typed wire struct to the map shape ProviderResponse expects.
func toMap(v any) map[string]any {
	b, err := json.Marshal(v)
	if err != nil {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{}
	}
	return m
}

func stringMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

// rrToMap renders a record set with the output-only fields real GCP always
// includes: kind and an explicit (possibly empty) signatureRrdatas array.
func rrToMap(rr ResourceRecordSet) map[string]any {
	m := toMap(rr)
	if _, ok := m["kind"]; !ok {
		m["kind"] = kindResourceRecordSet
	}
	if _, ok := m["signatureRrdatas"]; !ok {
		m["signatureRrdatas"] = []any{}
	}
	if _, ok := m["rrdatas"]; !ok {
		m["rrdatas"] = []any{}
	}
	return m
}

// changeToMap renders a change with the output-only fields real GCP always
// includes: explicit (possibly empty) additions/deletions arrays and
// per-record kind/signatureRrdatas.
func changeToMap(c Change) map[string]any {
	m := toMap(c)
	for _, key := range []string{"additions", "deletions"} {
		arr, ok := m[key].([]any)
		if !ok {
			m[key] = []any{}
			continue
		}
		for i, e := range arr {
			em, ok := e.(map[string]any)
			if !ok {
				continue
			}
			if _, ok := em["kind"]; !ok {
				em["kind"] = kindResourceRecordSet
			}
			if _, ok := em["signatureRrdatas"]; !ok {
				em["signatureRrdatas"] = []any{}
			}
			if _, ok := em["rrdatas"]; !ok {
				em["rrdatas"] = []any{}
			}
			arr[i] = em
		}
		m[key] = arr
	}
	return m
}

// fqdn ensures a DNS name carries its trailing dot (Cloud DNS's canonical form).
func fqdn(name string) string {
	if name == "" || strings.HasSuffix(name, ".") {
		return name
	}
	return name + "."
}

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// numericID derives a stable decimal id from a string, mirroring Cloud DNS's
// server-generated numeric managed-zone/project ids. Cloud DNS ids are uint64 on
// the wire, but real server-generated ids are small; the official provider reads
// `managed_zone_id` as a signed Go int (tpgresource.StringToFixed64 → int) and
// fails the set with "expected type 'int'" when the value overflows int64, so
// the synthesized id is masked to the positive int64 range.
func numericID(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	n := h.Sum64() & (1<<63 - 1)
	if n == 0 {
		n = 1
	}
	return strconv.FormatUint(n, 10)
}

// nameServers returns the synthesized delegation set for a managed zone. The
// emulator serves no DNS, so these never resolve.
func nameServers() []string {
	return []string{
		"ns-cloud-a1.googledomains.com.",
		"ns-cloud-b1.googledomains.com.",
		"ns-cloud-c1.googledomains.com.",
		"ns-cloud-d1.googledomains.com.",
	}
}

func newChangeID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", 16)
	}
	return hex.EncodeToString(b)
}

func storeError(err error, what string) error {
	if errors.Is(err, store.ErrNotFound) {
		return model.NewProviderError("NotFound", what+" not found", 404)
	}
	return err
}

// defaultMaxResults is the page size Cloud DNS's emulator serves when the
// request omits maxResults (the Discovery document leaves the size to the
// server).
const defaultMaxResults = 100

// Cloud DNS's maxResults range. The Discovery document declares only an int32
// with no minimum or maximum ("If unspecified, the server decides how many
// results to return"), but real GCP's list backend accepts 1..1000 and rejects
// everything else with 400 "Invalid value for 'parameters.maxResults': '<v>'"
// — zero and negatives included, unlike Cloud SQL, which clamps an over-large
// value instead (DC61). Probe-recorded for managedZones, rrsets and changes
// (DC62).
const (
	minMaxResults = 1
	maxMaxResults = 1000
)

// maxResultsKeys is the emulator's page-size surface for Cloud DNS: the
// documented maxResults (int32) plus the emulator's pageSize alias. Both resolve
// through the same range, since the emulator cannot ignore the alias the way
// real GCP ignores an unknown query parameter.
var maxResultsKeys = []string{"maxResults", "pageSize"}

// bindPageSize applies Cloud DNS's maxResults query binding. Real GCP validates
// the binding in the API frontend, before routing, so an unparseable value is
// 400 even when the parent zone does not exist (probed: rrsets?maxResults=abc
// on a missing zone is 400). The backend's 1..1000 range check is separate and
// runs *after* the parent-zone lookup (rrsets?maxResults=0 on a missing zone is
// the zone's 404), so handlers call bindPageSize first and Resolve once the
// zone is known — or immediately on the parent-less managedZones.list.
func bindPageSize(params map[string]any) (paging.MaxResultsBinding, error) {
	return paging.BindMaxResults(params, maxResultsKeys)
}

// resolvePageSize applies the backend half of Cloud DNS's maxResults contract:
// the emulator's default when the parameter was absent, and the probed 1..1000
// range otherwise (an explicitly empty value is rejected by the binding before
// this point, DC63).
func resolvePageSize(b paging.MaxResultsBinding) (int, error) {
	return b.Resolve(defaultMaxResults, minMaxResults, maxMaxResults)
}

// paginate sorts items by key and applies maxResults/pageToken cursor
// pagination (Cloud DNS's list convention). The next page token is the
// base64url-encoded key of the last returned item. size is resolved by the
// caller before any parent lookup, so an invalid maxResults answers the binding
// error rather than a 404 for a missing zone.
func paginate[T any](items []T, key func(T) string, params map[string]any, size int) (page []T, nextPageToken string) {
	sort.Slice(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })
	start := 0
	if tok, _ := params["pageToken"].(string); tok != "" {
		if raw, err := base64.RawURLEncoding.DecodeString(tok); err == nil {
			cursor := string(raw)
			for start < len(items) && key(items[start]) <= cursor {
				start++
			}
		}
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	page = items[start:end]
	if end < len(items) {
		nextPageToken = base64.RawURLEncoding.EncodeToString([]byte(key(items[end-1])))
	}
	return page, nextPageToken
}
