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
// zones, rrsets, and changes are the supported surface; every DNSSEC, policy,
// and IAM method is deferred and fails loud with Unimplemented.
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
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource types in the shared ResourceStore.
const (
	rtManagedZone = "gcp_dns_managed_zone"
	rtRRSet       = "gcp_dns_rrset"
	rtChange      = "gcp_dns_change"
)

// Kind constants for the GCP wire envelope.
const (
	kindManagedZone        = "dns#managedZone"
	kindManagedZoneList    = "dns#managedZonesListResponse"
	kindResourceRecordSet  = "dns#resourceRecordSet"
	kindResourceRecordList = "dns#resourceRecordSetsListResponse"
	kindChange             = "dns#change"
	kindChangeList         = "dns#changesListResponse"
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

		"CloudDNS.ProjectGet":    p.GetProject,
		"CloudDNS.Unimplemented": p.Unimplemented,
	}
}

// ─── Wire types ───────────────────────────────────────────────────────────────

// ManagedZone is the dns#managedZone representation. id/nameServers/creationTime
// are output-only and synthesized on the server; the mutable fields are echoed
// from the request.
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
	// CloudLoggingConfig is output-only; real GCP always reports it (an empty
	// config carrying only the kind). Modeled so the wire response matches.
	CloudLoggingConfig map[string]any `json:"cloudLoggingConfig,omitempty"`
}

// withZoneDefaults fills the output-only managed-zone fields real GCP always
// reports, so zones created before these fields existed still render fully.
func withZoneDefaults(mz ManagedZone) ManagedZone {
	if mz.Visibility == "" {
		mz.Visibility = "public"
	}
	if mz.CloudLoggingConfig == nil {
		mz.CloudLoggingConfig = map[string]any{"kind": "dns#managedZoneCloudLoggingConfig"}
	}
	return mz
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
	return provider.OK(p.pageManagedZones(nr, zones)), nil
}

func (p *Provider) PatchManagedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.applyManagedZoneUpdate(ctx, nr)
}

func (p *Provider) UpdateManagedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.applyManagedZoneUpdate(ctx, nr)
}

// applyManagedZoneUpdate merges the mutable managed-zone fields (description,
// visibility, labels) from the request body. Both the PATCH and PUT wire
// methods share these merge semantics; dnsName is immutable and ignored.
func (p *Provider) applyManagedZoneUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "managedZone")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	mz, err := p.loadManagedZone(ctx, nr, name)
	if err != nil {
		return nil, err
	}
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
	}
	data, err := json.Marshal(mz)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtManagedZone, ID: mz.Name, Data: data}); err != nil {
		return nil, storeError(err, "managed zone")
	}
	return provider.OK(toMap(withZoneDefaults(mz))), nil
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

func (p *Provider) pageManagedZones(nr *model.NormalizedRequest, zones []ManagedZone) map[string]any {
	page, nextPageToken := paginate(zones, func(z ManagedZone) string { return z.Name }, nr.Params)
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
	zone := strParam(nr, "managedZone")
	if zone == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
	}
	if _, err := p.loadManagedZone(ctx, nr, zone); err != nil {
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
	return provider.OK(p.pageRRSets(nr, sets)), nil
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

func (p *Provider) pageRRSets(nr *model.NormalizedRequest, sets []ResourceRecordSet) map[string]any {
	page, nextPageToken := paginate(sets, func(r ResourceRecordSet) string { return r.Name + "/" + r.Type }, nr.Params)
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
	zone := strParam(nr, "managedZone")
	if zone == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing managedZone", 400)
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
	return provider.OK(p.pageChanges(nr, changes)), nil
}

func (p *Provider) pageChanges(nr *model.NormalizedRequest, changes []Change) map[string]any {
	page, nextPageToken := paginate(changes, func(c Change) string { return c.ID }, nr.Params)
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
// (DNSSEC keys, policies, response policies, and IAM).
func (p *Provider) Unimplemented(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("Unimplemented", "operation is not supported by the Cloud DNS emulator", 501)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
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
// server-generated numeric managed-zone/project ids.
func numericID(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	n := h.Sum64()
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

// paginate sorts items by key and applies maxResults/pageToken cursor
// pagination (Cloud DNS's list convention). The next page token is the
// base64url-encoded key of the last returned item.
func paginate[T any](items []T, key func(T) string, params map[string]any) (page []T, nextPageToken string) {
	sort.Slice(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })
	size := maxResults(params)
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

// maxResults reads the Cloud DNS maxResults query parameter, falling back to
// pageSize and the default of 100.
func maxResults(params map[string]any) int {
	for _, k := range []string{"maxResults", "pageSize"} {
		switch v := params[k].(type) {
		case string:
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				return n
			}
		case int:
			if v > 0 {
				return v
			}
		case float64:
			if v > 0 {
				return int(v)
			}
		}
	}
	return 100
}
