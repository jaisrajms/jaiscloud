// Package memorystore implements the Memorystore for Redis v1 control-plane
// provider (redis.googleapis.com/v1): instances plus the synthesized location
// discovery surface.
//
// This is metadata-only, mirroring the Cloud DNS provider: instances are stored
// as records in the shared ResourceStore, keyed by the project (AccountID) and
// the location (region scope). The emulator never stands up a real Redis
// server, so there is no data plane, no persistence, and no AUTH: an instance
// is born in state READY with a synthesized host and port 6379 that nothing
// listens on. Create/Update/Delete return the google.longrunning.Operation
// inline with done: true (the Dataproc/Metastore convention) because the
// shared locations/{location}/operations/{id} path is host-ambiguous with
// Cloud Workflows' LRO surface and is deliberately not claimed here.
//
// Deferred surfaces (import/export/failover/reschedule-maintenance/auth-string,
// backup collections, and the cluster surface) are not routed and fall through
// to the adapter's generic 404 rather than silently succeeding.
package memorystore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource types in the shared ResourceStore.
const (
	rtInstance = "gcp_memorystore_instance"
)

// Operation metadata @type for the Redis v1 control plane.
const operationMetadataType = "type.googleapis.com/google.cloud.redis.v1.OperationMetadata"

// Default Redis engine version when the caller does not pick one.
const defaultRedisVersion = "REDIS_7_0"

// Provider handles Memorystore for Redis v1 instances and locations.
type Provider struct {
	resources store.ResourceStore
}

// New returns a Provider backed by the shared ResourceStore.
func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

// Routes maps "Memorystore.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Memorystore.CreateInstance":  p.CreateInstance,
		"Memorystore.GetInstance":     p.GetInstance,
		"Memorystore.ListInstances":   p.ListInstances,
		"Memorystore.UpdateInstance":  p.UpdateInstance,
		"Memorystore.DeleteInstance":  p.DeleteInstance,
		"Memorystore.UpgradeInstance": p.UpgradeInstance,

		"Memorystore.GetLocation":   p.GetLocation,
		"Memorystore.ListLocations": p.ListLocations,
	}
}

// ─── Wire types ───────────────────────────────────────────────────────────────

// Instance is the redis.v1.Instance representation. id/creationTimestamp are
// synthesized (the real proto carries createTime and no id).
type Instance struct {
	Name              string            `json:"name,omitempty"`
	DisplayName       string            `json:"displayName,omitempty"`
	State             string            `json:"state,omitempty"`
	Tier              string            `json:"tier,omitempty"`
	MemorySizeGb      int64             `json:"memorySizeGb,omitempty"`
	RedisVersion      string            `json:"redisVersion,omitempty"`
	Host              string            `json:"host,omitempty"`
	Port              int64             `json:"port,omitempty"`
	CurrentLocationID string            `json:"currentLocationId,omitempty"`
	LocationID        string            `json:"locationId,omitempty"`
	CreateTime        string            `json:"createTime,omitempty"`
	RedisConfigs      map[string]string `json:"redisConfigs,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	ID                string            `json:"id,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
}

// Location is the redis.v1.Location representation.
type Location struct {
	Name        string            `json:"name,omitempty"`
	LocationID  string            `json:"locationId,omitempty"`
	DisplayName string            `json:"displayName,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// ─── Instance operations ──────────────────────────────────────────────────────

func (p *Provider) CreateInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	id := strParam(nr, "instanceId")
	if id == "" {
		id = instanceIDFromName(bodyString(body, "name"))
	}
	if id == "" {
		return nil, model.NewProviderError("InvalidArgument", "instanceId is required", 400)
	}

	inst := Instance{}
	_ = decodeBody(body, &inst)
	inst.Name = nr.ResourceID("memorystore-instance", location+"/"+id)
	if inst.Tier == "" {
		inst.Tier = "BASIC"
	}
	if inst.Tier != "BASIC" && inst.Tier != "STANDARD_HA" {
		return nil, model.NewProviderError("InvalidArgument", "tier must be BASIC or STANDARD_HA", 400)
	}
	if inst.MemorySizeGb <= 0 {
		inst.MemorySizeGb = 1
	}
	if inst.RedisVersion == "" {
		inst.RedisVersion = defaultRedisVersion
	}
	inst.LocationID = location
	inst.CurrentLocationID = location
	inst.State = "READY"
	inst.Host = synthHost(inst.Name)
	inst.Port = 6379
	inst.CreateTime = formatTimestamp(clock.Now())
	inst.CreationTimestamp = inst.CreateTime
	inst.ID = numericID(inst.Name)

	data, err := json.Marshal(inst)
	if err != nil {
		return nil, err
	}
	err = p.resources.Create(ctx, nr.AccountID, location, store.ResourceEntry{Type: rtInstance, ID: id, Data: data})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, model.NewProviderError("AlreadyExists", fmt.Sprintf("instance %q already exists", id), 409)
	}
	if err != nil {
		return nil, err
	}
	return provider.OK(p.operation(nr, location, "create", inst.Name, toMap(inst))), nil
}

func (p *Provider) GetInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location, id := instanceParams(nr)
	if location == "" || id == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or instance id", 400)
	}
	inst, err := p.loadInstance(ctx, nr.AccountID, location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(toMap(inst)), nil
}

func (p *Provider) ListInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	entries, err := p.resources.List(ctx, nr.AccountID, location, rtInstance, "")
	if err != nil {
		return nil, err
	}
	instances := make([]Instance, 0, len(entries))
	for _, e := range entries {
		var inst Instance
		if json.Unmarshal(e.Data, &inst) == nil {
			instances = append(instances, inst)
		}
	}
	page, next := paging.Page(instances, func(i Instance) string { return i.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, inst := range page {
		items = append(items, toMap(inst))
	}
	resp := map[string]any{"instances": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location, id := instanceParams(nr)
	if location == "" || id == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or instance id", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	mask := strParam(nr, "updateMask")

	updated, err := p.resources.UpsertAtomic(ctx, nr.AccountID, location, rtInstance, id, func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
		if !exists {
			return store.ResourceEntry{}, store.ErrNotFound
		}
		var inst Instance
		if err := json.Unmarshal(current.Data, &inst); err != nil {
			return store.ResourceEntry{}, err
		}
		if err := applyInstanceUpdate(&inst, body, mask); err != nil {
			return store.ResourceEntry{}, err
		}
		inst.State = "READY"
		data, err := json.Marshal(inst)
		if err != nil {
			return store.ResourceEntry{}, err
		}
		current.Data = data
		return current, nil
	})
	if err != nil {
		return nil, mapErr(err, "instance")
	}
	var inst Instance
	if err := json.Unmarshal(updated.Data, &inst); err != nil {
		return nil, err
	}
	return provider.OK(p.operation(nr, location, "update", inst.Name, toMap(inst))), nil
}

func (p *Provider) DeleteInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location, id := instanceParams(nr)
	if location == "" || id == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or instance id", 400)
	}
	inst, err := p.loadInstance(ctx, nr.AccountID, location, id)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, nr.AccountID, location, rtInstance, id); err != nil {
		return nil, mapErr(err, "instance")
	}
	return provider.OK(p.operation(nr, location, "delete", inst.Name, map[string]any{})), nil
}

// UpgradeInstance applies a redisVersion upgrade (the only field on
// UpgradeInstanceRequest) and returns the updated instance in a done operation.
func (p *Provider) UpgradeInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location, id := instanceParams(nr)
	if location == "" || id == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location or instance id", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	version := bodyString(body, "redisVersion")

	updated, err := p.resources.UpsertAtomic(ctx, nr.AccountID, location, rtInstance, id, func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
		if !exists {
			return store.ResourceEntry{}, store.ErrNotFound
		}
		var inst Instance
		if err := json.Unmarshal(current.Data, &inst); err != nil {
			return store.ResourceEntry{}, err
		}
		if version != "" {
			inst.RedisVersion = version
		}
		inst.State = "READY"
		data, err := json.Marshal(inst)
		if err != nil {
			return store.ResourceEntry{}, err
		}
		current.Data = data
		return current, nil
	})
	if err != nil {
		return nil, mapErr(err, "instance")
	}
	var inst Instance
	if err := json.Unmarshal(updated.Data, &inst); err != nil {
		return nil, err
	}
	return provider.OK(p.operation(nr, location, "upgrade", inst.Name, toMap(inst))), nil
}

func (p *Provider) loadInstance(ctx context.Context, account, location, id string) (Instance, error) {
	e, err := p.resources.Get(ctx, account, location, rtInstance, id)
	if err != nil {
		return Instance{}, mapErr(err, "instance")
	}
	var inst Instance
	if err := json.Unmarshal(e.Data, &inst); err != nil {
		return Instance{}, err
	}
	return inst, nil
}

// applyInstanceUpdate merges the mutable instance fields. A non-empty mask
// names the paths to overlay (masked paths take the incoming value, unmasked
// paths retain the stored value); an empty mask overlays every field present in
// the request. Unsupported mask paths fail loud rather than silently no-op.
func applyInstanceUpdate(inst *Instance, body map[string]any, mask string) error {
	// Validate every masked path up front so an unsupported field fails loud
	// rather than being silently no-op-ed.
	for _, field := range splitMask(mask) {
		if !supportedMaskField(field) {
			return model.NewProviderError("InvalidArgument", "unsupported updateMask field "+field, 400)
		}
	}
	incoming := Instance{}
	if err := decodeBody(body, &incoming); err != nil {
		return err
	}
	apply := func(field string) bool {
		return mask == "" || containsMaskField(mask, field)
	}
	if apply("displayName") && bodyHas(body, "displayName") {
		inst.DisplayName = incoming.DisplayName
	}
	if apply("labels") && bodyHas(body, "labels") {
		inst.Labels = incoming.Labels
	}
	if apply("memorySizeGb") && bodyHas(body, "memorySizeGb") {
		if incoming.MemorySizeGb > 0 {
			inst.MemorySizeGb = incoming.MemorySizeGb
		}
	}
	if (apply("redisVersion") || apply("redis_version")) && bodyHas(body, "redisVersion") {
		if incoming.RedisVersion != "" {
			inst.RedisVersion = incoming.RedisVersion
		}
	}
	for _, field := range []string{"redisConfigs", "redisConfig", "redis_config"} {
		if apply(field) && redisConfigsPresent(body) {
			inst.RedisConfigs = incoming.RedisConfigs
			break
		}
	}
	return nil
}

// redisConfigsPresent reports whether any of the accepted redis-config JSON
// keys is present in the request body.
func redisConfigsPresent(body map[string]any) bool {
	_, a := body["redisConfigs"]
	_, b := body["redisConfig"]
	_, c := body["redis_config"]
	return a || b || c
}

func supportedMaskField(field string) bool {
	switch field {
	case "displayName", "labels", "memorySizeGb", "redisVersion", "redis_version",
		"redisConfigs", "redisConfig", "redis_config", "replicaCount", "replica_count":
		return true
	}
	return false
}

func bodyHas(body map[string]any, key string) bool {
	if body == nil {
		return false
	}
	_, ok := body[key]
	return ok
}

// ─── Location operations ──────────────────────────────────────────────────────

// memorystoreRegions is the synthesized set of locations the emulator advertises.
// Real Memorystore is available in many more regions; this is a stable subset.
var memorystoreRegions = []string{
	"asia-east1",
	"asia-northeast1",
	"asia-southeast1",
	"europe-north1",
	"europe-west1",
	"europe-west4",
	"us-central1",
	"us-east1",
	"us-east4",
	"us-west1",
}

func (p *Provider) GetLocation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	return provider.OK(toMap(Location{
		Name:        nr.ResourceID("memorystore-location", location),
		LocationID:  location,
		DisplayName: location,
	})), nil
}

func (p *Provider) ListLocations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	locations := make([]Location, 0, len(memorystoreRegions))
	for _, r := range memorystoreRegions {
		locations = append(locations, Location{
			Name:        nr.ResourceID("memorystore-location", r),
			LocationID:  r,
			DisplayName: r,
		})
	}
	page, next := paging.Page(locations, func(l Location) string { return l.LocationID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, l := range page {
		items = append(items, toMap(l))
	}
	resp := map[string]any{"locations": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// operation renders a completed google.longrunning.Operation. The emulator
// completes every mutation synchronously, so done is always true and the
// operation is never persisted (its name is not addressable).
func (p *Provider) operation(nr *model.NormalizedRequest, location, verb, target string, response map[string]any) map[string]any {
	now := clock.Now().UTC()
	op := map[string]any{
		"name": nr.ResourceID("memorystore-operation", location+"/"+randomHex(12)),
		"metadata": map[string]any{
			"@type":      operationMetadataType,
			"createTime": formatTimestamp(now),
			"endTime":    formatTimestamp(now),
			"target":     target,
			"verb":       verb,
			"apiVersion": "v1",
		},
		"done": true,
	}
	if response != nil {
		op["response"] = response
	}
	return op
}

// instanceParams returns (location, id) for an instance request, falling back to
// the full resource name when a discrete param is absent.
func instanceParams(nr *model.NormalizedRequest) (location, id string) {
	location = strParam(nr, "location")
	name := strParam(nr, "name")
	id = strParam(nr, "instanceId")
	if id == "" {
		id = instanceIDFromName(name)
	}
	if location == "" {
		location = locationFromName(name)
	}
	return location, id
}

// instanceIDFromName extracts the id from a "…/instances/{id}" resource name.
func instanceIDFromName(name string) string {
	parts := strings.Split(name, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "instances" {
			return parts[i+1]
		}
	}
	return ""
}

// locationFromName extracts the location from a "…/locations/{loc}/…" name.
func locationFromName(name string) string {
	parts := strings.Split(name, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "locations" {
			return parts[i+1]
		}
	}
	return ""
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
	return s
}

// mapErr translates store sentinels into GCP ProviderErrors.
func mapErr(err error, what string) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return model.NewProviderError("NotFound", what+" not found", 404)
	case errors.Is(err, store.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", what+" already exists", 409)
	}
	return err
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

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
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

// synthHost derives a stable, cosmetic IPv4 endpoint from the instance name.
// Nothing listens on it; a real Memorystore host is a private VPC IP.
func synthHost(seed string) string {
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

// containsMaskField reports whether a comma-separated updateMask contains the
// given field.
func containsMaskField(mask, field string) bool {
	for _, part := range splitMask(mask) {
		if part == field {
			return true
		}
	}
	return false
}

func splitMask(mask string) []string {
	var out []string
	for _, p := range strings.Split(mask, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
