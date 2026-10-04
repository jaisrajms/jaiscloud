// Package eventarc is the transport-neutral core of the Eventarc v1 control
// plane (eventarc.googleapis.com): trigger-based event routing metadata.
//
// Eventarc is the GCP analogue of AWS EventBridge in purpose, but a *different*
// model — this is deliberately NOT a port of EventBridge's bus + rule +
// event-pattern + target fan-out. A Trigger routes events from a source (a
// Pub/Sub topic or a Channel) to a destination (Cloud Run service, Cloud
// Functions v2, Workflows, GKE) with optional eventFilters, plus Channel
// (3rd-party source) and Provider (discovery) resources.
//
// It owns all trigger/channel/provider and trigger/channel IAM business logic
// over internal/gcp/store/eventarc. It deliberately has no dependency on
// protobuf or on NormalizedRequest: the gRPC transport
// (internal/gcp/transport/grpc/eventarc) and the REST transport
// (internal/gcp/transport/rest/eventarc) both transcode their wire format into
// this package's typed API and call the SAME Service instance. That is the
// dual-protocol invariant: one core, one piece of state, so the transports
// cannot drift.
//
// The core also owns event delivery for non-function destinations: on a Pub/Sub
// publish it matches triggers and sends a binary-mode CloudEvents request to
// destination.httpEndpoint or destination.cloudRun (a later phase adds Cloud
// Storage sources). Cloud Functions destinations are delivered by the functions
// engine; this package only routes to them via eventing.TargetIndex.
package eventarc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/eventing"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"github.com/google/uuid"
)

// FunctionExister reports whether a Cloud Functions function exists, so Eventarc
// can validate a destination.cloudFunction without importing the functions
// core. A nil FunctionExister disables the existence check (the destination is
// still validated structurally).
type FunctionExister interface {
	FunctionExists(ctx context.Context, project, location, id string) (bool, error)
}

// rtTopic is the Pub/Sub topic resource type in the shared ResourceStore
// (mirrors provider/pubsub's unexported constant). Eventarc validates
// transport.pubsub.topic references against it.
const rtTopic = "gcp_topic"

// rtTriggerPolicy / rtChannelPolicy are the generic ResourceStore types for
// Eventarc trigger/channel IAM policies (mirrors provider/functions).
const (
	rtTriggerPolicy = "gcp_eventarc_trigger_policy"
	rtChannelPolicy = "gcp_eventarc_channel_policy"
)

// Service is the transport-neutral Eventarc v1 service.
type Service struct {
	store         eventarcstore.Store
	resources     store.ResourceStore  // shared control-plane store (Pub/Sub topic existence + IAM)
	workflows     workflowsstore.Store // Cloud Workflows store (destination.workflow existence)
	functions     FunctionExister      // Cloud Functions existence (destination.cloudFunction)
	cloudRun      CloudRunInvoker      // Cloud Run delivery seam (destination.cloudRun)
	subscriptions eventing.SubscriptionProvisioner
	httpClient    httpDoer       // outbound delivery client; nil disables delivery
	deliveryWG    sync.WaitGroup // tracks in-flight fire-and-forget deliveries (tests)
}

// NewService returns an Eventarc core backed by the given stores. Outbound
// event delivery is enabled with a bounded default HTTP client.
func NewService(s eventarcstore.Store, resources store.ResourceStore, workflows workflowsstore.Store) *Service {
	return &Service{
		store:      s,
		resources:  resources,
		workflows:  workflows,
		httpClient: eventarcHTTPClient(),
	}
}

// SetFunctionExister wires the Cloud Functions existence check used to validate
// a destination.cloudFunction. A nil exister (the default) skips the check.
func (s *Service) SetFunctionExister(f FunctionExister) { s.functions = f }

// SetCloudRunInvoker wires the Cloud Run delivery seam used by a
// destination.cloudRun trigger. A nil invoker (the default) logs-and-drops
// cloudRun deliveries, matching a destination the emulator cannot reach.
func (s *Service) SetCloudRunInvoker(inv CloudRunInvoker) { s.cloudRun = inv }

// SetSubscriptionProvisioner wires the Pub/Sub provisioner that creates the
// transport subscription backing an Eventarc trigger (its dead-letter surface).
// A nil provisioner (the default) skips subscription provisioning.
func (s *Service) SetSubscriptionProvisioner(p eventing.SubscriptionProvisioner) { s.subscriptions = p }

// Reset wipes the store.
func (s *Service) Reset(ctx context.Context) { s.store.Reset(ctx) }

// --- config decoding helpers ---

// decodeBody unmarshals a raw JSON request body into a map, returning nil for
// an absent or non-object body.
func decodeBody(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	return m
}

func bodyMap(body map[string]any, key string) map[string]any {
	if body == nil {
		return nil
	}
	m, _ := body[key].(map[string]any)
	return m
}

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	v, _ := body[key].(string)
	return v
}

func bodyStringMap(body map[string]any, key string) map[string]string {
	m := bodyMap(body, key)
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// labelsFromConfig extracts the top-level "labels" object, or nil when unset.
func labelsFromConfig(raw json.RawMessage) map[string]string {
	return bodyStringMap(decodeBody(raw), "labels")
}

// BodyEtag extracts the top-level "etag" precondition from a decoded request
// body. It is the transport-neutral accessor the REST adapter uses for the
// Trigger/Channel update OCC precondition (the gRPC transport reads the proto
// field directly).
func BodyEtag(body map[string]any) string { return bodyString(body, "etag") }

// pageParams builds the shared cursor-pagination parameter map.
func pageParams(pageSize int, pageToken string) map[string]any {
	params := map[string]any{}
	if pageSize > 0 {
		params["pageSize"] = pageSize
	}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	return params
}

// --- errors ---

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// InvalidIAMResource is the canonical InvalidArgument error for an IAM request
// naming a resource the Eventarc service does not own.
func InvalidIAMResource() error {
	return invalidArgument("invalid resource name")
}

// mapErr maps an eventarc store error onto a canonical provider error.
func mapErr(err error) error {
	switch {
	case errors.Is(err, eventarcstore.ErrNoSuchTrigger):
		return model.NewProviderError("NotFound", "trigger not found", 404)
	case errors.Is(err, eventarcstore.ErrNoSuchChannel):
		return model.NewProviderError("NotFound", "channel not found", 404)
	case errors.Is(err, eventarcstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	}
	return err
}

// abortedEtagMismatch is the etag-precondition failure returned as HTTP 409
// with the ABORTED google.rpc status (the exact status is unverified against
// real Eventarc; ABORTED matches the sibling shared-IAM OCC contract).
func abortedEtagMismatch() error {
	return &model.ProviderError{
		Code:       "Aborted",
		Message:    "etag mismatch: optimistic concurrency control failed",
		HTTPStatus: 409,
		Status:     "ABORTED",
	}
}

// checkEtag enforces the request-supplied etag precondition against the stored
// value. An empty request etag is a no-op (the caller opted out of OCC).
func checkEtag(reqEtag, storedEtag string) error {
	if reqEtag != "" && reqEtag != storedEtag {
		return abortedEtagMismatch()
	}
	return nil
}

// --- update-mask helpers ---

// maskPaths splits a comma-separated updateMask query value into its non-empty
// field paths. An empty mask returns nil, meaning "apply every field present in
// the request body" (the historical merge behavior).
func maskPaths(mask string) []string {
	if mask == "" {
		return nil
	}
	parts := strings.Split(mask, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func maskRoot(path string) string {
	if i := strings.IndexByte(path, '.'); i >= 0 {
		return path[:i]
	}
	return path
}

// normalizeMaskField folds a field-mask path segment to a case/underscore-
// insensitive form so both the proto snake_case (event_filters) and the JSON
// camelCase (eventFilters) spellings resolve to the same field.
func normalizeMaskField(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

// triggerMaskCanonical maps a normalized update_mask root to the canonical
// camelCase JSON key of an updatable Trigger field. Output-only fields are
// intentionally absent so a mask naming one fails loud rather than silently
// echoing.
var triggerMaskCanonical = map[string]string{
	"destination":          "destination",
	"eventfilters":         "eventFilters",
	"serviceaccount":       "serviceAccount",
	"transport":            "transport",
	"channel":              "channel",
	"labels":               "labels",
	"eventdatacontenttype": "eventDataContentType",
	"retrypolicy":          "retryPolicy",
}

// channelMaskCanonical maps a normalized update_mask root to the canonical
// camelCase JSON key of an updatable Channel field.
var channelMaskCanonical = map[string]string{
	"provider":      "provider",
	"cryptokeyname": "cryptoKeyName",
	"labels":        "labels",
}

// applyTriggerMask merges an incoming Trigger body into the stored body
// according to the updateMask paths: a masked path takes the incoming value,
// every unmasked path retains the stored value. An empty mask merges every body
// field. An unsupported path fails loud with Unimplemented.
func applyTriggerMask(stored, incoming map[string]any, paths []string) (map[string]any, error) {
	merged := make(map[string]any, len(stored)+len(incoming))
	for k, v := range stored {
		merged[k] = v
	}
	if len(paths) == 0 {
		for k, v := range incoming {
			merged[k] = v
		}
		return merged, nil
	}
	for _, path := range paths {
		canon, ok := triggerMaskCanonical[normalizeMaskField(maskRoot(path))]
		if !ok {
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+path, 501)
		}
		if v, present := incoming[canon]; present {
			merged[canon] = v
		}
	}
	return merged, nil
}

// applyChannelMask is applyTriggerMask for Channel bodies.
func applyChannelMask(stored, incoming map[string]any, paths []string) (map[string]any, error) {
	merged := make(map[string]any, len(stored)+len(incoming))
	for k, v := range stored {
		merged[k] = v
	}
	if len(paths) == 0 {
		for k, v := range incoming {
			merged[k] = v
		}
		return merged, nil
	}
	for _, path := range paths {
		canon, ok := channelMaskCanonical[normalizeMaskField(maskRoot(path))]
		if !ok {
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+path, 501)
		}
		if v, present := incoming[canon]; present {
			merged[canon] = v
		}
	}
	return merged, nil
}

// --- validation ---

// validateDestination enforces the destination oneof contract: exactly one of
// cloudFunction / cloudRun / gke / workflow / httpEndpoint must be set. A
// destination.cloudFunction names a deployed function (as the Cloud Functions
// service itself sets when it provisions an Eventarc trigger); the existence
// check lives in validateReferences. A destination.cloudRun must carry its
// required fields (see validateCloudRunDestination).
func validateDestination(project string, dest map[string]any) error {
	if dest == nil {
		return invalidArgument("destination is required")
	}
	cloudRunDest, _ := dest["cloudRun"].(map[string]any)
	set := 0
	if cf, _ := dest["cloudFunction"].(string); cf != "" {
		set++
	}
	if cloudRunDest != nil {
		set++
	}
	if m, _ := dest["gke"].(map[string]any); m != nil {
		set++
	}
	if wf, _ := dest["workflow"].(string); wf != "" {
		set++
	}
	if m, _ := dest["httpEndpoint"].(map[string]any); m != nil {
		set++
	}
	if set == 0 {
		return invalidArgument("destination must specify one of cloudFunction, cloudRun, gke, workflow, or httpEndpoint")
	}
	if set > 1 {
		return invalidArgument("destination must specify exactly one of cloudFunction, cloudRun, gke, workflow, or httpEndpoint")
	}
	if cloudRunDest != nil {
		return validateCloudRunDestination(project, cloudRunDest)
	}
	return nil
}

// validateCloudRunDestination enforces the required shape of a
// destination.cloudRun, matching the Eventarc v1 CloudRun schema: service and
// region are both required, and the service must live in the trigger's project.
// A full service resource name (projects/{p}/locations/{l}/services/{s}) is
// accepted leniently: when region is omitted it is derived from the name's
// location, but any other slash-containing value is rejected and an explicit
// region that disagrees with the name's location is rejected. floci's short-id
// form ({"service":"svc","region":"us-central1"}) is accepted as-is.
//
// This is intentionally stricter than cloudRunDestination (delivery.go), which
// stays lenient so state stored before this validation existed can still be
// delivered.
func validateCloudRunDestination(project string, m map[string]any) error {
	service := strings.TrimSpace(bodyString(m, "service"))
	if service == "" {
		return invalidArgument("destination.cloudRun.service is required")
	}
	region := strings.TrimSpace(bodyString(m, "region"))
	if strings.Contains(service, "/") {
		name := ParseName(service)
		if name.Project == "" || name.Location == "" {
			return invalidArgument("destination.cloudRun.service is not a valid service resource name")
		}
		if project != "" && name.Project != project {
			return invalidArgument("destination.cloudRun.service must be in the trigger's project: " + project)
		}
		if region == "" {
			region = name.Location
		} else if region != name.Location {
			return invalidArgument("destination.cloudRun.region disagrees with destination.cloudRun.service location")
		}
	}
	if region == "" {
		return invalidArgument("destination.cloudRun.region is required")
	}
	return nil
}

// validateFilters enforces the eventFilters contract: at least one filter,
// each with a non-empty attribute and value, and at least one filter whose
// attribute is "type" — real Eventarc requires a type filter on every trigger.
// A trigger whose type filter names a Cloud Storage event is a Cloud Storage
// trigger and must additionally declare a non-empty "bucket" filter; real
// Eventarc rejects a Cloud Storage trigger without one, and accepting it would
// deliver events from every bucket.
func validateFilters(body map[string]any) error {
	filters, ok := body["eventFilters"].([]any)
	if !ok || len(filters) == 0 {
		return invalidArgument("eventFilters is required")
	}
	hasType, hasBucket, storageType := false, false, false
	for _, f := range filters {
		fm, ok := f.(map[string]any)
		if !ok {
			return invalidArgument("eventFilters entries must be objects")
		}
		attr, _ := fm["attribute"].(string)
		if attr == "" {
			return invalidArgument("eventFilters[].attribute is required")
		}
		value, ok := fm["value"].(string)
		if !ok {
			return invalidArgument("eventFilters[].value is required")
		}
		switch {
		case attr == "type":
			if value != "" {
				hasType = true
			}
			if eventing.IsCloudStorageEventType(value) {
				storageType = true
			}
		case attr == "bucket" && value != "":
			hasBucket = true
		}
	}
	if !hasType {
		return invalidArgument(`eventFilters must contain a filter with attribute "type"`)
	}
	if storageType && !hasBucket {
		return invalidArgument(`Cloud Storage triggers must contain a filter with attribute "bucket"`)
	}
	return nil
}

// validateReferences enforces the source/destination reference contract: a
// transport.pubsub.topic must name an existing Pub/Sub topic, a
// destination.workflow must name an existing Workflow, and a channel must name
// an existing Channel.
func (s *Service) validateReferences(ctx context.Context, project string, body map[string]any) error {
	if dest := bodyMap(body, "destination"); dest != nil {
		if wf, _ := dest["workflow"].(string); wf != "" {
			loc, id := locationOf(wf), lastSegment(wf)
			if loc == "" || id == "" {
				return invalidArgument("invalid destination.workflow resource name")
			}
			if _, err := s.workflows.GetWorkflow(ctx, project, loc, id); err != nil {
				return model.NewProviderError("NotFound", "destination.workflow not found: "+wf, 404)
			}
		}
		if cf, _ := dest["cloudFunction"].(string); cf != "" {
			loc, id := locationOf(cf), lastSegment(cf)
			if loc == "" || id == "" {
				return invalidArgument("invalid destination.cloudFunction resource name")
			}
			if s.functions != nil {
				ok, err := s.functions.FunctionExists(ctx, project, loc, id)
				if err != nil {
					return err
				}
				if !ok {
					return model.NewProviderError("NotFound", "destination.cloudFunction not found: "+cf, 404)
				}
			}
		}
	}
	if transport := bodyMap(body, "transport"); transport != nil {
		if pubsub := bodyMap(transport, "pubsub"); pubsub != nil {
			if topic, _ := pubsub["topic"].(string); topic != "" {
				topicID := lastSegment(topic)
				if _, err := s.resources.Get(ctx, project, store.GlobalRegion, rtTopic, topicID); err != nil {
					return model.NewProviderError("NotFound", "transport.pubsub.topic not found: "+topic, 404)
				}
			}
		}
	}
	if ch, _ := body["channel"].(string); ch != "" {
		loc, id := locationOf(ch), lastSegment(ch)
		if loc == "" || id == "" {
			return invalidArgument("invalid channel resource name")
		}
		if _, err := s.store.GetChannel(ctx, project, loc, id); err != nil {
			return model.NewProviderError("NotFound", "channel not found: "+ch, 404)
		}
	}
	return nil
}

func (s *Service) validateTrigger(ctx context.Context, project string, body map[string]any) error {
	if body == nil {
		return invalidArgument("missing trigger body")
	}
	if err := validateFilters(body); err != nil {
		return err
	}
	if err := validateDestination(project, bodyMap(body, "destination")); err != nil {
		return err
	}
	return s.validateReferences(ctx, project, body)
}

func (s *Service) validateChannel(ctx context.Context, project string, body map[string]any) error {
	if body == nil {
		return invalidArgument("missing channel body")
	}
	return s.validateReferences(ctx, project, body)
}

// --- Triggers ---

// CreateTrigger creates a trigger and returns it with the done create
// operation. validateOnly performs the validation and existence check without
// persisting.
func (s *Service) CreateTrigger(ctx context.Context, project, location, triggerID string, cfg json.RawMessage, validateOnly bool) (eventarcstore.Trigger, Operation, error) {
	if location == "" || triggerID == "" {
		return eventarcstore.Trigger{}, Operation{}, invalidArgument("missing location or triggerId")
	}
	body := decodeBody(cfg)
	if err := s.validateTrigger(ctx, project, body); err != nil {
		return eventarcstore.Trigger{}, Operation{}, err
	}
	now := clock.Now().UTC()
	t := eventarcstore.Trigger{
		Location:   location,
		Name:       triggerID,
		Labels:     labelsFromConfig(cfg),
		UID:        uuid.NewString(),
		CreateTime: now,
		UpdateTime: now,
	}
	if len(cfg) > 0 {
		t.Config = cfg
	}
	t.Etag = triggerEtag(t)
	target := TriggerName(project, location, triggerID)
	if validateOnly {
		if _, err := s.store.GetTrigger(ctx, project, location, triggerID); err == nil {
			return eventarcstore.Trigger{}, Operation{}, mapErr(eventarcstore.ErrAlreadyExists)
		} else if !errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
			return eventarcstore.Trigger{}, Operation{}, mapErr(err)
		}
		return t, newOperation(location, "create", target), nil
	}
	if err := s.store.CreateTrigger(ctx, project, location, t); err != nil {
		return eventarcstore.Trigger{}, Operation{}, mapErr(err)
	}
	if err := s.SyncTriggerSubscription(ctx, project, t); err != nil {
		return eventarcstore.Trigger{}, Operation{}, err
	}
	return t, newOperation(location, "create", target), nil
}

// GetTrigger returns one trigger.
func (s *Service) GetTrigger(ctx context.Context, project, location, triggerID string) (eventarcstore.Trigger, error) {
	if location == "" || triggerID == "" {
		return eventarcstore.Trigger{}, invalidArgument("missing location or trigger id")
	}
	t, err := s.store.GetTrigger(ctx, project, location, triggerID)
	if err != nil {
		return eventarcstore.Trigger{}, mapErr(err)
	}
	return t, nil
}

// ListTriggers returns a cursor page of the triggers in a location.
func (s *Service) ListTriggers(ctx context.Context, project, location string, pageSize int, pageToken string) ([]eventarcstore.Trigger, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	triggers, err := s.store.ListTriggers(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(triggers, func(t eventarcstore.Trigger) string { return t.Name }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// ListTriggersByProject returns every trigger in a project across all
// locations, sorted by location then id. It backs the location-optional
// console list (the REST/gRPC ListTriggers method is per-location).
func (s *Service) ListTriggersByProject(ctx context.Context, project string) ([]eventarcstore.Trigger, error) {
	triggers, err := s.store.ListTriggersAllLocations(ctx, project)
	if err != nil {
		return nil, mapErr(err)
	}
	return triggers, nil
}

// UpdateTrigger merges the caller's fields into the stored trigger and returns
// it with the done update operation. The merge honors updateMask (comma-
// separated; empty means apply every field in the body). reqEtag is the
// body-supplied etag precondition.
func (s *Service) UpdateTrigger(ctx context.Context, project, location, triggerID string, cfg json.RawMessage, mask, reqEtag string, validateOnly bool) (eventarcstore.Trigger, Operation, error) {
	if location == "" || triggerID == "" {
		return eventarcstore.Trigger{}, Operation{}, invalidArgument("missing location or trigger id")
	}
	body := decodeBody(cfg)
	paths := maskPaths(mask)

	// Reference validation reads the shared stores (e.g. a channel named in the
	// body), so it must run outside UpdateTriggerAtomic's locked closure: the
	// store mutex is not reentrant and validateReferences re-enters it.
	stored, err := s.store.GetTrigger(ctx, project, location, triggerID)
	if err != nil {
		return eventarcstore.Trigger{}, Operation{}, mapErr(err)
	}
	if err := checkEtag(reqEtag, stored.Etag); err != nil {
		return eventarcstore.Trigger{}, Operation{}, err
	}
	next, merged, err := mergeTriggerBody(stored, body, paths)
	if err != nil {
		return eventarcstore.Trigger{}, Operation{}, mapErr(err)
	}
	if err := s.validateTrigger(ctx, project, merged); err != nil {
		return eventarcstore.Trigger{}, Operation{}, mapErr(err)
	}
	target := TriggerName(project, location, triggerID)
	if validateOnly {
		return next, newOperation(location, "update", target), nil
	}

	updated, err := s.store.UpdateTriggerAtomic(ctx, project, location, triggerID, func(t eventarcstore.Trigger) (eventarcstore.Trigger, error) {
		// The etag precondition and the masked merge both run inside the
		// store's locked mutate closure, so a stale etag can't slip past a
		// concurrent update and a masked PATCH can't clobber a concurrent
		// PATCH's disjoint fields.
		if err := checkEtag(reqEtag, t.Etag); err != nil {
			return eventarcstore.Trigger{}, err
		}
		next, _, err := mergeTriggerBody(t, body, paths)
		if err != nil {
			return eventarcstore.Trigger{}, err
		}
		next.UpdateTime = clock.Now().UTC()
		next.Etag = triggerEtag(next)
		return next, nil
	})
	if err != nil {
		return eventarcstore.Trigger{}, Operation{}, mapErr(err)
	}
	if err := s.SyncTriggerSubscription(ctx, project, updated); err != nil {
		return eventarcstore.Trigger{}, Operation{}, err
	}
	return updated, newOperation(location, "update", target), nil
}

// mergeTriggerBody applies a Trigger PATCH body to the stored trigger under the
// given updateMask paths. It performs no reference validation and never touches
// the store, so it is safe to call from inside the store's locked mutate
// closure.
func mergeTriggerBody(t eventarcstore.Trigger, body map[string]any, paths []string) (eventarcstore.Trigger, map[string]any, error) {
	stored := map[string]any{}
	if len(t.Config) > 0 {
		_ = json.Unmarshal(t.Config, &stored)
	}
	merged, err := applyTriggerMask(stored, body, paths)
	if err != nil {
		return eventarcstore.Trigger{}, nil, err
	}
	if labels := bodyStringMap(merged, "labels"); labels != nil {
		t.Labels = labels
	}
	if data, err := json.Marshal(merged); err == nil {
		t.Config = data
	}
	return t, merged, nil
}

// DeleteTrigger deletes a trigger and returns the deleted trigger with the done
// delete operation (the proto's delete response_type is Trigger).
func (s *Service) DeleteTrigger(ctx context.Context, project, location, triggerID, reqEtag string, validateOnly bool) (eventarcstore.Trigger, Operation, error) {
	if location == "" || triggerID == "" {
		return eventarcstore.Trigger{}, Operation{}, invalidArgument("missing location or trigger id")
	}
	target := TriggerName(project, location, triggerID)
	if validateOnly {
		stored, err := s.store.GetTrigger(ctx, project, location, triggerID)
		if err != nil {
			return eventarcstore.Trigger{}, Operation{}, mapErr(err)
		}
		if err := checkEtag(reqEtag, stored.Etag); err != nil {
			return eventarcstore.Trigger{}, Operation{}, err
		}
		return stored, newOperation(location, "delete", target), nil
	}
	var deleted eventarcstore.Trigger
	if err := s.store.DeleteTriggerAtomic(ctx, project, location, triggerID, func(t eventarcstore.Trigger) error {
		if err := checkEtag(reqEtag, t.Etag); err != nil {
			return err
		}
		deleted = t
		return nil
	}); err != nil {
		return eventarcstore.Trigger{}, Operation{}, mapErr(err)
	}
	s.DeleteTriggerSubscription(ctx, project, deleted)
	return deleted, newOperation(location, "delete", target), nil
}

// --- Channels ---

// CreateChannel creates a channel and returns it with the done create
// operation.
func (s *Service) CreateChannel(ctx context.Context, project, location, channelID string, cfg json.RawMessage, validateOnly bool) (eventarcstore.Channel, Operation, error) {
	if location == "" || channelID == "" {
		return eventarcstore.Channel{}, Operation{}, invalidArgument("missing location or channelId")
	}
	body := decodeBody(cfg)
	if err := s.validateChannel(ctx, project, body); err != nil {
		return eventarcstore.Channel{}, Operation{}, err
	}
	now := clock.Now().UTC()
	c := eventarcstore.Channel{
		Location:        location,
		Name:            channelID,
		Labels:          labelsFromConfig(cfg),
		UID:             uuid.NewString(),
		ActivationToken: randomHex(32),
		CreateTime:      now,
		UpdateTime:      now,
	}
	if len(cfg) > 0 {
		c.Config = cfg
	}
	c.Etag = channelEtag(c)
	target := ChannelName(project, location, channelID)
	if validateOnly {
		if _, err := s.store.GetChannel(ctx, project, location, channelID); err == nil {
			return eventarcstore.Channel{}, Operation{}, mapErr(eventarcstore.ErrAlreadyExists)
		} else if !errors.Is(err, eventarcstore.ErrNoSuchChannel) {
			return eventarcstore.Channel{}, Operation{}, mapErr(err)
		}
		return c, newOperation(location, "create", target), nil
	}
	if err := s.store.CreateChannel(ctx, project, location, c); err != nil {
		return eventarcstore.Channel{}, Operation{}, mapErr(err)
	}
	return c, newOperation(location, "create", target), nil
}

// GetChannel returns one channel.
func (s *Service) GetChannel(ctx context.Context, project, location, channelID string) (eventarcstore.Channel, error) {
	if location == "" || channelID == "" {
		return eventarcstore.Channel{}, invalidArgument("missing location or channel id")
	}
	c, err := s.store.GetChannel(ctx, project, location, channelID)
	if err != nil {
		return eventarcstore.Channel{}, mapErr(err)
	}
	return c, nil
}

// ListChannels returns a cursor page of the channels in a location.
func (s *Service) ListChannels(ctx context.Context, project, location string, pageSize int, pageToken string) ([]eventarcstore.Channel, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	channels, err := s.store.ListChannels(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(channels, func(c eventarcstore.Channel) string { return c.Name }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// ListChannelsByProject returns every channel in a project across all
// locations, sorted by location then id. It backs the location-optional
// console list (the REST/gRPC ListChannels method is per-location).
func (s *Service) ListChannelsByProject(ctx context.Context, project string) ([]eventarcstore.Channel, error) {
	channels, err := s.store.ListChannelsAllLocations(ctx, project)
	if err != nil {
		return nil, mapErr(err)
	}
	return channels, nil
}

// UpdateChannel merges the caller's fields into the stored channel and returns
// it with the done update operation. reqEtag is the body-supplied etag
// precondition.
func (s *Service) UpdateChannel(ctx context.Context, project, location, channelID string, cfg json.RawMessage, mask, reqEtag string, validateOnly bool) (eventarcstore.Channel, Operation, error) {
	if location == "" || channelID == "" {
		return eventarcstore.Channel{}, Operation{}, invalidArgument("missing location or channel id")
	}
	body := decodeBody(cfg)
	paths := maskPaths(mask)

	// See UpdateTrigger: reference validation must run outside the locked
	// closure.
	stored, err := s.store.GetChannel(ctx, project, location, channelID)
	if err != nil {
		return eventarcstore.Channel{}, Operation{}, mapErr(err)
	}
	if err := checkEtag(reqEtag, stored.Etag); err != nil {
		return eventarcstore.Channel{}, Operation{}, err
	}
	next, merged, err := mergeChannelBody(stored, body, paths)
	if err != nil {
		return eventarcstore.Channel{}, Operation{}, mapErr(err)
	}
	if err := s.validateChannel(ctx, project, merged); err != nil {
		return eventarcstore.Channel{}, Operation{}, mapErr(err)
	}
	target := ChannelName(project, location, channelID)
	if validateOnly {
		return next, newOperation(location, "update", target), nil
	}

	updated, err := s.store.UpdateChannelAtomic(ctx, project, location, channelID, func(c eventarcstore.Channel) (eventarcstore.Channel, error) {
		// See UpdateTrigger: etag check and masked merge are both inside the
		// locked mutate closure.
		if err := checkEtag(reqEtag, c.Etag); err != nil {
			return eventarcstore.Channel{}, err
		}
		next, _, err := mergeChannelBody(c, body, paths)
		if err != nil {
			return eventarcstore.Channel{}, err
		}
		next.UpdateTime = clock.Now().UTC()
		next.Etag = channelEtag(next)
		return next, nil
	})
	if err != nil {
		return eventarcstore.Channel{}, Operation{}, mapErr(err)
	}
	return updated, newOperation(location, "update", target), nil
}

// mergeChannelBody applies a Channel PATCH body to the stored channel under the
// given updateMask paths. It performs no reference validation and never touches
// the store, so it is safe to call from inside the store's locked mutate
// closure.
func mergeChannelBody(c eventarcstore.Channel, body map[string]any, paths []string) (eventarcstore.Channel, map[string]any, error) {
	stored := map[string]any{}
	if len(c.Config) > 0 {
		_ = json.Unmarshal(c.Config, &stored)
	}
	merged, err := applyChannelMask(stored, body, paths)
	if err != nil {
		return eventarcstore.Channel{}, nil, err
	}
	if labels := bodyStringMap(merged, "labels"); labels != nil {
		c.Labels = labels
	}
	if data, err := json.Marshal(merged); err == nil {
		c.Config = data
	}
	return c, merged, nil
}

// DeleteChannel deletes a channel and returns the deleted channel with the done
// delete operation (the proto's delete response_type is Channel).
func (s *Service) DeleteChannel(ctx context.Context, project, location, channelID, reqEtag string, validateOnly bool) (eventarcstore.Channel, Operation, error) {
	if location == "" || channelID == "" {
		return eventarcstore.Channel{}, Operation{}, invalidArgument("missing location or channel id")
	}
	target := ChannelName(project, location, channelID)
	if validateOnly {
		stored, err := s.store.GetChannel(ctx, project, location, channelID)
		if err != nil {
			return eventarcstore.Channel{}, Operation{}, mapErr(err)
		}
		if err := checkEtag(reqEtag, stored.Etag); err != nil {
			return eventarcstore.Channel{}, Operation{}, err
		}
		return stored, newOperation(location, "delete", target), nil
	}
	var deleted eventarcstore.Channel
	if err := s.store.DeleteChannelAtomic(ctx, project, location, channelID, func(c eventarcstore.Channel) error {
		if err := checkEtag(reqEtag, c.Etag); err != nil {
			return err
		}
		deleted = c
		return nil
	}); err != nil {
		return eventarcstore.Channel{}, Operation{}, mapErr(err)
	}
	return deleted, newOperation(location, "delete", target), nil
}

// --- IAM (triggers / channels) ---

// requireTrigger resolves the IAM policy resource id for a trigger and
// requires the trigger to exist (NotFound otherwise), mirroring
// functions.requireFunction.
func (s *Service) requireTrigger(ctx context.Context, project, location, id string) error {
	if location == "" || id == "" {
		return invalidArgument("missing trigger name or location")
	}
	if _, err := s.store.GetTrigger(ctx, project, location, id); err != nil {
		return mapErr(err)
	}
	return nil
}

// requireChannel resolves the IAM policy resource id for a channel and
// requires the channel to exist (NotFound otherwise).
func (s *Service) requireChannel(ctx context.Context, project, location, id string) error {
	if location == "" || id == "" {
		return invalidArgument("missing channel name or location")
	}
	if _, err := s.store.GetChannel(ctx, project, location, id); err != nil {
		return mapErr(err)
	}
	return nil
}

func (s *Service) TriggerGetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error) {
	if err := s.requireTrigger(ctx, project, location, id); err != nil {
		return policy.Policy{}, err
	}
	return policy.Load(ctx, s.resources, project, rtTriggerPolicy, location+"/"+id), nil
}

func (s *Service) TriggerSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	if err := s.requireTrigger(ctx, project, location, id); err != nil {
		return policy.Policy{}, err
	}
	return policy.Set(ctx, s.resources, project, rtTriggerPolicy, location+"/"+id, body)
}

func (s *Service) TriggerTestIamPermissions(ctx context.Context, project, location, id string, perms []string) ([]string, error) {
	if err := s.requireTrigger(ctx, project, location, id); err != nil {
		return nil, err
	}
	return policy.TestPermissions(perms), nil
}

func (s *Service) ChannelGetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error) {
	if err := s.requireChannel(ctx, project, location, id); err != nil {
		return policy.Policy{}, err
	}
	return policy.Load(ctx, s.resources, project, rtChannelPolicy, location+"/"+id), nil
}

func (s *Service) ChannelSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	if err := s.requireChannel(ctx, project, location, id); err != nil {
		return policy.Policy{}, err
	}
	return policy.Set(ctx, s.resources, project, rtChannelPolicy, location+"/"+id, body)
}

func (s *Service) ChannelTestIamPermissions(ctx context.Context, project, location, id string, perms []string) ([]string, error) {
	if err := s.requireChannel(ctx, project, location, id); err != nil {
		return nil, err
	}
	return policy.TestPermissions(perms), nil
}

// --- event delivery ---

// TargetsForEvent returns the Cloud Functions functions that Eventarc triggers
// route an event to. Eventarc delivery is a metadata-driven match: a Pub/Sub
// event routes to every trigger whose transport.pubsub.topic names the event's
// topic (and whose eventFilters match); a Cloud Storage event routes to every
// trigger whose eventFilters select the object event. In both cases the
// trigger's destination.cloudFunction must name a function. Triggers with
// non-function destinations (Cloud Run, Workflows, GKE, HTTP) are ignored: the
// emulator only executes Cloud Functions. It implements eventing.TargetIndex so
// the functions delivery engine never imports this package.
func (s *Service) TargetsForEvent(ctx context.Context, ev eventing.Event) []eventing.Target {
	if ev.Source != eventing.SourcePubSub && ev.Source != eventing.SourceStorage {
		return nil
	}
	triggers, err := s.store.ListTriggersAllLocations(ctx, ev.Project)
	if err != nil {
		return nil
	}
	var out []eventing.Target
	for _, t := range triggers {
		body := decodeBody(t.Config)
		dest := bodyMap(body, "destination")
		cf, _ := dest["cloudFunction"].(string)
		if cf == "" {
			continue
		}
		if !eventarcEventMatches(body, ev) {
			continue
		}
		loc, id := locationOf(cf), lastSegment(cf)
		if loc == "" || id == "" {
			continue
		}
		target := eventing.Target{
			Project:    ev.Project,
			Location:   loc,
			FunctionID: id,
			Retry:      eventarcRetries(body),
		}
		// The emulator provisions a backing subscription only for a trigger
		// whose transport.pubsub.topic is set (SyncTriggerSubscription); do not
		// advertise a dead-letter surface that does not exist.
		if triggerTransportTopic(body) != "" {
			target.Subscription = eventing.EventarcSubscriptionID(t.Location, t.Name)
		}
		out = append(out, target)
	}
	return out
}

// eventarcSourceMatches reports whether a trigger's transport.pubsub.topic
// names the given topic id.
func eventarcSourceMatches(body map[string]any, topic string) bool {
	if topic == "" {
		return false
	}
	transport := bodyMap(body, "transport")
	if transport == nil {
		return false
	}
	pubsub := bodyMap(transport, "pubsub")
	if pubsub == nil {
		return false
	}
	src, _ := pubsub["topic"].(string)
	return eventing.ResourceID(src) == topic
}

// filtersMatch evaluates a trigger's eventFilters against an attribute map and
// an event type. An empty operator is an exact match; "match-path-pattern" is a
// prefix match on the value (GCP treats the rest as a path wildcard). The
// required "type" filter is compared through the shared event-type
// normalization so a trigger may name either the CloudEvent type or a Cloud
// Functions alias. topic/bucket values additionally match by last path segment
// (attributeValueMatches), so short and fully-qualified forms are equivalent.
func filtersMatch(filters []any, attrs map[string]string, eventType string) bool {
	for _, f := range filters {
		fm, ok := f.(map[string]any)
		if !ok {
			continue
		}
		attr, _ := fm["attribute"].(string)
		value, _ := fm["value"].(string)
		operator, _ := fm["operator"].(string)
		if attr == "" {
			continue
		}
		if attr == "type" {
			if !eventing.TypeMatches(value, eventType) {
				return false
			}
			continue
		}
		got, _ := attrs[attr]
		if operator == "match-path-pattern" {
			// GCP's match-path-pattern treats the trailing segment(s) as a
			// wildcard; the emulator compares the literal prefix.
			if !strings.HasPrefix(got, strings.TrimSuffix(value, "*")) {
				return false
			}
			continue
		}
		if !attributeValueMatches(attr, value, got) {
			return false
		}
	}
	return true
}

// attributeValueMatches compares a filter value to an actual event attribute.
// topic and bucket values fall back to a last-segment comparison so a short
// value ("my-topic") and a fully-qualified one ("projects/p/topics/my-topic")
// both match, mirroring floci's matchAttributeValue.
func attributeValueMatches(name, filterVal, actualVal string) bool {
	if filterVal == actualVal {
		return true
	}
	if name == "topic" || name == "bucket" {
		return eventing.ResourceID(filterVal) == eventing.ResourceID(actualVal)
	}
	return false
}

// eventarcRetries reports whether a trigger's retryPolicy retries a failed
// delivery.
func eventarcRetries(body map[string]any) bool {
	rp, _ := body["retryPolicy"].(map[string]any)
	if rp == nil {
		return false
	}
	switch n := rp["maxAttempts"].(type) {
	case float64:
		return n > 0
	case int:
		return n > 0
	}
	return false
}
