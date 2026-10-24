package eventarc

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/gcp/eventing"
	"jaiscloud/internal/gcp/resource"

	"github.com/google/uuid"
)

// httpDoer is the subset of *http.Client the dispatcher needs. Tests inject a
// fake; a nil doer disables outbound delivery.
type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// eventarcConnectTimeout bounds the connect phase of an Eventarc delivery, and
// eventarcDeliveryTimeout bounds the whole request (a sink that accepts and then
// stalls would otherwise leak a goroutine). Delivery is fire-and-forget (the
// the emulator contract): the emulator never retries or dead-letters a non-function
// delivery, so a slow or unreachable sink is logged, never surfaced to the
// producer.
const (
	eventarcConnectTimeout  = 10 * time.Second
	eventarcDeliveryTimeout = 30 * time.Second
)

// eventarcHTTPClient is the default outbound client.
func eventarcHTTPClient() httpDoer {
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: eventarcConnectTimeout}).DialContext,
			ResponseHeaderTimeout: eventarcConnectTimeout,
		},
		Timeout: eventarcDeliveryTimeout,
	}
}

// CloudRunInvoker delivers one CloudEvents request to a Cloud Run service's
// latest ready revision. It is implemented outside this package (over the run
// core's runtime seam) so the Eventarc core never imports Cloud Run: the
// invoker resolves the stored service and forwards the request through the
// runtime manager, so delivery needs no DNS and works in k8s executor mode.
// A nil invoker (the default) means a cloudRun destination is logged and
// dropped — the emulator logs and drops unreachable destinations.
type CloudRunInvoker interface {
	Invoke(ctx context.Context, project, region, service, path string, headers map[string]string, body []byte) (int, error)
}

// DispatchEvent is the eventing.Dispatcher entry point. It delivers a produced
// Pub/Sub or Cloud Storage event to every matching Eventarc trigger whose
// destination is not a Cloud Function — the functions delivery engine owns
// those, so skipping them here is what keeps a single event from being
// delivered twice.
//
// Delivery is a binary-mode CloudEvents POST, fire-and-forget on
// its own goroutine, failures logged and never retried. An httpEndpoint
// destination is POSTed directly; a cloudRun destination is forwarded through
// the CloudRunInvoker seam, honoring its path. gke and workflow destinations
// are logged and dropped. Other event sources are ignored.
func (s *Service) DispatchEvent(ctx context.Context, ev eventing.Event) {
	if s == nil {
		return
	}
	if ev.Source != eventing.SourcePubSub && ev.Source != eventing.SourceStorage {
		return
	}
	triggers, err := s.store.ListTriggersAllLocations(ctx, ev.Project)
	if err != nil {
		slog.Warn("eventarc: list triggers for event", "project", ev.Project, "err", err)
		return
	}
	for _, t := range triggers {
		body := decodeBody(t.Config)
		dest := bodyMap(body, "destination")
		if dest == nil {
			continue
		}
		if cf, _ := dest["cloudFunction"].(string); cf != "" {
			continue // the Cloud Functions delivery engine owns this destination
		}
		uri := httpEndpointURI(dest)
		if uri != "" && s.httpClient == nil {
			uri = "" // no outbound HTTP client configured
		}
		runTarget, isCloudRun := cloudRunDestination(dest)
		if !eventarcEventMatches(body, ev) {
			continue
		}
		if uri == "" && !isCloudRun {
			// Only a matched event reaches this: the emulator logs-and-drops the
			// destinations it cannot reach (gke and workflow), and a malformed
			// cloudRun is not deliverable either.
			slog.Warn("eventarc: destination not deliverable; dropping event",
				"trigger", t.Name, "destination", dest)
			continue
		}
		payload, headers := buildCloudEvent(ev.Project, t.Location, t.Name, ev)
		switch {
		case uri != "":
			s.deliver(uri, headers, payload)
		case s.cloudRun != nil:
			s.deliverCloudRun(ev.Project, runTarget, headers, payload)
		default:
			slog.Warn("eventarc: cloudRun destination but no run service available; dropping event",
				"trigger", t.Name, "service", runTarget.service, "region", runTarget.region)
		}
	}
}

// pubsubEventAttributes synthesizes the attribute map matched for a Pub/Sub
// event on: the message's own attributes plus the reserved type and topic
// attributes (topic carries the fully-qualified resource so a filter may use
// either the short id or the full name — see attributeValueMatches).
func pubsubEventAttributes(ev eventing.Event) map[string]string {
	attrs := make(map[string]string, len(ev.Attributes)+2)
	for k, v := range ev.Attributes {
		attrs[k] = v
	}
	attrs["type"] = eventing.TypePubSubPublishCloudEvent
	attrs["topic"] = ev.Resource
	return attrs
}

// eventarcEventMatches reports whether a trigger's stored config selects a
// produced event. A Pub/Sub trigger is selected by a matching
// transport.pubsub.topic AND its eventFilters (the transport is the trigger's
// source); a Cloud Storage trigger has no Pub/Sub transport, so only its
// eventFilters can select it — and, mirroring the GCS event handler, a trigger
// with no eventFilters never matches. Shared by the Cloud Functions target
// index (TargetsForEvent) and this dispatcher so the two engines cannot drift.
func eventarcEventMatches(body map[string]any, ev eventing.Event) bool {
	filters, _ := body["eventFilters"].([]any)
	if ev.Source == eventing.SourceStorage {
		if len(filters) == 0 {
			return false
		}
		return filtersMatch(filters, storageEventAttributes(ev), ev.EventType)
	}
	if !eventarcSourceMatches(body, eventing.ResourceID(ev.Resource)) {
		return false
	}
	return filtersMatch(filters, pubsubEventAttributes(ev), ev.EventType)
}

// storageEventAttributes synthesizes the attribute map a Cloud Storage event is
// matched on, mirroring the GCS event handler: the reserved "bucket" and — when the
// event carries one — "object". The producer's own attributes (bucketId/
// objectId/...) are carried through so a trigger may filter on them too. The
// "type" filter is handled separately by filtersMatch against the event type, so
// it is not added here. bucket resolves from the event attributes or, failing
// that, the event resource ("projects/_/buckets/{bucket}").
func storageEventAttributes(ev eventing.Event) map[string]string {
	attrs := make(map[string]string, len(ev.Attributes)+2)
	for k, v := range ev.Attributes {
		attrs[k] = v
	}
	bucket := ev.Attributes["bucketId"]
	if bucket == "" {
		bucket = eventing.ResourceID(ev.Resource)
	}
	if bucket != "" {
		attrs["bucket"] = bucket
	}
	if object := ev.Attributes["objectId"]; object != "" {
		attrs["object"] = object
	}
	return attrs
}

// buildCloudEvent builds the binary-mode CloudEvents request for a produced
// event: the Pub/Sub push-delivery JSON for a Pub/Sub source, or the
// StorageObjectData JSON for a Cloud Storage source (matching the
// google-cloudevents schema and real Eventarc's storage delivery).
func buildCloudEvent(project, location, triggerID string, ev eventing.Event) ([]byte, map[string]string) {
	if ev.Source == eventing.SourceStorage {
		return buildStorageCloudEvent(ev)
	}
	return buildPubSubCloudEvent(project, location, triggerID, ev)
}

// buildStorageCloudEvent builds the CloudEvents POST real Eventarc sends for a
// Cloud Storage object event: source
// //storage.googleapis.com/projects/_/buckets/{bucket}, the CloudEvent type
// (google.cloud.storage.object.v1.finalized|deleted) and the object metadata
// (StorageObjectData) as the JSON body. ev.Data already carries the producer's
// object-metadata JSON. ce-id is minted fresh per event (real GCP and the emulator both
// use a unique id): the producer's EventID is derived from bucket/object/
// generation, which would collide between a finalize and a delete of the same
// generation. subject is the object's name ("objects/{object}").
func buildStorageCloudEvent(ev eventing.Event) ([]byte, map[string]string) {
	body := ev.Data
	if len(body) == 0 {
		body = []byte("{}")
	}
	headers := map[string]string{
		"Content-Type":   "application/json",
		"ce-id":          uuid.NewString(),
		"ce-source":      eventing.StorageSource(ev.Resource),
		"ce-specversion": "1.0",
		"ce-type":        eventing.StorageCloudEventType(ev.EventType),
		"ce-time":        ev.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	if subject := eventing.StorageObjectSubject(ev.Attributes); subject != "" {
		headers["ce-subject"] = subject
	}
	return body, headers
}

// httpEndpointURI returns the destination's httpEndpoint.uri, or "" when the
// destination has none (a cloudRun/gke/workflow destination).
func httpEndpointURI(dest map[string]any) string {
	ep, _ := dest["httpEndpoint"].(map[string]any)
	if ep == nil {
		return ""
	}
	uri, _ := ep["uri"].(string)
	return strings.TrimSpace(uri)
}

// cloudRunTarget is a resolved destination.cloudRun invocation target.
type cloudRunTarget struct {
	region  string
	service string
	path    string
}

// cloudRunDestination resolves destination.cloudRun into its invocation target,
// The delivery flow:
//
//   - service may be the short id (with region set — the documented example is
//     {"service":"hello-run","region":"us-central1"}) or the full resource name
//     projects/{p}/locations/{l}/services/{s}, in which case the id is the last
//     segment and the location is the fallback region.
//   - path defaults to "/"; a non-empty relative path is made absolute (the emulator
//     prefixes "/" when the configured path lacks one).
//
// It returns false when there is no cloudRun destination or it names no service
// or region — there is nothing to invoke.
func cloudRunDestination(dest map[string]any) (cloudRunTarget, bool) {
	m, _ := dest["cloudRun"].(map[string]any)
	if m == nil {
		return cloudRunTarget{}, false
	}
	service := strings.TrimSpace(bodyString(m, "service"))
	region := strings.TrimSpace(bodyString(m, "region"))
	path := bodyString(m, "path")
	if service == "" {
		return cloudRunTarget{}, false
	}
	if strings.Contains(service, "/") {
		// A full resource name: derive the region from its location segment when
		// the explicit region field is absent, then keep only the service id.
		if region == "" {
			region = locationOf(service)
		}
		service = lastSegment(service)
	}
	if service == "" || region == "" {
		return cloudRunTarget{}, false
	}
	if path == "" {
		path = "/"
	} else if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return cloudRunTarget{region: region, service: service, path: path}, true
}

// deliverCloudRun invokes a Cloud Run service on its own goroutine, mirroring
// deliver's fire-and-forget contract. The invoker maps a missing service to 404
// and an unreachable runtime to 502/503/504; a failure is logged and never
// retried or dead-lettered.
func (s *Service) deliverCloudRun(project string, target cloudRunTarget, headers map[string]string, body []byte) {
	invoker := s.cloudRun
	if invoker == nil {
		return
	}
	s.deliveryWG.Add(1)
	go func() {
		defer s.deliveryWG.Done()
		ctx, cancel := context.WithTimeout(context.Background(), eventarcDeliveryTimeout)
		defer cancel()
		status, err := invoker.Invoke(ctx, project, target.region, target.service, target.path, headers, body)
		if err != nil {
			slog.Warn("eventarc: deliver event to cloudRun",
				"service", target.service, "region", target.region, "status", status, "err", err)
			return
		}
		if status >= 300 {
			slog.Warn("eventarc: cloudRun delivery non-2xx",
				"service", target.service, "region", target.region, "status", status)
		}
	}()
}

// buildPubSubCloudEvent builds the binary-mode CloudEvents request the emulator sends
// for a Pub/Sub message: the ce-* headers and the Pub/Sub push-delivery JSON
// body ({message:{data,attributes,messageId,publishTime},subscription}). The
// subscription is the emulator's own provisioned id
// (eventarc-{location}-{triggerId}), which is what subscriptions.list returns.
func buildPubSubCloudEvent(project, location, triggerID string, ev eventing.Event) ([]byte, map[string]string) {
	sub := resource.ResourceID(project)("pubsub-subscription", eventing.EventarcSubscriptionID(location, triggerID))
	body := eventing.PubSubMessagePublished(ev.EventID, ev.OccurredAt, ev.Data, ev.Attributes, sub)
	headers := map[string]string{
		"Content-Type":   "application/json",
		"ce-id":          ev.EventID,
		"ce-source":      eventing.PubSubSource(ev.Resource),
		"ce-specversion": "1.0",
		"ce-type":        eventing.TypePubSubPublishCloudEvent,
		"ce-time":        ev.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	return body, headers
}

// deliver POSTs one event on its own goroutine. The waitgroup lets tests await
// in-flight deliveries; production never waits on them.
func (s *Service) deliver(uri string, headers map[string]string, body []byte) {
	s.deliveryWG.Add(1)
	go func() {
		defer s.deliveryWG.Done()
		req, err := http.NewRequest(http.MethodPost, uri, bytes.NewReader(body))
		if err != nil {
			slog.Warn("eventarc: build delivery request", "uri", uri, "err", err)
			return
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			slog.Warn("eventarc: deliver event", "uri", uri, "err", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			slog.Warn("eventarc: delivery non-2xx", "uri", uri, "status", resp.StatusCode)
		}
	}()
}

// waitDeliveries blocks until in-flight deliveries finish. It exists for tests;
// a producer never waits on fire-and-forget delivery.
func (s *Service) waitDeliveries() { s.deliveryWG.Wait() }
