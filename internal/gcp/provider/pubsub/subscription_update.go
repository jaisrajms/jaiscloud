package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	pubsubcore "jaiscloud/internal/gcp/service/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Mutable-field bounds from the Pub/Sub Subscription reference: message
// retention is 10 minutes–31 days, and an expiration policy TTL is 1 day–365
// days (a missing TTL means "never expire").
const (
	minMessageRetention = 600 * time.Second      // 10 minutes
	maxMessageRetention = 2678400 * time.Second  // 31 days
	minExpirationTTL    = 86400 * time.Second    // 24 hours
	maxExpirationTTL    = 31536000 * time.Second // 365 days
	defaultRetention    = "604800s"              // 7 days
)

// SubscriptionUpdate applies an updateMask to a subscription
// (pubsub.projects.subscriptions.patch). The REST request is an
// UpdateSubscriptionRequest: the fields to update live under "subscription", the
// mask is a body field ("updateMask"; a query fallback is tolerated). It mirrors
// the gRPC UpdateSubscription handler so the two transports cannot drift: filter
// is immutable, and labels, ackDeadlineSeconds, enableExactlyOnceDelivery,
// enableMessageOrdering, deadLetterPolicy, retryPolicy, messageRetentionDuration,
// retainAckedMessages, expirationPolicy, pushConfig, bigqueryConfig,
// cloudStorageConfig and messageTransforms are supported. state is OUTPUT_ONLY
// and accepted as a no-op. An unknown mask path fails loud with InvalidArgument
// rather than being silently ignored.
func (p *Provider) SubscriptionUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	s := strings.TrimPrefix(name, "subscriptions/")
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtSubscription, s)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "subscription not found", 404)
		}
		return nil, err
	}
	var meta map[string]any
	if json.Unmarshal(e.Data, &meta) != nil || meta == nil {
		meta = map[string]any{}
	}
	body, _ := nr.Params["body"].(map[string]any)
	// A real client sends UpdateSubscriptionRequest{subscription, updateMask};
	// tolerate a bare Subscription body for convenience.
	in := body
	if nested, ok := body["subscription"].(map[string]any); ok {
		in = nested
	}
	mask := ""
	if m, ok := body["updateMask"].(string); ok {
		mask = m
	}
	if mask == "" {
		mask = nrStringParam(nr, "updateMask")
	}
	paths := splitMaskPaths(mask)
	if len(paths) == 0 {
		// Mirrors the gRPC handler: an empty mask applies labels.
		paths = []string{"labels"}
	}
	for _, path := range paths {
		// retryPolicy is handled before the root switch because its nested
		// field-mask leaves need per-leaf merge semantics (AIP-161).
		if _, _, ok := retryPolicyLeaf(path); ok {
			if err := applyRetryPolicyUpdate(meta, in, path); err != nil {
				return nil, err
			}
			continue
		}
		switch normalizeMaskPath(maskRoot(path)) {
		case "filter":
			return nil, model.NewProviderError("InvalidArgument",
				"subscription filter is immutable and cannot be updated", 400)
		case "labels":
			if labels := stringMap(in, "labels"); len(labels) == 0 {
				delete(meta, "labels")
			} else {
				meta["labels"] = labels
			}
		case "ackdeadlineseconds":
			v := 0
			if n, ok := in["ackDeadlineSeconds"].(float64); ok {
				v = int(n)
			}
			if v != 0 && (v < 10 || v > 600) {
				return nil, model.NewProviderError("InvalidArgument",
					fmt.Sprintf("ackDeadlineSeconds must be between 10 and 600 (got %d)", v), 400)
			}
			if v == 0 {
				v = 10
			}
			meta["ackDeadlineSeconds"] = v
		case "enableexactlyoncedelivery":
			// The push/exactly-once invariant is asserted once after the whole
			// mask is applied (below), so a patch that touches both fields is
			// rejected regardless of path order.
			if v, _ := in["enableExactlyOnceDelivery"].(bool); v {
				meta["enableExactlyOnceDelivery"] = true
			} else {
				delete(meta, "enableExactlyOnceDelivery")
			}
		case "enablemessageordering":
			if v, _ := in["enableMessageOrdering"].(bool); v {
				meta["enableMessageOrdering"] = true
			} else {
				delete(meta, "enableMessageOrdering")
			}
		case "messageretentionduration":
			// Protobuf-JSON duration ("600s"), never Go's "10m0s": the official
			// REST client unmarshals this string as google.protobuf.Duration.
			// An absent/empty value under the mask resets to the documented
			// 7-day default rather than deleting the key.
			raw, present := in["messageRetentionDuration"]
			if !present || raw == nil {
				meta["messageRetentionDuration"] = defaultRetention
				break
			}
			s, isString := raw.(string)
			if !isString {
				return nil, model.NewProviderError("InvalidArgument",
					"messageRetentionDuration must be a duration string", 400)
			}
			if s == "" {
				meta["messageRetentionDuration"] = defaultRetention
				break
			}
			d, err := parseProtoDuration(s)
			if err != nil {
				return nil, model.NewProviderError("InvalidArgument",
					"invalid messageRetentionDuration: "+s, 400)
			}
			if d < minMessageRetention || d > maxMessageRetention {
				return nil, model.NewProviderError("InvalidArgument",
					fmt.Sprintf("messageRetentionDuration must be between 600s and 2678400s (got %s)", s), 400)
			}
			meta["messageRetentionDuration"] = formatProtoDuration(d)
		case "retainackedmessages":
			// Real Pub/Sub omits `false` booleans from the JSON response, so a
			// false value clears the key (matching the create path).
			if v, _ := in["retainAckedMessages"].(bool); v {
				meta["retainAckedMessages"] = true
			} else {
				delete(meta, "retainAckedMessages")
			}
		case "expirationpolicy":
			// null / {} / a missing ttl means "never expire": store an empty
			// object so it stays distinguishable from an unset policy.
			raw, present := in["expirationPolicy"]
			if !present || raw == nil {
				meta["expirationPolicy"] = map[string]any{}
				break
			}
			ep, isObject := raw.(map[string]any)
			if !isObject {
				return nil, model.NewProviderError("InvalidArgument",
					"expirationPolicy must be an object", 400)
			}
			ttl, _ := ep["ttl"].(string)
			if ttl == "" {
				meta["expirationPolicy"] = map[string]any{}
				break
			}
			d, err := parseProtoDuration(ttl)
			if err != nil {
				return nil, model.NewProviderError("InvalidArgument",
					"invalid expirationPolicy.ttl: "+ttl, 400)
			}
			if d < minExpirationTTL || d > maxExpirationTTL {
				return nil, model.NewProviderError("InvalidArgument",
					fmt.Sprintf("expirationPolicy.ttl must be between 86400s and 31536000s (got %s)", ttl), 400)
			}
			stored := make(map[string]any, len(ep))
			for k, v := range ep {
				stored[k] = v
			}
			stored["ttl"] = formatProtoDuration(d)
			meta["expirationPolicy"] = stored
		case "pushconfig":
			// absent / null / {} / an empty pushEndpoint clears push delivery,
			// which is how the provider switches a subscription back to pull.
			raw, present := in["pushConfig"]
			if !present || raw == nil {
				delete(meta, "pushConfig")
				break
			}
			pc, isObject := raw.(map[string]any)
			if !isObject {
				return nil, model.NewProviderError("InvalidArgument",
					"pushConfig must be an object", 400)
			}
			if ep, _ := pc["pushEndpoint"].(string); ep == "" {
				delete(meta, "pushConfig")
				break
			}
			meta["pushConfig"] = pc
		case "bigqueryconfig":
			// Store-and-echo only: delivery to BigQuery is not executed.
			// An absent or JSON-null object clears the target (AIP-134).
			if err := applyStorageConfigUpdate(meta, in, "bigqueryConfig"); err != nil {
				return nil, err
			}
		case "cloudstorageconfig":
			// Store-and-echo only: delivery to GCS is not executed.
			if err := applyStorageConfigUpdate(meta, in, "cloudStorageConfig"); err != nil {
				return nil, err
			}
		case "messagetransforms":
			raw, present := in["messageTransforms"]
			if !present || raw == nil {
				delete(meta, "messageTransforms")
				break
			}
			list, isList := raw.([]any)
			if !isList {
				return nil, model.NewProviderError("InvalidArgument",
					"messageTransforms must be a list", 400)
			}
			if len(list) == 0 {
				delete(meta, "messageTransforms")
				break
			}
			meta["messageTransforms"] = list
		case "state":
			// OUTPUT_ONLY in Discovery: accept the mask path as a no-op and
			// never persist a client-supplied value (detach has its own flag).
		case "topic":
			// A subscription cannot move to another topic through the public API. The
			// emulator re-points its own Eventarc transport subscription when a trigger's
			// topic changes (seams.PubSub.EnsureEventarcSubscription), keeping every other
			// setting and the queued messages, so only an internal caller may do it.
			if nr.Origin != model.OriginInternal {
				return nil, model.NewProviderError("InvalidArgument", "unsupported update_mask path: "+path, 400)
			}
			topic, _ := in["topic"].(string)
			if !p.topicExists(ctx, nr.AccountID, lastSegment(topic)) {
				return nil, model.NewProviderError("NotFound", "topic not found", 404)
			}
			meta["topic"] = nr.ResourceID("pubsub-topic", lastSegment(topic))
		case "deadletterpolicy":
			dp, _ := in["deadLetterPolicy"].(map[string]any)
			if dp == nil {
				delete(meta, "deadLetterPolicy")
				break
			}
			normalized, err := p.normalizeDeadLetterPolicy(ctx, nr.AccountID, dp)
			if err != nil {
				return nil, err
			}
			meta["deadLetterPolicy"] = normalized
		default:
			return nil, model.NewProviderError("InvalidArgument", "unsupported update_mask path: "+path, 400)
		}
	}
	// Assert the push/exactly-once invariant once, against the fully-merged
	// state, so a patch naming both fields is rejected in either order.
	if err := validateExactlyOncePush(
		boolField(meta, "enableExactlyOnceDelivery"),
		pushEndpointOf(meta),
	); err != nil {
		return nil, err
	}
	data, _ := json.Marshal(meta)
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: s, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(meta), nil
}

// applyStorageConfigUpdate stores in[key] as an object when it is present and
// non-null, and deletes meta[key] otherwise (absent or JSON null under the mask
// clears the target, per AIP-134). The target's `state` is OUTPUT_ONLY in the
// Subscription proto, so a caller-supplied value is dropped rather than stored.
func applyStorageConfigUpdate(meta, in map[string]any, key string) error {
	raw, present := in[key]
	if !present || raw == nil {
		delete(meta, key)
		return nil
	}
	obj, isObject := raw.(map[string]any)
	if !isObject {
		return model.NewProviderError("InvalidArgument", key+" must be an object", 400)
	}
	meta[key] = pubsubcore.WithoutStorageConfigState(obj)
	return nil
}

// validateExactlyOncePush rejects exactly-once delivery on a subscription that
// also carries a push endpoint: real Pub/Sub treats exactly-once as pull-only,
// because a push subscriber never reports whether delivery was processed. The
// create and update paths both call it, so the rule cannot drift.
func validateExactlyOncePush(exactlyOnce bool, endpoint string) error {
	if exactlyOnce && endpoint != "" {
		return model.NewProviderError("InvalidArgument",
			"exactly-once delivery is not supported for push subscriptions", 400)
	}
	return nil
}

// boolField reports whether meta[key] is the boolean true (a persisted map has
// no type-preserving zero value).
func boolField(meta map[string]any, key string) bool {
	v, _ := meta[key].(bool)
	return v
}

// pushEndpointOf returns the stored push endpoint, or "" for a pull subscription.
func pushEndpointOf(meta map[string]any) string {
	pc, _ := meta["pushConfig"].(map[string]any)
	ep, _ := pc["pushEndpoint"].(string)
	return ep
}

// nrStringParam returns a query parameter as a string, tolerating a repeated
// parameter delivered as a slice.
func nrStringParam(nr *model.NormalizedRequest, key string) string {
	switch v := nr.Params[key].(type) {
	case string:
		return v
	case []string:
		return strings.Join(v, ",")
	case []any:
		parts := make([]string, 0, len(v))
		for _, x := range v {
			if s, ok := x.(string); ok {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, ",")
	}
	return ""
}

// splitMaskPaths splits a comma-separated updateMask into its non-empty paths.
func splitMaskPaths(mask string) []string {
	if mask == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(mask, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// maskRoot returns the root segment of a field-mask path
// ("dead_letter_policy.max_delivery_attempts" → "dead_letter_policy"), so a
// nested path applies the whole carried object (the emulator stores and merges
// the object as a unit).
func maskRoot(path string) string {
	if i := strings.IndexByte(path, '.'); i >= 0 {
		return path[:i]
	}
	return path
}

// normalizeMaskPath folds a field-mask path segment to a case/underscore-
// insensitive form so the proto snake_case (dead_letter_policy) and the JSON
// camelCase (deadLetterPolicy) resolve to the same field.
func normalizeMaskPath(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

// stringMap extracts body[key] as a string map, or nil when absent/malformed.
func stringMap(body map[string]any, key string) map[string]string {
	v, _ := body[key].(map[string]any)
	if v == nil {
		return nil
	}
	out := make(map[string]string, len(v))
	for k, x := range v {
		if s, ok := x.(string); ok {
			out[k] = s
		}
	}
	return out
}
