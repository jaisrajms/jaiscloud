package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// SubscriptionUpdate applies an updateMask to a subscription
// (pubsub.projects.subscriptions.patch). The REST request is an
// UpdateSubscriptionRequest: the fields to update live under "subscription", the
// mask is a body field ("updateMask"; a query fallback is tolerated). It mirrors
// the gRPC UpdateSubscription handler so the two transports cannot drift: filter
// is immutable, and labels, ackDeadlineSeconds, enableExactlyOnceDelivery,
// enableMessageOrdering, deadLetterPolicy, and retryPolicy are supported. An
// unknown mask path fails loud with InvalidArgument rather than being silently
// ignored.
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
			if v, _ := in["enableExactlyOnceDelivery"].(bool); v {
				if pc, ok := meta["pushConfig"].(map[string]any); ok {
					if ep, _ := pc["pushEndpoint"].(string); ep != "" {
						return nil, model.NewProviderError("InvalidArgument",
							"exactly-once delivery is not supported for push subscriptions", 400)
					}
				}
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
	data, _ := json.Marshal(meta)
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: s, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(meta), nil
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
