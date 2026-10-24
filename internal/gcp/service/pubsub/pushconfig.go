package pubsub

import (
	"context"
	"encoding/json"
	"errors"

	"jaiscloud/internal/store"
)

// OIDCToken is the transport-neutral OIDC token of a push configuration.
type OIDCToken struct {
	Audience            string
	ServiceAccountEmail string
}

// PushConfig is the transport-neutral push configuration of a subscription. An
// empty Endpoint clears the subscription's push configuration.
type PushConfig struct {
	Endpoint   string
	Attributes map[string]string
	OIDCToken  *OIDCToken
}

// ModifyPushConfig replaces (or clears) a subscription's push configuration.
//
// An empty Endpoint removes the configuration. Exactly-once delivery cannot be
// enabled on a push subscription, so setting a push endpoint on an EOD-enabled
// subscription is rejected rather than silently accepted.
func (s *Service) ModifyPushConfig(ctx context.Context, project, sub string, pc *PushConfig) error {
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, sub)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return notFound("subscription not found")
		}
		return err
	}
	var meta map[string]any
	json.Unmarshal(e.Data, &meta)

	if pc == nil || pc.Endpoint == "" {
		delete(meta, "pushConfig")
	} else {
		// Exactly-once delivery cannot be enabled on a push subscription.
		if eod, _ := meta["enableExactlyOnceDelivery"].(bool); eod {
			return invalidArgument("exactly-once delivery is not supported for push subscriptions")
		}
		pm := map[string]any{"pushEndpoint": pc.Endpoint}
		if len(pc.Attributes) > 0 {
			pm["attributes"] = pc.Attributes
		}
		if pc.OIDCToken != nil {
			pm["oidcToken"] = map[string]any{
				"audience":            pc.OIDCToken.Audience,
				"serviceAccountEmail": pc.OIDCToken.ServiceAccountEmail,
			}
		}
		meta["pushConfig"] = pm
	}
	data, _ := json.Marshal(meta)
	return s.resources.Update(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtSubscription, ID: sub, Data: data})
}
