package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	pubsubcore "jaiscloud/internal/gcp/service/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// TopicUpdate applies an updateMask to a topic (pubsub.projects.topics.patch).
// The REST request is an UpdateTopicRequest: the fields to update live under
// "topic", the mask is a body field ("updateMask"; a query fallback is
// tolerated). It mirrors the gRPC UpdateTopic handler through the shared
// pubsubcore.ApplyTopicUpdate helper, so the two transports cannot drift:
// every mutable topic field is maskable, nested AIP-134 paths merge into the
// stored object, a field absent/empty under its mask is cleared, and an
// unknown mask path fails loud with InvalidArgument rather than being silently
// ignored. An empty mask applies labels (PTF8 tolerance).
func (p *Provider) TopicUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	t := strings.TrimPrefix(name, "topics/")
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtTopic, t)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "topic not found", 404)
		}
		return nil, err
	}
	var meta map[string]any
	if json.Unmarshal(e.Data, &meta) != nil || meta == nil {
		meta = map[string]any{}
	}
	body, _ := nr.Params["body"].(map[string]any)
	// A real client sends UpdateTopicRequest{topic, updateMask}; tolerate a bare
	// Topic body for convenience.
	in := body
	if nested, ok := body["topic"].(map[string]any); ok {
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
	// Validate the complete fields the body carries up front, so an invalid
	// root value fails before anything is persisted. A nested mask may carry a
	// partial object (only the masked leaf); ApplyTopicUpdate validates the
	// field merged with its stored value instead.
	if _, nerr := pubsubcore.NormalizeTopicInput(in); nerr != nil && !pubsubcore.TopicMaskHasNestedPath(paths) {
		return nil, nerr
	}
	if err := pubsubcore.ApplyTopicUpdate(meta, in, paths); err != nil {
		return nil, err
	}
	data, _ := json.Marshal(meta)
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtTopic, ID: t, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(meta), nil
}
