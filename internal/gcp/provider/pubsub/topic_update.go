package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// TopicUpdate applies an updateMask to a topic (pubsub.projects.topics.patch).
// The REST request is an UpdateTopicRequest: the fields to update live under
// "topic", the mask is a body field ("updateMask"; a query fallback is
// tolerated). It mirrors the gRPC UpdateTopic handler so the two transports
// cannot drift: labels and messageRetentionDuration are supported, an empty mask
// applies labels, and an unknown mask path fails loud with InvalidArgument
// rather than being silently ignored.
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
	for _, path := range paths {
		switch normalizeMaskPath(path) {
		case "labels":
			if labels := stringMap(in, "labels"); len(labels) == 0 {
				delete(meta, "labels")
			} else {
				meta["labels"] = labels
			}
		case "messageretentionduration":
			ret, _ := in["messageRetentionDuration"].(string)
			if ret == "" {
				delete(meta, "messageRetentionDuration")
				break
			}
			d, perr := time.ParseDuration(ret)
			if perr != nil {
				return nil, model.NewProviderError("InvalidArgument", "invalid messageRetentionDuration: "+ret, 400)
			}
			// Store the protobuf-JSON Duration form ("600s"), not Go's
			// "10m0s": the official REST client unmarshals this string as a
			// google.protobuf.Duration, which only accepts the seconds-suffixed
			// form, so the Go form would break every later REST read.
			meta["messageRetentionDuration"] = fmt.Sprintf("%ds", int64(d.Seconds()))
		default:
			return nil, model.NewProviderError("InvalidArgument", "unsupported update_mask path: "+path, 400)
		}
	}
	data, _ := json.Marshal(meta)
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtTopic, ID: t, Data: data}); err != nil {
		return nil, err
	}
	return provider.OK(meta), nil
}
