package pubsub

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// TestTopicUpdateMasked covers pubsub.projects.topics.patch: the
// UpdateTopicRequest{ topic, updateMask } body applies only the masked fields,
// mirroring the gRPC UpdateTopic handler.
func TestTopicUpdateMasked(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/u"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	resp, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/u",
		"body": map[string]any{
			"topic": map[string]any{
				"labels":                   map[string]any{"env": "test"},
				"messageRetentionDuration": "600s",
			},
			"updateMask": "labels,messageRetentionDuration",
		},
	}))
	if err != nil {
		t.Fatalf("TopicUpdate: %v", err)
	}
	if got, _ := resp.Data["labels"].(map[string]string); got["env"] != "test" {
		t.Errorf("labels = %v, want env=test", resp.Data["labels"])
	}
	// The protobuf-JSON Duration form, not Go's "10m0s" (F1: the REST client
	// only unmarshals the seconds-suffixed form).
	if got, _ := resp.Data["messageRetentionDuration"].(string); got != "600s" {
		t.Errorf("retention = %q, want 600s", got)
	}

	// The update must be persisted, not just echoed.
	got, err := p.TopicGet(ctx, newNR(map[string]any{"name": "topics/u"}))
	if err != nil {
		t.Fatalf("TopicGet: %v", err)
	}
	if v := labelValue(got.Data["labels"], "env"); v != "test" {
		t.Errorf("persisted labels = %v, want env=test", got.Data["labels"])
	}
}

// labelValue reads a label from a stored label map, which JSON round-trips to
// map[string]any (the in-memory response is map[string]string).
func labelValue(v any, key string) string {
	switch m := v.(type) {
	case map[string]string:
		return m[key]
	case map[string]any:
		s, _ := m[key].(string)
		return s
	}
	return ""
}

// TestTopicUpdateEmptyMaskAppliesLabels pins the gRPC-mirrored default: an empty
// mask updates labels only, and an empty label set clears them.
func TestTopicUpdateEmptyMaskAppliesLabels(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/e", "body": map[string]any{"labels": map[string]any{"old": "1"}}})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	resp, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/e",
		"body": map[string]any{"topic": map[string]any{"labels": map[string]any{"new": "2"}}},
	}))
	if err != nil {
		t.Fatalf("TopicUpdate: %v", err)
	}
	lbls, _ := resp.Data["labels"].(map[string]string)
	if lbls["new"] != "2" || len(lbls) != 1 {
		t.Errorf("labels = %v, want {new:2}", resp.Data["labels"])
	}

	// Empty labels clears the set.
	resp, err = p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/e",
		"body": map[string]any{"topic": map[string]any{}, "updateMask": "labels"},
	}))
	if err != nil {
		t.Fatalf("TopicUpdate clear: %v", err)
	}
	if _, ok := resp.Data["labels"]; ok {
		t.Errorf("labels present after clear: %v", resp.Data["labels"])
	}
}

// TestTopicUpdateNormalizesRetention proves a non-canonical Duration input is
// rewritten to the protobuf-JSON seconds form, so REST readers can always
// unmarshal it.
func TestTopicUpdateNormalizesRetention(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/n"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	resp, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/n",
		"body": map[string]any{
			"topic":      map[string]any{"messageRetentionDuration": "10m0s"},
			"updateMask": "messageRetentionDuration",
		},
	}))
	if err != nil {
		t.Fatalf("TopicUpdate: %v", err)
	}
	if got, _ := resp.Data["messageRetentionDuration"].(string); got != "600s" {
		t.Errorf("retention = %q, want 600s", got)
	}
}

// TestTopicUpdateUnsupportedMask rejects an unknown mask path rather than
// silently ignoring it (400 InvalidArgument), matching the gRPC handler.
func TestTopicUpdateUnsupportedMask(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/x"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	_, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/x",
		"body": map[string]any{"topic": map[string]any{"kmsKeyName": "k"}, "updateMask": "kmsKeyName"},
	}))
	if err == nil || errStatus(err) != 400 {
		t.Fatalf("err = %v, want 400 InvalidArgument", err)
	}
}

// TestTopicUpdateNotFound returns 404 for an unknown topic.
func TestTopicUpdateNotFound(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	_, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/missing",
		"body": map[string]any{"topic": map[string]any{"labels": map[string]any{"a": "b"}}},
	}))
	if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Fatalf("err = %v, want 404 ProviderError", err)
	}
}
