package pubsub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/crypto"
	pubsubcore "jaiscloud/internal/gcp/service/pubsub"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
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
// unmarshal it. The update path shares the create validator, so only
// protobuf-JSON forms are accepted ("600.000s", not Go's "10m0s").
func TestTopicUpdateNormalizesRetention(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/n"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	resp, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/n",
		"body": map[string]any{
			"topic":      map[string]any{"messageRetentionDuration": "600.000s"},
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
// `state` is output-only and never maskable, so it stays the pinned guard now
// that kmsKeyName and the rest of the mutable surface are supported.
func TestTopicUpdateUnsupportedMask(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/x"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	_, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/x",
		"body": map[string]any{"topic": map[string]any{"state": "ACTIVE"}, "updateMask": "state"},
	}))
	if err == nil || errStatus(err) != 400 {
		t.Fatalf("err = %v, want 400 InvalidArgument", err)
	}
	if pe, ok := err.(*model.ProviderError); ok && !strings.Contains(pe.Message, "unsupported update_mask path: state") {
		t.Errorf("message = %q, want unsupported update_mask path: state", pe.Message)
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

// ─── the provider's mask matrix ──────────────────────────────────────────────

const (
	updateOldKey = "projects/proj/locations/global/keyRings/old/cryptoKeys/old-key"
	updateNewKey = "projects/proj/locations/global/keyRings/new/cryptoKeys/new-key"
)

// updateBaselineTopicBody seeds every mutable topic field. Each mask case starts
// from it and replaces exactly the field under test.
func updateBaselineTopicBody() map[string]any {
	return map[string]any{
		"labels":                   map[string]any{"env": "old", "team": "data"},
		"messageRetentionDuration": "600s",
		"kmsKeyName":               updateOldKey,
		"messageStoragePolicy": map[string]any{
			"allowedPersistenceRegions": []any{"us-east1"},
			"enforceInTransit":          true,
		},
		"schemaSettings": map[string]any{
			"schema":   "projects/proj/schemas/orders",
			"encoding": "BINARY",
		},
		"ingestionDataSourceSettings": map[string]any{
			"cloudStorage": map[string]any{"bucket": "old-bucket"},
		},
		"messageTransforms": []any{
			map[string]any{
				"javascriptUdf": map[string]any{"functionName": "old", "code": "function old(m) { return m; }"},
				"enabled":       true,
			},
		},
	}
}

// updateBaselineStored is what updateBaselineTopicBody persists: canonical
// durations, string labels and no output-only members.
func updateBaselineStored() map[string]any {
	return map[string]any{
		"labels":                   map[string]any{"env": "old", "team": "data"},
		"messageRetentionDuration": "600s",
		"kmsKeyName":               updateOldKey,
		"messageStoragePolicy": map[string]any{
			"allowedPersistenceRegions": []any{"us-east1"},
			"enforceInTransit":          true,
		},
		"schemaSettings": map[string]any{
			"schema":   "projects/proj/schemas/orders",
			"encoding": "BINARY",
		},
		"ingestionDataSourceSettings": map[string]any{
			"cloudStorage": map[string]any{"bucket": "old-bucket"},
		},
		"messageTransforms": []any{
			map[string]any{
				"javascriptUdf": map[string]any{"functionName": "old", "code": "function old(m) { return m; }"},
				"enabled":       true,
			},
		},
	}
}

// jsonShape renders any Go value through JSON so map[string]string and
// []string compare equal to their decoded map[string]any / []any form.
func jsonShape(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	return out
}

// storedTopicFields returns the mutable fields of a topic response in their
// decoded JSON shape, ignoring the resource name and internal keys.
func storedTopicFields(t *testing.T, data map[string]any) map[string]any {
	t.Helper()
	out := map[string]any{}
	for _, name := range pubsubcore.TopicFieldNames() {
		if v, ok := data[name]; ok {
			out[name] = jsonShape(t, v)
		}
	}
	return out
}

func cloneFields(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func updateTopicBody(field string, value any, mask string) map[string]any {
	topic := updateBaselineTopicBody()
	topic[field] = value
	return map[string]any{"topic": topic, "updateMask": mask}
}

// TestTopicUpdateProviderMaskMatrix is the REST table: one case per field the
// google provider emits in its update_mask, in the provider's order and
// spelling, each body as the provider sends it (the whole topic) and the mask
// as it sends it. Every case asserts the PATCH response and a following GET.
func TestTopicUpdateProviderMaskMatrix(t *testing.T) {
	ctx := context.Background()
	transforms := []any{
		map[string]any{
			"javascriptUdf": map[string]any{"functionName": "updated", "code": "function updated(m) { return m; }"},
			"enabled":       true,
		},
	}
	cases := []struct {
		name  string
		field string
		mask  string
		value any
		want  any
	}{
		{"kmsKeyName", "kmsKeyName", "kmsKeyName", updateNewKey, updateNewKey},
		{"messageStoragePolicy", "messageStoragePolicy", "messageStoragePolicy",
			map[string]any{"allowedPersistenceRegions": []any{"europe-west1", "asia-east1"}, "enforceInTransit": true},
			map[string]any{"allowedPersistenceRegions": []any{"europe-west1", "asia-east1"}, "enforceInTransit": true}},
		{"schemaSettings", "schemaSettings", "schemaSettings",
			map[string]any{"schema": "projects/proj/schemas/updated", "encoding": "JSON"},
			map[string]any{"schema": "projects/proj/schemas/updated", "encoding": "JSON"}},
		{"messageRetentionDuration", "messageRetentionDuration", "messageRetentionDuration", "1800s", "1800s"},
		{"ingestionDataSourceSettings", "ingestionDataSourceSettings", "ingestionDataSourceSettings",
			map[string]any{"cloudStorage": map[string]any{"bucket": "new-bucket"}},
			map[string]any{"cloudStorage": map[string]any{"bucket": "new-bucket"}}},
		{"messageTransforms", "messageTransforms", "messageTransforms", transforms, transforms},
		{"labels", "labels", "labels",
			map[string]any{"env": "new", "team": "data"},
			map[string]any{"env": "new", "team": "data"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestProvider()
			if _, err := p.TopicCreate(ctx, newNR(map[string]any{
				"name": "topics/masked-" + tc.name,
				"body": updateBaselineTopicBody(),
			})); err != nil {
				t.Fatalf("topic create: %v", err)
			}

			want := updateBaselineStored()
			want[tc.field] = tc.want

			resp, err := p.TopicUpdate(ctx, newNR(map[string]any{
				"name": "topics/masked-" + tc.name,
				"body": updateTopicBody(tc.field, tc.value, tc.mask),
			}))
			if err != nil {
				t.Fatalf("TopicUpdate: %v", err)
			}
			if got := storedTopicFields(t, resp.Data); !reflect.DeepEqual(got, want) {
				t.Fatalf("update response fields\n got: %#v\nwant: %#v", got, want)
			}

			get, err := p.TopicGet(ctx, newNR(map[string]any{"name": "topics/masked-" + tc.name}))
			if err != nil {
				t.Fatalf("TopicGet: %v", err)
			}
			if got := storedTopicFields(t, get.Data); !reflect.DeepEqual(got, want) {
				t.Fatalf("persisted fields\n got: %#v\nwant: %#v", got, want)
			}
		})
	}
}

// TestTopicUpdateCompositeProviderMask covers the multi-field mask the provider
// emits for a change spanning kmsKeyName, retention and labels.
func TestTopicUpdateCompositeProviderMask(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{
		"name": "topics/composite",
		"body": updateBaselineTopicBody(),
	})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	topic := updateBaselineTopicBody()
	topic["kmsKeyName"] = updateNewKey
	topic["messageRetentionDuration"] = "900s"
	topic["labels"] = map[string]any{"env": "composite"}
	resp, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/composite",
		"body": map[string]any{
			"topic":      topic,
			"updateMask": "kmsKeyName,messageRetentionDuration,labels",
		},
	}))
	if err != nil {
		t.Fatalf("TopicUpdate: %v", err)
	}
	want := updateBaselineStored()
	want["kmsKeyName"] = updateNewKey
	want["messageRetentionDuration"] = "900s"
	want["labels"] = map[string]any{"env": "composite"}
	if got := storedTopicFields(t, resp.Data); !reflect.DeepEqual(got, want) {
		t.Fatalf("update response fields\n got: %#v\nwant: %#v", got, want)
	}
	get, err := p.TopicGet(ctx, newNR(map[string]any{"name": "topics/composite"}))
	if err != nil {
		t.Fatalf("TopicGet: %v", err)
	}
	if got := storedTopicFields(t, get.Data); !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted fields\n got: %#v\nwant: %#v", got, want)
	}
}

// TestTopicUpdateClearsAbsentField pins the clear contract the Terraform
// provider's re-apply loop depends on: a field named by the mask but absent
// from the body is deleted (storage policy, transforms and ingestion
// explicitly), and the deletion survives a following GET.
func TestTopicUpdateClearsAbsentField(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		field string
		mask  string
	}{
		{"messageStoragePolicy", "messageStoragePolicy", "messageStoragePolicy"},
		{"messageTransforms", "messageTransforms", "messageTransforms"},
		{"ingestionDataSourceSettings", "ingestionDataSourceSettings", "ingestionDataSourceSettings"},
		{"kmsKeyName", "kmsKeyName", "kmsKeyName"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestProvider()
			id := "topics/clear-" + tc.name
			if _, err := p.TopicCreate(ctx, newNR(map[string]any{
				"name": id,
				"body": updateBaselineTopicBody(),
			})); err != nil {
				t.Fatalf("topic create: %v", err)
			}
			topic := updateBaselineTopicBody()
			delete(topic, tc.field)
			resp, err := p.TopicUpdate(ctx, newNR(map[string]any{
				"name": id,
				"body": map[string]any{"topic": topic, "updateMask": tc.mask},
			}))
			if err != nil {
				t.Fatalf("TopicUpdate: %v", err)
			}
			if _, ok := resp.Data[tc.field]; ok {
				t.Errorf("response still carries %q: %v", tc.field, resp.Data[tc.field])
			}
			get, err := p.TopicGet(ctx, newNR(map[string]any{"name": id}))
			if err != nil {
				t.Fatalf("TopicGet: %v", err)
			}
			if _, ok := get.Data[tc.field]; ok {
				t.Errorf("persisted %q after clear: %v", tc.field, get.Data[tc.field])
			}

			want := updateBaselineStored()
			delete(want, tc.field)
			if got := storedTopicFields(t, get.Data); !reflect.DeepEqual(got, want) {
				t.Fatalf("fields after clear\n got: %#v\nwant: %#v", got, want)
			}
		})
	}
}

// TestTopicUpdateNestedMasks exercises the generic AIP-134 path resolution:
// a nested leaf replaces just that leaf and leaves sibling values intact.
func TestTopicUpdateNestedMasks(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{
		"name": "topics/nested",
		"body": updateBaselineTopicBody(),
	})); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	// allowedPersistenceRegions is a repeated leaf: the whole list is replaced,
	// and enforceInTransit is untouched.
	if _, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/nested",
		"body": map[string]any{
			"topic": map[string]any{
				"messageStoragePolicy": map[string]any{
					"allowedPersistenceRegions": []any{"europe-west1"},
				},
			},
			"updateMask": "messageStoragePolicy.allowedPersistenceRegions",
		},
	})); err != nil {
		t.Fatalf("storage policy nested update: %v", err)
	}
	get, err := p.TopicGet(ctx, newNR(map[string]any{"name": "topics/nested"}))
	if err != nil {
		t.Fatalf("TopicGet: %v", err)
	}
	policy, _ := get.Data["messageStoragePolicy"].(map[string]any)
	if got := jsonShape(t, policy["allowedPersistenceRegions"]); !reflect.DeepEqual(got, []any{"europe-west1"}) {
		t.Errorf("allowedPersistenceRegions = %v, want [europe-west1]", got)
	}
	if policy["enforceInTransit"] != true {
		t.Errorf("enforceInTransit = %v, want true (sibling retained)", policy["enforceInTransit"])
	}

	// A nested mask whose body is a partial object: only the encoding is sent,
	// and the shared helper merges it over the stored schema.
	if _, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/nested",
		"body": map[string]any{
			"topic": map[string]any{
				"schemaSettings": map[string]any{"encoding": "JSON"},
			},
			"updateMask": "schemaSettings.encoding",
		},
	})); err != nil {
		t.Fatalf("schema nested update: %v", err)
	}
	get, err = p.TopicGet(ctx, newNR(map[string]any{"name": "topics/nested"}))
	if err != nil {
		t.Fatalf("TopicGet: %v", err)
	}
	schema, _ := get.Data["schemaSettings"].(map[string]any)
	if schema["schema"] != "projects/proj/schemas/orders" || schema["encoding"] != "JSON" {
		t.Errorf("schemaSettings = %v, want the stored schema with encoding JSON", schema)
	}

	// ingestionDataSourceSettings.cloudStorage.bucket keeps the source.
	if _, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/nested",
		"body": map[string]any{
			"topic": map[string]any{
				"ingestionDataSourceSettings": map[string]any{
					"cloudStorage": map[string]any{"bucket": "nested-bucket"},
				},
			},
			"updateMask": "ingestionDataSourceSettings.cloudStorage.bucket",
		},
	})); err != nil {
		t.Fatalf("ingestion nested update: %v", err)
	}
	get, err = p.TopicGet(ctx, newNR(map[string]any{"name": "topics/nested"}))
	if err != nil {
		t.Fatalf("TopicGet: %v", err)
	}
	ingestion, _ := get.Data["ingestionDataSourceSettings"].(map[string]any)
	storage, _ := ingestion["cloudStorage"].(map[string]any)
	if storage["bucket"] != "nested-bucket" {
		t.Errorf("cloudStorage.bucket = %v, want nested-bucket", storage["bucket"])
	}

	// labels.env replaces one map entry and leaves the others.
	if _, err := p.TopicUpdate(ctx, newNR(map[string]any{
		"name": "topics/nested",
		"body": map[string]any{
			"topic":      map[string]any{"labels": map[string]any{"env": "only"}},
			"updateMask": "labels.env",
		},
	})); err != nil {
		t.Fatalf("labels nested update: %v", err)
	}
	get, err = p.TopicGet(ctx, newNR(map[string]any{"name": "topics/nested"}))
	if err != nil {
		t.Fatalf("TopicGet: %v", err)
	}
	if labels, _ := get.Data["labels"].(map[string]any); labels["env"] != "only" || labels["team"] != "data" {
		t.Errorf("labels = %v, want env=only team=data", get.Data["labels"])
	}
}

// TestTopicUpdateRejectsInvalidFields proves an invalid masked value is a 400
// and leaves the stored topic untouched.
func TestTopicUpdateRejectsInvalidFields(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		field string
		mask  string
		value any
	}{
		{"storage policy without regions", "messageStoragePolicy", "messageStoragePolicy",
			map[string]any{"allowedPersistenceRegions": []any{}}},
		{"storage policy bad region type", "messageStoragePolicy", "messageStoragePolicy",
			map[string]any{"allowedPersistenceRegions": []any{42}}},
		{"schema encoding", "schemaSettings", "schemaSettings",
			map[string]any{"schema": "projects/proj/schemas/orders", "encoding": "YAML"}},
		{"transform without oneof", "messageTransforms", "messageTransforms",
			[]any{map[string]any{"enabled": true}}},
		{"retention below minimum", "messageRetentionDuration", "messageRetentionDuration", "599s"},
		{"retention not a duration", "messageRetentionDuration", "messageRetentionDuration", "10m"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestProvider()
			id := "topics/reject-" + strings.ReplaceAll(tc.name, " ", "-")
			if _, err := p.TopicCreate(ctx, newNR(map[string]any{
				"name": id,
				"body": updateBaselineTopicBody(),
			})); err != nil {
				t.Fatalf("topic create: %v", err)
			}
			_, err := p.TopicUpdate(ctx, newNR(map[string]any{
				"name": id,
				"body": updateTopicBody(tc.field, tc.value, tc.mask),
			}))
			if err == nil || errStatus(err) != 400 {
				t.Fatalf("err = %v, want 400 InvalidArgument", err)
			}
			get, err := p.TopicGet(ctx, newNR(map[string]any{"name": id}))
			if err != nil {
				t.Fatalf("TopicGet: %v", err)
			}
			if got, want := storedTopicFields(t, get.Data), updateBaselineStored(); !reflect.DeepEqual(got, want) {
				t.Fatalf("rejected update persisted fields\n got: %#v\nwant: %#v", got, want)
			}
		})
	}
}

// TestTopicUpdateKMKRotation pins the documented non-retroactive CMEK contract:
// rotating kmsKeyName affects only later publishes. The old message keeps its
// per-message DEK (wrapped by the old key) and still pulls/decrypts.
func TestTopicUpdateKMKRotation(t *testing.T) {
	ctx := context.Background()
	kmsStore := kmsstore.NewMemoryStore()
	enc := crypto.NewEnvelopeEncryptor(kmsStore)
	p := New(store.NewMemoryResourceStore(), pubsubstore.NewMemoryMessages(), enc)

	const project = "proj"
	if err := kmsStore.CreateKeyRing(ctx, project, "global", "ring", kmsstore.KeyRing{ID: "ring"}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"old-key", "new-key"} {
		if err := kmsStore.CreateCryptoKey(ctx, project, "global", "ring", key, kmsstore.CryptoKey{ID: key, Purpose: "ENCRYPT_DECRYPT"}); err != nil {
			t.Fatal(err)
		}
	}
	keyName := func(k string) string {
		return "projects/" + project + "/locations/global/keyRings/ring/cryptoKeys/" + k
	}

	nr := newNR(map[string]any{"name": "topics/rotate", "body": map[string]any{"kmsKeyName": keyName("old-key")}})
	nr.AccountID = project
	if _, err := p.TopicCreate(ctx, nr); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	nr = newNR(map[string]any{"name": "subscriptions/rotate", "body": map[string]any{"topic": "projects/" + project + "/topics/rotate"}})
	nr.AccountID = project
	if _, err := p.SubscriptionCreate(ctx, nr); err != nil {
		t.Fatalf("subscription create: %v", err)
	}

	publish := func(payload string) {
		t.Helper()
		nr := newNR(map[string]any{
			"name": "topics/rotate",
			"body": map[string]any{"messages": []any{map[string]any{"data": base64.StdEncoding.EncodeToString([]byte(payload))}}},
		})
		nr.AccountID = project
		if _, err := p.TopicPublish(ctx, nr); err != nil {
			t.Fatalf("publish %q: %v", payload, err)
		}
	}
	publish("before-rotation")

	nr = newNR(map[string]any{
		"name": "topics/rotate",
		"body": map[string]any{
			"topic":      map[string]any{"kmsKeyName": keyName("new-key")},
			"updateMask": "kmsKeyName",
		},
	})
	nr.AccountID = project
	if _, err := p.TopicUpdate(ctx, nr); err != nil {
		t.Fatalf("rotate kmsKeyName: %v", err)
	}
	publish("after-rotation")

	msgs, err := p.messages.List(ctx, "rotate")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 stored messages, got %d", len(msgs))
	}
	if msgs[0].KmsKeyName != keyName("old-key") {
		t.Errorf("old message key = %q, want the pre-rotation key", msgs[0].KmsKeyName)
	}
	if msgs[1].KmsKeyName != keyName("new-key") {
		t.Errorf("new message key = %q, want the post-rotation key", msgs[1].KmsKeyName)
	}
	// The old message still unwraps and decrypts with its per-message DEK.
	rawDEK, err := p.encryptor.Unwrap(ctx, project, msgs[0].KmsKeyName, msgs[0].WrappedDEK)
	if err != nil {
		t.Fatalf("unwrap old message: %v", err)
	}
	ct, _ := base64.StdEncoding.DecodeString(msgs[0].Data)
	plain, err := kmsstore.DecryptData(rawDEK, ct, nil)
	if err != nil {
		t.Fatalf("decrypt old message: %v", err)
	}
	if string(plain) != "before-rotation" {
		t.Errorf("old message plaintext = %q, want before-rotation", plain)
	}
}
