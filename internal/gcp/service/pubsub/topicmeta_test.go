package pubsub

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"jaiscloud/internal/model"
)

// topicInputValues is one valid protobuf-JSON value per mutable field, keyed by
// the field table's names. Every per-field test starts from one of these.
func topicInputValues() map[string]any {
	return map[string]any{
		"labels":                   map[string]any{"env": "prod", "team": "data"},
		"messageRetentionDuration": "600s",
		"kmsKeyName":               "projects/proj/locations/global/keyRings/r/cryptoKeys/k",
		"messageStoragePolicy": map[string]any{
			"allowedPersistenceRegions": []any{"us-central1", "europe-west1"},
			"enforceInTransit":          true,
		},
		"schemaSettings": map[string]any{
			"schema":          "projects/proj/schemas/orders",
			"encoding":        "JSON",
			"firstRevisionId": "1",
			"lastRevisionId":  "9",
		},
		"ingestionDataSourceSettings": map[string]any{
			"cloudStorage": map[string]any{
				"bucket": "ingest-bucket",
				"state":  "ACTIVE",
			},
			"platformLogsSettings": map[string]any{"severity": "WARNING"},
		},
		"messageTransforms": []any{
			map[string]any{
				"javascriptUdf": map[string]any{"functionName": "transform", "code": "function transform(m) { return m; }"},
				"enabled":       true,
			},
		},
	}
}

func topicStoredValues() map[string]any {
	return map[string]any{
		"labels":                   map[string]string{"env": "prod", "team": "data"},
		"messageRetentionDuration": "600s",
		"kmsKeyName":               "projects/proj/locations/global/keyRings/r/cryptoKeys/k",
		"messageStoragePolicy": map[string]any{
			"allowedPersistenceRegions": []string{"us-central1", "europe-west1"},
			"enforceInTransit":          true,
		},
		"schemaSettings": map[string]any{
			"schema":   "projects/proj/schemas/orders",
			"encoding": "JSON",
		},
		"ingestionDataSourceSettings": map[string]any{
			"cloudStorage":         map[string]any{"bucket": "ingest-bucket"},
			"platformLogsSettings": map[string]any{"severity": "WARNING"},
		},
		"messageTransforms": []any{
			map[string]any{
				"javascriptUdf": map[string]any{"functionName": "transform", "code": "function transform(m) { return m; }"},
				"enabled":       true,
			},
		},
	}
}

// TestNormalizeTopicInputRoundTrip pins the stored shape of every mutable field:
// the whole module body survives, with canonical durations, string labels,
// stripped output-only members and typed nested objects.
func TestNormalizeTopicInputRoundTrip(t *testing.T) {
	got, err := NormalizeTopicInput(topicInputValues())
	if err != nil {
		t.Fatalf("NormalizeTopicInput: %v", err)
	}
	want := topicStoredValues()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized topic\n got: %#v\nwant: %#v", got, want)
	}
}

// TestNormalizeTopicInputPerField feeds each mutable field through on its own,
// so a failure names the field that broke rather than the whole body.
func TestNormalizeTopicInputPerField(t *testing.T) {
	in := topicInputValues()
	want := topicStoredValues()
	for _, name := range TopicFieldNames() {
		value, ok := in[name]
		if !ok {
			t.Fatalf("topicInputValues has no value for %q", name)
		}
		got, err := NormalizeTopicInput(map[string]any{name: value})
		if err != nil {
			t.Errorf("%s: NormalizeTopicInput: %v", name, err)
			continue
		}
		expected := map[string]any{name: want[name]}
		if !reflect.DeepEqual(got, expected) {
			t.Errorf("%s: got %#v, want %#v", name, got, expected)
		}
	}
}

// TestNormalizeTopicInputStripsOutputOnly verifies a client can echo back a
// topic it read: output-only and input-only fields are never persisted.
func TestNormalizeTopicInputStripsOutputOnly(t *testing.T) {
	in := topicInputValues()
	in["name"] = "projects/proj/topics/echoed"
	in["state"] = "ACTIVE"
	in["satisfiesPzs"] = true
	in["tags"] = map[string]any{"123/env": "prod"}

	got, err := NormalizeTopicInput(in)
	if err != nil {
		t.Fatalf("NormalizeTopicInput: %v", err)
	}
	for _, name := range []string{"name", "state", "satisfiesPzs", "tags"} {
		if _, ok := got[name]; ok {
			t.Errorf("output-only field %q was stored: %v", name, got[name])
		}
	}
	// schemaSettings.firstRevisionId/lastRevisionId and each ingestion source's
	// state are output-only members of mutable fields.
	schema := got["schemaSettings"].(map[string]any)
	for _, key := range []string{"firstRevisionId", "lastRevisionId"} {
		if _, ok := schema[key]; ok {
			t.Errorf("schemaSettings.%s was stored", key)
		}
	}
	ing := got["ingestionDataSourceSettings"].(map[string]any)
	storage := ing["cloudStorage"].(map[string]any)
	if _, ok := storage["state"]; ok {
		t.Errorf("ingestionDataSourceSettings.cloudStorage.state was stored")
	}
}

// TestNormalizeTopicInputUnknownFieldsIgnored pins the pre-existing tolerance
// (PTF7): an unknown field name is dropped rather than rejected, so the
// emulator keeps accepting bodies it does not model.
func TestNormalizeTopicInputUnknownFieldsIgnored(t *testing.T) {
	got, err := NormalizeTopicInput(map[string]any{
		"state":       "DELETED",
		"bogusField":  "ignored",
		"anotherOne":  map[string]any{"x": "y"},
		"messageType": "TYPE_UNSPECIFIED",
	})
	if err != nil {
		t.Fatalf("NormalizeTopicInput: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no stored fields, got %#v", got)
	}

	// An absent or nil body stores nothing either.
	got, err = NormalizeTopicInput(nil)
	if err != nil {
		t.Fatalf("NormalizeTopicInput(nil): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no stored fields, got %#v", got)
	}
}

// TestNormalizeTopicInputRejections drives one invalid value through each
// validator and asserts the error is a 400 InvalidArgument whose message names
// the offending field. Both transports map this one error, so the text is the
// client-visible parity contract.
func TestNormalizeTopicInputRejections(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		value   any
		wantMsg string
	}{
		{"labels not an object", "labels", "env=prod", "labels must be an object"},
		{"label value not a string", "labels", map[string]any{"env": float64(1)}, `labels["env"] must be a string`},
		{"retention not a string", "messageRetentionDuration", float64(600), "messageRetentionDuration must be a duration string"},
		{"retention below the floor", "messageRetentionDuration", "599s", "between 10 minutes (600s) and 31 days"},
		{"retention above the ceiling", "messageRetentionDuration", "2678401s", "between 10 minutes (600s) and 31 days"},
		{"retention negative", "messageRetentionDuration", "-600s", "between 10 minutes (600s) and 31 days"},
		{"retention in Go syntax", "messageRetentionDuration", "10m", `is not a duration string like "600s"`},
		{"retention garbage", "messageRetentionDuration", "soon", `is not a duration string like "600s"`},
		{"kms key not a string", "kmsKeyName", float64(1), "kmsKeyName must be a string"},
		{"kms key wrong shape", "kmsKeyName", "projects/p/keyRings/r/cryptoKeys/k", "must be a CryptoKey resource name"},
		{"kms key version", "kmsKeyName", "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1", "must be a CryptoKey resource name"},
		{"storage policy not an object", "messageStoragePolicy", "us-central1", "messageStoragePolicy must be an object"},
		{"storage policy missing regions", "messageStoragePolicy", map[string]any{"enforceInTransit": true}, "allowedPersistenceRegions must be set"},
		{"storage policy empty regions", "messageStoragePolicy", map[string]any{"allowedPersistenceRegions": []any{}}, "at least one region"},
		{"storage policy region not a string", "messageStoragePolicy", map[string]any{"allowedPersistenceRegions": []any{float64(1)}}, "must be a non-empty region string"},
		{"storage policy flag not a bool", "messageStoragePolicy", map[string]any{"allowedPersistenceRegions": []any{"us-central1"}, "enforceInTransit": "yes"}, "enforceInTransit must be a boolean"},
		{"schema settings not an object", "schemaSettings", "projects/p/schemas/s", "schemaSettings must be an object"},
		{"schema missing", "schemaSettings", map[string]any{"encoding": "JSON"}, "schemaSettings.schema is required"},
		{"schema wrong shape", "schemaSettings", map[string]any{"schema": "projects/p/topics/t"}, "must be a Schema resource name"},
		{"schema encoding unknown", "schemaSettings", map[string]any{"schema": "projects/p/schemas/s", "encoding": "AVRO"}, "must be one of ENCODING_UNSPECIFIED, JSON, BINARY"},
		{"schema encoding not a string", "schemaSettings", map[string]any{"schema": "projects/p/schemas/s", "encoding": float64(1)}, "must be one of ENCODING_UNSPECIFIED, JSON, BINARY"},
		{"ingestion not an object", "ingestionDataSourceSettings", "cloudStorage", "ingestionDataSourceSettings must be an object"},
		{"ingestion no source", "ingestionDataSourceSettings", map[string]any{"platformLogsSettings": map[string]any{"severity": "INFO"}}, "exactly one ingestion source"},
		{"ingestion two sources", "ingestionDataSourceSettings", map[string]any{
			"cloudStorage": map[string]any{"bucket": "b"},
			"awsKinesis":   map[string]any{"streamArn": "arn"},
		}, "exactly one ingestion source"},
		{"ingestion source not an object", "ingestionDataSourceSettings", map[string]any{"cloudStorage": "bucket"}, "cloudStorage must be an object"},
		{"platform logs severity unknown", "ingestionDataSourceSettings", map[string]any{
			"cloudStorage":         map[string]any{"bucket": "b"},
			"platformLogsSettings": map[string]any{"severity": "TRACE"},
		}, "severity"},
		{"transform list not a list", "messageTransforms", map[string]any{}, "messageTransforms must be a list"},
		{"transform not an object", "messageTransforms", []any{"udf"}, "messageTransforms[0] must be an object"},
		{"transform no variant", "messageTransforms", []any{map[string]any{"enabled": true}}, "must set exactly one of"},
		{"transform two variants", "messageTransforms", []any{map[string]any{
			"javascriptUdf": map[string]any{"functionName": "f"},
			"aiInference":   map[string]any{"endpoint": "e"},
		}}, "must set exactly one of"},
		{"transform variant not an object", "messageTransforms", []any{map[string]any{"javascriptUdf": "udf"}}, "javascriptUdf must be an object"},
		{"transform flag not a bool", "messageTransforms", []any{map[string]any{
			"aiInference": map[string]any{"endpoint": "e"},
			"disabled":    "no",
		}}, "disabled must be a boolean"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeTopicInput(map[string]any{tc.field: tc.value})
			if err == nil {
				t.Fatalf("expected a rejection, got nil")
			}
			var pe *model.ProviderError
			if !errors.As(err, &pe) {
				t.Fatalf("error %v is not a ProviderError", err)
			}
			if pe.HTTPStatus != 400 {
				t.Errorf("HTTPStatus = %d, want 400", pe.HTTPStatus)
			}
			if !strings.Contains(pe.Message, tc.wantMsg) {
				t.Errorf("message %q does not contain %q", pe.Message, tc.wantMsg)
			}
		})
	}
}

// TestNormalizeTopicRetentionBounds pins the documented window's edges and the
// canonical stored form: whole seconds are rendered "600s", not "10m0s".
func TestNormalizeTopicRetentionBounds(t *testing.T) {
	tests := []struct {
		in   string
		want string
		ok   bool
	}{
		{"600s", "600s", true},
		{"2678400s", "2678400s", true},
		{"604800s", "604800s", true},
		{"600.000s", "600s", true},
		{"600.500s", "600.500s", true},
		{"599s", "", false},
		{"2678401s", "", false},
		{"0s", "", false},
	}
	for _, tc := range tests {
		got, err := NormalizeTopicInput(map[string]any{"messageRetentionDuration": tc.in})
		if !tc.ok {
			if err == nil {
				t.Errorf("%q: expected a rejection, got %v", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", tc.in, err)
			continue
		}
		if got["messageRetentionDuration"] != tc.want {
			t.Errorf("%q: stored %v, want %q", tc.in, got["messageRetentionDuration"], tc.want)
		}
	}
}

// TestNormalizeTopicEmptyValuesUnset verifies empty optional values store
// nothing, so GET never echoes empty containers or zero enums.
func TestNormalizeTopicEmptyValuesUnset(t *testing.T) {
	got, err := NormalizeTopicInput(map[string]any{
		"labels":                   map[string]any{},
		"messageRetentionDuration": "",
		"kmsKeyName":               "",
		"messageTransforms":        []any{},
		"messageStoragePolicy": map[string]any{
			"allowedPersistenceRegions": []any{"us-central1"},
			"enforceInTransit":          false,
		},
		"schemaSettings": map[string]any{
			"schema":   "projects/p/schemas/s",
			"encoding": "ENCODING_UNSPECIFIED",
		},
		"ingestionDataSourceSettings": map[string]any{
			"cloudStorage":         map[string]any{"bucket": "b"},
			"platformLogsSettings": map[string]any{"severity": "SEVERITY_UNSPECIFIED"},
		},
	})
	if err != nil {
		t.Fatalf("NormalizeTopicInput: %v", err)
	}
	want := map[string]any{
		"messageStoragePolicy":        map[string]any{"allowedPersistenceRegions": []string{"us-central1"}},
		"schemaSettings":              map[string]any{"schema": "projects/p/schemas/s"},
		"ingestionDataSourceSettings": map[string]any{"cloudStorage": map[string]any{"bucket": "b"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

// TestTopicFieldNamesMatchesTable guards the transcoder contract: the gRPC
// subset is the field table, so the two can never diverge.
func TestTopicFieldNamesMatchesTable(t *testing.T) {
	names := TopicFieldNames()
	if len(names) != len(topicMutableFields) {
		t.Fatalf("TopicFieldNames returned %d names for %d fields", len(names), len(topicMutableFields))
	}
	for i, f := range topicMutableFields {
		if names[i] != f.name {
			t.Errorf("names[%d] = %q, want %q", i, names[i], f.name)
		}
	}
}

// ─── ApplyTopicUpdate ────────────────────────────────────────────────────────

func mustNormalizedTopic(t *testing.T) map[string]any {
	t.Helper()
	meta, err := NormalizeTopicInput(topicInputValues())
	if err != nil {
		t.Fatalf("NormalizeTopicInput: %v", err)
	}
	return meta
}

// TestApplyTopicUpdateRootPaths replaces every mutable field through its root
// mask path and proves the stored shape is the same one NormalizeTopicInput
// produces.
func TestApplyTopicUpdateRootPaths(t *testing.T) {
	want := topicStoredValues()
	for _, name := range TopicFieldNames() {
		t.Run(name, func(t *testing.T) {
			meta := map[string]any{}
			if err := ApplyTopicUpdate(meta, topicInputValues(), []string{name}); err != nil {
				t.Fatalf("ApplyTopicUpdate: %v", err)
			}
			if !reflect.DeepEqual(meta[name], want[name]) {
				t.Errorf("stored %s = %#v, want %#v", name, meta[name], want[name])
			}
		})
	}
}

// TestApplyTopicUpdateClearRoot removes every mutable field by naming it in the
// mask and omitting it from the body.
func TestApplyTopicUpdateClearRoot(t *testing.T) {
	for _, name := range TopicFieldNames() {
		t.Run(name, func(t *testing.T) {
			meta := mustNormalizedTopic(t)
			if err := ApplyTopicUpdate(meta, map[string]any{}, []string{name}); err != nil {
				t.Fatalf("ApplyTopicUpdate: %v", err)
			}
			if v, ok := meta[name]; ok {
				t.Errorf("stored %s after clear = %#v, want absent", name, v)
			}
		})
	}
}

// TestApplyTopicUpdateEmptyValueClears covers the empty forms that mean "clear":
// an empty string, map or list. An empty ingestion object clears the field
// without tripping the exactly-one-source validator.
func TestApplyTopicUpdateEmptyValueClears(t *testing.T) {
	cases := map[string]any{
		"labels":                      map[string]any{},
		"messageRetentionDuration":    "",
		"kmsKeyName":                  "",
		"messageStoragePolicy":        map[string]any{},
		"schemaSettings":              nil,
		"ingestionDataSourceSettings": map[string]any{},
		"messageTransforms":           []any{},
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			meta := mustNormalizedTopic(t)
			if err := ApplyTopicUpdate(meta, map[string]any{name: value}, []string{name}); err != nil {
				t.Fatalf("ApplyTopicUpdate: %v", err)
			}
			if v, ok := meta[name]; ok {
				t.Errorf("stored %s after empty value = %#v, want absent", name, v)
			}
		})
	}
}

// TestApplyTopicUpdateNestedPaths merges a nested leaf into the stored object
// and leaves its siblings alone.
func TestApplyTopicUpdateNestedPaths(t *testing.T) {
	t.Run("repeated leaf replaces the list", func(t *testing.T) {
		meta := mustNormalizedTopic(t)
		in := map[string]any{"messageStoragePolicy": map[string]any{
			"allowedPersistenceRegions": []any{"asia-east1"},
		}}
		if err := ApplyTopicUpdate(meta, in, []string{"messageStoragePolicy.allowed_persistence_regions"}); err != nil {
			t.Fatalf("ApplyTopicUpdate: %v", err)
		}
		policy := meta["messageStoragePolicy"].(map[string]any)
		if got := policy["allowedPersistenceRegions"]; !reflect.DeepEqual(got, []string{"asia-east1"}) {
			t.Errorf("regions = %#v, want [asia-east1]", got)
		}
		if policy["enforceInTransit"] != true {
			t.Errorf("enforceInTransit = %v, want true (sibling retained)", policy["enforceInTransit"])
		}
	})

	t.Run("partial schema object keeps the stored schema", func(t *testing.T) {
		meta := mustNormalizedTopic(t)
		in := map[string]any{"schemaSettings": map[string]any{"encoding": "JSON"}}
		if err := ApplyTopicUpdate(meta, in, []string{"schema_settings.encoding"}); err != nil {
			t.Fatalf("ApplyTopicUpdate: %v", err)
		}
		schema := meta["schemaSettings"].(map[string]any)
		if schema["schema"] != "projects/proj/schemas/orders" || schema["encoding"] != "JSON" {
			t.Errorf("schemaSettings = %#v, want stored schema with JSON encoding", schema)
		}
	})

	t.Run("nested bucket keeps the ingestion source", func(t *testing.T) {
		meta := mustNormalizedTopic(t)
		in := map[string]any{"ingestionDataSourceSettings": map[string]any{
			"cloudStorage": map[string]any{"bucket": "new-bucket"},
		}}
		if err := ApplyTopicUpdate(meta, in, []string{"ingestionDataSourceSettings.cloudStorage.bucket"}); err != nil {
			t.Fatalf("ApplyTopicUpdate: %v", err)
		}
		ingestion := meta["ingestionDataSourceSettings"].(map[string]any)
		storage := ingestion["cloudStorage"].(map[string]any)
		if storage["bucket"] != "new-bucket" {
			t.Errorf("bucket = %v, want new-bucket", storage["bucket"])
		}
	})

	t.Run("map entry replaces one label", func(t *testing.T) {
		meta := mustNormalizedTopic(t)
		in := map[string]any{"labels": map[string]any{"env": "staging"}}
		if err := ApplyTopicUpdate(meta, in, []string{"labels.env"}); err != nil {
			t.Fatalf("ApplyTopicUpdate: %v", err)
		}
		labels := meta["labels"].(map[string]string)
		if labels["env"] != "staging" || labels["team"] != "data" {
			t.Errorf("labels = %#v, want env=staging team=data", labels)
		}
	})
}

// TestApplyTopicUpdateNestedClears deletes a nested leaf, prunes empty parents,
// and removes the whole field once its last leaf is gone.
func TestApplyTopicUpdateNestedClears(t *testing.T) {
	t.Run("leaf with siblings retained", func(t *testing.T) {
		meta := mustNormalizedTopic(t)
		in := map[string]any{"messageStoragePolicy": map[string]any{}}
		if err := ApplyTopicUpdate(meta, in, []string{"messageStoragePolicy.enforceInTransit"}); err != nil {
			t.Fatalf("ApplyTopicUpdate: %v", err)
		}
		policy, ok := meta["messageStoragePolicy"].(map[string]any)
		if !ok {
			t.Fatalf("messageStoragePolicy removed, want regions retained")
		}
		if _, ok := policy["enforceInTransit"]; ok {
			t.Errorf("enforceInTransit = %v, want cleared", policy["enforceInTransit"])
		}
	})

	t.Run("prune empty parent", func(t *testing.T) {
		meta := map[string]any{"ingestionDataSourceSettings": map[string]any{
			"cloudStorage": map[string]any{"bucket": "only"},
		}}
		in := map[string]any{"ingestionDataSourceSettings": map[string]any{
			"cloudStorage": map[string]any{},
		}}
		if err := ApplyTopicUpdate(meta, in, []string{"ingestionDataSourceSettings.cloudStorage.bucket"}); err != nil {
			t.Fatalf("ApplyTopicUpdate: %v", err)
		}
		if v, ok := meta["ingestionDataSourceSettings"]; ok {
			t.Errorf("ingestionDataSourceSettings = %#v, want absent after pruning", v)
		}
	})

	t.Run("last label clears the field", func(t *testing.T) {
		meta := map[string]any{"labels": map[string]string{"env": "prod"}}
		if err := ApplyTopicUpdate(meta, map[string]any{"labels": map[string]any{}}, []string{"labels.env"}); err != nil {
			t.Fatalf("ApplyTopicUpdate: %v", err)
		}
		if v, ok := meta["labels"]; ok {
			t.Errorf("labels = %#v, want absent", v)
		}
	})
}

// TestApplyTopicUpdateRejectsUnknownAndInvalid paths stay loud and never mutate
// the stored map on failure.
func TestApplyTopicUpdateRejectsUnknownAndInvalid(t *testing.T) {
	meta := mustNormalizedTopic(t)
	before, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal before: %v", err)
	}
	cases := []struct {
		name string
		in   map[string]any
		mask []string
	}{
		{"unknown root", map[string]any{"state": "ACTIVE"}, []string{"state"}},
		{"name is not a field", map[string]any{"name": "projects/p/topics/t"}, []string{"name"}},
		{"empty path", map[string]any{}, []string{""}},
		{"invalid retention", map[string]any{"messageRetentionDuration": "599s"}, []string{"messageRetentionDuration"}},
		{"nested policy without regions", map[string]any{"messageStoragePolicy": map[string]any{}}, []string{"messageStoragePolicy.allowedPersistenceRegions"}},
		{"nested list on a repeated root", map[string]any{"messageTransforms": topicInputValues()["messageTransforms"]}, []string{"messageTransforms.foo"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ApplyTopicUpdate(meta, tc.in, tc.mask); err == nil {
				t.Fatalf("ApplyTopicUpdate(%v) = nil, want InvalidArgument", tc.mask)
			}
			after, err := json.Marshal(meta)
			if err != nil {
				t.Fatalf("marshal after: %v", err)
			}
			if string(after) != string(before) {
				t.Fatalf("failed update mutated the stored map\n got: %s\nwant: %s", after, before)
			}
		})
	}
}

// TestApplyTopicUpdateSpellingsAndHelperBranches covers the snake/camel mask
// spelling fold, multi-path application and the small predicate helpers.
func TestApplyTopicUpdateSpellingsAndHelperBranches(t *testing.T) {
	meta := mustNormalizedTopic(t)
	in := topicInputValues()
	if err := ApplyTopicUpdate(meta, in, []string{
		"MESSAGE_STORAGE_POLICY",
		"message_retention_duration",
	}); err != nil {
		t.Fatalf("ApplyTopicUpdate: %v", err)
	}
	if meta["messageRetentionDuration"] != "600s" {
		t.Errorf("retention = %v, want 600s", meta["messageRetentionDuration"])
	}

	if !TopicMaskHasNestedPath([]string{"labels", "schemaSettings.encoding"}) {
		t.Error("TopicMaskHasNestedPath = false for a nested path")
	}
	if TopicMaskHasNestedPath([]string{"labels", "kmsKeyName"}) {
		t.Error("TopicMaskHasNestedPath = true for root-only paths")
	}
	emptyCases := []struct {
		value any
		want  bool
	}{
		{nil, true},
		{"", true},
		{map[string]any{}, true},
		{[]any{}, true},
		{"x", false},
		{map[string]any{"a": 1}, false},
		{[]any{1}, false},
		{false, false},
	}
	for _, tc := range emptyCases {
		if got := isEmptyTopicValue(tc.value); got != tc.want {
			t.Errorf("isEmptyTopicValue(%#v) = %v, want %v", tc.value, got, tc.want)
		}
	}
}
