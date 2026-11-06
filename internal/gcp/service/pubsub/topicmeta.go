// Topic metadata: the single topic-field model both Pub/Sub transports share.
//
// A topic is persisted as a protobuf-JSON map (camelCase keys, enum names,
// google-duration strings), which is exactly the shape `protojson.Marshal`
// renders for a `google.pubsub.v1.Topic`. The REST provider decodes the request
// body into that map directly; the gRPC service marshals the request Topic into
// it. Both then hand the map to NormalizeTopicInput, so the two create paths
// cannot disagree about which fields are stored, which are output-only, or what
// a valid field value is, and the error text a client sees is the same on both
// transports.
//
// The model is field-table driven: one entry per mutable Topic field, with a
// validator that works on the map shape. Adding a field to the API means adding
// one table row (plus, for the gRPC transport, nothing — topicToProto rebuilds
// the Topic from the same table).
package pubsub

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Topic retention bounds from the Discovery document: messageRetentionDuration
// "Cannot be more than 31 days or less than 10 minutes".
const (
	minTopicRetention = 600 * time.Second     // 10 minutes
	maxTopicRetention = 2678400 * time.Second // 31 days
)

// Known enum values of the mutable topic fields. Any other value is rejected:
// the wire contract is the protobuf JSON encoding, which only accepts the
// declared names.
var (
	topicSchemaEncodings   = []string{"ENCODING_UNSPECIFIED", "JSON", "BINARY"}
	topicPlatformLogLevels = []string{"SEVERITY_UNSPECIFIED", "DISABLED", "DEBUG", "INFO", "WARNING", "ERROR"}

	// ingestionSourceFields are the members of IngestionDataSourceSettings'
	// `source` oneof (exactly one may be set). platformLogsSettings is not a
	// source: it configures logs and rides alongside whichever source is set.
	ingestionSourceFields = []string{"awsKinesis", "cloudStorage", "azureEventHubs", "awsMsk", "confluentCloud"}

	// messageTransformVariants are the members of MessageTransform's `transform`
	// oneof. javascriptUdf and aiInference are the documented transform types;
	// compression is the third variant the Discovery schema declares, so it is
	// accepted (and stored) rather than rejected as an unknown field.
	messageTransformVariants = []string{"javascriptUdf", "compression", "aiInference"}
)

// topicField is one mutable Topic field: its protobuf-JSON name and the
// validator that turns the request value into the stored value. A validator
// returns nil to store nothing (an empty value is "unset", matching proto3
// default omission on the wire).
type topicField struct {
	name      string
	normalize func(value any) (any, error)
}

// topicMutableFields is the table NormalizeTopicInput and topicToProto share.
// Every field the API documents as mutable is here; input-only `tags` and
// output-only `state`/`satisfiesPzs` deliberately are not (the first is not
// modelled yet, the others are stripped before storage).
var topicMutableFields = []topicField{
	{"labels", normalizeTopicLabels},
	{"messageRetentionDuration", normalizeTopicRetention},
	{"kmsKeyName", normalizeTopicKMSKeyName},
	{"messageStoragePolicy", normalizeTopicStoragePolicy},
	{"schemaSettings", normalizeTopicSchemaSettings},
	{"ingestionDataSourceSettings", normalizeTopicIngestionDataSourceSettings},
	{"messageTransforms", normalizeTopicTransforms},
}

// TopicFieldNames returns the mutable Topic fields both transports store and
// serve, in the table's order. The gRPC transcoder unmarshals exactly this
// subset, so a stored field can never be invisible over one transport only.
// The caller owns the returned slice.
func TopicFieldNames() []string {
	names := make([]string, len(topicMutableFields))
	for i, f := range topicMutableFields {
		names[i] = f.name
	}
	return names
}

// NormalizeTopicInput validates a topic body in its protobuf-JSON map form and
// returns the fields to persist. Only known mutable fields are copied; unknown
// names (including input-only `tags`) and output-only fields (`state`,
// `satisfiesPzs`, `schemaSettings.firstRevisionId`/`lastRevisionId`, and each
// ingestion source's `state`) are dropped rather than stored, so a client can
// echo a topic it read back. `name` is not a field: the resource path owns it.
//
// Every violation is an InvalidArgument, and because both transports call this
// one function they answer with the same message.
func NormalizeTopicInput(in map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(topicMutableFields))
	for _, f := range topicMutableFields {
		raw, ok := in[f.name]
		if !ok || raw == nil {
			continue
		}
		v, err := f.normalize(raw)
		if err != nil {
			return nil, err
		}
		if v != nil {
			out[f.name] = v
		}
	}
	return out, nil
}

// TopicMaskHasNestedPath reports whether any mask path addresses a field below
// a topic root. Both update handlers use it to tolerate a partial nested object
// (for example a mask of schemaSettings.encoding whose body carries only the
// encoding): ApplyTopicUpdate validates the field merged with the stored value.
func TopicMaskHasNestedPath(paths []string) bool {
	for _, p := range paths {
		if strings.Contains(p, ".") {
			return true
		}
	}
	return false
}

// ApplyTopicUpdate applies an AIP-134 update mask to a stored topic map. `in` is
// the request topic body in protobuf-JSON form (camelCase keys, enum names,
// "600s" durations) and maskPaths is the update mask. Each path's root must be
// a mutable topic field; an unknown root is an InvalidArgument, never a silent
// ignore.
//
// Resolution is generic over the field tree. A root path replaces the whole
// field, validated through the field table. A nested path merges into the stored
// object and replaces just that leaf; a segment that lands on a repeated field
// replaces the whole list. A field that is absent or empty under its mask is
// cleared: the key is removed and parents left empty are pruned. Clearing
// messageStoragePolicy means "no constraint" here; real GCP falls back to the
// project/organization policy, which the emulator does not model.
//
// The stored value is always re-validated after the merge, so a nested mask
// cannot write a value the create path would reject. A kmsKeyName change only
// affects later publishes (messages already carry their per-message DEK key).
func ApplyTopicUpdate(meta map[string]any, in map[string]any, maskPaths []string) error {
	for _, path := range maskPaths {
		if err := applyTopicUpdatePath(meta, in, path); err != nil {
			return err
		}
	}
	return nil
}

// applyTopicUpdatePath applies one mask path. The root segment is matched
// against the mutable field table (case/underscore insensitive); the remaining
// segments are resolved against the request body and merged into the stored
// value.
func applyTopicUpdatePath(meta, in map[string]any, rawPath string) error {
	segments := strings.Split(rawPath, ".")
	if segments[0] == "" {
		return invalidArgument("unsupported update_mask path: " + rawPath)
	}
	field, ok := topicFieldByName(segments[0])
	if !ok {
		return invalidArgument("unsupported update_mask path: " + rawPath)
	}
	if len(segments) == 1 {
		value, present := lookupTopicKey(in, segments[0])
		return storeTopicField(meta, field, value, present)
	}

	value, present := lookupTopicPath(in, segments)
	// A repeated field has no per-element mask path: the update replaces the
	// whole list with the list the request carried (AIP-161 repeated-field
	// semantics). The nested segments only identify which list to replace.
	if list, repeated := meta[field.name].([]any); repeated {
		// The nested segment names an element member; an unknown member is a
		// bad mask path, not a whole-list replacement.
		if len(segments) > 1 && !repeatedElementHasKey(list, segments[1]) {
			return invalidArgument("unsupported update_mask path: " + rawPath)
		}
		return storeTopicField(meta, field, value, present)
	}
	existing := topicMapValue(meta[field.name])
	merged := cloneTopicMap(existing)
	if !present || isEmptyTopicValue(value) {
		deleteTopicPath(merged, segments[1:])
	} else {
		setTopicPath(merged, segments[1:], value)
	}
	if len(merged) == 0 {
		delete(meta, field.name)
		return nil
	}
	normalized, err := field.normalize(merged)
	if err != nil {
		return err
	}
	if normalized == nil {
		delete(meta, field.name)
		return nil
	}
	meta[field.name] = normalized
	return nil
}

// storeTopicField replaces (or clears) a whole root field.
func storeTopicField(meta map[string]any, field topicField, value any, present bool) error {
	if !present || isEmptyTopicValue(value) {
		delete(meta, field.name)
		return nil
	}
	normalized, err := field.normalize(value)
	if err != nil {
		return err
	}
	if normalized == nil {
		delete(meta, field.name)
		return nil
	}
	meta[field.name] = normalized
	return nil
}

// topicFieldByName resolves a mask segment to a mutable field. Field names are
// compared with normalizeMaskSegment, so message_storage_policy and
// messageStoragePolicy both name the same field.
func topicFieldByName(segment string) (topicField, bool) {
	want := normalizeMaskSegment(segment)
	for _, f := range topicMutableFields {
		if normalizeMaskSegment(f.name) == want {
			return f, true
		}
	}
	return topicField{}, false
}

// normalizeMaskSegment folds a mask-path segment to a case/underscore-insensitive
// form, matching the subscription handler's normalizeMaskPath.
func normalizeMaskSegment(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

// lookupTopicKey returns the value stored under a key whose spelling folds to
// segment, so a camelCase body resolves against a snake_case mask (and back).
func lookupTopicKey(m map[string]any, segment string) (any, bool) {
	want := normalizeMaskSegment(segment)
	for k, v := range m {
		if normalizeMaskSegment(k) == want {
			return v, true
		}
	}
	return nil, false
}

// lookupTopicPath resolves a nested mask path's value from the request body.
// A segment that lands on a repeated field yields the whole list: repeated
// fields are always replaced wholesale, never indexed by a mask path.
func lookupTopicPath(in map[string]any, segments []string) (any, bool) {
	var cur any = in
	for _, seg := range segments {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok := lookupTopicKey(m, seg)
		if !ok {
			return nil, false
		}
		cur = v
		if _, isList := cur.([]any); isList {
			return cur, true
		}
	}
	return cur, true
}

// repeatedElementHasKey reports whether any element of a stored repeated field
// carries the named member (case/underscore insensitive), so a mask path naming
// an element member can be told apart from an unknown path.
func repeatedElementHasKey(list []any, segment string) bool {
	want := normalizeMaskSegment(segment)
	for _, el := range list {
		m, ok := el.(map[string]any)
		if !ok {
			continue
		}
		for k := range m {
			if normalizeMaskSegment(k) == want {
				return true
			}
		}
	}
	return false
}

// setTopicPath writes value at a nested path, reusing the object's existing key
// spellings. Intermediate objects are created as needed.
func setTopicPath(obj map[string]any, segments []string, value any) {
	key := topicKeyFor(obj, segments[0])
	if len(segments) == 1 {
		obj[key] = value
		return
	}
	child, _ := obj[key].(map[string]any)
	if child == nil {
		child = map[string]any{}
	}
	obj[key] = child
	setTopicPath(child, segments[1:], value)
}

// deleteTopicPath removes a nested leaf and prunes any parent object left empty.
func deleteTopicPath(obj map[string]any, segments []string) {
	key := topicKeyFor(obj, segments[0])
	if _, ok := obj[key]; !ok {
		return
	}
	if len(segments) == 1 {
		delete(obj, key)
		return
	}
	child, ok := obj[key].(map[string]any)
	if !ok {
		return
	}
	deleteTopicPath(child, segments[1:])
	if len(child) == 0 {
		delete(obj, key)
	}
}

// topicKeyFor returns the object's existing spelling of segment, or the segment
// itself when the key is not present yet.
func topicKeyFor(obj map[string]any, segment string) string {
	want := normalizeMaskSegment(segment)
	for k := range obj {
		if normalizeMaskSegment(k) == want {
			return k
		}
	}
	return segment
}

// isEmptyTopicValue reports whether a masked value means "clear the field":
// absent (handled by the caller's present flag), null, an empty string, or an
// empty object/list. A false boolean is a real value, not an empty one.
func isEmptyTopicValue(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return v == ""
	case map[string]any:
		return len(v) == 0
	case []any:
		return len(v) == 0
	}
	return false
}

// topicMapValue coerces a stored object field to the plain JSON object shape.
// Records loaded from the store are already map[string]any; NormalizeTopicInput
// returns map[string]string for labels, so the unit-level caller can pass either.
func topicMapValue(v any) map[string]any {
	switch m := v.(type) {
	case map[string]any:
		return m
	case map[string]string:
		out := make(map[string]any, len(m))
		for k, item := range m {
			out[k] = item
		}
		return out
	}
	return nil
}

// cloneTopicMap deep-copies a stored field object so a nested update never
// mutates the live map before validation succeeds.
func cloneTopicMap(src map[string]any) map[string]any {
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = cloneTopicValue(v)
	}
	return out
}

func cloneTopicValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneTopicMap(t)
	case map[string]string:
		out := make(map[string]any, len(t))
		for k, item := range t {
			out[k] = item
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = cloneTopicValue(item)
		}
		return out
	case []string:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = item
		}
		return out
	}
	return v
}

// normalizeTopicLabels stores labels as a string map. An empty map leaves no
// key, so GET never echoes "labels": {} or "labels": null.
func normalizeTopicLabels(value any) (any, error) {
	m, ok := value.(map[string]any)
	if !ok {
		return nil, invalidArgument("labels must be an object of string values")
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		s, ok := v.(string)
		if !ok {
			return nil, invalidArgument(fmt.Sprintf("labels[%q] must be a string", k))
		}
		out[k] = s
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// normalizeTopicRetention validates the 10-minute–31-day window and stores the
// canonical google-duration form ("600s"), which is what the official REST
// client unmarshals as a google.protobuf.Duration.
func normalizeTopicRetention(value any) (any, error) {
	s, ok := value.(string)
	if !ok {
		return nil, invalidArgument("messageRetentionDuration must be a duration string like \"600s\"")
	}
	if s == "" {
		return nil, nil
	}
	d, err := parseTopicDuration(s)
	if err != nil {
		return nil, invalidArgument(fmt.Sprintf("messageRetentionDuration %q is not a duration string like \"600s\"", s))
	}
	if d < minTopicRetention || d > maxTopicRetention {
		return nil, invalidArgument(fmt.Sprintf(
			"messageRetentionDuration must be between 10 minutes (600s) and 31 days (2678400s), got %q", s))
	}
	canonical, err := formatTopicDuration(d)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

// normalizeTopicKMSKeyName stores the CMEK key after a format check. Existence
// and grants are not checked: the emulator has no Pub/Sub service agent, so a
// stricter check would reject keys a real project can use. A bad key still
// fails at publish time, when the envelope encryptor wraps the DEK.
func normalizeTopicKMSKeyName(value any) (any, error) {
	s, ok := value.(string)
	if !ok {
		return nil, invalidArgument("kmsKeyName must be a string")
	}
	if s == "" {
		return nil, nil
	}
	if !validTopicKMSKeyName(s) {
		return nil, invalidArgument(fmt.Sprintf(
			"kmsKeyName %q must be a CryptoKey resource name (projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key})", s))
	}
	return s, nil
}

// normalizeTopicStoragePolicy validates the policy's shape: a non-empty list of
// persisted regions plus the in-transit flag. An empty list is not a valid
// configuration (Discovery): it would allow no region at all.
func normalizeTopicStoragePolicy(value any) (any, error) {
	m, ok := value.(map[string]any)
	if !ok {
		return nil, invalidArgument("messageStoragePolicy must be an object")
	}
	raw, ok := m["allowedPersistenceRegions"]
	if !ok || raw == nil {
		return nil, invalidArgument("messageStoragePolicy.allowedPersistenceRegions must be set")
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, invalidArgument("messageStoragePolicy.allowedPersistenceRegions must be a list of region strings")
	}
	if len(list) == 0 {
		return nil, invalidArgument("messageStoragePolicy.allowedPersistenceRegions must contain at least one region")
	}
	regions := make([]string, 0, len(list))
	for i, item := range list {
		s, ok := item.(string)
		if !ok || s == "" {
			return nil, invalidArgument(fmt.Sprintf(
				"messageStoragePolicy.allowedPersistenceRegions[%d] must be a non-empty region string", i))
		}
		regions = append(regions, s)
	}
	out := map[string]any{"allowedPersistenceRegions": regions}
	if b, ok := m["enforceInTransit"]; ok {
		v, ok := b.(bool)
		if !ok {
			return nil, invalidArgument("messageStoragePolicy.enforceInTransit must be a boolean")
		}
		if v {
			out["enforceInTransit"] = true
		}
	}
	return out, nil
}

// normalizeTopicSchemaSettings validates the schema name's format and the
// encoding enum, and strips the output-only revision bounds. Schema existence is
// not checked: the emulator serves no `projects.schemas` API to consult.
func normalizeTopicSchemaSettings(value any) (any, error) {
	m, ok := value.(map[string]any)
	if !ok {
		return nil, invalidArgument("schemaSettings must be an object")
	}
	schema, _ := m["schema"].(string)
	if schema == "" {
		return nil, invalidArgument("schemaSettings.schema is required")
	}
	if !validTopicSchemaName(schema) {
		return nil, invalidArgument(fmt.Sprintf(
			"schemaSettings.schema %q must be a Schema resource name (projects/{project}/schemas/{schema})", schema))
	}
	out := map[string]any{"schema": schema}
	if raw, ok := m["encoding"]; ok {
		enc, ok := raw.(string)
		if !ok || !contains(topicSchemaEncodings, enc) {
			return nil, invalidArgument(fmt.Sprintf(
				"schemaSettings.encoding %v must be one of %s", raw, strings.Join(topicSchemaEncodings, ", ")))
		}
		// ENCODING_UNSPECIFIED is the proto3 default: storing it would make GET
		// echo a value the API omits.
		if enc != "ENCODING_UNSPECIFIED" {
			out["encoding"] = enc
		}
	}
	return out, nil
}

// normalizeTopicIngestionDataSourceSettings stores exactly one ingestion source
// (the `source` oneof) plus optional platform-log settings, dropping each
// source's output-only `state`. Ingestion is stored and echoed, never polled.
func normalizeTopicIngestionDataSourceSettings(value any) (any, error) {
	m, ok := value.(map[string]any)
	if !ok {
		return nil, invalidArgument("ingestionDataSourceSettings must be an object")
	}
	out := map[string]any{}
	sources := 0
	for _, name := range ingestionSourceFields {
		raw, ok := m[name]
		if !ok || raw == nil {
			continue
		}
		src, ok := raw.(map[string]any)
		if !ok {
			return nil, invalidArgument(fmt.Sprintf("ingestionDataSourceSettings.%s must be an object", name))
		}
		sources++
		stored := make(map[string]any, len(src))
		for k, v := range src {
			if k == "state" {
				continue
			}
			stored[k] = v
		}
		out[name] = stored
	}
	if sources != 1 {
		return nil, invalidArgument(fmt.Sprintf(
			"ingestionDataSourceSettings must set exactly one ingestion source (%s), got %d",
			strings.Join(ingestionSourceFields, ", "), sources))
	}
	if raw, ok := m["platformLogsSettings"]; ok && raw != nil {
		pls, ok := raw.(map[string]any)
		if !ok {
			return nil, invalidArgument("ingestionDataSourceSettings.platformLogsSettings must be an object")
		}
		stored := map[string]any{}
		if rawSev, ok := pls["severity"]; ok {
			sev, ok := rawSev.(string)
			if !ok || !contains(topicPlatformLogLevels, sev) {
				return nil, invalidArgument(fmt.Sprintf(
					"ingestionDataSourceSettings.platformLogsSettings.severity %v must be one of %s",
					rawSev, strings.Join(topicPlatformLogLevels, ", ")))
			}
			if sev != "SEVERITY_UNSPECIFIED" {
				stored["severity"] = sev
			}
		}
		if len(stored) > 0 {
			out["platformLogsSettings"] = stored
		}
	}
	return out, nil
}

// normalizeTopicTransforms validates the transform list: each entry sets exactly
// one member of the `transform` oneof. An empty list stores nothing. Transforms
// are stored and echoed, never executed (no JS runtime or AI inference here).
func normalizeTopicTransforms(value any) (any, error) {
	list, ok := value.([]any)
	if !ok {
		return nil, invalidArgument("messageTransforms must be a list")
	}
	if len(list) == 0 {
		return nil, nil
	}
	out := make([]any, 0, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, invalidArgument(fmt.Sprintf("messageTransforms[%d] must be an object", i))
		}
		entry := map[string]any{}
		set := ""
		for _, variant := range messageTransformVariants {
			raw, ok := m[variant]
			if !ok || raw == nil {
				continue
			}
			if set != "" {
				return nil, invalidArgument(fmt.Sprintf(
					"messageTransforms[%d] must set exactly one of %s, got both %s and %s",
					i, strings.Join(messageTransformVariants, ", "), set, variant))
			}
			obj, ok := raw.(map[string]any)
			if !ok {
				return nil, invalidArgument(fmt.Sprintf("messageTransforms[%d].%s must be an object", i, variant))
			}
			entry[variant] = obj
			set = variant
		}
		if set == "" {
			return nil, invalidArgument(fmt.Sprintf(
				"messageTransforms[%d] must set exactly one of %s", i, strings.Join(messageTransformVariants, ", ")))
		}
		for _, flag := range []string{"enabled", "disabled"} {
			raw, ok := m[flag]
			if !ok {
				continue
			}
			v, ok := raw.(bool)
			if !ok {
				return nil, invalidArgument(fmt.Sprintf("messageTransforms[%d].%s must be a boolean", i, flag))
			}
			if v {
				entry[flag] = true
			}
		}
		out = append(out, entry)
	}
	return out, nil
}

// WithoutStorageConfigState copies a subscription delivery-target config
// (BigQueryConfig or CloudStorageConfig) without its OUTPUT_ONLY `state`
// member. Every write path uses it so a caller can echo a subscription it read
// back without pinning the server-owned delivery state.
func WithoutStorageConfigState(obj map[string]any) map[string]any {
	if _, ok := obj["state"]; !ok {
		return obj
	}
	out := make(map[string]any, len(obj))
	for k, v := range obj {
		if k == "state" {
			continue
		}
		out[k] = v
	}
	return out
}

// validTopicKMSKeyName reports whether s is a Cloud KMS CryptoKey resource name:
// projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}.
func validTopicKMSKeyName(s string) bool {
	parts := strings.Split(s, "/")
	return len(parts) == 8 &&
		parts[0] == "projects" && parts[1] != "" &&
		parts[2] == "locations" && parts[3] != "" &&
		parts[4] == "keyRings" && parts[5] != "" &&
		parts[6] == "cryptoKeys" && parts[7] != ""
}

// validTopicSchemaName reports whether s is a Pub/Sub schema resource name:
// projects/{project}/schemas/{schema}.
func validTopicSchemaName(s string) bool {
	parts := strings.Split(s, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[1] != "" && parts[2] == "schemas" && parts[3] != ""
}

func contains(values []string, v string) bool {
	for _, candidate := range values {
		if candidate == v {
			return true
		}
	}
	return false
}

// parseTopicDuration parses the google.protobuf.Duration JSON form ("600s",
// "600.500s") with the proto runtime rather than time.ParseDuration, which would
// also accept the Go-only "10m". A malformed value is the caller's error to
// report.
func parseTopicDuration(s string) (time.Duration, error) {
	var d durationpb.Duration
	if err := protojson.Unmarshal([]byte(strconv.Quote(s)), &d); err != nil {
		return 0, err
	}
	return d.AsDuration(), nil
}

// formatTopicDuration renders a duration as canonical protobuf JSON ("600s"),
// the form both transports persist.
func formatTopicDuration(d time.Duration) (string, error) {
	raw, err := protojson.Marshal(durationpb.New(d))
	if err != nil {
		return "", internal("cannot encode duration")
	}
	return strings.Trim(string(raw), `"`), nil
}
