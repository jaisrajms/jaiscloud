package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// Cloud Monitoring uptime check configurations
// (google.monitoring.v3.UptimeCheckConfig, UptimeCheckService) are stored and
// echoed declaratively. The emulator has no egress plane: it cannot run GCP's
// global probes, so it does not synthesize probe results. Everything the
// Discovery document declares on the resource is stored verbatim as a
// protojson-shaped document and returned unchanged, with the one server-managed
// field it does assign — `name` — filled in.
//
// The probe-derived surfaces stay loud: uptimeCheckIps (the static checker
// address list) and any checker state the probes would report are answered with
// UNIMPLEMENTED rather than fabricated. That split is the honest contract
// recorded in docs/gcp/wire-contract.md §5 and the fidelity matrix.
//
// The configuration lives in the shared ResourceStore, so it survives the same
// snapshot/--dsn lifecycle as every other control-plane resource, and the REST
// and gRPC transports share one copy of it.
const uptimeCheckConfigResourceType = "gcp_monitoring_uptime_check_config"

// UptimeCheckConfig is one stored uptime check configuration. Document is the
// whole configuration as a protojson-shaped JSON object with the server-managed
// name set; every other field is the client's, echoed verbatim.
type UptimeCheckConfig struct {
	ID          string
	Name        string
	DisplayName string
	Document    json.RawMessage
}

// ListUptimeCheckConfigs returns one page of the project's uptime check
// configurations plus the next-page token. filter supports the documented
// filtering subset (equality on display_name and user_labels.<key>, clauses
// joined by AND); any other key or operator is rejected rather than silently
// matching everything.
func (s *Service) ListUptimeCheckConfigs(ctx context.Context, project, filter string, pageSize int, pageToken string) ([]UptimeCheckConfig, string, error) {
	if err := s.requireResourceStore(); err != nil {
		return nil, "", err
	}
	f, err := compileUptimeCheckFilter(filter)
	if err != nil {
		return nil, "", invalidArgument("invalid filter: " + err.Error())
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, uptimeCheckConfigResourceType, "")
	if err != nil {
		return nil, "", err
	}
	matching := make([]UptimeCheckConfig, 0, len(entries))
	for _, e := range entries {
		c, err := uptimeCheckFromEntry(e)
		if err != nil {
			return nil, "", err
		}
		if f.match(c) {
			matching = append(matching, c)
		}
	}
	page, next, err := paging.Page(matching, func(c UptimeCheckConfig) string { return c.ID }, pageParams(pageSize, pageToken))
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// GetUptimeCheckConfig returns one configuration by id. A missing one is
// NOT_FOUND.
func (s *Service) GetUptimeCheckConfig(ctx context.Context, project, id string) (UptimeCheckConfig, error) {
	if err := s.requireResourceStore(); err != nil {
		return UptimeCheckConfig{}, err
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, uptimeCheckConfigResourceType, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return UptimeCheckConfig{}, uptimeCheckNotFound(id)
		}
		return UptimeCheckConfig{}, err
	}
	return uptimeCheckFromEntry(e)
}

// CreateUptimeCheckConfig stores a new configuration. Per the Discovery
// document the body's name "should be omitted when creating"; a supplied one
// must be "projects/{parent-project}/uptimeCheckConfigs/{id}" (INVALID_ARGUMENT
// otherwise) and its id is honored, matching how real Monitoring honors a
// caller-chosen id in the name. The rest of the document is validated against
// the Discovery schema and stored verbatim.
func (s *Service) CreateUptimeCheckConfig(ctx context.Context, project string, body json.RawMessage) (UptimeCheckConfig, error) {
	if err := s.requireResourceStore(); err != nil {
		return UptimeCheckConfig{}, err
	}
	fields, err := decodeUptimeCheckFields(body)
	if err != nil {
		return UptimeCheckConfig{}, err
	}
	id := uuid.NewString()
	if name, present, err := uptimeCheckRequestName(fields); err != nil {
		return UptimeCheckConfig{}, err
	} else if present && name != "" {
		nameProject, nameID, ok := SplitUptimeCheckConfigName(name)
		if !ok || nameProject != project {
			return UptimeCheckConfig{}, invalidArgument("uptime check config name must be projects/" + project + "/uptimeCheckConfigs/{uptime_check_config_id}")
		}
		id = nameID
	}
	cfg, err := buildUptimeCheckConfig(project, id, fields)
	if err != nil {
		return UptimeCheckConfig{}, err
	}
	err = s.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{
		Type: uptimeCheckConfigResourceType, ID: id, Data: cfg.Document,
	})
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return UptimeCheckConfig{}, model.NewProviderError("AlreadyExists", "uptime check config already exists", 409)
		}
		return UptimeCheckConfig{}, err
	}
	return cfg, nil
}

// UpdateUptimeCheckConfig replaces or field-merges a stored configuration. The
// Discovery document declares the semantics: an empty updateMask "completely
// replaces" the current configuration, a non-empty one updates "only the listed
// fields … with values from the new configuration". Paths are FieldMask paths
// (snake_case from the proto, camelCase from the REST query parameter); a path
// naming no field of the resource is INVALID_ARGUMENT (AIP-134/AIP-161).
//
// The body's name is the path-bound resource name: an absent/empty name is
// filled from the path, a non-empty one that disagrees with it is
// INVALID_ARGUMENT.
func (s *Service) UpdateUptimeCheckConfig(ctx context.Context, project, id string, body json.RawMessage, updateMask []string) (UptimeCheckConfig, error) {
	if err := s.requireResourceStore(); err != nil {
		return UptimeCheckConfig{}, err
	}
	fields, err := decodeUptimeCheckFields(body)
	if err != nil {
		return UptimeCheckConfig{}, err
	}
	pathName := UptimeCheckConfigName(project, id)
	name, _, err := uptimeCheckRequestName(fields)
	if err != nil {
		return UptimeCheckConfig{}, err
	}
	if name != "" && name != pathName {
		return UptimeCheckConfig{}, invalidArgument("uptime check config name must match the request path name " + pathName)
	}
	var result UptimeCheckConfig
	_, err = s.resources.UpsertAtomic(ctx, project, store.GlobalRegion, uptimeCheckConfigResourceType, id,
		func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			if !exists {
				return current, uptimeCheckNotFound(id)
			}
			stored, err := uptimeCheckFromEntry(current)
			if err != nil {
				return current, err
			}
			next := fields
			if len(updateMask) > 0 {
				next, err = applyUptimeCheckMask(stored.Document, fields, updateMask)
				if err != nil {
					return current, err
				}
			}
			cfg, err := buildUptimeCheckConfig(project, id, next)
			if err != nil {
				return current, err
			}
			result = cfg
			return store.ResourceEntry{Type: uptimeCheckConfigResourceType, ID: id, Data: cfg.Document}, nil
		})
	if err != nil {
		return UptimeCheckConfig{}, err
	}
	return result, nil
}

// DeleteUptimeCheckConfig deletes one configuration. A missing one is
// NOT_FOUND. Real Monitoring also refuses the delete while an alert policy
// references the check; the emulator does not inspect references, so a
// referencing alert policy is left as declared.
func (s *Service) DeleteUptimeCheckConfig(ctx context.Context, project, id string) error {
	if err := s.requireResourceStore(); err != nil {
		return err
	}
	err := s.resources.Delete(ctx, project, store.GlobalRegion, uptimeCheckConfigResourceType, id)
	if errors.Is(err, store.ErrNotFound) {
		return uptimeCheckNotFound(id)
	}
	return err
}

// ─── validation ───────────────────────────────────────────────────────────────

// decodeUptimeCheckFields decodes a configuration body into its top-level
// fields while preserving every value as raw JSON, so the check definition is
// stored verbatim and only the fields the Discovery document makes the emulator
// interpret (name, displayName, and the documented constraints) are decoded.
func decodeUptimeCheckFields(body json.RawMessage) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, invalidArgument("uptime check config body is required")
	}
	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(trimmed, &fields); err != nil || fields == nil {
		return nil, invalidArgument("uptime check config body must be a JSON object")
	}
	return fields, nil
}

// uptimeCheckRequestName returns the body's name field, reporting whether it
// was present. A non-string name is INVALID_ARGUMENT.
func uptimeCheckRequestName(fields map[string]json.RawMessage) (string, bool, error) {
	raw, ok := fields["name"]
	if !ok {
		return "", false, nil
	}
	var name string
	if err := json.Unmarshal(raw, &name); err != nil {
		return "", true, invalidArgument("uptime check config name must be a string")
	}
	return name, true, nil
}

// uptimeCheckAllowedPeriods are the only period values the Discovery document
// documents ("Currently, the only supported values are 60s, 300s, 600s, and
// 900s"). A period outside the set is rejected the way real Monitoring rejects
// it, rather than stored as an unhonorable declaration.
var uptimeCheckAllowedPeriods = map[time.Duration]bool{
	60 * time.Second:  true,
	300 * time.Second: true,
	600 * time.Second: true,
	900 * time.Second: true,
}

// validateUptimeCheckFields enforces the constraints the Discovery document
// states on UptimeCheckConfig and its messages. Everything else is stored as
// declared: the emulator never runs the probe, so it has nothing to validate the
// check definition against.
func validateUptimeCheckFields(fields map[string]json.RawMessage) error {
	for key := range fields {
		if _, ok := uptimeCheckConfigFields[key]; !ok {
			return invalidArgument(fmt.Sprintf("unknown field %q in uptime check config", key))
		}
	}
	// displayName: "A human-friendly name … Required."
	raw, ok := fields["displayName"]
	if !ok {
		return invalidArgument("uptime check config display_name is required")
	}
	var displayName string
	if err := json.Unmarshal(raw, &displayName); err != nil {
		return invalidArgument("uptime check config display_name must be a string")
	}
	if displayName == "" {
		return invalidArgument("uptime check config display_name is required")
	}
	// resource: "The resource the check is checking. Required." — exactly one
	// member of the oneof may be set.
	set := 0
	for _, key := range []string{"monitoredResource", "resourceGroup", "syntheticMonitor"} {
		if _, ok := fields[key]; ok {
			set++
		}
	}
	if set != 1 {
		return invalidArgument("uptime check config must set exactly one of monitored_resource, resource_group, or synthetic_monitor")
	}
	// timeout: "The maximum amount of time to wait for the request to complete
	// (must be between 1 and 60 seconds). Required."
	timeout, ok, err := uptimeCheckDuration(fields, "timeout")
	if err != nil {
		return err
	}
	if !ok {
		return invalidArgument("uptime check config timeout is required")
	}
	if timeout < time.Second || timeout > 60*time.Second {
		return invalidArgument("uptime check config timeout must be between 1 and 60 seconds")
	}
	// period: "Optional, defaults to 60s" over a fixed allowed set.
	period, ok, err := uptimeCheckDuration(fields, "period")
	if err != nil {
		return err
	}
	if ok && !uptimeCheckAllowedPeriods[period] {
		return invalidArgument("uptime check config period must be one of 60s, 300s, 600s, or 900s")
	}
	// httpCheck: the documented content_type/request_method consistency rules.
	return validateUptimeCheckHTTP(fields)
}

// validateUptimeCheckHTTP enforces the HttpCheck rules the Discovery document
// states as errors: a content type on a GET (or unspecified) request method, a
// POST without one, and a custom content type that does not match the declared
// content type.
func validateUptimeCheckHTTP(fields map[string]json.RawMessage) error {
	raw, ok := fields["httpCheck"]
	if !ok {
		return nil
	}
	http := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &http); err != nil || http == nil {
		return invalidArgument("uptime check config http_check must be a JSON object")
	}
	method := stringFieldOf(http, "requestMethod")
	contentType := stringFieldOf(http, "contentType")
	custom := stringFieldOf(http, "customContentType")
	switch {
	case method == "POST" && contentType == "":
		return invalidArgument("uptime check config http_check content_type is required for a POST request")
	case method != "POST" && contentType != "":
		return invalidArgument("uptime check config http_check content_type requires request_method POST")
	case contentType == "URL_ENCODED" && custom != "":
		return invalidArgument("uptime check config http_check custom_content_type requires content_type USER_PROVIDED")
	case contentType == "USER_PROVIDED" && custom == "":
		return invalidArgument("uptime check config http_check custom_content_type is required for content_type USER_PROVIDED")
	}
	return nil
}

// stringFieldOf returns a decoded string field, or "" when absent or not a
// string (the latter is caught by the protojson-shaped schema check in the
// transports).
func stringFieldOf(fields map[string]json.RawMessage, key string) string {
	raw, ok := fields[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

// uptimeCheckDuration decodes a google.protobuf.Duration field ("10s",
// "1.500s") and reports whether it was present.
func uptimeCheckDuration(fields map[string]json.RawMessage, key string) (time.Duration, bool, error) {
	raw, ok := fields[key]
	if !ok {
		return 0, false, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, true, invalidArgument("uptime check config " + key + " must be a duration string")
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, true, invalidArgument("uptime check config " + key + " is not a valid duration")
	}
	return d, true, nil
}

// ─── update_mask ──────────────────────────────────────────────────────────────

// configField is one field of a Discovery message in the mask tree: jsonName is
// the canonical JSON spelling the document stores, and sub is the field set of
// a singular message field. A repeated, map, or scalar field carries no sub —
// a protobuf FieldMask cannot address inside one, so a path descending into it
// names no field.
type configField struct {
	jsonName string
	sub      map[string]*configField
}

func scalarField(jsonName string) *configField { return &configField{jsonName: jsonName} }

func messageField(jsonName string, sub map[string]*configField) *configField {
	return &configField{jsonName: jsonName, sub: sub}
}

func fieldTree(fields ...*configField) map[string]*configField {
	tree := make(map[string]*configField, len(fields))
	for _, f := range fields {
		tree[f.jsonName] = f
	}
	return tree
}

// uptimeCheckConfigFields is the Discovery schema of UptimeCheckConfig
// (google.monitoring.v3.UptimeCheckConfig). It is the field set an update_mask
// path is validated against.
var uptimeCheckConfigFields = fieldTree(
	scalarField("name"),
	scalarField("displayName"),
	messageField("monitoredResource", fieldTree(
		scalarField("type"),
		scalarField("labels"),
	)),
	messageField("resourceGroup", fieldTree(
		scalarField("groupId"),
		scalarField("resourceType"),
	)),
	messageField("syntheticMonitor", fieldTree(
		messageField("cloudFunctionV2", fieldTree(
			scalarField("name"),
		)),
	)),
	messageField("httpCheck", fieldTree(
		scalarField("requestMethod"),
		scalarField("useSsl"),
		scalarField("path"),
		scalarField("port"),
		scalarField("authInfo"),
		scalarField("maskHeaders"),
		scalarField("headers"),
		scalarField("contentType"),
		scalarField("customContentType"),
		scalarField("validateSsl"),
		scalarField("body"),
		scalarField("acceptedResponseStatusCodes"),
		messageField("pingConfig", fieldTree(scalarField("pingsCount"))),
		messageField("serviceAgentAuthentication", fieldTree(scalarField("type"))),
	)),
	messageField("tcpCheck", fieldTree(
		scalarField("port"),
		messageField("pingConfig", fieldTree(scalarField("pingsCount"))),
	)),
	scalarField("period"),
	scalarField("timeout"),
	scalarField("contentMatchers"),
	scalarField("checkerType"),
	scalarField("selectedRegions"),
	scalarField("isInternal"),
	scalarField("internalCheckers"),
	scalarField("userLabels"),
	scalarField("disabled"),
	scalarField("logCheckFailures"),
)

// applyUptimeCheckMask merges the incoming document into the stored one
// according to the FieldMask paths, returning the merged top-level fields. Each
// path is resolved against the Discovery schema; a masked field the request omits
// is cleared, which is what "updated with the value from the new configuration"
// means for an unset proto field.
func applyUptimeCheckMask(storedDoc json.RawMessage, incoming map[string]json.RawMessage, updateMask []string) (map[string]json.RawMessage, error) {
	stored := map[string]json.RawMessage{}
	if err := json.Unmarshal(storedDoc, &stored); err != nil || stored == nil {
		return nil, model.NewProviderError("Internal", "stored uptime check config is not a JSON object", 500)
	}
	for _, raw := range updateMask {
		path := strings.TrimSpace(raw)
		if path == "" {
			continue
		}
		if err := mergeUptimeCheckPath(stored, incoming, uptimeCheckConfigFields, strings.Split(path, "."), path); err != nil {
			return nil, err
		}
	}
	return stored, nil
}

// mergeUptimeCheckPath applies one FieldMask path to the stored document. It
// descends the schema tree with the path and copies the incoming value at the
// leaf, clearing the stored value when the request omitted it.
func mergeUptimeCheckPath(stored, incoming map[string]json.RawMessage, fields map[string]*configField, segments []string, path string) error {
	f, ok := resolveConfigField(fields, segments[0])
	if !ok {
		return invalidMaskPath(path)
	}
	if len(segments) == 1 {
		if v, present := incoming[f.jsonName]; present {
			stored[f.jsonName] = v
		} else {
			delete(stored, f.jsonName)
		}
		return nil
	}
	if f.sub == nil {
		return invalidMaskPath(path)
	}
	// Validate the whole path against the schema before touching the document, so
	// a path that names no field is rejected even when the request omits the
	// message it descends into.
	if _, err := resolveConfigPath(f.sub, segments[1:], path); err != nil {
		return err
	}
	storedSub := map[string]json.RawMessage{}
	if raw, present := stored[f.jsonName]; present {
		if err := json.Unmarshal(raw, &storedSub); err != nil || storedSub == nil {
			storedSub = map[string]json.RawMessage{}
		}
	}
	incomingSub := map[string]json.RawMessage{}
	if raw, present := incoming[f.jsonName]; present {
		if err := json.Unmarshal(raw, &incomingSub); err != nil || incomingSub == nil {
			// A masked submessage that is not an object cannot be merged into.
			return invalidArgument("uptime check config " + f.jsonName + " must be a JSON object")
		}
	} else {
		// The masked submessage is absent from the request, so every masked path
		// inside it is cleared with it.
		delete(stored, f.jsonName)
		return nil
	}
	if err := mergeUptimeCheckPath(storedSub, incomingSub, f.sub, segments[1:], path); err != nil {
		return err
	}
	if len(storedSub) == 0 {
		delete(stored, f.jsonName)
		return nil
	}
	raw, err := json.Marshal(storedSub)
	if err != nil {
		return model.NewProviderError("Internal", err.Error(), 500)
	}
	stored[f.jsonName] = raw
	return nil
}

// resolveConfigPath walks the remaining FieldMask segments against the schema,
// returning the leaf field. It validates a whole path up front so a path naming
// no field is rejected before any document mutation.
func resolveConfigPath(fields map[string]*configField, segments []string, path string) (*configField, error) {
	f, ok := resolveConfigField(fields, segments[0])
	if !ok {
		return nil, invalidMaskPath(path)
	}
	if len(segments) > 1 {
		if f.sub == nil {
			return nil, invalidMaskPath(path)
		}
		return resolveConfigPath(f.sub, segments[1:], path)
	}
	return f, nil
}

// resolveConfigField resolves one FieldMask path segment against a message's
// field set. The gRPC FieldMask carries proto names (snake_case) and the REST
// FieldMask query parameter carries the JSON names, so both spellings resolve to
// the same field.
func resolveConfigField(fields map[string]*configField, segment string) (*configField, bool) {
	if f, ok := fields[segment]; ok {
		return f, true
	}
	if camel := snakeToCamel(segment); camel != segment {
		if f, ok := fields[camel]; ok {
			return f, true
		}
	}
	return nil, false
}

// snakeToCamel converts a proto field name to its JSON spelling
// ("display_name" → "displayName").
func snakeToCamel(s string) string {
	if !strings.Contains(s, "_") {
		return s
	}
	var b strings.Builder
	upper := false
	for _, r := range s {
		if r == '_' {
			upper = true
			continue
		}
		if upper {
			b.WriteString(strings.ToUpper(string(r)))
			upper = false
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// ─── documents ────────────────────────────────────────────────────────────────

// buildUptimeCheckConfig validates the requested fields and assembles the stored
// document: the client's configuration verbatim with the server-assigned name.
func buildUptimeCheckConfig(project, id string, fields map[string]json.RawMessage) (UptimeCheckConfig, error) {
	if err := validateUptimeCheckFields(fields); err != nil {
		return UptimeCheckConfig{}, err
	}
	name := UptimeCheckConfigName(project, id)
	nameRaw, err := json.Marshal(name)
	if err != nil {
		return UptimeCheckConfig{}, err
	}
	// Copy so the caller's map is not mutated.
	doc := make(map[string]json.RawMessage, len(fields)+1)
	for k, v := range fields {
		doc[k] = v
	}
	doc["name"] = nameRaw
	data, err := json.Marshal(doc)
	if err != nil {
		return UptimeCheckConfig{}, model.NewProviderError("Internal", err.Error(), 500)
	}
	return UptimeCheckConfig{
		ID:          id,
		Name:        name,
		DisplayName: stringFieldOf(fields, "displayName"),
		Document:    data,
	}, nil
}

// uptimeCheckFromEntry decodes a stored configuration entry.
func uptimeCheckFromEntry(e store.ResourceEntry) (UptimeCheckConfig, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &fields); err != nil || fields == nil {
		return UptimeCheckConfig{}, model.NewProviderError("Internal", "stored uptime check config is not a JSON object", 500)
	}
	name, _, err := uptimeCheckRequestName(fields)
	if err != nil {
		return UptimeCheckConfig{}, model.NewProviderError("Internal", "stored uptime check config has an invalid name", 500)
	}
	if name == "" {
		name = UptimeCheckConfigName(e.Account, e.ID)
	}
	return UptimeCheckConfig{
		ID:          e.ID,
		Name:        name,
		DisplayName: stringFieldOf(fields, "displayName"),
		Document:    e.Data,
	}, nil
}

// uptimeCheckNotFound is the canonical NOT_FOUND error for a configuration.
func uptimeCheckNotFound(id string) error {
	return notFound("uptime check config not found: " + id)
}

// ─── list filter ──────────────────────────────────────────────────────────────

// uptimeCheckFilter is the documented ListUptimeCheckConfigs filtering subset:
// a conjunction of equality predicates over display_name and user_labels.<key>.
type uptimeCheckFilter struct {
	displayName string
	userLabels  map[string]string
}

// compileUptimeCheckFilter parses the filter subset; an empty filter matches
// everything. Any other key, operator, or malformed clause fails closed.
func compileUptimeCheckFilter(filter string) (uptimeCheckFilter, error) {
	f := uptimeCheckFilter{}
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return f, nil
	}
	for _, raw := range strings.Split(filter, " AND ") {
		clause := strings.TrimSpace(raw)
		if clause == "" {
			return uptimeCheckFilter{}, fmt.Errorf("empty filter clause")
		}
		key, val, ok := ParseEquality(clause)
		if !ok {
			return uptimeCheckFilter{}, fmt.Errorf("unsupported filter clause %q (only key = \"value\" is supported)", clause)
		}
		switch {
		case key == "display_name":
			f.displayName = val
		case strings.HasPrefix(key, "user_labels."):
			lk := strings.TrimPrefix(key, "user_labels.")
			if lk == "" {
				return uptimeCheckFilter{}, fmt.Errorf("unsupported filter key %q", key)
			}
			if f.userLabels == nil {
				f.userLabels = map[string]string{}
			}
			f.userLabels[lk] = val
		default:
			return uptimeCheckFilter{}, fmt.Errorf("unsupported filter key %q (only display_name and user_labels.<key> are supported)", key)
		}
	}
	return f, nil
}

// match reports whether a configuration satisfies the filter.
func (f uptimeCheckFilter) match(c UptimeCheckConfig) bool {
	if f.displayName != "" && c.DisplayName != f.displayName {
		return false
	}
	if len(f.userLabels) == 0 {
		return true
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(c.Document, &fields); err != nil {
		return false
	}
	var labels map[string]string
	if raw, ok := fields["userLabels"]; ok {
		if err := json.Unmarshal(raw, &labels); err != nil {
			return false
		}
	}
	for k, want := range f.userLabels {
		if got, ok := labels[k]; !ok || got != want {
			return false
		}
	}
	return true
}

// parseMaskPaths splits the REST updateMask query parameter: the transcoder
// accepts either a single comma-separated FieldMask or repeated parameters, and
// the gRPC transport passes the FieldMask paths through directly.
func ParseMaskPaths(v any) []string {
	switch x := v.(type) {
	case string:
		return splitMask(x)
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			if s, ok := item.(string); ok {
				out = append(out, splitMask(s)...)
			}
		}
		return out
	case []string:
		out := make([]string, 0, len(x))
		for _, s := range x {
			out = append(out, splitMask(s)...)
		}
		return out
	}
	return nil
}

func splitMask(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
