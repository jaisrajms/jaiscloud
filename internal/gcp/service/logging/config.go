package logging

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"

	loggingstore "jaiscloud/internal/gcp/store/logging"
)

// This file adds the Cloud Logging config plane to the transport-neutral core:
// sinks (export routes) and resource-level exclusions, plus the write-path
// routing evaluation. It follows the same shape as the entry surface: the
// REST and gRPC transports transcode their wire form into these typed
// methods, so both transports share one implementation and one store.
//
// Resource names follow the Logging v2 form:
//
//	{scope}/{scopeID}/sinks/{SINK_ID}
//	{scope}/{scopeID}/exclusions/{EXCLUSION_ID}
//
// where the scope is projects, organizations, folders, or billingAccounts.
// On the wire the sink id is the short client-assigned identifier (and the
// REST response additionally carries the full path as resourceName); the store
// keys sinks/exclusions by their short id within the canonical scope parent.

// ─── resource names ───────────────────────────────────────────────────────────

// SplitConfigName parses a Logging config resource name of the form
// {scope}/{scopeID}/{collection}/{id} and returns the canonical scope parent,
// the collection, and the decoded id. collection is "sinks" or "exclusions".
func SplitConfigName(name string) (scopeParent, collection, id string, err error) {
	trimmed := strings.TrimPrefix(name, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 4 || parts[1] == "" || parts[3] == "" {
		return "", "", "", invalidParent(name)
	}
	if _, ok := logScopes[parts[0]]; !ok {
		return "", "", "", invalidParent(name)
	}
	switch parts[2] {
	case "sinks", "exclusions":
	default:
		return "", "", "", invalidParent(name)
	}
	return parts[0] + "/" + parts[1], parts[2], parts[3], nil
}

// ParseSinkName parses {scope}/{scopeID}/sinks/{SINK_ID} into the canonical
// scope parent and the short sink id.
func ParseSinkName(name string) (scopeParent, id string, err error) {
	scope, collection, id, err := SplitConfigName(name)
	if err != nil {
		return "", "", err
	}
	if collection != "sinks" {
		return "", "", invalidParent(name)
	}
	return scope, id, nil
}

// ParseExclusionName parses {scope}/{scopeID}/exclusions/{EXCLUSION_ID} into the
// canonical scope parent and the short exclusion id.
func ParseExclusionName(name string) (scopeParent, id string, err error) {
	scope, collection, id, err := SplitConfigName(name)
	if err != nil {
		return "", "", err
	}
	if collection != "exclusions" {
		return "", "", invalidParent(name)
	}
	return scope, id, nil
}

// SinkResourceName builds the full sink resource name from a scope parent and
// short id.
func SinkResourceName(scopeParent, id string) string { return scopeParent + "/sinks/" + id }

// ExclusionResourceName builds the full exclusion resource name from a scope
// parent and short id.
func ExclusionResourceName(scopeParent, id string) string {
	return scopeParent + "/exclusions/" + id
}

// validConfigID reports whether id is a legal Logging sink/exclusion identifier:
// 1–100 characters from [A-Za-z0-9_.-] with an alphanumeric first character.
func validConfigID(id string) bool {
	if id == "" || len(id) > 100 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_' || c == '-' || c == '.':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// sharedWriterIdentity is the sink writer identity used when the caller does
// not request a unique one: a stable Google-managed service account derived
// from the scope. Real Cloud Logging returns a fixed service agent here.
func sharedWriterIdentity(scopeParent string) string {
	return "serviceAccount:service-" + sanitizeIdentity(scopeParent) + "@gcp-sa-logging.iam.gserviceaccount.com"
}

// uniqueWriterIdentityFor derives a deterministic per-sink writer identity,
// used when unique_writer_identity is set (mirrors the distinct identity real
// Logging returns for a unique-writer sink).
func uniqueWriterIdentityFor(scopeParent, sinkID string) string {
	return "serviceAccount:service-" + sanitizeIdentity(scopeParent) + "-" + sanitizeIdentity(sinkID) + "@gcp-sa-logging.iam.gserviceaccount.com"
}

// sinkWriterIdentity resolves the writer identity for a create/update: an
// explicit custom_writer_identity wins, then unique_writer_identity, then the
// shared identity.
func sinkWriterIdentity(scopeParent, sinkID string, unique bool, custom string) string {
	switch {
	case custom != "":
		return custom
	case unique:
		return uniqueWriterIdentityFor(scopeParent, sinkID)
	default:
		return sharedWriterIdentity(scopeParent)
	}
}

// sanitizeIdentity reduces a resource path to the [a-z0-9-] characters legal in
// a synthesized service-account email local part.
func sanitizeIdentity(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "emulator"
	}
	return out
}

// ─── errors ───────────────────────────────────────────────────────────────────

func notFound(msg string) error {
	return model.NewProviderError("NotFound", msg, 404)
}

func alreadyExists(msg string) error {
	return model.NewProviderError("AlreadyExists", msg, 409)
}

func mapConfigStoreError(err error) error {
	switch {
	case errors.Is(err, loggingstore.ErrSinkNotFound):
		return notFound("sink not found")
	case errors.Is(err, loggingstore.ErrSinkExists):
		return alreadyExists("sink already exists")
	case errors.Is(err, loggingstore.ErrExclusionNotFound):
		return notFound("exclusion not found")
	case errors.Is(err, loggingstore.ErrExclusionExists):
		return alreadyExists("exclusion already exists")
	case errors.Is(err, loggingstore.ErrMetricNotFound):
		return notFound("metric not found")
	case errors.Is(err, loggingstore.ErrMetricExists):
		return alreadyExists("metric already exists")
	}
	return err
}

// validateSink checks the writable sink fields and validates the sink's
// filters with the shared filter engine (so an unsupported filter fails loud at
// write time rather than silently routing nothing).
func validateSink(in loggingstore.LogSink) error {
	if in.Destination == "" {
		return invalidArgument("sink destination is required")
	}
	if _, err := CompileFilter(in.Filter); err != nil {
		return invalidArgument("invalid sink filter: " + err.Error())
	}
	for _, ex := range in.Exclusions {
		if err := validateExclusion(ex); err != nil {
			return err
		}
	}
	return nil
}

func validateExclusion(in loggingstore.LogExclusion) error {
	if in.Filter == "" {
		return invalidArgument("exclusion filter is required")
	}
	if _, err := CompileFilter(in.Filter); err != nil {
		return invalidArgument("invalid exclusion filter: " + err.Error())
	}
	return nil
}

// ─── sinks ────────────────────────────────────────────────────────────────────

// CreateSink adds a sink under parent. in.Name is the short client-assigned id;
// when empty the create request is rejected (real Logging requires it). The
// returned sink carries the server-assigned timestamps and writer identity.
func (s *Service) CreateSink(ctx context.Context, parent string, in loggingstore.LogSink, uniqueWriterIdentity bool, customWriterIdentity string) (loggingstore.LogSink, error) {
	scope, err := ParseScopeParent(parent)
	if err != nil {
		return loggingstore.LogSink{}, err
	}
	if !validConfigID(in.Name) {
		return loggingstore.LogSink{}, invalidArgument("invalid sink name: " + in.Name)
	}
	if err := validateSink(in); err != nil {
		return loggingstore.LogSink{}, err
	}
	now := clock.Now()
	in.WriterIdentity = sinkWriterIdentity(scope, in.Name, uniqueWriterIdentity, customWriterIdentity)
	in.CreateTime = now
	in.UpdateTime = now
	if err := s.store.CreateSink(ctx, scope, in); err != nil {
		return loggingstore.LogSink{}, mapConfigStoreError(err)
	}
	return in, nil
}

// GetSink fetches a sink by full resource name.
func (s *Service) GetSink(ctx context.Context, name string) (loggingstore.LogSink, error) {
	scope, id, err := ParseSinkName(name)
	if err != nil {
		return loggingstore.LogSink{}, err
	}
	sink, err := s.store.GetSink(ctx, scope, id)
	if err != nil {
		return loggingstore.LogSink{}, mapConfigStoreError(err)
	}
	return sink, nil
}

// ListSinks returns a page of the parent's sinks ordered by name.
func (s *Service) ListSinks(ctx context.Context, parent string, pageSize int, pageToken string) ([]loggingstore.LogSink, string, error) {
	scope, err := ParseScopeParent(parent)
	if err != nil {
		return nil, "", err
	}
	sinks, err := s.store.ListSinks(ctx, scope)
	if err != nil {
		return nil, "", err
	}
	page, next, err := paginateConfig(sinks, pageSize, pageToken)
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// UpdateSink merges a sink by full resource name. A non-empty mask merges the
// named fields; an empty mask uses the proto's documented backward-compatible
// default `destination,filter,includeChildren` (output-only fields and the
// sink's description/disabled/inline exclusions are preserved). The name is
// immutable. customWriterIdentity wins over uniqueWriterIdentity, which selects
// a per-sink identity.
func (s *Service) UpdateSink(ctx context.Context, name string, in loggingstore.LogSink, updateMask []string, uniqueWriterIdentity bool, customWriterIdentity string) (loggingstore.LogSink, error) {
	scope, id, err := ParseSinkName(name)
	if err != nil {
		return loggingstore.LogSink{}, err
	}
	stored, err := s.store.GetSink(ctx, scope, id)
	if err != nil {
		return loggingstore.LogSink{}, mapConfigStoreError(err)
	}
	in.Name = id
	if len(updateMask) == 0 {
		// The proto treats an empty mask as `destination,filter,includeChildren`
		// for backwards compatibility.
		updateMask = []string{"destination", "filter", "includeChildren"}
	}
	merged, merr := applySinkMask(stored, in, updateMask)
	if merr != nil {
		return loggingstore.LogSink{}, merr
	}
	if err := validateSink(merged); err != nil {
		return loggingstore.LogSink{}, err
	}
	stored = merged
	// The writer identity is output-only. An explicit custom identity wins; a
	// requested unique identity replaces the shared one; otherwise preserve the
	// stored value (real Logging leaves writer_identity unchanged when
	// unique_writer_identity is unchanged).
	switch {
	case customWriterIdentity != "":
		stored.WriterIdentity = customWriterIdentity
	case uniqueWriterIdentity:
		stored.WriterIdentity = uniqueWriterIdentityFor(scope, id)
	case stored.WriterIdentity == "":
		stored.WriterIdentity = sharedWriterIdentity(scope)
	}
	stored.UpdateTime = clock.Now()
	if err := s.store.UpdateSink(ctx, scope, stored); err != nil {
		return loggingstore.LogSink{}, mapConfigStoreError(err)
	}
	return stored, nil
}

// DeleteSink removes a sink by full resource name.
func (s *Service) DeleteSink(ctx context.Context, name string) error {
	scope, id, err := ParseSinkName(name)
	if err != nil {
		return err
	}
	return mapConfigStoreError(s.store.DeleteSink(ctx, scope, id))
}

// applySinkMask merges the incoming sink into the stored one for the named
// field paths. Paths are normalized so both proto (snake_case) and REST
// (camelCase) spellings are accepted.
func applySinkMask(stored, incoming loggingstore.LogSink, updateMask []string) (loggingstore.LogSink, error) {
	for _, raw := range updateMask {
		switch normalizeConfigMaskPath(raw) {
		case "destination":
			stored.Destination = incoming.Destination
		case "filter":
			stored.Filter = incoming.Filter
		case "description":
			stored.Description = incoming.Description
		case "disabled":
			stored.Disabled = incoming.Disabled
		case "exclusions":
			stored.Exclusions = incoming.Exclusions
		case "include_children":
			stored.IncludeChildren = incoming.IncludeChildren
		default:
			return stored, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+raw, 501)
		}
	}
	return stored, nil
}

// ─── exclusions ───────────────────────────────────────────────────────────────

// CreateExclusion adds a resource-level exclusion under parent. in.Name is the
// short client-assigned id.
func (s *Service) CreateExclusion(ctx context.Context, parent string, in loggingstore.LogExclusion) (loggingstore.LogExclusion, error) {
	scope, err := ParseScopeParent(parent)
	if err != nil {
		return loggingstore.LogExclusion{}, err
	}
	if !validConfigID(in.Name) {
		return loggingstore.LogExclusion{}, invalidArgument("invalid exclusion name: " + in.Name)
	}
	if err := validateExclusion(in); err != nil {
		return loggingstore.LogExclusion{}, err
	}
	now := clock.Now()
	in.CreateTime = now
	in.UpdateTime = now
	if err := s.store.CreateExclusion(ctx, scope, in); err != nil {
		return loggingstore.LogExclusion{}, mapConfigStoreError(err)
	}
	return in, nil
}

// GetExclusion fetches an exclusion by full resource name.
func (s *Service) GetExclusion(ctx context.Context, name string) (loggingstore.LogExclusion, error) {
	scope, id, err := ParseExclusionName(name)
	if err != nil {
		return loggingstore.LogExclusion{}, err
	}
	e, err := s.store.GetExclusion(ctx, scope, id)
	if err != nil {
		return loggingstore.LogExclusion{}, mapConfigStoreError(err)
	}
	return e, nil
}

// ListExclusions returns a page of the parent's exclusions ordered by name.
func (s *Service) ListExclusions(ctx context.Context, parent string, pageSize int, pageToken string) ([]loggingstore.LogExclusion, string, error) {
	scope, err := ParseScopeParent(parent)
	if err != nil {
		return nil, "", err
	}
	list, err := s.store.ListExclusions(ctx, scope)
	if err != nil {
		return nil, "", err
	}
	page, next, err := paginateConfig(list, pageSize, pageToken)
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// UpdateExclusion merges an exclusion by full resource name. A non-empty mask
// is required (as the proto mandates) and merges the named fields.
func (s *Service) UpdateExclusion(ctx context.Context, name string, in loggingstore.LogExclusion, updateMask []string) (loggingstore.LogExclusion, error) {
	scope, id, err := ParseExclusionName(name)
	if err != nil {
		return loggingstore.LogExclusion{}, err
	}
	stored, err := s.store.GetExclusion(ctx, scope, id)
	if err != nil {
		return loggingstore.LogExclusion{}, mapConfigStoreError(err)
	}
	in.Name = id
	// The proto marks UpdateExclusionRequest.update_mask REQUIRED and non-empty;
	// real Logging rejects a missing mask rather than replacing every field.
	if len(updateMask) == 0 {
		return loggingstore.LogExclusion{}, invalidArgument("update_mask is required for an exclusion update")
	}
	merged, merr := applyExclusionMask(stored, in, updateMask)
	if merr != nil {
		return loggingstore.LogExclusion{}, merr
	}
	if err := validateExclusion(merged); err != nil {
		return loggingstore.LogExclusion{}, err
	}
	stored = merged
	stored.UpdateTime = clock.Now()
	if err := s.store.UpdateExclusion(ctx, scope, stored); err != nil {
		return loggingstore.LogExclusion{}, mapConfigStoreError(err)
	}
	return stored, nil
}

// DeleteExclusion removes an exclusion by full resource name.
func (s *Service) DeleteExclusion(ctx context.Context, name string) error {
	scope, id, err := ParseExclusionName(name)
	if err != nil {
		return err
	}
	return mapConfigStoreError(s.store.DeleteExclusion(ctx, scope, id))
}

func applyExclusionMask(stored, incoming loggingstore.LogExclusion, updateMask []string) (loggingstore.LogExclusion, error) {
	for _, raw := range updateMask {
		switch normalizeConfigMaskPath(raw) {
		case "description":
			stored.Description = incoming.Description
		case "filter":
			stored.Filter = incoming.Filter
		case "disabled":
			stored.Disabled = incoming.Disabled
		default:
			return stored, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+raw, 501)
		}
	}
	return stored, nil
}

// normalizeConfigMaskPath converts a FieldMask path to its canonical snake_case
// root field so proto (snake_case) and REST (camelCase) spellings agree.
func normalizeConfigMaskPath(path string) string {
	path = strings.TrimSpace(path)
	if i := strings.IndexByte(path, '.'); i >= 0 {
		path = path[:i]
	}
	var b strings.Builder
	for i := 0; i < len(path); i++ {
		c := path[i]
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteByte(c + ('a' - 'A'))
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// ─── routing evaluation ───────────────────────────────────────────────────────

// RoutingDecision records what happens to a written entry under a scope's
// project-level exclusions and sinks. Delivery is not performed (the emulator
// has no export destinations); the decision is the observable routing result.
type RoutingDecision struct {
	// Excluded is true when the entry matches an enabled resource-level
	// exclusion, so no sink exports it.
	Excluded bool
	// Sinks lists the full resource names of the enabled sinks whose filter
	// matches the entry and whose inline exclusions do not.
	Sinks []string
}

// RouteEntry evaluates an entry against the scope's resource-level exclusions
// and then its sinks, mirroring Cloud Logging's routing order: exclusions are
// applied first, then each sink's inline exclusions, then the sink filter.
// Invalid stored filters are skipped (they are validated on write).
func (s *Service) RouteEntry(ctx context.Context, scope string, e loggingstore.LogEntry) (RoutingDecision, error) {
	decision := RoutingDecision{}

	exclusions, err := s.store.ListExclusions(ctx, scope)
	if err != nil {
		return decision, err
	}
	for _, ex := range exclusions {
		if ex.Disabled {
			continue
		}
		pred, perr := CompileFilter(ex.Filter)
		if perr != nil {
			continue
		}
		if pred.Match(e) {
			decision.Excluded = true
			return decision, nil
		}
	}

	sinks, err := s.store.ListSinks(ctx, scope)
	if err != nil {
		return decision, err
	}
	for _, sink := range sinks {
		if sink.Disabled {
			continue
		}
		pred, perr := CompileFilter(sink.Filter)
		if perr != nil {
			continue
		}
		if !pred.Match(e) {
			continue
		}
		if sinkExcluded(sink, e) {
			continue
		}
		decision.Sinks = append(decision.Sinks, SinkResourceName(scope, sink.Name))
	}
	return decision, nil
}

// sinkExcluded reports whether an entry matches any enabled inline exclusion of
// a sink.
func sinkExcluded(sink loggingstore.LogSink, e loggingstore.LogEntry) bool {
	for _, ex := range sink.Exclusions {
		if ex.Disabled {
			continue
		}
		pred, err := CompileFilter(ex.Filter)
		if err != nil {
			continue
		}
		if pred.Match(e) {
			return true
		}
	}
	return false
}

// recordRouting evaluates and logs the routing decision for a written entry.
// It is best-effort observability: a routing error never fails the write.
func (s *Service) recordRouting(ctx context.Context, e loggingstore.LogEntry) {
	scope, _, err := ParseLogName(e.LogName)
	if err != nil {
		return
	}
	decision, err := s.RouteEntry(ctx, scope, e)
	if err != nil {
		return
	}
	if decision.Excluded {
		slog.Debug("gcp.logging.routing", "logName", e.LogName, "excluded", true)
		return
	}
	if len(decision.Sinks) > 0 {
		slog.Debug("gcp.logging.routing", "logName", e.LogName, "sinks", decision.Sinks)
	}
}

// paginateConfig applies the standard Cloud Logging pageSize/pageToken contract
// (0 → 50, max 1000) to a name-sorted slice.
func paginateConfig[T any](items []T, pageSize int, token string) ([]T, string, error) {
	if pageSize < 0 || pageSize > 1000 {
		return nil, "", invalidArgument("page_size must be between 0 and 1000")
	}
	if pageSize == 0 {
		pageSize = 50
	}
	start := decodeOffset(token)
	if start > len(items) {
		start = len(items)
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = encodeOffset(end)
	}
	return items[start:end], next, nil
}
