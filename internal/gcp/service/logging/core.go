// Package logging is the transport-neutral core of the Cloud Logging v2
// service. It owns all Cloud Logging business logic — the write path
// (defaults, per-entry validation, atomic vs partial_success), the read path
// (advanced-log filter engine, multi-scope merge, ordering and pagination),
// log listing/deletion, and the monitored-resource-descriptor catalog — over
// the shared logging store.
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/logging) and the REST transport
// (internal/gcp/transport/rest/logging) both transcode their wire format into
// this package's typed API and then call the SAME Service instance. That is the
// dual-protocol invariant: one core, one piece of state, so the transports
// cannot drift.
//
// Resource names follow the Logging v2 form {scope}/{scopeID}/logs/{LOG_ID}
// where the scope is projects, organizations, folders, or billingAccounts; log
// IDs are URL-encoded in the canonical stored form. TailLogEntries is a
// streaming RPC and therefore stays in the gRPC transport, but it consumes the
// filter, scope, and entry-listing primitives this package exports.
package logging

import (
	"context"
	"sort"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
)

// Service is the transport-neutral Cloud Logging service over the shared store.
type Service struct {
	store       loggingstore.Store
	defaultProj string
}

// NewService returns a Logging core backed by the shared store. defaultProj is
// the config-default project; transports use it when a request carries no
// project/scope.
func NewService(store loggingstore.Store, defaultProj string) *Service {
	return &Service{store: store, defaultProj: defaultProj}
}

// ─── write path ───────────────────────────────────────────────────────────────

// WriteEntries validates and writes a batch of log entries. Defaults on the
// request (log name, monitored resource, labels) are applied to entries that
// omit them, and entries with no timestamp get clock.Now().
//
// partial_success mirrors Cloud Logging: when set, entries that fail a
// permanent validation error are dropped and the valid ones are still written;
// when unset, any invalid entry rejects the whole batch atomically. dry_run
// validates but never persists.
func (s *Service) WriteEntries(ctx context.Context, req *WriteRequest) error {
	// Pass 1: apply defaults + validate every entry BEFORE writing any, so a
	// permanently-invalid entry rejects the whole batch atomically.
	entries := make([]loggingstore.LogEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		if e.LogName == "" {
			e.LogName = req.LogName
		}
		if e.ResourceType == "" {
			e.ResourceType = req.ResourceType
		}
		if len(e.ResourceLabels) == 0 && len(req.ResourceLabels) > 0 {
			e.ResourceLabels = req.ResourceLabels
		}
		if len(req.Labels) > 0 {
			if e.Labels == nil {
				e.Labels = make(map[string]string, len(req.Labels))
			}
			for k, v := range req.Labels {
				if _, ok := e.Labels[k]; !ok {
					e.Labels[k] = v
				}
			}
		}
		if e.Timestamp.IsZero() {
			e.Timestamp = clock.Now()
		}
		scope, logID, err := ParseLogName(e.LogName)
		if err != nil {
			if req.PartialSuccess {
				continue // drop the invalid entry, keep the rest of the batch
			}
			return err
		}
		e.LogName = CanonicalLogName(scope, logID)
		entries = append(entries, e)
	}

	if req.DryRun {
		return nil
	}

	// Pass 2: write every validated entry, then record its routing decision
	// against the scope's exclusions and sinks. Routing is observability only —
	// the emulator performs no export delivery.
	for _, e := range entries {
		scope, _, _ := ParseLogName(e.LogName)
		if err := s.store.Write(ctx, scope, e); err != nil {
			return err
		}
		s.recordRouting(ctx, e)
	}
	return nil
}

// ─── read path ────────────────────────────────────────────────────────────────

// ListEntries returns a filtered, ordered, paginated page of log entries across
// the requested resource scopes (or the fallback scope when none are given).
func (s *Service) ListEntries(ctx context.Context, req *ListEntriesRequest) (*ListEntriesResult, error) {
	pred, err := CompileFilter(req.Filter)
	if err != nil {
		return nil, invalidArgument("invalid filter: " + err.Error())
	}

	var scopes []string
	seenScope := make(map[string]struct{}, len(req.ResourceNames))
	for _, rn := range req.ResourceNames {
		scope, perr := ParseScopeParent(rn)
		if perr != nil {
			return nil, perr
		}
		if _, seen := seenScope[scope]; !seen {
			seenScope[scope] = struct{}{}
			scopes = append(scopes, scope)
		}
	}
	if len(scopes) == 0 && req.Scope != "" {
		scopes = append(scopes, req.Scope)
	}

	var entries []loggingstore.LogEntry
	for _, scope := range scopes {
		list, lerr := s.store.List(ctx, scope)
		if lerr != nil {
			return nil, lerr
		}
		entries = append(entries, list...)
	}
	if len(scopes) > 1 {
		sort.SliceStable(entries, func(i, j int) bool {
			if entries[i].Timestamp.Equal(entries[j].Timestamp) {
				return entries[i].ID < entries[j].ID
			}
			return entries[i].Timestamp.Before(entries[j].Timestamp)
		})
	}
	filtered := entries[:0]
	for _, e := range entries {
		if pred.Match(e) {
			filtered = append(filtered, e)
		}
	}

	if err := ValidateOrderBy(req.OrderBy); err != nil {
		return nil, err
	}
	orderEntries(filtered, req.OrderBy)
	page, next, err := paginateEntries(filtered, req.PageSize, req.PageToken)
	if err != nil {
		return nil, err
	}
	return &ListEntriesResult{Entries: page, NextPageToken: next}, nil
}

// ListLogs returns the distinct full log names under a scope parent
// ({scope}/{scopeID}/logs/{LOG_ID}), sorted and cursor-paginated. parent is the
// raw resource name; when empty, defaultScope is used.
func (s *Service) ListLogs(ctx context.Context, parent, defaultScope string, pageSize int, pageToken string) ([]string, string, error) {
	scope := defaultScope
	if parent != "" {
		parsed, err := ParseScopeParent(parent)
		if err != nil {
			return nil, "", err
		}
		scope = parsed
	}
	names, err := s.store.ListLogs(ctx, scope)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(names, func(n string) string { return n },
		map[string]any{"pageSize": pageSize, "pageToken": pageToken})
	return page, next, nil
}

// DeleteLog deletes every entry belonging to a full log resource name. It is
// idempotent for a well-formed, now-absent log.
func (s *Service) DeleteLog(ctx context.Context, logName string) error {
	scope, logID, err := ParseLogName(logName)
	if err != nil {
		return err
	}
	return s.store.DeleteLog(ctx, scope, CanonicalLogName(scope, logID))
}

// LatestEntryID returns the highest stored entry id in a scope, or -1 when
// there are none. The gRPC TailLogEntries implementation uses it to seed its
// "new since stream start" cursor; -1 (not 0) is required because the in-memory
// store's first entry gets id 0.
func (s *Service) LatestEntryID(ctx context.Context, scope string) (int64, error) {
	entries, err := s.ListScope(ctx, scope)
	if err != nil {
		return -1, err
	}
	max := int64(-1)
	for _, e := range entries {
		if e.ID > max {
			max = e.ID
		}
	}
	return max, nil
}

// ListScope returns every entry in a scope, ordered by (timestamp, id). It is
// the raw read the streaming TailLogEntries implementation polls on.
func (s *Service) ListScope(ctx context.Context, scope string) ([]loggingstore.LogEntry, error) {
	return s.store.List(ctx, scope)
}

// ─── ordering / pagination ────────────────────────────────────────────────────

// orderEntries orders the filtered entries by timestamp in place. The default
// is "timestamp asc" (the store's natural order); "timestamp desc" reverses it.
// Callers must reject any other value first via ValidateOrderBy.
func orderEntries(entries []loggingstore.LogEntry, orderBy string) {
	switch strings.TrimSpace(orderBy) {
	case "", "timestamp asc":
		return // already ascending from the store
	case "timestamp desc":
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}
	}
}

// ValidateOrderBy returns InvalidArgument for any order_by other than "",
// "timestamp asc", or "timestamp desc".
func ValidateOrderBy(orderBy string) error {
	switch strings.TrimSpace(orderBy) {
	case "", "timestamp asc", "timestamp desc":
		return nil
	default:
		return invalidArgument("invalid order_by: " + orderBy)
	}
}

func paginateEntries(entries []loggingstore.LogEntry, pageSize int, token string) ([]loggingstore.LogEntry, string, error) {
	return paginateConfig(entries, pageSize, token)
}
