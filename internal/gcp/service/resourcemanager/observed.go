package resourcemanager

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/store"
)

// rtObservedProject is the resource type for a project the wire saw but the
// registry does not hold. Entries are stored per project (account = id =
// project), exactly like rtProject, so a cross-scope List (account="", region="")
// enumerates every observed project.
const rtObservedProject = "gcp_observed_project"

const (
	// observedCap bounds the registry: typos and throwaway test ids must not
	// grow it without bound. The oldest last-seen entries are evicted first.
	observedCap = 200
	// observedFlushInterval throttles the last-seen/request-count write for a
	// project that is already recorded: a hot project costs at most one store
	// write per interval instead of one per request.
	observedFlushInterval = 30 * time.Second
	// maxObservedServices bounds the per-project service set carried in the
	// record (the console shows a few chips, not a service list).
	maxObservedServices = 16
)

// ObservedProject is a project the emulator has served requests for without it
// being registered (created) or configured. It exists so the console can show
// where a workload is actually writing — a project that only exists because the
// wire API synthesizes unknown ids as ACTIVE leaves no other trace.
type ObservedProject struct {
	ProjectID string    `json:"projectId"`
	FirstSeen time.Time `json:"firstSeen"`
	LastSeen  time.Time `json:"lastSeen"`
	Requests  int64     `json:"requests"`
	Services  []string  `json:"services,omitempty"`
}

// observedCache is the in-memory view of the registry. It is loaded from the
// store on first use and written back on a throttle, so observing stays cheap on
// the request path.
type observedCache struct {
	mu     sync.Mutex
	loaded bool
	byID   map[string]*observedEntry
}

type observedEntry struct {
	project ObservedProject
	// flushed is when the entry was last written to the store.
	flushed time.Time
	// persisted reports whether the store holds a copy of this entry.
	persisted bool
}

// ObserveAccount records that `service` served a request for `account` (the
// project). It implements gateway.AccountObserver when wired from the GCP
// binary. Registered and configured projects are recorded too; the read surface
// drops them, so the classification lives in one place (ListObservedProjects)
// instead of on the request path.
func (s *Service) ObserveAccount(ctx context.Context, account, service string) {
	if account == "" {
		return
	}
	now := clock.Now()

	s.observed.mu.Lock()
	s.loadObservedLocked(ctx)
	e := s.observed.byID[account]
	flush := false
	if e == nil {
		e = &observedEntry{project: ObservedProject{ProjectID: account, FirstSeen: now}}
		s.observed.byID[account] = e
		flush = true
	}
	e.project.LastSeen = now
	e.project.Requests++
	if service != "" && !containsString(e.project.Services, service) && len(e.project.Services) < maxObservedServices {
		e.project.Services = append(e.project.Services, service)
		sort.Strings(e.project.Services)
		flush = true
	}
	if flush || !e.persisted || now.Sub(e.flushed) >= observedFlushInterval {
		s.flushObservedLocked(ctx, e)
	}
	s.observed.mu.Unlock()
	s.evictObserved(ctx)
}

// observedProjectIDs returns the ids in the observed-project registry, sorted
// for a deterministic caller. It is the id set a wildcard project-number
// resolution can use for a project the emulator served but never registered.
func (s *Service) observedProjectIDs(ctx context.Context) []string {
	s.observed.mu.Lock()
	s.loadObservedLocked(ctx)
	ids := make([]string, 0, len(s.observed.byID))
	for id := range s.observed.byID {
		ids = append(ids, id)
	}
	s.observed.mu.Unlock()
	sort.Strings(ids)
	return ids
}

// ListObservedProjects returns the observed-project registry, newest activity
// first. Registered and configured projects are not returned: they have their
// own groups in the console (created / configured) and would otherwise appear
// twice.
func (s *Service) ListObservedProjects(ctx context.Context) []ObservedProject {
	s.observed.mu.Lock()
	s.loadObservedLocked(ctx)
	out := make([]ObservedProject, 0, len(s.observed.byID))
	for _, e := range s.observed.byID {
		p := e.project
		p.Services = append([]string(nil), p.Services...)
		out = append(out, p)
	}
	s.observed.mu.Unlock()

	// The classification touches the store per id, so it runs outside the cache
	// lock.
	kept := out[:0]
	for _, p := range out {
		if s.isKnown(p.ProjectID) || s.isRegistered(ctx, p.ProjectID) {
			continue
		}
		kept = append(kept, p)
	}
	out = kept
	sort.Slice(out, func(i, j int) bool {
		if !out[i].LastSeen.Equal(out[j].LastSeen) {
			return out[i].LastSeen.After(out[j].LastSeen)
		}
		return out[i].ProjectID < out[j].ProjectID
	})
	if len(out) > observedCap {
		out = out[:observedCap]
	}
	return out
}

// ResetObserved clears the observed-project registry (POST /_jaiscloud/reset).
func (s *Service) ResetObserved(ctx context.Context) error {
	s.observed.mu.Lock()
	s.loadObservedLocked(ctx)
	ids := make([]string, 0, len(s.observed.byID))
	for id := range s.observed.byID {
		ids = append(ids, id)
	}
	s.observed.byID = map[string]*observedEntry{}
	s.observed.mu.Unlock()

	if s.resources == nil {
		return nil
	}
	for _, id := range ids {
		if err := s.resources.Delete(ctx, id, store.GlobalRegion, rtObservedProject, id); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return nil
}

// loadObservedLocked populates the cache from the store once. Called with the
// lock held; a store error leaves the cache empty (observation must never fail a
// request) and retries on the next call.
func (s *Service) loadObservedLocked(ctx context.Context) {
	if s.observed.loaded {
		return
	}
	s.observed.loaded = true
	if s.observed.byID == nil {
		s.observed.byID = map[string]*observedEntry{}
	}
	if s.resources == nil {
		return
	}
	entries, err := s.resources.List(ctx, "", "", rtObservedProject, "")
	if err != nil {
		s.observed.loaded = false
		return
	}
	for _, entry := range entries {
		var p ObservedProject
		if err := json.Unmarshal(entry.Data, &p); err != nil {
			continue
		}
		if p.ProjectID == "" {
			p.ProjectID = entry.ID
		}
		s.observed.byID[p.ProjectID] = &observedEntry{
			project:   p,
			flushed:   clock.Now(),
			persisted: true,
		}
	}
}

// flushObservedLocked persists one entry. Failures are swallowed: the registry
// is a discovery aid, not a correctness surface.
func (s *Service) flushObservedLocked(ctx context.Context, e *observedEntry) {
	if s.resources == nil {
		return
	}
	data, err := json.Marshal(e.project)
	if err != nil {
		return
	}
	err = s.resources.Upsert(ctx, e.project.ProjectID, store.GlobalRegion,
		store.ResourceEntry{Type: rtObservedProject, ID: e.project.ProjectID, Data: data})
	if err != nil {
		return
	}
	e.flushed = clock.Now()
	e.persisted = true
}

// evictObserved trims the cache to observedCap and deletes the store copies of
// the entries it drops, so a burst of throwaway ids cannot leave the registry
// permanently over its cap on disk.
func (s *Service) evictObserved(ctx context.Context) {
	s.observed.mu.Lock()
	if len(s.observed.byID) <= observedCap {
		s.observed.mu.Unlock()
		return
	}
	byAge := make([]string, 0, len(s.observed.byID))
	for id := range s.observed.byID {
		byAge = append(byAge, id)
	}
	sort.Slice(byAge, func(i, j int) bool {
		return s.observed.byID[byAge[i]].project.LastSeen.Before(s.observed.byID[byAge[j]].project.LastSeen)
	})
	victims := append([]string(nil), byAge[:len(byAge)-observedCap]...)
	for _, id := range victims {
		delete(s.observed.byID, id)
	}
	s.observed.mu.Unlock()

	if s.resources == nil {
		return
	}
	for _, id := range victims {
		_ = s.resources.Delete(ctx, id, store.GlobalRegion, rtObservedProject, id)
	}
}

// isRegistered reports whether the project has a registry entry.
func (s *Service) isRegistered(ctx context.Context, project string) bool {
	if s.resources == nil {
		return false
	}
	_, err := s.resources.Get(ctx, project, store.GlobalRegion, rtProject, project)
	return err == nil
}

func containsString(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// Reset implements snapshottypes.Resetter: /_jaiscloud/reset clears the
// observed-project registry (in memory and in the store). The created-project
// registry itself lives in the shared ResourceStore, which the admin reset wipes
// independently.
func (s *Service) Reset(ctx context.Context) {
	_ = s.ResetObserved(ctx)
}
