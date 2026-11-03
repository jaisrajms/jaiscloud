package hms

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sort"
	"sync"
)

// LegacyCatalogID is the catalog served on the base Hive Metastore port
// (--hms-port, 9083 by default). It is the pre-per-Service global catalog: a
// client that connects to the base port without naming a Dataproc Metastore
// Service gets this one, exactly as before per-Service catalogs existed. Every
// Dataproc Metastore Service has its own catalog, keyed by its canonical
// resource name (see CatalogID), served on its own port.
const LegacyCatalogID = ""

// CatalogID returns the store catalog id for a Dataproc Metastore service: its
// canonical resource name. The serving plane partitions the Hive store by this
// id, so two Services — in the same project or in different projects/locations
// — never see each other's databases, tables or locks.
func CatalogID(project, location, service string) string {
	const prefix = "projects/" //gcplint:ignore resource-prefix store key derived from the resource name, never a response name
	return prefix + project + "/locations/" + location + "/services/" + service
}

// Catalogs is the multi-catalog Hive store: one [Store] per catalog id, plus
// the legacy global catalog (LegacyCatalogID). A Store keeps the single-catalog
// contract unchanged (databases/tables keyed by name); Catalogs supplies the
// per-Service partitioning the serving plane needs and owns the snapshot,
// reset and emptiness state across every catalog.
type Catalogs interface {
	// Catalog returns the store for id, creating an empty one on first use.
	// The legacy catalog id returns the store for the shared global catalog.
	Catalog(id string) Store
	// DropCatalog deletes every record in id's catalog (a Service was deleted).
	DropCatalog(ctx context.Context, id string)
	Reset(ctx context.Context)
	IsEmpty(ctx context.Context) (bool, error)
	Snapshot(ctx context.Context, w io.Writer) error
	Restore(ctx context.Context, r io.Reader) error
}

// catalogsSnapshotVersion is the snapshot envelope version. Version 1 was the
// flat single-catalog shape (no envelope); a v2 snapshot is
// {"version":2,"catalogs":{"<id>":<the backend's flat catalog snapshot>}}.
// Restore accepts both, so a snapshot taken before per-Service catalogs existed
// restores into the legacy catalog unchanged.
const catalogsSnapshotVersion = 2

type catalogsSnapshot struct {
	Version  int                        `json:"version"`
	Catalogs map[string]json.RawMessage `json:"catalogs"`
}

// decodeCatalogsSnapshot returns the per-catalog documents in a snapshot. ok is
// false when the input is a pre-per-Service (version 1) flat snapshot, which the
// caller restores into the legacy catalog.
func decodeCatalogsSnapshot(raw []byte) (catalogsSnapshot, bool, error) {
	var env catalogsSnapshot
	if err := json.Unmarshal(raw, &env); err != nil {
		return env, false, err
	}
	if len(env.Catalogs) == 0 {
		return env, false, nil
	}
	return env, true, nil
}

// --- Memory ---

// MemoryCatalogs is the in-memory Catalogs: the legacy catalog plus one
// MemoryStore per Dataproc Metastore Service catalog.
type MemoryCatalogs struct {
	mu       sync.Mutex
	legacy   *MemoryStore
	catalogs map[string]*MemoryStore
}

// NewMemoryCatalogs returns an empty in-memory catalog set. legacy is the store
// serving LegacyCatalogID (nil creates a fresh one); passing the process's
// existing global store keeps any state it already holds reachable.
func NewMemoryCatalogs(legacy *MemoryStore) *MemoryCatalogs {
	if legacy == nil {
		legacy = NewMemoryStore()
	}
	return &MemoryCatalogs{legacy: legacy, catalogs: make(map[string]*MemoryStore)}
}

// Catalog implements Catalogs.
func (c *MemoryCatalogs) Catalog(id string) Store {
	return c.memoryCatalog(id)
}

// memoryCatalog is Catalog with the concrete type, for the snapshot path.
func (c *MemoryCatalogs) memoryCatalog(id string) *MemoryStore {
	if id == LegacyCatalogID {
		return c.legacy
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.catalogs[id]
	if !ok {
		s = NewMemoryStore()
		c.catalogs[id] = s
	}
	return s
}

// DropCatalog deletes id's catalog: its databases, tables and partitions go
// away, and the id gets a fresh empty store if it is addressed again.
func (c *MemoryCatalogs) DropCatalog(_ context.Context, id string) {
	if id == LegacyCatalogID {
		c.legacy = NewMemoryStore()
		return
	}
	c.mu.Lock()
	delete(c.catalogs, id)
	c.mu.Unlock()
}

// Reset clears every catalog (the legacy one and every Service catalog).
func (c *MemoryCatalogs) Reset(ctx context.Context) {
	c.mu.Lock()
	c.catalogs = make(map[string]*MemoryStore)
	c.mu.Unlock()
	c.legacy.Reset(ctx)
}

// IsEmpty reports whether every catalog is empty.
func (c *MemoryCatalogs) IsEmpty(ctx context.Context) (bool, error) {
	if empty, err := c.legacy.IsEmpty(ctx); err != nil || !empty {
		return empty, err
	}
	c.mu.Lock()
	stores := make([]*MemoryStore, 0, len(c.catalogs))
	for _, s := range c.catalogs {
		stores = append(stores, s)
	}
	c.mu.Unlock()
	for _, s := range stores {
		if empty, err := s.IsEmpty(ctx); err != nil || !empty {
			return empty, err
		}
	}
	return true, nil
}

// Snapshot writes the v2 envelope: every catalog's own flat snapshot keyed by
// catalog id (the legacy catalog is "").
func (c *MemoryCatalogs) Snapshot(ctx context.Context, w io.Writer) error {
	c.mu.Lock()
	ids := make([]string, 0, len(c.catalogs))
	stores := make(map[string]*MemoryStore, len(c.catalogs))
	for id, s := range c.catalogs {
		ids = append(ids, id)
		stores[id] = s
	}
	c.mu.Unlock()
	sort.Strings(ids)

	env := catalogsSnapshot{Version: catalogsSnapshotVersion, Catalogs: map[string]json.RawMessage{}}
	for _, id := range append([]string{LegacyCatalogID}, ids...) {
		store := c.legacy
		if id != LegacyCatalogID {
			store = stores[id]
		}
		var buf bytes.Buffer
		if err := store.Snapshot(ctx, &buf); err != nil {
			return err
		}
		env.Catalogs[id] = buf.Bytes()
	}
	return json.NewEncoder(w).Encode(env)
}

// Restore replaces every catalog from a snapshot. A v2 envelope restores each
// catalog it names (and drops the ones it does not); a version 1 flat snapshot
// restores into the legacy catalog, preserving pre-per-Service snapshots.
func (c *MemoryCatalogs) Restore(ctx context.Context, r io.Reader) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	env, ok, err := decodeCatalogsSnapshot(raw)
	if err != nil {
		return err
	}
	if !ok {
		return c.legacy.Restore(ctx, bytes.NewReader(raw))
	}
	c.mu.Lock()
	c.catalogs = make(map[string]*MemoryStore, len(env.Catalogs))
	c.mu.Unlock()
	c.legacy.Reset(ctx)
	for id, data := range env.Catalogs {
		if err := c.memoryCatalog(id).Restore(ctx, bytes.NewReader(data)); err != nil {
			return err
		}
	}
	return nil
}
