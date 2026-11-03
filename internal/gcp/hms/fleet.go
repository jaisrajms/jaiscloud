package hms

import (
	"context"
	"fmt"
	"sync"

	hmsstore "jaiscloud/internal/gcp/store/hms"
)

// Fleet serves one Hive Metastore listener per catalog: each Dataproc Metastore
// Service gets its own Thrift listener on its own port, backed by its own
// catalog partition, so two Services — in the same project or in different
// projects/locations — never share databases, the `default` database, or Iceberg
// commit locks. (The Thrift protocol carries no service identity, so the catalog
// a connection belongs to is the listener it connected to.) The legacy global
// catalog on the base port is served by a plain [Server], not by a Fleet.
type Fleet struct {
	mu       sync.Mutex
	catalogs hmsstore.Catalogs
	servers  map[string]*Server
}

// NewFleet returns an empty fleet over catalogs.
func NewFleet(catalogs hmsstore.Catalogs) *Fleet {
	return &Fleet{catalogs: catalogs, servers: make(map[string]*Server)}
}

// Open binds id's catalog listener on port and starts serving it. The bind is
// synchronous, so the caller learns about an unavailable port; an id already
// open on the same port is a no-op, and one open on a different port is
// rebound.
func (f *Fleet) Open(id string, port int) error {
	if port <= 0 {
		return fmt.Errorf("hms catalog %q: invalid port %d", id, port)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if srv, ok := f.servers[id]; ok {
		if srv.addr == fmt.Sprintf(":%d", port) {
			return nil
		}
		srv.Stop()
		delete(f.servers, id)
	}
	srv := NewServer(fmt.Sprintf(":%d", port), f.catalogs.Catalog(id))
	if err := srv.Listen(); err != nil {
		return err
	}
	f.servers[id] = srv
	go func() { _ = srv.Serve() }()
	return nil
}

// Close stops id's listener and forgets it. The catalog's stored data is
// untouched (the control plane drops the catalog separately).
func (f *Fleet) Close(id string) {
	f.mu.Lock()
	srv := f.servers[id]
	delete(f.servers, id)
	f.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
}

// Ports returns the port each open catalog is listening on.
func (f *Fleet) Ports() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.servers))
	for id, srv := range f.servers {
		var port int
		if _, err := fmt.Sscanf(srv.addr, ":%d", &port); err == nil {
			out[id] = port
		}
	}
	return out
}

// Reset stops every per-Service listener. An admin Reset clears the control
// plane, so the catalogs those listeners served no longer exist.
func (f *Fleet) Reset(context.Context) {
	f.Stop()
}

// Stop stops every listener in the fleet.
func (f *Fleet) Stop() {
	f.mu.Lock()
	servers := make([]*Server, 0, len(f.servers))
	for _, srv := range f.servers {
		servers = append(servers, srv)
	}
	f.servers = make(map[string]*Server)
	f.mu.Unlock()
	for _, srv := range servers {
		srv.Stop()
	}
}
