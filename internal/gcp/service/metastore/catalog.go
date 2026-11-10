package metastore

import (
	"context"

	hmsstore "jaiscloud/internal/gcp/store/hms"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

// CatalogHost is the Hive serving plane as the control plane sees it. A
// Dataproc Metastore Service has its own Hive Metastore in real GCP; the
// emulator models that with one catalog (and one Thrift listener) per Service,
// so creating a Service must open its catalog and deleting one must drop it.
// The implementation lives in main.go (backed by internal/gcp/hms.Fleet) so this
// package keeps no serving-plane dependency. A nil host (unit tests, deployments
// without the metastore service enabled) leaves catalogs unmaterialized.
type CatalogHost interface {
	// OpenCatalog makes id's catalog reachable on port.
	OpenCatalog(ctx context.Context, id string, port int) error
	// CloseCatalog drops id's catalog and releases its port.
	CloseCatalog(ctx context.Context, id string)
	// Catalog returns id's catalog admin surface, for the admin/DR operations
	// that mutate catalog metadata directly (moveTableToDatabase,
	// alterLocation). A deployment that serves no catalog for id returns an
	// error (or a nil CatalogAdmin), which the caller reports as a failed
	// precondition rather than as a completed mutation.
	Catalog(ctx context.Context, id string) (CatalogAdmin, error)
}

// CatalogAdmin is one Service's Hive Metastore catalog as the control plane
// mutates it. Every method reports the serving-plane store's sentinel errors
// (ErrDatabaseNotFound / ErrTableNotFound / ErrPartitionNotFound /
// ErrTableExists), which the core maps onto its wire errors.
type CatalogAdmin interface {
	// MoveTable moves a table to another database in the same catalog, keeping
	// its name and its partitions.
	MoveTable(ctx context.Context, db, tableName, destDB string) error
	// SetDatabaseLocation re-points a database's root directory.
	SetDatabaseLocation(ctx context.Context, db, locationURI string) error
	// SetTableLocation re-points a table's storage descriptor location.
	SetTableLocation(ctx context.Context, db, tableName, locationURI string) error
	// SetPartitionLocation re-points one partition's storage descriptor
	// location.
	SetPartitionLocation(ctx context.Context, db, tableName string, values []string, locationURI string) error
}

// WithCatalogHost wires the Hive serving plane. Nil is a no-op.
func WithCatalogHost(h CatalogHost) Option {
	return func(s *Service) { s.catalogHost = h }
}

// WithHMSPort sets the base Hive Metastore Thrift port (--hms-port). Services
// are numbered above it; the zero value means metastorestore.DefaultServicePort.
func WithHMSPort(port int) Option {
	return func(s *Service) { s.hmsPort = port }
}

// CatalogID returns the serving-plane catalog id for a Service: its canonical
// resource name. It is the same id the Dataproc attachment resolver derives from
// a cluster's metastoreConfig, so a job is pointed at exactly the catalog its
// Service owns.
func CatalogID(project, location, service string) string {
	return hmsstore.CatalogID(project, location, service)
}

// EndpointURI formats a serving-plane Thrift endpoint. The host is the
// per-Service DNS name real GCP publishes; the port is the Service's own
// catalog listener. A deployment whose clients cannot resolve the name replaces
// the host (see Dataproc's JAISCLOUD_DATAPROC_HMS_ENDPOINT) and keeps the port.
func EndpointURI(id, location string, port int) string {
	return endpointURI(id, location, port)
}

// baseHMSPort is the configured base port, defaulted.
func (s *Service) baseHMSPort() int {
	if s.hmsPort > 0 {
		return s.hmsPort
	}
	return metastorestore.DefaultServicePort
}

// nextServicePort returns the port to assign a new Service: one above the
// highest port already assigned (so ports are never reused while a Service
// holds one) and never below base+1. A caller-requested port is honored in
// CreateService (see ports.go); this is only the fallback. Assigned ports are
// row state, so a restart keeps every endpoint stable.
func (s *Service) nextServicePort(ctx context.Context) int {
	port := s.baseHMSPort()
	services, err := s.store.ListAllServices(ctx)
	if err != nil {
		return port + 1
	}
	for _, svc := range services {
		if svc.Port > port {
			port = svc.Port
		}
	}
	return port + 1
}
