package seams

import (
	"context"

	metastorecore "jaiscloud/internal/gcp/service/metastore"
)

// Metastore is Dataproc Metastore as seen by Dataproc: it checks that the
// service a cluster attaches to exists, by asking for it like a client.
type Metastore struct {
	s    *Set
	core *metastorecore.Service
}

// Metastore returns the Metastore seam. core supplies only the pure parts
// (reference parsing and the thrift endpoint format), which touch no state.
func (s *Set) Metastore(core *metastorecore.Service) *Metastore {
	return &Metastore{s: s, core: core}
}

// ValidateMetastoreService checks ref names an existing service in the cluster's
// region: a malformed reference is InvalidArgument, an unknown service NotFound.
func (m *Metastore) ValidateMetastoreService(ctx context.Context, ref, defaultProject, defaultLocation string) error {
	project, location, service, err := metastorecore.ResolveServiceRef(ref, defaultProject, defaultLocation)
	if err != nil {
		return err
	}
	_, err = m.s.dispatch(ctx, "metastore", "GetService", project, map[string]any{
		"project": project, "location": location, "serviceId": service,
	}, nil)
	return err
}

// MetastoreEndpoint formats the thrift endpoint for ref. It reads no state, so
// there is no call for a plugin to see.
func (m *Metastore) MetastoreEndpoint(ref, defaultProject, defaultLocation string) (string, error) {
	return m.core.MetastoreEndpoint(ref, defaultProject, defaultLocation)
}
