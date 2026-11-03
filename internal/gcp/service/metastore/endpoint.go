package metastore

import (
	"context"
	"strings"

	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

// Metastore attachment resolution. These methods let a consumer of a Dataproc
// Metastore service (the Dataproc cluster core) validate a
// dataproc.v1.MetastoreConfig.dataprocMetastoreService reference and derive the
// thrift endpoint Spark should point at. They live here (not in the consumer) so
// the resource-name grammar and the endpoint format stay in one place; the
// consumer sees them only through an interface and never imports this package.

// ValidateMetastoreService validates that ref names an existing Dataproc
// Metastore service. Missing segments (project, location) are filled from
// defaultProject/defaultLocation so a cluster may reference its Metastore
// service with a short form. A malformed reference, or one that names a service
// in a different region than defaultLocation, is InvalidArgument; an unknown
// service is NotFound.
func (s *Service) ValidateMetastoreService(ctx context.Context, ref, defaultProject, defaultLocation string) error {
	project, location, service, err := ResolveServiceRef(ref, defaultProject, defaultLocation)
	if err != nil {
		return err
	}
	if _, err := s.store.GetService(ctx, project, location, service); err != nil {
		return mapErr(err)
	}
	return nil
}

// ResolveServiceRef is the part of ValidateMetastoreService that needs no store:
// it rejects a malformed reference or one in another region than
// defaultLocation, and returns the project, location and service it names. A
// caller that looks the service up itself (internal/gcp/seams) shares it.
func ResolveServiceRef(ref, defaultProject, defaultLocation string) (project, location, service string, err error) {
	// Real Dataproc requires the cluster and its Metastore service to be in the
	// same region (the reference is still region-scoped even when the project
	// is implied), so an explicit mismatching location is rejected.
	if loc := ParseName(strings.TrimSpace(ref)).Location; loc != "" && defaultLocation != "" && loc != defaultLocation {
		return "", "", "", invalidArgument("metastore service must be in the cluster region " + defaultLocation + ", got " + loc)
	}
	return parseServiceRef(ref, defaultProject, defaultLocation)
}

// MetastoreEndpoint formats the thrift endpoint for ref at job submission time.
// It reads the Service only for its assigned catalog port (the port is what
// selects the catalog on the serving plane) and does not re-validate the
// attachment: the reference was validated when the cluster was created.
func (s *Service) MetastoreEndpoint(ctx context.Context, ref, defaultProject, defaultLocation string) (string, error) {
	project, location, service, err := parseServiceRef(ref, defaultProject, defaultLocation)
	if err != nil {
		return "", err
	}
	port := metastorestore.DefaultServicePort
	if svc, err := s.store.GetService(ctx, project, location, service); err == nil {
		port = endpointPort(svc.Port)
	}
	return endpointURI(service, location, port), nil
}

// parseServiceRef parses a dataproc.v1.MetastoreConfig.dataprocMetastoreService
// reference. It accepts the canonical
// projects/{p}/locations/{l}/services/{s} form and the short forms
// locations/{l}/services/{s}, services/{s} and a bare service id; missing
// project/location segments are filled from defaultProject/defaultLocation. A
// reference that names a non-service Metastore resource (a backup, metadata
// import or operation) is rejected, as is a malformed name.
func parseServiceRef(ref, defaultProject, defaultLocation string) (project, location, service string, err error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", "", "", invalidArgument("metastoreConfig.dataprocMetastoreService is empty")
	}
	rn := ParseName(ref)
	if rn.Backup != "" || rn.MetadataImport != "" || rn.Operation != "" {
		return "", "", "", invalidArgument("metastoreConfig.dataprocMetastoreService must reference a service: " + ref)
	}
	switch {
	case rn.Service != "":
		project = firstNonEmpty(rn.Project, defaultProject)
		location = firstNonEmpty(rn.Location, defaultLocation)
		service = rn.Service
	case !strings.Contains(ref, "/"):
		// A bare service id, e.g. "hms".
		project, location, service = defaultProject, defaultLocation, ref
	default:
		return "", "", "", invalidArgument("malformed dataprocMetastoreService: " + ref)
	}
	if project == "" || location == "" || service == "" {
		return "", "", "", invalidArgument("dataprocMetastoreService requires project, location and service: " + ref)
	}
	return project, location, service, nil
}

// firstNonEmpty returns a when non-empty, else b.
func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
