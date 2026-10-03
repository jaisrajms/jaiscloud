package metastore

import (
	"context"
	"strings"
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
	// Real Dataproc requires the cluster and its Metastore service to be in the
	// same region (the reference is still region-scoped even when the project
	// is implied), so an explicit mismatching location is rejected.
	if loc := ParseName(strings.TrimSpace(ref)).Location; loc != "" && defaultLocation != "" && loc != defaultLocation {
		return invalidArgument("metastore service must be in the cluster region " + defaultLocation + ", got " + loc)
	}
	project, location, service, err := parseServiceRef(ref, defaultProject, defaultLocation)
	if err != nil {
		return err
	}
	if _, err := s.store.GetService(ctx, project, location, service); err != nil {
		return mapErr(err)
	}
	return nil
}

// MetastoreEndpoint formats the thrift endpoint for ref without touching the
// store. It is the pure counterpart of ValidateMetastoreService, used at job
// submission so the attachment validated when the cluster was created is
// honored at execution without re-checking the control plane.
func (s *Service) MetastoreEndpoint(ref, defaultProject, defaultLocation string) (string, error) {
	_, location, service, err := parseServiceRef(ref, defaultProject, defaultLocation)
	if err != nil {
		return "", err
	}
	return endpointURI(service, location), nil
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
