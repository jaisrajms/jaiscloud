package metastore

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// ServiceName is the Dataproc Metastore service resource name
// (projects/{p}/locations/{l}/services/{s}).
func ServiceName(project, location, service string) string {
	return resource.ResourceID(project)("metastore-service", location+"/"+service)
}

// BackupName is the Dataproc Metastore backup resource name
// (projects/{p}/locations/{l}/services/{s}/backups/{b}).
func BackupName(project, location, service, backup string) string {
	return resource.ResourceID(project)("metastore-backup", location+"/"+service+"/"+backup)
}

// MetadataImportName is the Dataproc Metastore metadata-import resource name
// (projects/{p}/locations/{l}/services/{s}/metadataImports/{m}).
func MetadataImportName(project, location, service, imp string) string {
	return resource.ResourceID(project)("metastore-metadata-import", location+"/"+service+"/"+imp)
}

// OperationName is the Dataproc Metastore long-running operation resource name
// (projects/{p}/locations/{l}/operations/{id}). It shares the
// google.longrunning.Operations name shape with Cloud Workflows.
func OperationName(project, location, id string) string {
	return resource.ResourceID(project)("metastore-operation", location+"/"+id)
}

// ResourceName is the parsed form of a Dataproc Metastore resource name.
type ResourceName struct {
	Project        string
	Location       string
	Service        string
	Backup         string
	MetadataImport string
	Operation      string
}

// ProjectFromName returns the project id from a "projects/{p}/..." resource
// name, or "" when the name is not project-scoped.
func ProjectFromName(name string) string { return ParseName(name).Project }

// ParseName extracts the hierarchical components of a Dataproc Metastore
// resource name. Segments are matched by their collection keyword; an unknown
// segment is skipped.
func ParseName(name string) ResourceName {
	var out ResourceName
	segs := strings.Split(name, "/")
	for i := 0; i < len(segs); i++ {
		switch segs[i] {
		case "projects":
			if i+1 < len(segs) {
				out.Project = segs[i+1]
				i++
			}
		case "locations":
			if i+1 < len(segs) {
				out.Location = segs[i+1]
				i++
			}
		case "services":
			if i+1 < len(segs) {
				out.Service = segs[i+1]
				i++
			}
		case "backups":
			if i+1 < len(segs) {
				out.Backup = segs[i+1]
				i++
			}
		case "metadataImports":
			if i+1 < len(segs) {
				out.MetadataImport = segs[i+1]
				i++
			}
		case "operations":
			if i+1 < len(segs) {
				out.Operation = segs[i+1]
				i++
			}
		}
	}
	return out
}
