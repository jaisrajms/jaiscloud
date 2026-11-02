package resource

// Stubs mirroring jaiscloud/internal/gcp/resource for the analyzer test.

func ResourceID(project string) func(resourceType, name string) string {
	return func(resourceType, name string) string { return project + "/" + resourceType + "/" + name }
}
