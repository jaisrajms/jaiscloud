package prefix

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// badConst is a prefixed literal assigned to a name rather than concatenated.
const badConst = "projects/" // want `\[resource-prefix\]`

func badBuilder(project string) string {
	var b strings.Builder
	b.WriteString("projects/") // want `\[resource-prefix\]`
	b.WriteString(project)
	return b.String()
}

// goodResourceID builds the name with the shared formatter.
func goodResourceID(project string) string {
	return resource.ResourceID(project)("topic", "t")
}
