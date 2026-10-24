package prefix

import (
	"fmt"
	"strings"
)

func badConcat(project string) string {
	return "projects/" + project + "/topics/t" // want `\[resource-prefix\]`
}

func badSprintf(project, id string) string {
	return fmt.Sprintf("projects/%s/topics/%s", project, id) // want `\[resource-prefix\]`
}

func badJoin(project string) string {
	return strings.Join([]string{"organizations/", project}, "") // want `\[resource-prefix\]`
}

// goodParse parses rather than constructs, so the literal is not flagged.
func goodParse(name string) bool {
	return strings.HasPrefix(name, "projects/")
}
