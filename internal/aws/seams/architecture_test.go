package seams_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// A service must not reach another service except through the registry: that is
// what lets a plugin that overrides a service answer every caller. A service
// package that imports another service's package can call it directly, so each
// such edge has to be listed here and justified. Adding an edge is a design
// decision, not a convenience: prefer a local interface that internal/aws/seams
// implements over the registry.
var allowedEdges = map[string]string{
	// events/targets formats payloads with the destination services' types; it
	// calls them only through the seam interfaces it declares itself.
	"events -> lambda":       "InvokeResult / InternalInvoker types for EventBridge targets",
	"events -> notification": "MessageAttribute type for SNS targets",
	"events -> queue":        "MessageAttribute / SourceContext types for SQS targets",
	// S3 and SQS share the SourceContext and event payload types.
	"object -> queue": "SourceContext for S3 notifications",
	"queue -> lambda": "ESM message types for the internal receive path",
}

// libraries are shared code under provider/ that serve no API: pure helpers other
// services may import freely.
var libraries = map[string]bool{
	"provider/sparkaws":       true, // Spark pod wiring shared by EMR on EC2 and EMR on EKS
	"provider/events/pattern": true, // event-pattern matcher (SNS filters, ESM filters, EventBridge rules)
}

const modulePrefix = "jaiscloud/internal/aws/"

// root returns the service a package belongs to ("provider/queue/..." -> "queue",
// "secret" -> "secret"), or "" when the package is not a service.
func root(importPath string) string {
	p := strings.TrimPrefix(importPath, modulePrefix)
	if libraries[p] {
		return ""
	}
	switch {
	case strings.HasPrefix(p, "provider/"):
		return strings.Split(strings.TrimPrefix(p, "provider/"), "/")[0]
	case p == "secret", p == "key", p == "parameter":
		return p
	}
	return ""
}

func TestServicesOnlyReachEachOtherThroughTheRegistry(t *testing.T) {
	base, _ := filepath.Abs("..") // internal/aws
	fset := token.NewFileSet()
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		rel, _ := filepath.Rel(base, filepath.Dir(path))
		self := root(modulePrefix + filepath.ToSlash(rel))
		if self == "" {
			return nil // seams, adapter, ui, store, ...: allowed to know every service
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			return perr
		}
		for _, imp := range f.Imports {
			ip := strings.Trim(imp.Path.Value, `"`)
			other := root(ip)
			if other == "" || other == self {
				continue
			}
			edge := self + " -> " + other
			if _, ok := allowedEdges[edge]; !ok {
				t.Errorf("%s imports %s (%s): services must call each other through the registry "+
					"(declare a narrow interface and let internal/aws/seams implement it), "+
					"or justify the edge in allowedEdges", rel, ip, edge)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
