package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"
)

// A service that is handed another service's provider or core can call it
// directly, bypassing the registry and so any plugin that overrides it. Calls
// between services go through internal/gcp/seams. Every direct link left in
// main.go is listed here with the reason it is not an API call between services;
// a new one fails this test until it is either seamed or listed on purpose.
var directLinks = map[string]string{
	// Constructors and registration: building a service, not calling one.
	"New": "constructor", "NewService": "constructor", "NewProvider": "constructor",
	"NewEvaluator": "constructor", "NewServiceV2": "constructor", "NewConfigService": "constructor",
	"NewMetricsService": "constructor", "NewRegistrar": "console registrar: WithRegistry and WithPluginChain route every console call that is a service API action through the plugin chain as origin ui (internal/gcp/ui/routed.go, typed_*.go); only emulator-only console helpers stay direct (ListObservedProjects, ListDeliveries)",
	"NewEngine": "constructor", "NewRunner": "constructor",
	"WithMetastoreResolver": "takes the Metastore seam; the nested constructor call is checked on its own",
	"Metastore":             "seam constructor: the core is used only for pure reference parsing and endpoint formatting",

	// Event delivery to Cloud Functions / Eventarc triggers is the emulator's own
	// event bus (internal/gcp/eventing), not a call to a service API.
	"SetFunctionDispatcher": "event bus: producers hand events to the delivery engines",
	"SetEventDispatcher":    "event bus: Dataproc lifecycle events to the delivery engine",

	// Functions <-> Eventarc trigger plumbing: one feature implemented by two cores
	// with a platform-owned protocol (naming, ownership labels, re-pointing); it is
	// not the Eventarc or Functions client API. Their Pub/Sub dependency, and every Storage, Metastore and Cloud Run call, IS seamed.
	"SetFunctionExister":       "eventarc validates destination.cloudFunction against the functions core",
	"SetEventTargets":          "functions delivery engine reads eventarc triggers",
	"SetTriggerProvisioner":    "function event triggers materialize a backing eventarc trigger",
	"SetOperationResolvers":    "async-LRO-only top-level operation lookup across services",
	"SetCodeLoader":            "executor loads function code, not a service API",
	"SetLambdaCodeFetcher":     "admin hook: function code for the executor",
	"SetEngine":                "background engine (scheduler/tasks), not a service",
	"SetAccountObserver":       "console project discovery hook",
	"WithServiceModes":         "UI configuration",
	"SetDockerExecutor":        "executor, not a service",
	"SetSparkRunner":           "executor, not a service",
	"WithExecutor":             "executor, not a service",
	"RegisterResetter":         "admin lifecycle",
	"RegisterSnapshotter":      "admin lifecycle",
	"RegisterSchedulerTicker":  "admin test hook",
	"RegisterTasksTicker":      "admin test hook",
	"RegisterMonitoringTicker": "admin test hook",
}

// infra are hooks that read provider state for the gateway itself.
var infra = map[string]string{
	"WithGCSCORSLookup": "gateway CORS response headers for GCS; not a service call",
	"append":            "builds an option slice; the calls inside it are checked on their own",
}

var providerIdent = regexp.MustCompile(`(P|Core)$`)

// Identifiers that match by spelling but are not providers.
var notProviders = map[string]bool{"ServiceToProvider": true, "CloudGCP": true}

func TestServicesCallEachOtherThroughSeams(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fun := call.Fun.(type) {
		case *ast.SelectorExpr:
			name = fun.Sel.Name
		case *ast.Ident:
			name = fun.Name
		}
		if name == "" || strings.HasPrefix(name, "Register") || strings.HasPrefix(name, "register") {
			if _, listed := directLinks[name]; !listed || name == "" {
				return true
			}
		}
		for _, m := range []map[string]string{directLinks, infra} {
			if _, ok := m[name]; ok {
				return true
			}
		}
		for _, arg := range call.Args {
			ast.Inspect(arg, func(m ast.Node) bool {
				if id, ok := m.(*ast.Ident); ok && providerIdent.MatchString(id.Name) && !notProviders[id.Name] {
					t.Errorf("%s: %s(...%s...) hands a concrete provider or core to another service; "+
						"pass a seam from internal/gcp/seams so the call goes through the registry "+
						"(or list it in directLinks with the reason)", fset.Position(id.Pos()), name, id.Name)
				}
				return true
			})
		}
		return true
	})
}
