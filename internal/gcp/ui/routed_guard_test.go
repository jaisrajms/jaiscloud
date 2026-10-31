package ui

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// routed.go has one forwarder per console method that is a registry action. A
// console method that is missed (or a key that is mistyped) does not fail to
// compile or to run; it silently keeps showing the built-in's state while a plugin
// owns the service. These tests keep the file complete and correct.

// routedServices pairs a console package with the sources of the provider whose
// Routes() it calls and the routeX constructor in routed.go.
var routedServices = []struct {
	ui       string
	provider []string
	ctor     any
}{
	{"pubsub", []string{"../provider/pubsub"}, routePubSub},
	{"storage", []string{"../provider/storage"}, routeStorage},
	{"firestore", []string{"../provider/firestore"}, routeFirestore},
	{"bigquery", []string{"../provider/bigquery"}, routeBigQuery},
	{"compute", []string{"../provider/compute"}, routeCompute},
	{"iam", []string{"../provider/iam"}, routeIAM},
	{"kms", []string{"../transport/rest/kms"}, routeKMS},
	{"secretmanager", []string{"../provider/secretmanager"}, routeSecret},
	{"logging", []string{"../transport/rest/logging"}, routeLogging},
	{"monitoring", []string{"../transport/rest/monitoring"}, routeMonitoring},
}

// stayOnBuiltIn are console methods that have a registry-action signature but are
// not an action of the service's API (none today: the methods that serve several keys
// by resource type pick the key from the resource, see kmsIAMKey).
var stayOnBuiltIn = map[string]string{}

func parseDir(t *testing.T, dir string) []*ast.File {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, e.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return files
}

// consoleMethods returns the registry-action-shaped methods of a console
// package's ProviderInterface.
func consoleMethods(t *testing.T, pkg string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, f := range parseDir(t, pkg) {
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || ts.Name.Name != "ProviderInterface" {
				return true
			}
			it, ok := ts.Type.(*ast.InterfaceType)
			if !ok {
				return true
			}
			for _, m := range it.Methods.List {
				ft, ok := m.Type.(*ast.FuncType)
				if !ok || len(m.Names) != 1 || !isHandlerShape(ft) {
					continue
				}
				out[m.Names[0].Name] = true
			}
			return true
		})
	}
	return out
}

// isHandlerShape reports (ctx, *model.NormalizedRequest) (*model.ProviderResponse, error).
func isHandlerShape(ft *ast.FuncType) bool {
	src := func(e ast.Expr) string {
		switch v := e.(type) {
		case *ast.SelectorExpr:
			return v.Sel.Name
		case *ast.StarExpr:
			if s, ok := v.X.(*ast.SelectorExpr); ok {
				return "*" + s.Sel.Name
			}
		case *ast.Ident:
			return v.Name
		}
		return ""
	}
	var params, results []string
	for _, p := range ft.Params.List {
		for range max(1, len(p.Names)) {
			params = append(params, src(p.Type))
		}
	}
	if ft.Results != nil {
		for _, r := range ft.Results.List {
			for range max(1, len(r.Names)) {
				results = append(results, src(r.Type))
			}
		}
	}
	return strings.Join(params, ",") == "Context,*NormalizedRequest" && strings.Join(results, ",") == "*ProviderResponse,error"
}

// forwarders maps "routedType.Method" to the key it dispatches, read from routed.go.
func forwarders(t *testing.T) map[string]string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "routed.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
			continue
		}
		recv, ok := fn.Recv.List[0].Type.(*ast.Ident)
		if !ok || !strings.HasPrefix(recv.Name, "routed") {
			continue
		}
		key := ""
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if bl, ok := n.(*ast.BasicLit); ok && bl.Kind == token.STRING && key == "" {
				key, _ = strconv.Unquote(bl.Value)
			}
			return true
		})
		if _, resourceKeyed := resourceKeyedMethods[recv.Name+"."+fn.Name.Name]; resourceKeyed {
			key = "" // the key depends on the resource, see TestKMSIAMKeyFollowsTheResource
		}
		out[recv.Name+"."+fn.Name.Name] = key
	}
	return out
}

// registeredKeys reads the keys a provider package's Routes() maps register.
func registeredKeys(t *testing.T, dirs []string) map[string]bool {
	t.Helper()
	keys := map[string]bool{}
	for _, d := range dirs {
		for _, f := range parseDir(t, d) {
			ast.Inspect(f, func(n ast.Node) bool {
				fn, ok := n.(*ast.FuncDecl)
				if !ok || fn.Name.Name != "Routes" {
					return true
				}
				ast.Inspect(fn, func(m ast.Node) bool {
					if kv, ok := m.(*ast.KeyValueExpr); ok {
						if bl, ok := kv.Key.(*ast.BasicLit); ok && bl.Kind == token.STRING {
							k, _ := strconv.Unquote(bl.Value)
							keys[k] = true
						}
					}
					return true
				})
				return true
			})
		}
	}
	return keys
}

// resourceKeyedMethods are forwarders whose registry key is chosen from the resource.
var resourceKeyedMethods = map[string]bool{
	"routedKMS.GetIamPolicy": true, "routedKMS.SetIamPolicy": true, "routedKMS.TestIamPermissions": true,
}

func TestEveryRegistryActionConsoleMethodIsForwardedOrListed(t *testing.T) {
	fwd := forwarders(t)
	for _, s := range routedServices {
		typ := "routed" + map[string]string{
			"pubsub": "PubSub", "storage": "Storage", "firestore": "Firestore", "bigquery": "BigQuery",
			"compute": "Compute", "iam": "IAM", "kms": "KMS", "secretmanager": "Secret",
			"logging": "Logging", "monitoring": "Monitoring",
		}[s.ui]
		for m := range consoleMethods(t, s.ui) {
			_, forwarded := fwd[typ+"."+m]
			_, listed := stayOnBuiltIn[s.ui+"."+m]
			if forwarded == listed {
				t.Errorf("%s.%s: forwarded=%v listed-as-built-in=%v; a console method must be exactly one of the two", s.ui, m, forwarded, listed)
			}
		}
	}
}

func TestEveryForwarderNamesAKeyAProviderRegisters(t *testing.T) {
	fwd := forwarders(t)
	all := map[string]bool{}
	for _, s := range routedServices {
		for k := range registeredKeys(t, s.provider) {
			all[k] = true
		}
	}
	for name, key := range fwd {
		if key == "" {
			continue // resource-keyed; TestKMSIAMKeyFollowsTheResource checks its keys
		}
		if !all[key] {
			t.Errorf("%s dispatches %q, which no provider registers", name, key)
		}
	}
}

// Every forwarder really goes through the registry, as origin "ui", to its key.
func TestForwardersDispatchThroughTheRegistryAsUI(t *testing.T) {
	fwd := forwarders(t)
	reg := provider.NewRegistry()
	type seen struct{ key, origin string }
	var got []seen
	for _, key := range fwd {
		if key == "" {
			continue
		}
		k := key
		reg.RegisterAll(map[string]provider.HandlerFunc{k: func(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			got = append(got, seen{k, string(nr.Origin)})
			return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
		}})
	}
	checked := 0
	for _, s := range routedServices {
		// A nil embedded provider panics if a forwarder falls through to the built-in.
		out := reflect.ValueOf(s.ctor).Call([]reflect.Value{reflect.ValueOf(reg), reflect.Zero(reflect.TypeOf(s.ctor).In(1))})[0]
		rt := out.Elem().Type()
		for i := 0; i < rt.NumMethod(); i++ {
			m := rt.Method(i)
			want, ok := fwd[rt.Name()+"."+m.Name]
			if !ok || want == "" {
				continue
			}
			got = nil
			out.Elem().Method(i).Call([]reflect.Value{reflect.ValueOf(context.Background()), reflect.ValueOf(&model.NormalizedRequest{Params: map[string]any{}})})
			if len(got) != 1 || got[0].key != want || got[0].origin != "ui" {
				t.Errorf("%s.%s: dispatched %v, want one call to %s as ui", rt.Name(), m.Name, got, want)
			}
			checked++
		}
	}
	want := 0
	for _, k := range fwd {
		if k != "" {
			want++
		}
	}
	if checked != want {
		t.Errorf("checked %d forwarders, routed.go has %d keyed ones", checked, want)
	}
}

// ---- typed console services ---------------------------------------------------------

// typedServices are the console services that work on typed store structs. Each is
// served by a typed<Service> wrapper (typed_*.go) that sends a covered call through
// the plugin chain.
var typedServices = []struct {
	ui       string
	wrapper  string
	provider []string
}{
	{"scheduler", "typedScheduler", []string{"../transport/rest/scheduler"}},
	{"tasks", "typedTasks", []string{"../transport/rest/tasks"}},
	{"workflows", "typedWorkflows", []string{"../transport/rest/workflows", "../transport/rest/workflowexecutions"}},
	{"run", "typedRun", []string{"../transport/rest/run"}},
	{"functions", "typedFunctions", []string{"../transport/rest/functions"}},
	{"eventarc", "typedEventarc", []string{"../transport/rest/eventarc"}},
	{"dataproc", "typedDataproc", []string{"../transport/rest/dataproc"}},
	{"managedkafka", "typedManagedKafka", []string{"../transport/rest/managedkafka"}},
	{"datastore", "typedDatastore", []string{"../transport/rest/datastore"}},
	{"resourcemanager", "typedResourceManager", []string{"../transport/rest/resourcemanager"}},
}

// typedStayOnBuiltIn are console methods that are not actions of the service's API,
// so there is no key a plugin could cover.
var typedStayOnBuiltIn = map[string]string{
	"functions.ListDeliveries":             "the emulator's own log of event deliveries, not a Cloud Functions action",
	"functions.PutSourceArchive":           "writes the source archive through the Cloud Storage seam, which is already a registry call",
	"resourcemanager.ListObservedProjects": "the registry of projects seen on the wire, console discovery only",
	"projects.ListObservedProjects":        "the registry of projects seen on the wire, console discovery only",
}

// allMethods returns every method of a console package's ProviderInterface.
func allMethods(t *testing.T, pkg string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, f := range parseDir(t, pkg) {
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || ts.Name.Name != "ProviderInterface" {
				return true
			}
			if it, ok := ts.Type.(*ast.InterfaceType); ok {
				for _, m := range it.Methods.List {
					if _, ok := m.Type.(*ast.FuncType); ok && len(m.Names) == 1 {
						out[m.Names[0].Name] = true
					}
				}
			}
			return true
		})
	}
	return out
}

// typedMethods maps wrapper type -> methods it declares, and collects the keys its
// methods hand to the plugin chain.
func typedMethods(t *testing.T) (methods map[string]map[string]bool, keys map[string]bool) {
	t.Helper()
	methods, keys = map[string]map[string]bool{}, map[string]bool{}
	ents, _ := os.ReadDir(".")
	for _, e := range ents {
		if !strings.HasPrefix(e.Name(), "typed_") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			if id, ok := fn.Recv.List[0].Type.(*ast.Ident); ok && strings.HasPrefix(id.Name, "typed") {
				if methods[id.Name] == nil {
					methods[id.Name] = map[string]bool{}
				}
				methods[id.Name][fn.Name.Name] = true
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) < 2 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "do" {
				return true
			}
			// do(ctx, key, ...) and do(ctx, key literal, ...)
			if bl, ok := call.Args[1].(*ast.BasicLit); ok && bl.Kind == token.STRING {
				k, _ := strconv.Unquote(bl.Value)
				keys[k] = true
			}
			return true
		})
		// Keys passed to a helper such as p.queue(ctx, "Tasks.QueuesGet", ...).
		ast.Inspect(f, func(n ast.Node) bool {
			bl, ok := n.(*ast.BasicLit)
			if ok && bl.Kind == token.STRING {
				k, _ := strconv.Unquote(bl.Value)
				if i := strings.IndexByte(k, '.'); i > 0 && !strings.ContainsAny(k, " /:") && k[0] >= 'A' && k[0] <= 'Z' && !strings.HasPrefix(k, "google.") {
					keys[k] = true
				}
			}
			return true
		})
	}
	return methods, keys
}

func TestEveryTypedConsoleMethodIsRoutedOrListed(t *testing.T) {
	methods, _ := typedMethods(t)
	for _, s := range typedServices {
		for m := range allMethods(t, s.ui) {
			_, routed := methods[s.wrapper][m]
			_, listed := typedStayOnBuiltIn[s.ui+"."+m]
			if routed == listed {
				t.Errorf("%s.%s: routed=%v listed-as-built-in=%v; a console method must be exactly one of the two", s.ui, m, routed, listed)
			}
		}
	}
	// The project picker's interface is served by the same Resource Manager wrapper.
	for m := range allMethods(t, "projects") {
		_, routed := methods["typedResourceManager"][m]
		_, listed := typedStayOnBuiltIn["projects."+m]
		if routed == listed {
			t.Errorf("projects.%s: routed=%v listed-as-built-in=%v", m, routed, listed)
		}
	}
}

func TestEveryTypedKeyIsRegisteredByAProvider(t *testing.T) {
	_, keys := typedMethods(t)
	all := map[string]bool{}
	for _, s := range typedServices {
		for k := range registeredKeys(t, s.provider) {
			all[k] = true
		}
	}
	for k := range keys {
		if !all[k] {
			t.Errorf("a typed console wrapper names the key %q, which no REST provider registers", k)
		}
	}
	if len(keys) == 0 {
		t.Fatal("found no keys in the typed wrappers")
	}
}

// The KMS IAM calls serve three resource types; each has its own key, and the forwarder
// picks it from the resource the console names.
func TestKMSIAMKeyFollowsTheResource(t *testing.T) {
	keys := registeredKeys(t, []string{"../transport/rest/kms"})
	cases := map[string]string{
		"projects/p/locations/l/keyRings/kr":               "KeyRing",
		"projects/p/locations/l/keyRings/kr/cryptoKeys/ck": "CryptoKey",
		"projects/p/locations/l/keyRings/kr/importJobs/ij": "ImportJob",
	}
	for name, typ := range cases {
		for _, op := range []string{"GetIamPolicy", "SetIamPolicy", "TestIamPermissions"} {
			nr := &model.NormalizedRequest{Params: map[string]any{"name": name}}
			got := kmsIAMKey(nr, op)
			if got != "KMS."+typ+op || !keys[got] {
				t.Errorf("%s %s: key %q (registered: %v), want KMS.%s%s", op, name, got, keys[got], typ, op)
			}
		}
	}
}
