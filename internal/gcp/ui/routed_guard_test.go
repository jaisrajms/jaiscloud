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

// stayOnBuiltIn are console methods that have a registry-action signature but
// serve several keys by resource type (or are console-only), so they cannot be
// forwarded to one key. They keep reading the built-in.
var stayOnBuiltIn = map[string]string{
	"iam.Update":             "serves IAM.ServiceAccountUpdate and IAM.ServiceAccountPatch",
	"kms.GetIamPolicy":       "serves the key ring, crypto key and import job IAM keys by resource type",
	"kms.SetIamPolicy":       "serves the key ring, crypto key and import job IAM keys by resource type",
	"kms.TestIamPermissions": "serves the key ring, crypto key and import job IAM keys by resource type",
}

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
			if !ok {
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
	if checked != len(fwd) {
		t.Errorf("checked %d forwarders, routed.go has %d", checked, len(fwd))
	}
}
