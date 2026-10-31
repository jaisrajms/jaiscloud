package ui

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"jaiscloud/internal/provider"
)

// routed.go is hand-written: one forwarder per console method. These tests keep it
// complete and correct without anyone having to remember to, because a console
// method that is missed (or a key that is mistyped) does not fail to compile or to
// run; it silently keeps showing the built-in's state while a plugin owns the service.

// routeFuncs lists every routeX constructor. TestRoutedTableCoversEveryConstructor
// fails when routed.go has a constructor that is not here.
var routeFuncs = map[string]any{
	"routeQueue": routeQueue, "routeObject": routeObject, "routeTable": routeTable,
	"routeFunction": routeFunction, "routeLogs": routeLogs, "routeCW": routeCW,
	"routeNotif": routeNotif, "routeIAM": routeIAM, "routeKey": routeKey,
	"routeSecret": routeSecret, "routeParam": routeParam, "routeAPIGW": routeAPIGW,
	"routeCatalog": routeCatalog, "routeEMR": routeEMR, "routeEMRC": routeEMRC,
	"routeEvents": routeEvents, "routeSfn": routeSfn, "routeCompute": routeCompute,
	"routeContainer": routeContainer, "routeEKS": routeEKS, "routeRDS": routeRDS,
	"routeCache": routeCache, "routeDNS": routeDNS, "routeStack": routeStack,
	"routeKinesis": routeKinesis, "routeFirehose": routeFirehose, "routeSES": routeSES,
	"routeELBv2": routeELBv2,
}

// consoleOnly are keys the console calls that are not registry actions on purpose:
// they read built-in state and fall through to it (uiCaller.call checks Has first).
var consoleOnly = map[string]bool{"Queue.PeekMessages": true}

type routedSource struct {
	declared map[string]map[string]bool // receiver type -> methods it declares
	keys     map[string][]string        // receiver type -> "Method=Key" pairs
	ctors    map[string]bool
}

func parseRouted(t *testing.T) routedSource {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "routed.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	src := routedSource{declared: map[string]map[string]bool{}, keys: map[string][]string{}, ctors: map[string]bool{}}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Recv == nil {
			if strings.HasPrefix(fn.Name.Name, "route") {
				src.ctors[fn.Name.Name] = true
			}
			continue
		}
		recv, _ := fn.Recv.List[0].Type.(*ast.Ident)
		if recv == nil || !strings.HasPrefix(recv.Name, "routed") {
			continue
		}
		if src.declared[recv.Name] == nil {
			src.declared[recv.Name] = map[string]bool{}
		}
		src.declared[recv.Name][fn.Name.Name] = true
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "call" || len(call.Args) < 2 {
				return true
			}
			if lit, ok := call.Args[1].(*ast.BasicLit); ok && lit.Kind == token.STRING {
				key, _ := strconv.Unquote(lit.Value)
				src.keys[recv.Name] = append(src.keys[recv.Name], fn.Name.Name+"="+key)
			}
			return true
		})
	}
	return src
}

func TestRoutedTableCoversEveryConstructor(t *testing.T) {
	src := parseRouted(t)
	for name := range src.ctors {
		if _, ok := routeFuncs[name]; !ok {
			t.Errorf("%s is in routed.go but not in routeFuncs; add it so its methods are checked", name)
		}
	}
	for name := range routeFuncs {
		if !src.ctors[name] {
			t.Errorf("routeFuncs lists %s, which routed.go no longer has", name)
		}
	}
}

// Every method of a console ProviderInterface must be declared on its routed type.
// A method that is only promoted from the embedded provider compiles, but calls the
// built-in directly and ignores the plugin.
func TestEveryConsoleMethodIsRouted(t *testing.T) {
	src := parseRouted(t)
	for name, fn := range routeFuncs {
		ft := reflect.TypeOf(fn)
		prov := reflect.New(ft.In(0).Elem())
		out := reflect.ValueOf(fn).Call([]reflect.Value{prov, reflect.ValueOf(uiCaller{reg: provider.NewRegistry()})})[0]
		routed := out.Elem().Type()
		iface := ft.Out(0)
		var missing []string
		for i := 0; i < iface.NumMethod(); i++ {
			if m := iface.Method(i).Name; !src.declared[routed.Name()][m] {
				missing = append(missing, m)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Errorf("%s: %s does not declare %v, so the console would call the built-in for them", name, routed.Name(), missing)
		}
	}
}

// Every key the console dispatches must be a key some provider registers, and name
// the method it sits in. uiCaller.call falls back to the built-in for a key the
// registry does not have, so a typo is otherwise invisible.
func TestRoutedKeysExistAndMatchTheirMethod(t *testing.T) {
	known := registeredKeys(t)
	src := parseRouted(t)
	for recv, pairs := range src.keys {
		for _, pair := range pairs {
			method, key, _ := strings.Cut(pair, "=")
			if consoleOnly[key] {
				continue
			}
			if !known[key] {
				t.Errorf("%s.%s dispatches %q, which no provider registers (typo?)", recv, method, key)
			}
			if _, action, _ := strings.Cut(key, "."); action != method {
				t.Errorf("%s.%s dispatches %q; the key should end in the method name", recv, method, key)
			}
		}
	}
}

var keyLiteral = regexp.MustCompile(`"([A-Z][A-Za-z0-9]*\.[A-Z][A-Za-z0-9]*)":\s`)

// registeredKeys collects the "Prefix.Action": handler entries of every provider's
// Routes() map from source, so the test needs no running emulator.
func registeredKeys(t *testing.T) map[string]bool {
	t.Helper()
	root := filepath.Join("..") // internal/aws
	keys := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		if strings.Contains(path, string(filepath.Separator)+"ui"+string(filepath.Separator)) {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for _, m := range keyLiteral.FindAllStringSubmatch(string(b), -1) {
			keys[m[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) < 200 {
		t.Fatalf("found only %d registered keys; the scan is broken", len(keys))
	}
	return keys
}
