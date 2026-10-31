package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"
)

// buildRegistry must hand services to each other as seams (internal/aws/seams),
// never as concrete providers: a provider passed to another provider's Set*/With*
// can be called directly, bypassing the registry and therefore any plugin that
// overrides it. The few exceptions below are not API calls between services.
var wiringExceptions = map[string]string{
	"SetExecutor": "container executor, not a service",
	"SetEngine":   "state-machine engine, not a service",
}

// Calls identified as receiver.Method for the exceptions that only hold on one
// receiver.
var receiverExceptions = map[string]string{
	"workerReg.Add": "hands a background worker's lifecycle (start/stop) to the worker registry, not a service call",
}

// Identifiers that match providerIdent by spelling but are not providers.
var notProviders = map[string]bool{
	"ServiceToProvider": true, // adapter method that maps a wire service to its registry prefix
}

var providerIdent = regexp.MustCompile(`(P|Prov|Provider)$`)

func TestBuildRegistryWiresServicesThroughSeams(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var fn *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "buildRegistry" {
			fn = fd
		}
	}
	if fn == nil {
		t.Fatal("buildRegistry not found")
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fun := call.Fun.(type) {
		case *ast.SelectorExpr:
			name = fun.Sel.Name
			if recv, ok := fun.X.(*ast.Ident); ok {
				if _, ok := receiverExceptions[recv.Name+"."+name]; ok {
					return true
				}
			}
		case *ast.Ident:
			name = fun.Name
		}
		// Registration is how a provider enters the registry; everything else that
		// receives a provider (setters, options and constructors alike) is a way for
		// one service to call another directly.
		if name == "" || strings.HasPrefix(name, "Register") || strings.HasPrefix(name, "register") {
			return true
		}
		if _, ok := wiringExceptions[name]; ok {
			return true
		}
		// A provider can be handed over directly (New(p)), through a method or a
		// field of it (New(p.AsKeyEncryptor()), New(p.Handle)), or by address
		// (New(&p)); look anywhere inside the argument, not only at its top level.
		for _, arg := range call.Args {
			ast.Inspect(arg, func(m ast.Node) bool {
				if id, ok := m.(*ast.Ident); ok && providerIdent.MatchString(id.Name) && !notProviders[id.Name] {
					t.Errorf("%s: %s(...%s...) hands a concrete provider to another service; "+
						"pass a seam from internal/aws/seams so the call goes through the registry",
						fset.Position(id.Pos()), name, id.Name)
				}
				return true
			})
		}
		return true
	})
}
