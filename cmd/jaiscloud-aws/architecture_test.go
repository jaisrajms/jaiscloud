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
	"SetCodeLoader": "fetches a function's code package for the executor, not a service call",
	"SetExecutor":   "container executor, not a service",
	"SetEngine":     "state-machine engine, not a service",
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
		for _, arg := range call.Args {
			if id, ok := arg.(*ast.Ident); ok && providerIdent.MatchString(id.Name) {
				t.Errorf("%s: %s(%s) hands a concrete provider to another service; "+
					"pass a seam from internal/aws/seams so the call goes through the registry",
					fset.Position(call.Pos()), name, id.Name)
			}
		}
		return true
	})
}
