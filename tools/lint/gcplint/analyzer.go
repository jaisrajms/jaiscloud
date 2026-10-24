package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/tools/go/analysis"
)

// Analyzer is the single gcplint analyzer; all rules run in one pass and carry
// their rule name in the diagnostic Category (used for suppression and by
// -json consumers).
var Analyzer = &analysis.Analyzer{
	Name: "gcplint",
	Doc:  "checks the GCP house conventions from CLAUDE.md (clock, resource formatters, store sentinels, unsafe assertions, Reset registration, cloud isolation)",
	Run:  run,
}

// gcpPath is the subtree the convention rules apply to (relative import path
// fragment). Tests override it to analyze the example.com fixtures.
var gcpPath = "internal/gcp"

// resetMarkers select the packages whose types are checked for Reset
// registration. Only provider/service cores are in scope: the shared stores and
// transport wrappers are registered indirectly and would be false positives.
var resetMarkers = []string{"/provider/", "/service/"}

// mutatingPrefixes identify a "mutating" type; a type must have at least one
// such method as well as Reset(context.Context) to be in scope for the
// reset-registration rule.
var mutatingPrefixes = []string{"Create", "Delete", "Update", "Patch", "Insert", "Put", "Set", "Add"}

// resourcePrefixes are the GCP resource-name roots that must be built via
// internal/gcp/resource, never a literal.
var resourcePrefixes = []string{"projects/", "organizations/", "folders/"}

// callConstructors are the functions whose string arguments build a resource
// name (as opposed to parsing one, e.g. strings.HasPrefix/TrimPrefix/Split).
var callConstructors = map[string]bool{
	"fmt.Sprintf":  true,
	"fmt.Sprint":   true,
	"fmt.Sprintln": true,
	"strings.Join": true,
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		fname := pass.Fset.Position(file.Pos()).Filename
		if !strings.HasSuffix(fname, ".go") || strings.HasSuffix(fname, ".pb.go") || strings.HasSuffix(fname, "_test.go") {
			continue
		}

		if inGCPScope(pass) {
			checkClock(pass, file)
			checkClockRealNow(pass, file)
			checkResourcePrefix(pass, file)
			checkStoreSentinel(pass, file)
			checkUnsafeAssert(pass, file)
			checkAWSImport(pass, file)
		}
	}
	if pass.Pkg.Name() == "main" {
		checkResetRegistration(pass)
	}
	return nil, nil
}

func inGCPScope(pass *analysis.Pass) bool {
	return strings.Contains(filepath.ToSlash(pass.Pkg.Path()), gcpPath)
}

// importsGCP reports whether the package directly imports something under the
// GCP tree.
func importsGCP(pass *analysis.Pass) bool {
	for _, imp := range pass.Pkg.Imports() {
		if strings.Contains(filepath.ToSlash(imp.Path()), gcpPath) {
			return true
		}
	}
	return false
}

// isResourceFormatter reports whether the package is the resource-name
// formatter itself (the one place literals are allowed).
func isResourceFormatter(pass *analysis.Pass) bool {
	return strings.HasSuffix(pass.Pkg.Path(), "internal/gcp/resource")
}

// ---- rule: clock ----------------------------------------------------------

func checkClock(pass *analysis.Pass, file *ast.File) {
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		obj, ok := pass.TypesInfo.Uses[sel.Sel].(*types.Func)
		if !ok || obj.Pkg() == nil || obj.Pkg().Path() != "time" {
			return true
		}
		switch obj.Name() {
		case "Now", "Since", "Until":
			report(pass, file, sel.Pos(), "clock",
				"use clock.Now()/clock.RealNow() instead of time.%s (CLAUDE.md: never call time.Now directly)", obj.Name())
		}
		return true
	})
}

// ---- rule: clock-realnow --------------------------------------------------

// clockPkg is the import path of the internal clock package. A clock.Now() call
// is resolved by package (and by a nil receiver, so a Clock interface method is
// not confused with the package function) so the rule never fires on an
// unrelated Now method.
const clockPkg = "jaiscloud/internal/clock"

// wallClockFuncs are the package-level scheduling/wait primitives that take a
// wall-clock duration or deadline. A business clock (clock.Now) feeding one of
// these is an infrastructure wait that must use clock.RealNow() so a frozen or
// offset clock cannot stall or skew it. Keyed "<pkgpath>.<Func>"; the resolver
// requires a package-level function, so the time.Time.After method is not
// mistaken for time.After.
var wallClockFuncs = map[string]bool{
	"time.After":           true,
	"time.AfterFunc":       true,
	"time.Sleep":           true,
	"time.NewTimer":        true,
	"time.NewTicker":       true,
	"time.Tick":            true,
	"context.WithDeadline": true,
}

// isWallClockCall reports whether call is a package-level wall-clock primitive.
func isWallClockCall(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	fn, ok := pass.TypesInfo.Uses[sel.Sel].(*types.Func)
	if !ok || fn.Pkg() == nil {
		return false
	}
	if sig, ok := fn.Type().(*types.Signature); !ok || sig.Recv() != nil {
		return false
	}
	return wallClockFuncs[fn.Pkg().Path()+"."+fn.Name()]
}

// checkClockRealNow flags clock.Now() calls that drive a wall-clock wait — a
// poll loop, a derived deadline compared inside a loop, or a wall-clock
// primitive argument — where clock.RealNow() is required (CLAUDE.md §Time:
// container lifecycle, keepalive/GC tickers, request latency, rate-limiter
// refill). It is deliberately conservative (it may miss a misuse it cannot see
// statically) and any genuine, documented exception is recorded in the
// allowlist.
func checkClockRealNow(pass *analysis.Pass, file *ast.File) {
	ast.Inspect(file, func(n ast.Node) bool {
		decl, ok := n.(*ast.FuncDecl)
		if !ok || decl.Body == nil {
			return true
		}
		auditClockRealNow(pass, file, decl.Body)
		return false // auditClockRealNow descends the body itself
	})
}

func auditClockRealNow(pass *analysis.Pass, file *ast.File, body *ast.BlockStmt) {
	// deadlineNow maps a local variable built from clock.Now() (a derived
	// deadline, e.g. `deadline := clock.Now().Add(timeout)`) to that call, so a
	// comparison against the alias inside a loop is caught.
	deadlineNow := map[string][]*ast.CallExpr{}
	inspectLocalDeadlines(pass, body, deadlineNow)

	seen := map[token.Pos]bool{}
	flag := func(c *ast.CallExpr) {
		if seen[c.Pos()] {
			return
		}
		seen[c.Pos()] = true
		report(pass, file, c.Pos(), "clock-realnow",
			"clock.Now() feeds a wall-clock wait/deadline; use clock.RealNow() for infrastructure time (CLAUDE.md: container lifecycle, keepalive/GC tickers, request latency, rate-limiter refill)")
	}

	ast.Inspect(body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.CallExpr:
			// A business clock passed to a wall-clock primitive.
			if isWallClockCall(pass, v) {
				for _, c := range clockNowCalls(pass, v) {
					flag(c)
				}
			}
		case *ast.ForStmt:
			// A poll loop whose condition reads the business clock.
			for _, c := range clockNowCalls(pass, v.Cond) {
				flag(c)
			}
			// A clock-derived deadline compared inside the loop body — either
			// read fresh (`clock.Now().Before(deadline)`) or through a derived
			// alias. Only comparisons that reference such a deadline are wait
			// loops; a business comparison against a stored value is not.
			ast.Inspect(v.Body, func(m ast.Node) bool {
				call, ok := m.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "Before" && sel.Sel.Name != "After") {
					return true
				}
				waitLoop := false
				ast.Inspect(call, func(x ast.Node) bool {
					if id, ok := x.(*ast.Ident); ok {
						if cs := deadlineNow[id.Name]; len(cs) > 0 {
							waitLoop = true
							for _, c := range cs {
								flag(c)
							}
						}
					}
					return true
				})
				if waitLoop {
					for _, c := range clockNowCalls(pass, call) {
						flag(c)
					}
				}
				return true
			})
		}
		return true
	})
}

// inspectLocalDeadlines records each local variable assigned from an expression
// that both calls clock.Now() and does duration arithmetic on it (`.Add`/
// `.Sub`), i.e. a derived deadline. A bare `now := clock.Now()` is not recorded:
// reading the business clock once and comparing it is not itself a wall-clock
// wait.
func inspectLocalDeadlines(pass *analysis.Pass, body *ast.BlockStmt, out map[string][]*ast.CallExpr) {
	record := func(name string, rhs ast.Expr) {
		if rhs == nil || !hasDurationMath(rhs) {
			return
		}
		if cs := clockNowCalls(pass, rhs); len(cs) > 0 {
			out[name] = append(out[name], cs...)
		}
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.AssignStmt:
			if len(v.Lhs) == len(v.Rhs) {
				for i := range v.Lhs {
					if id, ok := v.Lhs[i].(*ast.Ident); ok {
						record(id.Name, v.Rhs[i])
					}
				}
			}
		case *ast.ValueSpec:
			if len(v.Names) == len(v.Values) {
				for i := range v.Names {
					record(v.Names[i].Name, v.Values[i])
				}
			}
		}
		return true
	})
}

// hasDurationMath reports whether expr applies time.Duration arithmetic
// (`.Add(`/`.Sub(`) anywhere, the shape that turns a clock instant into a
// deadline or an elapsed duration.
func hasDurationMath(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if ok && (sel.Sel.Name == "Add" || sel.Sel.Name == "Sub") {
			found = true
		}
		return true
	})
	return found
}

// clockNowCalls returns every clock.Now() package-function call nested in node.
func clockNowCalls(pass *analysis.Pass, node ast.Node) []*ast.CallExpr {
	if node == nil {
		return nil
	}
	var out []*ast.CallExpr
	ast.Inspect(node, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		fn, ok := pass.TypesInfo.Uses[sel.Sel].(*types.Func)
		if !ok || fn.Pkg() == nil || fn.Pkg().Path() != clockPkg || fn.Name() != "Now" {
			return true
		}
		if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
			return true // a Clock method, not the package function
		}
		out = append(out, call)
		return true
	})
	return out
}

// ---- rule: resource-prefix ------------------------------------------------

func checkResourcePrefix(pass *analysis.Pass, file *ast.File) {
	if isResourceFormatter(pass) {
		return
	}
	seen := map[token.Pos]bool{}
	reportLit := func(lit *ast.BasicLit) {
		if lit == nil || seen[lit.Pos()] {
			return
		}
		s, ok := strVal(lit)
		if !ok {
			return
		}
		for _, p := range resourcePrefixes {
			if strings.HasPrefix(s, p) {
				seen[lit.Pos()] = true
				report(pass, file, lit.Pos(), "resource-prefix",
					"build GCP resource names with internal/gcp/resource.ResourceID, not a literal %q (CLAUDE.md)", p)
				return
			}
		}
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.BinaryExpr:
			if v.Op != token.ADD {
				return true
			}
			if lit, ok := v.X.(*ast.BasicLit); ok {
				reportLit(lit)
			}
			if lit, ok := v.Y.(*ast.BasicLit); ok {
				reportLit(lit)
			}
		case *ast.CallExpr:
			// A name-construction call: report any prefixed literal anywhere in
			// its arguments (they nest inside composite literals, e.g.
			// strings.Join([]string{"projects/", id}, "")).
			if !callConstructors[calledFuncName(pass, v)] {
				return true
			}
			ast.Inspect(v, func(m ast.Node) bool {
				if lit, ok := m.(*ast.BasicLit); ok {
					reportLit(lit)
				}
				return true
			})
		}
		return true
	})
}

// ---- rule: store-sentinel -------------------------------------------------

func checkStoreSentinel(pass *analysis.Pass, file *ast.File) {
	ast.Inspect(file, func(n ast.Node) bool {
		be, ok := n.(*ast.BinaryExpr)
		if !ok || (be.Op != token.EQL && be.Op != token.NEQ) {
			return true
		}
		for _, e := range []ast.Expr{be.X, be.Y} {
			sel, ok := e.(*ast.SelectorExpr)
			if !ok {
				continue
			}
			v, ok := pass.TypesInfo.Uses[sel.Sel].(*types.Var)
			if !ok || v.Pkg() == nil || v.Pkg().Path() != "jaiscloud/internal/store" {
				continue
			}
			switch v.Name() {
			case "ErrNotFound", "ErrAlreadyExists":
				report(pass, file, be.Pos(), "store-sentinel",
					"compare store.%s with errors.Is, not %s (CLAUDE.md)", v.Name(), be.Op)
			}
		}
		return true
	})
}

// ---- rule: unsafe-assert --------------------------------------------------

func checkUnsafeAssert(pass *analysis.Pass, file *ast.File) {
	safe := map[*ast.TypeAssertExpr]bool{}
	markSafe := func(ta *ast.TypeAssertExpr) { safe[ta] = true }
	ast.Inspect(file, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.AssignStmt:
			// comma-ok form: a, ok := x.(T)  (2 LHS, 1 RHS)
			if len(v.Lhs) == 2 && len(v.Rhs) == 1 {
				if ta, ok := v.Rhs[0].(*ast.TypeAssertExpr); ok {
					markSafe(ta)
				}
			}
		case *ast.ValueSpec:
			// var a, ok = x.(T)  (2 names, 1 value)
			if len(v.Names) == 2 && len(v.Values) == 1 {
				if ta, ok := v.Values[0].(*ast.TypeAssertExpr); ok {
					markSafe(ta)
				}
			}
		}
		return true
	})
	ast.Inspect(file, func(n ast.Node) bool {
		ta, ok := n.(*ast.TypeAssertExpr)
		if !ok || ta.Type == nil || safe[ta] {
			return true
		}
		if id, ok := ta.Type.(*ast.Ident); !ok || id.Name != "string" {
			return true
		}
		report(pass, file, ta.Pos(), "unsafe-assert",
			"unchecked .(string) assertion; use the comma-ok form s, ok := x.(string) (CLAUDE.md)")
		return true
	})
}

// ---- rule: aws-import -----------------------------------------------------

func checkAWSImport(pass *analysis.Pass, file *ast.File) {
	for _, imp := range file.Imports {
		path, ok := strVal(imp.Path)
		if !ok {
			continue
		}
		if path == "jaiscloud/internal/aws" || strings.HasPrefix(path, "jaiscloud/internal/aws/") {
			report(pass, file, imp.Pos(), "aws-import",
				"internal/gcp must not import %s (cloud isolation)", path)
		}
	}
}

// ---- rule: reset-registration ---------------------------------------------

func checkResetRegistration(pass *analysis.Pass) {
	if strings.HasSuffix(pass.Pkg.Path(), ".test") {
		// The test variant of the main package can be loaded from export data
		// (no source AST); skip it.
		return
	}
	if !importsGCP(pass) {
		// Only check a main package that actually pulls in the GCP tree, so
		// running over ./... never flags the AWS binary.
		return
	}
	// Key registered types by "<pkgpath>.<TypeName>" strings: object identity is
	// not stable across the mix of source-loaded and export-data-loaded
	// dependencies, but the fully-qualified name is.
	registered := map[string]bool{}
	for _, file := range pass.Files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch calleeName(call) {
			case "RegisterResetter", "RegisterSnapshotter":
				for _, a := range call.Args {
					if nt := namedType(pass.TypesInfo.TypeOf(a)); nt != nil {
						registered[qualifiedName(nt.Obj())] = true
					}
				}
			}
			return true
		})
	}

	unregistered := map[string][]string{}
	seen := map[string]bool{}
	for pkg := range transitiveImports(pass.Pkg) {
		path := pkg.Path()
		if !matchesAny(path, resetMarkers) || seen[path] {
			continue
		}
		seen[path] = true
		for _, name := range pkg.Scope().Names() {
			obj, ok := pkg.Scope().Lookup(name).(*types.TypeName)
			if !ok || !ast.IsExported(name) {
				continue
			}
			nt := namedType(obj.Type())
			if nt == nil || !hasResetMethod(nt) || !hasMutatingMethod(nt) {
				continue
			}
			if registered[qualifiedName(obj)] {
				continue
			}
			if allow().match(pass.Fset.Position(obj.Pos()).Filename, "reset-registration") {
				continue
			}
			unregistered[path] = append(unregistered[path], name)
		}
	}
	for path, names := range unregistered {
		file := fileForImport(pass, path)
		pos := importPos(pass, path)
		if file == nil || !pos.IsValid() {
			continue
		}
		report(pass, file, pos, "reset-registration",
			"type(s) %s (%s) implement Reset(context.Context) but are never passed to RegisterResetter/RegisterSnapshotter",
			strings.Join(names, ", "), path)
	}
}

func qualifiedName(obj *types.TypeName) string {
	if obj.Pkg() == nil {
		return obj.Name()
	}
	return obj.Pkg().Path() + "." + obj.Name()
}

// ---- helpers --------------------------------------------------------------

func report(pass *analysis.Pass, file *ast.File, pos token.Pos, rule, format string, args ...any) {
	if !pos.IsValid() || file == nil {
		return
	}
	fname := pass.Fset.Position(pos).Filename
	if allow().match(fname, rule) || suppressed(pass, file, pos, rule) {
		return
	}
	pass.Report(analysis.Diagnostic{
		Pos:      pos,
		Message:  "[" + rule + "] " + fmt.Sprintf(format, args...),
		Category: rule,
	})
}

// suppressed reports whether pos carries an inline `//gcplint:ignore` comment
// (same line, preceding line) or the file carries `//gcplint:ignore-file`.
func suppressed(pass *analysis.Pass, file *ast.File, pos token.Pos, rule string) bool {
	line := pass.Fset.Position(pos).Line
	for _, cg := range file.Comments {
		for _, c := range cg.List {
			if !strings.Contains(c.Text, "gcplint:ignore") {
				continue
			}
			if !ignoreMatches(c.Text, rule) {
				continue
			}
			if strings.Contains(c.Text, "gcplint:ignore-file") {
				return true
			}
			cl := pass.Fset.Position(c.Slash).Line
			if cl == line || cl == line-1 {
				return true
			}
		}
	}
	return false
}

// ignoreMatches parses the tail of a `gcplint:ignore[-file]` comment and reports
// whether it covers rule. A bare directive (no rule names) covers every rule.
func ignoreMatches(text, rule string) bool {
	idx := strings.Index(text, "gcplint:ignore")
	if idx < 0 {
		return false
	}
	rest := text[idx+len("gcplint:ignore"):]
	rest = strings.TrimPrefix(rest, "-file")
	if i := strings.Index(rest, "//"); i >= 0 {
		rest = rest[:i]
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return true
	}
	for _, f := range strings.FieldsFunc(rest, func(r rune) bool { return r == ' ' || r == ',' || r == '\t' }) {
		if f == rule {
			return true
		}
	}
	return false
}

func strVal(lit *ast.BasicLit) (string, bool) {
	if lit == nil || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return s, true
}

// calledFuncName returns "pkg.Func" for a selector call and "Func" for a plain
// identifier call, resolved through type information when available. Used for
// constructor detection where the package matters.
func calledFuncName(pass *analysis.Pass, call *ast.CallExpr) string {
	switch f := call.Fun.(type) {
	case *ast.SelectorExpr:
		if obj, ok := pass.TypesInfo.Uses[f.Sel].(*types.Func); ok && obj.Pkg() != nil {
			return obj.Pkg().Name() + "." + obj.Name()
		}
		if id, ok := f.X.(*ast.Ident); ok {
			return id.Name + "." + f.Sel.Name
		}
		return f.Sel.Name
	case *ast.Ident:
		return f.Name
	}
	return ""
}

// calleeName returns the unqualified function name of a call (e.g.
// "RegisterResetter" for adminHandler.RegisterResetter).
func calleeName(call *ast.CallExpr) string {
	switch f := call.Fun.(type) {
	case *ast.SelectorExpr:
		return f.Sel.Name
	case *ast.Ident:
		return f.Name
	}
	return ""
}

// ---- type helpers ---------------------------------------------------------

func namedType(t types.Type) *types.Named {
	for {
		switch v := t.(type) {
		case *types.Named:
			return v
		case *types.Pointer:
			t = v.Elem()
		case *types.Alias:
			t = v.Rhs()
		default:
			return nil
		}
	}
}

func hasResetMethod(nt *types.Named) bool {
	for _, t := range []types.Type{nt, types.NewPointer(nt)} {
		ms := types.NewMethodSet(t)
		for i := 0; i < ms.Len(); i++ {
			m := ms.At(i).Obj()
			if m.Name() != "Reset" {
				continue
			}
			sig, ok := m.Type().(*types.Signature)
			if !ok || sig.Params().Len() != 1 || sig.Results().Len() != 0 {
				continue
			}
			if isContextType(sig.Params().At(0).Type()) {
				return true
			}
		}
	}
	return false
}

func hasMutatingMethod(nt *types.Named) bool {
	ms := types.NewMethodSet(types.NewPointer(nt))
	for i := 0; i < ms.Len(); i++ {
		for _, p := range mutatingPrefixes {
			if strings.HasPrefix(ms.At(i).Obj().Name(), p) {
				return true
			}
		}
	}
	return false
}

func isContextType(t types.Type) bool {
	nt := namedType(t)
	if nt == nil || nt.Obj().Pkg() == nil {
		return false
	}
	return nt.Obj().Pkg().Path() == "context" && nt.Obj().Name() == "Context"
}

// transitiveImports returns pkg and every package reachable from it.
func transitiveImports(pkg *types.Package) map[*types.Package]bool {
	seen := map[*types.Package]bool{}
	var walk func(*types.Package)
	walk = func(p *types.Package) {
		if p == nil || seen[p] {
			return
		}
		seen[p] = true
		for _, imp := range p.Imports() {
			walk(imp)
		}
	}
	walk(pkg)
	return seen
}

func matchesAny(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func fileForImport(pass *analysis.Pass, path string) *ast.File {
	for _, file := range pass.Files {
		for _, imp := range file.Imports {
			if p, ok := strVal(imp.Path); ok && p == path {
				return file
			}
		}
	}
	if len(pass.Files) > 0 {
		return pass.Files[0]
	}
	return nil
}

func importPos(pass *analysis.Pass, path string) token.Pos {
	for _, file := range pass.Files {
		for _, imp := range file.Imports {
			if p, ok := strVal(imp.Path); ok && p == path {
				return imp.Pos()
			}
		}
	}
	if len(pass.Files) > 0 {
		return pass.Files[0].Package
	}
	return token.NoPos
}

// ---- allowlist ------------------------------------------------------------

type allowlist struct {
	rulePaths map[string][]string
}

func (a *allowlist) match(file, rule string) bool {
	if a == nil {
		return false
	}
	f := filepath.ToSlash(file)
	for _, suffix := range a.rulePaths[rule] {
		if strings.HasSuffix(f, suffix) {
			return true
		}
	}
	return false
}

var (
	allowOnce sync.Once
	allowData *allowlist
)

// allow returns the process-wide allowlist, loaded once from the file named by
// GCPLINT_ALLOWLIST (default tools/lint/gcplint/allowlist.txt relative to the
// working directory).
func allow() *allowlist {
	allowOnce.Do(func() { allowData = loadAllowlist() })
	return allowData
}

func allowlistPath() string {
	if p := os.Getenv("GCPLINT_ALLOWLIST"); p != "" {
		return p
	}
	return "tools/lint/gcplint/allowlist.txt"
}

// loadAllowlist reads the central allowlist file. A missing file yields an empty
// allowlist; a malformed line is reported on stderr and skipped.
func loadAllowlist() *allowlist {
	a := &allowlist{rulePaths: map[string][]string{}}
	f, err := os.Open(allowlistPath())
	if err != nil {
		return a
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		entry, reason, found := strings.Cut(text, "#")
		entry = strings.TrimSpace(entry)
		if !found || strings.TrimSpace(reason) == "" {
			fmt.Fprintf(os.Stderr, "gcplint: allowlist %s:%d: entry needs a trailing '# reason'\n", allowlistPath(), line)
			continue
		}
		path, rule, ok := strings.Cut(entry, ":")
		if !ok || path == "" || rule == "" {
			fmt.Fprintf(os.Stderr, "gcplint: allowlist %s:%d: want <path>:<rule> # reason\n", allowlistPath(), line)
			continue
		}
		a.rulePaths[strings.TrimSpace(rule)] = append(a.rulePaths[strings.TrimSpace(rule)], filepath.ToSlash(strings.TrimSpace(path)))
	}
	return a
}
